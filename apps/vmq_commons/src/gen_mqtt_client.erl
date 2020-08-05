%% Copyright 2018 Erlio GmbH Basel Switzerland (http://erl.io)
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(gen_mqtt_client).
-behaviour(gen_fsm).
-include("vmq_types.hrl").

-ifdef(nowarn_gen_fsm).
-compile([{nowarn_deprecated_function,
          [{gen_fsm,start,3},
           {gen_fsm,start,4},
           {gen_fsm,start_link,3},
           {gen_fsm,start_link,4},
           {gen_fsm,send_event,2},
           {gen_fsm,send_all_state_event,2},
           {gen_fsm,sync_send_all_state_event,2},
           {gen_fsm,send_event_after,2},
           {gen_fsm,cancel_timer,1}]}]).
-endif.

-define(PD_QDROP, pd_queue_drop).

-callback init(Args :: any()) ->
    {ok, State :: any()} |
    {ok, State :: any(), Extra :: any()} |
    {stop, Reason :: any()}.
-callback handle_call(Req :: any(), From :: any(), State :: any()) ->
    {reply, Reply :: any(), State :: any()} |
    {reply, Reply :: any(), State :: any(), Extra :: any()} |
    {noreply, State :: any()} |
    {noreply, State :: any(), Extra :: any()} |
    {stop, Reason :: any(), State :: any()} |
    {stop, Reason :: any(), Reply :: any(), State :: any()}.
-callback handle_info(Req :: any(), State :: any()) ->
    {noreply, State :: any()} |
    {noreply, State :: any(), Extra :: any()} |
    {stop, Reason :: any(), State :: any()} |
    {stop, Reason :: any(), Reply :: any(), State :: any()}.
-callback handle_cast(Req :: any(), State :: any()) ->
    {ok, State :: any()} |
    {ok, State :: any(), Extra :: any()} |
    {stop, Reason :: any(), State :: any()}.
-callback terminate(Reason :: any(), State :: any()) ->
    ok.
-callback code_change(_OldVsn :: any(), State :: any(), Extra :: any()) ->
    {ok, State :: any()}.
-callback on_connect(State :: any()) ->
    {ok, State :: any()} |
    {stop, Reason :: any()}.
-callback on_connect_error(Reason :: any(), State :: any()) ->
    {ok, State :: any()} |
    {stop, Reason :: any()}.
-callback on_disconnect(State :: any()) ->
    {ok, State :: any()} |
    {stop, Reason :: any()}.
-callback on_subscribe(Topics :: [any()], State :: any()) ->
    {ok, State :: any()} |
    {stop, Reason :: any()}.
-callback on_unsubscribe(Topics :: [any()], State :: any()) ->
    {ok, State :: any()} |
    {stop, Reason :: any()}.
-callback on_publish(Topic :: any(), Payload :: binary(), Opts :: map(), State :: any()) ->
    {ok, State :: any()} |
    {stop, Reason :: any()}.


%startup
-export([start_link/3,
         start_link/4,
         start/3,
         start/4,
         info/1,
         stats/1,
         subscribe/2,
         subscribe/3,
         unsubscribe/2,
         publish/4,
         publish/5,
         disconnect/1,
         call/2,
         cast/2,
         metrics/2]).


%% gen_fsm callbacks
-export([init/1,
    handle_info/3,
    handle_event/3,
    handle_sync_event/4,
    code_change/4,
    terminate/3]).

% fsm state
-export([
    waiting_for_connack/2,
    connected/2,
    connecting/2]).

-define(MQTT_PROTO_MAJOR, 3).

-record(queue, {
          queue = queue:new() :: queue:queue(),
          %% max queue size. 0 means disabled.
          max = 0 :: non_neg_integer(),
          size = 0 :: non_neg_integer(),
          drop = 0 :: non_neg_integer()
         }).

-type queue() :: #queue{}.

-record(state, {host                      :: inet:ip_address(),
                port                      :: inet:port_number(),
                sock                      :: undefined | gen_tcp:socket() | ssl:sslsocket(),
                msgid = 1                 :: non_neg_integer(),
                username                  :: binary(),
                password                  :: binary(),
                client                    :: string() ,
                clean_session = false     :: boolean(),
                last_will_topic           :: string() | undefined,
                last_will_msg             :: string() | undefined,
                last_will_qos             :: non_neg_integer(),
                buffer = <<>>             :: binary(),
                o_queue = #queue{}        :: queue(),
                waiting_acks = maps:new() :: map(),
                unacked_msgs = maps:new() :: map(),
                ping_tref                 :: timer:ref() | undefined,
                reconnect_timeout,
                keepalive_interval = 60000,
                retry_interval = 10000,
                proto_version = ?MQTT_PROTO_MAJOR,
                %%
                mod,
                mod_state=[],
                transport,
                parser = <<>>,
                info_fun}).

start_link(Module, Args, Opts) ->
    GenFSMOpts = proplists:get_value(gen_fsm, Opts, []),
    gen_fsm:start_link(?MODULE, [Module, Args, Opts], GenFSMOpts).

start_link(Name, Module, Args, Opts) ->
    GenFSMOpts = proplists:get_value(gen_fsm, Opts, []),
    gen_fsm:start_link(Name, ?MODULE, [Module, Args, Opts], GenFSMOpts).

start(Module, Args, Opts) ->
    GenFSMOpts = proplists:get_value(gen_fsm, Opts, []),
    gen_fsm:start(?MODULE, [Module, Args, Opts], GenFSMOpts).

start(Name, Module, Args, Opts) ->
    GenFSMOpts = proplists:get_value(gen_fsm, Opts, []),
    gen_fsm:start(Name, ?MODULE, [Module, Args, Opts], GenFSMOpts).

info(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, info, infinity).

metrics(Pid, CR) -> gen_fsm:sync_send_all_state_event(Pid, {get_metrics, CR}, infinity).

stats(Pid) ->
    case erlang:process_info(Pid, [dictionary]) of
        undefined ->
            undefined;
        [{dictionary, PD}] ->
            #{dropped => proplists:get_value(?PD_QDROP, PD, 0)}
    end.

publish(P, Topic, Payload, Qos) ->
    publish(P, Topic, Payload, Qos, false).

publish(P, Topic, Payload, Qos, Retain) ->
    gen_fsm:send_event(P, {publish, {Topic, Payload, Qos, Retain, false}}).

subscribe(P, [T|_] = Topics) when is_tuple(T) ->
    gen_fsm:send_event(P, {subscribe, Topics}).

subscribe(P, Topic, QoS) ->
    subscribe(P, [{Topic, QoS}]).

unsubscribe(P, [T|_] = Topics) when is_list(T) ->
    gen_fsm:send_event(P, {unsubscribe, Topics});
unsubscribe(P, Topic) ->
    unsubscribe(P, [Topic]).

disconnect(P) ->
    gen_fsm:send_event(P, disconnect).

call(P, Req) ->
    gen_fsm:sync_send_all_state_event(P, Req).

cast(P, Req) ->
    gen_fsm:send_all_state_event(P, Req).

wrap_res(StateName, init, Args, #state{mod=Mod} = State) ->
    case erlang:apply(Mod, init, Args) of
        {ok, ModState} ->
            {ok, StateName, State#state{mod_state=ModState}};
        {ok, ModState, Extra} ->
            {ok, StateName, State#state{mod_state=ModState}, Extra};
        {stop, Reason} ->
            {stop, Reason}
    end;
wrap_res(StateName, Function, Args, #state{mod=Mod, mod_state=ModState} = State) ->
    wrap_res(erlang:apply(Mod, Function, Args ++ [ModState]), StateName, State).

wrap_res({ok, ModState}, StateName, State) ->
    {next_state, StateName, State#state{mod_state=ModState}};
wrap_res({ok, ModState, Extra}, StateName, State) ->
    {next_state, StateName, State#state{mod_state=ModState}, Extra};
wrap_res({stop, Reason}, _StateName, _State) ->
    {stop, Reason};
wrap_res({reply, Reply, ModState}, StateName, State) ->
    {reply, Reply, StateName, State#state{mod_state=ModState}};
wrap_res({reply, Reply, ModState, Extra}, StateName, State) ->
    {reply, Reply, StateName, State#state{mod_state=ModState}, Extra};
wrap_res({noreply, ModState}, StateName, State) ->
    {next_state, StateName, State#state{mod_state=ModState}};
wrap_res({noreply, ModState, Extra}, StateName, State) ->
    {next_state, StateName, State#state{mod_state=ModState}, Extra};
wrap_res({stop, Reason, ModState}, _StateName, State) ->
    {stop, Reason, State#state{mod_state=ModState}};
wrap_res({stop, Reason, Reply, ModState}, _StateName, State) ->
    {stop, Reason, Reply, State#state{mod_state=ModState}};
wrap_res({ok}, _StateName, _State) ->
    ok;
wrap_res(ok, _StateName, _State) ->
    ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% State Callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
connecting(connect, State) ->
    #state{host = Host, port = Port, transport={Transport, Opts}, client=ClientId, info_fun=InfoFun} = State,
    case Transport:connect(Host, Port, [binary, {packet, raw}|Opts]) of
        {ok, Sock} ->
            NewInfoFun = call_info_fun({connect_out, ClientId}, InfoFun),
            NewState = State#state{sock=Sock, buffer= <<>>, info_fun=NewInfoFun},
            send_connect(NewState),
            active_once(Transport, Sock),
            {next_state, waiting_for_connack, NewState};
        {error, _Reason} ->
            error_logger:error_msg("connection to ~p:~p failed due to ~p", [Host, Port, _Reason]),
            gen_fsm:send_event_after(3000, connect),
            wrap_res(connecting, on_connect_error, [server_not_found], State)
    end;
connecting(disconnect, State) ->
    {stop,  normal, State};
connecting({publish, PubReq}, State) ->
    NewState = maybe_queue_outgoing(PubReq, State),
    {next_state, connecting, NewState};
connecting(_Event, State) ->
    {next_state, connecting, State}.

waiting_for_connack({publish, PubReq}, State) ->
    NewState = maybe_queue_outgoing(PubReq, State),
    {next_state, waiting_for_connack, NewState};
waiting_for_connack(_Event, State) ->
    {next_state, waiting_for_connack, State}.

connected({subscribe, Topics} = Msg, State=#state{transport={Transport,_}, msgid=MsgId,
    sock=Sock, info_fun=InfoFun}) ->
    Frame = #mqtt_subscribe{
        message_id = MsgId,
        topics = Topics
    },
    NewInfoFun = call_info_fun({subscribe_out, MsgId}, InfoFun),
    send_frame(Transport, Sock, Frame),
    Key = {subscribe, MsgId},
    {next_state, connected, retry(Key, Msg,
                                  State#state{msgid='++'(MsgId),
                                              info_fun=NewInfoFun})};

connected({unsubscribe, Topics} = Msg, State=#state{transport={Transport, _}, sock=Sock,
    msgid=MsgId, info_fun=InfoFun}) ->
    Frame = #mqtt_unsubscribe{
        message_id = MsgId,
        topics = Topics
    },
    NewInfoFun = call_info_fun({unsubscribe_out, MsgId}, InfoFun),
    send_frame(Transport, Sock, Frame),
    Key = {unsubscribe, MsgId},
    {next_state, connected, retry(Key, Msg,
                                  State#state{msgid='++'(MsgId),
                                              info_fun=NewInfoFun})};

connected({publish, PubReq}, #state{o_queue=#queue{size=Size} = _Q} = State) when Size > 0 ->
    NewState = maybe_queue_outgoing(PubReq, State),
    {next_state, connected, NewState};
                                            
connected({publish, PubReq}, State) ->
    {next_state, connected, send_publish(PubReq, State)};
                                            
connected({publish_from_queue, PubReq}, State) ->
    State1 = send_publish(PubReq, State),
    {next_state, connected, maybe_publish_offline_msgs(State1)};

connected({retry, Key}, State) ->
    {next_state, connected, handle_retry(Key, State)};

connected({ack, _MsgId}, State) ->
    {next_state, connected, State};

connected(ping, #state{transport={Transport, _}, sock=Sock,
    keepalive_interval=Int} = State) ->
    send_ping(Transport, Sock),
    Ref = gen_fsm:send_event_after(Int, ping),
    {next_state, connected, State#state{ping_tref=Ref}};

connected(disconnect, State=#state{transport={Transport, _}, sock=Sock}) ->
    send_disconnect(Transport, Sock),
    {stop, normal, State};

connected(maybe_reconnect, State) ->
    maybe_reconnect(on_disconnect, [], State);

connected(_Event, State) ->
    {stop, unknown_event, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_fsm callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init([Mod, Args, Opts]) ->
    Host = proplists:get_value(host, Opts, "localhost"),
    Port = proplists:get_value(port, Opts, 1883),
    Username = proplists:get_value(username, Opts, undefined),
    Password = proplists:get_value(password, Opts, undefined),
    ClientId = proplists:get_value(client, Opts, "vmq-bridge"),
    CleanSession = proplists:get_value(clean_session, Opts, true),
    LWTopic = proplists:get_value(last_will_topic, Opts, undefined),
    LWMsg = proplists:get_value(last_will_msg, Opts, undefined),
    LWQos = proplists:get_value(last_will_qos, Opts, 0),
    ReconnectTimeout = proplists:get_value(reconnect_timeout, Opts, undefined),
    KeepAliveInterval = proplists:get_value(keepalive_interval, Opts, 60),
    RetryInterval = proplists:get_value(retry_interval, Opts, 10),
    ProtoVer = proplists:get_value(proto_version, Opts, ?MQTT_PROTO_MAJOR),
    InfoFun = proplists:get_value(info_fun, Opts, {fun(_,_) -> ok end, []}),
    MaxQueueSize = proplists:get_value(max_queue_size, Opts, 0),
    {Transport, TransportOpts} = proplists:get_value(transport, Opts, {gen_tcp, []}),
    State = #state{host = Host, port = Port, proto_version=ProtoVer,
        username = Username, password = Password, client=ClientId, mod=Mod,
        clean_session=CleanSession, last_will_topic=LWTopic, last_will_msg=LWMsg, last_will_qos=LWQos,
        reconnect_timeout=case ReconnectTimeout of undefined -> undefined; _ -> 1000 * ReconnectTimeout end,
        keepalive_interval=1000 * KeepAliveInterval,
        retry_interval=1000* RetryInterval, transport={Transport, TransportOpts},
        o_queue = #queue{max=MaxQueueSize},
        info_fun=InfoFun},
    Res = wrap_res(connecting, init, [Args], State),
    gen_fsm:send_event_after(0, connect),
    Res.

handle_info({ssl, Socket, Bin}, StateName, #state{sock=Socket} = State) ->
    #state{transport={Transport, _}, buffer=Buffer} = State,
    active_once(Transport, Socket),
    process_bytes(<<Buffer/binary, Bin/binary>>, StateName, State);
handle_info({tcp, Socket, Bin}, StateName, #state{sock=Socket} = State) ->
    #state{transport={Transport, _}, buffer=Buffer} = State,
    active_once(Transport, Socket),
    process_bytes(<<Buffer/binary, Bin/binary>>, StateName, State);


handle_info({ssl_closed, Sock}, _, State=#state{sock=Sock, reconnect_timeout=undefined}) ->
    {stop, normal, State};
handle_info({ssl_closed, Sock}, _, State=#state{sock=Sock, reconnect_timeout=Timeout}) ->
    gen_fsm:send_event_after(Timeout, connect),
    wrap_res(connecting, on_disconnect, [], cleanup_session(State#state{sock=undefined}));
handle_info({ssl_error, Sock, _}, _, State=#state{sock=Sock, reconnect_timeout=undefined}) ->
    {stop, normal, State};
handle_info({ssl_error, Sock, _}, _, State=#state{sock=Sock, reconnect_timeout=Timeout}) ->
    gen_fsm:send_event_after(Timeout, connect),
    wrap_res(connecting, on_disconnect, [], cleanup_session(State#state{sock=undefined}));
handle_info({tcp_closed, Sock}, _, State=#state{sock=Sock, reconnect_timeout=undefined}) ->
    {stop, normal, State};
handle_info({tcp_closed, Sock}, _, State=#state{sock=Sock, reconnect_timeout=Timeout}) ->
    gen_fsm:send_event_after(Timeout, connect),
    wrap_res(connecting, on_disconnect, [], cleanup_session(State#state{sock=undefined}));
handle_info({tcp_error, Sock, _Error}, _, State=#state{sock=Sock, reconnect_timeout=undefined}) ->
    {stop, normal, State};
handle_info({tcp_error, Sock, _}, _, State=#state{sock=Sock, reconnect_timeout=Timeout}) ->
    gen_fsm:send_event_after(Timeout, connect),
    wrap_res(connecting, on_disconnect, [], cleanup_session(State#state{sock=undefined}));

handle_info(Info, StateName, State) ->
    wrap_res(StateName, handle_info, [Info], State).

process_bytes(Bytes, StateName, #state{parser=ParserState} = State) ->
    Data = <<ParserState/binary, Bytes/binary>>,
    case vmq_parser:parse(Data) of
        {error, _Reason} ->
            {next_state, StateName, State#state{parser= <<>>}};
        more ->
            {next_state, StateName, State#state{parser=Data}};
        {Frame, Rest} ->
            {next_state, NextStateName, NewState}
                = handle_frame(StateName, Frame, State),
            process_bytes(Rest, NextStateName, NewState#state{parser= <<>>})
    end.

handle_frame(waiting_for_connack, #mqtt_connack{return_code=ReturnCode}, State0) ->
    #state{client=ClientId, info_fun=InfoFun} = State0,
    case ReturnCode of
        ?CONNACK_ACCEPT ->
            NewInfoFun = call_info_fun({connack_in, ClientId}, InfoFun),
            State1 = resume_wacks_retry(State0),
            State2 = maybe_publish_offline_msgs(State1),
            wrap_res(connected, on_connect, [], start_ping_timer(State2#state{info_fun=NewInfoFun}));
        ?CONNACK_PROTO_VER ->
            maybe_reconnect(on_connect_error, [wrong_protocol_version], State0);
        ?CONNACK_INVALID_ID ->
            maybe_reconnect(on_connect_error, [invalid_id], State0);
        ?CONNACK_SERVER ->
            maybe_reconnect(on_connect_error, [server_not_available], State0);
        ?CONNACK_CREDENTIALS ->
            maybe_reconnect(on_connect_error, [invalid_credentials], State0);
        ?CONNACK_AUTH ->
            maybe_reconnect(on_connect_error, [not_authorized], State0)
    end;

handle_frame(connected, #mqtt_suback{message_id=MsgId, qos_table=QoSTable}, State0) ->
    #state{info_fun=InfoFun} = State0,
    Key = {subscribe, MsgId},
    case cancel_retry_and_get(Key, State0) of
        {ok, {{subscribe, Topics}, State1}} ->
            NewInfoFun = call_info_fun({suback, MsgId}, InfoFun),
            {TopicNames, _} = lists:unzip(Topics),
            case length(TopicNames) == length(QoSTable) of
                true ->
                    wrap_res(connected, on_subscribe, [lists:zip(TopicNames, QoSTable)],
                             State1#state{info_fun=NewInfoFun});
                false ->
                    wrap_res(connected, on_subscribe, [{error, Topics, QoSTable}],
                             State1#state{info_fun=NewInfoFun})
            end;
        {error, not_found} ->
            {next_state, connected, State0}
    end;

handle_frame(connected, #mqtt_unsuback{message_id=MsgId}, State0) ->
    #state{info_fun=InfoFun} = State0,
    Key = {unsubscribe, MsgId},
    case cancel_retry_and_get(Key, State0) of
        {ok, {{unsubscribe, Topics}, State1}} ->
            NewInfoFun = call_info_fun({unsuback, MsgId}, InfoFun),
            wrap_res(connected, on_unsubscribe, [Topics],
                State1#state{info_fun=NewInfoFun});
        {error, not_found} ->
            {next_state, connected, State0}
    end;

handle_frame(connected, #mqtt_puback{message_id=MessageId}, State0) ->
    #state{info_fun=InfoFun} = State0,
    %% qos1 flow
    Key = {publish, MessageId},
    case cancel_retry_and_get(Key, State0) of
        {ok, {#mqtt_publish{}, State1}} ->
            NewInfoFun = call_info_fun({puback_in, MessageId}, InfoFun),
            {next_state, connected, State1#state{info_fun=NewInfoFun}};
        {error, not_found} ->
            {next_state, connected, State0}
    end;

handle_frame(connected, #mqtt_pubrec{message_id=MessageId}, State0) ->
    #state{transport={Transport, _}, sock=Socket, info_fun=InfoFun} = State0,
    %% qos2 flow
    Key = {publish, MessageId},
    case cancel_retry_and_get(Key, State0) of
        {ok, {_Publish, State1}} ->
            NewInfoFun0 = call_info_fun({pubrec_in, MessageId}, InfoFun),
            NewKey = {pubrel, MessageId},
            PubRelFrame = #mqtt_pubrel{message_id=MessageId},
            send_frame(Transport, Socket, PubRelFrame),
            NewInfoFun1 = call_info_fun({pubrel_out, MessageId}, NewInfoFun0),
            {next_state, connected, retry(NewKey, PubRelFrame, State1#state{info_fun=NewInfoFun1})};
        {error, not_found} ->
            {next_state, connected, State0}
    end;

handle_frame(connected, #mqtt_pubrel{message_id=MessageId}, State0) ->
    #state{transport={Transport,_}, sock=Socket, info_fun=InfoFun} = State0,
    %% qos2 flow
    case get_remove_unacked_msg(MessageId, State0) of
        {ok, {{Topic, Payload, Opts}, State1}} ->
            NewInfoFun0 = call_info_fun({pubrel_in, MessageId}, InfoFun),
            {next_state, connected, State2} = wrap_res(connected, on_publish, [Topic, Payload, Opts], State1),
            NewInfoFun1 = call_info_fun({pubcomp_out, MessageId}, NewInfoFun0),
            PubCompFrame = #mqtt_pubcomp{message_id=MessageId},
            send_frame(Transport, Socket, PubCompFrame),
            {next_state, connected, State2#state{info_fun=NewInfoFun1}};
        error ->
            {next_state, connected, State0}
    end;

handle_frame(connected, #mqtt_pubcomp{message_id=MessageId}, State0) ->
    #state{info_fun=InfoFun} = State0,
    %% qos2 flow
    Key = {pubrel, MessageId},
    case cancel_retry_and_get(Key, State0) of
        {ok, {#mqtt_pubrel{}, State1}} ->
            NewInfoFun = call_info_fun({pubcomp_in, MessageId}, InfoFun),
            {next_state, connected, State1#state{info_fun=NewInfoFun}};
        {error, not_found} ->
            {next_state, connected, State0}
    end;

handle_frame(connected, #mqtt_publish{message_id=MessageId, topic=Topic,
    qos=QoS, payload=Payload, retain=Retain, dup=Dup}, State) ->
    #state{transport={Transport, _}, sock=Socket, info_fun=InfoFun} = State,
    NewInfoFun = call_info_fun({publish_in, MessageId, Payload, QoS}, InfoFun),
    Opts = #{qos => QoS,
             retain => unflag(Retain),
             dup => unflag(Dup)},
    case QoS of
        0 ->
            wrap_res(connected, on_publish, [Topic, Payload, Opts], State#state{info_fun=NewInfoFun});
        1 ->
            PubAckFrame = #mqtt_puback{message_id=MessageId},
            NewInfoFun1 = call_info_fun({puback_out, MessageId}, NewInfoFun),
            Res = wrap_res(connected, on_publish, [Topic, Payload, Opts], State#state{info_fun=NewInfoFun1}),
            send_frame(Transport, Socket, PubAckFrame),
            Res;
        2 ->
            PubRecFrame = #mqtt_pubrec{message_id=MessageId},
            NewInfoFun1 = call_info_fun({pubrec_out, MessageId}, NewInfoFun),
            send_frame(Transport, Socket, PubRecFrame),
            {next_state, connected, store_unacked_msg(MessageId, {Topic, Payload, Opts},
                                                          State#state{info_fun=NewInfoFun1})}
    end;

handle_frame(connected, #mqtt_pingresp{}, State) ->
    {next_state, connected, State};

handle_frame(connected, #mqtt_disconnect{}, #state{transport={Transport, _}} = State) ->
    Transport:close(State#state.sock),
    gen_fsm:send_event_after(5000, connect),
    wrap_res(connecting, on_disconnect, [], cleanup_session(State#state{sock=undefined})).

handle_event(Event, StateName, State) ->
    wrap_res(StateName, handle_cast, [Event], State).

handle_sync_event(info, _From, StateName,
                  #state{o_queue=#queue{size=Size,drop=Drop,max=Max}}=State) ->
    {message_queue_len, Len} = erlang:process_info(self(), message_queue_len),
    Info =
        #{out_queue_size=>Size,
          out_queue_dropped=>Drop,
          out_queue_max_size=>Max,
          process_mailbox_size => Len},
    {reply, {ok, Info}, StateName, State};

handle_sync_event({get_metrics, CR}, _From, StateName, State) ->
    Metrics = #{
    vmq_bridge_publish_out_0 => counters:get(CR, 1),
    vmq_bridge_publish_out_1 => counters:get(CR, 2),
    vmq_bridge_publish_out_2 => counters:get(CR, 3),
    vmq_bridge_publish_in_0 => counters:get(CR, 4),
    vmq_bridge_publish_in_1 => counters:get(CR, 5),
    vmq_bridge_publish_in_2 => counters:get(CR, 6)},
{reply, {ok, Metrics}, StateName, State};

handle_sync_event(Req, From, StateName, State) ->
    wrap_res(StateName, handle_call, [Req, From], State).

terminate(Reason, StateName, State) ->
    wrap_res(StateName, terminate, [Reason], State).

code_change(OldVsn, StateName, State, Extra) ->
    #state{mod=Mod, mod_state=ModState} = State,
    {ok, NewModState} = erlang:apply(Mod, code_change, [OldVsn, ModState, Extra]),
    {ok, StateName, State#state{mod_state=NewModState}}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% INTERNAL
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
send_connect(State= #state{transport={Transport, _}, sock=Sock, username=Username, password=Password, client=ClientId,
    clean_session=CleanSession, last_will_topic=LWTopic, last_will_msg=LWMsg,
    last_will_qos=LWQoS, proto_version=ProtoVer, keepalive_interval=Int}) ->
    Frame = #mqtt_connect{
        proto_ver = ProtoVer,
        username = Username,
        password = Password,
        clean_session = CleanSession,
        keep_alive = Int div 1000,
        client_id = list_to_binary(ClientId),
        will_retain = (LWTopic /= undefined) and (LWMsg /= undefined),
        will_qos   = case (LWTopic /= undefined) and (LWMsg /= undefined) of
                         true when is_integer(LWQoS) -> LWQoS;
                         _ -> 0
                     end,
        will_topic = LWTopic,
        will_msg = LWMsg
    },
    send_frame(Transport, Sock, Frame),
    State.

send_publish({Topic, Payload, QoS, Retain, Dup}, #state{msgid=MsgId} = State) ->
    send_publish(MsgId, Topic, Payload, QoS, Retain, Dup, State#state{msgid='++'(MsgId)});
send_publish({MsgId, Topic, Payload, QoS, Retain, _}, State) ->
    %% called in case of retry
    send_publish(MsgId, Topic, Payload, QoS, Retain, true, State).

send_publish(MsgId, Topic, Payload, QoS, Retain, Dup, State) ->
    #state{transport={Transport, _}, sock = Sock, info_fun=InfoFun} = State,
    Frame = #mqtt_publish{
        message_id = if QoS == 0 ->
            undefined;
                         true ->
                             MsgId
                     end,
        topic = Topic,
        qos = QoS,
        retain = Retain,
        dup = Dup,
        payload = Payload
    },
    NewInfoFun = call_info_fun({publish_out, MsgId, QoS}, InfoFun),
    send_frame(Transport, Sock, Frame),
    case QoS of
        0 ->
            State#state{info_fun=NewInfoFun};
        _ ->
            Key = {publish, MsgId},
            retry(Key, Frame#mqtt_publish{dup=true}, State#state{info_fun=NewInfoFun})
    end.

send_disconnect(Transport, Sock) ->
    send_frame(Transport, Sock, #mqtt_disconnect{}).

send_ping(Transport, Sock) ->
    send_frame(Transport, Sock, #mqtt_pingreq{}).

send_frame(Transport, Sock, Frame) ->
    case Transport:send(Sock, vmq_parser:serialise(Frame)) of
        ok ->
            ok;
        {error, _} ->
            gen_fsm:send_event(self(), maybe_reconnect)
    end.

maybe_reconnect(Fun, Args, #state{client=ClientId, reconnect_timeout=Timeout, transport={Transport,_}, info_fun=InfoFun} = State) ->
    case Timeout of
        undefined ->
            {stop, normal, State};
        _ ->
            Transport:close(State#state.sock),
            gen_fsm:send_event_after(Timeout, connect),
            NewInfoFun = call_info_fun({reconnect, ClientId}, InfoFun),
            wrap_res(connecting, Fun, Args,
                     cleanup_session(State#state{sock=undefined, info_fun=NewInfoFun}))
    end.

maybe_queue_outgoing(_PubReq, #state{o_queue=#queue{max=0}}=State) ->
    %% queue is disabled
    State;
maybe_queue_outgoing(PubReq, #state{o_queue=#queue{size=Size,max=Max}=Q}=State)
  when Size < Max ->
    State#state{o_queue=queue_outgoing(PubReq, Q)};
maybe_queue_outgoing(_PubReq, #state{o_queue=Q}=State) ->
    %% drop!
    State#state{o_queue=drop(Q)}.

queue_outgoing(Msg, #queue{size=Size, queue=QQ} = Q) ->
    Q#queue{size=Size+1, queue=queue:in(Msg,QQ)}.

maybe_publish_offline_msgs(#state{o_queue=#queue{size=Size} = Q} = State) when Size > 0 ->
    publish_from_queue(Q, State);
maybe_publish_offline_msgs(State) -> State.

publish_from_queue(#queue{size=Size, queue=QQ} = Q, State0) when Size > 0 ->
    {{value, PubReq}, NewQQ} = queue:out(QQ),
    gen_fsm:send_event(self(), {publish_from_queue, PubReq}),
    State0#state{o_queue=Q#queue{size=Size-1, queue=NewQQ}}.

drop(#queue{drop=D}=Q) ->
    put(?PD_QDROP, D+1),
    Q#queue{drop=D+1}.

handle_retry(Key, #state{transport={Transport,_}, sock=Sock, waiting_acks=WAcks} = State) ->
    case maps:find(Key, WAcks) of
        error ->
            State;
        {ok, {_Ref, #mqtt_publish{} = Frame}} ->
            send_frame(Transport, Sock, Frame#mqtt_publish{dup = true}),
            retry(Key, Frame, State);
        {ok, {_Ref, #mqtt_pubrel{} = Frame}} ->
            send_frame(Transport, Sock, Frame),
            retry(Key, Frame, State);
        {ok, {_Ref, Msg}} ->
            gen_fsm:send_event(self(), Msg),
            State#state{waiting_acks=maps:remove(Key, WAcks)}
    end.

retry(Key, Message, #state{retry_interval=RetryInterval, waiting_acks=WAcks} = State) ->
    NewRef = gen_fsm:send_event_after(RetryInterval, {retry, Key}),
    NewWAcks = maps:remove(Key, WAcks),
    State#state{waiting_acks=maps:put(Key, {NewRef, Message}, NewWAcks)}.

cancel_retry_and_get(Key, #state{waiting_acks=WAcks} = State) ->
    case maps:find(Key, WAcks) of
        {ok, {Ref, Val}} ->
            gen_fsm:cancel_timer(Ref),
            {ok, {Val, State#state{waiting_acks=maps:remove(Key, WAcks)}}};
        error ->
            {error, not_found}
    end.

cleanup_session(State0) ->
    State1 = cancel_wacks_retry(State0),
    State2 = cancel_ping_timer(State1),
    cleanup_unacked_msgs(State2).

cancel_wacks_retry(#state{waiting_acks=WAcks} = State) ->
    _ = maps:map(fun(_Key, {Ref, _Msg}) ->
                         gen_fsm:cancel_timer(Ref)
                 end, WAcks),
    case State#state.clean_session of
        true ->
            State#state{waiting_acks=maps:new()};
        false ->
            State
    end.

resume_wacks_retry(#state{waiting_acks=WAcks} = State) ->
    maps:fold(fun(Key, _, AccState) ->
                      handle_retry(Key, AccState)
              end, State, WAcks).

start_ping_timer(#state{keepalive_interval=0} = State) -> State;
start_ping_timer(#state{keepalive_interval=Int} = State) ->
    Ref = gen_fsm:send_event_after(Int, ping), % initial ping trigger
    State#state{ping_tref=Ref}.

cancel_ping_timer(#state{ping_tref=undefined} = State) -> State;
cancel_ping_timer(#state{ping_tref=Tref} = State) ->
    gen_fsm:cancel_timer(Tref),
    State#state{ping_tref=undefined}.

cleanup_unacked_msgs(#state{clean_session=true} = State) ->
    State#state{unacked_msgs=maps:new()};
cleanup_unacked_msgs(State) -> State.

store_unacked_msg(MessageId, {_Topic, _Payload, _Opts} = Msg, #state{unacked_msgs=UnackedMsgs} = State) ->
    State#state{unacked_msgs=maps:put(MessageId, Msg, UnackedMsgs)}.

get_remove_unacked_msg(MessageId, #state{unacked_msgs=UnackedMsgs} = State) ->
    case maps:find(MessageId, UnackedMsgs) of
        {ok, {_Topic, _Payload, _Opts} = Msg} ->
            {ok, {Msg, State#state{unacked_msgs=maps:remove(MessageId, UnackedMsgs)}}};
        _ ->
            error
    end.

'++'(65535) -> 1;
'++'(N) -> N + 1.

active_once(gen_tcp, Sock) ->
    ok = inet:setopts(Sock, [{active, once}]);
active_once(ssl, Sock) ->
    ok = ssl:setopts(Sock, [{active, once}]).

call_info_fun(Info, {Fun, FunState}) ->
    {Fun, Fun(Info, FunState)}.

unflag(0) -> false;
unflag(1) -> true.
