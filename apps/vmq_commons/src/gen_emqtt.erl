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

-module(gen_emqtt).
-behavior(gen_fsm).
-include("vmq_types.hrl").

-ifdef(otp20).
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

%startup
-export([start_link/3,
    start_link/4,
    start/3,
    start/4,
    subscribe/2,
    subscribe/3,
    unsubscribe/2,
    publish/4,
    publish/5,
    disconnect/1,
    call/2,
    cast/2]).


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
    connecting/2,
    disconnecting/2]).

-export([behaviour_info/1]).
-define(MQTT_PROTO_MAJOR, 3).
-define(MQTT_PROTO_MINOR, 1).

-record(state, {host                    :: inet:ip_address(),
    port                    :: inet:port_number(),
    sock                    :: gen_tcp:socket() | ssl:socket(),
    msgid = 1               :: non_neg_integer(),
    username                :: binary(),
    password                :: binary(),
    client                  :: string() ,
    clean_session = false   :: boolean(),
    last_will_topic         :: string() | undefined,
    last_will_msg           :: string() | undefined,
    last_will_qos           :: non_neg_integer(),
    buffer = <<>>           :: binary(),
    reconnect_timeout,
    keepalive_interval = 60000,
    retry_interval = 10000,
    proto_version = ?MQTT_PROTO_MAJOR,
    %%
    mod,
    mod_state=[],
    transport,
    parser = <<>>,
    waiting_acks = dict:new(),
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

publish(P, Topic, Payload, Qos) ->
    publish(P, Topic, Payload, Qos, false).

publish(P, Topic, Payload, Qos, Retain) ->
    gen_fsm:send_event(P, {publish, Topic, Payload, Qos, Retain, false}).

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


behaviour_info(callbacks) ->
    [{init,1},
        {handle_call, 3},
        {handle_cast, 2},
        {handle_info, 2},
        {terminate, 2},
        {code_change, 3},
        {on_connect, 1},
        {on_connect_error, 2},
        {on_disconnect, 1},
        {on_subscribe, 2},
        {on_unsubscribe, 2},
        {on_publish, 3}];
behaviour_info(_Other) ->
    undefined.
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
connecting(_Event, State) ->
    {next_state, connecting, State}.

waiting_for_connack(_Event, State) ->
    {next_state, waiting_for_connack, State}.

disconnecting({error, ConnectError}, #state{sock=Sock, transport={Transport,_}} = State) ->
    Transport:close(Sock),
    error_logger:error_msg("bridge disconnected due to connection error ~p", [ConnectError]),
    {stop, ConnectError, State}.

connected({subscribe, Topics}, State=#state{transport={Transport,_}, msgid=MsgId,
    sock=Sock, waiting_acks=WAcks,
    retry_interval=RetryInterval,
    info_fun=InfoFun}) ->
    Frame = #mqtt_subscribe{
        message_id = MsgId,
        topics = Topics
    },
    NewInfoFun = call_info_fun({subscribe_out, MsgId}, InfoFun),
    send_frame(Transport, Sock, Frame),
    Key = {subscribe, MsgId},
    Ref = gen_fsm:send_event_after(RetryInterval, {retry, Key}),
    {next_state, connected, State#state{msgid='++'(MsgId), waiting_acks=store(Key, {Ref, {subscribe, Topics}}, WAcks),
        info_fun=NewInfoFun}};

connected({unsubscribe, Topics}, State=#state{transport={Transport, _}, sock=Sock,
    msgid=MsgId, waiting_acks=WAcks,
    retry_interval=RetryInterval,
    info_fun=InfoFun}) ->
    Frame = #mqtt_unsubscribe{
        message_id = MsgId,
        topics = Topics
    },
    NewInfoFun = call_info_fun({unsubscribe_out, MsgId}, InfoFun),
    send_frame(Transport, Sock, Frame),
    Key = {unsubscribe, MsgId},
    Ref = gen_fsm:send_event_after(RetryInterval, {retry, Key}),
    {next_state, connected, State#state{msgid='++'(MsgId), waiting_acks=store(Key, {Ref, {unsubscribe, Topics}},
        WAcks), info_fun=NewInfoFun}};

connected({publish, Topic, Payload, QoS, Retain, Dup}, #state{msgid=MsgId} = State) ->
    {next_state, connected, send_publish(MsgId, Topic, Payload, QoS, Retain, Dup, State#state{msgid='++'(MsgId)})};

connected({publish, MsgId, Topic, Payload, QoS, Retain, _}, State) ->
    %% called in case of retry
    {next_state, connected, send_publish(MsgId, Topic, Payload, QoS, Retain, true, State)};

connected({retry, Key}, #state{transport={Transport,_}, sock=Sock, waiting_acks=WAcks,
    retry_interval=RetryInterval} = State) ->
    case dict:find(Key, WAcks) of
        error ->
            {next_state, connected, State};
        {ok, {_Ref, #mqtt_publish{} = Frame}} ->
            send_frame(Transport, Sock, Frame#mqtt_publish{dup = true}),
            NewRef = gen_fsm:send_event_after(RetryInterval, {retry, Key}),
            NewDict = erase(Key, WAcks),
            {next_state, connected, State#state{waiting_acks=store(Key, {NewRef, Frame}, NewDict)}};
        {ok, {_Ref, #mqtt_pubrel{} = Frame}} ->
            send_frame(Transport, Sock, Frame),
            NewRef = gen_fsm:send_event_after(RetryInterval, {retry, Key}),
            NewDict = erase(Key, WAcks),
            {next_state, connected, State#state{waiting_acks=store(Key, {NewRef, Frame}, NewDict)}};
        {ok, {_Ref, Msg}} ->
            gen_fsm:send_event(self(), Msg),
            {next_state, connected, State#state{waiting_acks=erase(Key, WAcks)}}
    end;

connected({ack, _MsgId}, State) ->
    {next_state, connected, State};

connected(ping, #state{transport={Transport, _}, sock=Sock,
    waiting_acks=WAcks, keepalive_interval=Int} = State) ->
    send_ping(Transport, Sock),
    Ref = gen_fsm:send_event_after(Int, ping),
    {next_state, connected,
        State#state{waiting_acks=store(ping, Ref, WAcks)}};

connected(disconnect, State=#state{transport={Transport, _}, sock=Sock}) ->
    send_disconnect(Transport, Sock),
    {stop, normal, State};

connected(maybe_reconnect, #state{client=ClientId, reconnect_timeout=Timeout, transport={Transport, _}, info_fun=InfoFun} = State) ->
    case Timeout of
        undefined ->
            {stop, normal, State};
        _ ->
            Transport:close(State#state.sock),
            gen_fsm:send_event_after(Timeout, connect),
            NewInfoFun = call_info_fun({reconnect, ClientId}, InfoFun),
            wrap_res(connecting, on_disconnect, [], State#state{sock=undefined, info_fun=NewInfoFun})
    end;

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
    ClientId = proplists:get_value(client, Opts, "emqttc"),
    CleanSession = proplists:get_value(clean_session, Opts, true),
    LWTopic = proplists:get_value(last_will_topic, Opts, undefined),
    LWMsg = proplists:get_value(last_will_msg, Opts, undefined),
    LWQos = proplists:get_value(last_will_qos, Opts, 0),
    ReconnectTimeout = proplists:get_value(reconnect_timeout, Opts, undefined),
    KeepAliveInterval = proplists:get_value(keepalive_interval, Opts, 60),
    RetryInterval = proplists:get_value(retry_interval, Opts, 10),
    ProtoVer = proplists:get_value(proto_version, Opts, ?MQTT_PROTO_MAJOR),
    InfoFun = proplists:get_value(info_fun, Opts, {fun(_,_) -> ok end, []}),

    {Transport, TransportOpts} = proplists:get_value(transport, Opts, {gen_tcp, []}),

    State = #state{host = Host, port = Port, proto_version=ProtoVer,
        username = Username, password = Password, client=ClientId, mod=Mod,
        clean_session=CleanSession, last_will_topic=LWTopic, last_will_msg=LWMsg, last_will_qos=LWQos,
        reconnect_timeout=case ReconnectTimeout of undefined -> undefined; _ -> 1000 * ReconnectTimeout end,
        keepalive_interval=1000 * KeepAliveInterval,
        retry_interval=1000* RetryInterval, transport={Transport, TransportOpts},
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
    wrap_res(connecting, on_disconnect, [], State#state{sock=undefined});
handle_info({tcp_closed, Sock}, _, State=#state{sock=Sock, reconnect_timeout=undefined}) ->
    {stop, normal, State};
handle_info({tcp_closed, Sock}, _, State=#state{sock=Sock, reconnect_timeout=Timeout}) ->
    gen_fsm:send_event_after(Timeout, connect),
    wrap_res(connecting, on_disconnect, [], State#state{sock=undefined});

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

handle_frame(waiting_for_connack, #mqtt_connack{return_code=ReturnCode}, State) ->
    #state{waiting_acks=WAcks, keepalive_interval=Int, client=ClientId, info_fun=InfoFun} = State,
    case ReturnCode of
        ?CONNACK_ACCEPT when Int == 0 ->
            NewInfoFun = call_info_fun({connack_in, ClientId}, InfoFun),
            wrap_res(connected, on_connect, [], State#state{info_fun=NewInfoFun});
        ?CONNACK_ACCEPT ->
            NewInfoFun = call_info_fun({connack_in, ClientId}, InfoFun),
            Ref = gen_fsm:send_event_after(Int, ping),
            wrap_res(connected, on_connect, [], State#state{waiting_acks=store(ping, Ref, WAcks), info_fun=NewInfoFun});
        ?CONNACK_PROTO_VER ->
            gen_fsm:send_event_after(0, {error, {connect_error, ?CONNACK_PROTO_VER}}),
            wrap_res(disconnecting, on_connect_error, [wrong_protocol_version], State);
        ?CONNACK_INVALID_ID ->
            gen_fsm:send_event_after(0, {error, {connect_error, ?CONNACK_INVALID_ID}}),
            wrap_res(disconnecting, on_connect_error, [invalid_id], State);
        ?CONNACK_SERVER ->
            gen_fsm:send_event_after(0, {error, {connect_error, ?CONNACK_SERVER}}),
            wrap_res(disconnecting, on_connect_error, [server_not_available], State);
        ?CONNACK_CREDENTIALS ->
            gen_fsm:send_event_after(0, {error, {connect_error, ?CONNACK_CREDENTIALS}}),
            wrap_res(disconnecting, on_connect_error, [invalid_credentials], State);
        ?CONNACK_AUTH ->
            gen_fsm:send_event_after(0, {error, {connect_error, ?CONNACK_AUTH}}),
            wrap_res(disconnecting, on_connect_error, [not_authorized], State)
    end;

handle_frame(connected, #mqtt_suback{message_id=MsgId, qos_table=QoSTable}, State) ->
    #state{waiting_acks=WAcks, info_fun=InfoFun} = State,
    Key = {subscribe, MsgId},
    case dict:find(Key, WAcks) of
        {ok, {Ref, {subscribe, Topics}}} ->
            gen_fsm:cancel_timer(Ref),
            NewInfoFun = call_info_fun({suback, MsgId}, InfoFun),
            {TopicNames, _} = lists:unzip(Topics),
            case length(TopicNames) == length(QoSTable) of
                true ->
                    wrap_res(connected, on_subscribe, [lists:zip(TopicNames, QoSTable)],
                        State#state{waiting_acks=erase(Key, WAcks),
                            info_fun=NewInfoFun});
                false ->
                    wrap_res(connected, on_subscribe, [{error, Topics, QoSTable}],
                        State#state{waiting_acks=erase(Key, WAcks),
                            info_fun=NewInfoFun})
            end;
        error ->
            {next_state, connected, State}
    end;

handle_frame(connected, #mqtt_unsuback{message_id=MsgId}, State) ->
    #state{waiting_acks=WAcks, info_fun=InfoFun} = State,
    Key = {unsubscribe, MsgId},
    case dict:find(Key, WAcks) of
        {ok, {Ref, {unsubscribe, Topics}}} ->
            NewInfoFun = call_info_fun({unsuback, MsgId}, InfoFun),
            gen_fsm:cancel_timer(Ref),
            wrap_res(connected, on_unsubscribe, [Topics],
                State#state{waiting_acks=erase(Key, WAcks),
                    info_fun=NewInfoFun});
        error ->
            {next_state, connected, State}
    end;

handle_frame(connected, #mqtt_puback{message_id=MessageId}, State) ->
    #state{waiting_acks=WAcks, info_fun=InfoFun} = State,
    %% qos1 flow
    Key = {publish, MessageId},
    case dict:find(Key, WAcks) of
        {ok, {Ref, _}} ->
            NewInfoFun = call_info_fun({puback_in, MessageId}, InfoFun),
            gen_fsm:cancel_timer(Ref),
            {next_state, connected, State#state{waiting_acks=erase(Key, WAcks),
                info_fun=NewInfoFun}};
        error ->
            {next_state, connected, State}
    end;

handle_frame(connected, #mqtt_pubrec{message_id=MessageId}, State) ->
    #state{transport={Transport, _}, waiting_acks=WAcks, sock=Socket,
        retry_interval=RetryInterval, info_fun=InfoFun} = State,
    %% qos2 flow
    Key = {publish, MessageId},
    %% is retried
    case dict:find(Key, WAcks) of
        {ok, {Ref, _}} ->
            NewInfoFun0 = call_info_fun({pubrec_in, MessageId}, InfoFun),
            gen_fsm:cancel_timer(Ref), % cancel republish timer
            PubRelFrame = #mqtt_pubrel{message_id=MessageId},
            NewInfoFun1 = call_info_fun({pubrel_out, MessageId}, NewInfoFun0),
            send_frame(Transport, Socket, PubRelFrame),
            NewWAcks = erase(Key, WAcks),
            NewKey = {pubrel, MessageId},
            NewRef = gen_fsm:send_event_after(RetryInterval, {retry, NewKey}),
            {next_state, connected, State#state{
                waiting_acks=store(NewKey, {NewRef, PubRelFrame}, NewWAcks),
                info_fun=NewInfoFun1}};
        error ->
            {next_state, connected, State}
    end;

handle_frame(connected, #mqtt_pubrel{message_id=MessageId}, State) ->
    #state{transport={Transport,_}, waiting_acks=WAcks, sock=Socket, info_fun=InfoFun} = State,
    %% qos2 flow
    case dict:find({store, MessageId}, WAcks) of
        {ok, {Topic, Payload}} ->
            NewInfoFun0 = call_info_fun({pubrel_in, MessageId}, InfoFun),
            {next_state, connected, NewState} = wrap_res(connected, on_publish, [Topic, Payload], State),
            NewInfoFun1 = call_info_fun({pubcomp_out, MessageId}, NewInfoFun0),
            PubCompFrame = #mqtt_pubcomp{message_id=MessageId},
            send_frame(Transport, Socket, PubCompFrame),
            {next_state, connected, NewState#state{
                waiting_acks=erase({store, MessageId}, WAcks),
                info_fun=NewInfoFun1}};
        error ->
            {next_state, connected, State}
    end;

handle_frame(connected, #mqtt_pubcomp{message_id=MessageId}, State) ->
    #state{waiting_acks=WAcks, info_fun=InfoFun} = State,
    %% qos2 flow
    Key = {pubrel, MessageId},
    case dict:find(Key, WAcks) of
        {ok, {Ref, _}} ->
            NewInfoFun = call_info_fun({pubcomp_in, MessageId}, InfoFun),
            gen_fsm:cancel_timer(Ref), % cancel republish timer
            {next_state, connected, State#state{waiting_acks=erase(Key, WAcks),
                info_fun=NewInfoFun}};
        error ->
            {next_state, connected, State}
    end;

handle_frame(connected, #mqtt_publish{message_id=MessageId, topic=Topic,
    qos=QoS, payload=Payload}, State) ->
    #state{transport={Transport, _}, sock=Socket, waiting_acks=WAcks, info_fun=InfoFun} = State,
    NewInfoFun = call_info_fun({publish_in, MessageId, Payload, QoS}, InfoFun),
    case QoS of
        0 ->
            wrap_res(connected, on_publish, [Topic, Payload], State#state{info_fun=NewInfoFun});
        1 ->
            PubAckFrame = #mqtt_puback{message_id=MessageId},
            NewInfoFun1 = call_info_fun({puback_out, MessageId}, NewInfoFun),
            Res = wrap_res(connected, on_publish, [Topic, Payload], State#state{info_fun=NewInfoFun1}),
            send_frame(Transport, Socket, PubAckFrame),
            Res;
        2 ->
            PubRecFrame = #mqtt_pubrec{message_id=MessageId},
            NewInfoFun1 = call_info_fun({pubrec_out, MessageId}, NewInfoFun),
            send_frame(Transport, Socket, PubRecFrame),
            {next_state, connected, State#state{waiting_acks=store({store, MessageId}, {Topic, Payload}, WAcks),
                info_fun=NewInfoFun1}}
    end;

handle_frame(connected, #mqtt_pingresp{}, State) ->
    {next_state, connected, State};

handle_frame(connected, #mqtt_disconnect{}, State) ->
    gen_fsm:send_event_after(5000, connect),
    wrap_res(connecting, on_disconnect, [], State).

handle_event(Event, StateName, State) ->
    wrap_res(StateName, handle_cast, [Event], State).

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
        client_id = ClientId,
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

send_publish(MsgId, Topic, Payload, QoS, Retain, Dup, State) ->
    #state{transport={Transport, _}, sock = Sock, waiting_acks=WAcks,
        retry_interval=RetryInterval, info_fun=InfoFun} = State,
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
            Msg = {publish, MsgId, Topic, Payload, QoS, Retain, true},
            Key = {publish, MsgId},
            Ref = gen_fsm:send_event_after(RetryInterval, {retry, Key}),
            State#state{waiting_acks=store(Key, {Ref, Msg}, WAcks),
                info_fun=NewInfoFun}
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


store(K,V,D) ->
    dict:store(K,V,D).

erase(K,D) ->
    dict:erase(K,D).

'++'(65535) -> 1;
'++'(N) -> N + 1.

active_once(gen_tcp, Sock) ->
    ok = inet:setopts(Sock, [{active, once}]);
active_once(ssl, Sock) ->
    ok = ssl:setopts(Sock, [{active, once}]).

call_info_fun(Info, {Fun, FunState}) ->
    {Fun, Fun(Info, FunState)}.
