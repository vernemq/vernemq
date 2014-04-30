-module(emqttd_handler_fsm).
-behaviour(gen_fsm).
-behaviour(ranch_protocol).
-include_lib("emqtt_commons/include/emqtt_frame.hrl").

-export([start_link/4,
         deliver/5,
         disconnect/1]).

-export([init/1,
         handle_sync_event/4,
         handle_event/3,
         handle_info/3,
         terminate/3,
         code_change/4
        ]).

-export([wait_for_connect/2,
        connection_attempted/2,
        connected/2,
        connected/3]).



-define(CLOSE_AFTER, 5000).

-record(state, {

                parser_state=emqtt_frame:initial_state(),
                peer,
                client_id,
                username,
                socket,
                transport,
                auth_providers,
                next_msg_id=1,
                connection_attempted=false,
                will_topic,
                will_msg,
                will_qos,
                waiting_acks=dict:new(),
                buffer= <<>>}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% API FUNCTIONS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_link(Ref, Socket, Transport, Opts) ->
    proc_lib:start_link(?MODULE, init, [[Ref, Socket, Transport, Opts]]).

deliver(FsmPid, Topic, Payload, QoS, IsRetained) ->
    gen_fsm:send_event(FsmPid, {deliver, Topic, Payload, QoS, IsRetained}).

disconnect(FsmPid) ->
    gen_fsm:sync_send_event(FsmPid, disconnect).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% STATE FUNCTIONS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
wait_for_connect(timeout, State) ->
    io:format("[~p] stop due to timeout~n", [self()]),
    {stop, normal, State}.

connection_attempted(timeout, State) ->
    {next_state, wait_for_connect, State, ?CLOSE_AFTER}.

connected(timeout, State) ->
    {next_state, connected, State};

connected({deliver, Topic, Payload, QoS, _IsRetained}, State) ->
    #state{transport=Transport, socket=Socket, waiting_acks=WAcks} = State,
    {OutgoingMsgId, State1} = get_msg_id(QoS, State),
    Frame = #mqtt_frame{
               fixed=#mqtt_frame_fixed{
                        type=?PUBLISH,
                        qos=QoS
                       },
               variable=#mqtt_frame_publish{
                           topic_name=Topic,
                           message_id=OutgoingMsgId},
               payload=Payload
              },
    case send_publish_frame(Transport, Socket, OutgoingMsgId, Frame, QoS, State1) of
        {error, Reason} ->
            io:format("[~p] stop due to ~p~n", [self(), Reason]),
            {stop, normal, State};
        NewState when QoS == 0 ->
            % io:format("[~p] in deliver ~p ~p~n", [self(), Topic, byte_size(Payload)]),
            {next_state, connected, NewState};
        NewState when QoS > 0 ->
            % io:format("[~p] in deliver ~p ~p~n", [self(), Topic, byte_size(Payload)]),
            Ref = gen_fsm:send_event_after(10000, {retry, OutgoingMsgId}),
            {next_state, connected, NewState#state{waiting_acks=dict:store(OutgoingMsgId, {Frame, Ref}, WAcks)}}
    end;

connected({retry, MessageId}, State) ->
    #state{transport=Transport, socket=Socket, waiting_acks=WAcks} = State,
    Bin =
    case dict:fetch(MessageId, WAcks) of
        {#mqtt_frame{fixed=Fixed} = Frame, _} ->
            emqtt_frame:serialise(Frame#mqtt_frame{fixed=Fixed#mqtt_frame_fixed{dup=true}});
        {BinFrame, _} -> BinFrame
    end,
    io:format("[~p] send_bin ~p ~p~n", [self(), retry, byte_size(Bin)]),
    send_bin(Transport, Socket, Bin),
    Ref = gen_fsm:send_event_after(10000, {retry, MessageId}),
    {next_state, connected, State#state{
                              waiting_acks=dict:store(MessageId, {Bin, Ref}, WAcks)}}.

connected(disconnect, _From, State) ->
    io:format("[~p] stop due to ~p~n", [self(), disconnect]),
    {stop, normal, ok, State}.





%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% NETWORK HANDLER FUNCTIONS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
handle_frame(wait_for_connect, _, #mqtt_frame_connect{} = Var, _, State) ->
    #state{peer=Peer, auth_providers=AuthProviders} = State,
    #mqtt_frame_connect{
       client_id=Id,
       username=User,
       password=Password,
       proto_ver=Version,
       clean_sess=CleanSession,
       will_qos=WillQoS,
       will_topic=WillTopic,
       will_msg=WillMsg} = Var,

    case check_version(Version) of
        true ->
            case auth_user(Peer, Id, User, Password, AuthProviders) of
                ok ->
                    case emqttd_trie:register_client(Id, CleanSession) of
                        ok ->
                            {next_state, connected,
                             send_connack(?CONNACK_ACCEPT, State#state{
                                                             client_id=Id,
                                                             username=User,
                                                             will_qos=WillQoS,
                                                             will_topic=WillTopic,
                                                             will_msg=WillMsg})};
                        {error, _Reason} ->
                            {next_state, connection_attempted, send_connack(?CONNACK_SERVER, State)}
                    end;
                {error, unknown} ->
                    {next_state, connection_attempted, send_connack(?CONNACK_INVALID_ID, State)};
                {error, invalid_credentials} ->
                    {next_state, connection_attempted, send_connack(?CONNACK_CREDENTIALS, State)};
                {error, not_authorized} ->
                    {next_state, connection_attempted, send_connack(?CONNACK_AUTH, State)}
            end;
        false ->
            {next_state, connection_attempted, send_connack(?CONNACK_PROTO_VER, State)}
    end;

handle_frame(connected, #mqtt_frame_fixed{type=?PUBACK}, Var, _, State) ->
    #state{waiting_acks=WAcks} = State,
    #mqtt_frame_publish{message_id=MessageId} = Var,
    %% qos1 flow
    % io:format("got PUBACK ~p ~p~n", [MessageId, now()]),
    %io:format("lists keyfind ~p in ~p~n", [MessageId, WAcks]),
    case dict:find(MessageId, WAcks) of
        {ok, {_, Ref}} ->
            gen_fsm:cancel_timer(Ref),
            {next_state, connected, State#state{waiting_acks=dict:erase(MessageId, WAcks)}};
        error ->
            {next_state, connected, State}
    end;

handle_frame(connected, #mqtt_frame_fixed{type=?PUBREC}, Var, _, State) ->
    #state{waiting_acks=WAcks, transport=Transport, socket=Socket} = State,
    #mqtt_frame_publish{message_id=MessageId} = Var,
    %% qos2 flow
    {_, Ref} = dict:fetch(MessageId, WAcks),
    gen_fsm:cancel_timer(Ref), % cancel republish timer
    PubRelFrame =#mqtt_frame{
                             fixed=#mqtt_frame_fixed{type=?PUBREL, qos=1},
                             variable=#mqtt_frame_publish{message_id=MessageId},
                             payload= <<>>},
    Bin = emqtt_frame:serialise(PubRelFrame),
    io:format("[~p] send_bin ~p ~p~n", [self(), pubrel, byte_size(Bin)]),
    send_bin(Transport, Socket, Bin),
    NewRef = gen_fsm:send_event_after(10000, {retry, MessageId}),
    {next_state, connected, State#state{
                              waiting_acks=dict:store(MessageId, {PubRelFrame, NewRef}, WAcks)}};

handle_frame(connected, #mqtt_frame_fixed{type=?PUBREL}, Var, _, State) ->
    #state{client_id=ClientId} = State,
    #mqtt_frame_publish{message_id=MessageId} = Var,
    %% qos2 flow
    ok = emqttd_buffer:release_and_forward(ClientId, MessageId),
    NewState = send_frame(?PUBCOMP, #mqtt_frame_publish{message_id=MessageId}, <<>>, State),
    {next_state, connected, NewState};

handle_frame(connected, #mqtt_frame_fixed{type=?PUBCOMP}, Var, _, State) ->
    #state{waiting_acks=WAcks} = State,
    #mqtt_frame_publish{message_id=MessageId} = Var,
    %% qos2 flow
    {_, Ref} = dict:fetch(MessageId, WAcks),
    gen_fsm:cancel_timer(Ref), % cancel rpubrel timer
    {next_state, connected, State#state{waiting_acks=dict:erase(MessageId, WAcks)}};

handle_frame(connected, #mqtt_frame_fixed{type=?PUBLISH, retain=1}, Var, <<>>, State) ->
    #mqtt_frame_publish{topic_name=Topic, message_id=MessageId} = Var,
    %% delete retained msg,
    case emqttd_trie:publish(Topic, <<>>, true) of
        ok ->
            NewState = send_frame(?PUBACK, #mqtt_frame_publish{message_id=MessageId}, <<>>, State),
            {next_state, connected, NewState};
        {error, _Reason} ->
            %% we can't delete the retained message, due to a network split,
            %% if the client uses QoS 1 it will retry this message.
            {next_state, connected, State}
    end;

handle_frame(connected, #mqtt_frame_fixed{type=?PUBLISH, qos=QoS, retain=IsRetain}, Var, Payload, State) ->
    #mqtt_frame_publish{topic_name=Topic, message_id=MessageId} = Var,
    % io:format("got PUBLISH ~p ~p~n", [MessageId, now()]),
    {next_state, connected, dispatch_publish(QoS, MessageId, Topic, Payload, IsRetain, State)};

handle_frame(connected, #mqtt_frame_fixed{type=?SUBSCRIBE}, Var, _, State) ->
    #mqtt_frame_subscribe{topic_table=Topics, message_id=MessageId} = Var,
    TTopics = [{Name, QoS} || #mqtt_topic{name=Name, qos=QoS} <- Topics],
    case emqttd_trie:subscribe(State#state.client_id, TTopics) of
        ok ->
            {_, QoSs} = lists:unzip(TTopics),
            NewState = send_frame(?SUBACK, #mqtt_frame_suback{message_id=MessageId, qos_table=QoSs}, <<>>, State),
            {next_state, connected, NewState};
        {error, _Reason} ->
            %% cant subscribe due to netsplit, Subscribe uses QoS 1 so the client will retry
            {next_state, connected, State}
    end;

handle_frame(connected, #mqtt_frame_fixed{type=?UNSUBSCRIBE}, Var, _, State) ->
    #mqtt_frame_subscribe{topic_table=Topics, message_id=MessageId} = Var,
    TTopics = [Name || #mqtt_topic{name=Name} <- Topics],
    case emqttd_trie:unsubscribe(State#state.client_id, TTopics) of
        ok ->
            NewState = send_frame(?UNSUBACK, #mqtt_frame_suback{message_id=MessageId}, <<>>, State),
            {next_state, connected, NewState};
        {error, _Reason} ->
            %% cant unsubscribe due to netsplit, Unsubscribe uses QoS 1 so the client will retry
            {next_state, connected, State}
    end;

handle_frame(connected, #mqtt_frame_fixed{type=?PINGREQ}, _, _, State) ->
    NewState = send_frame(?PINGRESP, undefined, <<>>, State),
    {next_state, connected, NewState};

handle_frame(connected, #mqtt_frame_fixed{type=?DISCONNECT}, _, _, State) ->
    {stop, normal, State}.

handle_closed(_StateName, State) ->
    io:format("[~p] stop due to ~p~n", [self(), tcp_closed]),
    {stop, normal, State}.

handle_error(_StateName, Reason, State) ->
    io:format("[~p] stop due to ~p~n", [self(), Reason]),
    {stop, Reason, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% GEN_FSM CALLBACKS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init([Ref, Socket, Transport, Opts]) ->
    ok = proc_lib:init_ack({ok, self()}),
    ok = ranch:accept_ack(Ref),
    ok = Transport:setopts(Socket, [{nodelay, true},
                                    {packet, raw},
                                    {active, once}]),
    {ok, Peer} = Transport:peername(Socket),
    {_, AuthProviders} = lists:keyfind(auth_providers, 1, Opts),
    gen_fsm:enter_loop(?MODULE, [], wait_for_connect,
                       #state{socket=Socket, transport=Transport, peer=Peer,
                              auth_providers=AuthProviders}, ?CLOSE_AFTER).

handle_sync_event(_Event, _From, StateName, State) ->
    {reply, ok, StateName, State}.

handle_event({unhandled_transport_error, Reason}, _StateName, State) ->
    io:format("[~p] stop due to ~p~n", [self(), Reason]),
    {stop, normal, State};
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_info({tcp, _Socket, Data}, StateName, State) ->
    handle_input(Data, StateName, State);
handle_info({ssl, _Socket, Data}, StateName, State) ->
    handle_input(Data, StateName, State);
handle_info({tcp_closed, _Socket}, StateName, State) ->
    handle_closed(StateName, State);
handle_info({ssl_closed, _Socket}, StateName, State) ->
    handle_closed(StateName, State);
handle_info({tcp_error, _Socket, Reason}, StateName, State) ->
    handle_error(StateName, Reason, State);
handle_info({ssl_error, _Socket, Reason}, StateName, State) ->
    handle_error(StateName, Reason, State);
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.


terminate(_Reason, _StateName, State) ->
    #state{peer=_Peer, socket=Socket, transport=Transport} = State,
    Transport:close(Socket),
    maybe_publish_last_will(State),
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% INTERNAL
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
handle_input(Input, StateName, State) ->
    #state{transport=Transport, socket=Socket, buffer=Buffer} = State,
    Transport:setopts(Socket, [{active, once}]),
    process_bytes(<<Buffer/binary, Input/binary>>, StateName, State).

process_bytes(Bytes, StateName, #state{parser_state=ParserState} = State) ->
    case emqtt_frame:parse(Bytes, ParserState) of
        {more, NewParserState} ->
            {next_state, StateName, State#state{parser_state=NewParserState}};
        {ok, #mqtt_frame{fixed=Fixed, variable=Variable, payload=Payload}, Rest} ->
            {next_state, NextStateName, NewState}
            = handle_frame(StateName, Fixed, Variable, Payload, State),
            PS = emqtt_frame:initial_state(),
            process_bytes(Rest, NextStateName, NewState#state{parser_state=PS});
        {error, Reason} ->
            io:format("parse error ~p~n", [Reason]),
            {next_state, StateName, State#state{buffer= <<>>,
                                                parser_state=emqtt_frame:initial_state()}}
    end.

check_version(?MQTT_PROTO_MAJOR) -> true;
check_version(_) -> false.

auth_user(_, _, _, _, []) -> {error, unknown};
auth_user(SrcIp, Id, User, Password, [AuthProvider|AuthProviders]) ->
    case apply(AuthProvider, authenticate, [SrcIp, Id, User, Password]) of
        ok ->
            ok;
        {error, unknown} ->
            % loop through Auth Providers
            auth_user(SrcIp, Id, User, Password, AuthProviders);
        {error, Reason} ->
            {error, Reason}
    end.

send_connack(ReturnCode, State) ->
    send_frame(?CONNACK, #mqtt_frame_connack{return_code=ReturnCode}, <<>>, State).

send_frame(Type, Variable, Payload, State) ->
    send_frame(Type, false, Variable, Payload, State).

send_frame(Type, DUP, Variable, Payload, #state{socket=Socket, transport=Transport} = State) ->
    Bin = emqtt_frame:serialise(#mqtt_frame{
                             fixed=#mqtt_frame_fixed{type=Type, dup=DUP},
                             variable=Variable,
                             payload=Payload
                            }),
    % io:format("send ~p~n", [Bin]),
    % io:format("[~p] senda_bin ~p ~p~n", [self(), Type, byte_size(Bin)]),
    send_bin(Transport, Socket, Bin),
    State.


maybe_publish_last_will(#state{will_topic=undefined} = State) -> State;
maybe_publish_last_will(#state{will_qos=QoS, will_topic=Topic,
                               will_msg=Msg } = State) ->
    {MsgId, NewState} = get_msg_id(QoS, State),
    dispatch_publish(QoS, MsgId, Topic, Msg, false, NewState).


dispatch_publish(0, MessageId, Topic, Payload, IsRetain, State) ->
    dispatch_publish_qos0(MessageId, Topic, Payload, IsRetain, State);
dispatch_publish(1, MessageId, Topic, Payload, IsRetain, State) ->
    dispatch_publish_qos1(MessageId, Topic, Payload, IsRetain, State);
dispatch_publish(2, MessageId, Topic, Payload, IsRetain, State) ->
    dispatch_publish_qos2(MessageId, Topic, Payload, IsRetain, State).

dispatch_publish_qos0(_MessageId, Topic, Payload, IsRetain, State) ->
    %io:format("dispatch0  ~p ~p ~p~n", [_MessageId, Topic, self()]),
    emqttd_trie:publish(Topic, Payload, IsRetain),
    State.

dispatch_publish_qos1(MessageId, Topic, Payload, IsRetain, State) ->
    %io:format("dispatch1  ~p ~p ~p~n", [MessageId, Topic, self()]),
    ok = emqttd_buffer:store_and_forward(State#state.client_id, Topic, Payload, IsRetain),
    send_frame(?PUBACK, #mqtt_frame_publish{message_id=MessageId}, <<>>, State).

dispatch_publish_qos2(MessageId, Topic, Payload, IsRetain, State) ->
    ok = emqttd_buffer:store(State#state.client_id, MessageId, Topic, Payload, IsRetain),
    send_frame(?PUBREC, #mqtt_frame_publish{message_id=MessageId}, <<>>, State).

get_msg_id(0, State) ->
    {undefined, State};
get_msg_id(_, #state{next_msg_id=65535} = State) ->
    {1, State#state{next_msg_id=2}};
get_msg_id(_, #state{next_msg_id=MsgId} = State) ->
    {MsgId, State#state{next_msg_id=MsgId + 1}}.

send_publish_frame(Transport, Socket, _OutgoingMsgId, Frame, QoS, State) ->
    Bin = emqtt_frame:serialise(Frame),
    % io:format("[~p] send_pub ~p~n", [self(), byte_size(Bin)]),
    case Transport:send(Socket, Bin) of
        ok ->
            State;
        {error, Reason} when QoS > 0 ->
            %% we cant send, process will die, store msg so we can retry when client
            %% reconnects.
            #mqtt_frame{variable=#mqtt_frame_publish{topic_name=Topic},
                        payload=Payload} = Frame,
            %% save to call even in a split brain situation
            emqttd_msg_store:persist_for_later([{State#state.client_id, QoS}], Topic, Payload),
            {error, Reason};
        {error, Reason} ->
            {error, Reason}
    end.


send_bin(Transport, Socket, Bin) ->
    case Transport:send(Socket, Bin) of
        ok ->
            ok;
        {error, Reason} ->
            gen_fsm:send_all_state_event(self(), {unhandled_transport_error, Reason})
    end.
