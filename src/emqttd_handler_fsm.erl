-module(emqttd_handler_fsm).
-behaviour(gen_fsm).
-behaviour(ranch_protocol).
-include("emqtt_frame.hrl").

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
                waiting_acks=[],
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
    {stop, normal, State}.

connection_attempted(timeout, State) ->
    {next_state, wait_for_connect, State, ?CLOSE_AFTER}.

connected(timeout, State) ->
    {next_state, connected, State};

connected({deliver, Topic, Payload, QoS, _IsRetained}, State) ->
    #state{transport=Transport, socket=Socket} = State,
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
    io:format("[~p] in deliver ~p~n", [self(), Frame]),
    case send_publish_frame(Transport, Socket, OutgoingMsgId, Frame, QoS, State1) of
        {error, _Reason} ->
            {stop, normal, State};
        NewState ->
            {next_state, connected, NewState}
    end;

connected({retry, MessageId}, State) ->
    #state{transport=Transport, socket=Socket, waiting_acks=WAcks} = State,
    Bin =
    case lists:keyfind(MessageId, 1, WAcks) of
        {_, #mqtt_frame{fixed=Fixed} = Frame, _} ->
            emqtt_frame:serialise(Frame#mqtt_frame{fixed=Fixed#mqtt_frame_fixed{dup=true}});
        {_, BinFrame, _} -> BinFrame
    end,
    send_bin(Transport, Socket, Bin),
    Ref = gen_fsm:send_event_after(10000, {retry, MessageId}),
    {next_state, connected, State#state{
                              waiting_acks=lists:keyreplace(MessageId, 1, WAcks,
                                                            {MessageId, Bin, Ref})}}.

connected(disconnect, _From, State) ->
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
                    emqttd_trie:register_client(Id, CleanSession),
                    {next_state, connected,
                        send_connack(?CONNACK_ACCEPT, State#state{
                                                    client_id=Id,
                                                    username=User,
                                                    will_qos=WillQoS,
                                                    will_topic=WillTopic,
                                                    will_msg=WillMsg})};
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
    {_, _, Ref} = lists:keyfind(MessageId, 1, WAcks),
    gen_fsm:cancel_timer(Ref),
    {next_state, connected, State#state{waiting_acks=lists:keydelete(MessageId, 1, WAcks)}};

handle_frame(connected, #mqtt_frame_fixed{type=?PUBREC}, Var, _, State) ->
    #state{waiting_acks=WAcks, transport=Transport, socket=Socket} = State,
    #mqtt_frame_publish{message_id=MessageId} = Var,
    %% qos2 flow
    {_, _, Ref} = lists:keyfind(MessageId, 1, WAcks),
    gen_fsm:cancel_timer(Ref), % cancel republish timer
    PubRelFrame =#mqtt_frame{
                             fixed=#mqtt_frame_fixed{type=?PUBREL},
                             variable=#mqtt_frame_publish{message_id=MessageId},
                             payload= <<>>},
    Bin = emqtt_frame:serialise(PubRelFrame),
    send_bin(Transport, Socket, Bin),
    NewRef = gen_fsm:send_event_after(10000, {retry, MessageId}),
    {next_state, connected, State#state{
                              waiting_acks=lists:keyreplace(MessageId, 1, WAcks,
                                                            {MessageId, PubRelFrame, NewRef})}};

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
    {_, _, Ref} = lists:keyfind(MessageId, 1, WAcks),
    gen_fsm:cancel_timer(Ref), % cancel rpubrel timer
    {next_state, connected, State#state{waiting_acks=lists:keydelete(MessageId, 1, WAcks)}};

handle_frame(connected, #mqtt_frame_fixed{type=?PUBLISH, retain=1}, Var, <<>>, State) ->
    #mqtt_frame_publish{topic_name=Topic, message_id=MessageId} = Var,
    %% delete retained msg,
    emqttd_trie:publish(Topic, <<>>, true),
    NewState = send_frame(?PUBACK, #mqtt_frame_publish{message_id=MessageId}, <<>>, State),
    {next_state, connected, NewState};

handle_frame(connected, #mqtt_frame_fixed{type=?PUBLISH, qos=QoS, retain=IsRetain}, Var, Payload, State) ->
    #mqtt_frame_publish{topic_name=Topic, message_id=MessageId} = Var,
    {next_state, connected, dispatch_publish(QoS, MessageId, Topic, Payload, IsRetain, State)};

handle_frame(connected, #mqtt_frame_fixed{type=?SUBSCRIBE}, Var, _, State) ->
    #mqtt_frame_subscribe{topic_table=Topics, message_id=MessageId} = Var,
    QoSs = subscribe(State#state.client_id, Topics, []),
    NewState = send_frame(?SUBACK, #mqtt_frame_suback{message_id=MessageId, qos_table=QoSs}, <<>>, State),
    {next_state, connected, NewState};

handle_frame(connected, #mqtt_frame_fixed{type=?UNSUBSCRIBE}, Var, _, State) ->
    #mqtt_frame_subscribe{topic_table=Topics, message_id=MessageId} = Var,
    unsubscribe(State#state.client_id, Topics),
    NewState = send_frame(?UNSUBACK, #mqtt_frame_suback{message_id=MessageId}, <<>>, State),
    {next_state, connected, NewState};

handle_frame(connected, #mqtt_frame_fixed{type=?PINGREQ}, _, _, State) ->
    NewState = send_frame(?PINGRESP, undefined, <<>>, State),
    {next_state, connected, NewState};

handle_frame(connected, #mqtt_frame_fixed{type=?DISCONNECT}, _, _, State) ->
    {stop, normal, State}.

handle_closed(_StateName, State) ->
    {stop, normal, State}.

handle_error(_StateName, Reason, State) ->
    {stop, Reason, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% GEN_FSM CALLBACKS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init([Ref, Socket, Transport, Opts]) ->
    ok = proc_lib:init_ack({ok, self()}),
    ok = ranch:accept_ack(Ref),
    ok = Transport:setopts(Socket, [{active, once}]),
    {ok, Peer} = Transport:peername(Socket),
    {_, AuthProviders} = lists:keyfind(auth_providers, 1, Opts),
    gen_fsm:enter_loop(?MODULE, [], wait_for_connect,
                       #state{socket=Socket, transport=Transport, peer=Peer,
                              auth_providers=AuthProviders}, ?CLOSE_AFTER).

handle_sync_event(_Event, _From, StateName, State) ->
    {reply, ok, StateName, State}.

handle_event({unhandled_transport_error, _Reason}, _StateName, State) ->
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


terminate(Reason, _StateName, State) ->
    #state{peer=Peer, socket=Socket, transport=Transport} = State,
    io:format("~p terminated due to: ~p~n", [Peer, Reason]),
    Transport:close(Socket),
    maybe_publish_last_will(State),
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% INTERNAL
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
handle_input(Input, StateName, State) ->
    #state{transport=Transport, socket=Socket, buffer=Buffer, parser_state=ParserState} = State,
    Transport:setopts(Socket, [{active, once}]),
    case emqtt_frame:parse(<<Buffer/binary, Input/binary>>, ParserState) of
        {ok, #mqtt_frame{fixed=Fixed, variable=Variable, payload=Payload}, Rest} ->
            handle_frame(StateName, Fixed, Variable, Payload, State#state{buffer=Rest,
                                                       parser_state=emqtt_frame:initial_state()});
        {more, Cont} ->
            {next_state, StateName, State#state{buffer= <<>>,
                                                parser_state=Cont}};
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
    io:format("send ~p~n", [Bin]),
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

dispatch_publish_qos0(MessageId, Topic, Payload, IsRetain, State) ->
    io:format("dispatch direct ~p ~p ~p~n", [MessageId, Topic, Payload]),
    emqttd_trie:publish(Topic, Payload, IsRetain),
    State.

dispatch_publish_qos1(MessageId, Topic, Payload, IsRetain, State) ->
    ok = emqttd_buffer:store_and_forward(State#state.client_id, Topic, Payload, IsRetain),
    send_frame(?PUBACK, #mqtt_frame_publish{message_id=MessageId}, <<>>, State).

dispatch_publish_qos2(MessageId, Topic, Payload, IsRetain, State) ->
    ok = emqttd_buffer:store(State#state.client_id, MessageId, Topic, Payload, IsRetain),
    send_frame(?PUBREC, #mqtt_frame_publish{message_id=MessageId}, <<>>, State).

subscribe(_, [], Acc) -> lists:reverse(Acc);
subscribe(ClientId, [#mqtt_topic{name=Name, qos=QoS}|Rest], Acc) ->
    emqttd_trie:subscribe(ClientId, Name, QoS),
    subscribe(ClientId, Rest, [QoS|Acc]).

unsubscribe(_, []) -> ok;
unsubscribe(ClientId, [#mqtt_topic{name=Name}|Rest]) ->
    emqttd_trie:unsubscribe(ClientId, Name),
    unsubscribe(ClientId, Rest).

get_msg_id(0, State) ->
    {undefined, State};
get_msg_id(_, #state{next_msg_id=65535} = State) ->
    {1, State#state{next_msg_id=2}};
get_msg_id(_, #state{next_msg_id=MsgId} = State) ->
    {MsgId, State#state{next_msg_id=MsgId + 1}}.

send_publish_frame(Transport, Socket, OutgoingMsgId, Frame, QoS, State) ->
    Bin = emqtt_frame:serialise(Frame),
    case Transport:send(Socket, Bin) of
        ok when QoS > 0 ->
            %% if we won't get a PUBACK or PUBREC in time, we retry
            Ref = gen_fsm:send_event_after(10000, {retry, OutgoingMsgId}),
            %% ?TODO is it save to store the frame in the process state?
            State#state{waiting_acks=[{OutgoingMsgId, Frame, Ref}|State#state.waiting_acks]};
        ok ->
            State;
        {error, Reason} when QoS > 0 ->
            %% we cant send, process will die, store msg so we can retry when client
            %% reconnects.
            #mqtt_frame{variable=#mqtt_frame_publish{topic_name=Topic},
                        payload=Payload} = Frame,
            %% save to call even in a split brain situation
            emqttd_msg_store:persist_for_later([State#state.client_id], Topic, Payload),
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

