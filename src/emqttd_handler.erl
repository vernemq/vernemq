-module(emqttd_handler).
-include("emqtt_frame.hrl").
-behaviour(ranch_protocol).

-export([start_link/4]).
-export([init/4]).
-define(MAX_QUEUE_LEN, 100).
-define(CLOSE_AFTER, 5000).
-record(state, {connected=false,
                closed,
                parser_state=emqtt_frame:initial_state(),
                src_ip,
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

start_link(Ref, Socket, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
    {ok, Pid}.

init(Ref, Socket, Transport, Opts) ->
    ok = ranch:accept_ack(Ref),
    {_, AuthProviders} = lists:keyfind(auth_providers, 1, Opts),
    {ok, {Address, _Port}} = (i(Transport)):peername(Socket),
    erlang:send_after(?CLOSE_AFTER, self(), close_connection_if_not_attempted),
    loop(#state{socket=Socket, transport=Transport, src_ip=Address, auth_providers=AuthProviders}).

loop(#state{closed=true}) -> ok; %% DISCONNECTED BY CLIENT
loop(#state{socket=Socket, transport=Transport, buffer=Buffer, waiting_acks=WAcks} = State) ->
    case erlang:process_info(self(), message_queue_len) of
        {message_queue_len, Len} when Len < ?MAX_QUEUE_LEN ->
            (i(Transport)):setopts(Socket, [{active, once}]);
        {message_queue_len, _Len} ->
            ok
    end,
    receive
        {tcp, Socket, Input} ->
            loop(handle_input(<<Buffer/binary, Input/binary>>, State));
        {tcp_closed, Socket} ->
            maybe_publish_last_will(State);
        {tcp_error, Socket, _Reason} ->
            maybe_publish_last_will(State);
        % SSL
        {ssl, Socket, Input} ->
            loop(handle_input(<<Buffer/binary, Input/binary>>, State));
        {ssl_closed, Socket} ->
            maybe_publish_last_will(State);
        {ssl_error, Socket, _Reason} ->
            maybe_publish_last_will(State);
        close_connection_if_not_attempted when State#state.connection_attempted == true ->
            loop(State);
        disconnect ->
            %% we have to disconnect due to a newer registered client
            maybe_publish_last_will(State);
        {route, Topic, Payload, QoS} ->
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
            io:format("[~p] in route ~p~n", [self(), Frame]),
            Bin = emqtt_frame:serialise(Frame),
            ok = (t(Transport)):send(Socket, Bin), %% !!! PUBLISH
            case QoS > 0 of
                true ->
                    erlang:send_after(10000, self(), {retry, publish, OutgoingMsgId}),
                    loop(State1#state{waiting_acks=[{{publish, OutgoingMsgId}, Frame}|WAcks]});
                false ->
                    loop(State1)
            end;
        {route_retained, Topic, Payload, QoS} ->
            {OutgoingMsgId, State1} = get_msg_id(QoS, State),
            Frame = #mqtt_frame{
                       fixed=#mqtt_frame_fixed{
                                type=?PUBLISH,
                                qos=QoS,
                                retain=1
                               },
                       variable=#mqtt_frame_publish{
                                   topic_name=Topic,
                                   message_id=OutgoingMsgId},
                       payload=Payload
                      },
            io:format("[~p] in route retained ~p~n", [self(),Frame]),
            Bin = emqtt_frame:serialise(Frame),
            ok = (t(Transport)):send(Socket, Bin), %% !!! PUBLISH
            case QoS > 0 of
                true ->
                    erlang:send_after(10000, self(), {retry, publish, OutgoingMsgId}),
                    loop(State1#state{waiting_acks=[{{publish, OutgoingMsgId}, Frame}|WAcks]});
                false ->
                    loop(State1)
            end;

        {retry, publish, MessageId} ->
            case lists:keyfind({publish, MessageId}, 1, WAcks) of
                false ->
                    %% removed during handle PUBACK
                    loop(State);
                {_, #mqtt_frame{fixed=#mqtt_frame_fixed{qos=1}=Fixed} = Frame} ->
                    Bin = emqtt_frame:serialise(Frame#mqtt_frame{fixed=Fixed#mqtt_frame_fixed{dup=true}}),
                    ok = (t(Transport)):send(Socket, Bin),
                    erlang:send_after(10000, self(), {retry, publish, MessageId}),
                    loop(State)
            end;
        {retry, pubrel, MessageId} ->
            case lists:keyfind({pubrel, MessageId}, 1, WAcks) of
                false ->
                    %% removed during handle PUBREL
                    loop(State);
                {_, ok} ->
                    send_frame(?PUBREL, true, #mqtt_frame_publish{message_id=MessageId}, <<>>, State),
                    erlang:send_after(10000, self(), {retry, pubrel, MessageId}),
                    loop(State)
            end;
        _ ->
            % this will close the connection
            maybe_publish_last_will(State)
    end.

handle_input(Input, #state{parser_state=ParserState} = State) ->
    case emqtt_frame:parse(Input, ParserState) of
        {ok, #mqtt_frame{fixed=Fixed, variable=VariablePart, payload=Payload}, Rest} ->
            NewState = handle_frame(VariablePart, Fixed, Payload, State),
            NewState#state{buffer=Rest, parser_state=emqtt_frame:initial_state()};
        {more, Cont} ->
            Cont;
        {error, Reason} ->
            io:format("parse error ~p~n", [Reason]),
            State#state{buffer= <<>>, parser_state=emqtt_frame:initial_state()}
    end.

handle_frame(#mqtt_frame_connect{client_id=Id, username=User, password=Password, proto_ver=Version, clean_sess=CleanSession, will_qos=WillQoS, will_topic=WillTopic, will_msg=WillMsg}, _, _, #state{connected=false, src_ip=Src, auth_providers=AuthProviders} = State) ->
    State1 =
    case check_version(Version) of
        true ->
            case auth_user(Src, Id, User, Password, AuthProviders) of
                ok ->
                    emqttd_connection_reg:register_client(Id, CleanSession),
                    send_connack(?CONNACK_ACCEPT, State#state{
                                                    client_id=Id,
                                                    username=User,
                                                    will_qos=WillQoS,
                                                    will_topic=WillTopic,
                                                    will_msg=WillMsg,
                                                    connected=true});
                {error, unknown} ->
                    send_connack(?CONNACK_INVALID_ID, State);
                {error, invalid_credentials} ->
                    send_connack(?CONNACK_CREDENTIALS, State);
                {error, not_authorized} ->
                    send_connack(?CONNACK_AUTH, State)
            end;
        false ->
            send_connack(?CONNACK_PROTO_VER, State)
    end,
    State1#state{connection_attempted=true};

handle_frame(#mqtt_frame_publish{message_id=MessageId}, #mqtt_frame_fixed{type=?PUBACK}, _, State) ->
    %% qos1 flow
    #state{waiting_acks=WAcks} = State,
    State#state{waiting_acks=lists:keydelete({publish, MessageId}, 1, WAcks)};
handle_frame(#mqtt_frame_publish{message_id=MessageId}, #mqtt_frame_fixed{type=?PUBREC}, _, State) ->
    %% qos2 flow
    #state{waiting_acks=WAcks} = State,
    send_frame(?PUBREL, #mqtt_frame_publish{message_id=MessageId}, <<>>, State),
    erlang:send_after(10000, self(), {retry, pubrel, MessageId}),
    State#state{waiting_acks=[{{pubrel, MessageId}, ok}|lists:keydelete({publish, MessageId}, 1, WAcks)]};
handle_frame(#mqtt_frame_publish{message_id=MessageId}, #mqtt_frame_fixed{type=?PUBREL}, _, State) ->
    %% qos2 flow
    #state{client_id=ClientId, waiting_acks=WAcks} = State,
    ok = emqttd_buffer:release_and_forward(ClientId, MessageId),
    send_frame(?PUBCOMP, #mqtt_frame_publish{message_id=MessageId}, <<>>, State),
    State#state{waiting_acks=lists:keydelete({pubrel, MessageId}, 1, WAcks)};
handle_frame(#mqtt_frame_publish{message_id=MessageId}, #mqtt_frame_fixed{type=?PUBCOMP}, _, State) ->
    %% qos2 flow
    #state{waiting_acks=WAcks} = State,
    State#state{waiting_acks=lists:keydelete({pubrel, MessageId}, 1, WAcks)};
handle_frame(#mqtt_frame_publish{topic_name=Topic}, #mqtt_frame_fixed{retain=1}, <<>>, State) ->
    %% delete retained msg
    emqttd_msg_store:reset_retained_msg(Topic),
    State;
handle_frame(#mqtt_frame_publish{topic_name=Topic, message_id=MessageId}, Fixed, Payload, State) ->
    #mqtt_frame_fixed{qos=QoS, retain=IsRetain} = Fixed,
    dispatch_publish(QoS, MessageId, Topic, Payload, IsRetain, State);


handle_frame(#mqtt_frame_subscribe{topic_table=Topics, message_id=MessageId},
             #mqtt_frame_fixed{type=?SUBSCRIBE}, _, State) ->
    QoSs = subscribe(State#state.client_id, Topics, []),
    send_frame(?SUBACK, #mqtt_frame_suback{message_id=MessageId, qos_table=QoSs}, <<>>, State);

handle_frame(#mqtt_frame_subscribe{topic_table=Topics, message_id=MessageId},
             #mqtt_frame_fixed{type=?UNSUBSCRIBE}, _, State) ->
    unsubscribe(State#state.client_id, Topics),
    send_frame(?UNSUBACK, #mqtt_frame_suback{message_id=MessageId}, <<>>, State);

handle_frame(undefined, #mqtt_frame_fixed{type=?PINGREQ}, _, State) ->
    send_frame(?PINGRESP, undefined, <<>>, State);

handle_frame(undefined, #mqtt_frame_fixed{type=?DISCONNECT}, _, State) ->
    State#state{closed=true}.




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
    ok = (t(Transport)):send(Socket, Bin),
    State.


dispatch_publish(0, MessageId, Topic, Payload, IsRetain, State) ->
    dispatch_publish_qos0(MessageId, Topic, Payload, IsRetain, State);
dispatch_publish(1, MessageId, Topic, Payload, IsRetain, State) ->
    dispatch_publish_qos1(MessageId, Topic, Payload, IsRetain, State);
dispatch_publish(2, MessageId, Topic, Payload, IsRetain, State) ->
    dispatch_publish_qos2(MessageId, Topic, Payload, IsRetain, State).

dispatch_publish_qos0(MessageId, Topic, Payload, IsRetain, State) ->
    io:format("dispatch direct ~p ~p ~p~n", [MessageId, Topic, Payload]),
    emqttd_connection_reg:publish(Topic, Payload, IsRetain),
    State.

dispatch_publish_qos1(MessageId, Topic, Payload, IsRetain, State) ->
    ok = emqttd_buffer:store_and_forward(State#state.client_id, Topic, Payload, IsRetain),
    send_frame(?PUBACK, #mqtt_frame_publish{message_id=MessageId}, <<>>, State).

dispatch_publish_qos2(MessageId, Topic, Payload, IsRetain, State) ->
    ok = emqttd_buffer:store(State#state.client_id, MessageId, Topic, Payload, IsRetain),
    send_frame(?PUBREC, #mqtt_frame_publish{message_id=MessageId}, <<>>, State).

subscribe(_, [], Acc) -> lists:reverse(Acc);
subscribe(ClientId, [#mqtt_topic{name=Name, qos=QoS}|Rest], Acc) ->
    emqttd_connection_reg:subscribe(ClientId, Name, QoS),
    subscribe(ClientId, Rest, [QoS|Acc]).

unsubscribe(_, []) -> ok;
unsubscribe(ClientId, [#mqtt_topic{name=Name}|Rest]) ->
    emqttd_connection_reg:unsubscribe(ClientId, Name),
    unsubscribe(ClientId, Rest).

get_msg_id(0, State) ->
    {undefined, State};
get_msg_id(_, #state{next_msg_id=65535} = State) ->
    {1, State#state{next_msg_id=2}};
get_msg_id(_, #state{next_msg_id=MsgId} = State) ->
    {MsgId, State#state{next_msg_id=MsgId + 1}}.

maybe_publish_last_will(#state{will_topic=undefined} = State) -> State;
maybe_publish_last_will(#state{will_qos=QoS, will_topic=Topic,
                               will_msg=Msg } = State) ->
    {MsgId, NewState} = get_msg_id(QoS, State),
    dispatch_publish(QoS, MsgId, Topic, Msg, false, NewState).

-compile({inline,[i/1,t/1]}).
i(ranch_tcp) -> inet;
i(ranch_ssl) -> ssl;
i(gen_tcp) -> inet;
i(ssl) -> ssl.
t(ranch_tcp) -> gen_tcp;
t(ranch_ssl) -> ssl.
