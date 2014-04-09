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
                connection_attempted=false,
                buffer= <<>>}).

start_link(Ref, Socket, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
    {ok, Pid}.

init(Ref, Socket, Transport, Opts) ->
    ok = ranch:accept_ack(Ref),
    {_, AuthProviders} = lists:keyfind(auth_providers, 1, Opts),
    {ok, {Address, _Port}} = (i(Transport)):peername(Socket),
    erlang:send_after(?CLOSE_AFTER, self(), close_connection_if_not_attempted),
    loop(#state{socket=Socket, transport=Transport, src_ip=Address, auth_providers=AuthProviders}),
    emqttd_connection_reg:down(self()).

loop(#state{closed=true}) -> ok; %% DISCONNECTED BY CLIENT
loop(#state{socket=Socket, transport=Transport, buffer=Buffer} = State) ->
    case erlang:process_info(self(), message_queue_len) of
        {message_queue_len, Len} when Len < ?MAX_QUEUE_LEN ->
            (i(Transport)):setopts(Socket, [{active, once}]);
        {message_queue_len, _Len} ->
            ok
    end,
    receive
        {tcp, Socket, Input} ->
            loop(handle_input(<<Buffer/binary, Input/binary>>, State));
        {tcp_closed, Socket} -> ok;
        {tcp_error, Socket, _Reason} -> ok;
        % SSL
        {ssl, Socket, Input} ->
            loop(handle_input(<<Buffer/binary, Input/binary>>, State));
        {ssl_closed, Socket} -> ok;
        {ssl_error, Socket, _Reason} -> ok;
        close_connection_if_not_attempted when State#state.connection_attempted == true ->
            loop(State);
        {disconnect, DuplicateClientPid} ->
            DuplicateClientPid ! disconnected,
            ok;
        {route, MessageId, Topic, Payload, QoS} ->
            Bin = emqtt_frame:serialise(#mqtt_frame{
                                           fixed=#mqtt_frame_fixed{
                                                    type=?PUBLISH,
                                                    qos=QoS
                                                   },
                                           variable=#mqtt_frame_publish{
                                                       topic_name=Topic,
                                                       message_id=MessageId},
                                           payload=Payload
                                          }),
            ok = (t(Transport)):send(Socket, Bin),
            loop(State);
        _ ->
            % this will close the connection
            ok
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

handle_frame(#mqtt_frame_connect{client_id=Id, username=User, password=Password, proto_ver=Version}, _, _, #state{connected=false, src_ip=Src, auth_providers=AuthProviders} = State) ->
    State1 =
    case check_version(Version) of
        true ->
            case auth_user(Src, Id, User, Password, AuthProviders) of
                ok ->
                    send_connack(?CONNACK_ACCEPT, State#state{
                                                    client_id=Id,
                                                    username=User,
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

handle_frame(#mqtt_frame_publish{topic_name=Topic, message_id=MessageId}, Fixed, Payload, State) ->
    #mqtt_frame_fixed{qos=QoS} = Fixed,
    case QoS of
        0 ->
            dispatch_publish_direct(MessageId, Topic, Payload);
        _ ->
            io:format("QoS ~p not yet implemented~n", [QoS])
    end,
    State;

handle_frame(#mqtt_frame_subscribe{topic_table=Topics, message_id=MessageId}, _, _, State) ->
    QoSs = subscribe(Topics, []),
    send_frame(?SUBACK, #mqtt_frame_suback{message_id=MessageId, qos_table=QoSs}, <<>>, State);

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
            auth_user(SrcIp, Id, User, Password, AuthProviders);
        {error, not_authorized} ->
            {error, not_authorized};
        {error, invalid_credentials} ->
            {error, invalid_credentials}
    end.

send_connack(ReturnCode, State) ->
    send_frame(?CONNACK, #mqtt_frame_connack{return_code=ReturnCode}, <<>>, State).

send_frame(Type, Variable, Payload, #state{socket=Socket, transport=Transport} = State) ->
    Bin = emqtt_frame:serialise(#mqtt_frame{
                             fixed=#mqtt_frame_fixed{type=Type},
                             variable=Variable,
                             payload=Payload
                            }),
    io:format("send ~p~n", [Bin]),
    ok = (t(Transport)):send(Socket, Bin),
    State.


dispatch_publish_direct(MessageId, Topic, Payload) ->
    io:format("dispatch direct ~p ~p ~p~n", [MessageId, Topic, Payload]),
    emqttd_connection_reg:publish(MessageId, Topic, Payload).

subscribe([], Acc) -> lists:reverse(Acc);
subscribe([#mqtt_topic{name=Name, qos=QoS}|Rest], Acc) ->
    emqttd_connection_reg:subscribe(Name, QoS),
    subscribe(Rest, [QoS|Acc]).


-compile({inline,[i/1,t/1]}).
i(ranch_tcp) -> inet;
i(ranch_ssl) -> ssl;
i(gen_tcp) -> inet;
i(ssl) -> ssl.
t(ranch_tcp) -> gen_tcp;
t(ranch_ssl) -> ssl.
