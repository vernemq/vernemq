-module(packetv5).
-include_lib("vmq_commons/include/vmq_types.hrl").
-export([do_client_connect/2,
         do_client_connect/3,
         expect_frame/2,
         receive_frame/1,
         receive_frame/3,
         receive_frame/4]).

-export([gen_connect/2,
         gen_connack/0,
         gen_connack/1,
         gen_connack/2,
         gen_connack/3,
         gen_publish/4,
         gen_puback/1,
         gen_puback/3,
         gen_pubrec/1,
         gen_pubrec/3,
         gen_pubrel/1,
         gen_pubrel/3,
         gen_pubcomp/1,
         gen_pubcomp/3,
         gen_subtopic/2,
         gen_subtopic/5,
         gen_subscribe/3,
         gen_suback/3,
         gen_unsubscribe/3,
         gen_unsuback/3,
         gen_pingreq/0,
         gen_pingresp/0,
         gen_disconnect/0,
         gen_disconnect/2,
         gen_auth/0,
         gen_auth/2
        ]).

gen_connect(ClientId, Opts) ->
    vmq_parser_mqtt5:gen_connect(ClientId, Opts).

gen_connack() ->
    gen_connack(?M5_CONNACK_ACCEPT).
gen_connack(RC) ->
    gen_connack(0, RC).
gen_connack(SP, RC) ->
    gen_connack(SP, RC, #{}).
gen_connack(SP, RC, Properties) when is_map(Properties) ->
    vmq_parser_mqtt5:gen_connack(SP, RC, Properties).

gen_publish(Topic, QoS, Payload, Opts) ->
    vmq_parser_mqtt5:gen_publish(Topic, QoS, Payload, Opts).

gen_puback(MId) ->
    gen_puback(MId, ?M5_SUCCESS, #{}).

gen_puback(MId, ReasonCode, Properties) when is_map(Properties) ->
    vmq_parser_mqtt5:gen_puback(MId, ReasonCode, Properties).


gen_pubrec(MId) ->
    gen_pubrec(MId, ?M5_SUCCESS, #{}).
gen_pubrec(MId, ReasonCode, Properties) when is_map(Properties) ->
    vmq_parser_mqtt5:gen_pubrec(MId, ReasonCode, Properties).

gen_pubrel(MId) ->
    gen_pubrel(MId, ?M5_SUCCESS, #{}).
gen_pubrel(MId, ReasonCode, Properties) when is_map(Properties) ->
    vmq_parser_mqtt5:gen_pubrel(MId, ReasonCode, Properties).

gen_pubcomp(MId) ->
    gen_pubcomp(MId, ?M5_SUCCESS, #{}).
gen_pubcomp(MId, ReasonCode, Properties) when is_map(Properties) ->
    vmq_parser_mqtt5:gen_pubcomp(MId, ReasonCode, Properties).

gen_subtopic(Topic, QoS) ->
    gen_subtopic(Topic, QoS, false, false, send_retain).
gen_subtopic(Topic, QoS, NL, Rap, RH) ->
    #mqtt5_subscribe_topic{
       topic = Topic,
       qos = QoS,
       no_local = NL,
       rap = Rap,
       retain_handling = RH}.

gen_subscribe(Mid, Topics, Properties) when is_map(Properties) ->
    vmq_parser_mqtt5:gen_subscribe(Mid, Topics, Properties).

gen_suback(MId, ReasonCodes, Properties) when is_map(Properties) ->
    vmq_parser_mqtt5:gen_suback(MId, ReasonCodes, Properties).

gen_unsubscribe(MId, Topics, Properties) when is_map(Properties) ->
    vmq_parser_mqtt5:gen_unsubscribe(MId, Topics, Properties).

gen_unsuback(MId, ReasonCodes, Properties) when is_map(Properties) ->
    vmq_parser_mqtt5:gen_unsuback(MId, ReasonCodes, Properties).

gen_pingreq() ->
    vmq_parser_mqtt5:gen_pingreq().

gen_pingresp() ->
    vmq_parser_mqtt5:gen_pingresp().

gen_disconnect() ->
    gen_disconnect(?M5_NORMAL_DISCONNECT, #{}).

gen_disconnect(RC, Properties) when is_map(Properties) ->
    vmq_parser_mqtt5:gen_disconnect(RC, Properties).

gen_auth() ->
    gen_auth(?M5_SUCCESS, #{}).

gen_auth(RC, Properties) when is_map(Properties) ->
    vmq_parser_mqtt5:gen_auth(RC, Properties).

do_client_connect(ConnectPacket, Connack, Opts) ->
    Host = proplists:get_value(hostname, Opts, "localhost"),
    Port = proplists:get_value(port, Opts, 1888),
    Timeout = proplists:get_value(timeout, Opts, 60000),
    Transport = proplists:get_value(transport, Opts, gen_tcp),
    ConnOpts = [binary, {reuseaddr, true},{active, false}, {packet, raw}|
                proplists:get_value(conn_opts, Opts, [])],
    case Transport:connect(Host, Port, ConnOpts, Timeout) of
        {ok, Socket} ->
            Transport:send(Socket, ConnectPacket),
            case expect_frame(Transport, Socket, Connack) of
                ok ->
                    {ok, Socket};
                E ->
                    Transport:close(Socket),
                    E
            end
    end.

do_client_connect(ConnectPacket, Opts) ->
    Host = proplists:get_value(hostname, Opts, "localhost"),
    Port = proplists:get_value(port, Opts, 1888),
    Timeout = proplists:get_value(timeout, Opts, 60000),
    Transport = proplists:get_value(transport, Opts, gen_tcp),
    case Transport:connect(Host, Port, [binary, {reuseaddr, true},{active, false}, {packet, raw}], Timeout) of
        {ok, Socket} ->
            Transport:send(Socket, ConnectPacket),
            case receive_frame(Socket) of
                {ok, Frame, Rest} ->
                    {ok, Socket, Frame, Rest};
                E ->
                    Transport:close(Socket),
                    E
            end
    end.

expect_frame(Socket, Want) ->
    expect_frame(gen_tcp, Socket, Want).

expect_frame(Transport, Socket, Want) ->
    expect_frame(Transport, Socket, Want, 5000).

expect_frame(Transport, Socket, Want, Timeout) ->
    case Transport:recv(Socket, byte_size(Want), Timeout) of
        {ok, Want} ->
            ok;
        {ok, Got} ->
            {WantPacket, <<>>} = vmq_parser_mqtt5:parse(Want),
            {GotPacket, <<>>} = vmq_parser_mqtt5:parse(Got),
            case compare_packets(WantPacket, GotPacket) of
                ok ->
                    ok;
                E ->
                    io:format(user, "want ~p: got: ~p~n", [WantPacket, GotPacket]),
                    E
            end;
        E -> E
    end.

receive_frame(Socket) ->
    receive_frame(gen_tcp, Socket).
receive_frame(Transport, Socket) ->
    receive_frame(Transport, Socket, 5000).
receive_frame(Transport, Socket, Timeout) ->
    receive_frame(Transport, Socket, Timeout, <<>>).

receive_frame(Transport, Socket, Timeout, Incomplete) ->
    case vmq_parser_mqtt5:parse(Incomplete) of
        more ->
            case Transport:recv(Socket, 0, Timeout) of
                {ok, Data} ->
                    NewData = <<Incomplete/binary, Data/binary>>,
                    case vmq_parser_mqtt5:parse(NewData) of
                        more ->
                            receive_frame(Transport, Socket, Timeout, NewData);
                        {error, R} ->
                            {error, R};
                        {Frame, Rest} ->
                            {ok, Frame, Rest}
                    end;
                E -> E
            end;
        {error, R} ->
            {error, R};
        {Frame, Rest} ->
            {ok, Frame, Rest}
    end.

compare_packets(Same, Same) -> ok;
compare_packets(Want, Got) -> {error, {frames_not_equal, {want, Want}, {got, Got}}}.

