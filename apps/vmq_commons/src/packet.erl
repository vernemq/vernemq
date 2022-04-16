-module(packet).
-include_lib("vmq_commons/include/vmq_types.hrl").
-export([expect_packet/3,
         expect_packet/4,
         expect_packet/5,
         do_client_connect/3,
         gen_connect/2,
         gen_connack/0,
         gen_connack/1,
         gen_connack/2,
         gen_publish/4,
         gen_puback/1,
         gen_pubrec/1,
         gen_pubrel/1,
         gen_pubcomp/1,
         gen_subscribe/2,
         gen_subscribe/3,
         gen_suback/2,
         gen_unsubscribe/2,
         gen_unsuback/1,
         gen_pingreq/0,
         gen_pingresp/0,
         gen_disconnect/0]).

expect_packet(Socket, Name, Expected) ->
    expect_packet(gen_tcp, Socket, Name, Expected).
expect_packet(Transport, Socket, Name, Expected) ->
    expect_packet(Transport, Socket, Name, Expected, 15000).
expect_packet(Transport, Socket, _Name, Expected, Timeout) ->
    RLen =
    case byte_size(Expected) of
        L when L > 0 -> L;
        _ -> 1
    end,
    case Transport:recv(Socket, RLen, Timeout) of
        {ok, Expected} ->
            ok;
        {ok, Different} ->
            io:format(user, "exp ~p: diff ~p~n", [Expected, Different]),
            {ExpectedFrame, <<>>} = vmq_parser:parse(Expected),
            {DifferentFrame, <<>>} = vmq_parser:parse(Different),
            {error, diff(ExpectedFrame, DifferentFrame)};
        E ->
            E
    end.

diff(Rec1, Rec2) ->
    diff_record(Rec1, Rec2).

diff_record(T1, T2) ->
    [RecordName|L1] = tuple_to_list(T1),
    [RecordName|L2] = tuple_to_list(T2),
    Fields = fields(RecordName),
    PL1 = lists:zip(Fields, L1),
    PL2 = lists:zip(Fields, L2),
    [begin
         {_,VD} = lists:keyfind(K, 1, PL2),
         {K, V, VD}
     end || {K,V} = I <- PL1, lists:keyfind(K, 1, PL2) /= I].

fields(mqtt_publish) -> record_info(fields, mqtt_publish);
fields(mqtt_connect) -> record_info(fields, mqtt_connect);
fields(mqtt_subscribe) -> record_info(fields, mqtt_subscribe);
fields(mqtt_unsubscribe) -> record_info(fields, mqtt_unsubscribe);
fields(mqtt_puback) -> record_info(fields, mqtt_puback);
fields(mqtt_suback) -> record_info(fields, mqtt_suback);
fields(mqtt_connack) -> record_info(fields, mqtt_connack);
fields(mqtt_unsuback) -> record_info(fields, mqtt_unsuback);
fields(mqtt_pubrel) -> record_info(fields, mqtt_pubrel);
fields(mqtt_pubrec) -> record_info(fields, mqtt_pubrec);
fields(mqtt_pubcomp) -> record_info(fields, mqtt_pubcomp).

do_client_connect(ConnectPacket, ConnackPacket, Opts) ->
    Host = proplists:get_value(hostname, Opts, "localhost"),
    Port = proplists:get_value(port, Opts, 1888),
    Timeout = proplists:get_value(timeout, Opts, 60000),
    Transport = proplists:get_value(transport, Opts, gen_tcp),
    ConnackError = proplists:get_value(connack_error, Opts, "connack"),
    ConnOpts = [binary, {reuseaddr, true},{active, false}, {packet, raw}|
                proplists:get_value(conn_opts, Opts, [])],
    case Transport:connect(Host, Port, ConnOpts, Timeout) of
        {ok, Socket} ->
            Transport:send(Socket, ConnectPacket),
            case expect_packet(Transport, Socket, ConnackError, ConnackPacket) of
                ok ->
                    {ok, Socket};
                E ->
                    Transport:close(Socket),
                    E
            end;
        ConnectError ->
            ConnectError
    end.

gen_connect(ClientId, Opts) ->
    vmq_parser:gen_connect(ClientId, Opts).

gen_connack() ->
    gen_connack(0).
gen_connack(RC) ->
    vmq_parser:gen_connack(RC).
gen_connack(SessionPresent, RC) ->
    vmq_parser:gen_connack(SessionPresent, RC).

gen_publish(Topic, Qos, Payload, Opts) ->
    vmq_parser:gen_publish(Topic, Qos, Payload, Opts).

gen_puback(Mid) ->
    vmq_parser:gen_puback(Mid).

gen_pubrec(Mid) ->
    vmq_parser:gen_pubrec(Mid).

gen_pubrel(Mid) ->
    vmq_parser:gen_pubrel(Mid).

gen_pubcomp(Mid) ->
    vmq_parser:gen_pubcomp(Mid).

gen_subscribe(MId, Topics) ->
    vmq_parser:gen_subscribe(MId, Topics).

gen_subscribe(MId, Topic, Qos) ->
    vmq_parser:gen_subscribe(MId, Topic, Qos).

gen_suback(Mid, Qos) ->
    vmq_parser:gen_suback(Mid, Qos).

gen_unsubscribe(MId, Topic) ->
    vmq_parser:gen_unsubscribe(MId, Topic).

gen_unsuback(MId) ->
    vmq_parser:gen_unsuback(MId).

gen_pingreq() ->
    vmq_parser:gen_pingreq().

gen_pingresp() ->
    vmq_parser:gen_pingresp().

gen_disconnect() ->
    vmq_parser:gen_disconnect().
