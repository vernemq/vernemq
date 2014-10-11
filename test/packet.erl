-module(packet).
-include_lib("emqtt_commons/include/emqtt_frame.hrl").
-export([expect_packet/3,
         expect_packet/4,
         do_client_connect/3,
         gen_connect/2,
         gen_connack/0,
         gen_connack/1,
         gen_publish/4,
         gen_puback/1,
         gen_pubrec/1,
         gen_pubrel/1,
         gen_pubrel/2,
         gen_pubcomp/1,
         gen_subscribe/3,
         gen_suback/2,
         gen_unsubscribe/2,
         gen_unsuback/1,
         gen_pingreq/0,
         gen_pingresp/0,
         gen_disconnect/0]).

expect_packet(Socket, Name, Expected) ->
    expect_packet(gen_tcp, Socket, Name, Expected).
expect_packet(Transport, Socket, _Name, Expected) ->
    RLen =
    case byte_size(Expected) of
        L when L > 0 -> L;
        _ -> 1
    end,
    case Transport:recv(Socket, RLen, 60000) of
        {ok, Expected} ->
            ok;
        {ok, Different} ->
            io:format(user, "exp ~p: diff ~p~n", [Expected, Different]),
            InitState = emqtt_frame:initial_state(),
            {ok, ExpectedFrame, _} = emqtt_frame:parse(Expected, InitState),
            {ok, DifferentFrame, _} = emqtt_frame:parse(Different, InitState),
            {error, diff(ExpectedFrame, DifferentFrame)};
        E ->
            E
    end.

diff(#mqtt_frame{fixed=EFixed, variable=EVariable, payload=EPayload},
     #mqtt_frame{fixed=DFixed, variable=DVariable, payload=DPayload}) ->
    diff_fixed(EFixed, DFixed)
    ++ diff_variable(EVariable, DVariable)
    ++ diff_payload(EPayload, DPayload).

diff_fixed(R1,R2) -> diff_record(R1, R2).
diff_variable(R1,R2) -> diff_record(R1, R2).
diff_payload(P,P) -> [];
diff_payload(P1, P2) -> [{payload, P1, P2}].

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



fields(mqtt_frame_fixed) -> record_info(fields, mqtt_frame_fixed);
fields(mqtt_frame_connect) -> record_info(fields, mqtt_frame_connect);
fields(mqtt_frame_connack) -> record_info(fields, mqtt_frame_connack);
fields(mqtt_frame_publish) -> record_info(fields, mqtt_frame_publish);
fields(mqtt_frame_subscribe) -> record_info(fields, mqtt_frame_subscribe);
fields(mqtt_frame_suback) -> record_info(fields, mqtt_frame_suback).

do_client_connect(ConnectPacket, ConnackPacket, Opts) ->
    Host = proplists:get_value(hostname, Opts, "localhost"),
    Port = proplists:get_value(port, Opts, 1888),
    Timeout = proplists:get_value(timeout, Opts, 60000),
    ConnackError = proplists:get_value(connack_error, Opts, "connack"),
    case gen_tcp:connect(Host, Port, [binary, {reuseaddr, true},{active, false}, {packet, raw}], Timeout) of
        {ok, Socket} ->
            gen_tcp:send(Socket, ConnectPacket),
            case expect_packet(Socket, ConnackError, ConnackPacket) of
                ok ->
                    {ok, Socket};
                E ->
                    gen_tcp:close(Socket),
                    E
            end
    end.


gen_connect(ClientId, Opts) ->
    Frame =
    #mqtt_frame{

       fixed = #mqtt_frame_fixed{
                  type = ?CONNECT,
                  dup = false,
                  qos = 0,
                  retain = false},

       variable = #mqtt_frame_connect{
                     client_id =      ClientId,
                     clean_sess =     proplists:get_value(clean_session, Opts, true),
                     keep_alive =     proplists:get_value(keepalive, Opts, 60),
                     username =       proplists:get_value(username, Opts),
                     password =       proplists:get_value(password, Opts),
                     proto_ver =      proplists:get_value(proto_ver, Opts, 3),
                     will_topic =     proplists:get_value(will_topic, Opts, ""),
                     will_qos =       proplists:get_value(will_qos, Opts, 0),
                     will_retain =    proplists:get_value(will_retain, Opts, false),
                     will_msg =       proplists:get_value(will_payload, Opts, <<>>),
                     will_flag =      proplists:get_value(will_topic, Opts) /= undefined
                    }
      },
    emqtt_frame:serialise(Frame).

gen_connack() ->
    gen_connack(?CONNACK_ACCEPT).
gen_connack(RC) ->
    Frame =
    #mqtt_frame{
      fixed = #mqtt_frame_fixed{
                type = ?CONNACK,
                dup = false,
                qos = 0,
                retain = false
                },
      variable = #mqtt_frame_connack{
                   return_code = RC
                   },
      payload = <<>>
      },
    emqtt_frame:serialise(Frame).

gen_publish(Topic, Qos, Payload, Opts) ->
    Frame =
    #mqtt_frame{
      fixed = #mqtt_frame_fixed{
                type = ?PUBLISH,
                dup =               proplists:get_value(dup, Opts, false),
                qos =               Qos,
                retain =            proplists:get_value(retain, Opts, false)
                },
      variable = #mqtt_frame_publish{
                    topic_name =    Topic,
                    message_id =    proplists:get_value(mid, Opts, 0)
                   },
      payload = Payload
      },
    emqtt_frame:serialise(Frame).

gen_puback(MId) ->
    Frame =
    #mqtt_frame{
      fixed = #mqtt_frame_fixed{
                type = ?PUBACK,
                dup = false,
                qos = 0,
                retain = false
                },
      variable = #mqtt_frame_publish{
                   message_id = MId
                   },
      payload = <<>>
      },
    emqtt_frame:serialise(Frame).

gen_pubrec(MId) ->
    Frame =
    #mqtt_frame{
      fixed = #mqtt_frame_fixed{
                type = ?PUBREC,
                dup = false,
                qos = 0,
                retain = false
                },
      variable = #mqtt_frame_publish{
                   message_id = MId
                   },
      payload = <<>>
      },
    emqtt_frame:serialise(Frame).

gen_pubrel(MId) ->
    gen_pubrel(MId, false).
gen_pubrel(MId, Dup) ->
    Frame =
    #mqtt_frame{
      fixed = #mqtt_frame_fixed{
                type = ?PUBREL,
                dup = Dup,
                qos = 1,
                retain = false
                },
      variable = #mqtt_frame_publish{
                   message_id = MId
                   },
      payload = <<>>
      },
    emqtt_frame:serialise(Frame).

gen_pubcomp(MId) ->
    Frame =
    #mqtt_frame{
      fixed = #mqtt_frame_fixed{
                type = ?PUBCOMP,
                dup = false,
                qos = 0,
                retain = false
                },
      variable = #mqtt_frame_publish{
                   message_id = MId
                   },
      payload = <<>>
      },
    emqtt_frame:serialise(Frame).

gen_subscribe(MId, Topic, Qos) ->
    Frame =
    #mqtt_frame{
      fixed = #mqtt_frame_fixed{
                type = ?SUBSCRIBE,
                dup = false,
                qos = 1,
                retain = false
                },
      variable = #mqtt_frame_subscribe{
                   message_id = MId,
                   topic_table = [#mqtt_topic{name=Topic, qos=Qos}]
                   },
      payload = <<>>
      },
    emqtt_frame:serialise(Frame).

gen_suback(MId, Qos) ->
    Frame =
    #mqtt_frame{
      fixed = #mqtt_frame_fixed{
                type = ?SUBACK,
                dup = false,
                qos = 0,
                retain = false
                },
      variable = #mqtt_frame_suback{
                   message_id = MId,
                   qos_table = [Qos]
                   },
      payload = <<>>
      },
    emqtt_frame:serialise(Frame).

gen_unsubscribe(MId, Topic) ->
    Frame =
    #mqtt_frame{
      fixed = #mqtt_frame_fixed{
                type = ?UNSUBSCRIBE,
                dup = false,
                qos = 1,
                retain = false
                },
      variable = #mqtt_frame_subscribe{
                   message_id = MId,
                   topic_table = [Topic]
                   },
      payload = <<>>
      },
    emqtt_frame:serialise(Frame).

gen_unsuback(MId) ->
    Frame =
    #mqtt_frame{
      fixed = #mqtt_frame_fixed{
                type = ?UNSUBACK,
                dup = false,
                qos = 0,
                retain = false
                },
      variable = #mqtt_frame_suback{
                   message_id = MId
                   },
      payload = <<>>
      },
    emqtt_frame:serialise(Frame).

gen_pingreq() ->
    Frame =
    #mqtt_frame{
       fixed = #mqtt_frame_fixed{
                  type = ?PINGREQ,
                  qos = 0,
                  retain = false,
                  dup = false}
      },
    emqtt_frame:serialise(Frame).

gen_pingresp() ->
    Frame =
    #mqtt_frame{
       fixed = #mqtt_frame_fixed{
                  type = ?PINGRESP,
                  qos = 0,
                  retain = false,
                  dup = false}
      },
    emqtt_frame:serialise(Frame).

gen_disconnect() ->
    Frame =
    #mqtt_frame{
       fixed = #mqtt_frame_fixed{
                  type = ?DISCONNECT,
                  qos = 0,
                  retain = false,
                  dup = false}
      },
    emqtt_frame:serialise(Frame).
