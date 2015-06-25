-module(vmq_parser).
-include("vmq_types.hrl").
-export([parse/1, serialise/1]).

-export([gen_connect/2,
         gen_connack/0,
         gen_connack/1,
         gen_publish/4,
         gen_puback/1,
         gen_pubrec/1,
         gen_pubrel/1,
         gen_pubcomp/1,
         gen_subscribe/3,
         gen_suback/2,
         gen_unsubscribe/2,
         gen_unsuback/1,
         gen_pingreq/0,
         gen_pingresp/0,
         gen_disconnect/0]).

%% frame types
-define(CONNECT,      1).
-define(CONNACK,      2).
-define(PUBLISH,      3).
-define(PUBACK,       4).
-define(PUBREC,       5).
-define(PUBREL,       6).
-define(PUBCOMP,      7).
-define(SUBSCRIBE,    8).
-define(SUBACK,       9).
-define(UNSUBSCRIBE, 10).
-define(UNSUBACK,    11).
-define(PINGREQ,     12).
-define(PINGRESP,    13).
-define(DISCONNECT,  14).

-define(RESERVED, 0).
-define(PROTOCOL_MAGIC_31, <<"MQIsdp">>).
-define(PROTOCOL_MAGIC_311, <<"MQTT">>).
-define(MAX_LEN, 16#fffffff).
-define(HIGHBIT, 2#10000000).
-define(LOWBITS, 2#01111111).

-define(MAX_PACKET_SIZE, 266338304).

-spec parse(binary()) -> {mqtt_frame(), binary()} | {error, binary()} | more.
parse(<<Fixed:1/binary, 0:1, L1:7, Data:L1/binary, Rest/binary>>) ->
    {parse(Fixed, Data), Rest};
parse(<<Fixed:1/binary, 1:1, L1:7, 0:1, L2:7, Data/binary>>) ->
    DataSize = L1 + (L2 bsl 7),
    case Data of
        <<Var:DataSize/binary, Rest/binary>> -> {parse(Fixed, Var), Rest};
        _ -> more
    end;
parse(<<Fixed:1/binary, 1:1, L1:7, 1:1, L2:7, 0:1, L3:7, Data/binary>>) ->
    DataSize = L1 + (L2 bsl 7) + (L3 bsl 14),
    case Data of
        <<Var:DataSize/binary, Rest/binary>> -> {parse(Fixed, Var), Rest};
        _ -> more
    end;
parse(<<Fixed:1/binary, 1:1, L1:7, 1:1, L2:7, 1:1, L3:7, 0:1, L4:7, Data/binary>>) ->
    DataSize = L1 + (L2 bsl 7) + (L3 bsl 14) + (L4 bsl 21),
    case Data of
        <<Var:DataSize/binary, Rest/binary>> -> {parse(Fixed, Var), Rest};
        _ -> more
    end;
parse(Data) when byte_size(Data) =< 8 -> more;
parse(_) -> {error, <<>>}.

parse(<<?PUBLISH:4, Dup:1, 0:2, Retain:1>>, <<TopicLen:16/big, Topic:TopicLen/binary, Payload/binary>>) ->
    LTopic = binary_to_list(Topic),
    case vmq_topic:validate({publish, LTopic}) of
        true ->
            #mqtt_publish{dup=Dup,
                          retain=Retain,
                          topic=list(Topic),
                          qos=0,
                          payload=Payload};
        false ->
            error
    end;
parse(<<?PUBLISH:4, Dup:1, QoS:2, Retain:1>>, <<TopicLen:16/big, Topic:TopicLen/binary, MessageId:16/big, Payload/binary>>)
  when QoS < 3 ->
    LTopic = binary_to_list(Topic),
    case vmq_topic:validate({publish, LTopic}) of
        true ->
            #mqtt_publish{dup=Dup,
                          retain=Retain,
                          topic=LTopic,
                          qos=QoS,
                          message_id=MessageId,
                          payload=Payload};
        false ->
            error
    end;
parse(<<?PUBACK:4, 0:4>>, <<MessageId:16/big>>) ->
    #mqtt_puback{message_id=MessageId};
parse(<<?PUBREC:4, 0:4>>, <<MessageId:16/big>>) ->
    #mqtt_pubrec{message_id=MessageId};
parse(<<?PUBREL:4, 0:2, 1:1, 0:1>>, <<MessageId:16/big>>) ->
    #mqtt_pubrel{message_id=MessageId};
parse(<<?PUBCOMP:4, 0:4>>, <<MessageId:16/big>>) ->
    #mqtt_pubcomp{message_id=MessageId};
parse(<<?SUBSCRIBE:4, 0:2, 1:1, 0:1>>, <<MessageId:16/big, Topics/binary>>) ->
    case parse_topics(?SUBSCRIBE, Topics, []) of
        error -> error;
        ParsedTopics ->
            #mqtt_subscribe{topics=ParsedTopics,
                            message_id=MessageId}
    end;
parse(<<?UNSUBSCRIBE:4, 0:2, 1:1, 0:1>>, <<MessageId:16/big, Topics/binary>>) ->
    case parse_topics(?UNSUBSCRIBE, Topics, []) of
        error -> error;
        ParsedTopics ->
            #mqtt_unsubscribe{topics=ParsedTopics,
                              message_id=MessageId}
    end;
parse(<<?SUBACK:4, 0:4>>, <<MessageId:16/big, Acks/binary>>) ->
    #mqtt_suback{qos_table=parse_acks(Acks, []),
                 message_id=MessageId};
parse(<<?UNSUBACK:4, 0:4>>, <<MessageId:16/big>>) ->
    #mqtt_unsuback{message_id=MessageId};
parse(<<?CONNECT:4, 0:4>>, <<L:16/big, PMagic:L/binary, _/binary>>)
  when not ((PMagic == ?PROTOCOL_MAGIC_311) or
             (PMagic == ?PROTOCOL_MAGIC_31)) ->
    error;
parse(<<?CONNECT:4, 0:4>>,
    <<L:16/big, _:L/binary, ProtoVersion:8,
      UserNameFlag:1, PasswordFlag:1, WillRetain:1, WillQos:2, WillFlag:1,
      CleanSession:1,
      0:1,  % reserved
      KeepAlive: 16/big,
      ClientIdLen:16/big, ClientId:ClientIdLen/binary, Rest/binary>>) ->
    {WillTopic, WillMsg, Rest1} =
    case {WillFlag, Rest} of
        {0, _} -> {<<>>, <<>>, Rest};
        {1,<<WillTopicLen:16/big, WT:WillTopicLen/binary,
             WillMsgLen:16/big, WM:WillMsgLen/binary, R1/binary>>} ->
            {WT, WM, R1}
    end,
    {UserName, Rest2} =
    case {UserNameFlag, Rest1} of
        {0, _} -> {<<>>, Rest1};
        {1, <<UserNameLen:16/big, U:UserNameLen/binary, R2/binary>>} ->
            {U, R2}
    end,
    {Password, <<>>} =
    case {PasswordFlag, Rest2} of
        {0, _} -> {<<>>, Rest2};
        {1, <<PasswordLen:16/big, P:PasswordLen/binary, R3/binary>>} ->
            {P, R3}
    end,
    case {WillTopic, WillMsg} of
        {<<>>, <<>>} ->
            #mqtt_connect{proto_ver=ProtoVersion,
                          username=list(UserName),
                          password=list(Password),
                          clean_session=CleanSession,
                          keep_alive=KeepAlive,
                          client_id=list(ClientId)};
        _ ->
            LWillTopic = binary_to_list(WillTopic),
            case vmq_topic:validate({publish, LWillTopic}) of
                true ->
                    #mqtt_connect{proto_ver=ProtoVersion,
                                  username=list(UserName),
                                  password=list(Password),
                                  will_retain=WillRetain,
                                  will_qos=WillQos,
                                  clean_session=CleanSession,
                                  keep_alive=KeepAlive,
                                  client_id=list(ClientId),
                                  will_topic=LWillTopic,
                                  will_msg=WillMsg};
                false ->
                    error
            end
    end;
parse(<<?CONNACK:4, 0:4>>, <<0:8, ReturnCode:8/big>>) ->
    #mqtt_connack{return_code=ReturnCode};
parse(<<?PINGREQ:4, 0:4>>, <<>>) ->
    #mqtt_pingreq{};
parse(<<?PINGRESP:4, 0:4>>, <<>>) ->
    #mqtt_pingresp{};
parse(<<?DISCONNECT:4, 0:4>>, <<>>) ->
    #mqtt_disconnect{};
parse(_, _) -> error.

parse_topics(_, <<>>, []) -> error;
parse_topics(_, <<>>, Topics) -> Topics;
parse_topics(?SUBSCRIBE = Sub, <<L:16/big, Topic:L/binary, 0:6, QoS:2, Rest/binary>>, Acc)
  when (QoS >= 0) and (QoS < 3) ->
    LTopic = binary_to_list(Topic),
    case vmq_topic:validate({subscribe, LTopic}) of
        true ->
            parse_topics(Sub, Rest, [{LTopic, QoS}|Acc]);
        false ->
            error
    end;
parse_topics(?UNSUBSCRIBE = Sub, <<L:16/big, Topic:L/binary, Rest/binary>>, Acc) ->
    LTopic = binary_to_list(Topic),
    case vmq_topic:validate({subscribe, LTopic}) of
        true ->
            parse_topics(Sub, Rest, [LTopic|Acc]);
        false ->
            error
    end;
parse_topics(_, _, _) -> error.

parse_acks(<<>>, Acks) ->
    Acks;
parse_acks(<<_:6, QoS:2, Rest/binary>>, Acks) ->
    parse_acks(Rest, [QoS | Acks]).

-spec serialise(mqtt_frame()) -> binary() | iolist().
serialise(#mqtt_publish{qos=0,
                        topic=Topic,
                        retain=Retain,
                        dup=Dup,
                        payload=Payload}) ->
    Var = [utf8(Topic), Payload],
    LenBytes = serialise_len(iolist_size(Var)),
    [<<?PUBLISH:4, (flag(Dup)):1/integer, 0:2/integer, (flag(Retain)):1/integer>>, LenBytes, Var];
serialise(#mqtt_publish{message_id=MessageId,
                        topic=Topic,
                        qos=QoS,
                        retain=Retain,
                        dup=Dup,
                        payload=Payload}) ->
    Var = [utf8(Topic), msg_id(MessageId), Payload],
    LenBytes = serialise_len(iolist_size(Var)),
    [<<?PUBLISH:4, (flag(Dup)):1/integer,
       (default(QoS, 0)):2/integer, (flag(Retain)):1/integer>>, LenBytes, Var];

serialise(#mqtt_puback{message_id=MessageId}) ->
    <<?PUBACK:4, 0:4, 2, MessageId:16/big>>;
serialise(#mqtt_pubrel{message_id=MessageId}) ->
    <<?PUBREL:4, 0:2, 1:1, 0:1, 2, MessageId:16/big>>;
serialise(#mqtt_pubrec{message_id=MessageId}) ->
    <<?PUBREC:4, 0:4, 2, MessageId:16/big>>;
serialise(#mqtt_pubcomp{message_id=MessageId}) ->
    <<?PUBCOMP:4, 0:4, 2, MessageId:16/big>>;
serialise(#mqtt_connect{proto_ver=ProtoVersion,
                        username=UserName,
                        password=Password,
                        will_retain=WillRetain,
                        will_qos=WillQos,
                        clean_session=CleanSession,
                        keep_alive=KeepAlive,
                        client_id=ClientId,
                        will_topic=WillTopic,
                        will_msg=WillMsg}) ->
    {PMagicL, PMagic} = proto(ProtoVersion),
    Var = [<<PMagicL:16/big-unsigned-integer, PMagic/binary,
             ProtoVersion:8/unsigned-integer,
             (flag(UserName)):1/integer,
             (flag(Password)):1/integer,
             (flag(WillRetain)):1/integer,
             (default(WillQos, 0)):2/integer,
             (flag(WillTopic)):1/integer,
             (flag(CleanSession)):1/integer,
             0:1,  % reserved
             (default(KeepAlive, 0)):16/big-unsigned-integer>>,
           utf8(ClientId),
           utf8(WillTopic),
           utf8(WillMsg),
           utf8(UserName),
           utf8(Password)],
    LenBytes = serialise_len(iolist_size(Var)),
    [<<?CONNECT:4, 0:4>>, LenBytes, Var];
serialise(#mqtt_connack{return_code=RC}) ->
    [<<?CONNACK:4, 0:4>>, serialise_len(2), <<RC:16/big>>];
serialise(#mqtt_subscribe{message_id=MessageId, topics=Topics}) ->
    SerialisedTopics = serialise_topics(?SUBSCRIBE, Topics, []),
    LenBytes = serialise_len(iolist_size(SerialisedTopics) + 2),
    [<<?SUBSCRIBE:4, 0:2, 1:1, 0:1>>, LenBytes, <<MessageId:16/big>>, SerialisedTopics];
serialise(#mqtt_suback{message_id=MessageId, qos_table=QosTable}) ->
    SerialisedAcks = serialise_acks(QosTable, []),
    LenBytes = serialise_len(iolist_size(SerialisedAcks) + 2),
    [<<?SUBACK:4, 0:4>>, LenBytes, <<MessageId:16/big>>, SerialisedAcks];
serialise(#mqtt_unsubscribe{message_id=MessageId, topics=Topics}) ->
    SerialisedTopics = serialise_topics(?UNSUBSCRIBE, Topics, []),
    LenBytes = serialise_len(iolist_size(SerialisedTopics) + 2),
    [<<?UNSUBSCRIBE:4, 0:2, 1:1, 0:1>>, LenBytes, <<MessageId:16/big>>,
     SerialisedTopics];
serialise(#mqtt_unsuback{message_id=MessageId}) ->
    <<?UNSUBACK:4, 0:4, 2, MessageId:16/big>>;
serialise(#mqtt_pingreq{}) ->
    <<?PINGREQ:4, 0:4, 0>>;
serialise(#mqtt_pingresp{}) ->
    <<?PINGRESP:4, 0:4, 0>>;
serialise(#mqtt_disconnect{}) ->
    <<?DISCONNECT:4, 0:4, 0>>.

serialise_len(N) when N =< ?LOWBITS ->
    <<0:1, N:7>>;
serialise_len(N) ->
    <<1:1, (N rem ?HIGHBIT):7, (serialise_len(N div ?HIGHBIT))/binary>>.

serialise_topics(_, [], Topics) ->
    Topics;
serialise_topics(?SUBSCRIBE = Sub, [{Topic, QoS}|Rest], Acc) ->
    serialise_topics(Sub, Rest, [utf8(Topic), <<0:6, QoS:2>>|Acc]);
serialise_topics(?UNSUBSCRIBE = Sub, [Topic|Rest], Acc) ->
    serialise_topics(Sub, Rest, [utf8(Topic)|Acc]).

serialise_acks([], Acks) ->
    Acks;
serialise_acks([QoS|Rest], Acks) ->
    serialise_acks(Rest, [<<0:6, QoS:2>>|Acks]).

proto(4) -> {4, ?PROTOCOL_MAGIC_311};
proto(3) -> {6, ?PROTOCOL_MAGIC_31};
proto(131) -> {6, ?PROTOCOL_MAGIC_31}.

flag([]) -> 0;
flag(undefined) -> 0;
flag(0) -> 0;
flag(1) -> 1;
flag(false) -> 0;
flag(true) -> 1;
flag(V) when is_binary(V) orelse is_list(V) -> 1;
flag(empty) -> 1;
flag(_) -> 0.

list(<<>>) -> undefined;
list(B) -> binary_to_list(B).

msg_id(undefined) -> <<>>;
msg_id(MsgId) -> <<MsgId:16/big>>.

default(undefined, Default) -> Default;
default(Val, _) -> Val.

utf8(<<>>) -> [];
utf8(undefined) -> [];
utf8(empty) -> <<0:16/big>>; %% useful if you want to encode an empty string..
utf8(List) when is_list(List) ->
    utf8(list_to_binary(List));
utf8(Bin) -> <<(byte_size(Bin)):16/big, Bin/binary>>.


%%%%%%% packet generator functions (useful for testing)
gen_connect(ClientId, Opts) ->
    Frame = #mqtt_connect{
              client_id = ClientId,
              clean_session =  proplists:get_value(clean_session, Opts, true),
              keep_alive =     proplists:get_value(keepalive, Opts, 60),
              username =       proplists:get_value(username, Opts),
              password =       proplists:get_value(password, Opts),
              proto_ver =      proplists:get_value(proto_ver, Opts, 3),
              will_topic =     proplists:get_value(will_topic, Opts),
              will_qos =       proplists:get_value(will_qos, Opts, 0),
              will_retain =    proplists:get_value(will_retain, Opts, false),
              will_msg =       proplists:get_value(will_msg, Opts)
              },
    iolist_to_binary(serialise(Frame)).

gen_connack() ->
    gen_connack(?CONNACK_ACCEPT).
gen_connack(RC) ->
    iolist_to_binary(serialise(#mqtt_connack{return_code=RC})).

gen_publish(Topic, Qos, Payload, Opts) ->
    Frame = #mqtt_publish{
               dup =               proplists:get_value(dup, Opts, false),
               qos =               Qos,
               retain =            proplists:get_value(retain, Opts, false),
               topic =             Topic,
               message_id =        proplists:get_value(mid, Opts, 0),
               payload =           Payload
              },
    iolist_to_binary(serialise(Frame)).

gen_puback(MId) ->
    iolist_to_binary(serialise(#mqtt_puback{message_id=MId})).

gen_pubrec(MId) ->
    iolist_to_binary(serialise(#mqtt_pubrec{message_id=MId})).

gen_pubrel(MId) ->
    iolist_to_binary(serialise(#mqtt_pubrel{message_id=MId})).

gen_pubcomp(MId) ->
    iolist_to_binary(serialise(#mqtt_pubcomp{message_id=MId})).

gen_subscribe(MId, Topic, Qos) ->
    iolist_to_binary(serialise(#mqtt_subscribe{topics=[{Topic, Qos}], message_id=MId})).

gen_suback(MId, Qos) ->
    iolist_to_binary(serialise(#mqtt_suback{qos_table=[Qos], message_id=MId})).

gen_unsubscribe(MId, Topic) ->
    iolist_to_binary(serialise(#mqtt_unsubscribe{topics=[Topic], message_id=MId})).

gen_unsuback(MId) ->
    iolist_to_binary(serialise(#mqtt_unsuback{message_id=MId})).

gen_pingreq() ->
    iolist_to_binary(serialise(#mqtt_pingreq{})).

gen_pingresp() ->
    iolist_to_binary(serialise(#mqtt_pingresp{})).

gen_disconnect() ->
    iolist_to_binary(serialise(#mqtt_disconnect{})).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

parser_test() ->
    compare_frame("connect1", gen_connect("test-client", [])),
    compare_frame("connect2", gen_connect("test-client", [{will_topic, "test-will-topic"},
                                                          {will_msg, "this is a samp"},
                                                          {will_qos, 2},
                                                          {username, "joe"},
                                                          {password, "secret"}])),
    compare_frame("connack", gen_connack()),
    compare_frame("publish1", gen_publish("test-topic", 0, <<"test-payload">>, [{dup, true}, {retain, true}])),
    compare_frame("publish2", gen_publish("test-topic", 2, crypto:rand_bytes(1000), [{dup, true}, {retain, true}])),
    compare_frame("publish3", gen_publish("test-topic", 2, crypto:rand_bytes(100000), [{dup, true}, {retain, true}])),
    compare_frame("publish4", gen_publish("test-topic", 2, crypto:rand_bytes(2097153), [{dup, true}, {retain, true}])),

    compare_frame("puback", gen_puback(123)),
    compare_frame("pubrec", gen_pubrec(123)),
    compare_frame("pubrel1", gen_pubrel(123)),
    compare_frame("pubcomp", gen_pubcomp(123)),

    compare_frame("subscribe", gen_subscribe(123, "test/hello/world", 2)),
    compare_frame("suback", gen_suback(123, 2)),
    compare_frame("unsubscribe", gen_unsubscribe(123, "test/hello/world")),
    compare_frame("unsuback", gen_unsuback(123)),

    compare_frame("pingreq", gen_pingreq()),
    compare_frame("pingresp", gen_pingresp()),
    compare_frame("disconnect", gen_disconnect()).

compare_frame(Test, Frame) ->
    io:format(user, "---- compare test: ~p~n", [Test]),
    {ParsedFrame, <<>>} = parse(Frame),
    SerializedFrame = iolist_to_binary(serialise(ParsedFrame)),
    compare_frame(Test, Frame, SerializedFrame).
compare_frame(_, F, F) -> true.
-endif.
