-module(vmq_parser).
-include("vmq_types.hrl").
-export([parse/1, serialise/1]).

-export([gen_connect/2,
         gen_connack/0,
         gen_connack/1,
         gen_connack/2,
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

-spec parse(binary()) -> {mqtt_frame(), binary()} | {error, atom()} | more.
parse(<<Fixed:1/binary, 0:1, L1:7, Data/binary>>) ->
    case Data of
        <<Var:L1/binary, Rest/binary>> ->
            case parse(Fixed, Var) of
                {error, _} = E -> E;
                Frame -> {Frame, Rest}
            end;
        _ -> more
    end;
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
parse(<<_:8/binary, _/binary>>) ->
    {error, cant_parse_fixed_header};
parse(_) ->
    more.

-spec parse(binary(), binary()) -> mqtt_frame() | {error, atom()}.
parse(<<?PUBLISH:4, Dup:1, 0:2, Retain:1>>, <<TopicLen:16/big, Topic:TopicLen/binary, Payload/binary>>) ->
    case vmq_topic:validate_topic(publish, Topic) of
        {ok, ParsedTopic} ->
            #mqtt_publish{dup=Dup,
                          retain=Retain,
                          topic=ParsedTopic,
                          qos=0,
                          payload=Payload};
        {error, Reason} ->
            {error, Reason}
    end;
parse(<<?PUBLISH:4, Dup:1, QoS:2, Retain:1>>, <<TopicLen:16/big, Topic:TopicLen/binary, MessageId:16/big, Payload/binary>>)
  when QoS < 3 ->
    case vmq_topic:validate_topic(publish, Topic) of
        {ok, ParsedTopic} ->
            #mqtt_publish{dup=Dup,
                          retain=Retain,
                          topic=ParsedTopic,
                          qos=QoS,
                          message_id=MessageId,
                          payload=Payload};
        {error, Reason} ->
            {error, Reason}
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
    case parse_topics(Topics, ?SUBSCRIBE, []) of
        {ok, ParsedTopics} ->
            #mqtt_subscribe{topics=ParsedTopics,
                            message_id=MessageId};
        E -> E
    end;
parse(<<?UNSUBSCRIBE:4, 0:2, 1:1, 0:1>>, <<MessageId:16/big, Topics/binary>>) ->
    case parse_topics(Topics, ?UNSUBSCRIBE, []) of
        {ok, ParsedTopics} ->
            #mqtt_unsubscribe{topics=ParsedTopics,
                              message_id=MessageId};
        E ->
            E
    end;
parse(<<?SUBACK:4, 0:4>>, <<MessageId:16/big, Acks/binary>>) ->
    #mqtt_suback{qos_table=parse_acks(Acks, []),
                 message_id=MessageId};
parse(<<?UNSUBACK:4, 0:4>>, <<MessageId:16/big>>) ->
    #mqtt_unsuback{message_id=MessageId};
parse(<<?CONNECT:4, 0:4>>, <<L:16/big, PMagic:L/binary, _/binary>>)
  when not ((PMagic == ?PROTOCOL_MAGIC_311) or
             (PMagic == ?PROTOCOL_MAGIC_31)) ->
    {error, unknown_protocol_magic};

parse(<<?CONNECT:4, 0:4>>,
    <<L:16/big, _:L/binary, ProtoVersion:8,
      UserNameFlag:1, PasswordFlag:1, WillRetain:1, WillQos:2, WillFlag:1,
      CleanSession:1,
      0:1,  % reserved
      KeepAlive: 16/big,
      ClientIdLen:16/big, ClientId:ClientIdLen/binary, Rest0/binary>>) ->

    Conn0 = #mqtt_connect{proto_ver=ProtoVersion,
                          clean_session=CleanSession,
                          keep_alive=KeepAlive,
                          client_id=ClientId},

    case parse_last_will_topic(Rest0, WillFlag, WillRetain, WillQos, Conn0) of
        {ok, Rest1, Conn1} ->
            case parse_username(Rest1, UserNameFlag, Conn1) of
                {ok, Rest2, Conn2} ->
                    case parse_password(Rest2, UserNameFlag, PasswordFlag, Conn2) of
                        {ok, _, Conn3} ->
                            Conn3;
                        E -> E
                    end;
                E -> E
            end;
        E -> E
    end;
parse(<<?CONNACK:4, 0:4>>, <<0:7, SP:1, ReturnCode:8/big>>) ->
    #mqtt_connack{session_present=SP, return_code=ReturnCode};
parse(<<?PINGREQ:4, 0:4>>, <<>>) ->
    #mqtt_pingreq{};
parse(<<?PINGRESP:4, 0:4>>, <<>>) ->
    #mqtt_pingresp{};
parse(<<?DISCONNECT:4, 0:4>>, <<>>) ->
    #mqtt_disconnect{};
parse(_, _) -> {error, cant_parse_variable_header}.

parse_last_will_topic(Rest, 0, _, _, Conn) -> {ok, Rest, Conn};
parse_last_will_topic(<<WillTopicLen:16/big, WillTopic:WillTopicLen/binary,
                        WillMsgLen:16/big, WillMsg:WillMsgLen/binary,
                        Rest/binary>>, 1, Retain, QoS, Conn) ->
    case vmq_topic:validate_topic(publish, WillTopic) of
        {ok, ParsedTopic} ->
            {ok, Rest, Conn#mqtt_connect{will_msg=WillMsg,
                                         will_topic=ParsedTopic,
                                         will_retain=Retain,
                                         will_qos=QoS}};
        _ ->
            {error, cant_validate_last_will_topic}
    end;
parse_last_will_topic(_, 1, _, _,_) ->
    {error, cant_parse_last_will}.

parse_username(Rest, 0, Conn) -> {ok, Rest, Conn};
parse_username(<<Len:16/big, UserName:Len/binary, Rest/binary>>, 1, Conn) ->
    {ok, Rest, Conn#mqtt_connect{username=UserName}};
parse_username(_, 1, _) ->
    {error, cant_parse_username}.

parse_password(Rest, _, 0, Conn) -> {ok, Rest, Conn};
parse_password(<<Len:16/big, Password:Len/binary, Rest/binary>>, 1, 1, Conn) ->
    {ok, Rest, Conn#mqtt_connect{password=Password}};
parse_password(_, 0, 1, _) ->
    {error, username_flag_not_set};
parse_password(_, _, 1, _) ->
    {error, cant_parse_password}.


parse_topics(<<>>, _, []) -> {error, no_topic_provided};
parse_topics(<<>>, _, Topics) -> {ok, Topics};
parse_topics(<<L:16/big, Topic:L/binary, 0:6, QoS:2, Rest/binary>>, ?SUBSCRIBE = Sub, Acc)
  when (QoS >= 0) and (QoS < 3) ->
    case vmq_topic:validate_topic(subscribe, Topic) of
        {ok, ParsedTopic} ->
            parse_topics(Rest, Sub, [{ParsedTopic, QoS}|Acc]);
        E -> E
    end;
parse_topics(<<L:16/big, Topic:L/binary, Rest/binary>>, ?UNSUBSCRIBE = Sub, Acc) ->
    case vmq_topic:validate_topic(subscribe, Topic) of
        {ok, ParsedTopic} ->
            parse_topics(Rest, Sub, [ParsedTopic|Acc]);
        E -> E
    end;
parse_topics(_, _, _) -> {error, cant_parse_topics}.

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
    Var = [utf8(vmq_topic:unword(Topic)), Payload],
    LenBytes = serialise_len(iolist_size(Var)),
    [<<?PUBLISH:4, (flag(Dup)):1/integer, 0:2/integer, (flag(Retain)):1/integer>>, LenBytes, Var];
serialise(#mqtt_publish{message_id=MessageId,
                        topic=Topic,
                        qos=QoS,
                        retain=Retain,
                        dup=Dup,
                        payload=Payload}) ->
    Var = [utf8(vmq_topic:unword(Topic)), msg_id(MessageId), Payload],
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
           utf8(vmq_topic:unword(WillTopic)),
           utf8(WillMsg),
           utf8(UserName),
           utf8(Password)],
    LenBytes = serialise_len(iolist_size(Var)),
    [<<?CONNECT:4, 0:4>>, LenBytes, Var];
serialise(#mqtt_connack{session_present=SP, return_code=RC}) ->
    [<<?CONNACK:4, 0:4>>, serialise_len(2), <<0:7, (flag(SP)):1/integer>>, <<RC:8/big>>];
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
    serialise_topics(Sub, Rest, [utf8(vmq_topic:unword(Topic)), <<0:6, QoS:2>>|Acc]);
serialise_topics(?UNSUBSCRIBE = Sub, [Topic|Rest], Acc) ->
    serialise_topics(Sub, Rest, [utf8(vmq_topic:unword(Topic))|Acc]).

serialise_acks([], Acks) ->
    Acks;
serialise_acks([QoS|Rest], Acks) ->
    serialise_acks(Rest, [<<0:6, QoS:2>>|Acks]).

proto(4) -> {4, ?PROTOCOL_MAGIC_311};
proto(3) -> {6, ?PROTOCOL_MAGIC_31};
proto(131) -> {6, ?PROTOCOL_MAGIC_31}.

flag(<<>>) -> 0;
flag(undefined) -> 0;
flag(0) -> 0;
flag(1) -> 1;
flag(false) -> 0;
flag(true) -> 1;
flag(V) when is_binary(V) orelse is_list(V) -> 1;
flag(empty) -> 1; %% for test purposes
flag(_) -> 0.

msg_id(undefined) -> <<>>;
msg_id(MsgId) -> <<MsgId:16/big>>.

default(undefined, Default) -> Default;
default(Val, _) -> Val.

utf8(<<>>) -> <<>>;
utf8(undefined) -> <<>>;
utf8(empty) -> <<0:16/big>>; %% for test purposes, useful if you want to encode an empty string..
utf8(IoList) when is_list(IoList) ->
    [<<(iolist_size(IoList)):16/big>>, IoList];
utf8(Bin) when is_binary(Bin) ->
    <<(byte_size(Bin)):16/big, Bin/binary>>.

ensure_binary(L) when is_list(L) -> list_to_binary(L);
ensure_binary(B) when is_binary(B) -> B;
ensure_binary(undefined) -> undefined;
ensure_binary(empty) -> empty. % for test purposes
%%%%%%% packet generator functions (useful for testing)
gen_connect(ClientId, Opts) ->
    Frame = #mqtt_connect{
              client_id = ensure_binary(ClientId),
              clean_session =  proplists:get_value(clean_session, Opts, true),
              keep_alive =     proplists:get_value(keepalive, Opts, 60),
              username =       ensure_binary(proplists:get_value(username, Opts)),
              password =       ensure_binary(proplists:get_value(password, Opts)),
              proto_ver =      proplists:get_value(proto_ver, Opts, 3),
              will_topic =     ensure_binary(proplists:get_value(will_topic, Opts)),
              will_qos =       proplists:get_value(will_qos, Opts, 0),
              will_retain =    proplists:get_value(will_retain, Opts, false),
              will_msg =       ensure_binary(proplists:get_value(will_msg, Opts))
              },
    iolist_to_binary(serialise(Frame)).

gen_connack() ->
    gen_connack(?CONNACK_ACCEPT).
gen_connack(RC) ->
    gen_connack(0, RC).
gen_connack(SP, RC) ->
    iolist_to_binary(serialise(#mqtt_connack{session_present=flag(SP), return_code=RC})).

gen_publish(Topic, Qos, Payload, Opts) ->
    Frame = #mqtt_publish{
               dup =               proplists:get_value(dup, Opts, false),
               qos =               Qos,
               retain =            proplists:get_value(retain, Opts, false),
               topic =             ensure_binary(Topic),
               message_id =        proplists:get_value(mid, Opts, 0),
               payload =           ensure_binary(Payload)
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
    iolist_to_binary(serialise(#mqtt_subscribe{topics=[{ensure_binary(Topic), Qos}], message_id=MId})).

gen_suback(MId, Qos) ->
    iolist_to_binary(serialise(#mqtt_suback{qos_table=[Qos], message_id=MId})).

gen_unsubscribe(MId, Topic) ->
    iolist_to_binary(serialise(#mqtt_unsubscribe{topics=[ensure_binary(Topic)], message_id=MId})).

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
    compare_frame("connackSessionPresentTrue", gen_connack(1, 0)),
    compare_frame("connackSessionPresentFalse", gen_connack(0, 0)),
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
