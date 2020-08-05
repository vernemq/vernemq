-module(vmq_parser).
-include("vmq_types.hrl").
-export([parse/1, parse/2, serialise/1]).

-dialyzer({no_match, utf8/1}).

-export([gen_connect/2,
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

-define(MAX_PACKET_SIZE, 268435455).

-spec parse(binary()) -> {mqtt_frame(), binary()} | {error, atom()} | {{error, atom()}, any()} |more.
parse(Data) ->
    parse(Data, ?MAX_PACKET_SIZE).

-spec parse(binary(), non_neg_integer()) ->  {mqtt_frame(), binary()} | {error, atom()} | more.
parse(<<Fixed:1/binary, 0:1, DataSize:7, Data/binary>>, MaxSize)->
    parse(DataSize, MaxSize, Fixed, Data);
parse(<<Fixed:1/binary, 1:1, L1:7, 0:1, L2:7, Data/binary>>, MaxSize) ->
    parse(L1 + (L2 bsl 7), MaxSize, Fixed, Data);
parse(<<Fixed:1/binary, 1:1, L1:7, 1:1, L2:7, 0:1, L3:7, Data/binary>>, MaxSize) ->
    parse(L1 + (L2 bsl 7) + (L3 bsl 14), MaxSize, Fixed, Data);
parse(<<Fixed:1/binary, 1:1, L1:7, 1:1, L2:7, 1:1, L3:7, 0:1, L4:7, Data/binary>>, MaxSize) ->
    parse(L1 + (L2 bsl 7) + (L3 bsl 14) + (L4 bsl 21), MaxSize, Fixed, Data);
parse(<<_:8/binary, _/binary>>, _) ->
    {error, cant_parse_fixed_header};
parse(_, _) ->
    more.

parse(DataSize, 0, Fixed, Data) when byte_size(Data) >= DataSize ->
    %% no max size limit
    <<Var:DataSize/binary, Rest/binary>> = Data,
    {variable(Fixed, Var), Rest};
parse(DataSize, 0, _Fixed, Data) when byte_size(Data) < DataSize ->
    more;
parse(DataSize, MaxSize, Fixed, Data)
  when byte_size(Data) >= DataSize,
       byte_size(Data) =< MaxSize ->
    <<Var:DataSize/binary, Rest/binary>> = Data,
    {variable(Fixed, Var), Rest};
parse(DataSize, MaxSize, _, _)
  when DataSize > MaxSize ->
    {error, packet_exceeds_max_size};
parse(_, _, _, _) -> more.


-spec variable(binary(), binary()) -> mqtt_frame() | {error, atom()}.
variable(<<?PUBLISH:4, Dup:1, 0:2, Retain:1>>, <<TopicLen:16/big, Topic:TopicLen/binary, Payload/binary>>) ->
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
variable(<<?PUBLISH:4, Dup:1, QoS:2, Retain:1>>, <<TopicLen:16/big, Topic:TopicLen/binary, MessageId:16/big, Payload/binary>>)
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
variable(<<?PUBACK:4, 0:4>>, <<MessageId:16/big>>) ->
    #mqtt_puback{message_id=MessageId};
variable(<<?PUBREC:4, 0:4>>, <<MessageId:16/big>>) ->
    #mqtt_pubrec{message_id=MessageId};
variable(<<?PUBREL:4, 0:2, 1:1, 0:1>>, <<MessageId:16/big>>) ->
    #mqtt_pubrel{message_id=MessageId};
variable(<<?PUBCOMP:4, 0:4>>, <<MessageId:16/big>>) ->
    #mqtt_pubcomp{message_id=MessageId};
variable(<<?SUBSCRIBE:4, 0:2, 1:1, 0:1>>, <<MessageId:16/big, Topics/binary>>) ->
    case parse_topics(Topics, ?SUBSCRIBE, []) of
        {ok, ParsedTopics} ->
            #mqtt_subscribe{topics=ParsedTopics,
                            message_id=MessageId};
        E -> E
    end;
variable(<<?UNSUBSCRIBE:4, 0:2, 1:1, 0:1>>, <<MessageId:16/big, Topics/binary>>) ->
    case parse_topics(Topics, ?UNSUBSCRIBE, []) of
        {ok, ParsedTopics} ->
            #mqtt_unsubscribe{topics=ParsedTopics,
                              message_id=MessageId};
        E ->
            E
    end;
variable(<<?SUBACK:4, 0:4>>, <<MessageId:16/big, Acks/binary>>) ->
    #mqtt_suback{qos_table=parse_acks(Acks, []),
                 message_id=MessageId};
variable(<<?UNSUBACK:4, 0:4>>, <<MessageId:16/big>>) ->
    #mqtt_unsuback{message_id=MessageId};
variable(<<?CONNECT:4, 0:4>>, <<L:16/big, PMagic:L/binary, _/binary>>)
  when not ((PMagic == ?PROTOCOL_MAGIC_311) or
             (PMagic == ?PROTOCOL_MAGIC_31)) ->
    {error, unknown_protocol_magic};
variable(<<?CONNECT:4, 0:4>>,
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
                        {ok, <<>>, Conn3} ->
                            Conn3;
                        {ok, _, _} ->
                            {error, invalid_rest_of_binary};
                        E -> E
                    end;
                E -> E
            end;
        E -> E
    end;
variable(<<?CONNACK:4, 0:4>>, <<0:7, SP:1, ReturnCode:8/big>>) ->
    #mqtt_connack{session_present=SP, return_code=ReturnCode};
variable(<<?PINGREQ:4, 0:4>>, <<>>) ->
    #mqtt_pingreq{};
variable(<<?PINGRESP:4, 0:4>>, <<>>) ->
    #mqtt_pingresp{};
variable(<<?DISCONNECT:4, 0:4>>, <<>>) ->
    #mqtt_disconnect{};
variable(_, _) -> {error, cant_parse_variable_header}.

parse_last_will_topic(Rest, 0, 0, 0, Conn) -> {ok, Rest, Conn};
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
parse_last_will_topic(_, _, _, _,_) ->
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
parse_acks(<<128:8, Rest/binary>>, Acks) ->
    parse_acks(Rest, [not_allowed | Acks]);
parse_acks(<<_:6, QoS:2, Rest/binary>>, Acks)
  when is_integer(QoS) and ((QoS >= 0) and (QoS =< 2)) ->
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

serialise_topics(?SUBSCRIBE = Sub, [{Topic, QoS}|Rest], Acc) ->
    serialise_topics(Sub, Rest, [utf8(vmq_topic:unword(Topic)), <<0:6, QoS:2>>|Acc]);
serialise_topics(?UNSUBSCRIBE = Sub, [Topic|Rest], Acc) ->
    serialise_topics(Sub, Rest, [utf8(vmq_topic:unword(Topic))|Acc]);
serialise_topics(_, [], Topics) ->
    Topics.

serialise_acks([QoS|Rest], Acks) when is_integer(QoS) and ((QoS >= 0) and (QoS =< 2)) ->
    serialise_acks(Rest, [<<0:6, QoS:2>>|Acks]);
serialise_acks([_|Rest], Acks) ->
    %% use 0x80 failure code for everything else
    serialise_acks(Rest, [<<128:8>>|Acks]);
serialise_acks([], Acks) ->
    Acks.

proto(4) -> {4, ?PROTOCOL_MAGIC_311};
proto(3) -> {6, ?PROTOCOL_MAGIC_31};
proto(131) -> {6, ?PROTOCOL_MAGIC_31};
proto(132) -> {4, ?PROTOCOL_MAGIC_311}.

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

gen_subscribe(MId, [{_, _}|_] = Topics) ->
    BinTopics = [{ensure_binary(Topic), QoS} || {Topic, QoS} <- Topics],
    iolist_to_binary(serialise(#mqtt_subscribe{topics=BinTopics, message_id=MId})).
gen_subscribe(MId, Topic, QoS) ->
    gen_subscribe(MId, [{Topic, QoS}]).

gen_suback(MId, QoSs) when is_list(QoSs) ->
    iolist_to_binary(serialise(#mqtt_suback{qos_table=QoSs, message_id=MId}));
gen_suback(MId, QoS) ->
    gen_suback(MId, [QoS]).

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
