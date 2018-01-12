-module(vmq_parser_mqtt5).

-export([parse/1, parse/2, serialise/1]).

-export([gen_connect/2
        %% ,
        %%  gen_connack/0,
        %%  gen_connack/1,
        %%  gen_connack/2,
        %%  gen_publish/4,
        %%  gen_puback/1,
        %%  gen_pubrec/1,
        %%  gen_pubrel/1,
        %%  gen_pubcomp/1,
        %%  gen_subscribe/2,
        %%  gen_subscribe/3,
        %%  gen_suback/2,
        %%  gen_unsubscribe/2,
        %%  gen_unsuback/1,
        %%  gen_pingreq/0,
        %%  gen_pingresp/0,
        %%  gen_disconnect/0
        ]).

-include("vmq_types_mqtt5.hrl").
-include("vmq_parser_mqtt5.hrl").

-spec parse(binary()) -> {mqtt5_frame(), binary()} |
                         {error, any()} |
                         more.
parse(Data) ->
    parse(Data, ?MAX_PACKET_SIZE).

-spec parse(binary(), non_neg_integer()) ->  {mqtt5_frame(), binary()} | {error, atom()} | more.
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


%% -spec variable(binary(), binary()) -> mqtt5_frame() | {error, atom()}.
%% variable(<<?PUBLISH:4, Dup:1, 0:2, Retain:1>>, <<TopicLen:16/big, Topic:TopicLen/binary, Payload/binary>>) ->
%%     case vmq_topic:validate_topic(publish, Topic) of
%%         {ok, ParsedTopic} ->
%%             #mqtt5_publish{dup=Dup,
%%                           retain=Retain,
%%                           topic=ParsedTopic,
%%                           qos=0,
%%                           payload=Payload};
%%         {error, Reason} ->
%%             {error, Reason}
%%     end;
%% variable(<<?PUBLISH:4, Dup:1, QoS:2, Retain:1>>, <<TopicLen:16/big, Topic:TopicLen/binary, MessageId:16/big, Payload/binary>>)
%%   when QoS < 3 ->
%%     case vmq_topic:validate_topic(publish, Topic) of
%%         {ok, ParsedTopic} ->
%%             #mqtt5_publish{dup=Dup,
%%                           retain=Retain,
%%                           topic=ParsedTopic,
%%                           qos=QoS,
%%                           message_id=MessageId,
%%                           payload=Payload};
%%         {error, Reason} ->
%%             {error, Reason}
%%     end;
%% variable(<<?PUBACK:4, 0:4>>, <<MessageId:16/big>>) ->
%%     #mqtt5_puback{message_id=MessageId};
%% variable(<<?PUBREC:4, 0:4>>, <<MessageId:16/big>>) ->
%%     #mqtt5_pubrec{message_id=MessageId};
%% variable(<<?PUBREL:4, 0:2, 1:1, 0:1>>, <<MessageId:16/big>>) ->
%%     #mqtt5_pubrel{message_id=MessageId};
%% variable(<<?PUBCOMP:4, 0:4>>, <<MessageId:16/big>>) ->
%%     #mqtt5_pubcomp{message_id=MessageId};
%% variable(<<?SUBSCRIBE:4, 0:2, 1:1, 0:1>>, <<MessageId:16/big, Topics/binary>>) ->
%%     case parse_topics(Topics, ?SUBSCRIBE, []) of
%%         {ok, ParsedTopics} ->
%%             #mqtt5_subscribe{topics=ParsedTopics,
%%                             message_id=MessageId};
%%         E -> E
%%     end;
%% variable(<<?UNSUBSCRIBE:4, 0:2, 1:1, 0:1>>, <<MessageId:16/big, Topics/binary>>) ->
%%     case parse_topics(Topics, ?UNSUBSCRIBE, []) of
%%         {ok, ParsedTopics} ->
%%             #mqtt5_unsubscribe{topics=ParsedTopics,
%%                               message_id=MessageId};
%%         E ->
%%             E
%%     end;
%% variable(<<?SUBACK:4, 0:4>>, <<MessageId:16/big, Acks/binary>>) ->
%%     #mqtt5_suback{qos_table=parse_acks(Acks, []),
%%                  message_id=MessageId};
%% variable(<<?UNSUBACK:4, 0:4>>, <<MessageId:16/big>>) ->
%%     #mqtt5_unsuback{message_id=MessageId};
%% variable(<<?CONNECT:4, 0:4>>, <<L:16/big, PMagic:L/binary, _/binary>>)
%%   when not ((PMagic == ?PROTOCOL_MAGIC_5) or (PMagic == ?PROTOCOL_MAGIC_311) or
%%              (PMagic == ?PROTOCOL_MAGIC_31)) ->
%%     {error, unknown_protocol_magic};


variable(<<?CONNECT:4, 0:4>>, <<4:16/big, "MQTT", ?PROTOCOL_5:8,
                                Flags:6/bitstring, CleanStart:1, 0:1, %% bit 0 is reserved
                                KeepAlive: 16/big, Rest0/binary>>) ->
    Conn0 = #mqtt5_connect{proto_ver=?PROTOCOL_5,
                           clean_start=CleanStart == 1,
                           keep_alive=KeepAlive},

    %% The properties are the last element of the variable header.
    case parse_properties(Rest0) of
        {ok, Properties, Rest1} ->
            %% Parse the payload.
            case Rest1 of
                <<ClientIdLen:16/big, ClientId:ClientIdLen/binary, Rest2/binary>> ->
                    Conn1 = Conn0#mqtt5_connect{properties = Properties,
                                                client_id = ClientId},
                    parse_will_properties(Rest2, Flags, Conn1);
                _ ->
                    {error, cant_parse_client_id}
            end;
        E -> E
    end;

%% variable(<<?CONNACK:4, 0:4>>, <<0:7, SP:1, ReturnCode:8/big>>) ->
%%     #mqtt5_connack{session_present=SP, return_code=ReturnCode};
%% variable(<<?PINGREQ:4, 0:4>>, <<>>) ->
%%     #mqtt5_pingreq{};
%% variable(<<?PINGRESP:4, 0:4>>, <<>>) ->
%%     #mqtt5_pingresp{};
%% variable(<<?DISCONNECT:4, 0:4>>, <<>>) ->
%%     #mqtt5_disconnect{};
variable(_, _) ->
    %% Covers: MQTT5-3.1.2.3
    {error,  cant_parse_varint}.

parse_will_properties(Rest0, Flags, Conn0) ->
    case parse_properties(Rest0) of
        {ok, WillProperties, Rest1} ->
            Conn1 = Conn0#mqtt5_connect{will_properties = WillProperties},
            parse_last_will_topic(Rest1, Flags, Conn1);
        E -> E
    end.

parse_last_will_topic(Rest, <<_:2, 0:4>> = Flags, Conn) ->
    %% All will flags are zero, no last will.
    parse_username(Rest, Flags, Conn);
parse_last_will_topic(<<WillTopicLen:16/big, WillTopic:WillTopicLen/binary,
                        WillMsgLen:16/big, WillMsg:WillMsgLen/binary,
                        Rest/binary>>,
                      <<_:2, Retain:1, QoS:2, 1:1>> = Flags,
                      Conn) ->
    case vmq_topic:validate_topic(publish, WillTopic) of
        {ok, ParsedTopic} ->
            Conn1 = Conn#mqtt5_connect{will_msg=WillMsg,
                                       will_topic=ParsedTopic,
                                       will_retain=Retain == 1,
                                       will_qos=QoS},
            parse_username(Rest, Flags, Conn1);
        _ ->
            %% FIXME: return correct error here
            {error, cant_validate_last_will_topic}
    end;
parse_last_will_topic(_, _, _) ->
    %% FIXME: return correct error here
    {error, cant_parse_last_will}.

parse_username(Rest, <<0:1, _:5>> = Flags, Conn) ->
    %% Username bit is zero, no username.
    parse_password(Rest, Flags, Conn);
parse_username(<<Len:16/big, UserName:Len/binary, Rest/binary>>, <<1:1, _:5>> = Flags, Conn) ->
    parse_password(Rest, Flags, Conn#mqtt5_connect{username=UserName});
parse_username(_, _, _) ->
    %% FIXME: return correct error here
    {error, cant_parse_username}.

parse_password(<<>>, <<_:1, 0:1, _:4>>, Conn) ->
    Conn;
parse_password(<<Len:16/big, Password:Len/binary>>, <<_:1, 1:1, _:4>>, Conn) ->
    Conn#mqtt5_connect{password=Password};
parse_password(_, _, _) ->
    %% FIXME: return correct error here
    {error, cant_parse_password}.


%% parse_topics(<<>>, _, []) -> {error, no_topic_provided};
%% parse_topics(<<>>, _, Topics) -> {ok, Topics};
%% parse_topics(<<L:16/big, Topic:L/binary, 0:6, QoS:2, Rest/binary>>, ?SUBSCRIBE = Sub, Acc)
%%   when (QoS >= 0) and (QoS < 3) ->
%%     case vmq_topic:validate_topic(subscribe, Topic) of
%%         {ok, ParsedTopic} ->
%%             parse_topics(Rest, Sub, [{ParsedTopic, QoS}|Acc]);
%%         E -> E
%%     end;
%% parse_topics(<<L:16/big, Topic:L/binary, Rest/binary>>, ?UNSUBSCRIBE = Sub, Acc) ->
%%     case vmq_topic:validate_topic(subscribe, Topic) of
%%         {ok, ParsedTopic} ->
%%             parse_topics(Rest, Sub, [ParsedTopic|Acc]);
%%         E -> E
%%     end;
%% parse_topics(_, _, _) -> {error, cant_parse_topics}.

%% parse_acks(<<>>, Acks) ->
%%     Acks;
%% parse_acks(<<128:8, Rest/binary>>, Acks) ->
%%     parse_acks(Rest, [not_allowed | Acks]);
%% parse_acks(<<_:6, QoS:2, Rest/binary>>, Acks)
%%   when is_integer(QoS) and ((QoS >= 0) and (QoS =< 2)) ->
%%     parse_acks(Rest, [QoS | Acks]).

-spec serialise(mqtt5_frame()) -> binary() | iolist().
%% serialise(#mqtt5_publish{qos=0,
%%                         topic=Topic,
%%                         retain=Retain,
%%                         dup=Dup,
%%                         payload=Payload}) ->
%%     Var = [utf8(vmq_topic:unword(Topic)), Payload],
%%     LenBytes = serialise_len(iolist_size(Var)),
%%     [<<?PUBLISH:4, (flag(Dup)):1/integer, 0:2/integer, (flag(Retain)):1/integer>>, LenBytes, Var];
%% serialise(#mqtt5_publish{message_id=MessageId,
%%                         topic=Topic,
%%                         qos=QoS,
%%                         retain=Retain,
%%                         dup=Dup,
%%                         payload=Payload}) ->
%%     Var = [utf8(vmq_topic:unword(Topic)), msg_id(MessageId), Payload],
%%     LenBytes = serialise_len(iolist_size(Var)),
%%     [<<?PUBLISH:4, (flag(Dup)):1/integer,
%%        (default(QoS, 0)):2/integer, (flag(Retain)):1/integer>>, LenBytes, Var];
%% serialise(#mqtt5_puback{message_id=MessageId}) ->
%%     <<?PUBACK:4, 0:4, 2, MessageId:16/big>>;
%% serialise(#mqtt5_pubrel{message_id=MessageId}) ->
%%     <<?PUBREL:4, 0:2, 1:1, 0:1, 2, MessageId:16/big>>;
%% serialise(#mqtt5_pubrec{message_id=MessageId}) ->
%%     <<?PUBREC:4, 0:4, 2, MessageId:16/big>>;
%% serialise(#mqtt5_pubcomp{message_id=MessageId}) ->
%%     <<?PUBCOMP:4, 0:4, 2, MessageId:16/big>>;
serialise(#mqtt5_connect{proto_ver=ProtoVersion,
                         username=UserName,
                         password=Password,
                         will_retain=WillRetain,
                         will_qos=WillQos,
                         clean_start=CleanSession,
                         keep_alive=KeepAlive,
                         client_id=ClientId,
                         will_properties=WillProperties,
                         will_topic=WillTopic,
                         will_msg=WillMsg,
                         properties=Properties}) ->
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
           properties(Properties),
           utf8(ClientId),
           properties(WillProperties),
           utf8(vmq_topic:unword(WillTopic)),
           utf8(WillMsg),
           utf8(UserName),
           utf8(Password)],
    LenBytes = serialise_len(iolist_size(Var)),
    [<<?CONNECT:4, 0:4>>, LenBytes, Var].
%% serialise(#mqtt5_connack{session_present=SP, return_code=RC}) ->
%%     [<<?CONNACK:4, 0:4>>, serialise_len(2), <<0:7, (flag(SP)):1/integer>>, <<RC:8/big>>];
%% serialise(#mqtt5_subscribe{message_id=MessageId, topics=Topics}) ->
%%     SerialisedTopics = serialise_topics(?SUBSCRIBE, Topics, []),
%%     LenBytes = serialise_len(iolist_size(SerialisedTopics) + 2),
%%     [<<?SUBSCRIBE:4, 0:2, 1:1, 0:1>>, LenBytes, <<MessageId:16/big>>, SerialisedTopics];
%% serialise(#mqtt5_suback{message_id=MessageId, qos_table=QosTable}) ->
%%     SerialisedAcks = serialise_acks(QosTable, []),
%%     LenBytes = serialise_len(iolist_size(SerialisedAcks) + 2),
%%     [<<?SUBACK:4, 0:4>>, LenBytes, <<MessageId:16/big>>, SerialisedAcks];
%% serialise(#mqtt5_unsubscribe{message_id=MessageId, topics=Topics}) ->
%%     SerialisedTopics = serialise_topics(?UNSUBSCRIBE, Topics, []),
%%     LenBytes = serialise_len(iolist_size(SerialisedTopics) + 2),
%%     [<<?UNSUBSCRIBE:4, 0:2, 1:1, 0:1>>, LenBytes, <<MessageId:16/big>>,
%%      SerialisedTopics];
%% serialise(#mqtt5_unsuback{message_id=MessageId}) ->
%%     <<?UNSUBACK:4, 0:4, 2, MessageId:16/big>>;
%% serialise(#mqtt5_pingreq{}) ->
%%     <<?PINGREQ:4, 0:4, 0>>;
%% serialise(#mqtt5_pingresp{}) ->
%%     <<?PINGRESP:4, 0:4, 0>>;
%% serialise(#mqtt5_disconnect{}) ->
%%     <<?DISCONNECT:4, 0:4, 0>>.

serialise_len(N) when N =< ?LOWBITS ->
    <<0:1, N:7>>;
serialise_len(N) ->
    <<1:1, (N rem ?HIGHBIT):7, (serialise_len(N div ?HIGHBIT))/binary>>.

%% serialise_topics(?SUBSCRIBE = Sub, [{Topic, QoS}|Rest], Acc) ->
%%     serialise_topics(Sub, Rest, [utf8(vmq_topic:unword(Topic)), <<0:6, QoS:2>>|Acc]);
%% serialise_topics(?UNSUBSCRIBE = Sub, [Topic|Rest], Acc) ->
%%     serialise_topics(Sub, Rest, [utf8(vmq_topic:unword(Topic))|Acc]);
%% serialise_topics(_, [], Topics) ->
%%     Topics.

%% serialise_acks([QoS|Rest], Acks) when is_integer(QoS) and ((QoS >= 0) and (QoS =< 2)) ->
%%     serialise_acks(Rest, [<<0:6, QoS:2>>|Acks]);
%% serialise_acks([_|Rest], Acks) ->
%%     %% use 0x80 failure code for everything else
%%     serialise_acks(Rest, [<<128:8>>|Acks]);
%% serialise_acks([], Acks) ->
%%     Acks.

proto(5) -> {4, ?PROTOCOL_MAGIC_5};
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

properties(undefined) ->
    <<0:8>>;
properties([]) ->
    <<0:8>>.

%%%%%%% packet generator functions (useful for testing)
gen_connect(ClientId, Opts) ->
    Frame = #mqtt5_connect{
               client_id =      ensure_binary(ClientId),
               clean_start =    proplists:get_value(clean_session, Opts, true),
               keep_alive =     proplists:get_value(keepalive, Opts, 60),
               username =       ensure_binary(proplists:get_value(username, Opts)),
               password =       ensure_binary(proplists:get_value(password, Opts)),
               proto_ver =      5,
               will_topic =     ensure_binary(proplists:get_value(will_topic, Opts)),
               will_qos =       proplists:get_value(will_qos, Opts, 0),
               will_retain =    proplists:get_value(will_retain, Opts, false),
               will_msg =       ensure_binary(proplists:get_value(will_msg, Opts)),
               properties =     proplists:get_value(properties, Opts)
              },
    iolist_to_binary(serialise(Frame)).

%% gen_connack() ->
%%     gen_connack(?CONNACK_ACCEPT).
%% gen_connack(RC) ->
%%     gen_connack(0, RC).
%% gen_connack(SP, RC) ->
%%     iolist_to_binary(serialise(#mqtt5_connack{session_present=flag(SP), return_code=RC})).

%% gen_publish(Topic, Qos, Payload, Opts) ->
%%     Frame = #mqtt5_publish{
%%                dup =               proplists:get_value(dup, Opts, false),
%%                qos =               Qos,
%%                retain =            proplists:get_value(retain, Opts, false),
%%                topic =             ensure_binary(Topic),
%%                message_id =        proplists:get_value(mid, Opts, 0),
%%                payload =           ensure_binary(Payload)
%%               },
%%     iolist_to_binary(serialise(Frame)).

%% gen_puback(MId) ->
%%     iolist_to_binary(serialise(#mqtt5_puback{message_id=MId})).

%% gen_pubrec(MId) ->
%%     iolist_to_binary(serialise(#mqtt5_pubrec{message_id=MId})).

%% gen_pubrel(MId) ->
%%     iolist_to_binary(serialise(#mqtt5_pubrel{message_id=MId})).

%% gen_pubcomp(MId) ->
%%     iolist_to_binary(serialise(#mqtt5_pubcomp{message_id=MId})).

%% gen_subscribe(MId, [{_, _}|_] = Topics) ->
%%     BinTopics = [{ensure_binary(Topic), QoS} || {Topic, QoS} <- Topics],
%%     iolist_to_binary(serialise(#mqtt5_subscribe{topics=BinTopics, message_id=MId})).
%% gen_subscribe(MId, Topic, QoS) ->
%%     gen_subscribe(MId, [{Topic, QoS}]).

%% gen_suback(MId, QoSs) when is_list(QoSs) ->
%%     iolist_to_binary(serialise(#mqtt5_suback{qos_table=QoSs, message_id=MId}));
%% gen_suback(MId, QoS) ->
%%     gen_suback(MId, [QoS]).

%% gen_unsubscribe(MId, Topic) ->
%%     iolist_to_binary(serialise(#mqtt5_unsubscribe{topics=[ensure_binary(Topic)], message_id=MId})).

%% gen_unsuback(MId) ->
%%     iolist_to_binary(serialise(#mqtt5_unsuback{message_id=MId})).

%% gen_pingreq() ->
%%     iolist_to_binary(serialise(#mqtt5_pingreq{})).

%% gen_pingresp() ->
%%     iolist_to_binary(serialise(#mqtt5_pingresp{})).

%% gen_disconnect() ->
%%     iolist_to_binary(serialise(#mqtt5_disconnect{})).


-spec parse_properties(binary()) -> {ok, [mqtt5_property()], binary} |
                                    {error, any()}.
parse_properties(Data) ->
    case parse_varint(Data) of
        {PropertiesData, Rest} ->
            case parse_properties(PropertiesData, []) of
                Properties when is_list(Properties) ->
                    {ok, Properties, Rest};
                {error, _} = E ->
                    E
            end;
        {error, _} -> {error, cant_parse_properties}
    end.

-spec parse_properties(binary(), [mqtt5_property()])
    -> [mqtt5_property()] |
       {error, any()}.
parse_properties(<<>>, Acc) ->
    lists:reverse(Acc);
%% Note, the property ids are specified as a varint, but in MQTT5 all
%% indicator ids fit within one byte, so we parse it as such to keep
%% things simple.
parse_properties(<<?M5P_PAYLOAD_FORMAT_INDICATOR:8, Val:8, Rest/binary>>, Acc) ->
    case Val of
        0 ->
            P = #p_payload_format_indicator{value = unspecified},
            parse_properties(Rest, [P|Acc]);
        1 ->
            P = #p_payload_format_indicator{value = utf8},
            parse_properties(Rest, [P|Acc]);
        _ ->
            {error, cant_parse_properties}
    end;
parse_properties(<<?M5P_MESSAGE_EXPIRY_INTERVAL:8, Val:32/big, Rest/binary>>, Acc) ->
    P = #p_message_expiry_interval{value = Val},
    parse_properties(Rest, [P|Acc]);
parse_properties(<<?M5P_CONTENT_TYPE:8, Len:16/big, Val:Len/binary, Rest/binary>>, Acc) ->
    P = #p_content_type{value = Val},
    parse_properties(Rest, [P|Acc]);
parse_properties(<<?M5P_RESPONSE_TOPIC:8, Len:16/big, Val:Len/binary, Rest/binary>>, Acc) ->
    P = #p_response_topic{value = Val},
    parse_properties(Rest, [P|Acc]);
parse_properties(<<?M5P_CORRELATION_DATA:8, Len:16/big, Val:Len/binary, Rest/binary>>, Acc) ->
    P = #p_correlation_data{value = Val},
    parse_properties(Rest, [P|Acc]);
parse_properties(_, _) ->
    {error, cant_parse_properties}.


parse_varint(<<0:1, DataSize:7, Data:DataSize/binary, Rest/binary>>) ->
    {Data, Rest};
parse_varint(<<1:1, L1:7, 0:1, L2:7, Rest/binary>>) ->
    Len = L1 + (L2 bsl 7),
    parse_varint(Len, Rest);
parse_varint(<<1:1, L1:7, 1:1, L2:7, 0:1, L3:7, Rest/binary>>) ->
    Len = L1 + (L2 bsl 7) + (L3 bsl 14),
    parse_varint(Len, Rest);
parse_varint(<<1:1, L1:7, 1:1, L2:7, 1:1, L3:7, 0:1, L4:7, Rest/binary>>) ->
    Len = L1 + (L2 bsl 7) + (L3 bsl 14) + (L4 bsl 21),
    parse_varint(Len, Rest);
parse_varint(_) ->
    error.

parse_varint(Len, Data) when byte_size(Data) >= Len ->
    <<Var:Len/binary, Rest/binary>> = Data,
    {Var, Rest}.

-spec reason_code(reason_type()) -> reason_code().
reason_code(granted_qos0)                   -> ?M5_GRANTED_QOS0;
reason_code(granted_qos1)                   -> ?M5_GRANTED_QOS1;
reason_code(granted_qos2)                   -> ?M5_GRANTED_QOS2;
reason_code(disconnect_with_will_msg)       -> ?M5_DISCONNECT_WITH_WILL_MSG;
reason_code(no_matching_subscribers)        -> ?M5_NO_MATCHING_SUBSCRIBERS;
reason_code(no_subscription_existed)        -> ?M5_NO_SUBSCRIPTION_EXISTED;
reason_code(continue_authentication)        -> ?M5_CONTINUE_AUTHENTICATION;
reason_code(reauthenticate)                 -> ?M5_REAUTHENTICATE;
reason_code(unspecified_error)              -> ?M5_UNSPECIFIED_ERROR;
reason_code(malformed_packet)               -> ?M5_MALFORMED_PACKET;
reason_code(protocol_error)                 -> ?M5_PROTOCOL_ERROR;
reason_code(impl_specific_error)            -> ?M5_IMPL_SPECIFIC_ERROR;
reason_code(unsupported_protocol_VERSION)   -> ?M5_UNSUPPORTED_PROTOCOL_VERSION;
reason_code(client_identifier_not_valid)    -> ?M5_CLIENT_IDENTIFIER_NOT_VALID;
reason_code(bad_username_or_password)       -> ?M5_BAD_USERNAME_OR_PASSWORD;
reason_code(not_authorized)                 -> ?M5_NOT_AUTHORIZED;
reason_code(server_unavailable)             -> ?M5_SERVER_UNAVAILABLE;
reason_code(server_busy)                    -> ?M5_SERVER_BUSY;
reason_code(banned)                         -> ?M5_BANNED;
reason_code(server_shutting_down)           -> ?M5_SERVER_SHUTTING_DOWN;
reason_code(bad_authentication_method)      -> ?M5_BAD_AUTHENTICATION_METHOD;
reason_code(keep_alive_timeout)             -> ?M5_KEEP_ALIVE_TIMEOUT;
reason_code(session_taken_over)             -> ?M5_SESSION_TAKEN_OVER;
reason_code(topic_filter_invalid)           -> ?M5_TOPIC_FILTER_INVALID;
reason_code(topic_name_invalid)             -> ?M5_TOPIC_NAME_INVALID;
reason_code(packet_id_in_use)               -> ?M5_PACKET_ID_IN_USE;
reason_code(packet_id_not_found)            -> ?M5_PACKET_ID_NOT_FOUND;
reason_code(receive_max_exceeded)           -> ?M5_RECEIVE_MAX_EXCEEDED;
reason_code(topic_alias_invalid)            -> ?M5_TOPIC_ALIAS_INVALID;
reason_code(packet_too_large)               -> ?M5_PACKET_TOO_LARGE;
reason_code(message_rate_too_high)          -> ?M5_MESSAGE_RATE_TOO_HIGH;
reason_code(quota_exceeded)                 -> ?M5_QUOTA_EXCEEDED;
reason_code(administrative_action)          -> ?M5_ADMINISTRATIVE_ACTION;
reason_code(payload_format_invalid)         -> ?M5_PAYLOAD_FORMAT_INVALID;
reason_code(retain_not_supported)           -> ?M5_RETAIN_NOT_SUPPORTED;
reason_code(qos_not_supported)              -> ?M5_QOS_NOT_SUPPORTED;
reason_code(use_another_server)             -> ?M5_USE_ANOTHER_SERVER;
reason_code(server_moved)                   -> ?M5_SERVER_MOVED;
reason_code(shared_subs_not_supported)      -> ?M5_SHARED_SUBS_NOT_SUPPORTED;
reason_code(connection_rate_exceeded)       -> ?M5_CONNECTION_RATE_EXCEEDED;
reason_code(max_connect_time)               -> ?M5_MAX_CONNECT_TIME;
reason_code(subscription_ids_not_supported) -> ?M5_SUBSCRIPTION_IDS_NOT_SUPPORTED;
reason_code(wildcard_subs_not_supported)    -> ?M5_WILDCARD_SUBS_NOT_SUPPORTED.
                                                  
