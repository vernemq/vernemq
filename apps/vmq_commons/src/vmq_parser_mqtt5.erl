-module(vmq_parser_mqtt5).

-include_lib("vernemq_dev/include/vernemq_dev.hrl").
-include("vmq_parser_mqtt5.hrl").

-dialyzer({no_match, utf8/1}).

-export([parse/1, parse/2, serialise/1]).

-export([gen_connect/2,
         gen_connack/3,
         gen_publish/4,
         gen_puback/3,
         gen_pubrec/3,
         gen_pubrel/3,
         gen_pubcomp/3,
         gen_subscribe/3,
         gen_suback/3,
         gen_unsubscribe/3,
         gen_unsuback/3,
         gen_pingreq/0,
         gen_pingresp/0,
         gen_disconnect/2,
         gen_auth/2,

         rcn2rc/1,
         rc2rcn/1
        ]).

-type serialized() :: binary() | iolist().
-export_type([serialized/0]).

%% exported for testing
-export([parse_properties/2,
         enc_properties/1]).

-spec parse(binary()) -> {mqtt5_frame(), binary()} |
                         {error, any()} |
                         more.
parse(Data) ->
    parse(Data, ?MAX_PACKET_SIZE).

-spec parse(binary(), non_neg_integer()) ->  {mqtt5_frame(), binary()} | {error, atom()} | {{error, atom()}, any()} | more.
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

-spec variable(binary(), binary()) -> mqtt5_frame() | {error, atom()}.
variable(<<?PUBLISH:4, Dup:1, 0:2, Retain:1>>,
         <<TopicLen:16/big, Topic:TopicLen/binary, Rest/binary>>) ->
    %% QoS 0
    case validate_publish_topic(Topic) of
        {ok, ParsedTopic} ->
            case parse_properties(Rest) of
                {ok, Properties, Payload} ->
                    #mqtt5_publish{topic=ParsedTopic,
                                   qos=0,
                                   retain=Retain,
                                   dup=Dup,
                                   properties=Properties,
                                   payload=Payload};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end;
variable(<<?PUBLISH:4, Dup:1, QoS:2, Retain:1>>,
         <<TopicLen:16/big, Topic:TopicLen/binary, MessageId:16/big, Rest/binary>>)
  when QoS < 3 ->
    case validate_publish_topic(Topic) of
        {ok, ParsedTopic} ->
            case parse_properties(Rest) of
                {ok, Properties, Payload} ->
                    #mqtt5_publish{message_id = MessageId,
                                   topic=ParsedTopic,
                                   qos=QoS,
                                   retain=Retain,
                                   dup=Dup,
                                   properties=Properties,
                                   payload=Payload};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end;
variable(<<?PUBACK:4, 0:4>>, <<MessageId:16/big>>) ->
     #mqtt5_puback{message_id=MessageId,
                   reason_code=?M5_SUCCESS,
                   properties=#{}};
variable(<<?PUBACK:4, 0:4>>, <<MessageId:16/big, ReasonCode:8>>) ->
     #mqtt5_puback{message_id=MessageId,
                   reason_code=ReasonCode,
                   properties=#{}};
variable(<<?PUBACK:4, 0:4>>, <<MessageId:16/big, ReasonCode:8, Rest/binary>>) ->
    case parse_properties(Rest) of
        {ok, Properties, <<>>} ->
            #mqtt5_puback{message_id=MessageId,
                          reason_code=ReasonCode,
                          properties=Properties};
        {error, Reason} ->
            {error, Reason}
    end;
variable(<<?PUBREC:4, 0:4>>, <<MessageId:16/big>>) ->
     #mqtt5_pubrec{message_id=MessageId,
                   reason_code=?M5_SUCCESS,
                   properties=#{}};
variable(<<?PUBREC:4, 0:4>>, <<MessageId:16/big, ReasonCode:8>>) ->
     #mqtt5_pubrec{message_id=MessageId,
                   reason_code=ReasonCode,
                   properties=#{}};
variable(<<?PUBREC:4, 0:4>>, <<MessageId:16/big, ReasonCode:8, Rest/binary>>) ->
    case parse_properties(Rest) of
        {ok, Properties, <<>>} ->
            #mqtt5_pubrec{message_id=MessageId,
                          reason_code=ReasonCode,
                          properties=Properties};
        {error, Reason} ->
            {error, Reason}
    end;
variable(<<?PUBREL:4, 0:2, 1:1, 0:1>>, <<MessageId:16/big>>) ->
     #mqtt5_pubrel{message_id=MessageId,
                   reason_code=?M5_SUCCESS,
                   properties=#{}};
variable(<<?PUBREL:4, 0:2, 1:1, 0:1>>, <<MessageId:16/big, ReasonCode:8>>) ->
     #mqtt5_pubrel{message_id=MessageId,
                   reason_code=ReasonCode,
                   properties=#{}};
variable(<<?PUBREL:4, 0:2, 1:1, 0:1>>, <<MessageId:16/big, ReasonCode:8, Rest/binary>>) ->
    case parse_properties(Rest) of
        {ok, Properties, <<>>} ->
            #mqtt5_pubrel{message_id=MessageId,
                          reason_code=ReasonCode,
                          properties=Properties};
        {error, Reason} ->
            {error, Reason}
    end;
variable(<<?PUBCOMP:4, 0:4>>, <<MessageId:16/big>>) ->
     #mqtt5_pubcomp{message_id=MessageId,
                    reason_code=?M5_SUCCESS,
                    properties=#{}};
variable(<<?PUBCOMP:4, 0:4>>, <<MessageId:16/big, ReasonCode:8>>) ->
     #mqtt5_pubcomp{message_id=MessageId,
                    reason_code=ReasonCode,
                    properties=#{}};
variable(<<?PUBCOMP:4, 0:4>>, <<MessageId:16/big, ReasonCode:8, Rest/binary>>) ->
    case parse_properties(Rest) of
        {ok, Properties, <<>>} ->
            #mqtt5_pubcomp{message_id=MessageId,
                           reason_code=ReasonCode,
                           properties=Properties};
        {error, Reason} ->
            {error, Reason}
    end;
variable(<<?SUBSCRIBE:4, 0:2, 1:1, 0:1>>, <<MessageId:16/big, Rest/binary>>) ->
    case parse_properties(Rest) of
        {ok, Properties, Topics} ->
            case parse_topics(Topics, ?SUBSCRIBE, []) of
                {ok, ParsedTopics} ->
                    #mqtt5_subscribe{topics=ParsedTopics,
                                     message_id=MessageId,
                                     properties=Properties};
                E -> E
            end;
        E -> E
    end;
variable(<<?SUBACK:4, 0:4>>, <<MessageId:16/big, Rest/binary>>) ->
    case parse_properties(Rest) of
        {ok, Properties, RCData} ->
            case parse_acks(RCData, [], ?allowedSubackRCs) of
                {ok, ReasonCodes} ->
                    #mqtt5_suback{message_id=MessageId,
                                  reason_codes=ReasonCodes,
                                  properties=Properties};
                E -> E
            end;
        E -> E
    end;
variable(<<?UNSUBSCRIBE:4, 0:2, 1:1, 0:1>>, <<MessageId:16/big, Rest/binary>>) ->
    case parse_properties(Rest) of
        {ok, Properties, TopicData} ->
            case parse_topics(TopicData, ?UNSUBSCRIBE, []) of
                {ok, ParsedTopics} ->
                    #mqtt5_unsubscribe{topics=ParsedTopics,
                                       message_id=MessageId,
                                       properties=Properties};
                E ->
                    E
            end;
        E -> E
    end;
variable(<<?UNSUBACK:4, 0:4>>, <<MessageId:16/big, Rest/binary>>) ->
    case parse_properties(Rest) of
        {ok, Properties, RCData} ->
            case parse_acks(RCData, [], ?allowedUnsubackRCs) of
                {ok, ReasonCodes} ->
                    #mqtt5_unsuback{message_id=MessageId,
                                    reason_codes=ReasonCodes,
                                    properties=Properties};
                E -> E
            end;
        E -> E
    end;
variable(<<?CONNECT:4, 0:4>>, <<4:16/big, "MQTT", ?PROTOCOL_5:8,
                                Flags:6/bitstring, CleanStart:1, 0:1, %% bit 0 is reserved
                                KeepAlive: 16/big, Rest0/binary>>) ->
    %% The properties are the last element of the variable header.
    case parse_properties(Rest0) of
        {ok, Properties, Rest1} ->
            %% Parse the payload.
            case Rest1 of
                <<ClientIdLen:16/big, ClientId:ClientIdLen/binary, Rest2/binary>> ->
                    case parse_will_properties(Rest2, Flags) of
                        #{lwt := LWT,
                          username := Username,
                          password := Password} ->
                        #mqtt5_connect{proto_ver=?PROTOCOL_5,
                                       username=Username,
                                       password=Password,
                                       clean_start=CleanStart == 1,
                                       keep_alive=KeepAlive,
                                       client_id=ClientId,
                                       lwt=LWT,
                                       properties=Properties};
                        {error, _} = E -> E
                    end;
                _ ->
                    {error, cant_parse_client_id}
            end;
        E -> E
    end;
variable(<<?CONNACK:4, 0:4>>, <<0:7, SP:1, ReasonCode:8/big, Rest0/binary>>) ->
    case parse_properties(Rest0) of
        {ok, Properties, <<>>} ->
            #mqtt5_connack{session_present=SP,
                           reason_code=ReasonCode,
                           properties=Properties};
        E -> E
    end;
variable(<<?PINGREQ:4, 0:4>>, <<>>) ->
    #mqtt5_pingreq{};
variable(<<?PINGRESP:4, 0:4>>, <<>>) ->
    #mqtt5_pingresp{};
variable(<<?DISCONNECT:4, 0:4>>, <<>>) ->
    #mqtt5_disconnect{reason_code=?M5_NORMAL_DISCONNECT, properties=#{}};
variable(<<?DISCONNECT:4, 0:4>>, <<RC:8>>) ->
    #mqtt5_disconnect{reason_code=RC, properties=#{}};
variable(<<?DISCONNECT:4, 0:4>>, <<RC:8, Rest/binary>>) ->
    case parse_properties(Rest) of
        {ok, Properties, <<>>} ->
            #mqtt5_disconnect{reason_code=RC, properties=Properties};
        E -> E
    end;
variable(<<?AUTH:4, 0:4>>, <<>>) ->
    #mqtt5_auth{reason_code=?M5_SUCCESS, properties=#{}};
variable(<<?AUTH:4, 0:4>>, <<RC:8, Rest/binary>>) ->
    case parse_properties(Rest) of
        {ok, Properties, <<>>} ->
            #mqtt5_auth{reason_code=RC, properties=Properties};
        E -> E
    end;
variable(_, _) ->
    {error,  cant_parse_frame}.

parse_will_properties(Rest0, <<_:2, 0:4>> = Flags) ->
    %% All will flags are zero, no last will.
    parse_username(Rest0, Flags, #{lwt => undefined});
parse_will_properties(Rest0, <<_:2, Retain:1, QoS:2, 1:1>> = Flags) ->
    case parse_properties(Rest0) of
        {ok, WillProperties,
         <<WillTopicLen:16/big, WillTopic:WillTopicLen/binary,
           WillMsgLen:16/big, WillMsg:WillMsgLen/binary,
           Rest1/binary>>} ->
            case vmq_topic:validate_topic(publish, WillTopic) of
                {ok, ParsedTopic} ->
                    M = #{
                      lwt => #mqtt5_lwt{will_properties = WillProperties,
                                        will_msg = WillMsg,
                                        will_topic = ParsedTopic,
                                        will_retain = Retain == 1,
                                        will_qos = QoS}},
                    parse_username(Rest1, Flags, M);
                _ ->
                    %% FIXME: return correct error here
                    {error, cant_validate_last_will_topic}
            end;
        E -> E
    end.

parse_username(Rest, <<0:1, _:5>> = Flags, M) ->
    %% Username bit is zero, no username.
    parse_password(Rest, Flags, M#{username=>undefined});
parse_username(<<Len:16/big, UserName:Len/binary, Rest/binary>>, <<1:1, _:5>> = Flags, M) ->
    parse_password(Rest, Flags, M#{username=>UserName});
parse_username(_, _, _) ->
    %% FIXME: return correct error here
    {error, cant_parse_username}.

parse_password(<<>>, <<_:1, 0:1, _:4>>, M) ->
    M#{password=>undefined};
parse_password(<<Len:16/big, Password:Len/binary>>, <<_:1, 1:1, _:4>>, M) ->
    M#{password=>Password};
parse_password(_, _, _) ->
    %% FIXME: return correct error here
    {error, cant_parse_password}.


parse_topics(<<>>, _, []) -> {error, no_topic_provided};
parse_topics(<<>>, _, Topics) -> {ok, Topics};
parse_topics(<<L:16/big, Topic:L/binary, 0:2, RetainHandling:2, Rap:1, NL:1, QoS:2, Rest/binary>>, ?SUBSCRIBE = Sub, Acc)
  when (QoS >= 0), (QoS < 3), RetainHandling < 3 ->
    case vmq_topic:validate_topic(subscribe, Topic) of
        {ok, ParsedTopic} ->
            T = #mqtt5_subscribe_topic{
                   topic = ParsedTopic,
                   qos = QoS,
                   no_local = to_bool(NL),
                   rap = to_bool(Rap),
                   retain_handling = sub_retain_handling(RetainHandling)
                  },
            parse_topics(Rest, Sub, [T|Acc]);
        E -> E
    end;
parse_topics(<<L:16/big, Topic:L/binary, Rest/binary>>, ?UNSUBSCRIBE = Sub, Acc) ->
    case vmq_topic:validate_topic(subscribe, Topic) of
        {ok, ParsedTopic} ->
            parse_topics(Rest, Sub, [ParsedTopic|Acc]);
        E -> E
    end;
parse_topics(_, _, _) -> {error, cant_parse_topics}.

-spec parse_acks(binary(), [reason_code()], [reason_code()]) -> {ok, [reason_code()]} | {error, cant_parse_acks}.
parse_acks(<<>>, Acks, _) ->
    {ok, Acks};
parse_acks(<<RC:8, Rest/binary>>, Acks, AllowedRCs) ->
    case lists:member(RC, AllowedRCs) of
        true ->
            parse_acks(Rest, [RC | Acks], AllowedRCs);
        _ ->
            {error, cant_parse_acks}
    end.
            

-spec serialise(mqtt5_frame()) -> serialized().
serialise(#mqtt5_publish{qos=0,
                         topic=Topic,
                         retain=Retain,
                         dup=Dup,
                         properties=Properties,
                         payload=Payload}) ->
    Var = [utf8(vmq_topic:unword(Topic)), properties(Properties), Payload],
    LenBytes = serialise_len(iolist_size(Var)),
    [<<?PUBLISH:4, (flag(Dup)):1/integer, 0:2/integer, (flag(Retain)):1/integer>>, LenBytes, Var];
serialise(#mqtt5_publish{message_id=MessageId,
                         topic=Topic,
                         qos=QoS,
                         retain=Retain,
                         dup=Dup,
                         properties=Properties,
                         payload=Payload}) ->
    Var = [utf8(vmq_topic:unword(Topic)), msg_id(MessageId), properties(Properties), Payload],
    LenBytes = serialise_len(iolist_size(Var)),
    [<<?PUBLISH:4, (flag(Dup)):1/integer,
       QoS:2/integer, (flag(Retain)):1/integer>>, LenBytes, Var];
serialise(#mqtt5_puback{message_id=MessageId, reason_code=?M5_SUCCESS, properties=P})
  when map_size(P) == 0 ->
    <<?PUBACK:4, 0:4, 2, MessageId:16/big>>;
serialise(#mqtt5_puback{message_id=MessageId, reason_code=ReasonCode, properties=Properties}) ->
    Var = [<<MessageId:16/big, ReasonCode:8/integer>>, properties(Properties)],
    LenBytes = serialise_len(iolist_size(Var)),
    [<<?PUBACK:4, 0:4>>, LenBytes, Var];
serialise(#mqtt5_pubrec{message_id=MessageId, reason_code=?M5_SUCCESS, properties=P})
  when map_size(P) == 0 ->
    <<?PUBREC:4, 0:4, 2, MessageId:16/big>>;
serialise(#mqtt5_pubrec{message_id=MessageId, reason_code=ReasonCode, properties=Properties}) ->
    Var = [<<MessageId:16/big, ReasonCode:8/integer>>, properties(Properties)],
    LenBytes = serialise_len(iolist_size(Var)),
    [<<?PUBREC:4, 0:4>>, LenBytes, Var];
serialise(#mqtt5_pubrel{message_id=MessageId, reason_code=?M5_SUCCESS, properties=P})
  when map_size(P) == 0 ->
    <<?PUBREL:4, 0:2, 1:1, 0:1, 2, MessageId:16/big>>;
serialise(#mqtt5_pubrel{message_id=MessageId, reason_code=ReasonCode, properties=Properties}) ->
    Var = [<<MessageId:16/big, ReasonCode:8/integer>>, properties(Properties)],
    LenBytes = serialise_len(iolist_size(Var)),
    [<<?PUBREL:4, 0:2, 1:1, 0:1>>, LenBytes, Var];
serialise(#mqtt5_pubcomp{message_id=MessageId, reason_code=?M5_SUCCESS, properties=P})
  when map_size(P) == 0 ->
    <<?PUBCOMP:4, 0:4, 2, MessageId:16/big>>;
serialise(#mqtt5_pubcomp{message_id=MessageId, reason_code=ReasonCode, properties=Properties}) ->
    Var = [<<MessageId:16/big, ReasonCode:8/integer>>, properties(Properties)],
    LenBytes = serialise_len(iolist_size(Var)),
    [<<?PUBCOMP:4, 0:4>>, LenBytes, Var];
serialise(#mqtt5_connect{proto_ver=ProtoVersion,
                         username=UserName,
                         password=Password,
                         lwt=LWT,
                         clean_start=CleanSession,
                         keep_alive=KeepAlive,
                         client_id=ClientId,
                         properties=Properties}) ->
    {PMagicL, PMagic} = proto(ProtoVersion),
    Var = [<<PMagicL:16/big-unsigned-integer, PMagic/binary,
             ProtoVersion:8/unsigned-integer,
             (flag(UserName)):1/integer,
             (flag(Password)):1/integer,
             (flag(lwt_retain(LWT))):1/integer,
             (lwt_qos(LWT)):2/integer,
             (flag(lwt_flag(LWT))):1/integer,
             (flag(CleanSession)):1/integer,
             0:1,  % reserved
             KeepAlive:16/big-unsigned-integer>>,
           properties(Properties),
           utf8(ClientId),
           lwt_properties(LWT),
           lwt_topic(LWT),
           lwt_msg(LWT),
           utf8(UserName),
           utf8(Password)],
    LenBytes = serialise_len(iolist_size(Var)),
    [<<?CONNECT:4, 0:4>>, LenBytes, Var];
serialise(#mqtt5_connack{session_present=SP, reason_code=RC, properties=Properties}) ->
    Var = [<<0:7, (flag(SP)):1/integer>>,
           <<RC:8/big>>,
           properties(Properties)],
    LenBytes = serialise_len(iolist_size(Var)),
    [<<?CONNACK:4, 0:4>>, LenBytes, Var];
serialise(#mqtt5_subscribe{message_id=MessageId, topics=Topics, properties=Properties}) ->
    Var = [<<MessageId:16/big>>,
           properties(Properties),
           serialise_topics(?SUBSCRIBE, Topics, [])],
    LenBytes = serialise_len(iolist_size(Var)),
    [<<?SUBSCRIBE:4, 0:2, 1:1, 0:1>>, LenBytes, Var];
serialise(#mqtt5_suback{message_id=MessageId, reason_codes=ReasonCodes, properties=Properties}) ->
    Var = [<<MessageId:16/big>>,
           properties(Properties),
           serialise_acks(ReasonCodes, [])],
    LenBytes = serialise_len(iolist_size(Var)),
    [<<?SUBACK:4, 0:4>>, LenBytes, Var];
serialise(#mqtt5_unsubscribe{message_id=MessageId, topics=Topics, properties=Properties}) ->
    Var = [<<MessageId:16/big>>,
           properties(Properties),
           serialise_topics(?UNSUBSCRIBE, Topics, [])],
    LenBytes = serialise_len(iolist_size(Var)),
    [<<?UNSUBSCRIBE:4, 0:2, 1:1, 0:1>>, LenBytes, Var];
serialise(#mqtt5_unsuback{message_id=MessageId, reason_codes=ReasonCodes, properties=Properties}) ->
    Var = [<<MessageId:16/big>>,
           properties(Properties),
           serialise_acks(ReasonCodes, [])],
    LenBytes = serialise_len(iolist_size(Var)),
    [<<?UNSUBACK:4, 0:4>>, LenBytes, Var];
serialise(#mqtt5_pingreq{}) ->
    <<?PINGREQ:4, 0:4, 0>>;
serialise(#mqtt5_pingresp{}) ->
    <<?PINGRESP:4, 0:4, 0>>;
serialise(#mqtt5_disconnect{reason_code=?M5_NORMAL_DISCONNECT, properties=Properties})
  when map_size(Properties) =:= 0 ->
    <<?DISCONNECT:4, 0:4, 0>>;
serialise(#mqtt5_disconnect{reason_code=RC, properties=Properties}) ->
    Var = [<<RC:8>>,
           properties(Properties)],
    LenBytes = serialise_len(iolist_size(Var)),
    [<<?DISCONNECT:4, 0:4>>, LenBytes, Var];
serialise(#mqtt5_auth{reason_code=?M5_SUCCESS, properties=Properties})
  when map_size(Properties) =:= 0 ->
    <<?AUTH:4, 0:4, 0>>;
serialise(#mqtt5_auth{reason_code=RC, properties=Properties}) ->
    Var = [<<RC:8>>,
           properties(Properties)],
    LenBytes = serialise_len(iolist_size(Var)),
    [<<?AUTH:4, 0:4>>, LenBytes, Var].

serialise_len(N) when N =< ?LOWBITS ->
    <<0:1, N:7>>;
serialise_len(N) ->
    <<1:1, (N rem ?HIGHBIT):7, (serialise_len(N div ?HIGHBIT))/binary>>.

serialise_topics(?SUBSCRIBE = Sub,
                 [#mqtt5_subscribe_topic{
                     topic = Topic,
                     qos = QoS,
                     no_local = NL,
                     rap = Rap,
                     retain_handling = RetainFlags}|Rest], Acc) ->
    serialise_topics(Sub, Rest,
                     [utf8(vmq_topic:unword(Topic)),
                      <<0:2, (sub_retain_handling(RetainFlags)):2, (flag(Rap)):1, (flag(NL)):1, QoS:2>>|Acc]);
serialise_topics(?UNSUBSCRIBE = Sub, [Topic|Rest], Acc) ->
    serialise_topics(Sub, Rest, [utf8(vmq_topic:unword(Topic))|Acc]);
serialise_topics(_, [], Topics) ->
    Topics.

sub_retain_handling(send_retain) -> 0;
sub_retain_handling(send_if_new_sub) -> 1;
sub_retain_handling(dont_send) -> 2;
sub_retain_handling(0) -> send_retain;
sub_retain_handling(1) -> send_if_new_sub;
sub_retain_handling(2) -> dont_send.

-spec serialise_acks([reason_code()], [binary()]) -> [binary()].
serialise_acks([RC|Rest], Acks) when is_integer(RC),
                                     (RC >= 0), (RC =< 255) ->
    serialise_acks(Rest, [<<RC:8>>|Acks]);
serialise_acks([], Acks) ->
    Acks.

proto(5) -> {4, ?PROTOCOL_MAGIC_5}.

lwt_qos(undefined) -> 0;
lwt_qos(#mqtt5_lwt{will_qos=QoS}) -> QoS.
lwt_retain(undefined) -> 0;
lwt_retain(#mqtt5_lwt{will_retain=QoS}) -> QoS.
lwt_properties(undefined) -> <<>>;
lwt_properties(#mqtt5_lwt{will_properties=P}) ->
    properties(P).
lwt_flag(undefined) -> 0;
lwt_flag(#mqtt5_lwt{}) -> 1.
lwt_topic(undefined) -> <<>>;
lwt_topic(#mqtt5_lwt{will_topic=Topic}) ->
    utf8(vmq_topic:unword(Topic)).
lwt_msg(undefined) -> <<>>;
lwt_msg(#mqtt5_lwt{will_msg=Msg}) -> utf8(Msg).

flag(<<>>) -> 0;
flag(undefined) -> 0;
flag(0) -> 0;
flag(1) -> 1;
flag(false) -> 0;
flag(true) -> 1;
flag(V) when is_binary(V) orelse is_list(V) -> 1;
flag(empty) -> 1; %% for test purposes
flag(_) -> 0.

to_bool(0) -> false;
to_bool(1) -> true.

msg_id(undefined) -> <<>>;
msg_id(MsgId) -> <<MsgId:16/big>>.

utf8(<<>>) -> <<0:16/big>>;
utf8(undefined) -> <<>>;
utf8(empty) -> <<0:16/big>>; %% for test purposes, useful if you want to encode an empty string..
utf8(IoList) when is_list(IoList) ->
    [<<(iolist_size(IoList)):16/big>>, IoList];
utf8(Bin) when is_binary(Bin) ->
    <<(byte_size(Bin)):16/big, Bin/binary>>.

binary(X) ->
    %% We encode MQTT binaries the same as utf8, but use this function
    %% to document the difference.
    utf8(X).

ensure_binary(L) when is_list(L) -> list_to_binary(L);
ensure_binary(B) when is_binary(B) -> B;
ensure_binary(undefined) -> undefined;
ensure_binary(empty) -> empty. % for test purposes

properties(#{} = M) when map_size(M) =:= 0 -> <<0:8>>;
properties(Properties) ->
    %% TODOv5: Consider if it would make sense to not convert to a
    %% list here but work directly on the map - would that be faster?
    %% easier?
    IoProps = enc_properties(maps:to_list(Properties)),
    [serialise_len(iolist_size(IoProps)), IoProps].

enc_properties([]) ->
    [];
enc_properties([#p_payload_format_indicator{value = Val}|Rest]) ->
    Indicator =
    case Val of
        utf8 -> 1;
        unspecified -> 0
    end,
    [<<?M5P_PAYLOAD_FORMAT_INDICATOR:8, Indicator:8/big>>|enc_properties(Rest)];
enc_properties([#p_message_expiry_interval{value = Val}|Rest]) ->
    [<<?M5P_MESSAGE_EXPIRY_INTERVAL:8, Val:32/big>>|enc_properties(Rest)];
enc_properties([#p_content_type{value = Val}|Rest]) ->
    [<<?M5P_CONTENT_TYPE:8>>, utf8(Val)|enc_properties(Rest)];
enc_properties([#p_response_topic{value = Topic}|Rest]) ->
    [<<?M5P_RESPONSE_TOPIC:8>>, utf8(vmq_topic:unword(Topic))|enc_properties(Rest)];
enc_properties([#p_correlation_data{value = Data}|Rest]) ->
    [<<?M5P_CORRELATION_DATA:8>>, binary(Data)|enc_properties(Rest)];
enc_properties([#p_subscription_id{value = Ids}|Rest]) ->
    [
     [ [<<?M5P_SUBSCRIPTION_ID:8>>, serialise_len(Id)] || Id <- Ids ]
     |enc_properties(Rest)];
enc_properties([#p_session_expiry_interval{value = Val}|Rest]) ->
    [<<?M5P_SESSION_EXPIRY_INTERVAL:8, Val:32/big>>|enc_properties(Rest)];
enc_properties([#p_assigned_client_id{value = Val}|Rest]) ->
    [<<?M5P_ASSIGNED_CLIENT_ID:8>>, utf8(Val)|enc_properties(Rest)];
enc_properties([#p_server_keep_alive{value = Val}|Rest]) ->
    [<<?M5P_SERVER_KEEP_ALIVE:8, Val:16/big>>|enc_properties(Rest)];
enc_properties([#p_authentication_method{value = Val}|Rest]) ->
    [<<?M5P_AUTHENTICATION_METHOD:8>>, utf8(Val)|enc_properties(Rest)];
enc_properties([#p_authentication_data{value = Val}|Rest]) ->
    [<<?M5P_AUTHENTICATION_DATA:8>>, binary(Val)|enc_properties(Rest)];
enc_properties([#p_request_problem_info{value = Bool}|Rest]) ->
    Val = flag(Bool),
    [<<?M5P_REQUEST_PROBLEM_INFO:8, Val:8>>|enc_properties(Rest)];
enc_properties([#p_will_delay_interval{value = Val}|Rest]) ->
    [<<?M5P_WILL_DELAY_INTERVAL:8, Val:32/big>>|enc_properties(Rest)];
enc_properties([#p_request_response_info{value = Bool}|Rest]) ->
    Val = flag(Bool),
    [<<?M5P_REQUEST_RESPONSE_INFO:8, Val:8>>|enc_properties(Rest)];
enc_properties([#p_response_info{value = Val}|Rest]) ->
    [<<?M5P_RESPONSE_INFO:8>>, utf8(Val)|enc_properties(Rest)];
enc_properties([#p_server_ref{value = Val}|Rest]) ->
    [<<?M5P_SERVER_REF:8>>, utf8(Val)|enc_properties(Rest)];
enc_properties([#p_reason_string{value = Val}|Rest]) ->
    [<<?M5P_REASON_STRING:8>>, utf8(Val)|enc_properties(Rest)];
enc_properties([#p_receive_max{value = Val}|Rest]) ->
    [<<?M5P_RECEIVE_MAX:8, Val:16/big>>|enc_properties(Rest)];
enc_properties([#p_topic_alias_max{value = Val}|Rest]) ->
    [<<?M5P_TOPIC_ALIAS_MAX:8, Val:16/big>>|enc_properties(Rest)];
enc_properties([#p_topic_alias{value = Val}|Rest]) ->
    [<<?M5P_TOPIC_ALIAS:8, Val:16/big>>|enc_properties(Rest)];
enc_properties([#p_max_qos{value = Val}|Rest]) ->
    [<<?M5P_MAXIMUM_QOS:8, Val:8>>|enc_properties(Rest)];
enc_properties([#p_retain_available{value = Bool}|Rest]) ->
    Val = flag(Bool),
    [<<?M5P_RETAIN_AVAILABLE:8, Val:8>>|enc_properties(Rest)];
enc_properties([#p_user_property{value = [{_,_}|_] = UserProps}|Rest]) ->
    [
     [[<<?M5P_USER_PROPERTY:8>>, utf8(Key), utf8(Val)] || {Key, Val} <- UserProps]
     |enc_properties(Rest)];
enc_properties([#p_max_packet_size{value = Val}|Rest]) ->
    [<<?M5P_MAX_PACKET_SIZE:8, Val:32/big>>|enc_properties(Rest)];
enc_properties([#p_wildcard_subs_available{value = Bool}|Rest]) ->
    Val = flag(Bool),
    [<<?M5P_WILDCARD_SUBS_AVAILABLE:8, Val:8>>|enc_properties(Rest)];
enc_properties([#p_sub_ids_available{value = Bool}|Rest]) ->
    Val = flag(Bool),
    [<<?M5P_SUB_IDS_AVAILABLE:8, Val:8>>|enc_properties(Rest)];
enc_properties([#p_shared_subs_available{value = Bool}|Rest]) ->
    Val = flag(Bool),
    [<<?M5P_SHARED_SUBS_AVAILABLE:8, Val:8>>|enc_properties(Rest)].




%%%%%%% packet generator functions (useful for testing)
gen_connect(ClientId, Opts) ->
    Frame = #mqtt5_connect{
               client_id       = ensure_binary(ClientId),
               clean_start     = proplists:get_value(clean_start, Opts, true),
               keep_alive      = proplists:get_value(keepalive, Opts, 60),
               username        = ensure_binary(proplists:get_value(username, Opts)),
               password        = ensure_binary(proplists:get_value(password, Opts)),
               proto_ver       = ?PROTOCOL_5,
               lwt             = proplists:get_value(lwt, Opts, undefined),
               properties      = proplists:get_value(properties, Opts, #{})
              },
    iolist_to_binary(serialise(Frame)).

gen_connack(SP, RC, Properties) ->
    iolist_to_binary(serialise(#mqtt5_connack{session_present=flag(SP), reason_code=RC,
                                              properties = Properties})).

gen_publish(Topic, Qos, Payload, Opts) ->
    Frame = #mqtt5_publish{
               message_id        = proplists:get_value(mid, Opts, 0),
               topic             = ensure_binary(Topic),
               qos               = Qos,
               retain            = proplists:get_value(retain, Opts, false),
               dup               = proplists:get_value(dup, Opts, false),
               properties        = proplists:get_value(properties, Opts, #{}),
               payload           = ensure_binary(Payload)
              },
    iolist_to_binary(serialise(Frame)).

gen_puback(MId, ReasonCode, Properties) ->
    iolist_to_binary(serialise(#mqtt5_puback{message_id=MId,
                                             reason_code=ReasonCode,
                                             properties=Properties
                                            })).

gen_pubrec(MId, ReasonCode, Properties) ->
    iolist_to_binary(serialise(#mqtt5_pubrec{message_id=MId,
                                             reason_code=ReasonCode,
                                             properties=Properties})).

gen_pubrel(MId, ReasonCode, Properties) ->
    iolist_to_binary(serialise(#mqtt5_pubrel{message_id=MId,
                                             reason_code=ReasonCode,
                                             properties=Properties})).

gen_pubcomp(MId, ReasonCode, Properties) ->
    iolist_to_binary(serialise(#mqtt5_pubcomp{message_id=MId,
                                              reason_code=ReasonCode,
                                              properties=Properties})).

gen_subscribe(MId, [#mqtt5_subscribe_topic{}|_] = Topics, Properties) ->
    TTopics = [E#mqtt5_subscribe_topic{topic=ensure_binary(T)} 
               || #mqtt5_subscribe_topic{topic = T} = E <- Topics],
    iolist_to_binary(serialise(#mqtt5_subscribe{topics=TTopics,
                                                message_id=MId,
                                                properties=Properties})).

gen_suback(MId, ReasonCodes, Properties) when is_list(ReasonCodes) ->
    iolist_to_binary(
      serialise(
        #mqtt5_suback{message_id=MId, reason_codes=ReasonCodes, properties=Properties})).

gen_unsubscribe(MId, Topics, Properties) ->
    iolist_to_binary(
      serialise(
        #mqtt5_unsubscribe{topics=[ensure_binary(Topic) || Topic <- Topics],
                           message_id=MId,
                           properties=Properties})).

gen_unsuback(MId, ReasonCodes, Properties) when is_list(ReasonCodes) ->
    iolist_to_binary(
      serialise(
        #mqtt5_unsuback{message_id=MId, reason_codes=ReasonCodes, properties=Properties})).

gen_pingreq() ->
    iolist_to_binary(serialise(#mqtt5_pingreq{})).

gen_pingresp() ->
    iolist_to_binary(serialise(#mqtt5_pingresp{})).

gen_disconnect(RC, Properties) ->
    iolist_to_binary(
      serialise(
        #mqtt5_disconnect{reason_code=RC,
                          properties=Properties})).

gen_auth(RC, Properties) ->
    iolist_to_binary(
      serialise(
        #mqtt5_auth{reason_code=RC,
                    properties=Properties})).

-spec parse_properties(binary()) -> {ok, map(), binary()} |
                                    {error, any()}.
parse_properties(Data) ->
    case varint_data(Data) of
        {error, _} ->
            {error, cant_parse_properties};
        {PropertiesData, Rest} ->
            case parse_properties(PropertiesData, #{}) of
                #{p_user_property := UserProps} = Props ->
                    %% Make sure to preserve order of the user properties
                    {ok, Props#{p_user_property => lists:reverse(UserProps)}, Rest};
                Properties when is_map(Properties) ->
                    {ok, Properties, Rest};
                {error, _} = E ->
                    E
            end
    end.

-spec parse_properties(binary(), map()) -> map() | {error, any()}.
parse_properties(<<>>, Acc) ->
    Acc;
%% Note, the property ids are specified as a varint, but in MQTT5 all
%% indicator ids fit within one byte, so we parse it as such to keep
%% things simple.
parse_properties(<<?M5P_PAYLOAD_FORMAT_INDICATOR:8, _Rest/binary>>, #{p_payload_format_indicator := _})  ->
    {error, property_is_only_allowed_once};
parse_properties(<<?M5P_PAYLOAD_FORMAT_INDICATOR:8, Val:8, Rest/binary>>, Acc) when Val == 0 ->
    parse_properties(Rest, Acc#{p_payload_format_indicator => unspecified});
parse_properties(<<?M5P_PAYLOAD_FORMAT_INDICATOR:8, Val:8, Rest/binary>>, Acc) when Val == 1 ->
    parse_properties(Rest, Acc#{p_payload_format_indicator => utf8});

parse_properties(<<?M5P_MESSAGE_EXPIRY_INTERVAL:8, _Rest/binary>>, #{p_message_expiry_interval := _})  ->
    {error, property_is_only_allowed_once};
parse_properties(<<?M5P_MESSAGE_EXPIRY_INTERVAL:8, Val:32/big, Rest/binary>>, Acc) ->
    parse_properties(Rest, Acc#{p_message_expiry_interval => Val});

parse_properties(<<?M5P_CONTENT_TYPE, _Rest/binary>>, #{p_content_type := _})  ->
    {error, property_is_only_allowed_once};
parse_properties(<<?M5P_CONTENT_TYPE:8, Len:16/big, Val:Len/binary, Rest/binary>>, Acc) ->
    parse_properties(Rest, Acc#{p_content_type => Val});

parse_properties(<<?M5P_RESPONSE_TOPIC:8, _Rest/binary>>, #{p_response_topic := _}) ->
    {error, property_is_only_allowed_once};
parse_properties(<<?M5P_RESPONSE_TOPIC:8, Len:16/big, Val:Len/binary, Rest/binary>>, Acc) ->
    parse_properties(Rest, Acc#{p_response_topic => Val});

parse_properties(<<?M5P_CORRELATION_DATA:8, _Rest/binary>>, #{p_correlation_data := _}) ->
    {error, property_is_only_allowed_once};
parse_properties(<<?M5P_CORRELATION_DATA:8, Len:16/big, Val:Len/binary, Rest/binary>>, Acc) ->
    parse_properties(Rest, Acc#{p_correlation_data => Val});

parse_properties(<<?M5P_SUBSCRIPTION_ID:8, Data/binary>>, Acc) ->
    case varint(Data) of
        {VarInt, Rest} when 1 =< VarInt, VarInt =< 268435455 ->
            NewAcc =
                case maps:find(p_subscription_id, Acc) of
                    error ->
                        Acc#{p_subscription_id => [VarInt]};
                    {ok, Vals} ->
                        Acc#{p_subscription_id => [VarInt|Vals]}
                end,
            parse_properties(Rest, NewAcc);
        error -> {error, cant_parse_properties}
    end;

parse_properties(<<?M5P_SESSION_EXPIRY_INTERVAL:8, _Rest/binary>>, #{p_session_expiry_interval := _}) ->
    {error, property_is_only_allowed_once};
parse_properties(<<?M5P_SESSION_EXPIRY_INTERVAL:8, Val:32/big, Rest/binary>>, Acc) ->
    parse_properties(Rest, Acc#{p_session_expiry_interval => Val});

parse_properties(<<?M5P_ASSIGNED_CLIENT_ID:8, _Rest/binary>>, #{p_assigned_client_id := _}) ->
    {error, property_is_only_allowed_once};
parse_properties(<<?M5P_ASSIGNED_CLIENT_ID:8, Len:16/big, Val:Len/binary, Rest/binary>>, Acc) ->
    parse_properties(Rest, Acc#{p_assigned_client_id => Val});

parse_properties(<<?M5P_SERVER_KEEP_ALIVE:8, _Rest/binary>>, #{p_server_keep_alive := _}) ->
    {error, property_is_only_allowed_once};
parse_properties(<<?M5P_SERVER_KEEP_ALIVE:8, Val:16/big, Rest/binary>>, Acc) ->
    parse_properties(Rest, Acc#{p_server_keep_alive => Val});

parse_properties(<<?M5P_AUTHENTICATION_METHOD:8, _Rest/binary>>, #{p_authentication_method := _}) ->
    {error, property_is_only_allowed_once};
parse_properties(<<?M5P_AUTHENTICATION_METHOD:8, Len:16/big, Val:Len/binary, Rest/binary>>, Acc) ->
    parse_properties(Rest, Acc#{p_authentication_method => Val});

parse_properties(<<?M5P_AUTHENTICATION_DATA:8, _Rest/binary>>, #{p_authentication_data := _}) ->
    {error, property_is_only_allowed_once};
parse_properties(<<?M5P_AUTHENTICATION_DATA:8, Len:16/big, Val:Len/binary, Rest/binary>>, Acc) ->
    parse_properties(Rest, Acc#{p_authentication_data => Val});

parse_properties(<<?M5P_REQUEST_PROBLEM_INFO:8, _Rest/binary>>, #{p_request_problem_info := _}) ->
    {error, property_is_only_allowed_once};
parse_properties(<<?M5P_REQUEST_PROBLEM_INFO:8, Val:8/big, Rest/binary>>, Acc) when Val == 0; Val == 1 ->
    parse_properties(Rest, Acc#{p_request_problem_info => Val == 1});

parse_properties(<<?M5P_WILL_DELAY_INTERVAL:8, _Rest/binary>>, #{p_will_delay_interval := _}) ->
    {error, property_is_only_allowed_once};
parse_properties(<<?M5P_WILL_DELAY_INTERVAL:8, Val:32/big, Rest/binary>>, Acc) ->
    parse_properties(Rest, Acc#{p_will_delay_interval => Val});

parse_properties(<<?M5P_REQUEST_RESPONSE_INFO:8, _Rest/binary>>, #{p_request_response_info := _}) ->
    {error, property_is_only_allowed_once};
parse_properties(<<?M5P_REQUEST_RESPONSE_INFO:8, Val:8/big, Rest/binary>>, Acc) when Val == 0; Val == 1 ->
    parse_properties(Rest, Acc#{p_request_response_info => Val == 1});

parse_properties(<<?M5P_RESPONSE_INFO:8, _Rest/binary>>, #{p_response_info := _}) ->
    {error, property_is_only_allowed_once};
parse_properties(<<?M5P_RESPONSE_INFO:8, Len:16/big, Val:Len/binary, Rest/binary>>, Acc) ->
    parse_properties(Rest, Acc#{p_response_info => Val});

parse_properties(<<?M5P_SERVER_REF:8, _Rest/binary>>, #{p_server_ref := _}) ->
    {error, property_is_only_allowed_once};
parse_properties(<<?M5P_SERVER_REF:8, Len:16/big, Val:Len/binary, Rest/binary>>, Acc) ->
    parse_properties(Rest, Acc#{p_server_ref => Val});

parse_properties(<<?M5P_REASON_STRING:8, _Rest/binary>>, #{p_reason_string := _}) ->
    {error, property_is_only_allowed_once};
parse_properties(<<?M5P_REASON_STRING:8, Len:16/big, Val:Len/binary, Rest/binary>>, Acc) ->
    parse_properties(Rest, Acc#{p_reason_string => Val});

parse_properties(<<?M5P_RECEIVE_MAX:8, _Rest/binary>>, #{p_receive_max := _}) ->
    {error, property_is_only_allowed_once};
parse_properties(<<?M5P_RECEIVE_MAX:8, Val:16/big, Rest/binary>>, Acc) when Val > 0 ->
    parse_properties(Rest, Acc#{p_receive_max => Val});

parse_properties(<<?M5P_TOPIC_ALIAS_MAX:8, _Rest/binary>>, #{p_topic_alias_max := _}) ->
    {error, property_is_only_allowed_once};
parse_properties(<<?M5P_TOPIC_ALIAS_MAX:8, Val:16/big, Rest/binary>>, Acc) ->
    parse_properties(Rest, Acc#{p_topic_alias_max => Val});

parse_properties(<<?M5P_TOPIC_ALIAS:8, _Rest/binary>>, #{p_topic_alias := _}) ->
    {error, property_is_only_allowed_once};
parse_properties(<<?M5P_TOPIC_ALIAS:8, Val:16/big, Rest/binary>>, Acc) ->
    parse_properties(Rest, Acc#{p_topic_alias => Val});

parse_properties(<<?M5P_MAXIMUM_QOS:8, _Rest/binary>>, #{p_max_qos := _}) ->
    {error, property_is_only_allowed_once};
parse_properties(<<?M5P_MAXIMUM_QOS:8, Val:8, Rest/binary>>, Acc) when Val == 0; Val == 1 ->
    parse_properties(Rest, Acc#{p_max_qos => Val});

parse_properties(<<?M5P_RETAIN_AVAILABLE:8, _Rest/binary>>, #{p_retain_available := _}) ->
    {error, property_is_only_allowed_once};
parse_properties(<<?M5P_RETAIN_AVAILABLE:8, Val:8, Rest/binary>>, Acc) when Val == 0; Val == 1 ->
    parse_properties(Rest, Acc#{p_retain_available => Val == 1});

parse_properties(<<?M5P_USER_PROPERTY:8, KLen:16/big, Key:KLen/binary,
                   VLen:16/big, Val:VLen/binary, Rest/binary>>, Acc) ->
    NewAcc =
        case maps:find(p_user_property, Acc) of
            error ->
                Acc#{p_user_property => [{Key,Val}]};
            {ok, Vals} ->
                Acc#{p_user_property => [{Key,Val}|Vals]}
        end,
    parse_properties(Rest, NewAcc);
parse_properties(<<?M5P_MAX_PACKET_SIZE:8, _Rest/binary>>, #{p_max_packet_size := _}) ->
    {error, property_is_only_allowed_once};
parse_properties(<<?M5P_MAX_PACKET_SIZE:8, Val:32/big, Rest/binary>>, Acc) when Val > 0 ->
    parse_properties(Rest, Acc#{p_max_packet_size => Val});

parse_properties(<<?M5P_WILDCARD_SUBS_AVAILABLE:8, _Rest/binary>>, #{p_wildcard_subs_available := _}) ->
    {error, property_is_only_allowed_once};
parse_properties(<<?M5P_WILDCARD_SUBS_AVAILABLE:8, Val:8, Rest/binary>>, Acc) when Val == 0; Val == 1 ->
    parse_properties(Rest, Acc#{p_wildcard_subs_available => Val == 1});

parse_properties(<<?M5P_SUB_IDS_AVAILABLE:8, _Rest/binary>>, #{p_sub_ids_available := _}) ->
    {error, property_is_only_allowed_once};
parse_properties(<<?M5P_SUB_IDS_AVAILABLE:8, Val:8, Rest/binary>>, Acc) when Val == 0; Val == 1 ->
    parse_properties(Rest, Acc#{p_sub_ids_available => Val == 1});

parse_properties(<<?M5P_SHARED_SUBS_AVAILABLE:8, _Rest/binary>>, #{p_shared_subs_available := _}) ->
    {error, property_is_only_allowed_once};
parse_properties(<<?M5P_SHARED_SUBS_AVAILABLE:8, Val:8, Rest/binary>>, Acc) when Val == 0; Val == 1 ->
    parse_properties(Rest, Acc#{p_shared_subs_available => Val == 1});
parse_properties(_, _) ->
    {error, cant_parse_properties}.

%% @doc parse a varint and return the following data as well as any
%% remaining data.
-spec varint_data(binary()) -> {binary(), binary()} | {error, any()}.
varint_data(Data) ->
    case varint(Data) of
        {VarInt, Rest} when byte_size(Rest) >= VarInt ->
            <<VarData:VarInt/binary, Rest1/binary>> = Rest,
            {VarData, Rest1};
        error -> {error, cant_parse_varint}
    end.

%% @doc parse a varint and return if and with any remaining data.
-spec varint(binary()) -> {non_neg_integer(), binary()} | error.
varint(<<0:1, DataSize:7, Rest/binary>>) ->
    {DataSize, Rest};
varint(<<1:1, L1:7, 0:1, L2:7, Rest/binary>>) ->
    Len = L1 + (L2 bsl 7),
    {Len, Rest};
varint(<<1:1, L1:7, 1:1, L2:7, 0:1, L3:7, Rest/binary>>) ->
    Len = L1 + (L2 bsl 7) + (L3 bsl 14),
    {Len, Rest};
varint(<<1:1, L1:7, 1:1, L2:7, 1:1, L3:7, 0:1, L4:7, Rest/binary>>) ->
    Len = L1 + (L2 bsl 7) + (L3 bsl 14) + (L4 bsl 21),
    {Len, Rest};
varint(_) ->
    error.

validate_publish_topic(<<>>) ->
    %% empty topics are allowed in mqttv5 if used together with topic
    %% aliases.
    {ok, []};
validate_publish_topic(Topic) ->
    vmq_topic:validate_topic(publish, Topic).

-spec rcn2rc(reason_code_name()) -> reason_code().
rcn2rc(success)                        -> ?M5_SUCCESS;
rcn2rc(granted_qos0)                   -> ?M5_GRANTED_QOS0;
rcn2rc(granted_qos1)                   -> ?M5_GRANTED_QOS1;
rcn2rc(granted_qos2)                   -> ?M5_GRANTED_QOS2;
rcn2rc(normal_disconnect)              -> ?M5_NORMAL_DISCONNECT;
rcn2rc(disconnect_with_will_msg)       -> ?M5_DISCONNECT_WITH_WILL_MSG;
rcn2rc(no_matching_subscribers)        -> ?M5_NO_MATCHING_SUBSCRIBERS;
rcn2rc(no_subscription_existed)        -> ?M5_NO_SUBSCRIPTION_EXISTED;
rcn2rc(continue_authentication)        -> ?M5_CONTINUE_AUTHENTICATION;
rcn2rc(reauthenticate)                 -> ?M5_REAUTHENTICATE;
rcn2rc(unspecified_error)              -> ?M5_UNSPECIFIED_ERROR;
rcn2rc(malformed_packet)               -> ?M5_MALFORMED_PACKET;
rcn2rc(protocol_error)                 -> ?M5_PROTOCOL_ERROR;
rcn2rc(impl_specific_error)            -> ?M5_IMPL_SPECIFIC_ERROR;
rcn2rc(unsupported_protocol_version)   -> ?M5_UNSUPPORTED_PROTOCOL_VERSION;
rcn2rc(client_identifier_not_valid)    -> ?M5_CLIENT_IDENTIFIER_NOT_VALID;
rcn2rc(bad_username_or_password)       -> ?M5_BAD_USERNAME_OR_PASSWORD;
rcn2rc(not_authorized)                 -> ?M5_NOT_AUTHORIZED;
rcn2rc(server_unavailable)             -> ?M5_SERVER_UNAVAILABLE;
rcn2rc(server_busy)                    -> ?M5_SERVER_BUSY;
rcn2rc(banned)                         -> ?M5_BANNED;
rcn2rc(server_shutting_down)           -> ?M5_SERVER_SHUTTING_DOWN;
rcn2rc(bad_authentication_method)      -> ?M5_BAD_AUTHENTICATION_METHOD;
rcn2rc(keep_alive_timeout)             -> ?M5_KEEP_ALIVE_TIMEOUT;
rcn2rc(session_taken_over)             -> ?M5_SESSION_TAKEN_OVER;
rcn2rc(topic_filter_invalid)           -> ?M5_TOPIC_FILTER_INVALID;
rcn2rc(topic_name_invalid)             -> ?M5_TOPIC_NAME_INVALID;
rcn2rc(packet_id_in_use)               -> ?M5_PACKET_ID_IN_USE;
rcn2rc(packet_id_not_found)            -> ?M5_PACKET_ID_NOT_FOUND;
rcn2rc(receive_max_exceeded)           -> ?M5_RECEIVE_MAX_EXCEEDED;
rcn2rc(topic_alias_invalid)            -> ?M5_TOPIC_ALIAS_INVALID;
rcn2rc(packet_too_large)               -> ?M5_PACKET_TOO_LARGE;
rcn2rc(message_rate_too_high)          -> ?M5_MESSAGE_RATE_TOO_HIGH;
rcn2rc(quota_exceeded)                 -> ?M5_QUOTA_EXCEEDED;
rcn2rc(administrative_action)          -> ?M5_ADMINISTRATIVE_ACTION;
rcn2rc(payload_format_invalid)         -> ?M5_PAYLOAD_FORMAT_INVALID;
rcn2rc(retain_not_supported)           -> ?M5_RETAIN_NOT_SUPPORTED;
rcn2rc(qos_not_supported)              -> ?M5_QOS_NOT_SUPPORTED;
rcn2rc(use_another_server)             -> ?M5_USE_ANOTHER_SERVER;
rcn2rc(server_moved)                   -> ?M5_SERVER_MOVED;
rcn2rc(shared_subs_not_supported)      -> ?M5_SHARED_SUBS_NOT_SUPPORTED;
rcn2rc(connection_rate_exceeded)       -> ?M5_CONNECTION_RATE_EXCEEDED;
rcn2rc(max_connect_time)               -> ?M5_MAX_CONNECT_TIME;
rcn2rc(subscription_ids_not_supported) -> ?M5_SUBSCRIPTION_IDS_NOT_SUPPORTED;
rcn2rc(wildcard_subs_not_supported)    -> ?M5_WILDCARD_SUBS_NOT_SUPPORTED.


-spec rc2rcn(reason_code()) -> reason_code_name().
rc2rcn(?M5_SUCCESS)                        -> success;
rc2rcn(?M5_DISCONNECT_WITH_WILL_MSG)       -> disconnect_with_will_msg;
rc2rcn(?M5_NO_MATCHING_SUBSCRIBERS)        -> no_matching_subscribers;
rc2rcn(?M5_NO_SUBSCRIPTION_EXISTED)        -> no_subscription_existed;
rc2rcn(?M5_CONTINUE_AUTHENTICATION)        -> continue_authentication;
rc2rcn(?M5_REAUTHENTICATE)                 -> reauthenticate;
rc2rcn(?M5_UNSPECIFIED_ERROR)              -> unspecified_error;
rc2rcn(?M5_MALFORMED_PACKET)               -> malformed_packet;
rc2rcn(?M5_PROTOCOL_ERROR)                 -> protocol_error;
rc2rcn(?M5_IMPL_SPECIFIC_ERROR)            -> impl_specific_error;
rc2rcn(?M5_UNSUPPORTED_PROTOCOL_VERSION)   -> unsupported_protocol_VERSION;
rc2rcn(?M5_CLIENT_IDENTIFIER_NOT_VALID)    -> client_identifier_not_valid;
rc2rcn(?M5_BAD_USERNAME_OR_PASSWORD)       -> bad_username_or_password;
rc2rcn(?M5_NOT_AUTHORIZED)                 -> not_authorized;
rc2rcn(?M5_SERVER_UNAVAILABLE)             -> server_unavailable;
rc2rcn(?M5_SERVER_BUSY)                    -> server_busy;
rc2rcn(?M5_BANNED)                         -> banned;
rc2rcn(?M5_SERVER_SHUTTING_DOWN)           -> server_shutting_down;
rc2rcn(?M5_BAD_AUTHENTICATION_METHOD)      -> bad_authentication_method;
rc2rcn(?M5_KEEP_ALIVE_TIMEOUT)             -> keep_alive_timeout;
rc2rcn(?M5_SESSION_TAKEN_OVER)             -> session_taken_over;
rc2rcn(?M5_TOPIC_FILTER_INVALID)           -> topic_filter_invalid;
rc2rcn(?M5_TOPIC_NAME_INVALID)             -> topic_name_invalid;
rc2rcn(?M5_PACKET_ID_IN_USE)               -> packet_id_in_use;
rc2rcn(?M5_PACKET_ID_NOT_FOUND)            -> packet_id_not_found;
rc2rcn(?M5_RECEIVE_MAX_EXCEEDED)           -> receive_max_exceeded;
rc2rcn(?M5_TOPIC_ALIAS_INVALID)            -> topic_alias_invalid;
rc2rcn(?M5_PACKET_TOO_LARGE)               -> packet_too_large;
rc2rcn(?M5_MESSAGE_RATE_TOO_HIGH)          -> message_rate_too_high;
rc2rcn(?M5_QUOTA_EXCEEDED)                 -> quota_exceeded;
rc2rcn(?M5_ADMINISTRATIVE_ACTION)          -> administrative_action;
rc2rcn(?M5_PAYLOAD_FORMAT_INVALID)         -> payload_format_invalid;
rc2rcn(?M5_RETAIN_NOT_SUPPORTED)           -> retain_not_supported;
rc2rcn(?M5_QOS_NOT_SUPPORTED)              -> qos_not_supported;
rc2rcn(?M5_USE_ANOTHER_SERVER)             -> use_another_server;
rc2rcn(?M5_SERVER_MOVED)                   -> server_moved;
rc2rcn(?M5_SHARED_SUBS_NOT_SUPPORTED)      -> shared_subs_not_supported;
rc2rcn(?M5_CONNECTION_RATE_EXCEEDED)       -> connection_rate_exceeded;
rc2rcn(?M5_MAX_CONNECT_TIME)               -> max_connect_time;
rc2rcn(?M5_SUBSCRIPTION_IDS_NOT_SUPPORTED) -> subscription_ids_not_supported;
rc2rcn(?M5_WILDCARD_SUBS_NOT_SUPPORTED)    ->  wildcard_subs_not_supported.
