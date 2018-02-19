%% @doc compat layer to run mqtt v4 tests on v5.
-module(mqtt5_v4compat).

-export([do_client_connect/4,
         expect_packet/4,
         gen_connect/3,
         gen_connack/2,
         gen_subscribe/4,
         gen_suback/3,
         gen_publish/5,
         gen_disconnect/1]).

-include_lib("vmq_commons/include/vmq_types_mqtt5.hrl").

do_client_connect(Connect, Connack, Opts, Config) ->
    do_client_connect_(protover(Config), Connect, Connack, Opts).

do_client_connect_(4, Connect, Connack, Opts) ->
    packet:do_client_connect(Connect, Connack, Opts);
do_client_connect_(5, Connect, Connack, Opts) ->
    {ExpectedFrame, <<>>} = vmq_parser_mqtt5:parse(Connack),
    case packetv5:do_client_connect(Connect, Opts) of
        {ok, Socket, Frame, <<>>} ->
            case ExpectedFrame of
                Frame ->
                    {ok, Socket};
                _ ->
                    {error, not_equal, ExpectedFrame, Frame}
            end;
        E -> E
    end.

expect_packet(Socket, Name, Frame, Config) ->
    expect_packet_(protover(Config), Socket, Name, Frame).

expect_packet_(4, Socket, Name, Packet) ->
    packet:expect_packet(Socket, Name, Packet);
expect_packet_(5, Socket, _Name, ExpectedPacket) ->
    ExpectedFrame = vmq_parser_mqtt5:parse(ExpectedPacket),
    {ok, GotFrame, <<>>} = packetv5:receive_frame(Socket),
    case ExpectedFrame of
        {GotFrame, <<>>} ->
            ok;
        _ ->
            {error, not_equal, ExpectedFrame, GotFrame}
    end.

gen_connect(ClientId, Opts, Config) ->
    gen_connect_(protover(Config), ClientId, Opts).

gen_connect_(4, ClientId, Opts) ->
    packet:gen_connect(ClientId, [{protover, 4}|Opts]);
gen_connect_(5, ClientId, Opts) ->
    WillMsg = proplists:get_value(will_msg, Opts),
    WillTopic = proplists:get_value(will_topic, Opts),
    case {WillMsg, WillTopic} of
        {undefined, undefined} ->
            vmq_parser_mqtt5:gen_connect(ClientId, Opts);
        {WillMsg, WillTopic} when WillMsg =/= undefined,
                                  WillTopic =/= undefined ->
            WillQoS = proplists:get_value(will_qos, Opts, 0),
            WillRetain = proplists:get_value(will_retain, Opts, false),
            LWT = #mqtt5_lwt{
                     will_properties = [],
                     will_retain = WillRetain,
                     will_qos = WillQoS,
                     will_topic = ensure_binary(WillTopic),
                     will_msg = ensure_binary(WillMsg)},
            vmq_parser_mqtt5:gen_connect(ClientId, [{lwt, LWT}|Opts])
    end.

gen_connack(RC, Config) ->
    gen_connack_(protover(Config), RC).

gen_connack_(4, RC) ->
    RCv4 =
        case RC of
            success -> 0;
            bad_proto -> 1;
            bad_identifier -> 2;
            server_unavailable -> 3;
            malformed_credentials -> 4;
            not_authorized -> 5
    end,
    packet:gen_connack(RCv4);
gen_connack_(5, RC) ->
    RCv5 =
        case RC of
            success -> 16#00;
            bad_authn -> 16#86;
            bad_identifier -> 16#85;
            server_unavailable -> 16#88;
            not_authorized -> 16#87
        end,
    vmq_parser_mqtt5:gen_connack(RCv5).

gen_subscribe(Mid, Topic, QoS, Config) ->
    gen_subscribe_(protover(Config), Mid, Topic, QoS).

gen_subscribe_(4, Mid, Topic, QoS) ->
    packet:gen_subscribe(Mid, Topic, QoS);
gen_subscribe_(5, Mid, Topic, QoS) ->
    Topics = [#mqtt5_subscribe_topic{
                 topic=Topic,
                 qos=QoS,
                 no_local=false,
                 rap=false,
                 retain_handling=send_retain
                }],
    vmq_parser_mqtt5:gen_subscribe(Mid, Topics, []).

gen_suback(Mid, Exp, Config) ->
    gen_suback_(protover(Config), Mid, Exp).

gen_suback_(4, Mid, RC) ->
    packet:gen_suback(Mid, RC);
gen_suback_(5, Mid, RC) ->
    vmq_parser_mqtt5:gen_suback(Mid, [RC], []).

gen_publish(Topic, QoS, Payload, Opts, Config) ->
    gen_publish_(protover(Config), Topic, QoS, Payload, Opts).

gen_publish_(4, Topic, QoS, Payload, Opts) ->
    packet:gen_publish(Topic, QoS, Payload, Opts);
gen_publish_(5, Topic, QoS, Payload, Opts) ->
    vmq_parser_mqtt5:gen_publish(Topic, QoS, Payload, Opts).

gen_disconnect(Config) ->
    case protover(Config) of
        4 -> packet:gen_disconnect();
        5 -> vmq_parser_mqtt5:gen_disconnect()
    end.

ensure_binary(L) when is_list(L) -> list_to_binary(L);
ensure_binary(B) when is_binary(B) -> B;
ensure_binary(empty) -> empty. % for test purposes

protover(Config) ->
    proplists:get_value(protover, Config).
