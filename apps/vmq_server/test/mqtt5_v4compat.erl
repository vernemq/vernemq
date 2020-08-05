%% @doc compat layer to run mqtt v4 tests on v5.
-module(mqtt5_v4compat).

-export([do_client_connect/4,
         expect_packet/4,
         gen_connect/3,
         gen_connack/2,
         gen_connack/3,
         gen_subscribe/4,
         gen_suback/3,
         gen_unsubscribe/3,
         gen_unsuback/2,
         gen_publish/5,
         gen_puback/2,
         gen_pubrec/2,
         gen_pubrel/2,
         gen_pubcomp/2,
         gen_disconnect/1]).

-export([protover/1]).

-include_lib("vmq_commons/include/vmq_types.hrl").

do_client_connect(Connect, Connack, Opts, Config) ->
    do_client_connect_(protover(Config), Connect, Connack, Opts).

do_client_connect_(4, Connect, Connack, Opts) ->
    packet:do_client_connect(Connect, Connack, Opts);
do_client_connect_(5, Connect, Connack, Opts) ->
    packetv5:do_client_connect(Connect, Connack, Opts).

expect_packet(Socket, Name, Frame, Config) ->
    expect_packet_(protover(Config), Socket, Name, Frame).

expect_packet_(4, Socket, Name, Packet) ->
    packet:expect_packet(Socket, Name, Packet);
expect_packet_(5, Socket, _Name, ExpectedPacket) ->
    packetv5:expect_frame(Socket, ExpectedPacket).

gen_connect(ClientId, Opts, Config) ->
    gen_connect_(protover(Config), ClientId, Opts).

gen_connect_(4, ClientId, Opts) ->
    packet:gen_connect(ClientId, [{protover, 4}|Opts]);
gen_connect_(5, ClientId, Opts) ->
    WillMsg = proplists:get_value(will_msg, Opts),
    WillTopic = proplists:get_value(will_topic, Opts),
    case {WillMsg, WillTopic} of
        {undefined, undefined} ->
            packetv5:gen_connect(ClientId, fixup_v4_opts(Opts));
        {WillMsg, WillTopic} when WillMsg =/= undefined,
                                  WillTopic =/= undefined ->
            WillQoS = proplists:get_value(will_qos, Opts, 0),
            WillRetain = proplists:get_value(will_retain, Opts, false),
            WillProperties = proplists:get_value(will_properties, Opts, #{}),
            LWT = #mqtt5_lwt{
                     will_properties = WillProperties,
                     will_retain = WillRetain,
                     will_qos = WillQoS,
                     will_topic = ensure_binary(WillTopic),
                     will_msg = ensure_binary(WillMsg)},
            packetv5:gen_connect(ClientId, [{lwt, LWT}|fixup_v4_opts(Opts)])
    end.

fixup_v4_opts(Opts) ->
    Properties = proplists:get_value(properties, Opts, #{}),
    case lists:keyfind(clean_session, 1, Opts) of
        {clean_session, false} ->
            [{clean_start, false},
             {properties,
              maps:put(p_session_expiry_interval,
                       maps:get(p_session_expiry_interval, Properties, 16#FFFFFFFF), Properties)
             }|lists:keydelete(properties, 1, Opts)];
        _ ->
            % assumption clean_session=true
            [{clean_start, true},
             {properties,
              maps:remove(p_session_expiry_interval, Properties)
             }|lists:keydelete(properties, 1, Opts)]
    end.

gen_connack(RC, Config) ->
    gen_connack(false, RC, Config).

gen_connack(SP, RC, Config) ->
    gen_connack_(protover(Config), SP, RC).

gen_connack_(4, SP, RC) ->
    RCv4 =
        case RC of
            success -> 0;
            bad_proto -> 1;
            bad_identifier -> 2;
            server_unavailable -> 3;
            malformed_credentials -> 4;
            not_authorized -> 5
    end,
    packet:gen_connack(SP, RCv4);
gen_connack_(5, SP, RC) ->
    RCv5 =
        case RC of
            success -> 16#00;
            bad_authn -> 16#86;
            bad_identifier -> 16#85;
            server_unavailable -> 16#88;
            not_authorized -> 16#87
        end,
    SPv5 = case SP of
               true -> 1;
               false -> 0
           end,
    packetv5:gen_connack(SPv5, RCv5).

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
    packetv5:gen_subscribe(Mid, Topics, #{}).

gen_suback(Mid, Exp, Config) ->
    gen_suback_(protover(Config), Mid, Exp).

gen_suback_(4, Mid, RC) ->
    packet:gen_suback(Mid, RC);
gen_suback_(5, Mid, RC) ->
    packetv5:gen_suback(Mid, [RC], #{}).

gen_unsubscribe(MId, Topic, Config) ->
    gen_unsubscribe_(protover(Config), MId, Topic).

gen_unsubscribe_(4, MId, Topic) ->
    packet:gen_unsubscribe(MId, Topic);
gen_unsubscribe_(5, MId, Topic) ->
    packetv5:gen_unsubscribe(MId, [Topic], #{}).

gen_unsuback(Mid, Config) ->
    gen_unsuback_(protover(Config), Mid).

gen_unsuback_(4, Mid) ->
    packet:gen_unsuback(Mid);
gen_unsuback_(5, Mid) ->
    packetv5:gen_unsuback(Mid, [?M5_SUCCESS], #{}).

gen_publish(Topic, QoS, Payload, Opts, Config) ->
    gen_publish_(protover(Config), Topic, QoS, Payload, Opts).

gen_publish_(4, Topic, QoS, Payload, Opts) ->
    packet:gen_publish(Topic, QoS, Payload, Opts);
gen_publish_(5, Topic, QoS, Payload, Opts) ->
    packetv5:gen_publish(Topic, QoS, Payload, Opts).

gen_puback(MId, Config) ->
    gen_puback_(protover(Config), MId).

gen_puback_(4, MId) ->
    packet:gen_puback(MId);
gen_puback_(5, MId) ->
    packetv5:gen_puback(MId).

gen_pubrec(MId, Config) ->
    gen_pubrec_(protover(Config), MId).

gen_pubrec_(4, MId) ->
    packet:gen_pubrec(MId);
gen_pubrec_(5, MId) ->
    packetv5:gen_pubrec(MId).

gen_pubrel(MId, Config) ->
    gen_pubrel_(protover(Config), MId).

gen_pubrel_(4, MId) ->
    packet:gen_pubrel(MId);
gen_pubrel_(5, MId) ->
    packetv5:gen_pubrel(MId).

gen_pubcomp(MId, Config) ->
    gen_pubcomp_(protover(Config), MId).

gen_pubcomp_(4, MId) ->
    packet:gen_pubcomp(MId);
gen_pubcomp_(5, MId) ->
    packetv5:gen_pubcomp(MId).

gen_disconnect(Config) ->
    case protover(Config) of
        4 -> packet:gen_disconnect();
        5 -> packetv5:gen_disconnect()
    end.

ensure_binary(L) when is_list(L) -> list_to_binary(L);
ensure_binary(B) when is_binary(B) -> B;
ensure_binary(empty) -> empty. % for test purposes

protover(Config) ->
    proplists:get_value(protover, Config).
