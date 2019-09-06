-module(vmq_subscribe_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("vmq_commons/include/vmq_types.hrl").

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(_Config) ->
    cover:start(),
    vmq_test_utils:setup(),
    vmq_server_cmd:listener_start(1888, [{allowed_protocol_versions, "3,4,5,131"}]),
    enable_on_subscribe(),
    enable_on_publish(),
    [{ct_hooks, vmq_cth} |_Config].

end_per_suite(_Config) ->
    disable_on_subscribe(),
    disable_on_publish(),
    vmq_test_utils:teardown(),
    _Config.

init_per_testcase(_Case, Config) ->
    vmq_server_cmd:set_config(allow_anonymous, true),
    vmq_server_cmd:set_config(max_client_id_size, 100),
    vmq_server_cmd:set_config(retry_interval, 10),
    Config.

end_per_testcase(_, Config) ->
    Config.

all() ->
    [
     {group, mqttv4},
     {group, mqttv5}
    ].

groups() ->
    Tests =
        [subscribe_qos0_test,
         subscribe_qos1_test,
         subscribe_qos2_test,
         suback_with_nack_test,
         subnack_test,
         unsubscribe_qos0_test,
         unsubscribe_qos1_test,
         unsubscribe_qos2_test,
         subpub_qos0_test,
         subpub_qos1_test,
         subpub_qos2_test,
         resubscribe_test,
         bridge_protocol_retain_as_publish_test,
         bridge_protocol_no_local_test],
    [
     {mqttv4, [shuffle,sequence], Tests},
     {mqttv5, [shuffle],
      [
       subscribe_no_local_test,
       subscribe_illegal_opt,
       subscription_ids
      ]}
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

bridge_protocol_retain_as_publish_test(Cfg) ->
    SubClientId = vmq_cth:ustr(Cfg) ++ "-subscriber",
    Topic = vmq_cth:utopic(Cfg) ++ "/retain_as_pub",
    SubConnect = packet:gen_connect(SubClientId, [{proto_ver, 131}]),
    SubConnack = packet:gen_connack(),
    Subscribe = packet:gen_subscribe(1, Topic, 0),
    SubAck = packet:gen_suback(1, [0]),
    {ok, SubSocket} = packet:do_client_connect(SubConnect, SubConnack, []),
    ok = gen_tcp:send(SubSocket, Subscribe),
    ok = packet:expect_packet(SubSocket, "suback", SubAck),

    PubClientId = vmq_cth:ustr(Cfg) ++ "-publisher",
    PubConnect = packet:gen_connect(PubClientId, [{proto_ver, 4}]),
    PubConnack = packet:gen_connack(),
    {ok, PubSocket} = packet:do_client_connect(PubConnect, PubConnack, []),

    PublishRetain = packet:gen_publish(Topic, 0, <<"msg">>, [{mid, 1},
                                                             {retain, true}]),

    ok = gen_tcp:send(PubSocket, PublishRetain),
    ok = packet:expect_packet(SubSocket, "publish", PublishRetain).

bridge_protocol_no_local_test(Cfg) ->
    ClientId = vmq_cth:ustr(Cfg),
    Topic = vmq_cth:utopic(Cfg),
    Connect = packet:gen_connect(ClientId, [{proto_ver, 131}]),
    Connack = packet:gen_connack(),
    Subscribe = packet:gen_subscribe(1, Topic, 0),
    SubAck = packet:gen_suback(1, [0]),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", SubAck),

    Publish = packet:gen_publish(Topic, 0, <<"msg">>, [{mid, 1}]),

    ok = gen_tcp:send(Socket, Publish),
    %% We shouldn't receive anything.
    {error, timeout} = gen_tcp:recv(Socket, 0, 100).

subscribe_no_local_test(Cfg) ->
    %% Bit 2 of the Subscription Options represents the No Local
    %% option. If the value is 1, Application Messages MUST NOT be
    %% forwarded to a connection with a ClientID equal to the ClientID
    %% of the publishing connection [MQTT-3.8.3-3].

    ClientId = vmq_cth:ustr(Cfg),
    Topic = vmq_cth:utopic(Cfg) ++ "/nolocalfalse",
    TopicNoLocal = vmq_cth:utopic(Cfg) ++ "/nolocaltrue",
    Connect = packetv5:gen_connect(ClientId, [{keepalive, 10}]),
    Connack = packetv5:gen_connack(),
    Disconnect = packetv5:gen_disconnect(),
    NoLocalTrue = true,
    NoLocalFalse = false,
    Rap = false,
    RH = dont_send,

    {ok, Socket} = packetv5:do_client_connect(Connect, Connack, []),

    SubTopic = packetv5:gen_subtopic(Topic, 0, NoLocalFalse, Rap, RH),
    Subscribe = packetv5:gen_subscribe(16, [SubTopic], #{}),
    SubAck0 = packetv5:gen_suback(16, [0], #{}),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packetv5:expect_frame(Socket, SubAck0),

    SubTopicNoLocal = packetv5:gen_subtopic(TopicNoLocal, 0, NoLocalTrue, Rap, RH),
    SubscribeNoLocal = packetv5:gen_subscribe(17, [SubTopicNoLocal], #{}),
    SubAck1 = packetv5:gen_suback(17, [0], #{}),
    ok = gen_tcp:send(Socket, SubscribeNoLocal),
    ok = packetv5:expect_frame(Socket, SubAck1),

    %% NoLocal = false we receive the message
    Publish = packetv5:gen_publish(Topic, 0, <<"msg">>, []),
    ok = gen_tcp:send(Socket, Publish),
    ok = packetv5:expect_frame(Socket, Publish),

    %% NoLocal = true we don't receive the message
    PublishNoLocal = packetv5:gen_publish(TopicNoLocal, 0, <<"msg">>, []),
    ok = gen_tcp:send(Socket, PublishNoLocal),
    {error, timeout} = gen_tcp:recv(Socket, 0, 100),
    ok = gen_tcp:send(Socket, Disconnect),
    ok = gen_tcp:close(Socket).

subscribe_illegal_opt(_) ->
    %% It is a Protocol Error to set the No Local bit to 1 on a Shared
    %% Subscription [MQTT-3.8.3-4].

    %% Bits 6 and 7 of the Subscription Options byte are reserved for
    %% future use. The Server MUST treat a SUBSCRIBE packet as
    %% malformed if any of Reserved bits in the Payload are non-zero
    %% [MQTT-3.8.3-5].
    {skip, not_implemented}.

subscription_ids(Cfg) ->

    %% If the Client specified a Subscription Identifier for any of
    %% the overlapping subscriptions the Server MUST send those
    %% Subscription Identifiers in the message which is published as
    %% the result of the subscriptions [MQTT-3.3.4-3].

    %% If the Server sends a single copy of the message it MUST
    %% include in the PUBLISH packet the Subscription Identifiers for
    %% all matching subscriptions which have a Subscription
    %% Identifiers, their order is not significant [MQTT-3.3.4-4].

    %% If the Server sends multiple PUBLISH packets it MUST send, in
    %% each of them, the Subscription Identifier of the matching
    %% subscription if it has a Subscription Identifier
    %% [MQTT-3.3.4-5].

    %% Note: VerneMQ currently sends multiple publish packets for
    %% overlapping subscriptions. So we adhere to [MQTT-3.3.4-5].

    ClientId = vmq_cth:ustr(Cfg),
    BT = vmq_cth:utopic(Cfg),
    Connect = packetv5:gen_connect(ClientId, []),
    Connack = packetv5:gen_connack(),
    Sub1 =
        packetv5:gen_subscribe(
          5, [packetv5:gen_subtopic(BT ++ "/l1/#",0)], #{p_subscription_id => [5]}),
    Sub2 =
        packetv5:gen_subscribe(
          6, [packetv5:gen_subtopic(BT ++ "/l1/l2",0)], #{p_subscription_id => [6]}),
    Sub3 =
        packetv5:gen_subscribe(
          7, [packetv5:gen_subtopic(BT ++ "/+/t6",0),
              packetv5:gen_subtopic(BT ++ "/t5/t6",0)], #{p_subscription_id => [7]}),
    Sub4 =
        packetv5:gen_subscribe(
          8, [packetv5:gen_subtopic(BT ++ "/no-overlap",0)], #{p_subscription_id => [8]}),
    Sub5 =
        packetv5:gen_subscribe(
          9, [packetv5:gen_subtopic("$share/group/" ++ BT ++ "/no-overlap-ss", 0)], #{p_subscription_id => [9]}),

    SubAck1 = packetv5:gen_suback(5,[0],#{}),
    SubAck2 = packetv5:gen_suback(6,[0],#{}),
    SubAck3 = packetv5:gen_suback(7,[0,0],#{}),
    SubAck4 = packetv5:gen_suback(8,[0],#{}),
    SubAck5 = packetv5:gen_suback(9,[0],#{}),

    {ok, Socket} = packetv5:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Sub1),
    ok = packetv5:expect_frame(Socket, SubAck1),
    ok = gen_tcp:send(Socket, Sub2),
    ok = packetv5:expect_frame(Socket, SubAck2),
    ok = gen_tcp:send(Socket, Sub3),
    ok = packetv5:expect_frame(Socket, SubAck3),
    ok = gen_tcp:send(Socket, Sub4),
    ok = packetv5:expect_frame(Socket, SubAck4),
    ok = gen_tcp:send(Socket, Sub5),
    ok = packetv5:expect_frame(Socket, SubAck5),

    Pub = fun(T,M) ->
                  Publish = packetv5:gen_publish(T, 0, M, []),
                  ok = gen_tcp:send(Socket, Publish)
          end,
    FTopic = fun(Topic) ->
                     %% convert string topic to a frame topic.
                     binary:split(list_to_binary(Topic), <<"/">>, [global])
             end,
    ExpPub =
        fun(T, M, SubIds) ->
                 Publish = packetv5:gen_publish(T, 0, M,
                                                [{properties,
                                                  #{p_subscription_id => SubIds}}]),
                 ok = packetv5:expect_frame(Socket, Publish)
        end,

    %% publish to topic with overlapping subscriptions
    ok = Pub(BT ++ "/l1/l2", <<"msg1">>),
    begin
        Ids = [5,6],
        Topic = FTopic(BT ++ "/l1/l2"),
        {ok, #mqtt5_publish{
                topic = Topic,
                payload = <<"msg1">>,
                properties = #{p_subscription_id := [Id1]}}, SecondFrame} =
            packetv5:receive_frame(Socket),
        Ids1 = Ids -- [Id1],
        {#mqtt5_publish{
            topic = Topic,
            payload = <<"msg1">>,
            properties = #{p_subscription_id := [Id2]}}, <<>>} =
            vmq_parser_mqtt5:parse(SecondFrame),
            [] = Ids1 -- [Id2]
    end,

    %% publish to sub only matchin the wildcard sub
    ok = Pub(BT ++ "/l1/notl2", <<"msg2">>),
    ok = ExpPub(BT ++ "/l1/notl2", <<"msg2">>, [5]),

    %% publish to the two overlapping subs with same sub-id
    ok = Pub(BT ++ "/t5/t6", <<"msg3">>),
    ok = ExpPub(BT ++ "/t5/t6", <<"msg3">>, [7]),
    ok = ExpPub(BT ++ "/t5/t6", <<"msg3">>, [7]),

    %% publish to sub only matching wildcard sub of subs with same
    %% sub-id
    ok = Pub(BT ++ "/nott5/t6", <<"msg4">>),
    ok = ExpPub(BT ++ "/nott5/t6", <<"msg4">>, [7]),

    %% publish to single non overlapping subscription
    ok = Pub(BT ++ "/no-overlap", <<"msg5">>),
    ok = ExpPub(BT ++ "/no-overlap", <<"msg5">>, [8]),

    %% publish to shared subscription with subscription id
    ok = Pub(BT ++ "/no-overlap-ss", <<"msg6">>),
    ok = ExpPub(BT ++ "/no-overlap-ss", <<"msg6">>, [9]),

    {error, timeout} = gen_tcp:recv(Socket, 0, 100).

subscribe_qos0_test(_) ->
    Connect = packet:gen_connect("subscribe-qos0-test", [{keepalive,10}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(53, "qos0/test", 0),
    Suback = packet:gen_suback(53, 0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = gen_tcp:close(Socket).

subscribe_qos1_test(_) ->
    Connect = packet:gen_connect("subscribe-qos1-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(79, "qos1/test", 1),
    Suback = packet:gen_suback(79, 1),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = gen_tcp:close(Socket).

subscribe_qos2_test(_) ->
    Connect = packet:gen_connect("subscribe-qos2-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(3, "qos2/test", 2),
    Suback = packet:gen_suback(3, 2),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = gen_tcp:close(Socket).

suback_with_nack_test(_) ->
    Connect = packet:gen_connect("subscribe-multi1-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(3, [{"qos0/test", 0},
                                         {"qos1/test", 1},
                                         {"qos2/test", 2}]),
    SubackWithNack = packet:gen_suback(3, [0, not_allowed, 2]),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "subackWithNack", SubackWithNack),
    ok = gen_tcp:close(Socket).

subnack_test(_) ->
    Connect = packet:gen_connect("subscribe-multi2-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(3, [{"qos0/test", 0},
                                         {"qos1/test", 1},
                                         {"qos2/test", 2}]),
    SubNack = packet:gen_suback(3, [not_allowed, not_allowed, not_allowed]),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "subNack", SubNack),
    ok = gen_tcp:close(Socket).


unsubscribe_qos0_test(_) ->
    Connect = packet:gen_connect("unsubscribe-qos0-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Unsubscribe = packet:gen_unsubscribe(53, "qos0/test"),
    Unsuback = packet:gen_unsuback(53),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Unsubscribe),
    ok = packet:expect_packet(Socket, "unsuback", Unsuback),
    ok = gen_tcp:close(Socket).

unsubscribe_qos1_test(_) ->
    Connect = packet:gen_connect("unsubscribe-qos1-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Unsubscribe = packet:gen_unsubscribe(79, "qos1/test"),
    Unsuback = packet:gen_unsuback(79),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Unsubscribe),
    ok = packet:expect_packet(Socket, "unsuback", Unsuback),
    ok = gen_tcp:close(Socket).

unsubscribe_qos2_test(_) ->
    Connect = packet:gen_connect("unsubscribe-qos2-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Unsubscribe = packet:gen_unsubscribe(3, "qos2/test"),
    Unsuback = packet:gen_unsuback(3),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Unsubscribe),
    ok = packet:expect_packet(Socket, "unsuback", Unsuback),
    ok = gen_tcp:close(Socket).

subpub_qos0_test(_) ->
    Connect = packet:gen_connect("subpub-qos0-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(53, "subpub/qos0", 0),
    Suback = packet:gen_suback(53, 0),
    Publish = packet:gen_publish("subpub/qos0", 0, <<"message">>, []),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = gen_tcp:send(Socket, Publish),
    ok = packet:expect_packet(Socket, "publish", Publish),
    ok = gen_tcp:close(Socket).

subpub_qos1_test(_) ->
    Connect = packet:gen_connect("subpub-qos1-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(530, "subpub/qos1", 1),
    Suback = packet:gen_suback(530, 1),
    Publish1 = packet:gen_publish("subpub/qos1", 1, <<"message">>, [{mid, 300}]),
    Puback1 = packet:gen_puback(300),
    Publish2 = packet:gen_publish("subpub/qos1", 1, <<"message">>, [{mid, 1}]),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = gen_tcp:send(Socket, Publish1),
    ok = packet:expect_packet(Socket, "puback", Puback1),
    ok = packet:expect_packet(Socket, "publish", Publish2),
    ok = gen_tcp:close(Socket).

subpub_qos2_test(_) ->
    Connect = packet:gen_connect("subpub-qos2-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(530, "subpub/qos2", 2),
    Suback = packet:gen_suback(530, 2),
    Publish1 = packet:gen_publish("subpub/qos2", 2, <<"message">>, [{mid, 301}]),
    Pubrec1 = packet:gen_pubrec(301),
    Pubrel1 = packet:gen_pubrel(301),
    Pubcomp1 = packet:gen_pubcomp(301),
    Publish2 = packet:gen_publish("subpub/qos2", 2, <<"message">>, [{mid, 1}]),
    Pubrec2 = packet:gen_pubrec(1),
    Pubrel2 = packet:gen_pubrel(1),

    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = gen_tcp:send(Socket, Publish1),
    ok = packet:expect_packet(Socket, "pubrec", Pubrec1),
    ok = packet:expect_packet(Socket, "publish2", Publish2),
    ok = gen_tcp:send(Socket, Pubrel1),
    ok = packet:expect_packet(Socket, "pubcomp", Pubcomp1),
    ok = gen_tcp:send(Socket, Pubrec2),
    ok = packet:expect_packet(Socket, "pubrel2", Pubrel2),
    ok = gen_tcp:close(Socket).

resubscribe_test(_) ->
    %% test that we can override a subscription
    Connect = packet:gen_connect("sub-override-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(530, "sub/override/qos", 0),
    Suback = packet:gen_suback(530, 0),
    ReSubscribe = packet:gen_subscribe(531, "sub/override/qos", 1),
    ReSuback = packet:gen_suback(531, 1),
    Publish1 = packet:gen_publish("sub/override/qos", 1, <<"message">>, [{mid, 1}]),
    Puback = packet:gen_puback(1),
    Publish0 = packet:gen_publish("sub/override/qos", 0, <<"message">>, []),


    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    %% max qos is 0, receive qos0 publish
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = gen_tcp:send(Socket, Publish1),
    ok = packet:expect_packet(Socket, "puback", Puback),
    ok = packet:expect_packet(Socket, "publish0", Publish0),

    %% max qos is 1, receive qos1 publish
    ok = gen_tcp:send(Socket, ReSubscribe),
    ok = packet:expect_packet(Socket, "suback", ReSuback),
    ok = gen_tcp:send(Socket, Publish1),
    ok = packet:expect_packet(Socket, "puback", Puback),
    ok = packet:expect_packet(Socket, "publish1", Publish1),
    ok = gen_tcp:send(Socket, Puback),

    ok = gen_tcp:close(Socket).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
hook_auth_on_subscribe(_,{"", <<"subscribe-multi1-test">>},
                       [{[<<"qos0">>,<<"test">>], 0},
                        {[<<"qos1">>,<<"test">>], 1},
                        {[<<"qos2">>,<<"test">>], 2}]) ->
    {ok, [{[<<"qos0">>,<<"test">>], 0},
          {[<<"qos1">>,<<"test">>], not_allowed},
          {[<<"qos2">>,<<"test">>], 2}]};
hook_auth_on_subscribe(_,{"", <<"subscribe-multi2-test">>},
                       [{[<<"qos0">>,<<"test">>], 0},
                        {[<<"qos1">>,<<"test">>], 1},
                        {[<<"qos2">>,<<"test">>], 2}]) ->
    {error, not_allowed};
hook_auth_on_subscribe(_,_,_) -> ok.

hook_auth_on_publish(_, _, _, _, _, _) -> ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Helper
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
enable_on_subscribe() ->
    ok = vmq_plugin_mgr:enable_module_plugin(
           auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3),
    ok = vmq_plugin_mgr:enable_module_plugin(
           auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3,
           [{compat, {auth_on_subscribe_m5, vmq_plugin_compat_m5,
                     convert, 4}}]).
enable_on_publish() ->
    ok = vmq_plugin_mgr:enable_module_plugin(
           auth_on_publish, ?MODULE, hook_auth_on_publish, 6),
    ok = vmq_plugin_mgr:enable_module_plugin(
           auth_on_publish, ?MODULE, hook_auth_on_publish, 6,
           [{compat, {auth_on_publish_m5, vmq_plugin_compat_m5,
                    convert, 7}}]).
disable_on_subscribe() ->
    ok = vmq_plugin_mgr:disable_module_plugin(
           auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3),
    ok = vmq_plugin_mgr:disable_module_plugin(
           auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3,
           [{compat, {auth_on_subscribe_m5, vmq_plugin_compat_m5,
                      convert, 4}}]).
disable_on_publish() ->
    ok = vmq_plugin_mgr:disable_module_plugin(
           auth_on_publish, ?MODULE, hook_auth_on_publish, 6),
    ok = vmq_plugin_mgr:disable_module_plugin(
           auth_on_publish, ?MODULE, hook_auth_on_publish, 6,
           [{compat, {auth_on_publish_m5, vmq_plugin_compat_m5,
                      convert, 7}}]).
