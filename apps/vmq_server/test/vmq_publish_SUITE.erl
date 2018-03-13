-module(vmq_publish_SUITE).

-include_lib("vmq_commons/include/vmq_types_mqtt5.hrl").

-compile(export_all).
-compile(nowarn_export_all).

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(Config) ->
    S = vmq_test_utils:get_suite_rand_seed(),
    cover:start(),
    vmq_test_utils:setup(),
    vmq_server_cmd:listener_start(1888, []),
    [S|Config].

end_per_suite(_Config) ->
    vmq_server_cmd:listener_stop(1888, "127.0.0.1", false),
    vmq_test_utils:teardown(),
    _Config.

init_per_group(mqttv3, Config) ->
    Config;
init_per_group(mqttv4, Config) ->
    [{protover, 4}|Config];
init_per_group(mqttv5, Config) ->
    [{protover, 5}|Config].

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    vmq_test_utils:seed_rand(Config),
    vmq_server_cmd:set_config(allow_anonymous, true),
    vmq_server_cmd:set_config(retry_interval, 2),
    vmq_server_cmd:set_config(max_client_id_size, 100),
    vmq_server_cmd:set_config(topic_alias_max, 0),
    Config.

end_per_testcase(_, Config) ->
    Config.

all() ->
    [
     {group, mqttv3},
     {group, mqttv4},
     {group, mqttv5}
    ].

groups() ->
    V4V5Tests =
        [publish_qos1_test,
         publish_qos2_test,
         publish_b2c_disconnect_qos1_test,
         publish_b2c_disconnect_qos2_test,
         publish_b2c_timeout_qos1_test,
         publish_b2c_timeout_qos2_test,
         publish_c2b_disconnect_qos2_test,
         publish_c2b_timeout_qos2_test,
         pattern_matching_test
        ],
    [
     {mqttv3, [shuffle,sequence], [
                   not_allowed_publish_close_qos0_mqtt_3_1,
                   not_allowed_publish_close_qos1_mqtt_3_1,
                   not_allowed_publish_close_qos2_mqtt_3_1,
                   message_size_exceeded_close
                  ]},
     {mqttv4, [shuffle,sequence], [
                   not_allowed_publish_close_qos0_mqtt_3_1_1,
                   not_allowed_publish_close_qos1_mqtt_3_1_1,
                   not_allowed_publish_close_qos2_mqtt_3_1_1,
                   message_size_exceeded_close,
                   shared_subscription_offline,
                   shared_subscription_online_first
                  ] ++  V4V5Tests },
     {mqttv5, [sequence],
      [message_expiry,
       publish_c2b_topic_alias,
       forward_properties|
       V4V5Tests]}
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
publish_qos1_test(Config) ->
    Connect = mqtt5_v4compat:gen_connect("pub-qos1-test", [{keepalive, 60}], Config),
    Connack = mqtt5_v4compat:gen_connack(success, Config),
    Publish = mqtt5_v4compat:gen_publish("pub/qos1/test", 1, <<"message">>,
                                         [{mid, 19}], Config),
    Puback = mqtt5_v4compat:gen_puback(19, Config),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Config),
    enable_on_publish(),
    ok = gen_tcp:send(Socket, Publish),
    ok = mqtt5_v4compat:expect_packet(Socket, "puback", Puback, Config),
    disable_on_publish(),
    ok = gen_tcp:close(Socket).

publish_qos2_test(Config) ->
    Connect = mqtt5_v4compat:gen_connect("pub-qos2-test", [{keepalive, 60}], Config),
    Connack = mqtt5_v4compat:gen_connack(success, Config),
    Publish = mqtt5_v4compat:gen_publish("pub/qos2/test", 2, <<"message">>,
                                 [{mid, 312}], Config),
    Pubrec = mqtt5_v4compat:gen_pubrec(312, Config),
    Pubrel = mqtt5_v4compat:gen_pubrel(312, Config),
    Pubcomp = mqtt5_v4compat:gen_pubcomp(312, Config),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Config),
    enable_on_publish(),
    ok = gen_tcp:send(Socket, Publish),
    ok = mqtt5_v4compat:expect_packet(Socket, "pubrec", Pubrec, Config),
    ok = gen_tcp:send(Socket, Pubrel),
    ok = mqtt5_v4compat:expect_packet(Socket, "pubcomp", Pubcomp, Config),
    disable_on_publish(),
    ok = gen_tcp:close(Socket).


publish_b2c_disconnect_qos1_test(Config) ->
    ClientId = mqtt5_v4compat:groupify("pub-qos1-disco-test", Config),
    Connect = mqtt5_v4compat:gen_connect(ClientId,
                                         [{keepalive, 60}, {clean_session, false}], Config),
    Connack1 = mqtt5_v4compat:gen_connack(success, Config),
    Connack2 = mqtt5_v4compat:gen_connack(true, success, Config),
    Subscribe = mqtt5_v4compat:gen_subscribe(3265, "qos1/disconnect/test", 1, Config),
    Suback = mqtt5_v4compat:gen_suback(3265, 1, Config),
    Publish = mqtt5_v4compat:gen_publish("qos1/disconnect/test", 1,
                                         <<"disconnect-message">>, [{mid, 1}], Config),
    PublishDup = mqtt5_v4compat:gen_publish("qos1/disconnect/test", 1,
                                            <<"disconnect-message">>,
                                            [{mid, 1}, {dup, true}], Config),
    Puback = mqtt5_v4compat:gen_puback(1, Config),
    Publish2 = mqtt5_v4compat:gen_publish("qos1/outgoing", 1,
                                  <<"outgoing-message">>, [{mid, 3266}], Config),
    enable_on_publish(),
    enable_on_subscribe(),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack1, [], Config),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "suback", Suback, Config),
    helper_pub_qos1("test-helper", 1, Publish, Config),
    %% should have now received a publish command
    ok = mqtt5_v4compat:expect_packet(Socket, "publish", Publish, Config),
    %% expect packet, but we don't ack
    %% send our outgoing message, when we disconnect the broker
    %% should get rid of it and ssume we're going to retry
    ok = gen_tcp:send(Socket, Publish2),
    ok = gen_tcp:close(Socket),
    %% TODO: assert Publish2 deleted on broker
    %% reconnect
    {ok, Socket1} = mqtt5_v4compat:do_client_connect(Connect, Connack2, [], Config),
    ok = mqtt5_v4compat:expect_packet(Socket1, "dup publish", PublishDup, Config),
    ok = gen_tcp:send(Socket1, Puback),
    disable_on_publish(),
    disable_on_subscribe(),
    ok = gen_tcp:close(Socket1).

publish_b2c_disconnect_qos2_test(Config) ->
    ClientId = mqtt5_v4compat:groupify("pub-b2c-qos2-disco-test", Config),
    Connect = mqtt5_v4compat:gen_connect(ClientId,
                                 [{keepalive, 60}, {clean_session, false}], Config),
    Connack1 = mqtt5_v4compat:gen_connack(success, Config),
    Connack2 = mqtt5_v4compat:gen_connack(true, success, Config),
    Subscribe = mqtt5_v4compat:gen_subscribe(3265, "qos2/disconnect/test", 2, Config),
    Suback = mqtt5_v4compat:gen_suback(3265, 2, Config),
    Publish = mqtt5_v4compat:gen_publish("qos2/disconnect/test", 2,
                                 <<"disconnect-message">>, [{mid, 1}], Config),
    PublishDup = mqtt5_v4compat:gen_publish("qos2/disconnect/test", 2,
                                    <<"disconnect-message">>,
                                    [{mid, 1}, {dup, true}], Config),
    Pubrel = mqtt5_v4compat:gen_pubrel(1, Config),
    Pubrec = mqtt5_v4compat:gen_pubrec(1, Config),
    Pubcomp = mqtt5_v4compat:gen_pubcomp(1, Config),
    Publish2 = mqtt5_v4compat:gen_publish("qos1/outgoing", 1,
                                  <<"outgoing-message">>, [{mid, 3266}], Config),
    enable_on_publish(),
    enable_on_subscribe(),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack1, [], Config),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "suback", Suback, Config),
    helper_pub_qos2("test-helper", 1, Publish, Config),
    %% should have now received a publish command
    ok = mqtt5_v4compat:expect_packet(Socket, "publish", Publish, Config),
    %% expect packet, but we don't ack
    %% send our outgoing message, when we disconnect the broker
    %% should get rid of it and ssume we're going to retry
    ok = gen_tcp:send(Socket, Publish2),
    ok = gen_tcp:close(Socket),
    %% TODO: assert Publish2 deleted on broker
    %% reconnect
    {ok, Socket1} = mqtt5_v4compat:do_client_connect(Connect, Connack2, [], Config),
    ok = mqtt5_v4compat:expect_packet(Socket1, "dup publish", PublishDup, Config),
    ok = gen_tcp:send(Socket1, Pubrec),
    ok = mqtt5_v4compat:expect_packet(Socket1, "pubrel", Pubrel, Config),
    ok = gen_tcp:close(Socket1),
    %% Expect Pubrel
    {ok, Socket2} = mqtt5_v4compat:do_client_connect(Connect, Connack2, [], Config),
    ok = mqtt5_v4compat:expect_packet(Socket2, "pubrel", Pubrel, Config),
    ok = gen_tcp:send(Socket2, Pubcomp),
    disable_on_publish(),
    disable_on_subscribe(),
    ok = gen_tcp:close(Socket2).

publish_b2c_timeout_qos1_test(Config) ->
    Connect = mqtt5_v4compat:gen_connect("pub-qos1-timeout-test", [{keepalive, 60}], Config),
    Connack = mqtt5_v4compat:gen_connack(success, Config),
    Subscribe = mqtt5_v4compat:gen_subscribe(3265, "qos1/timeout/test", 1, Config),
    Suback = mqtt5_v4compat:gen_suback(3265, 1, Config),
    Publish = mqtt5_v4compat:gen_publish("qos1/timeout/test", 1,
                                 <<"timeout-message">>, [{mid, 1}], Config),
    PublishDup = mqtt5_v4compat:gen_publish("qos1/timeout/test", 1,
                                    <<"timeout-message">>,
                                    [{mid, 1}, {dup, true}], Config),
    Puback = mqtt5_v4compat:gen_puback(1, Config),
    enable_on_publish(),
    enable_on_subscribe(),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Config),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "suback", Suback, Config),
    helper_pub_qos1("test-helper", 1, Publish, Config),
    %% should have now received a publish command
    ok = mqtt5_v4compat:expect_packet(Socket, "publish", Publish, Config),
    %% expect packet, but we don't ack
    %% The broker should repeat the PUBLISH with dup set
    ok = mqtt5_v4compat:expect_packet(Socket, "dup publish", PublishDup, Config),
    ok = gen_tcp:send(Socket, Puback),
    disable_on_publish(),
    disable_on_subscribe(),
    ok = gen_tcp:close(Socket).

publish_b2c_timeout_qos2_test(Config) ->
    Connect = mqtt5_v4compat:gen_connect("pub-b2c-qos2-timeout-test", [{keepalive, 60}], Config),
    Connack = mqtt5_v4compat:gen_connack(success, Config),
    Subscribe = mqtt5_v4compat:gen_subscribe(3265, "qos2/timeout/test", 2, Config),
    Suback = mqtt5_v4compat:gen_suback(3265, 2, Config),
    Publish = mqtt5_v4compat:gen_publish("qos2/timeout/test", 2,
                                 <<"timeout-message">>, [{mid, 1}], Config),
    PublishDup = mqtt5_v4compat:gen_publish("qos2/timeout/test", 2,
                                    <<"timeout-message">>,
                                    [{mid, 1}, {dup, true}], Config),
    Pubrec = mqtt5_v4compat:gen_pubrec(1, Config),
    Pubrel = mqtt5_v4compat:gen_pubrel(1, Config),
    Pubcomp = mqtt5_v4compat:gen_pubcomp(1, Config),
    enable_on_publish(),
    enable_on_subscribe(),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Config),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "suback", Suback, Config),
    helper_pub_qos2("test-helper", 1, Publish, Config),
    %% should have now received a publish command
    ok = mqtt5_v4compat:expect_packet(Socket, "publish", Publish, Config),
    %% expect packet, but we don't ack
    %% The broker should repeat the PUBLISH with dup set
    ok = mqtt5_v4compat:expect_packet(Socket, "dup publish", PublishDup, Config),
    ok = gen_tcp:send(Socket, Pubrec),
    ok = mqtt5_v4compat:expect_packet(Socket, "pubrel", Pubrel, Config),
    %% The broker should repeat the PUBREL NO dup set according to MQTT-3.6.1-1
    ok = mqtt5_v4compat:expect_packet(Socket, "pubrel", Pubrel, Config),
    ok = gen_tcp:send(Socket, Pubcomp),
    disable_on_publish(),
    disable_on_subscribe(),
    ok = gen_tcp:close(Socket).

publish_c2b_disconnect_qos2_test(Config) ->
    ClientId = mqtt5_v4compat:groupify("pub-c2b-qos2-disco-test", Config),
    Connect = mqtt5_v4compat:gen_connect(ClientId,
                                         [{keepalive, 60}, {clean_session, false}], Config),
    Connack1 = mqtt5_v4compat:gen_connack(success, Config),
    Connack2 = mqtt5_v4compat:gen_connack(true, success, Config),
    Publish = mqtt5_v4compat:gen_publish("qos2/disconnect/test", 2,
                                         <<"disconnect-message">>, [{mid, 1}], Config),
    PublishDup = mqtt5_v4compat:gen_publish("qos2/disconnect/test", 2,
                                            <<"disconnect-message">>,
                                            [{mid, 1}, {dup, true}], Config),
    Pubrec = mqtt5_v4compat:gen_pubrec(1, Config),
    Pubrel = mqtt5_v4compat:gen_pubrel(1, Config),
    Pubcomp = mqtt5_v4compat:gen_pubcomp(1, Config),
    enable_on_publish(),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack1, [], Config),
    ok = gen_tcp:send(Socket, Publish),
    ok = mqtt5_v4compat:expect_packet(Socket, "pubrec", Pubrec, Config),
    %% We're now going to disconnect and pretend we didn't receive the pubrec
    ok = gen_tcp:close(Socket),
    {ok, Socket1} = mqtt5_v4compat:do_client_connect(Connect, Connack2, [], Config),
    ok = gen_tcp:send(Socket1, PublishDup),
    ok = mqtt5_v4compat:expect_packet(Socket1, "pubrec", Pubrec, Config),
    ok = gen_tcp:send(Socket1, Pubrel),
    ok = mqtt5_v4compat:expect_packet(Socket1, "pubcomp", Pubcomp, Config),
    %% Again, pretend we didn't receive this pubcomp
    ok = gen_tcp:close(Socket1),
    {ok, Socket2} = mqtt5_v4compat:do_client_connect(Connect, Connack2, [], Config),
    ok = gen_tcp:send(Socket2, Pubrel),
    ok = mqtt5_v4compat:expect_packet(Socket2, "pubcomp", Pubcomp, Config),
    disable_on_publish(),
    ok = gen_tcp:close(Socket2).

publish_c2b_timeout_qos2_test(Config) ->
    Connect = mqtt5_v4compat:gen_connect("pub-c2b-qos2-timeout-test", [{keepalive, 60}], Config),
    Connack = mqtt5_v4compat:gen_connack(success, Config),
    Publish = mqtt5_v4compat:gen_publish("pub/qos2/test", 2,
                                 <<"timeout-message">>, [{mid, 1926}], Config),
    Pubrec = mqtt5_v4compat:gen_pubrec(1926, Config),
    Pubrel = mqtt5_v4compat:gen_pubrel(1926, Config),
    Pubcomp = mqtt5_v4compat:gen_pubcomp(1926, Config),
    enable_on_publish(),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Config),
    ok = gen_tcp:send(Socket, Publish),
    ok = mqtt5_v4compat:expect_packet(Socket, "pubrec", Pubrec, Config),
    %% The broker should repeat the PUBREC
    ok = mqtt5_v4compat:expect_packet(Socket, "pubrec", Pubrec, Config),
    ok = gen_tcp:send(Socket, Pubrel),
    ok = mqtt5_v4compat:expect_packet(Socket, "pubcomp", Pubcomp, Config),
    disable_on_publish(),
    ok = gen_tcp:close(Socket).

pattern_matching_test(Config) ->
    ok = pattern_test("#", "test/topic", Config),
    ok = pattern_test("#", "/test/topic", Config),
    ok = pattern_test("foo/#", "foo/bar/baz", Config),
    ok = pattern_test("foo/+/baz", "foo/bar/baz", Config),
    ok = pattern_test("foo/+/baz/#", "foo/bar/baz", Config),
    ok = pattern_test("foo/+/baz/#", "foo/bar/baz/bar", Config),
    ok = pattern_test("foo/foo/baz/#", "foo/foo/baz/bar", Config),
    ok = pattern_test("foo/#", "foo", Config),
    ok = pattern_test("/#", "/foo", Config),
    ok = pattern_test("test/topic/", "test/topic/", Config),
    ok = pattern_test("test/topic/+", "test/topic/", Config),
    ok = pattern_test("+/+/+/+/+/+/+/+/+/+/test",
                      "one/two/three/four/five/six/seven/eight/nine/ten/test", Config),
    ok = pattern_test("#", "test////a//topic", Config),
    ok = pattern_test("#", "/test////a//topic", Config),
    ok = pattern_test("foo/#", "foo//bar///baz", Config),
    ok = pattern_test("foo/+/baz", "foo//baz", Config),
    ok = pattern_test("foo/+/baz//", "foo//baz//", Config),
    ok = pattern_test("foo/+/baz/#", "foo//baz", Config),
    ok = pattern_test("foo/+/baz/#", "foo//baz/bar", Config),
    ok = pattern_test("foo//baz/#", "foo//baz/bar", Config),
    ok = pattern_test("foo/foo/baz/#", "foo/foo/baz/bar", Config),
    ok = pattern_test("/#", "////foo///bar", Config).

pattern_test(SubTopic, PubTopic, Config) ->
    Connect = mqtt5_v4compat:gen_connect("pattern-sub-test", [{keepalive, 60}], Config),
    Connack = mqtt5_v4compat:gen_connack(success, Config),
    Publish = mqtt5_v4compat:gen_publish(PubTopic, 0, <<"message">>, [], Config),
    PublishRetained = mqtt5_v4compat:gen_publish(PubTopic, 0, <<"message">>,
                                         [{retain, true}], Config),
    Subscribe = mqtt5_v4compat:gen_subscribe(312, SubTopic, 0, Config),
    Suback = mqtt5_v4compat:gen_suback(312, 0, Config),
    Unsubscribe = mqtt5_v4compat:gen_unsubscribe(234, SubTopic, Config),
    Unsuback = mqtt5_v4compat:gen_unsuback(234, Config),
    enable_on_publish(),
    enable_on_subscribe(),
    vmq_test_utils:reset_tables(),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Config),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "suback", Suback, Config),
    helper_pattern_matching(PubTopic, Config),
    ok = mqtt5_v4compat:expect_packet(Socket, "publish", Publish, Config),
    ok = gen_tcp:send(Socket, Unsubscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "unsuback", Unsuback, Config),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "suback", Suback, Config),
    ok = mqtt5_v4compat:expect_packet(Socket, "publish retained", PublishRetained, Config),
    disable_on_publish(),
    disable_on_subscribe(),
    ok = gen_tcp:close(Socket).

not_allowed_publish_close_qos0_mqtt_3_1(_) ->
    Connect = packet:gen_connect("pattern-sub-test", [{keepalive, 60}]),
    Connack = packet:gen_connack(0),
    Topic = "test/topic/not_allowed",
    Publish = packet:gen_publish(Topic, 0, <<"message">>, []),
    vmq_test_utils:reset_tables(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    gen_tcp:send(Socket, Publish),
    {error, timeout} = gen_tcp:recv(Socket, 0, 1000),
    gen_tcp:close(Socket).

not_allowed_publish_close_qos1_mqtt_3_1(_) ->
    Connect = packet:gen_connect("pattern-sub-test", [{keepalive, 60}]),
    Connack = packet:gen_connack(0),
    Topic = "test/topic/not_allowed",
    Publish = packet:gen_publish(Topic, 1, <<"message">>, [{mid, 1}]),
    Puback = packet:gen_puback(1),
    vmq_test_utils:reset_tables(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    gen_tcp:send(Socket, Publish),
    ok = packet:expect_packet(Socket, "puback", Puback),
    {error, timeout} = gen_tcp:recv(Socket, 0, 1000),
    gen_tcp:close(Socket).

not_allowed_publish_close_qos2_mqtt_3_1(_) ->
    Connect = packet:gen_connect("pattern-sub-test", [{keepalive, 60}]),
    Connack = packet:gen_connack(0),
    Topic = "test/topic/not_allowed",
    Publish = packet:gen_publish(Topic, 2, <<"message">>, [{mid, 1}]),
    Pubrec = packet:gen_pubrec(1),
    vmq_test_utils:reset_tables(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    gen_tcp:send(Socket, Publish),
    %% we receive proper pubrec
    ok = packet:expect_packet(Socket, "pubrec", Pubrec),
    gen_tcp:close(Socket).

not_allowed_publish_close_qos0_mqtt_3_1_1(_) ->
    Connect = packet:gen_connect("pattern-sub-test", [{keepalive, 60},
                                                      {proto_ver, 4}]),
    Connack = packet:gen_connack(0),
    Topic = "test/topic/not_allowed",
    Publish = packet:gen_publish(Topic, 0, <<"message">>, []),
    vmq_test_utils:reset_tables(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    gen_tcp:send(Socket, Publish),
    {error, closed} = gen_tcp:recv(Socket, 0, 1000).

not_allowed_publish_close_qos1_mqtt_3_1_1(_) ->
    Connect = packet:gen_connect("pattern-sub-test", [{keepalive, 60},
                                                      {proto_ver, 4}]),
    Connack = packet:gen_connack(0),
    Topic = "test/topic/not_allowed",
    Publish = packet:gen_publish(Topic, 1, <<"message">>, [{mid, 1}]),
    vmq_test_utils:reset_tables(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    gen_tcp:send(Socket, Publish),
    {error, closed} = gen_tcp:recv(Socket, 0, 1000).

not_allowed_publish_close_qos2_mqtt_3_1_1(_) ->
    Connect = packet:gen_connect("pattern-sub-test", [{keepalive, 60},
                                                      {proto_ver, 4}]),
    Connack = packet:gen_connack(0),
    Topic = "test/topic/not_allowed",
    Publish = packet:gen_publish(Topic, 2, <<"message">>, [{mid, 1}]),
    vmq_test_utils:reset_tables(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    gen_tcp:send(Socket, Publish),
    {error, closed} = gen_tcp:recv(Socket, 0, 1000).

message_size_exceeded_close(_) ->
    OldLimit = vmq_config:get_env(max_message_size),
    vmq_config:set_env(max_message_size, 1024, false),
    vmq_metrics:reset_counters(),
    Connect = packet:gen_connect("pub-excessive-test", [{keepalive, 60}]),
    Connack = packet:gen_connack(0),
    Publish = packet:gen_publish("pub/excessive/test", 0, vmq_test_utils:rand_bytes(1024),
                                 [{mid, 19}]),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    enable_on_publish(),
    ok = gen_tcp:send(Socket, Publish),
    {error, closed} = gen_tcp:recv(Socket, 0, 1000),
    true = lists:member({counter, mqtt_invalid_msg_size_error, 1}, vmq_metrics:metrics()),
    vmq_config:set_env(max_message_size, OldLimit, false),
    disable_on_publish().

shared_subscription_offline(_) ->
    enable_on_publish(),
    enable_on_subscribe(),
    Connack = packet:gen_connack(0),
    PubConnect = packet:gen_connect("single-offline-pub", [{keepalive, 60},
                                                           {proto_ver, 4}]),
    SubConnectOffline = packet:gen_connect("single-offline-sha-sub", [{keepalive, 60},
                                                                      {proto_ver, 4},
                                                                      {clean_session, false}]),
    Subscription = "$share/singleofflinesub/shared_sub_topic",
    {ok, PubSocket} = packet:do_client_connect(PubConnect, Connack, []),
    {ok, SubSocketOffline} = packet:do_client_connect(SubConnectOffline, Connack, []),
    Subscribe = packet:gen_subscribe(1, Subscription, 1),
    Suback = packet:gen_suback(1, 1),
    ok = gen_tcp:send(SubSocketOffline, Subscribe),
    ok = packet:expect_packet(SubSocketOffline, "suback", Suback),
    Disconnect = packet:gen_disconnect(),
    ok = gen_tcp:send(SubSocketOffline, Disconnect),

    PubFun
        = fun(Socket, Mid) ->
                  Publish = packet:gen_publish("shared_sub_topic", 1,
                                               vmq_test_utils:rand_bytes(1024),
                                               [{mid, Mid}]),
                  Puback = packet:gen_puback(Mid),
                  ok = gen_tcp:send(Socket, Publish),
                  ok = packet:expect_packet(Socket, "puback", Puback),
                  {Mid, Publish}
          end,
    Published = [PubFun(PubSocket, Mid) || Mid <- lists:seq(1,10)],
    ConnackSP = packet:gen_connack(1, 0),
    {ok, SubSocketOffline2} = packet:do_client_connect(SubConnectOffline, ConnackSP, []),
    [begin
         ok = packet:expect_packet(SubSocketOffline2, "publish", Expect),
         Puback = packet:gen_puback(Mid),
         ok = gen_tcp:send(SubSocketOffline2, Puback)
     end || {Mid, Expect} <- Published ],
    disable_on_publish(),
    disable_on_subscribe().

shared_subscription_online_first(_) ->
    enable_on_publish(),
    enable_on_subscribe(),
    Connack = packet:gen_connack(0),
    PubConnect = packet:gen_connect("shared-sub-pub", [{keepalive, 60},
                                                       {proto_ver, 4}]),
    SubConnectOnline = packet:gen_connect("shared-sub-sub-online", [{keepalive, 60},
                                                                    {proto_ver, 4},
                                                                    {clean_session, false}]),
    SubConnectOffline = packet:gen_connect("shared-sub-sub-offline", [{keepalive, 60},
                                                                      {proto_ver, 4},
                                                                      {clean_session, false}]),
    Subscription = "$share/group/shared_sub_topic",
    {ok, PubSocket} = packet:do_client_connect(PubConnect, Connack, []),
    {ok, SubSocketOnline} = packet:do_client_connect(SubConnectOnline, Connack, []),
    {ok, SubSocketOffline} = packet:do_client_connect(SubConnectOffline, Connack, []),
    Subscribe = packet:gen_subscribe(1, Subscription, 1),
    Suback = packet:gen_suback(1, 1),
    ok = gen_tcp:send(SubSocketOffline, Subscribe),
    ok = packet:expect_packet(SubSocketOffline, "suback", Suback),
    ok = gen_tcp:send(SubSocketOnline, Subscribe),
    ok = packet:expect_packet(SubSocketOnline, "suback", Suback),
    Disconnect = packet:gen_disconnect(),
    ok = gen_tcp:send(SubSocketOffline, Disconnect),

    PubFun
        = fun(Socket, Mid) ->
                  Publish = packet:gen_publish("shared_sub_topic", 1,
                                               vmq_test_utils:rand_bytes(1024),
                                               [{mid, Mid}]),
                  Puback = packet:gen_puback(Mid),
                  ok = gen_tcp:send(Socket, Publish),
                  ok = packet:expect_packet(Socket, "puback", Puback),
                  {Mid, Publish}
          end,
    Published = [PubFun(PubSocket, Mid) || Mid <- lists:seq(1,10)],
    %% since all messages should end up with the online subscriber, we
    %% can check they arrived one by one in the publish order.
    [begin
         ok = packet:expect_packet(SubSocketOnline, "publish", Expect),
         Puback = packet:gen_puback(Mid),
         ok = gen_tcp:send(SubSocketOnline, Puback)
     end || {Mid, Expect} <- Published ],
    disable_on_publish(),
    disable_on_subscribe().

message_expiry(_) ->
    %% If the Message Expiry Interval has passed and the Server has
    %% not managed to start onward delivery to a matching subscriber,
    %% then it MUST delete the copy of the message for that subscriber
    %% [MQTT-3.3.2-5].

    %% The PUBLISH packet sent to a Client by the Server MUST contain
    %% a Message Expiry Interval set to thereceived value minus the
    %% time that the Application Message has been waiting in the
    %% Server [MQTT-3.3.2-6].
    enable_on_publish(),
    enable_on_subscribe(),

    %% set up subscriber
    SubConnect = packetv5:gen_connect("message-expiry-sub", [{keepalive, 60},
                                                             {clean_start,false}]),
    Connack = packetv5:gen_connack(?M5_CONNACK_ACCEPT),
    {ok, SubSocket} = packetv5:do_client_connect(SubConnect, Connack, []),
    SubTopic60s = packetv5:gen_subtopic(<<"message/expiry/60s">>, 1),
    SubTopic1s = packetv5:gen_subtopic(<<"message/expiry/1s">>, 1),
    SubscribeAll = packetv5:gen_subscribe(10, [SubTopic60s, SubTopic1s], #{}),
    ok = gen_tcp:send(SubSocket, SubscribeAll),

    SubAck = packetv5:gen_suback(10, [1,1], #{}),
    ok = packetv5:expect_frame(SubSocket, SubAck),
    Disconnect = packetv5:gen_disconnect(),
    ok = gen_tcp:send(SubSocket, Disconnect),
    ok = gen_tcp:close(SubSocket),

    %% set up publisher
    PubConnect = packetv5:gen_connect("message-expiry-pub", [{keepalive, 60}]),
    {ok, PubSocket} = packetv5:do_client_connect(PubConnect, Connack, []),

    %% Publish some messages
    Expiry60s = #{p_message_expiry_interval => 60},
    PE60s = packetv5:gen_publish(<<"message/expiry/60s">>, 1, <<"e60s">>,
                                 [{properties, Expiry60s}, {mid, 0}]),
    ok = gen_tcp:send(PubSocket, PE60s),
    Puback0 = packetv5:gen_puback(0),
    ok = packetv5:expect_frame(PubSocket, Puback0),

    Expiry1s = #{p_message_expiry_interval => 1},
    PE1s = packetv5:gen_publish(<<"message/expiry/1s">>, 1, <<"e1s">>,
                                [{properties, Expiry1s}, {mid, 1}]),
    ok = gen_tcp:send(PubSocket, PE1s),
    Puback1 = packetv5:gen_puback(1),
    ok = packetv5:expect_frame(PubSocket, Puback1),
    ok = gen_tcp:close(PubSocket),

    %% Wait a bit to ensure the messages will have been held long
    %% enough in the queue of the offline session
    timer:sleep(1100),

    %% reconnect subscriber
    ConnackSP = packetv5:gen_connack(1, ?M5_CONNACK_ACCEPT),
    {ok, SubSocket1} = packetv5:do_client_connect(SubConnect, ConnackSP, []),

    %% receive the message with a long expiry interval
    {ok, RPE60s, <<>>} = packetv5:receive_frame(SubSocket1),
    #mqtt5_publish{topic = [<<"message">>, <<"expiry">>, <<"60s">>],
                   qos = 1,
                   properties = #{p_message_expiry_interval := Remaining}} = RPE60s,
    true = Remaining < 60,

    %% The 1s message shouldn't arrive, but let's just block a bit to
    %% make sure.
    {error, timeout} = gen_tcp:recv(SubSocket1, 0, 500),

    %% check that a message was really expired:
    1 = vmq_metrics:counter_val(queue_message_expired),

    ok = gen_tcp:close(SubSocket1),

    disable_on_publish(),
    disable_on_subscribe().

publish_c2b_topic_alias(_Config) ->
    enable_on_publish(),
    enable_on_subscribe(),
    {ok, _} = vmq_server_cmd:set_config(topic_alias_max, 3),

    Topic = "alias/topic",

    %% setup the subscriber
    SubConnect = packetv5:gen_connect("publish-c2b-topic-alias-subscriber", [{keepalive, 60}]),
    SubConnack = packetv5:gen_connack(0, ?M5_CONNACK_ACCEPT, #{p_topic_alias_max => 3}),
    {ok, SubSocket} = packetv5:do_client_connect(SubConnect, SubConnack, []),
    Subscribe = packetv5:gen_subscribe(77, [packetv5:gen_subtopic(Topic,0)], #{}),
    ok = gen_tcp:send(SubSocket, Subscribe),
    SubAck = packetv5:gen_suback(77, [0], #{}),
    ok = packetv5:expect_frame(SubSocket, SubAck),
    
    PubConnect = packetv5:gen_connect("publish-c2b-topic-alias", [{keepalive, 60}]),
    PubConnack = packetv5:gen_connack(0, ?M5_CONNACK_ACCEPT, #{p_topic_alias_max => 3}),
    {ok, PubSocket} = packetv5:do_client_connect(PubConnect, PubConnack, []),

    TA1 = #{p_topic_alias => 1},
    TASetup = packetv5:gen_publish(Topic, 0, <<"publish alias setup">>,
                                   [{properties, TA1}]),
    TAPub = packetv5:gen_publish(<<>>, 0, <<"publish alias pub">>,
                                 [{properties, TA1}]),
    
    ok = gen_tcp:send(PubSocket, TASetup),
    ok = gen_tcp:send(PubSocket, TAPub),

    ExpectSetupPub = packetv5:gen_publish(Topic, 0, <<"publish alias setup">>, []),
    ExpectTAPub = packetv5:gen_publish(Topic, 0, <<"publish alias pub">>, []),
    
    ok = packetv5:expect_frame(SubSocket, ExpectSetupPub),
    ok = packetv5:expect_frame(SubSocket, ExpectTAPub),

    ok = gen_tcp:close(PubSocket),
    ok = gen_tcp:close(PubSocket),

    disable_on_publish(),
    disable_on_subscribe(),
    ok.

forward_properties(_Config) ->

    enable_on_publish(),
    enable_on_subscribe(),

    Topic = "property/passthrough/test",

    %% setup subscriber
    SubConnect = packetv5:gen_connect("property-passthrough-sub-test", [{keepalive, 60}]),
    SubConnack = packetv5:gen_connack(0, ?M5_CONNACK_ACCEPT, #{}),
    {ok, SubSocket} = packetv5:do_client_connect(SubConnect, SubConnack, []),
    Subscribe = packetv5:gen_subscribe(77, [packetv5:gen_subtopic(Topic,0)], #{}),
    ok = gen_tcp:send(SubSocket, Subscribe),
    SubAck = packetv5:gen_suback(77, [0], #{}),
    ok = packetv5:expect_frame(SubSocket, SubAck),

    %% setup publisher
    Properties =
        [
         %% A Server MUST send the Payload Format Indicator unaltered to
         %% all subscribers receiving the Application Message
         %% [MQTT-3.3.2-4]
         #{p_payload_format_indicator => utf8},
         #{p_payload_format_indicator => unspecified},

         %% The Server MUST send the Response Topic unaltered to all
         %% subscribers receiving the Application Message
         %% [MQTT-3.3.2-15].
         #{p_response_topic => <<"response topic">>},

         %% The Server MUST send the Correlation Data unaltered to all
         %% subscribers receiving the Application Message
         %% [MQTT-3.3.2-16].
         #{p_correlation_data => <<"correlation data">>},

         %% The Server MUST send all User Properties unaltered in a
         %% PUBLISH packet when forwarding the Application Message to
         %% a Client [MQTT-3.3.2-17]. The Server MUST maintain the
         %% order of User Properties when forwarding the Application
         %% Message [MQTT-3.3.2-18].
         #{p_user_property => [{<<"k1">>, <<"v1">>},
                               {<<"k2">>, <<"v2">>},
                               {<<"k3">>, <<"v3">>},
                               {<<"k2">>, <<"v4">>}]},

         %% A Server MUST send the Content Type unaltered to all
         %% subscribers receiving the Application Message
         %% [MQTT-3.3.2-20].
         #{p_content_type => <<"content type">>}],

    PubConnect = packetv5:gen_connect("property-passthrough-pub-test", [{keepalive, 60}]),
    PubConnack = packetv5:gen_connack(0, ?M5_CONNACK_ACCEPT, #{}),
    {ok, PubSocket} = packetv5:do_client_connect(PubConnect, PubConnack, []),

    lists:foreach(
      fun(Property) ->
              ct:pal("Testing property: ~p", [Property]),
              Pub = packetv5:gen_publish(Topic, 0, <<"message">>,
                                         [{properties, Property}]),
              ok = gen_tcp:send(PubSocket, Pub),
              ok = packetv5:expect_frame(SubSocket, Pub)
      end, Properties),

    disable_on_publish(),
    disable_on_subscribe().

%% publish_c2b_invalid_topic_alias(Config) ->
%%     vmq_server_cmd:set_config(topic_alias_max, 10),
%%     %% The Client MUST NOT send a Topic Alias in a PUBLISH packet to
%%     %% the Server greater than this value [MQTT-3.2.2-17].

%%     vmq_server_cmd:set_config(topic_alias_max, 0),
%%     %% Topic Alias Maximum is absent or 0, the Client MUST NOT send
%%     %% any Topic Aliases on to the Server [MQTT-3.2.2-18].
%%     ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
hook_auth_on_subscribe(_, _, _) -> ok.
hook_auth_on_publish(_, _, _, _, _, _) -> ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Helper
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
enable_on_subscribe() ->
    ok = vmq_plugin_mgr:enable_module_plugin(
      auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3).
enable_on_publish() ->
    ok = vmq_plugin_mgr:enable_module_plugin(
      auth_on_publish, ?MODULE, hook_auth_on_publish, 6).
disable_on_subscribe() ->
    ok = vmq_plugin_mgr:disable_module_plugin(
      auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3).
disable_on_publish() ->
    ok = vmq_plugin_mgr:disable_module_plugin(
      auth_on_publish, ?MODULE, hook_auth_on_publish, 6).

helper_pub_qos1(ClientId, Mid, Publish, Config) ->
    Connect = mqtt5_v4compat:gen_connect(ClientId, [{keepalive, 60}], Config),
    Connack = mqtt5_v4compat:gen_connack(success, Config),
    Puback = mqtt5_v4compat:gen_puback(Mid, Config),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Config),
    ok = gen_tcp:send(Socket, Publish),
    ok = mqtt5_v4compat:expect_packet(Socket, "puback", Puback, Config),
    gen_tcp:close(Socket).

helper_pub_qos2(ClientId, Mid, Publish, Config) ->
    Connect = mqtt5_v4compat:gen_connect(ClientId, [{keepalive, 60}], Config),
    Connack = mqtt5_v4compat:gen_connack(success, Config),
    Pubrec = mqtt5_v4compat:gen_pubrec(Mid, Config),
    Pubrel = mqtt5_v4compat:gen_pubrel(Mid, Config),
    Pubcomp = mqtt5_v4compat:gen_pubcomp(Mid, Config),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Config),
    ok = gen_tcp:send(Socket, Publish),
    ok = mqtt5_v4compat:expect_packet(Socket, "pubrec", Pubrec, Config),
    ok = gen_tcp:send(Socket, Pubrel),
    ok = mqtt5_v4compat:expect_packet(Socket, "pubcomp", Pubcomp, Config),
    gen_tcp:close(Socket).

helper_pattern_matching(PubTopic, Config) ->
    Connect = mqtt5_v4compat:gen_connect("test-helper", [{keepalive, 60}], Config),
    Connack = mqtt5_v4compat:gen_connack(success, Config),
    Publish = mqtt5_v4compat:gen_publish(PubTopic, 0, <<"message">>, [{retain, true}], Config),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Config),
    ok = gen_tcp:send(Socket, Publish),
    ok = gen_tcp:send(Socket, mqtt5_v4compat:gen_disconnect(Config)),
    gen_tcp:close(Socket).
