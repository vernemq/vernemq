-module(vmq_publish_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("../src/vmq_metrics.hrl").

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

init_per_group(_Group, _Config) ->
    ok.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    vmq_test_utils:seed_rand(Config),
    vmq_server_cmd:set_config(allow_anonymous, true),
    vmq_server_cmd:set_config(retry_interval, 2),
    vmq_server_cmd:set_config(max_client_id_size, 25),
    Config.

end_per_testcase(_, Config) ->
    Config.

all() ->
    [
     {group, mqtt}
    ].

groups() ->
    Tests =
        [publish_qos1_test,
         publish_qos2_test,
         publish_b2c_disconnect_qos1_test,
         publish_b2c_disconnect_qos2_test,
         publish_b2c_timeout_qos1_test,
         publish_b2c_timeout_qos2_test,
         publish_c2b_disconnect_qos2_test,
         publish_c2b_timeout_qos2_test,
         pattern_matching_test,
         not_allowed_publish_close_qos0_mqtt_3_1,
         not_allowed_publish_close_qos1_mqtt_3_1,
         not_allowed_publish_close_qos2_mqtt_3_1,
         not_allowed_publish_close_qos0_mqtt_3_1_1,
         not_allowed_publish_close_qos1_mqtt_3_1_1,
         not_allowed_publish_close_qos2_mqtt_3_1_1,
         message_size_exceeded_close,
         shared_subscription_offline,
         shared_subscription_online_first
        ],
    [
     {mqtt, [shuffle,sequence], Tests}
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
publish_qos1_test(_) ->
    Connect = packet:gen_connect("pub-qos1-test", [{keepalive, 60}]),
    Connack = packet:gen_connack(0),
    Publish = packet:gen_publish("pub/qos1/test", 1, <<"message">>,
                                 [{mid, 19}]),
    Puback = packet:gen_puback(19),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    enable_on_publish(),
    ok = gen_tcp:send(Socket, Publish),
    ok = packet:expect_packet(Socket, "puback", Puback),
    disable_on_publish(),
    ok = expect_alive(Socket),
    ok = gen_tcp:close(Socket).

publish_qos2_test(_) ->
    Connect = packet:gen_connect("pub-qos2-test", [{keepalive, 60}]),
    Connack = packet:gen_connack(0),
    Publish = packet:gen_publish("pub/qos2/test", 2, <<"message">>,
                                 [{mid, 312}]),
    Pubrec = packet:gen_pubrec(312),
    Pubrel = packet:gen_pubrel(312),
    Pubcomp = packet:gen_pubcomp(312),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    enable_on_publish(),
    ok = gen_tcp:send(Socket, Publish),
    ok = packet:expect_packet(Socket, "pubrec", Pubrec),
    ok = gen_tcp:send(Socket, Pubrel),
    ok = packet:expect_packet(Socket, "pubcomp", Pubcomp),
    disable_on_publish(),
    ok = expect_alive(Socket),
    ok = gen_tcp:close(Socket).

publish_b2c_disconnect_qos1_test(_) ->
    Connect = packet:gen_connect("pub-qos1-disco-test",
                                 [{keepalive, 60}, {clean_session, false}]),
    Connack1 = packet:gen_connack(0),
    Connack2 = packet:gen_connack(true, 0),
    Subscribe = packet:gen_subscribe(3265, "qos1/disconnect/test", 1),
    Suback = packet:gen_suback(3265, 1),
    Publish = packet:gen_publish("qos1/disconnect/test", 1,
                                 <<"disconnect-message">>, [{mid, 1}]),
    PublishDup = packet:gen_publish("qos1/disconnect/test", 1,
                                    <<"disconnect-message">>,
                                    [{mid, 1}, {dup, true}]),
    Puback = packet:gen_puback(1),
    Publish2 = packet:gen_publish("qos1/outgoing", 1,
                                  <<"outgoing-message">>, [{mid, 3266}]),
    Puback2 = packet:gen_puback(3266),

    enable_on_publish(),
    enable_on_subscribe(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack1, []),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    helper_pub_qos1("test-helper", 1, Publish),
    %% should have now received a publish command
    ok = packet:expect_packet(Socket, "publish", Publish),
    %% expect packet, but we don't ack
    %% send our outgoing message, when we disconnect the broker
    %% should get rid of it and ssume we're going to retry
    ok = gen_tcp:send(Socket, Publish2),
    ok = packet:expect_packet(Socket, "puback", Puback2),
    ok = expect_alive(Socket),
    ok = gen_tcp:close(Socket),
    %% TODO: assert Publish2 deleted on broker
    %% reconnect
    {ok, Socket1} = packet:do_client_connect(Connect, Connack2, []),
    ok = packet:expect_packet(Socket1, "dup publish", PublishDup),
    ok = gen_tcp:send(Socket1, Puback),
    disable_on_publish(),
    disable_on_subscribe(),
    ok = expect_alive(Socket1),
    ok = gen_tcp:close(Socket1).

publish_b2c_disconnect_qos2_test(_) ->
    Connect = packet:gen_connect("pub-b2c-qos2-disco-test",
                                 [{keepalive, 60}, {clean_session, false}]),
    Connack1 = packet:gen_connack(0),
    Connack2 = packet:gen_connack(true, 0),
    Subscribe = packet:gen_subscribe(3265, "qos2/disconnect/test", 2),
    Suback = packet:gen_suback(3265, 2),
    Publish = packet:gen_publish("qos2/disconnect/test", 2,
                                 <<"disconnect-message">>, [{mid, 1}]),
    PublishDup = packet:gen_publish("qos2/disconnect/test", 2,
                                    <<"disconnect-message">>,
                                    [{mid, 1}, {dup, true}]),
    Pubrel = packet:gen_pubrel(1),
    Pubrec = packet:gen_pubrec(1),
    Pubcomp = packet:gen_pubcomp(1),
    Publish2 = packet:gen_publish("qos1/outgoing", 1,
                                  <<"outgoing-message">>, [{mid, 3266}]),
    enable_on_publish(),
    enable_on_subscribe(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack1, []),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    helper_pub_qos2("test-helper", 1, Publish),
    %% should have now received a publish command
    ok = packet:expect_packet(Socket, "publish", Publish),
    %% expect packet, but we don't ack
    %% send our outgoing message, when we disconnect the broker
    %% should get rid of it and ssume we're going to retry
    ok = gen_tcp:send(Socket, Publish2),
    ok = gen_tcp:close(Socket),
    %% TODO: assert Publish2 deleted on broker
    %% reconnect
    {ok, Socket1} = packet:do_client_connect(Connect, Connack2, []),
    ok = packet:expect_packet(Socket1, "dup publish", PublishDup),
    ok = gen_tcp:send(Socket1, Pubrec),
    ok = packet:expect_packet(Socket1, "pubrel", Pubrel),
    ok = gen_tcp:close(Socket1),
    %% Expect Pubrel
    {ok, Socket2} = packet:do_client_connect(Connect, Connack2, []),
    ok = packet:expect_packet(Socket2, "pubrel", Pubrel),
    ok = gen_tcp:send(Socket2, Pubcomp),
    disable_on_publish(),
    disable_on_subscribe(),
    ok = expect_alive(Socket2),
    ok = gen_tcp:close(Socket2).

publish_b2c_timeout_qos1_test(_) ->
    Connect = packet:gen_connect("pub-qos1-timeout-test", [{keepalive, 60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(3265, "qos1/timeout/test", 1),
    Suback = packet:gen_suback(3265, 1),
    Publish = packet:gen_publish("qos1/timeout/test", 1,
                                 <<"timeout-message">>, [{mid, 1}]),
    PublishDup = packet:gen_publish("qos1/timeout/test", 1,
                                    <<"timeout-message">>,
                                    [{mid, 1}, {dup, true}]),
    Puback = packet:gen_puback(1),
    enable_on_publish(),
    enable_on_subscribe(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    helper_pub_qos1("test-helper", 1, Publish),
    %% should have now received a publish command
    ok = packet:expect_packet(Socket, "publish", Publish),
    %% expect packet, but we don't ack
    %% The broker should repeat the PUBLISH with dup set
    ok = packet:expect_packet(Socket, "dup publish", PublishDup),
    ok = gen_tcp:send(Socket, Puback),
    disable_on_publish(),
    disable_on_subscribe(),
    ok = expect_alive(Socket),
    ok = gen_tcp:close(Socket).

publish_b2c_timeout_qos2_test(_) ->
    Connect = packet:gen_connect("pub-b2c-qos2-timeout-test", [{keepalive, 60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(3265, "qos2/timeout/test", 2),
    Suback = packet:gen_suback(3265, 2),
    Publish = packet:gen_publish("qos2/timeout/test", 2,
                                 <<"timeout-message">>, [{mid, 1}]),
    PublishDup = packet:gen_publish("qos2/timeout/test", 2,
                                    <<"timeout-message">>,
                                    [{mid, 1}, {dup, true}]),
    Pubrec = packet:gen_pubrec(1),
    Pubrel = packet:gen_pubrel(1),
    Pubcomp = packet:gen_pubcomp(1),
    enable_on_publish(),
    enable_on_subscribe(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    helper_pub_qos2("test-helper", 1, Publish),
    %% should have now received a publish command
    ok = packet:expect_packet(Socket, "publish", Publish),
    %% expect packet, but we don't ack
    %% The broker should repeat the PUBLISH with dup set
    ok = packet:expect_packet(Socket, "dup publish", PublishDup),
    ok = gen_tcp:send(Socket, Pubrec),
    ok = packet:expect_packet(Socket, "pubrel", Pubrel),
    %% The broker should repeat the PUBREL NO dup set according to MQTT-3.6.1-1
    ok = packet:expect_packet(Socket, "pubrel", Pubrel),
    ok = gen_tcp:send(Socket, Pubcomp),
    disable_on_publish(),
    disable_on_subscribe(),
    ok = expect_alive(Socket),
    ok = gen_tcp:close(Socket).

publish_c2b_disconnect_qos2_test(_) ->
    Connect = packet:gen_connect("pub-c2b-qos2-disco-test",
                                 [{keepalive, 60}, {clean_session, false}]),
    Connack1 = packet:gen_connack(0),
    Connack2 = packet:gen_connack(true, 0),
    Publish = packet:gen_publish("qos2/disconnect/test", 2,
                                 <<"disconnect-message">>, [{mid, 1}]),
    PublishDup = packet:gen_publish("qos2/disconnect/test", 2,
                                    <<"disconnect-message">>,
                                    [{mid, 1}, {dup, true}]),
    Pubrec = packet:gen_pubrec(1),
    Pubrel = packet:gen_pubrel(1),
    Pubcomp = packet:gen_pubcomp(1),
    enable_on_publish(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack1, []),
    ok = gen_tcp:send(Socket, Publish),
    ok = packet:expect_packet(Socket, "pubrec", Pubrec),
    %% We're now going to disconnect and pretend we didn't receive the pubrec
    ok = expect_alive(Socket),
    ok = gen_tcp:close(Socket),
    {ok, Socket1} = packet:do_client_connect(Connect, Connack2, []),
    ok = gen_tcp:send(Socket1, PublishDup),
    ok = packet:expect_packet(Socket1, "pubrec", Pubrec),
    ok = gen_tcp:send(Socket1, Pubrel),
    ok = packet:expect_packet(Socket1, "pubcomp", Pubcomp),
    %% Again, pretend we didn't receive this pubcomp
    ok = expect_alive(Socket1),
    ok = gen_tcp:close(Socket1),
    {ok, Socket2} = packet:do_client_connect(Connect, Connack2, []),
    ok = gen_tcp:send(Socket2, Pubrel),
    ok = packet:expect_packet(Socket2, "pubcomp", Pubcomp),
    disable_on_publish(),
    ok = expect_alive(Socket2),
    ok = gen_tcp:close(Socket2).

publish_c2b_timeout_qos2_test(_) ->
    Connect = packet:gen_connect("pub-c2b-qos2-timeout-test", [{keepalive, 60}]),
    Connack = packet:gen_connack(0),
    Publish = packet:gen_publish("pub/qos2/test", 2,
                                 <<"timeout-message">>, [{mid, 1926}]),
    Pubrec = packet:gen_pubrec(1926),
    Pubrel = packet:gen_pubrel(1926),
    Pubcomp = packet:gen_pubcomp(1926),
    enable_on_publish(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Publish),
    ok = packet:expect_packet(Socket, "pubrec", Pubrec),
    ok = gen_tcp:send(Socket, Pubrel),
    ok = packet:expect_packet(Socket, "pubcomp", Pubcomp),
    disable_on_publish(),
    ok = expect_alive(Socket),
    ok = gen_tcp:close(Socket).

pattern_matching_test(_) ->
    ok = pattern_test("#", "test/topic"),
    ok = pattern_test("#", "/test/topic"),
    ok = pattern_test("foo/#", "foo/bar/baz"),
    ok = pattern_test("foo/+/baz", "foo/bar/baz"),
    ok = pattern_test("foo/+/baz/#", "foo/bar/baz"),
    ok = pattern_test("foo/+/baz/#", "foo/bar/baz/bar"),
    ok = pattern_test("foo/foo/baz/#", "foo/foo/baz/bar"),
    ok = pattern_test("foo/#", "foo"),
    ok = pattern_test("/#", "/foo"),
    ok = pattern_test("test/topic/", "test/topic/"),
    ok = pattern_test("test/topic/+", "test/topic/"),
    ok = pattern_test("+/+/+/+/+/+/+/+/+/+/test",
                      "one/two/three/four/five/six/seven/eight/nine/ten/test"),
    ok = pattern_test("#", "test////a//topic"),
    ok = pattern_test("#", "/test////a//topic"),
    ok = pattern_test("foo/#", "foo//bar///baz"),
    ok = pattern_test("foo/+/baz", "foo//baz"),
    ok = pattern_test("foo/+/baz//", "foo//baz//"),
    ok = pattern_test("foo/+/baz/#", "foo//baz"),
    ok = pattern_test("foo/+/baz/#", "foo//baz/bar"),
    ok = pattern_test("foo//baz/#", "foo//baz/bar"),
    ok = pattern_test("foo/foo/baz/#", "foo/foo/baz/bar"),
    ok = pattern_test("/#", "////foo///bar").

pattern_test(SubTopic, PubTopic) ->
    Connect = packet:gen_connect("pattern-sub-test", [{keepalive, 60}]),
    Connack = packet:gen_connack(0),
    Publish = packet:gen_publish(PubTopic, 0, <<"message">>, []),
    PublishRetained = packet:gen_publish(PubTopic, 0, <<"message">>,
                                         [{retain, true}]),
    Subscribe = packet:gen_subscribe(312, SubTopic, 0),
    Suback = packet:gen_suback(312, 0),
    Unsubscribe = packet:gen_unsubscribe(234, SubTopic),
    Unsuback = packet:gen_unsuback(234),
    enable_on_publish(),
    enable_on_subscribe(),
    vmq_test_utils:reset_tables(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    helper_pattern_matching(PubTopic),
    ok = packet:expect_packet(Socket, "publish", Publish),
    ok = gen_tcp:send(Socket, Unsubscribe),
    ok = packet:expect_packet(Socket, "unsuback", Unsuback),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = packet:expect_packet(Socket, "publish retained", PublishRetained),
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
    Connect = packet:gen_connect("pub-excessive-test", [{keepalive, 60}]),
    Connack = packet:gen_connack(0),
    Publish = packet:gen_publish("pub/excessive/test", 0, vmq_test_utils:rand_bytes(1024),
                                 [{mid, 19}]),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    enable_on_publish(),
    ok = gen_tcp:send(Socket, Publish),
    {error, closed} = gen_tcp:recv(Socket, 0, 1000),
    true = lists:any(
             fun({#metric_def{
                     type = counter,
                     id = mqtt_invalid_msg_size_error}, 1}) -> true;
                (_) -> false
             end,vmq_metrics:metrics()),
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

helper_pub_qos1(ClientId, Mid, Publish) ->
    Connect = packet:gen_connect(ClientId, [{keepalive, 60}]),
    Connack = packet:gen_connack(0),
    Puback = packet:gen_puback(Mid),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Publish),
    ok = packet:expect_packet(Socket, "puback", Puback),
    gen_tcp:close(Socket).

helper_pub_qos2(ClientId, Mid, Publish) ->
    Connect = packet:gen_connect(ClientId, [{keepalive, 60}]),
    Connack = packet:gen_connack(0),
    Pubrec = packet:gen_pubrec(Mid),
    Pubrel = packet:gen_pubrel(Mid),
    Pubcomp = packet:gen_pubcomp(Mid),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Publish),
    ok = packet:expect_packet(Socket, "pubrec", Pubrec),
    ok = gen_tcp:send(Socket, Pubrel),
    ok = packet:expect_packet(Socket, "pubcomp", Pubcomp),
    gen_tcp:close(Socket).

helper_pattern_matching(PubTopic) ->
    Connect = packet:gen_connect("test-helper", [{keepalive, 60}]),
    Connack = packet:gen_connack(0),
    Publish = packet:gen_publish(PubTopic, 0, <<"message">>, [{retain, true}]),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Publish),
    ok = gen_tcp:send(Socket, packet:gen_disconnect()),
    gen_tcp:close(Socket).

expect_alive(Socket) ->
    Pingreq = packet:gen_pingreq(),
    Pingresp = packet:gen_pingresp(),
    ok = gen_tcp:send(Socket, Pingreq),
    ok = packet:expect_packet(Socket, "pingresp", Pingresp).
