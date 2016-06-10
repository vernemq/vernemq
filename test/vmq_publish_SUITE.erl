-module(vmq_publish_SUITE).
-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([publish_qos1_test/1,
         publish_qos2_test/1,
         publish_b2c_disconnect_qos1_test/1,
         publish_b2c_disconnect_qos2_test/1,
         publish_b2c_timeout_qos1_test/1,
         publish_b2c_timeout_qos2_test/1,
         publish_c2b_disconnect_qos2_test/1,
         publish_c2b_timeout_qos2_test/1,
         pattern_matching_test/1,
         not_allowed_publish_close_qos0_mqtt_3_1/1,
         not_allowed_publish_close_qos1_mqtt_3_1/1,
         not_allowed_publish_close_qos2_mqtt_3_1/1,
         not_allowed_publish_close_qos0_mqtt_3_1_1/1,
         not_allowed_publish_close_qos1_mqtt_3_1_1/1,
         not_allowed_publish_close_qos2_mqtt_3_1_1/1
        ]).

-export([hook_auth_on_subscribe/3,
         hook_auth_on_publish/6]).

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(_Config) ->
    cover:start(),
    _Config.

end_per_suite(_Config) ->
    _Config.

init_per_testcase(_Case, Config) ->
    vmq_test_utils:setup(),
    vmq_server_cmd:set_config(allow_anonymous, true),
    vmq_server_cmd:set_config(retry_interval, 10),
    vmq_server_cmd:set_config(max_client_id_size, 25),
    vmq_server_cmd:listener_start(1888, []),
    Config.

end_per_testcase(_, Config) ->
    vmq_test_utils:teardown(),
    Config.

all() ->
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
     not_allowed_publish_close_qos2_mqtt_3_1_1
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
    ok = gen_tcp:close(Socket),
    %% TODO: assert Publish2 deleted on broker
    %% reconnect
    {ok, Socket1} = packet:do_client_connect(Connect, Connack2, []),
    ok = packet:expect_packet(Socket1, "dup publish", PublishDup),
    ok = gen_tcp:send(Socket1, Puback),
    disable_on_publish(),
    disable_on_subscribe(),
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
    ok = gen_tcp:close(Socket),
    {ok, Socket1} = packet:do_client_connect(Connect, Connack2, []),
    ok = gen_tcp:send(Socket1, PublishDup),
    ok = packet:expect_packet(Socket1, "pubrec", Pubrec),
    ok = gen_tcp:send(Socket1, Pubrel),
    ok = packet:expect_packet(Socket1, "pubcomp", Pubcomp),
    %% Again, pretend we didn't receive this pubcomp
    ok = gen_tcp:close(Socket1),
    {ok, Socket2} = packet:do_client_connect(Connect, Connack2, []),
    ok = gen_tcp:send(Socket2, Pubrel),
    ok = packet:expect_packet(Socket2, "pubcomp", Pubcomp),
    disable_on_publish(),
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
    %% The broker should repeat the PUBREC
    ok = packet:expect_packet(Socket, "pubrec", Pubrec),
    ok = gen_tcp:send(Socket, Pubrel),
    ok = packet:expect_packet(Socket, "pubcomp", Pubcomp),
    disable_on_publish(),
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



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
hook_auth_on_subscribe(_, _, _) -> ok.
hook_auth_on_publish(_, _, _, _, _, _) -> ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Helper
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
enable_on_subscribe() ->
    vmq_plugin_mgr:enable_module_plugin(
      auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3).
enable_on_publish() ->
    vmq_plugin_mgr:enable_module_plugin(
      auth_on_publish, ?MODULE, hook_auth_on_publish, 6).
disable_on_subscribe() ->
    vmq_plugin_mgr:disable_module_plugin(
      auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3).
disable_on_publish() ->
    vmq_plugin_mgr:disable_module_plugin(
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
