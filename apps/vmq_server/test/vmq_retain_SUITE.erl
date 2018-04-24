-module(vmq_retain_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(Config) ->
    vmq_test_utils:setup(),
    vmq_server_cmd:set_config(allow_anonymous, true),
    vmq_server_cmd:set_config(max_client_id_size, 1000),
    vmq_server_cmd:set_config(retry_interval, 10),
    vmq_server_cmd:listener_start(1888, [{allowed_protocol_versions, "3,4,5"}]),
    cover:start(),
    enable_on_publish(),
    enable_on_subscribe(),
    [{ct_hooks, vmq_cth} | Config].

end_per_suite(_Config) ->
    disable_on_publish(),
    disable_on_subscribe(),
    vmq_test_utils:teardown(),
    _Config.

init_per_group(mqttv4, Config) ->
    [{protover, 4}|Config];
init_per_group(mqttv5, Config) ->
    [{protover, 5}|Config].

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_Case, Config) ->
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
        [retain_qos0_test,
         retain_qos0_repeated_test,
         retain_qos0_fresh_test,
         retain_qos0_clear_test,
         retain_qos1_qos0_test,
         retain_wildcard_test,
         publish_empty_retained_msg_test],
    [
     {mqttv4, [shuffle, parallel], Tests},
     {mqttv5, [shuffle, parallel], Tests}
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

retain_qos0_test(Cfg) ->
    Topic = vmq_cth:ustr(Cfg) ++ "retain/qos0/test",
    Connect = mqtt5_v4compat:gen_connect(vmq_cth:ustr(Cfg) ++ "retain-qos0-test", [{keepalive,60}], Cfg),
    Connack = mqtt5_v4compat:gen_connack(success, Cfg),
    Publish = mqtt5_v4compat:gen_publish(Topic, 0, <<"retained message">>, [{retain, true}], Cfg),
    Subscribe = mqtt5_v4compat:gen_subscribe(16, Topic, 0, Cfg),
    Suback = mqtt5_v4compat:gen_suback(16, 0, Cfg),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Cfg),
    %% Send retained message
    ok = gen_tcp:send(Socket, Publish),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "suback", Suback, Cfg),
    ok = mqtt5_v4compat:expect_packet(Socket, "publish", Publish, Cfg),
    ok = gen_tcp:close(Socket).

retain_qos0_repeated_test(Cfg) ->
    Topic = vmq_cth:ustr(Cfg) ++ "retain/qos0/reptest",
    Connect = mqtt5_v4compat:gen_connect(vmq_cth:ustr(Cfg)
                                 ++ "retain-qos0-rep-test", [{keepalive,60}], Cfg),
    Connack = mqtt5_v4compat:gen_connack(success, Cfg),
    Publish = mqtt5_v4compat:gen_publish(Topic, 0, <<"retained message">>, [{retain, true}], Cfg),
    Subscribe = mqtt5_v4compat:gen_subscribe(16, Topic, 0, Cfg),
    Suback = mqtt5_v4compat:gen_suback(16, 0, Cfg),
    Unsubscribe = mqtt5_v4compat:gen_unsubscribe(13, Topic, Cfg),
    Unsuback = mqtt5_v4compat:gen_unsuback(13, Cfg),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Cfg),
    %% Send retained message
    ok = gen_tcp:send(Socket, Publish),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "suback", Suback, Cfg),
    ok = mqtt5_v4compat:expect_packet(Socket, "publish", Publish, Cfg),
    ok = gen_tcp:send(Socket, Unsubscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "unsuback", Unsuback, Cfg),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "suback", Suback, Cfg),
    ok = mqtt5_v4compat:expect_packet(Socket, "publish", Publish, Cfg),
    ok = gen_tcp:close(Socket).

retain_qos0_fresh_test(Cfg) ->
    Topic = vmq_cth:ustr(Cfg) ++ "retain/qos0/freshtest",
    Connect = mqtt5_v4compat:gen_connect(vmq_cth:ustr(Cfg)
                                 ++ "retain-qos0-fresh-test", [{keepalive,60}], Cfg),
    Connack = mqtt5_v4compat:gen_connack(success, Cfg),
    Publish = mqtt5_v4compat:gen_publish(Topic, 0, <<"retained message">>, [{retain, true}], Cfg),
    PublishFresh = mqtt5_v4compat:gen_publish(Topic, 0, <<"retained message">>, [], Cfg),
    Subscribe = mqtt5_v4compat:gen_subscribe(16, Topic, 0, Cfg),
    Suback = mqtt5_v4compat:gen_suback(16, 0, Cfg),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Cfg),
    %% Send retained message
    ok = gen_tcp:send(Socket, Subscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "suback", Suback, Cfg),
    ok = gen_tcp:send(Socket, Publish),
    ok = mqtt5_v4compat:expect_packet(Socket, "publish", PublishFresh, Cfg),
    ok = gen_tcp:close(Socket).

retain_qos0_clear_test(Cfg) ->
    Topic = vmq_cth:ustr(Cfg) ++ "retain/clear/test",
    Connect = mqtt5_v4compat:gen_connect(vmq_cth:ustr(Cfg)
                                 ++ "retain-clear-test", [{keepalive,60}], Cfg),
    Connack = mqtt5_v4compat:gen_connack(success, Cfg),
    Publish = mqtt5_v4compat:gen_publish(Topic, 0, <<"retained message">>, [{retain, true}], Cfg),
    RetainClear = mqtt5_v4compat:gen_publish(Topic, 0, <<>>, [{retain, true}], Cfg),
    Subscribe = mqtt5_v4compat:gen_subscribe(592, Topic, 0, Cfg),
    Suback = mqtt5_v4compat:gen_suback(592, 0, Cfg),
    Unsubscribe = mqtt5_v4compat:gen_unsubscribe(593, Topic, Cfg),
    Unsuback = mqtt5_v4compat:gen_unsuback(593, Cfg),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Cfg),
    %% Send retained message
    ok = gen_tcp:send(Socket, Publish),
    %% Subscribe to topic, we should get the retained message back.
    ok = gen_tcp:send(Socket, Subscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "suback", Suback, Cfg),
    ok = mqtt5_v4compat:expect_packet(Socket, "publish", Publish, Cfg),
    %% Now unsubscribe from the topic before we clear the retained mesage
    ok = gen_tcp:send(Socket, Unsubscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "unsuback", Unsuback, Cfg),
    %% Now clear the retained message
    ok = gen_tcp:send(Socket, RetainClear),
    %% Subscribe to topic, we shouldn't get anything back apart from the SUBACK
    ok = gen_tcp:send(Socket, Subscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "suback", Suback, Cfg),
    {error, timeout} = gen_tcp:recv(Socket, 256, 1000),
    ok = gen_tcp:close(Socket).

retain_qos1_qos0_test(Cfg) ->
    Topic = vmq_cth:ustr(Cfg) ++ "retain/qos1/test",
    Connect = mqtt5_v4compat:gen_connect(vmq_cth:ustr(Cfg)
                                 ++ "retain-qos1-test", [{keepalive,60}], Cfg),
    Connack = mqtt5_v4compat:gen_connack(success, Cfg),
    Publish = mqtt5_v4compat:gen_publish(Topic, 1, <<"retained message">>, [{mid, 6}, {retain, true}], Cfg),
    Puback = mqtt5_v4compat:gen_puback(6, Cfg),
    Subscribe = mqtt5_v4compat:gen_subscribe(18, Topic, 0, Cfg),
    Suback = mqtt5_v4compat:gen_suback(18, 0, Cfg),
    Publish0 = mqtt5_v4compat:gen_publish(Topic, 0, <<"retained message">>, [{retain, true}], Cfg),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Cfg),
    %% Send retained message
    ok = gen_tcp:send(Socket, Publish),
    ok = mqtt5_v4compat:expect_packet(Socket, "puback", Puback, Cfg),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "suback", Suback, Cfg),
    ok = mqtt5_v4compat:expect_packet(Socket, "publish0", Publish0, Cfg),
    ok = gen_tcp:close(Socket).

publish_empty_retained_msg_test(Cfg) ->
    Topic = vmq_cth:ustr(Cfg) ++ "retain/clear/emptytest",
    Connect = mqtt5_v4compat:gen_connect(vmq_cth:ustr(Cfg)
                                 ++ "retain-clear-empty-test", [{keepalive,60}], Cfg),
    Connack = mqtt5_v4compat:gen_connack(success, Cfg),
    Publish = mqtt5_v4compat:gen_publish(Topic, 0, <<"retained message">>, [{retain, true}], Cfg),
    RetainClearPub = mqtt5_v4compat:gen_publish(Topic, 0, <<>>, [{retain, true}], Cfg),
    RetainClearSub = mqtt5_v4compat:gen_publish(Topic, 0, <<>>, [{retain, false}], Cfg),
    Subscribe = mqtt5_v4compat:gen_subscribe(592, Topic, 0, Cfg),
    Suback = mqtt5_v4compat:gen_suback(592, 0, Cfg),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Cfg),

    %% Send retained message
    ok = gen_tcp:send(Socket, Publish),
    %% Subscribe to topic, we should get the retained message back.
    ok = gen_tcp:send(Socket, Subscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "suback", Suback, Cfg),
    ok = mqtt5_v4compat:expect_packet(Socket, "publish", Publish, Cfg),

    %% Now clear the retained message
    ok = gen_tcp:send(Socket, RetainClearPub),
    %% Receive the empty payload msg as normal publish.
    ok = mqtt5_v4compat:expect_packet(Socket, "publish", RetainClearSub, Cfg),
    {error, timeout} = gen_tcp:recv(Socket, 256, 1000),
    ok = gen_tcp:close(Socket).

retain_wildcard_test(Cfg) ->
    Prefix = vmq_cth:ustr(Cfg),
    Topic = Prefix ++ "retainwildcard/wildcard/test",
    Connect = mqtt5_v4compat:gen_connect(vmq_cth:ustr(Cfg)
                                 ++ "retain-wildcard-test", [{keepalive,60}], Cfg),
    Connack = mqtt5_v4compat:gen_connack(success, Cfg),
    Publish = mqtt5_v4compat:gen_publish(Topic, 0, <<"retained message">>, [{retain, true}], Cfg),
    Subscribe = mqtt5_v4compat:gen_subscribe(16, Prefix ++ "retainwildcard/+/#", 0, Cfg),
    Suback = mqtt5_v4compat:gen_suback(16, 0, Cfg),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Cfg),
    %% Send retained message
    ok = gen_tcp:send(Socket, Publish),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "suback", Suback, Cfg),
    ok = mqtt5_v4compat:expect_packet(Socket, "publish", Publish, Cfg),
    ok = gen_tcp:close(Socket).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
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
           [{compat, {auth_on_subscribe_v1, vmq_plugin_compat_v1_v0,
                     convert, 3}}]).
enable_on_publish() ->
    ok = vmq_plugin_mgr:enable_module_plugin(
           auth_on_publish, ?MODULE, hook_auth_on_publish, 6),
    ok = vmq_plugin_mgr:enable_module_plugin(
           auth_on_publish, ?MODULE, hook_auth_on_publish, 6,
           [{compat, {auth_on_publish_v1, vmq_plugin_compat_v1_v0,
                    convert, 6}}]).
disable_on_subscribe() ->
    ok = vmq_plugin_mgr:disable_module_plugin(
           auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3),
    ok = vmq_plugin_mgr:disable_module_plugin(
           auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3,
           [{compat, {auth_on_subscribe_v1, vmq_plugin_compat_v1_v0,
                      convert, 3}}]).
disable_on_publish() ->
    ok = vmq_plugin_mgr:disable_module_plugin(
           auth_on_publish, ?MODULE, hook_auth_on_publish, 6),
    ok = vmq_plugin_mgr:disable_module_plugin(
           auth_on_publish, ?MODULE, hook_auth_on_publish, 6,
           [{compat, {auth_on_publish_v1, vmq_plugin_compat_v1_v0,
                      convert, 6}}]).
