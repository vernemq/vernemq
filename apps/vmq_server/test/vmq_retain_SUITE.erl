-module(vmq_retain_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(_Config) ->
    vmq_test_utils:setup(),
    vmq_server_cmd:set_config(allow_anonymous, true),
    vmq_server_cmd:set_config(retry_interval, 10),
    vmq_server_cmd:listener_start(1888, []),
    cover:start(),
    enable_on_publish(),
    enable_on_subscribe(),
    _Config.

end_per_suite(_Config) ->
    disable_on_publish(),
    disable_on_subscribe(),
    vmq_test_utils:teardown(),
    _Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_, Config) ->
    Config.

all() ->
    [
     {group, mqtt}
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
     {mqtt, [shuffle, parallel], Tests}
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

retain_qos0_test(_) ->
    Connect = packet:gen_connect("retain-qos0-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Publish = packet:gen_publish("retain/qos0/test", 0, <<"retained message">>, [{retain, true}]),
    Subscribe = packet:gen_subscribe(16, "retain/qos0/test", 0),
    Suback = packet:gen_suback(16, 0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    %% Send retained message
    ok = gen_tcp:send(Socket, Publish),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = packet:expect_packet(Socket, "publish", Publish),
    ok = gen_tcp:close(Socket).

retain_qos0_repeated_test(_) ->
    Connect = packet:gen_connect("retain-qos0-rep-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Publish = packet:gen_publish("retain/qos0/reptest", 0, <<"retained message">>, [{retain, true}]),
    Subscribe = packet:gen_subscribe(16, "retain/qos0/reptest", 0),
    Suback = packet:gen_suback(16, 0),
    Unsubscribe = packet:gen_unsubscribe(13, "retain/qos0/reptest"),
    Unsuback = packet:gen_unsuback(13),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    %% Send retained message
    ok = gen_tcp:send(Socket, Publish),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = packet:expect_packet(Socket, "publish", Publish),
    ok = gen_tcp:send(Socket, Unsubscribe),
    ok = packet:expect_packet(Socket, "unsuback", Unsuback),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = packet:expect_packet(Socket, "publish", Publish),
    ok = gen_tcp:close(Socket).

retain_qos0_fresh_test(_) ->
    Connect = packet:gen_connect("retain-qos0-fresh-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Publish = packet:gen_publish("retain/qos0/freshtest", 0, <<"retained message">>, [{retain, true}]),
    PublishFresh = packet:gen_publish("retain/qos0/freshtest", 0, <<"retained message">>, []),
    Subscribe = packet:gen_subscribe(16, "retain/qos0/freshtest", 0),
    Suback = packet:gen_suback(16, 0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    %% Send retained message
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = gen_tcp:send(Socket, Publish),
    ok = packet:expect_packet(Socket, "publish", PublishFresh),
    ok = gen_tcp:close(Socket).

retain_qos0_clear_test(_) ->
    Connect = packet:gen_connect("retain-clear-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Publish = packet:gen_publish("retain/clear/test", 0, <<"retained message">>, [{retain, true}]),
    RetainClear = packet:gen_publish("retain/clear/test", 0, <<>>, [{retain, true}]),
    Subscribe = packet:gen_subscribe(592, "retain/clear/test", 0),
    Suback = packet:gen_suback(592, 0),
    Unsubscribe = packet:gen_unsubscribe(593, "retain/clear/test"),
    Unsuback = packet:gen_unsuback(593),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    %% Send retained message
    ok = gen_tcp:send(Socket, Publish),
    %% Subscribe to topic, we should get the retained message back.
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = packet:expect_packet(Socket, "publish", Publish),
    %% Now unsubscribe from the topic before we clear the retained mesage
    ok = gen_tcp:send(Socket, Unsubscribe),
    ok = packet:expect_packet(Socket, "unsuback", Unsuback),
    %% Now clear the retained message
    ok = gen_tcp:send(Socket, RetainClear),
    %% Subscribe to topic, we shouldn't get anything back apart from the SUBACK
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    {error, timeout} = gen_tcp:recv(Socket, 256, 1000),
    ok = gen_tcp:close(Socket).

retain_qos1_qos0_test(_) ->
    Connect = packet:gen_connect("retain-qos1-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Publish = packet:gen_publish("retain/qos1/test", 1, <<"retained message">>, [{mid, 6}, {retain, true}]),
    Puback = packet:gen_puback(6),
    Subscribe = packet:gen_subscribe(18, "retain/qos1/test", 0),
    Suback = packet:gen_suback(18, 0),
    Publish0 = packet:gen_publish("retain/qos1/test", 0, <<"retained message">>, [{retain, true}]),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    %% Send retained message
    ok = gen_tcp:send(Socket, Publish),
    ok = packet:expect_packet(Socket, "puback", Puback),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = packet:expect_packet(Socket, "publish0", Publish0),
    ok = gen_tcp:close(Socket).

publish_empty_retained_msg_test(_) ->
    Connect = packet:gen_connect("retain-clear-empty-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Publish = packet:gen_publish("retain/clear/emptytest", 0, <<"retained message">>, [{retain, true}]),
    RetainClearPub = packet:gen_publish("retain/clear/emptytest", 0, <<>>, [{retain, true}]),
    RetainClearSub = packet:gen_publish("retain/clear/emptytest", 0, <<>>, [{retain, false}]),
    Subscribe = packet:gen_subscribe(592, "retain/clear/emptytest", 0),
    Suback = packet:gen_suback(592, 0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),

    %% Send retained message
    ok = gen_tcp:send(Socket, Publish),
    %% Subscribe to topic, we should get the retained message back.
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = packet:expect_packet(Socket, "publish", Publish),

    %% Now clear the retained message
    ok = gen_tcp:send(Socket, RetainClearPub),
    %% Receive the empty payload msg as normal publish.
    ok = packet:expect_packet(Socket, "publish", RetainClearSub),
    {error, timeout} = gen_tcp:recv(Socket, 256, 1000),
    ok = gen_tcp:close(Socket).

retain_wildcard_test(_) ->
    Connect = packet:gen_connect("retain-wildcard-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Publish = packet:gen_publish("retainwildcard/wildcard/test", 0, <<"retained message">>, [{retain, true}]),
    Subscribe = packet:gen_subscribe(16, "retainwildcard/+/#", 0),
    Suback = packet:gen_suback(16, 0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    %% Send retained message
    ok = gen_tcp:send(Socket, Publish),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = packet:expect_packet(Socket, "publish", Publish),
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
