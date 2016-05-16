-module(vmq_last_will_SUITE).
-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([will_denied_test/1,
         will_ignored_for_normal_disconnect_test/1,
         will_null_test/1,
         will_null_topic_test/1,
         will_qos0_test/1]).

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
    vmq_server_cmd:listener_start(1888, []),
    Config.

end_per_testcase(_, Config) ->
    vmq_test_utils:teardown(),
    Config.

all() ->
    [will_denied_test,
     will_null_test,
     will_null_topic_test,
     will_qos0_test,
     will_ignored_for_normal_disconnect_test].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
will_denied_test(_) ->
    ConnectOK = packet:gen_connect("will-acl-test", [{keepalive,60}, {will_topic, "ok"}, {will_msg, <<"should be ok">>}]),
    ConnackOK = packet:gen_connack(0),
    Connect = packet:gen_connect("will-acl-test", [{keepalive,60}, {will_topic, "will/acl/test"}, {will_msg, <<"should be denied">>}]),
    Connack = packet:gen_connack(5),
    enable_on_publish(),
    {ok, SocketOK} = packet:do_client_connect(ConnectOK, ConnackOK, []),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    disable_on_publish(),
    ok = gen_tcp:close(Socket),
    ok = gen_tcp:close(SocketOK).

will_ignored_for_normal_disconnect_test(_) ->
    ConnectPub = packet:gen_connect("will-ign-pub-test", [{keepalive,60}, {will_topic, "will/ignorenormal/test"}, {will_msg, <<"should be ignored">>}]),
    ConnackPub = packet:gen_connack(0),
    ConnectSub = packet:gen_connect("will-ign-sub-test", [{keepalive,60}]),
    ConnackSub = packet:gen_connack(0),
    enable_on_subscribe(),
    enable_on_publish(),
    {ok, SocketPub} = packet:do_client_connect(ConnectPub, ConnackPub, []),
    {ok, SocketSub} = packet:do_client_connect(ConnectSub, ConnackSub, []),
    Subscribe = packet:gen_subscribe(53, "will/ignorenormal/test", 0),
    Suback = packet:gen_suback(53, 0),
    ok = gen_tcp:send(SocketSub, Subscribe),
    ok = packet:expect_packet(SocketSub, "suback", Suback),
    Disconnect = packet:gen_disconnect(),
    ok = gen_tcp:send(SocketPub, Disconnect),
    case gen_tcp:recv(SocketSub, 0, 200) of
        % we don't match on any packet, so that we crash
        {error, timeout} -> ok
    end,
    disable_on_subscribe(),
    disable_on_publish(),
    ok = gen_tcp:close(SocketSub).

will_null_test(_) ->
    Connect = packet:gen_connect("will-qos0-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(53, "will/null/test", 0),
    Suback = packet:gen_suback(53, 0),
    Publish = packet:gen_publish("will/null/test", 0, <<>>, []),
    enable_on_subscribe(),
    enable_on_publish(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    will_null_helper(),
    ok = packet:expect_packet(Socket, "publish", Publish),
    disable_on_subscribe(),
    disable_on_publish(),
    ok = gen_tcp:close(Socket).

will_null_topic_test(_) ->
    Connect = packet:gen_connect("will-null-topic", [{keepalive,60}, {will_topic, empty}, {will_msg, <<"will message">>}]),
    Connack = packet:gen_connack(2),
    {error, closed} = packet:do_client_connect(Connect, Connack, []).

will_qos0_test(_) ->
    Connect = packet:gen_connect("will-qos0-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(53, "will/qos0/test", 0),
    Suback = packet:gen_suback(53, 0),
    Publish = packet:gen_publish("will/qos0/test", 0, <<"will-message">>, []),
    enable_on_subscribe(),
    enable_on_publish(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    will_qos0_helper(),
    ok = packet:expect_packet(Socket, "publish", Publish),
    disable_on_subscribe(),
    disable_on_publish(),
    ok = gen_tcp:close(Socket).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks (as explicit as possible)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
hook_auth_on_subscribe(_,{"", <<"will-qos0-test">>}, [{[<<"will">>, <<"null">>, <<"test">>],0}]) -> ok;
hook_auth_on_subscribe(_,{"", <<"will-qos0-test">>}, [{[<<"will">>, <<"qos0">>, <<"test">>],0}]) -> ok;
hook_auth_on_subscribe(_,{"", <<"will-ign-sub-test">>}, [{[<<"will">>, <<"ignorenormal">>, <<"test">>], 0}]) -> ok.

hook_auth_on_publish(_, _, _MsgId, [<<"ok">>], <<"should be ok">>, false) -> ok;
hook_auth_on_publish(_, _, _MsgId, [<<"will">>,<<"acl">>,<<"test">>], <<"should be denied">>, false) -> {error, not_auth};
hook_auth_on_publish(_, _, _MsgId, [<<"will">>,<<"null">>,<<"test">>], <<>>, false) -> ok;
hook_auth_on_publish(_, _, _MsgId, [<<"will">>,<<"qos0">>,<<"test">>], <<"will-message">>, false) -> ok;
hook_auth_on_publish(_, _, _MsgId, [<<"will">>,<<"ignorenormal">>,<<"test">>], <<"should be ignored">>, false) -> ok.
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

will_null_helper() ->
    Connect = packet:gen_connect("test-helper", [{keepalive,60}, {will_topic, "will/null/test"}, {will_msg, empty}]),
    Connack = packet:gen_connack(0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    gen_tcp:close(Socket).

will_qos0_helper() ->
    Connect = packet:gen_connect("test-helper", [{keepalive,60}, {will_topic, "will/qos0/test"}, {will_msg, <<"will-message">>}]),
    Connack = packet:gen_connack(0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    gen_tcp:close(Socket).

