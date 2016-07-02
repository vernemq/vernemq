-module(vmq_retain_SUITE).
-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([retain_qos0_test/1,
         retain_qos0_repeated_test/1,
         retain_qos0_fresh_test/1,
         retain_qos0_clear_test/1,
         retain_qos1_qos0_test/1,
         publish_empty_retained_msg_test/1]).

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
    [retain_qos0_test,
     retain_qos0_repeated_test,
     retain_qos0_fresh_test,
     retain_qos0_clear_test,
     retain_qos1_qos0_test,
     publish_empty_retained_msg_test].

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
    enable_on_publish(),
    enable_on_subscribe(),
    %% Send retained message
    ok = gen_tcp:send(Socket, Publish),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = packet:expect_packet(Socket, "publish", Publish),
    disable_on_publish(),
    disable_on_subscribe(),
    ok = gen_tcp:close(Socket).

retain_qos0_repeated_test(_) ->
    Connect = packet:gen_connect("retain-qos0-rep-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Publish = packet:gen_publish("retain/qos0/test", 0, <<"retained message">>, [{retain, true}]),
    Subscribe = packet:gen_subscribe(16, "retain/qos0/test", 0),
    Suback = packet:gen_suback(16, 0),
    Unsubscribe = packet:gen_unsubscribe(13, "retain/qos0/test"),
    Unsuback = packet:gen_unsuback(13),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    enable_on_publish(),
    enable_on_subscribe(),
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
    disable_on_publish(),
    disable_on_subscribe(),
    ok = gen_tcp:close(Socket).

retain_qos0_fresh_test(_) ->
    Connect = packet:gen_connect("retain-qos0-fresh-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Publish = packet:gen_publish("retain/qos0/test", 0, <<"retained message">>, [{retain, true}]),
    PublishFresh = packet:gen_publish("retain/qos0/test", 0, <<"retained message">>, []),
    Subscribe = packet:gen_subscribe(16, "retain/qos0/test", 0),
    Suback = packet:gen_suback(16, 0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    enable_on_publish(),
    enable_on_subscribe(),
    %% Send retained message
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = gen_tcp:send(Socket, Publish),
    ok = packet:expect_packet(Socket, "publish", PublishFresh),
    disable_on_publish(),
    disable_on_subscribe(),
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
    enable_on_publish(),
    enable_on_subscribe(),
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
    disable_on_publish(),
    disable_on_subscribe(),
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
    enable_on_publish(),
    enable_on_subscribe(),
    %% Send retained message
    ok = gen_tcp:send(Socket, Publish),
    ok = packet:expect_packet(Socket, "puback", Puback),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = packet:expect_packet(Socket, "publish0", Publish0),
    disable_on_publish(),
    disable_on_subscribe(),
    ok = gen_tcp:close(Socket).

publish_empty_retained_msg_test(_) ->
    Connect = packet:gen_connect("retain-clear-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Publish = packet:gen_publish("retain/clear/test", 0, <<"retained message">>, [{retain, true}]),
    RetainClearPub = packet:gen_publish("retain/clear/test", 0, <<>>, [{retain, true}]),
    RetainClearSub = packet:gen_publish("retain/clear/test", 0, <<>>, [{retain, false}]),
    Subscribe = packet:gen_subscribe(592, "retain/clear/test", 0),
    Suback = packet:gen_suback(592, 0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),

    enable_on_publish(),
    enable_on_subscribe(),
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
    disable_on_publish(),
    disable_on_subscribe(),
    ok = gen_tcp:close(Socket).
    %%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks (as explicit as possible)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
hook_auth_on_subscribe(_,{"", <<"retain-qos0-test">>}, [{[<<"retain">>, <<"qos0">>, <<"test">>],0}]) -> ok;
hook_auth_on_subscribe(_,{"", <<"retain-qos0-rep-test">>}, [{[<<"retain">>, <<"qos0">>, <<"test">>],0}]) -> ok;
hook_auth_on_subscribe(_,{"", <<"retain-qos0-fresh-test">>}, [{[<<"retain">>,<<"qos0">>,<<"test">>],0}]) -> ok;
hook_auth_on_subscribe(_,{"", <<"retain-clear-test">>}, [{[<<"retain">>,<<"clear">>,<<"test">>],0}]) -> ok;
hook_auth_on_subscribe(_,{"", <<"retain-qos1-test">>}, [{[<<"retain">>,<<"qos1">>,<<"test">>],0}]) -> ok.

hook_auth_on_publish(_, _, _MsgId, [<<"retain">>,<<"qos0">>,<<"test">>], <<"retained message">>, true) -> ok;
%% retain_qos0_clear(_) Both cases must be covered retain and clear-retain
hook_auth_on_publish(_, _, _MsgId, [<<"retain">>,<<"clear">>,<<"test">>], <<"retained message">>, true) -> ok;
hook_auth_on_publish(_, _, _MsgId, [<<"retain">>,<<"clear">>,<<"test">>], <<>>, true) -> ok;
%
hook_auth_on_publish(_, _, _MsgId, [<<"retain">>,<<"qos1">>,<<"test">>], <<"retained message">>, true) -> ok.
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
