-module(vmq_clean_session_SUITE).
-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([clean_session_qos1_test/1,
         session_present_test/1]).

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
    [clean_session_qos1_test,
     session_present_test].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
clean_session_qos1_test(_) ->
    Connect = packet:gen_connect("clean-qos1-test", [{keepalive,60}, {clean_session, false}]),
    Connack1 = packet:gen_connack(0),
    Connack2 = packet:gen_connack(true, 0),
    Disconnect = packet:gen_disconnect(),
    Subscribe = packet:gen_subscribe(109, "qos1/clean_session/test", 1),
    Suback = packet:gen_suback(109, 1),
    Publish = packet:gen_publish("qos1/clean_session/test", 1, <<"clean-session-message">>, [{mid, 1}]),
    Puback = packet:gen_puback(1),
    {ok, Socket} = packet:do_client_connect(Connect, Connack1, []),
    enable_on_publish(),
    enable_on_subscribe(),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = gen_tcp:send(Socket, Disconnect),
    ok = gen_tcp:close(Socket),
    %% we should be sure that this session is down,
    %% otherwise we'll get a dup=1 badmatch error
    timer:sleep(100),

    clean_session_qos1_helper(),
    %% Now reconnect and expect a publish message.
    {ok, Socket1} = packet:do_client_connect(Connect, Connack2, []),
    ok = packet:expect_packet(Socket1, "publish", Publish),
    ok = gen_tcp:send(Socket1, Puback),
    disable_on_publish(),
    disable_on_subscribe(),
    ok = gen_tcp:close(Socket1).

session_present_test(_) ->
    Connect = packet:gen_connect("clean-sesspres-test", [{keepalive,10}, {clean_session, false}]),
    ConnackSessionPresentFalse = packet:gen_connack(false, 0),
    ConnackSessionPresentTrue = packet:gen_connack(true, 0),

    {ok, Socket1} = packet:do_client_connect(Connect, ConnackSessionPresentFalse, []),
    ok = gen_tcp:close(Socket1),

    {ok, Socket2} = packet:do_client_connect(Connect, ConnackSessionPresentTrue, []),
    ok = gen_tcp:close(Socket2).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks (as explicit as possible)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
hook_auth_on_subscribe(_,{"", <<"clean-qos1-test">>}, [{[<<"qos1">>, <<"clean_session">>, <<"test">>],1}]) -> ok.

hook_auth_on_publish(_, _, _MsgId, [<<"qos1">>,<<"clean_session">>,<<"test">>], <<"clean-session-message">>, false) -> ok.
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

clean_session_qos1_helper() ->
    Connect = packet:gen_connect("test-helper", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Publish = packet:gen_publish("qos1/clean_session/test", 1, <<"clean-session-message">>, [{mid, 128}]),
    Puback = packet:gen_puback(128),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Publish),
    ok = packet:expect_packet(Socket, "puback", Puback).
