-module(vmq_clean_session_tests).
-include_lib("eunit/include/eunit.hrl").

-define(setup(F), {setup, fun setup/0, fun teardown/1, F}).
-define(listener(Port), {{{127,0,0,1}, Port}, [{max_connections, infinity},
                                               {nr_of_acceptors, 10},
                                               {mountpoint, ""}]}).
-export([hook_auth_on_subscribe/3,
         hook_auth_on_publish/6]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Tests Descriptions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
subscribe_test_() ->
    [
     {"Clean Session QoS 1",
      ?setup(fun clean_session_qos1/1)}
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Setup Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
setup() ->
    application:load(vmq_server),
    application:set_env(vmq_server, allow_anonymous, true),
    application:set_env(vmq_server, listeners,
                        [{mqtt, [?listener(1888)]}]),
    application:set_env(vmq_server, retry_interval, 10),
    vmq_server:start_no_auth(),
    vmq_msg_store:clean_all([]),
    vmq_reg:reset_all_tables([]),
    wait_til_ready().
teardown(_) ->
    [vmq_plugin_mgr:disable_plugin(P) || P <- vmq_plugin:info(all)],
    vmq_server:stop().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
clean_session_qos1(_) ->
    Connect = packet:gen_connect("clean-qos1-test", [{keepalive,60}, {clean_session, false}]),
    Connack = packet:gen_connack(0),
    Disconnect = packet:gen_disconnect(),
    Subscribe = packet:gen_subscribe(109, "qos1/clean_session/test", 1),
    Suback = packet:gen_suback(109, 1),
    Publish = packet:gen_publish("qos1/clean_session/test", 1, <<"clean-session-message">>, [{mid, 1}]),
    Puback = packet:gen_puback(1),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    enable_on_publish(),
    enable_on_subscribe(),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = gen_tcp:send(Socket, Disconnect),
    ok = gen_tcp:close(Socket),
    clean_session_qos1_helper(),
    %% Now reconnect and expect a publish message.
    {ok, Socket1} = packet:do_client_connect(Connect, Connack, []),
    ok = packet:expect_packet(Socket1, "publish", Publish),
    ok = gen_tcp:send(Socket1, Puback),
    disable_on_publish(),
    disable_on_subscribe(),
    ?_assertEqual(ok, gen_tcp:close(Socket1)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks (as explicit as possible)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
hook_auth_on_subscribe(_,{"", "clean-qos1-test"}, [{"qos1/clean_session/test",1}]) -> ok.

hook_auth_on_publish(_, _, _MsgId, "qos1/clean_session/test", <<"clean-session-message">>, false) -> ok.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Helper
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
wait_til_ready() ->
    wait_til_ready(vmq_cluster:is_ready(), 100).
wait_til_ready(true, _) -> ok;
wait_til_ready(false, I) when I > 0 ->
    timer:sleep(5),
    wait_til_ready(vmq_cluster:is_ready(), I - 1);
wait_til_ready(_, _) -> exit(not_ready).

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
