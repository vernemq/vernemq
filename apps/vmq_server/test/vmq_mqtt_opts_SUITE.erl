-module(vmq_mqtt_opts_SUITE).

-include_lib("vmq_commons/include/vmq_types.hrl").

-compile(export_all).
-compile(nowarn_export_all).

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(Config) ->
  cover:start(),
  [{ct_hooks, vmq_cth} | Config].


end_per_suite(_Config) ->
  _Config.

init_per_testcase(_Case, Config) ->
  vmq_test_utils:setup(),
  vmq_server_cmd:set_config(allow_anonymous, true),
  vmq_server_cmd:set_config(retry_interval, 10),
  vmq_server_cmd:set_config(max_client_id_size, 1000),
  vmq_server_cmd:listener_start(1888, [{allowed_protocol_versions, "3,4,5"}]),
  enable_auth_on_publish(),
  enable_auth_on_subscribe(),
  Config.

end_per_testcase(_, Config) ->
  disable_auth_on_publish(),
  disable_auth_on_subscribe(),
  vmq_test_utils:teardown(),
  Config.

all() ->
  [
    {group, mqttv4}
  ].

groups() ->
  V4Tests =
    [qos1_persist_test,
     qos1_non_persist_test,
     qos1_retry_test,
     qos1_non_retry_test],
  [
    {mqttv4, [], V4Tests}
  ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
qos1_persist_test(Cfg) ->
  Connect = packet:gen_connect(vmq_cth:ustr(Cfg) ++ "qos1-persist-test", [{keepalive,60}, {clean_session, false}]),
  Connack1 = packet:gen_connack(0),
  Connack2 = packet:gen_connack(true, 0),
  Disconnect = packet:gen_disconnect(),
  Subscribe = packet:gen_subscribe(109, "qos1/persistence/test", 1, false, false),
  Suback = packet:gen_suback(109, 1),
  Publish = packet:gen_publish("qos1/persistence/test", 1, <<"persist-message">>, [{mid, 1}]),
  {ok, Socket} = packet:do_client_connect(Connect, Connack1, []),
  enable_auth_on_publish(),
  enable_auth_on_subscribe(),
  ok = gen_tcp:send(Socket, Subscribe),
  ok = packet:expect_packet(Socket, "suback", Suback),
  ok = gen_tcp:send(Socket, Disconnect),
  ok = gen_tcp:close(Socket),
  %% we should be sure that this session is down,
  %% otherwise we'll get a dup=1 badmatch error
  timer:sleep(100),

  %% we publish message when client is offline and expect that client will not receive this message
  qos1_opts_helper(packet:gen_publish("qos1/persistence/test", 1, <<"persist-message">>, [{mid, 128}]), 128),
  %% Now reconnect and see that messages are received.
  {ok, Socket1} = packet:do_client_connect(Connect, Connack2, []),
  ok = packet:expect_packet(Socket1, "publish", Publish),
  disable_auth_on_publish(),
  disable_auth_on_subscribe(),
  ok = gen_tcp:close(Socket1).

qos1_non_persist_test(Cfg) ->
  Connect = packet:gen_connect(vmq_cth:ustr(Cfg) ++ "qos1-non-persist-test", [{keepalive,60}, {clean_session, false}]),
  Connack1 = packet:gen_connack(0),
  Connack2 = packet:gen_connack(true, 0),
  Disconnect = packet:gen_disconnect(),
  Subscribe = packet:gen_subscribe(109, "qos1/nonpersistence/test", 1, false, true),
  Suback = packet:gen_suback(109, 1),
  Publish = packet:gen_publish("qos1/nonpersistence/test", 1, <<"non-persist-message">>, [{mid, 1}]),
  {ok, Socket} = packet:do_client_connect(Connect, Connack1, []),
  enable_auth_on_publish(),
  enable_auth_on_subscribe(),
  ok = gen_tcp:send(Socket, Subscribe),
  ok = packet:expect_packet(Socket, "suback", Suback),
  ok = gen_tcp:send(Socket, Disconnect),
  ok = gen_tcp:close(Socket),
  %% we should be sure that this session is down,
  %% otherwise we'll get a dup=1 badmatch error
  timer:sleep(100),

  %% we publish message when client is offline and expect that client will not receive this message
  qos1_opts_helper(packet:gen_publish("qos1/nonpersistence/test", 1, <<"non-persist-message">>, [{mid, 128}]), 128),
  %% Now reconnect and see that no messages are received.
  {ok, Socket1} = packet:do_client_connect(Connect, Connack2, []),
  {1,0,0,0,0} = vmq_queue_sup_sup:summary(),
  {error,timeout} = packet:expect_packet(gen_tcp, Socket1, "publish", Publish, 500),
  disable_auth_on_publish(),
  disable_auth_on_subscribe(),
  ok = gen_tcp:close(Socket1).

qos1_retry_test(Cfg) ->
  Connect = packet:gen_connect(vmq_cth:ustr(Cfg) ++ "qos1-retry-test", [{keepalive,60}, {clean_session, false}]),
  Connack1 = packet:gen_connack(0),
  Disconnect = packet:gen_disconnect(),
  Subscribe = packet:gen_subscribe(109, "qos1/retry/test", 1, false, false),
  Suback = packet:gen_suback(109, 1),
  Publish = packet:gen_publish("qos1/retry/test", 1, <<"retry-message">>, [{mid, 1}]),
  Publish2 = packet:gen_publish("qos1/retry/test", 1, <<"retry-message">>, [{mid, 1}, {dup, 1}]),
  {ok, Socket} = packet:do_client_connect(Connect, Connack1, []),
  enable_auth_on_publish(),
  enable_auth_on_subscribe(),
  ok = gen_tcp:send(Socket, Subscribe),
  ok = packet:expect_packet(Socket, "suback", Suback),

  %% we publish message and expect that broker does not retry even if puback is not sent
  qos1_opts_helper(packet:gen_publish("qos1/retry/test", 1, <<"retry-message">>, [{mid, 128}]), 128),

  %% we receive the message but do not send puback
  ok = packet:expect_packet(Socket, "publish", Publish),

  %% wait for 25 seconds and check that we receive retried messages
  ok = packet:expect_packet(gen_tcp, Socket, "publish", Publish2, 25000),
  ok = gen_tcp:send(Socket, Disconnect),
  ok = gen_tcp:close(Socket),
  disable_auth_on_publish(),
  disable_auth_on_subscribe().

qos1_non_retry_test(Cfg) ->
  Connect = packet:gen_connect(vmq_cth:ustr(Cfg) ++ "qos1-non-retry-test", [{keepalive,60}, {clean_session, false}]),
  Connack1 = packet:gen_connack(0),
  Disconnect = packet:gen_disconnect(),
  Subscribe = packet:gen_subscribe(109, "qos1/nonretry/test", 1, true, false),
  Suback = packet:gen_suback(109, 1),
  Publish = packet:gen_publish("qos1/nonretry/test", 1, <<"non-retry-message">>, [{mid, 1}]),
  {ok, Socket} = packet:do_client_connect(Connect, Connack1, []),
  enable_auth_on_publish(),
  enable_auth_on_subscribe(),
  ok = gen_tcp:send(Socket, Subscribe),
  ok = packet:expect_packet(Socket, "suback", Suback),

  %% we publish message and expect that broker does not retry even if puback is not sent
  qos1_opts_helper(packet:gen_publish("qos1/nonretry/test", 1, <<"non-retry-message">>, [{mid, 128}]), 128),

  %% we receive the message but do not send puback
  ok = packet:expect_packet(Socket, "publish", Publish),

  %% wait for 25 seconds and check that we receive no further messages
  {error,timeout} = packet:expect_packet(gen_tcp, Socket, "publish", Publish, 25000),
  ok = gen_tcp:send(Socket, Disconnect),
  ok = gen_tcp:close(Socket),
  disable_auth_on_publish(),
  disable_auth_on_subscribe().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
hook_auth_on_subscribe(_,_, _) -> ok.
hook_auth_on_publish(_, _, _, _, _, _) -> ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Helper
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
enable_auth_on_subscribe() ->
  vmq_plugin_mgr:enable_module_plugin(
    auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3).
enable_auth_on_publish() ->
  vmq_plugin_mgr:enable_module_plugin(
    auth_on_publish, ?MODULE, hook_auth_on_publish, 6).
disable_auth_on_subscribe() ->
  vmq_plugin_mgr:disable_module_plugin(
    auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3).
disable_auth_on_publish() ->
  vmq_plugin_mgr:disable_module_plugin(
    auth_on_publish, ?MODULE, hook_auth_on_publish, 6).

qos1_opts_helper(Publish, MID) ->
  Connect = packet:gen_connect("test-helper", [{keepalive,60}]),
  Connack = packet:gen_connack(0),
  Puback = packet:gen_puback(MID),
  {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
  ok = gen_tcp:send(Socket, Publish),
  ok = packet:expect_packet(Socket, "puback", Puback),
  gen_tcp:close(Socket).
