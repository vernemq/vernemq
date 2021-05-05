-module(vmq_clean_session_SUITE).

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
    ct:pal("Clean sessions tests. Config: ~p~n", [Config]),
    vmq_test_utils:setup(),
    vmq_server_cmd:set_config(allow_anonymous, true),
    vmq_server_cmd:set_config(retry_interval, 10),
    vmq_server_cmd:set_config(max_client_id_size, 1000),
    vmq_server_cmd:set_config(metadata_plugin, vmq_swc), %doesn't work?
    vmq_server_cmd:listener_start(1888, [{allowed_protocol_versions, "3,4,5"}]),
    enable_on_publish(),
    enable_on_subscribe(),
    Config.

end_per_testcase(_, Config) ->
    disable_on_publish(),
    disable_on_subscribe(),
    vmq_test_utils:teardown(),
    Config.

all() ->
    [
     {group, mqttv4},
     {group, mqttv5}
    ].

groups() ->
    Tests =
    [clean_session_qos1_test,
     session_cleanup_test,
     session_present_test],
    [
     {mqttv4, [], Tests},
     {mqttv5, [shuffle],
      [session_expiration_connect_test,
       session_expiration_disconnect_test,
       session_expiration_reset_at_disconnect,
       session_exp_only_at_disconnect_is_illegal]}
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
clean_session_qos1_test(Cfg) ->
    ct:pal("Starting Summary in 'clean_session_qos1_test1' ~p~n", [vmq_queue_sup_sup:summary()]),

    Connect = packet:gen_connect(vmq_cth:ustr(Cfg) ++ "clean-qos1-test", [{keepalive,180}, {clean_session, false}]),
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
    timer:sleep(5000),

    clean_session_qos1_helper(),
    %% Now reconnect and expect a publish message.
    {ok, Socket1} = packet:do_client_connect(Connect, Connack2, []),
    ok = packet:expect_packet(Socket1, "publish", Publish),
    ok = gen_tcp:send(Socket1, Puback),
    disable_on_publish(),
    disable_on_subscribe(),
    ok = gen_tcp:close(Socket1).

session_cleanup_test(Cfg) ->
    ct:pal("Starting Summary ~p~n", [vmq_queue_sup_sup:summary()]),
    ClientId = vmq_cth:ustr(Cfg) ++ "clean-qos1-test",
    Connect1 = packet:gen_connect(ClientId, [{keepalive,60}, {clean_session, false}]),
    Connect2 = packet:gen_connect(ClientId, [{keepalive,60}, {clean_session, true}]),
    Connack = packet:gen_connack(0),
    Disconnect = packet:gen_disconnect(),
    Subscribe = packet:gen_subscribe(109, "qos1/clean_session/test", 1),
    Suback = packet:gen_suback(109, 1),
    {ok, Socket} = packet:do_client_connect(Connect1, Connack, []),
    ct:pal("Opening Socket ~p~n, We have Summary : ~p ~n", [Socket, vmq_queue_sup_sup:summary()]),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = gen_tcp:send(Socket, Disconnect),
    ct:pal("Closing Socket ~p~n with Summary ~p~n", [Socket, vmq_queue_sup_sup:summary()]),
    ok = gen_tcp:close(Socket),

    clean_session_qos1_helper(),
    timer:sleep(100),
    {0,0,0,1,1} = vmq_queue_sup_sup:summary(), % there should be 1 offline queue, and 1 stored message
    {ok, Socket1} = packet:do_client_connect(Connect2, Connack, []),
    ct:pal("Opening Socket1 ~p~n", [Socket1]),
    ok = gen_tcp:close(Socket1),
    timer:sleep(100),
    %% if queue cleanup woudln't have happen, we'd see a remaining offline message
    {0,0,0,0,0} = vmq_queue_sup_sup:summary().

session_present_test(Cfg) ->
    Connect = packet:gen_connect(vmq_cth:ustr(Cfg) ++ "clean-sesspres-test", [{keepalive,10}, {clean_session, false}]),
    ConnackSessionPresentFalse = packet:gen_connack(false, 0),
    ConnackSessionPresentTrue = packet:gen_connack(true, 0),

    {ok, Socket1} = packet:do_client_connect(Connect, ConnackSessionPresentFalse, []),
    ok = gen_tcp:close(Socket1),

    {ok, Socket2} = packet:do_client_connect(Connect, ConnackSessionPresentTrue, []),
    ok = gen_tcp:close(Socket2).

session_expiration_connect_test(Cfg) ->
    %% connect with clean_start false 1 sec expiration
    ClientId = vmq_cth:ustr(Cfg),
    Connect = packetv5:gen_connect(ClientId, [{keepalive, 60},
                                              {clean_start, false},
                                              {properties,
                                               #{p_session_expiry_interval => 1}}]),
    %% TODOv5: Should the session expiration interval be in here?
    Connack = packetv5:gen_connack(0, ?M5_CONNACK_ACCEPT, #{}),
    {ok, Socket} = packetv5:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:close(Socket),

    ConnackSP = packetv5:gen_connack(1, ?M5_CONNACK_ACCEPT, #{}),
    {ok, Socket1} = packetv5:do_client_connect(Connect, ConnackSP, []),
    ok = gen_tcp:close(Socket1),

    %% make sure the client has dissappeared.
    timer:sleep(1100),
    {ok, Socket2} = packetv5:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:close(Socket2).

session_expiration_disconnect_test(Cfg) ->
    %% connect with clean_start false 0 sec expiration
    ClientId = vmq_cth:ustr(Cfg),
    Connect = packetv5:gen_connect(ClientId, [{keepalive, 60},
                                              {clean_start, false},
                                              {properties,
                                               #{p_session_expiry_interval => 1}}]),
    Connack = packetv5:gen_connack(0, ?M5_CONNACK_ACCEPT, #{}),
    ConnackSP = packetv5:gen_connack(1, ?M5_CONNACK_ACCEPT, #{}),
    Disconnect1 = packetv5:gen_disconnect(0, #{p_session_expiry_interval => 1}),

    {ok, Socket} = packetv5:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Disconnect1),
    ok = gen_tcp:close(Socket),

    {ok, Socket1} = packetv5:do_client_connect(Connect, ConnackSP, []),
    ok = gen_tcp:send(Socket1, Disconnect1),
    ok = gen_tcp:close(Socket1),

    %% make sure the client has dissappeared.
    timer:sleep(1100),
    {ok, Socket2} = packetv5:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:close(Socket2).

session_expiration_reset_at_disconnect(Cfg) ->
    ClientId = vmq_cth:ustr(Cfg),
    Connect = packetv5:gen_connect(ClientId, [{keepalive, 60},
                                              {clean_start, true},
                                              {properties,
                                               #{p_session_expiry_interval => 5}}]),
    Connack = packetv5:gen_connack(0, ?M5_CONNACK_ACCEPT, #{}),
    Disconnect = packetv5:gen_disconnect(0, #{p_session_expiry_interval => 0}),

    {ok, Socket} = packetv5:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Disconnect),
    ok = gen_tcp:close(Socket),

    {ok, Socket1} = packetv5:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:close(Socket1).

session_exp_only_at_disconnect_is_illegal(Cfg) ->
    %% it's a protocol error to set session expiration only at
    %% disconnect time.

    %% When a Server detects a Malformed Packet or Protocol Error, and
    %% a Reason Code is given in the specification, it MUST close the
    %% Network Connection [MQTT-4.13.1-1]

    ClientId = vmq_cth:ustr(Cfg),
    Connect = packetv5:gen_connect(ClientId, [{keepalive, 60},
                                              {clean_start, false}]),
    Connack = packetv5:gen_connack(0, ?M5_CONNACK_ACCEPT, #{}),
    Disconnect = packetv5:gen_disconnect(0, #{p_session_expiry_interval => 100}),

    {ok, Socket} = packetv5:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Disconnect),
    DisconnectProtoErr = packetv5:gen_disconnect(?M5_PROTOCOL_ERROR, #{}),
    ok = packetv5:expect_frame(Socket, DisconnectProtoErr),
    {error, closed} = gen_tcp:recv(Socket, 0, 100).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
hook_auth_on_subscribe(_,_, _) -> ok.
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

clean_session_qos1_helper() ->
    Connect = packet:gen_connect("test-helper", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Publish = packet:gen_publish("qos1/clean_session/test", 1, <<"clean-session-message">>, [{mid, 128}]),
    Puback = packet:gen_puback(128),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ct:pal("Socket in the Publisher Helper: ~p~n with Summary ~p~n", [Socket, vmq_queue_sup_sup:summary()]),
    ok = gen_tcp:send(Socket, Publish),
    ok = packet:expect_packet(Socket, "puback", Puback),
    gen_tcp:close(Socket).
