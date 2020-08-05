-module(vmq_last_will_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-import(mqtt5_v4compat,
        [gen_connect/3,
         gen_connack/2,
         gen_subscribe/4,
         gen_suback/3,
         gen_publish/5,
         gen_disconnect/1,
         expect_packet/4,
         do_client_connect/4]).

-include_lib("vmq_commons/include/vmq_types.hrl").

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(Config) ->
    cover:start(),
    vmq_test_utils:setup(),
    vmq_server_cmd:set_config(allow_anonymous, true),
    vmq_server_cmd:set_config(max_last_will_delay, "1h"),
    vmq_server_cmd:set_config(retry_interval, 10),
    vmq_server_cmd:listener_start(1888, [{allowed_protocol_versions, "3,4,5"}]),
    [{ct_hooks, vmq_cth} |Config].

end_per_suite(_Config) ->
    vmq_test_utils:teardown(),
    _Config.

init_per_group(mqttv4, Config) ->
    [{protover, 4}|Config];
init_per_group(mqttv5, Config) ->
    [{protover, 5}|Config].

end_per_group(_, _Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    %% make sure to have sane defaults
    vmq_server_cmd:set_config(allow_anonymous, true),
    vmq_server_cmd:set_config(retry_interval, 10),
    vmq_server_cmd:set_config(suppress_lwt_on_session_takeover, false),
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
        [will_denied_test,
         will_ignored_for_normal_disconnect_test,
         will_null_test,
         will_null_topic_test,
         will_qos0_test,
         suppress_lwt_on_session_takeover_test],
    [
     {mqttv4, [shuffle], Tests ++ []},
     {mqttv5, [shuffle], Tests ++ [will_delay_v5_test,
                                   disconnect_with_will_msg_test]}
    ].


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
will_denied_test(Config) ->
    ConnectOK = gen_connect("will-acl-test",
                            [{keepalive,60}, {will_topic, "ok"}, {will_msg, <<"should be ok">>}],
                            Config),
    ConnackOK = gen_connack(success, Config),
    Connect = gen_connect("will-acl-test",
                          [{keepalive,60}, {will_topic, "will/acl/test"}, {will_msg, <<"should be denied">>}],
                          Config),
    Connack = gen_connack(not_authorized, Config),
    enable_on_publish(),
    {ok, SocketOK} = do_client_connect(ConnectOK, ConnackOK, [], Config),
    {ok, Socket} = do_client_connect(Connect, Connack, [], Config),
    disable_on_publish(),
    ok = gen_tcp:close(Socket),
    ok = gen_tcp:close(SocketOK).

will_ignored_for_normal_disconnect_test(Config) ->
    ConnectPub = gen_connect("will-ign-pub-test",
                             [{keepalive,60},
                              {will_topic, "will/ignorenormal/test"},
                              {will_msg, <<"should be ignored">>}],
                             Config),
    ConnackPub = gen_connack(success, Config),
    ConnectSub = gen_connect("will-ign-sub-test", [{keepalive,60}], Config),
    ConnackSub = gen_connack(success, Config),
    enable_on_subscribe(),
    enable_on_publish(),
    {ok, SocketPub} = do_client_connect(ConnectPub, ConnackPub, [], Config),
    {ok, SocketSub} = do_client_connect(ConnectSub, ConnackSub, [], Config),
    Subscribe = gen_subscribe(53, "will/ignorenormal/test", 0, Config),
    Suback = gen_suback(53, 0, Config),
    ok = gen_tcp:send(SocketSub, Subscribe),
    ok = expect_packet(SocketSub, "suback", Suback, Config),
    Disconnect = gen_disconnect(Config),
    ok = gen_tcp:send(SocketPub, Disconnect),
    %% we don't match on any packet, so that we crash
    {error, timeout} = gen_tcp:recv(SocketSub, 0, 200),
    disable_on_subscribe(),
    disable_on_publish(),
    ok = gen_tcp:close(SocketSub).

will_null_test(Config) ->
    Connect = gen_connect("will-qos0-test", [{keepalive,60}], Config),
    Connack = gen_connack(success, Config),
    Subscribe = gen_subscribe(53, "will/null/test", 0, Config),
    Suback = gen_suback(53, 0, Config),
    Publish = gen_publish("will/null/test", 0, <<>>, [], Config),
    enable_on_subscribe(),
    enable_on_publish(),
    {ok, Socket} = do_client_connect(Connect, Connack, [], Config),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = expect_packet(Socket, "suback", Suback, Config),
    will_null_helper(Config),
    ok = expect_packet(Socket, "publish", Publish, Config),
    disable_on_subscribe(),
    disable_on_publish(),
    ok = gen_tcp:close(Socket).

will_null_topic_test(Config) ->
    Connect = gen_connect("will-null-topic", [{keepalive,60}, {will_topic, empty}, {will_msg, <<"will message">>}], Config),
    %% doesn't matter which connack code we specify as the connection
    %% will be closed.
    Connack = gen_connack(bad_identifier, Config),
    {error, closed} = do_client_connect(Connect, Connack, [], Config).

will_qos0_test(Config) ->
    Connect = gen_connect("will-qos0-test", [{keepalive,60}], Config),
    Connack = gen_connack(success, Config),
    Subscribe = gen_subscribe(53, "will/qos0/test", 0, Config),
    Suback = gen_suback(53, 0, Config),
    Publish = gen_publish("will/qos0/test", 0, <<"will-message">>, [], Config),
    enable_on_subscribe(),
    enable_on_publish(),
    {ok, Socket} = do_client_connect(Connect, Connack, [], Config),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = expect_packet(Socket, "suback", Suback, Config),
    will_qos0_helper(Config),
    ok = expect_packet(Socket, "publish", Publish, Config),
    disable_on_subscribe(),
    disable_on_publish(),
    ok = gen_tcp:close(Socket).

will_delay_v5_test(_Config) ->
    %% [MQTT-3.1.2-8] The Will Message MUST be published after the
    %% Network Connection is subsequently closed and either the Will
    %% Delay Interval has elapsed or the Session ends, unless the Will
    %% Message has been deleted by the Server on receipt of a
    %% DISCONNECT packet with Reason Code 0x00 (Normal disconnection)
    %% or a new Network Connection for the ClientID is opened before
    %% the Will Delay Interval has elapsed.

    WillTopic = <<"will/delay/test/0">>,

    enable_on_subscribe(),
    enable_on_publish(),

    %% connect subscriber
    SubTopic = #mqtt5_subscribe_topic{
       topic = WillTopic,
       qos = 0,
       no_local = false,
       rap = false,
       retain_handling = send_retain
      },

    Connack = packetv5:gen_connack(),
    Subscribe = packetv5:gen_subscribe(53, [SubTopic], #{}),
    SubConnect = packetv5:gen_connect("will-delay-test-sub",
                                      [{keepalive, 60}]),

    {ok, SubSocket} = packetv5:do_client_connect(SubConnect, Connack, []),
    ok = gen_tcp:send(SubSocket, Subscribe),
    SubAck = packetv5:gen_suback(53, [0], #{}),
    ok = packetv5:expect_frame(SubSocket, SubAck),

    %% connect client with delayed last will
    WillMsg = <<"delayed_msg">>,
    WillDelay = 1, %% secs
    WillProperties = #{p_will_delay_interval => WillDelay},
    LastWill = #mqtt5_lwt{
                  will_properties = WillProperties,
                  will_retain = false,
                  will_qos = 0,
                  will_topic = WillTopic,
                  will_msg = WillMsg},
    WillConnect = packetv5:gen_connect("will-delay-test",
                                       [{keepalive, 60},
                                        {clean_start, false},
                                        {lwt, LastWill},
                                        {properties, #{p_session_expiry_interval => 10}}]),
    {ok, Socket} = packetv5:do_client_connect(WillConnect, Connack, []),

    %% Disconnect and measure that it takes at least the delay time
    %% for the message to arrive.
    T1 = ts(),
    ok = gen_tcp:close(Socket),
    LastWillPub = packetv5:gen_publish(WillTopic, 0, WillMsg, []),
    ok = packetv5:expect_frame(SubSocket, LastWillPub),
    T2 = ts(),
    assert_ge(T2 - T1, WillDelay*1000),
    disable_on_publish(),
    disable_on_subscribe().

assert_ge(V1, V2) when V1 >= V2 -> true.

ts() ->
    vmq_time:timestamp(millisecond).

%% TODO: port this test (and logic) to the MQTTv5 fsm as well!
suppress_lwt_on_session_takeover_test(Config) ->
    enable_on_subscribe(),
    enable_on_publish(),

    LWTTopic = "suppress/will/test",
    LWTMsg = <<"suppress-will-test-msg">>,
    ClientIdLWTSub = vmq_cth:ustr(Config) ++ "subscriber",

    %% setup a subscriber to receive LWT messages
    ConnectSub = gen_connect(ClientIdLWTSub, [{keepalive,60}], Config),
    Connack = gen_connack(success, Config),
    Subscribe = gen_subscribe(53, LWTTopic, 0, Config),
    Suback = gen_suback(53, 0, Config),
    {ok, Socket} = do_client_connect(ConnectSub, Connack, [], Config),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = expect_packet(Socket, "suback", Suback, Config),

    ExpLWTPublish = gen_publish(LWTTopic, 0, LWTMsg, [], Config),
    ClientId = vmq_cth:ustr(Config),
    Connect = gen_connect(ClientId,
                          [{keepalive,60}, {will_topic, LWTTopic}, {will_msg, LWTMsg}],
                          Config),

    %% connect a client which won't suppress the publication of lwt messages
    vmq_server_cmd:set_config(suppress_lwt_on_session_takeover, false),
    {ok, _} = do_client_connect(Connect, Connack, [], Config),

    %% do a session take-over and see the LWT message is published.
    %% also suppress LWT for the new session in case of a session
    %% take-over.
    vmq_server_cmd:set_config(suppress_lwt_on_session_takeover, true),
    {ok, _} = do_client_connect(Connect, Connack, [], Config),
    ok = expect_packet(Socket, "publish", ExpLWTPublish, Config),

    %% connecting again will produce no LWT message.
    {ok, _} = do_client_connect(Connect, Connack, [], Config),
    {error, timeout} = gen_tcp:recv(Socket, 0, 200),

    disable_on_subscribe(),
    disable_on_publish().

disconnect_with_will_msg_test(Config) ->
    enable_on_subscribe(),
    enable_on_publish(),

    Topic = "disconnect/with/will/msg",
    Msg = <<"disconnect-with-will-msg">>,
    ClientIdLWTSub = vmq_cth:ustr(Config) ++ "subscriber",

    %% setup a subscriber to receive LWT messages
    ConnectSub = gen_connect(ClientIdLWTSub, [{keepalive,60}], Config),
    Connack = gen_connack(success, Config),
    Subscribe = gen_subscribe(53, Topic, 0, Config),
    Suback = gen_suback(53, 0, Config),
    {ok, Socket} = do_client_connect(ConnectSub, Connack, [], Config),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = expect_packet(Socket, "suback", Suback, Config),

    ExpLWTPublish = gen_publish(Topic, 0, Msg, [], Config),
    ClientId = vmq_cth:ustr(Config),
    Connect = gen_connect(ClientId,
                          [{keepalive,60}, {will_topic, Topic}, {will_msg, Msg}],
                          Config),

    %% connect and disconnec with lwt msg
    {ok, LWTSocket} = do_client_connect(Connect, Connack, [], Config),
    Disconnect = packetv5:gen_disconnect(?M5_DISCONNECT_WITH_WILL_MSG, #{}),
    ok = gen_tcp:send(LWTSocket, Disconnect),
    ok = gen_tcp:close(LWTSocket),

    ok = expect_packet(Socket, "publish", ExpLWTPublish, Config),
    ok = gen_tcp:close(Socket),

    disable_on_subscribe(),
    disable_on_publish().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks (as explicit as possible)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
hook_auth_on_subscribe(_, _, _) -> ok.

hook_auth_on_publish(_, _, _MsgId, [<<"will">>,<<"acl">>,<<"test">>], <<"should be denied">>, false) -> {error, not_auth};
hook_auth_on_publish(_, _, _, _, _, _) -> ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Helper
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
enable_on_subscribe() ->
    vmq_plugin_mgr:enable_module_plugin(
      auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3),
    ok = vmq_plugin_mgr:enable_module_plugin(
           auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3,
           [{compat, {auth_on_subscribe_m5, vmq_plugin_compat_m5,
                      convert, 4}}]).
enable_on_publish() ->
    ok = vmq_plugin_mgr:enable_module_plugin(
           auth_on_publish, ?MODULE, hook_auth_on_publish, 6),
    ok = vmq_plugin_mgr:enable_module_plugin(
           auth_on_publish, ?MODULE, hook_auth_on_publish, 6,
           [{compat, {auth_on_publish_m5, vmq_plugin_compat_m5,
                    convert, 7}}]).
disable_on_subscribe() ->
    ok = vmq_plugin_mgr:disable_module_plugin(
           auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3),
    ok = vmq_plugin_mgr:disable_module_plugin(
           auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3,
           [{compat, {auth_on_subscribe_m5, vmq_plugin_compat_m5,
                      convert, 4}}]).
disable_on_publish() ->
    ok = vmq_plugin_mgr:disable_module_plugin(
           auth_on_publish, ?MODULE, hook_auth_on_publish, 6),
    ok = vmq_plugin_mgr:disable_module_plugin(
           auth_on_publish, ?MODULE, hook_auth_on_publish, 6,
           [{compat, {auth_on_publish_m5, vmq_plugin_compat_m5,
                      convert, 7}}]).

will_null_helper(Config) ->
    Connect = gen_connect("test-helper", [{keepalive,60}, {will_topic, "will/null/test"}, {will_msg, empty}], Config),
    Connack = gen_connack(success, Config),
    {ok, Socket} = do_client_connect(Connect, Connack, [], Config),
    gen_tcp:close(Socket).

will_qos0_helper(Config) ->
    Connect = gen_connect("test-helper", [{keepalive,60}, {will_topic, "will/qos0/test"}, {will_msg, <<"will-message">>}], Config),
    Connack = gen_connack(success, Config),
    {ok, Socket} = do_client_connect(Connect, Connack, [], Config),
    gen_tcp:close(Socket).

