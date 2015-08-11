-module(vmq_in_order_delivery_SUITE).
-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([in_order_qos1_test/1,
         in_order_qos2_test/1]).

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
    {ok, _} = vmq_server_cmd:set_config(max_inflight_messages, 1),
    vmq_server_cmd:listener_start(1888, []),
    Config.

end_per_testcase(_, Config) ->
    vmq_test_utils:teardown(),
    Config.

all() ->
    [in_order_qos1_test,
     in_order_qos2_test].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
in_order_qos1_test(_) ->
    Connect = packet:gen_connect("inflight-qos1-test", [{keepalive,60}, {clean_session, true}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(109, "qos1/inflight/test", 1),
    Suback = packet:gen_suback(109, 1),
    Publish1 = packet:gen_publish("qos1/inflight/test", 1, <<"inflight-message-1">>, [{mid, 1}]),
    Publish2 = packet:gen_publish("qos1/inflight/test", 1, <<"inflight-message-2">>, [{mid, 2}]),
    Publish3 = packet:gen_publish("qos1/inflight/test", 1, <<"inflight-message-3">>, [{mid, 3}]),
    Puback1 = packet:gen_puback(1),
    Puback2 = packet:gen_puback(2),
    Puback3 = packet:gen_puback(3),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    enable_on_publish(),
    enable_on_subscribe(),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    %% ^connected and subscribed
    %%
    %% in-order delivery
    inflight_test_qos1_helper(<<"inflight-message-1">>),
    inflight_test_qos1_helper(<<"inflight-message-2">>),
    inflight_test_qos1_helper(<<"inflight-message-3">>),
    ok = packet:expect_packet(Socket, "publish1", Publish1),
    ok = gen_tcp:send(Socket, Puback1),
    ok = packet:expect_packet(Socket, "publish2", Publish2),
    ok = gen_tcp:send(Socket, Puback2),
    ok = packet:expect_packet(Socket, "publish3", Publish3),
    ok = gen_tcp:send(Socket, Puback3),

    disable_on_publish(),
    disable_on_subscribe(),
    ok = gen_tcp:close(Socket).

in_order_qos2_test(_) ->
    Connect = packet:gen_connect("inflight-qos2-test", [{keepalive,60}, {clean_session, true}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(109, "qos2/inflight/test", 2),
    Suback = packet:gen_suback(109, 2),
    Publish1 = packet:gen_publish("qos2/inflight/test", 2, <<"inflight-message-1">>, [{mid, 1}]),
    Publish2 = packet:gen_publish("qos2/inflight/test", 2, <<"inflight-message-2">>, [{mid, 2}]),
    Publish3 = packet:gen_publish("qos2/inflight/test", 2, <<"inflight-message-3">>, [{mid, 3}]),
    Pubrec1 = packet:gen_pubrec(1),
    Pubrec2 = packet:gen_pubrec(2),
    Pubrec3 = packet:gen_pubrec(3),
    Pubrel1 = packet:gen_pubrel(1),
    Pubrel2 = packet:gen_pubrel(2),
    Pubrel3 = packet:gen_pubrel(3),
    Pubcomp1 = packet:gen_pubcomp(1),
    Pubcomp2 = packet:gen_pubcomp(2),
    Pubcomp3 = packet:gen_pubcomp(3),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    enable_on_publish(),
    enable_on_subscribe(),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    %% ^connected and subscribed
    %%
    %% in-order delivery
    inflight_test_qos2_helper(<<"inflight-message-1">>),
    inflight_test_qos2_helper(<<"inflight-message-2">>),
    inflight_test_qos2_helper(<<"inflight-message-3">>),

    ok = packet:expect_packet(Socket, "publish1", Publish1),
    ok = gen_tcp:send(Socket, Pubrec1),
    ok = packet:expect_packet(Socket, "pubrel1", Pubrel1),
    ok = gen_tcp:send(Socket, Pubcomp1),

    ok = packet:expect_packet(Socket, "publish2", Publish2),
    ok = gen_tcp:send(Socket, Pubrec2),
    ok = packet:expect_packet(Socket, "pubrel2", Pubrel2),
    ok = gen_tcp:send(Socket, Pubcomp2),

    ok = packet:expect_packet(Socket, "publish3", Publish3),
    ok = gen_tcp:send(Socket, Pubrec3),
    ok = packet:expect_packet(Socket, "pubrel3", Pubrel3),
    ok = gen_tcp:send(Socket, Pubcomp3),

    disable_on_publish(),
    disable_on_subscribe(),
    ok = gen_tcp:close(Socket).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks (as explicit as possible)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
hook_auth_on_subscribe(_,_,_) -> ok.
hook_auth_on_publish(_, _, _,_, _,_) -> ok.
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

inflight_test_qos1_helper(Payload) ->
    Connect = packet:gen_connect("test-helper", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Disconnect = packet:gen_disconnect(),
    Publish = packet:gen_publish("qos1/inflight/test", 1, Payload, [{mid, 128}]),
    Puback = packet:gen_puback(128),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Publish),
    ok = packet:expect_packet(Socket, "puback", Puback),
    ok = gen_tcp:send(Socket, Disconnect),
    ok = gen_tcp:close(Socket).


inflight_test_qos2_helper(Payload) ->
    Connect = packet:gen_connect("test-helper", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Disconnect = packet:gen_disconnect(),
    Publish = packet:gen_publish("qos2/inflight/test", 2, Payload, [{mid, 128}]),
    Pubrec = packet:gen_pubrec(128),
    Pubrel = packet:gen_pubrel(128),
    Pubcomp = packet:gen_pubcomp(128),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Publish),
    ok = packet:expect_packet(Socket, "pubrec", Pubrec),
    ok = gen_tcp:send(Socket, Pubrel),
    ok = packet:expect_packet(Socket, "pubcomp", Pubcomp),
    ok = gen_tcp:send(Socket, Disconnect),
    ok = gen_tcp:close(Socket).
