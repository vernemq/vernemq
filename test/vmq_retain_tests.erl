-module(vmq_retain_tests).
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
     {"Retain QoS 0",
      ?setup(fun retain_qos0/1)}
    ,{"Retain QoS 0 Repeated",
      ?setup(fun retain_qos0_repeated/1)}
    ,{"Retain QoS 0 Fresh",
      ?setup(fun retain_qos0_fresh/1)}
    ,{"Clear Retained Publish QoS 0",
      ?setup(fun retain_qos0_clear/1)}
    ,{"Retained Publish QoS 1 -> QoS 0",
      ?setup(fun retain_qos1_qos0/1)}
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Setup Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
setup() ->
    application:load(vmq_server),
    application:set_env(vmq_server, allow_anonymous, true),
    application:set_env(vmq_server, listeners,
                        {[?listener(1888)],[],[]}),
    application:set_env(vmq_server, retry_interval, 10),
    vmq_server:start_no_auth(),
    wait_til_ready().
teardown(_) ->
    vmq_msg_store:clean_all([]),
    vmq_reg:reset_all_tables([]),
    vmq_server:stop().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
retain_qos0(_) ->
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
    ?_assertEqual(ok, gen_tcp:close(Socket)).

retain_qos0_repeated(_) ->
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
    ?_assertEqual(ok, gen_tcp:close(Socket)).

retain_qos0_fresh(_) ->
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
    ?_assertEqual(ok, gen_tcp:close(Socket)).

retain_qos0_clear(_) ->
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
    ?_assertEqual(ok, gen_tcp:close(Socket)).

retain_qos1_qos0(_) ->
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
    ?_assertEqual(ok, gen_tcp:close(Socket)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks (as explicit as possible)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
hook_auth_on_subscribe(_,"retain-qos0-test", [{"retain/qos0/test",0}]) -> ok;
hook_auth_on_subscribe(_,"retain-qos0-rep-test", [{"retain/qos0/test",0}]) -> ok;
hook_auth_on_subscribe(_,"retain-qos0-fresh-test", [{"retain/qos0/test",0}]) -> ok;
hook_auth_on_subscribe(_,"retain-clear-test", [{"retain/clear/test",0}]) -> ok;
hook_auth_on_subscribe(_,"retain-qos1-test", [{"retain/qos1/test",0}]) -> ok.

hook_auth_on_publish(_, _, _MsgId, "retain/qos0/test", <<"retained message">>, true) -> ok;
%% retain_qos0_clear(_) Both cases must be covered retain and clear-retain
hook_auth_on_publish(_, _, _MsgId, "retain/clear/test", <<"retained message">>, true) -> ok;
hook_auth_on_publish(_, _, _MsgId, "retain/clear/test", <<>>, true) -> ok;
%
hook_auth_on_publish(_, _, _MsgId, "retain/qos1/test", <<"retained message">>, true) -> ok.
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
    vmq_hook:add(auth_on_subscribe, {?MODULE, hook_auth_on_subscribe, 3}).
enable_on_publish() ->
    vmq_hook:add(auth_on_publish, {?MODULE, hook_auth_on_publish, 6}).
disable_on_subscribe() ->
    vmq_hook:delete(auth_on_subscribe, only, 1, {?MODULE, hook_auth_on_subscribe, 3}).
disable_on_publish() ->
    vmq_hook:delete(auth_on_publish, only, 1, {?MODULE, hook_auth_on_publish, 6}).
