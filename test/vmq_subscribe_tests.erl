-module(vmq_subscribe_tests).
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
    [{"Subscribe QoS 0 Success",
      ?setup(fun subscribe_qos0/1)}
    ,{"Subscribe QoS 1 Success",
      ?setup(fun subscribe_qos1/1)}
    ,{"Subscribe QoS 2 Success",
      ?setup(fun subscribe_qos2/1)}
    ,{"Unsubscribe QoS 0 Success",
      ?setup(fun unsubscribe_qos0/1)}
    ,{"Unsubscribe QoS 1 Success",
      ?setup(fun unsubscribe_qos1/1)}
    ,{"Unsubscribe QoS 2 Success",
      ?setup(fun unsubscribe_qos2/1)}
    ,{"Subscribe and Publish QoS 0 Success",
      ?setup(fun subpub_qos0/1)}
    ,{"Subscribe and Publish QoS 1 Success",
      ?setup(fun subpub_qos1/1)}
    ,{"Subscribe and Publish QoS 2 Success",
      ?setup(fun subpub_qos2/1)}
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Setup Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
setup() ->
    application:load(vmq_server),
    application:set_env(vmq_server, allow_anonymous, true),
    application:set_env(vmq_server, listeners,
                        {[?listener(1888)],[],[],[]}),
    vmq_server:start_no_auth(),
    wait_til_ready().
teardown(_) -> vmq_server:stop().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

subscribe_qos0(_) ->
    Connect = packet:gen_connect("subscribe-qos0-test", [{keepalive,10}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(53, "qos0/test", 0),
    Suback = packet:gen_suback(53, 0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    enable_on_subscribe(),
    ok = gen_tcp:send(Socket, Subscribe),
    disable_on_subscribe(),
    [?_assertEqual(ok, packet:expect_packet(Socket, "suback", Suback))
    ,?_assertEqual(ok, gen_tcp:close(Socket))].

subscribe_qos1(_) ->
    Connect = packet:gen_connect("subscribe-qos1-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(79, "qos1/test", 1),
    Suback = packet:gen_suback(79, 1),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    enable_on_subscribe(),
    ok = gen_tcp:send(Socket, Subscribe),
    disable_on_subscribe(),
    [?_assertEqual(ok, packet:expect_packet(Socket, "suback", Suback))
    ,?_assertEqual(ok, gen_tcp:close(Socket))].

subscribe_qos2(_) ->
    Connect = packet:gen_connect("subscribe-qos2-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(3, "qos2/test", 2),
    Suback = packet:gen_suback(3, 2),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    enable_on_subscribe(),
    ok = gen_tcp:send(Socket, Subscribe),
    disable_on_subscribe(),
    [?_assertEqual(ok, packet:expect_packet(Socket, "suback", Suback))
    ,?_assertEqual(ok, gen_tcp:close(Socket))].

unsubscribe_qos0(_) ->
    Connect = packet:gen_connect("unsubscribe-qos0-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Unsubscribe = packet:gen_unsubscribe(53, "qos0/test"),
    Unsuback = packet:gen_unsuback(53),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Unsubscribe),
    [?_assertEqual(ok, packet:expect_packet(Socket, "unsuback", Unsuback))
    ,?_assertEqual(ok, gen_tcp:close(Socket))].

unsubscribe_qos1(_) ->
    Connect = packet:gen_connect("unsubscribe-qos1-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Unsubscribe = packet:gen_unsubscribe(79, "qos1/test"),
    Unsuback = packet:gen_unsuback(79),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Unsubscribe),
    [?_assertEqual(ok, packet:expect_packet(Socket, "unsuback", Unsuback))
    ,?_assertEqual(ok, gen_tcp:close(Socket))].

unsubscribe_qos2(_) ->
    Connect = packet:gen_connect("unsubscribe-qos2-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Unsubscribe = packet:gen_unsubscribe(3, "qos2/test"),
    Unsuback = packet:gen_unsuback(3),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Unsubscribe),
    [?_assertEqual(ok, packet:expect_packet(Socket, "unsuback", Unsuback))
    ,?_assertEqual(ok, gen_tcp:close(Socket))].

subpub_qos0(_) ->
    Connect = packet:gen_connect("subpub-qos0-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(53, "subpub/qos0", 0),
    Suback = packet:gen_suback(53, 0),
    Publish = packet:gen_publish("subpub/qos0", 0, <<"message">>, []),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    enable_on_subscribe(),
    enable_on_publish(),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = gen_tcp:send(Socket, Publish),
    ok = packet:expect_packet(Socket, "publish", Publish),
    disable_on_subscribe(),
    disable_on_publish(),
    ?_assertEqual(ok, gen_tcp:close(Socket)).

subpub_qos1(_) ->
    Connect = packet:gen_connect("subpub-qos1-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(530, "subpub/qos1", 1),
    Suback = packet:gen_suback(530, 1),
    Publish1 = packet:gen_publish("subpub/qos1", 1, <<"message">>, [{mid, 300}]),
    Puback1 = packet:gen_puback(300),
    Publish2 = packet:gen_publish("subpub/qos1", 1, <<"message">>, [{mid, 1}]),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    enable_on_subscribe(),
    enable_on_publish(),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = gen_tcp:send(Socket, Publish1),
    ok = packet:expect_packet(Socket, "puback", Puback1),
    ok = packet:expect_packet(Socket, "publish", Publish2),
    disable_on_subscribe(),
    disable_on_publish(),
    ?_assertEqual(ok, gen_tcp:close(Socket)).

subpub_qos2(_) ->
    Connect = packet:gen_connect("subpub-qos2-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(530, "subpub/qos2", 2),
    Suback = packet:gen_suback(530, 2),
    Publish1 = packet:gen_publish("subpub/qos2", 2, <<"message">>, [{mid, 301}]),
    Pubrec1 = packet:gen_pubrec(301),
    Pubrel1 = packet:gen_pubrel(301),
    Pubcomp1 = packet:gen_pubcomp(301),
    Publish2 = packet:gen_publish("subpub/qos2", 2, <<"message">>, [{mid, 1}]),
    Pubrec2 = packet:gen_pubrec(1),
    Pubrel2 = packet:gen_pubrel(1),

    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    enable_on_subscribe(),
    enable_on_publish(),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = gen_tcp:send(Socket, Publish1),
    ok = packet:expect_packet(Socket, "pubrec", Pubrec1),
    ok = gen_tcp:send(Socket, Pubrel1),
    ok = packet:expect_packet(Socket, "pubcomp", Pubcomp1),
    ok = packet:expect_packet(Socket, "publish2", Publish2),
    ok = gen_tcp:send(Socket, Pubrec2),
    ok = packet:expect_packet(Socket, "pubrel2", Pubrel2),
    disable_on_subscribe(),
    disable_on_publish(),
    ?_assertEqual(ok, gen_tcp:close(Socket)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
hook_auth_on_subscribe(_,"subscribe-qos0-test", [{"qos0/test", 0}]) -> ok;
hook_auth_on_subscribe(_,"subscribe-qos1-test", [{"qos1/test", 1}]) -> ok;
hook_auth_on_subscribe(_,"subscribe-qos2-test", [{"qos2/test", 2}]) -> ok;
hook_auth_on_subscribe(_,"subpub-qos0-test", [{"subpub/qos0", 0}]) -> ok;
hook_auth_on_subscribe(_,"subpub-qos1-test", [{"subpub/qos1", 1}]) -> ok;
hook_auth_on_subscribe(_,"subpub-qos2-test", [{"subpub/qos2", 2}]) -> ok.

hook_auth_on_publish(_, "subpub-qos0-test", _MsgId, "subpub/qos0", <<"message">>, false) -> ok;
hook_auth_on_publish(_, "subpub-qos1-test", _MsgId, "subpub/qos1", <<"message">>, false) -> ok;
hook_auth_on_publish(_, "subpub-qos2-test", _MsgId, "subpub/qos2", <<"message">>, false) -> ok.

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
