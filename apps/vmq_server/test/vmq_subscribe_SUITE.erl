-module(vmq_subscribe_SUITE).
-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([subscribe_qos0_test/1,
         subscribe_qos1_test/1,
         subscribe_qos2_test/1,
         suback_with_nack_test/1,
         subnack_test/1,
         unsubscribe_qos0_test/1,
         unsubscribe_qos1_test/1,
         unsubscribe_qos2_test/1,
         subpub_qos0_test/1,
         subpub_qos1_test/1,
         subpub_qos2_test/1,
         resubscribe_test/1]).

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
    enable_on_subscribe(),
    vmq_server_cmd:set_config(allow_anonymous, true),
    vmq_server_cmd:set_config(retry_interval, 10),
    vmq_server_cmd:listener_start(1888, []),
    Config.

end_per_testcase(_, Config) ->
    disable_on_subscribe(),
    vmq_test_utils:teardown(),
    Config.

all() ->
    [subscribe_qos0_test,
     subscribe_qos1_test,
     subscribe_qos2_test,
     suback_with_nack_test,
     subnack_test,
     unsubscribe_qos0_test,
     unsubscribe_qos1_test,
     unsubscribe_qos2_test,
     subpub_qos0_test,
     subpub_qos1_test,
     subpub_qos2_test,
     resubscribe_test].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

subscribe_qos0_test(_) ->
    Connect = packet:gen_connect("subscribe-qos0-test", [{keepalive,10}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(53, "qos0/test", 0),
    Suback = packet:gen_suback(53, 0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = gen_tcp:close(Socket).

subscribe_qos1_test(_) ->
    Connect = packet:gen_connect("subscribe-qos1-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(79, "qos1/test", 1),
    Suback = packet:gen_suback(79, 1),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = gen_tcp:close(Socket).

subscribe_qos2_test(_) ->
    Connect = packet:gen_connect("subscribe-qos2-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(3, "qos2/test", 2),
    Suback = packet:gen_suback(3, 2),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = gen_tcp:close(Socket).

suback_with_nack_test(_) ->
    Connect = packet:gen_connect("subscribe-multi1-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(3, [{"qos0/test", 0},
                                         {"qos1/test", 1},
                                         {"qos2/test", 2}]),
    SubackWithNack = packet:gen_suback(3, [0, not_allowed, 2]),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "subackWithNack", SubackWithNack),
    ok = gen_tcp:close(Socket).

subnack_test(_) ->
    Connect = packet:gen_connect("subscribe-multi2-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(3, [{"qos0/test", 0},
                                         {"qos1/test", 1},
                                         {"qos2/test", 2}]),
    SubNack = packet:gen_suback(3, [not_allowed, not_allowed, not_allowed]),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "subNack", SubNack),
    ok = gen_tcp:close(Socket).


unsubscribe_qos0_test(_) ->
    Connect = packet:gen_connect("unsubscribe-qos0-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Unsubscribe = packet:gen_unsubscribe(53, "qos0/test"),
    Unsuback = packet:gen_unsuback(53),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Unsubscribe),
    ok = packet:expect_packet(Socket, "unsuback", Unsuback),
    ok = gen_tcp:close(Socket).

unsubscribe_qos1_test(_) ->
    Connect = packet:gen_connect("unsubscribe-qos1-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Unsubscribe = packet:gen_unsubscribe(79, "qos1/test"),
    Unsuback = packet:gen_unsuback(79),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Unsubscribe),
    ok = packet:expect_packet(Socket, "unsuback", Unsuback),
    ok = gen_tcp:close(Socket).

unsubscribe_qos2_test(_) ->
    Connect = packet:gen_connect("unsubscribe-qos2-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Unsubscribe = packet:gen_unsubscribe(3, "qos2/test"),
    Unsuback = packet:gen_unsuback(3),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Unsubscribe),
    ok = packet:expect_packet(Socket, "unsuback", Unsuback),
    ok = gen_tcp:close(Socket).

subpub_qos0_test(_) ->
    Connect = packet:gen_connect("subpub-qos0-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(53, "subpub/qos0", 0),
    Suback = packet:gen_suback(53, 0),
    Publish = packet:gen_publish("subpub/qos0", 0, <<"message">>, []),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    enable_on_publish(),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = gen_tcp:send(Socket, Publish),
    ok = packet:expect_packet(Socket, "publish", Publish),
    disable_on_publish(),
    ok = gen_tcp:close(Socket).

subpub_qos1_test(_) ->
    Connect = packet:gen_connect("subpub-qos1-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(530, "subpub/qos1", 1),
    Suback = packet:gen_suback(530, 1),
    Publish1 = packet:gen_publish("subpub/qos1", 1, <<"message">>, [{mid, 300}]),
    Puback1 = packet:gen_puback(300),
    Publish2 = packet:gen_publish("subpub/qos1", 1, <<"message">>, [{mid, 1}]),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    enable_on_publish(),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = gen_tcp:send(Socket, Publish1),
    ok = packet:expect_packet(Socket, "puback", Puback1),
    ok = packet:expect_packet(Socket, "publish", Publish2),
    disable_on_publish(),
    ok = gen_tcp:close(Socket).

subpub_qos2_test(_) ->
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
    disable_on_publish(),
    ok = gen_tcp:close(Socket).

resubscribe_test(_) ->
    %% test that we can override a subscription
    Connect = packet:gen_connect("sub-override-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(530, "sub/override/qos", 0),
    Suback = packet:gen_suback(530, 0),
    ReSubscribe = packet:gen_subscribe(531, "sub/override/qos", 1),
    ReSuback = packet:gen_suback(531, 1),
    Publish1 = packet:gen_publish("sub/override/qos", 1, <<"message">>, [{mid, 1}]),
    Puback = packet:gen_puback(1),
    Publish0 = packet:gen_publish("sub/override/qos", 0, <<"message">>, []),


    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    enable_on_publish(),
    %% max qos is 0, receive qos0 publish
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = gen_tcp:send(Socket, Publish1),
    ok = packet:expect_packet(Socket, "puback", Puback),
    ok = packet:expect_packet(Socket, "publish0", Publish0),

    %% max qos is 1, receive qos1 publish
    ok = gen_tcp:send(Socket, ReSubscribe),
    ok = packet:expect_packet(Socket, "suback", ReSuback),
    ok = gen_tcp:send(Socket, Publish1),
    ok = packet:expect_packet(Socket, "puback", Puback),
    ok = packet:expect_packet(Socket, "publish1", Publish1),
    ok = gen_tcp:send(Socket, Puback),

    disable_on_publish(),
    ok = gen_tcp:close(Socket).







%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
hook_auth_on_subscribe(_,{"", <<"subscribe-qos0-test">>}, [{[<<"qos0">>,<<"test">>], 0}]) -> ok;
hook_auth_on_subscribe(_,{"", <<"subscribe-qos1-test">>}, [{[<<"qos1">>,<<"test">>], 1}]) -> ok;
hook_auth_on_subscribe(_,{"", <<"subscribe-qos2-test">>}, [{[<<"qos2">>,<<"test">>], 2}]) -> ok;
hook_auth_on_subscribe(_,{"", <<"subscribe-multi1-test">>},
                       [{[<<"qos0">>,<<"test">>], 0},
                        {[<<"qos1">>,<<"test">>], 1},
                        {[<<"qos2">>,<<"test">>], 2}]) ->
    {ok, [{[<<"qos0">>,<<"test">>], 0},
          {[<<"qos1">>,<<"test">>], not_allowed},
          {[<<"qos2">>,<<"test">>], 2}]};
hook_auth_on_subscribe(_,{"", <<"subscribe-multi2-test">>},
                       [{[<<"qos0">>,<<"test">>], 0},
                        {[<<"qos1">>,<<"test">>], 1},
                        {[<<"qos2">>,<<"test">>], 2}]) ->
    {error, not_allowed};
hook_auth_on_subscribe(_,{"", <<"subpub-qos0-test">>}, [{[<<"subpub">>,<<"qos0">>], 0}]) -> ok;
hook_auth_on_subscribe(_,{"", <<"subpub-qos1-test">>}, [{[<<"subpub">>,<<"qos1">>], 1}]) -> ok;
hook_auth_on_subscribe(_,{"", <<"subpub-qos2-test">>}, [{[<<"subpub">>,<<"qos2">>], 2}]) -> ok;
hook_auth_on_subscribe(_,{"", <<"sub-override-test">>},
                       [{[<<"sub">>,<<"override">>,<<"qos">>], _}]) -> ok.

hook_auth_on_publish(_, {"", <<"subpub-qos0-test">>}, _MsgId, [<<"subpub">>,<<"qos0">>], <<"message">>, false) -> ok;
hook_auth_on_publish(_, {"", <<"subpub-qos1-test">>}, _MsgId, [<<"subpub">>,<<"qos1">>], <<"message">>, false) -> ok;
hook_auth_on_publish(_, {"", <<"subpub-qos2-test">>}, _MsgId, [<<"subpub">>,<<"qos2">>], <<"message">>, false) -> ok;
hook_auth_on_publish(_, {"", <<"sub-override-test">>}, _MsgId, [<<"sub">>,<<"override">>,<<"qos">>], <<"message">>, false) -> ok.

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
