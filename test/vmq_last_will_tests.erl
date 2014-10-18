-module(vmq_last_will_tests).
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
     {"Check Last Will Denied",
      ?setup(fun will_denied/1)}
    ,{"Check Null Will Payload",
      ?setup(fun will_null/1)}
    ,{"Check Null Will Topic",
      ?setup(fun will_null_topic/1)}
    ,{"Check QoS0 Will Topic",
      ?setup(fun will_qos0/1)}
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Setup Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
setup() ->
    application:load(vmq_server),
    application:set_env(vmq_server, allow_anonymous, true),
    application:set_env(vmq_server, listeners,
                        {[?listener(1888)],[],[],[]}),
    application:set_env(vmq_server, retry_interval, 10),
    vmq_server:start_no_auth(),
    vmq_msg_store:clean_all([]),
    vmq_reg:reset_all_tables([]),
    wait_til_ready().
teardown(_) ->
    vmq_server:stop().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
will_denied(_) ->
    ConnectOK = packet:gen_connect("will-acl-test", [{keepalive,60}, {will_topic, "ok"}, {will_payload, <<"should be ok">>}]),
    ConnackOK = packet:gen_connack(0),
    Connect = packet:gen_connect("will-acl-test", [{keepalive,60}, {will_topic, "will/acl/test"}, {will_msg, <<"should be denied">>}]),
    Connack = packet:gen_connack(5),
    enable_on_publish(),
    {ok, SocketOK} = packet:do_client_connect(ConnectOK, ConnackOK, []),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    disable_on_publish(),
    ok = gen_tcp:close(Socket),
    ?_assertEqual(ok, gen_tcp:close(SocketOK)).

will_null(_) ->
    Connect = packet:gen_connect("will-qos0-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(53, "will/null/test", 0),
    Suback = packet:gen_suback(53, 0),
    Publish = packet:gen_publish("will/null/test", 0, <<>>, []),
    enable_on_subscribe(),
    enable_on_publish(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    will_null_helper(),
    ok = packet:expect_packet(Socket, "publish", Publish),
    disable_on_subscribe(),
    disable_on_publish(),
    ?_assertEqual(ok, gen_tcp:close(Socket)).

will_null_topic(_) ->
    Connect = packet:gen_connect("will-null-topic", [{keepalive,60}, {will_topic, ""}, {will_payload, <<"will message">>}]),
    Connack = packet:gen_connack(2),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ?_assertEqual(ok, gen_tcp:close(Socket)).

will_qos0(_) ->
    Connect = packet:gen_connect("will-qos0-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(53, "will/qos0/test", 0),
    Suback = packet:gen_suback(53, 0),
    Publish = packet:gen_publish("will/qos0/test", 0, <<"will-message">>, []),
    enable_on_subscribe(),
    enable_on_publish(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    will_qos0_helper(),
    ok = packet:expect_packet(Socket, "publish", Publish),
    disable_on_subscribe(),
    disable_on_publish(),
    ?_assertEqual(ok, gen_tcp:close(Socket)).




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks (as explicit as possible)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
hook_auth_on_subscribe(_,"will-qos0-test", [{"will/null/test",0}]) -> ok;
hook_auth_on_subscribe(_,"will-qos0-test", [{"will/qos0/test",0}]) -> ok.

hook_auth_on_publish(_, _, _MsgId, "ok", <<"should be ok">>, false) -> ok;
hook_auth_on_publish(_, _, _MsgId, "will/null/test", <<>>, false) -> ok;
hook_auth_on_publish(_, _, _MsgId, "will/qos0/test", <<"will-message">>, false) -> ok.
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

will_null_helper() ->
    Connect = packet:gen_connect("test-helper", [{keepalive,60}, {will_topic, "will/null/test"}]),
    Connack = packet:gen_connack(0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    gen_tcp:close(Socket).

will_qos0_helper() ->
    Connect = packet:gen_connect("test-helper", [{keepalive,60}, {will_topic, "will/qos0/test"}, {will_payload, <<"will-message">>}]),
    Connack = packet:gen_connack(0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    gen_tcp:close(Socket).

