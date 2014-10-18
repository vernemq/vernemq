-module(vmq_publish_tests).
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
     {"Publish QoS 1 Success",
      ?setup(fun publish_qos1/1)}
    ,{"Publish QoS 2 Success",
      ?setup(fun publish_qos2/1)}
    ,{"Publish B2C disconnect QoS 1 Success",
      ?setup(fun publish_b2c_disconnect_qos1/1)}
    ,{"Publish B2C disconnect QoS 2 Success",
      ?setup(fun publish_b2c_disconnect_qos2/1)}
    ,{"Publish B2C timeout QoS 1 Success",
      ?setup(fun publish_b2c_timeout_qos1/1)}
    ,{"Publish B2C timeout QoS 2 Success",
      ?setup(fun publish_b2c_timeout_qos2/1)}
    ,{"Publish C2B disconnect QoS 2 Success",
      ?setup(fun publish_c2b_disconnect_qos2/1)}
    ,{"Publish C2B timeout QoS 2 Success",
      ?setup(fun publish_c2b_timeout_qos2/1)}
    ,{"Pattern Matching PubSub Test",
      ?setup(fun pattern_matching/1)}
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
    wait_til_ready().
teardown(_) ->
    vmq_msg_store:clean_all([]),
    vmq_reg:reset_all_tables([]),
    vmq_server:stop().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
publish_qos1(_) ->
    Connect = packet:gen_connect("pub-qos1-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Publish = packet:gen_publish("pub/qos1/test", 1, <<"message">>, [{mid, 19}]),
    Puback = packet:gen_puback(19),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    enable_on_publish(),
    ok = gen_tcp:send(Socket, Publish),
    ok = packet:expect_packet(Socket, "puback", Puback),
    disable_on_publish(),
    ?_assertEqual(ok, gen_tcp:close(Socket)).

publish_qos2(_) ->
    Connect = packet:gen_connect("pub-qos2-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Publish = packet:gen_publish("pub/qos2/test", 2, <<"message">>, [{mid, 312}]),
    Pubrec = packet:gen_pubrec(312),
    Pubrel = packet:gen_pubrel(312),
    Pubcomp = packet:gen_pubcomp(312),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    enable_on_publish(),
    ok = gen_tcp:send(Socket, Publish),
    ok = packet:expect_packet(Socket, "pubrec", Pubrec),
    ok = gen_tcp:send(Socket, Pubrel),
    ok = packet:expect_packet(Socket, "pubcomp", Pubcomp),
    disable_on_publish(),
    ?_assertEqual(ok, gen_tcp:close(Socket)).

publish_b2c_disconnect_qos1(_) ->
    Connect = packet:gen_connect("pub-qos1-disco-test", [{keepalive,60}, {clean_session, false}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(3265, "qos1/disconnect/test", 1),
    Suback = packet:gen_suback(3265, 1),
    Publish = packet:gen_publish("qos1/disconnect/test", 1, <<"disconnect-message">>, [{mid, 1}]),
    PublishDup = packet:gen_publish("qos1/disconnect/test", 1, <<"disconnect-message">>, [{mid, 1}, {dup, true}]),
    Puback = packet:gen_puback(1),
    Publish2 = packet:gen_publish("qos1/outgoing", 1, <<"outgoing-message">>, [{mid, 3266}]),
    enable_on_publish(),
    enable_on_subscribe(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    helper_pub_qos1("test-helper", 1, Publish),
    %% should have now received a publish command
    ok = packet:expect_packet(Socket, "publish", Publish), %% expect packet, but we don't ack
    %% send our outgoing message, when we disconnect the broker
    %% should get rid of it and ssume we're going to retry
    ok = gen_tcp:send(Socket, Publish2),
    ok = gen_tcp:close(Socket),
    %% TODO: assert Publish2 deleted on broker
    %% reconnect
    {ok, Socket1} = packet:do_client_connect(Connect, Connack, []),
    ok = packet:expect_packet(Socket1, "dup publish", PublishDup),
    ok = gen_tcp:send(Socket1, Puback),
    disable_on_publish(),
    disable_on_subscribe(),
    ?_assertEqual(ok, gen_tcp:close(Socket1)).

publish_b2c_disconnect_qos2(_) ->
    Connect = packet:gen_connect("pub-qos2-disco-test", [{keepalive,60}, {clean_session, false}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(3265, "qos2/disconnect/test", 2),
    Suback = packet:gen_suback(3265, 2),
    Publish = packet:gen_publish("qos2/disconnect/test", 2, <<"disconnect-message">>, [{mid, 1}]),
    PublishDup = packet:gen_publish("qos2/disconnect/test", 2, <<"disconnect-message">>, [{mid, 1}, {dup, true}]),
    Pubrel = packet:gen_pubrel(1),
    Pubrec = packet:gen_pubrec(1),
    PubrelDup = packet:gen_pubrel(1, true),
    Pubcomp = packet:gen_pubcomp(1),
    Publish2 = packet:gen_publish("qos1/outgoing", 1, <<"outgoing-message">>, [{mid, 3266}]),
    enable_on_publish(),
    enable_on_subscribe(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    helper_pub_qos2("test-helper", 1, Publish),
    %% should have now received a publish command
    ok = packet:expect_packet(Socket, "publish", Publish), %% expect packet, but we don't ack
    %% send our outgoing message, when we disconnect the broker
    %% should get rid of it and ssume we're going to retry
    ok = gen_tcp:send(Socket, Publish2),
    ok = gen_tcp:close(Socket),
    %% TODO: assert Publish2 deleted on broker
    %% reconnect
    {ok, Socket1} = packet:do_client_connect(Connect, Connack, []),
    ok = packet:expect_packet(Socket1, "dup publish", PublishDup),
    ok = gen_tcp:send(Socket1, Pubrec),
    ok = packet:expect_packet(Socket1, "pubrel", Pubrel),
    ok = gen_tcp:close(Socket1),
    %% Expect PubrelDup
    {ok, Socket2} = packet:do_client_connect(Connect, Connack, []),
    ok = packet:expect_packet(Socket2, "dup pubrel", PubrelDup),
    ok = gen_tcp:send(Socket2, Pubcomp),
    disable_on_publish(),
    disable_on_subscribe(),
    ?_assertEqual(ok, gen_tcp:close(Socket2)).

publish_b2c_timeout_qos1(_) ->
    Connect = packet:gen_connect("pub-qos1-timeout-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(3265, "qos1/timeout/test", 1),
    Suback = packet:gen_suback(3265, 1),
    Publish = packet:gen_publish("qos1/timeout/test", 1, <<"timeout-message">>, [{mid, 1}]),
    PublishDup = packet:gen_publish("qos1/timeout/test", 1, <<"timeout-message">>, [{mid, 1}, {dup, true}]),
    Puback = packet:gen_puback(1),
    enable_on_publish(),
    enable_on_subscribe(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    helper_pub_qos1("test-helper", 1, Publish),
    %% should have now received a publish command
    ok = packet:expect_packet(Socket, "publish", Publish), %% expect packet, but we don't ack
    %% The broker should repeat the PUBLISH with dup set
    ok = packet:expect_packet(Socket, "dup publish", PublishDup),
    ok = gen_tcp:send(Socket, Puback),
    disable_on_publish(),
    disable_on_subscribe(),
    ?_assertEqual(ok, gen_tcp:close(Socket)).

publish_b2c_timeout_qos2(_) ->
    Connect = packet:gen_connect("pub-qos2-timeout-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(3265, "qos2/timeout/test", 2),
    Suback = packet:gen_suback(3265, 2),
    Publish = packet:gen_publish("qos2/timeout/test", 2, <<"timeout-message">>, [{mid, 1}]),
    PublishDup = packet:gen_publish("qos2/timeout/test", 2, <<"timeout-message">>, [{mid, 1}, {dup, true}]),
    Pubrec = packet:gen_pubrec(1),
    Pubrel = packet:gen_pubrel(1),
    PubrelDup = packet:gen_pubrel(1, true),
    Pubcomp = packet:gen_pubcomp(1),
    enable_on_publish(),
    enable_on_subscribe(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    helper_pub_qos2("test-helper", 1, Publish),
    %% should have now received a publish command
    ok = packet:expect_packet(Socket, "publish", Publish), %% expect packet, but we don't ack
    %% The broker should repeat the PUBLISH with dup set
    ok = packet:expect_packet(Socket, "dup publish", PublishDup),
    ok = gen_tcp:send(Socket, Pubrec),
    ok = packet:expect_packet(Socket, "pubrel", Pubrel),
    %% The broker should repeat the PUBREL with dup set
    ok = packet:expect_packet(Socket, "pubrel", PubrelDup),
    ok = gen_tcp:send(Socket, Pubcomp),
    disable_on_publish(),
    disable_on_subscribe(),
    ?_assertEqual(ok, gen_tcp:close(Socket)).

publish_c2b_disconnect_qos2(_) ->
    Connect = packet:gen_connect("pub-qos2-disco-test", [{keepalive,60}, {clean_session, false}]),
    Connack = packet:gen_connack(0),
    Publish = packet:gen_publish("qos2/disconnect/test", 2, <<"disconnect-message">>, [{mid, 1}]),
    PublishDup = packet:gen_publish("qos2/disconnect/test", 2, <<"disconnect-message">>, [{mid, 1}, {dup, true}]),
    Pubrec = packet:gen_pubrec(1),
    Pubrel = packet:gen_pubrel(1),
    PubrelDup = packet:gen_pubrel(1, true),
    Pubcomp = packet:gen_pubcomp(1),
    enable_on_publish(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Publish),
    ok = packet:expect_packet(Socket, "pubrec", Pubrec),
    %% We're now going to disconnect and pretend we didn't receive the pubrec
    ok = gen_tcp:close(Socket),
    {ok, Socket1} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket1, PublishDup),
    ok = packet:expect_packet(Socket1, "pubrec", Pubrec),
    ok = gen_tcp:send(Socket1, Pubrel),
    ok = packet:expect_packet(Socket1, "pubcomp", Pubcomp),
    %% Again, pretend we didn't receive this pubcomp
    ok = gen_tcp:close(Socket1),
    {ok, Socket2} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket2, PubrelDup),
    ok = packet:expect_packet(Socket2, "pubcomp", Pubcomp),
    disable_on_publish(),
    ?_assertEqual(ok, gen_tcp:close(Socket2)).

publish_c2b_timeout_qos2(_) ->
    Connect = packet:gen_connect("pub-qos2-timeout-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Publish = packet:gen_publish("pub/qos2/test", 2, <<"timeout-message">>, [{mid, 1926}]),
    Pubrec = packet:gen_pubrec(1926),
    Pubrel = packet:gen_pubrel(1926),
    Pubcomp = packet:gen_pubcomp(1926),
    enable_on_publish(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Publish),
    ok = packet:expect_packet(Socket, "pubrec", Pubrec),
    %% The broker should repeat the PUBREC
    ok = packet:expect_packet(Socket, "pubrec", Pubrec),
    ok = gen_tcp:send(Socket, Pubrel),
    ok = packet:expect_packet(Socket, "pubcomp", Pubcomp),
    disable_on_publish(),
    ?_assertEqual(ok, gen_tcp:close(Socket)).

pattern_matching(_) ->
    [?_assertEqual(ok, pattern_test("#", "test/topic"))
     ,?_assertEqual(ok, pattern_test("#", "/test/topic"))
     ,?_assertEqual(ok, pattern_test("foo/#", "foo/bar/baz"))
     ,?_assertEqual(ok, pattern_test("foo/+/baz", "foo/bar/baz"))
     ,?_assertEqual(ok, pattern_test("foo/+/baz/#", "foo/bar/baz"))
     ,?_assertEqual(ok, pattern_test("foo/+/baz/#", "foo/bar/baz/bar"))
     ,?_assertEqual(ok, pattern_test("foo/foo/baz/#", "foo/foo/baz/bar"))
     ,?_assertEqual(ok, pattern_test("foo/#", "foo"))
     ,?_assertEqual(ok, pattern_test("/#", "/foo"))
     ,?_assertEqual(ok, pattern_test("test/topic/", "test/topic/"))
     ,?_assertEqual(ok, pattern_test("test/topic/+", "test/topic/"))
     ,?_assertEqual(ok, pattern_test("+/+/+/+/+/+/+/+/+/+/test", "one/two/three/four/five/six/seven/eight/nine/ten/test"))
     ,?_assertEqual(ok, pattern_test("#", "test////a//topic"))
     ,?_assertEqual(ok, pattern_test("#", "/test////a//topic"))
     ,?_assertEqual(ok, pattern_test("foo/#", "foo//bar///baz"))
     ,?_assertEqual(ok, pattern_test("foo/+/baz", "foo//baz"))
     ,?_assertEqual(ok, pattern_test("foo/+/baz//", "foo//baz//"))
     ,?_assertEqual(ok, pattern_test("foo/+/baz/#", "foo//baz"))
     ,?_assertEqual(ok, pattern_test("foo/+/baz/#", "foo//baz/bar"))
     ,?_assertEqual(ok, pattern_test("foo//baz/#", "foo//baz/bar"))
     ,?_assertEqual(ok, pattern_test("foo/foo/baz/#", "foo/foo/baz/bar"))
     ,?_assertEqual(ok, pattern_test("/#", "////foo///bar"))
     ].

pattern_test(SubTopic, PubTopic) ->
    Connect = packet:gen_connect("pattern-sub-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Publish = packet:gen_publish(PubTopic, 0, <<"message">>, []),
    PublishRetained = packet:gen_publish(PubTopic, 0, <<"message">>, [{retain, true}]),
    Subscribe = packet:gen_subscribe(312, SubTopic, 0),
    Suback = packet:gen_suback(312, 0),
    Unsubscribe = packet:gen_unsubscribe(234, SubTopic),
    Unsuback = packet:gen_unsuback(234),
    enable_on_publish(),
    enable_on_subscribe(),
    vmq_msg_store:clean_all([]),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    helper_pattern_matching(PubTopic),
    ok = packet:expect_packet(Socket, "publish", Publish),
    ok = gen_tcp:send(Socket, Unsubscribe),
    ok = packet:expect_packet(Socket, "unsuback", Unsuback),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = packet:expect_packet(Socket, "publish retained", PublishRetained),
    disable_on_publish(),
    disable_on_subscribe(),
    ok = gen_tcp:close(Socket).




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks (as explicit as possible)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
hook_auth_on_subscribe(_,"pub-qos1-disco-test", [{"qos1/disconnect/test", 1}]) -> ok;
hook_auth_on_subscribe(_,"pub-qos2-disco-test", [{"qos2/disconnect/test", 2}]) -> ok;
hook_auth_on_subscribe(_,"pub-qos1-timeout-test", [{"qos1/timeout/test", 1}]) -> ok;
hook_auth_on_subscribe(_,"pub-qos2-timeout-test", [{"qos2/timeout/test", 2}]) -> ok;
hook_auth_on_subscribe(_,"pattern-sub-test", [{_,0}]) -> ok.

hook_auth_on_publish(_, "pub-qos1-test", _MsgId, "pub/qos1/test", <<"message">>, false) -> ok;
hook_auth_on_publish(_, "pub-qos2-test", _MsgId, "pub/qos2/test", <<"message">>, false) -> ok;
hook_auth_on_publish(_, "pub-qos1-disco-test", _MsgId, "qos1/outgoing", <<"outgoing-message">>, false) -> ok;
hook_auth_on_publish(_, "test-helper", _MsgId, "qos1/disconnect/test", <<"disconnect-message">>, false) -> ok;
hook_auth_on_publish(_, "pub-qos2-disco-test", _MsgId, "qos1/outgoing", <<"outgoing-message">>, false) -> ok;
hook_auth_on_publish(_, "test-helper", _MsgId, "qos2/disconnect/test", <<"disconnect-message">>, false) -> ok;
hook_auth_on_publish(_, "test-helper", _MsgId, "qos1/timeout/test", <<"timeout-message">>, false) -> ok;
hook_auth_on_publish(_, "test-helper", _MsgId, "qos2/timeout/test", <<"timeout-message">>, false) -> ok;
hook_auth_on_publish(_, "pub-qos2-disco-test", _MsgId, "qos2/disconnect/test", <<"disconnect-message">>, false) -> ok;
hook_auth_on_publish(_, "pub-qos2-timeout-test", _MsgId, "pub/qos2/test", <<"timeout-message">>, false) -> ok;
hook_auth_on_publish(_, "test-helper", _MsgId, _, <<"message">>, false) -> ok;
hook_auth_on_publish(_, "test-helper", _MsgId, _, <<"message">>, true) -> ok.
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

helper_pub_qos1(ClientId, Mid, Publish) ->
    Connect = packet:gen_connect(ClientId, [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Puback = packet:gen_puback(Mid),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Publish),
    ok = packet:expect_packet(Socket, "puback", Puback),
    gen_tcp:close(Socket).

helper_pub_qos2(ClientId, Mid, Publish) ->
    Connect = packet:gen_connect(ClientId, [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Pubrec = packet:gen_pubrec(Mid),
    Pubrel = packet:gen_pubrel(Mid),
    Pubcomp = packet:gen_pubcomp(Mid),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Publish),
    ok = packet:expect_packet(Socket, "pubrec", Pubrec),
    ok = gen_tcp:send(Socket, Pubrel),
    ok = packet:expect_packet(Socket, "pubcomp", Pubcomp),
    gen_tcp:close(Socket).

helper_pattern_matching(PubTopic) ->
    Connect = packet:gen_connect("test-helper", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Publish = packet:gen_publish(PubTopic, 0, <<"message">>, [{retain, true}]),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket, Publish),
    gen_tcp:close(Socket).
