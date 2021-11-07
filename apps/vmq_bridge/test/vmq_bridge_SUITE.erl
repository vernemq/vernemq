%%%-------------------------------------------------------------------
-module(vmq_bridge_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("vmq_commons/include/vmq_types.hrl").

init_per_suite(Config) ->
    cover:start(),
    [{ct_hooks, vmq_cth} | Config].

end_per_suite(_Config) ->
    ok.

init_per_group(mqttv4, Config) ->
    [{mqtt_version, 4} | Config];
init_per_group(mqttv3, Config) ->
    [{mqtt_version, 3} | Config];
init_per_group(try_private_3, Config) ->
    [{mqtt_version, 128+3} | Config];
init_per_group(try_private_4, Config) ->
    [{mqtt_version, 128+4} | Config].

end_per_group(_GroupName, _Config) ->
    ok.

% init_per_testcase(TestCase, Config) ->
%     case TestCase of
%      buffer_outgoing_test -> {skip, travis};
%     _ -> Config
%     end.

end_per_testcase(_TestCase, _Config) ->
    stop_bridge_plugin(),
    ok.

groups() ->
    Tests = [
            buffer_outgoing_test,
            prefixes_test
            ],
    ReconnectTests = [
                      bridge_reconnect_qos1_test,
                      bridge_reconnect_qos2_test,
                      remote_reconnect_qos1_test,
                      remote_reconnect_qos2_test
                     ],
    [
     % we only run the ReconnectTests for mqttv4 as they consume quite some time
     {mqttv4, [shuffle], Tests ++ ReconnectTests},
     {mqttv3, [shuffle], Tests},
     {try_private_3, [shuffle], Tests},
     {try_private_4, [shuffle], Tests}
    ].

all() ->
    [
     {group, mqttv4},
     {group, mqttv3},
     {group, try_private_3},
     {group, try_private_4}
    ].

prefixes_test(Cfg) ->
    %% topic in local-prefix remote-prefix:
    %% local-subs: none
    %% remote-subs: remote-prefix/topic
    %% rem-pub remote-prefix/topic -> local-pub local-prefix/topic
    start_bridge_plugin(#{
      mqtt_version => mqtt_version(Cfg),
      qos => 0,
      topics => [{"bridge-in", in, 0, "local-in-prefix", "remote-in-prefix"},
                 {"bridge-out", out, 0, "local-out-prefix", "remote-out-prefix"}]
     }),
    BridgePid = get_bridge_pid(),

    %% Start the 'broker' and let the bridge connect
    {ok, SSocket} = gen_tcp:listen(1890, [binary, {packet, raw}, {active, false}, {reuseaddr, true}]),
    {ok, BrokerSocket} = gen_tcp:accept(SSocket, 5000),
    Connect = packet:gen_connect("bridge-test", [{keepalive,60}, {clean_session, false},
                                                 {proto_ver, mqtt_version(Cfg)}]),
    Connack = packet:gen_connack(0),
    ok = gen_tcp:send(BrokerSocket, Connack),
    ok = packet:expect_packet(BrokerSocket, "connect", Connect),

    Subscribe = packet:gen_subscribe(1, "remote-in-prefix/bridge-in", 0),
    ok = packet:expect_packet(BrokerSocket, "subscribe", Subscribe),
    ok = bridge_rec({subscribe, BridgePid}, 100),

    %% out: publish to bridge and check it arrives at the broker
    ExpectPub0 = fun(Socket, Topic, Payload) ->
                         Pub = packet:gen_publish(Topic, 0, Payload, []),
                         ok = packet:expect_packet(Socket, "publish", Pub)
                 end,

    ok = pub_to_bridge(BridgePid, [<<"local-out-prefix">>, <<"bridge-out">>], "bridge-out-msg", 0),
    ok = ExpectPub0(BrokerSocket, "remote-out-prefix/bridge-out", "bridge-out-msg"),

    %% in: publish to broker and check it arrives at the bridge
    PublishBroker = packet:gen_publish("remote-in-prefix/bridge-in", 0, <<"bridge-in">>, []),
    ok = gen_tcp:send(BrokerSocket, PublishBroker),
    ok = bridge_rec({publish,[<<"local-in-prefix">>,<<"bridge-in">>],<<"bridge-in">>}, 100).

remote_reconnect_qos1_test(Cfg) ->
    start_bridge_plugin(#{
      mqtt_version => mqtt_version(Cfg),
      qos => 1,
      topics => [{"bridge/#", both, 1, "", ""}]
     }),

    Connect = packet:gen_connect("bridge-test", [{keepalive,60}, {clean_session, false},
                                                 {proto_ver, mqtt_version(Cfg)}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(1, "bridge/#", 1),
    Suback = packet:gen_suback(1, 1),
    Subscribe2 = packet:gen_subscribe(2, "bridge/#", 1),
    Suback2 = packet:gen_suback(2, 1),
    Publish = packet:gen_publish("bridge/disconnect/test", 1, <<"disconnect-message">>, [{mid, 3}]),
    Publish2 = packet:gen_publish("bridge/disconnect/test", 1, <<"disconnect-message">>, [{mid, 20}]),
    Puback2 = packet:gen_puback(20),
    {ok, SSocket} = gen_tcp:listen(1890, [binary, {packet, raw}, {active, false}, {reuseaddr, true}]),
    {ok, Bridge} = gen_tcp:accept(SSocket),
    ok = packet:expect_packet(Bridge, "connect", Connect),
    ok = gen_tcp:send(Bridge, Connack),
    _BridgeProc =
    ok = packet:expect_packet(Bridge, "subscribe", Subscribe),
    ok = gen_tcp:send(Bridge, Suback),
    ok = gen_tcp:send(Bridge, Publish),
    %% Bridge doesn't have time to respond but should expect us to retry
    %% and so remove PUBACK
    ok = gen_tcp:close(Bridge),
    {ok, Bridge2} = gen_tcp:accept(SSocket),
    ok = packet:expect_packet(Bridge2, "connect", Connect),
    ok = gen_tcp:send(Bridge2, Connack),
    ok = packet:expect_packet(Bridge2, "2nd subscribe", Subscribe2),
    ok = gen_tcp:send(Bridge2, Suback2),
    %% Send a different publish message to make sure the response isn't the old one
    ok = gen_tcp:send(Bridge2, Publish2),
    ok = packet:expect_packet(Bridge2, "puback", Puback2),
    ok = gen_tcp:close(Bridge2),
    ok = gen_tcp:close(SSocket).

remote_reconnect_qos2_test(Cfg) ->
    start_bridge_plugin(#{
      mqtt_version => mqtt_version(Cfg),
      qos => 2,
      topics => [{"bridge/#", both, 2, "", ""}]
     }),
    Connect = packet:gen_connect("bridge-test", [{keepalive,60}, {clean_session, false},
                                            {proto_ver, mqtt_version(Cfg)}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(1, "bridge/#", 2),
    Suback = packet:gen_suback(1, 2),
    Subscribe2 = packet:gen_subscribe(2, "bridge/#", 2),
    Suback2 = packet:gen_suback(2, 2),
    Subscribe3 = packet:gen_subscribe(3, "bridge/#", 2),
    Suback3 = packet:gen_suback(3, 2),
    Publish = packet:gen_publish("bridge/disconnect/test", 2, <<"disconnect-message">>, [{mid, 4}]),
    PublishDup = packet:gen_publish("bridge/disconnect/test", 2, <<"disconnect-message">>, [{mid, 4}, {dup, true}]),
    Pubrec = packet:gen_pubrec(4),
    Pubrel = packet:gen_pubrel(4),
    Pubcomp = packet:gen_pubcomp(4),
    {ok, SSocket} = gen_tcp:listen(1890, [binary, {packet, raw}, {active, false}, {reuseaddr, true}]),
    {ok, Bridge} = gen_tcp:accept(SSocket),
    ok = packet:expect_packet(Bridge, "connect", Connect),
    ok = gen_tcp:send(Bridge, Connack),
    ok = packet:expect_packet(Bridge, "subscribe", Subscribe),
    ok = gen_tcp:send(Bridge, Suback),
    ok = gen_tcp:send(Bridge, Publish),
    %% Bridge doesn't have time to respond but should expect us to retry
    %% and so remove PUBACK
    ok = gen_tcp:close(Bridge),
    {ok, Bridge2} = gen_tcp:accept(SSocket),
    ok = packet:expect_packet(Bridge2, "connect", Connect),
    ok = gen_tcp:send(Bridge2, Connack),
    ok = packet:expect_packet(Bridge2, "2nd subscribe", Subscribe2),
    ok = gen_tcp:send(Bridge2, Suback2),
    ok = gen_tcp:send(Bridge2, PublishDup),
    ok = packet:expect_packet(Bridge2, "pubrec", Pubrec),
    ok = gen_tcp:send(Bridge2, Pubrel),
    ok = gen_tcp:close(Bridge2),
    {ok, Bridge3} = gen_tcp:accept(SSocket),
    ok = packet:expect_packet(Bridge3, "connect", Connect),
    ok = gen_tcp:send(Bridge3, Connack),
    ok = packet:expect_packet(Bridge3, "3rd subscribe", Subscribe3),
    ok = gen_tcp:send(Bridge3, Suback3),
    ok = gen_tcp:send(Bridge3, PublishDup),
    ok = packet:expect_packet(Bridge3, "2nd pubrec", Pubrec),
    ok = gen_tcp:send(Bridge3, Pubrel),
    ok = packet:expect_packet(Bridge3, "pubcomp", Pubcomp),
    ok = gen_tcp:close(Bridge3),
    ok = gen_tcp:close(Bridge2),
    ok = gen_tcp:close(SSocket).

bridge_reconnect_qos1_test(Cfg) ->
    start_bridge_plugin(#{
      mqtt_version => mqtt_version(Cfg),
      qos => 1,
      topics => [{"bridge/#", both, 1, "", ""}]
     }),
    Connect = packet:gen_connect("bridge-test", [{keepalive,60}, {clean_session, false},
                                                 {proto_ver, mqtt_version(Cfg)}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(1, "bridge/#", 1),
    Suback = packet:gen_suback(1, 1),
    Publish = packet:gen_publish("bridge/disconnect/test", 1, <<"disconnect-message">>, [{mid, 2}]),
    PublishDup = packet:gen_publish("bridge/disconnect/test", 1, <<"disconnect-message">>, [{mid, 2}, {dup, true}]),
    Puback = packet:gen_puback(2),
    Subscribe2 = packet:gen_subscribe(3, "bridge/#", 1),
    Suback2 = packet:gen_suback(3, 1),
    {ok, SSocket} = gen_tcp:listen(1890, [binary, {packet, raw}, {active, false}, {reuseaddr, true}]),
    {ok, Bridge} = gen_tcp:accept(SSocket),
    ok = packet:expect_packet(Bridge, "connect", Connect),
    ok = gen_tcp:send(Bridge, Connack),
    BridgeProc =
    receive
        {subscribe, Pid} ->
            Pid
    end,
    ok = packet:expect_packet(Bridge, "subscribe", Subscribe),
    ok = gen_tcp:send(Bridge, Suback),
    disconnect_helper(BridgeProc),
    ok = packet:expect_packet(Bridge, "publish", Publish),
    ok = gen_tcp:close(Bridge),
    {ok, Bridge2} = gen_tcp:accept(SSocket),
    ok = packet:expect_packet(Bridge2, "2nd connect", Connect),
    ok = gen_tcp:send(Bridge2, Connack),
    ok = packet:expect_packet(Bridge2, "2nd publish", PublishDup),
    ok = packet:expect_packet(Bridge2, "2nd subscribe", Subscribe2),
    ok = gen_tcp:send(Bridge2, Suback2),
    % expect the 2nd publish being retried, as it's not acked
    ok = packet:expect_packet(Bridge2, "2nd publish", PublishDup),
    ok = gen_tcp:send(Bridge2, Puback),
    ok = gen_tcp:close(Bridge2),
    ok = gen_tcp:close(SSocket).

bridge_reconnect_qos2_test(Cfg) ->
    start_bridge_plugin(#{
      mqtt_version => mqtt_version(Cfg),
      qos => 2,
      topics => [{"bridge/#", both, 2, "", ""}]
     }),
    Connect = packet:gen_connect("bridge-test", [{keepalive,60}, {clean_session, false},
                                            {proto_ver, mqtt_version(Cfg)}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(1, "bridge/#", 2),
    Suback = packet:gen_suback(1, 2),
    Subscribe2 = packet:gen_subscribe(3, "bridge/#", 2),
    Suback2 = packet:gen_suback(3, 2),
    Subscribe3 = packet:gen_subscribe(4, "bridge/#", 2),
    Suback3 = packet:gen_suback(4, 2),
    Publish = packet:gen_publish("bridge/disconnect/test", 2, <<"disconnect-message">>, [{mid, 2}]),
    PublishDup = packet:gen_publish("bridge/disconnect/test", 2, <<"disconnect-message">>, [{mid, 2}, {dup, true}]),
    Pubrec = packet:gen_pubrec(2),
    Pubrel = packet:gen_pubrel(2),
    Pubcomp = packet:gen_pubcomp(2),
    {ok, SSocket} = gen_tcp:listen(1890, [binary, {packet, raw}, {active, false}, {reuseaddr, true}]),
    {ok, Bridge} = gen_tcp:accept(SSocket),
    ok = packet:expect_packet(Bridge, "connect", Connect),
    ok = gen_tcp:send(Bridge, Connack),
    BridgeProc =
    receive
        {subscribe, Pid} ->
            Pid
    end,
    ok = packet:expect_packet(Bridge, "subscribe", Subscribe),
    ok = gen_tcp:send(Bridge, Suback),
    disconnect_helper(BridgeProc),
    ok = packet:expect_packet(Bridge, "publish", Publish),
    ok = gen_tcp:close(Bridge),
    {ok, Bridge2} = gen_tcp:accept(SSocket),
    ok = packet:expect_packet(Bridge2, "2nd connect", Connect),
    ok = gen_tcp:send(Bridge2, Connack),
    Publish_2 = packet:expect_packet(Bridge2, "2nd publish", PublishDup),
    Subscribe_2 = packet:expect_packet(Bridge2, "2nd subscribe", Subscribe2),
    catch_undeterministic_packet(Publish_2, [Publish_2, Subscribe_2]), 
    ok = gen_tcp:send(Bridge2, Pubrec),
    catch_undeterministic_packet(Subscribe_2, [Publish_2, Subscribe_2]), 
    ok = gen_tcp:send(Bridge2, Suback2),
    ok = packet:expect_packet(Bridge2, "pubrel", Pubrel),
    ok = gen_tcp:close(Bridge2),
    {ok, Bridge3} = gen_tcp:accept(SSocket),
    ok = packet:expect_packet(Bridge3, "3rd connect", Connect),
    ok = gen_tcp:send(Bridge3, Connack),
    ok = packet:expect_packet(Bridge3, "2nd pubrel", Pubrel),
    ok = packet:expect_packet(Bridge3, "3rd subscribe", Subscribe3),
    ok = gen_tcp:send(Bridge3, Suback3),
    ok = gen_tcp:send(Bridge3, Pubcomp),
    ok = gen_tcp:close(Bridge3),
    ok = gen_tcp:close(SSocket).

catch_undeterministic_packet(Packet, PacketList) ->
    lists:member(Packet, PacketList).

buffer_outgoing_test(Cfg) ->
    %% start bridge
    start_bridge_plugin(#{
      mqtt_version => mqtt_version(Cfg),
      qos => 0,
      max_outgoing_buffered_messages => 10
     }),
    BridgePid = get_bridge_pid(),

    %% Publish some messages before connecting, should be queued.
    pub_to_bridge(BridgePid, <<"m1">>, 0),
    pub_to_bridge(BridgePid, <<"m2">>, 0),
    pub_to_bridge(BridgePid, <<"m3">>, 0),
    pub_to_bridge(BridgePid, <<"m4">>, 0),
    pub_to_bridge(BridgePid, <<"m5">>, 0),
    {ok, #{out_queue_dropped := 0,
           out_queue_max_size := 10,
           out_queue_size := 5}} = vmq_bridge:info(BridgePid),

    %% Start the 'broker' and let the bridge connect
    {ok, SSocket} = gen_tcp:listen(1890, [binary, {packet, raw}, {active, false}, {reuseaddr, true}]),
    {ok, BrokerSocket} = gen_tcp:accept(SSocket, 5000),
    Connect = packet:gen_connect("bridge-test", [{keepalive,60}, {clean_session, false},
                                            {proto_ver, mqtt_version(Cfg)}]),
    ok = packet:expect_packet(BrokerSocket, "connect", Connect),

    %% Publish some more while in the `waiting_for_connack` state
    pub_to_bridge(BridgePid, <<"m6">>, 0),
    pub_to_bridge(BridgePid, <<"m7">>, 0),
    pub_to_bridge(BridgePid, <<"m8">>, 0),
    pub_to_bridge(BridgePid, <<"m9">>, 0),
    pub_to_bridge(BridgePid, <<"m10">>, 0),
    pub_to_bridge(BridgePid, <<"m11">>, 0),
    pub_to_bridge(BridgePid, <<"m12">>, 0),

    {ok, #{out_queue_dropped := 2,
           out_queue_max_size := 10,
           out_queue_size := 10}} = vmq_bridge:info(BridgePid),
      
  [{counter, [], {vmq_bridge_queue_drop, _}, vmq_bridge_dropped_msgs,
      <<"The number of dropped messages (queue full)">>, 2}] = vmq_bridge_sup:metrics_for_tests(),

    %% Reply with the connack
    Connack = packet:gen_connack(0),
    ok = gen_tcp:send(BrokerSocket, Connack),
    ExpectPub = fun(Payload) ->
                        Pub = packet:gen_publish("bridge/topic", 0, Payload, []),
                        ok = packet:expect_packet(BrokerSocket, "publish", Pub)
                end,

    ExpectPub(<<"m1">>),
    ExpectPub(<<"m2">>),
    ExpectPub(<<"m3">>),
    ExpectPub(<<"m4">>),
    ExpectPub(<<"m5">>),
    ExpectPub(<<"m6">>),
    ExpectPub(<<"m7">>),
    ExpectPub(<<"m8">>),
    ExpectPub(<<"m9">>),
    ExpectPub(<<"m10">>),


    %% After having published the buffered messages the bridge will
    %% try to subscribe. TODO: Note this is not completely
    %% deterministic as it could happen that the subscribe is
    %% interleaved with the publishes above. We should fix this if
    %% that becomes a problem.
    Subscribe = packet:gen_subscribe(11, "bridge/#", 0),
    ok = packet:expect_packet(BrokerSocket, "subscribe", Subscribe),
    Suback = packet:gen_suback(11, 2),
    ok = gen_tcp:send(BrokerSocket, Suback),

    %% Everything should be flushed.
    {ok, #{out_queue_dropped := 2,
           out_queue_max_size := 10,
           out_queue_size := 0}} = vmq_bridge:info(BridgePid),

    ok = gen_tcp:close(BrokerSocket).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Test Helpers
%%%
mqtt_version(Config) ->
    proplists:get_value(mqtt_version, Config).

disconnect_helper(BridgeProc) when is_pid(BridgeProc) ->
    BridgeProc ! {deliver, [<<"bridge">>, <<"disconnect">>, <<"test">>], <<"disconnect-message">>, 0, false, false}.


start_bridge_plugin(Opts) ->
    MqttVersion = maps:get(mqtt_version, Opts),
    QoS = maps:get(qos, Opts),
    Max = maps:get(max_outgoing_buffered_messages, Opts, 100),
    Topics = maps:get(topics, Opts, [{"bridge/#", both, QoS, "", ""}]),
    application:load(vmq_bridge),
    application:set_env(vmq_bridge, registry_mfa,
                        {?MODULE, bridge_reg, [self(), []]}),
    application:set_env(vmq_bridge, config,
                        {[
                          %% TCP Bridges
                          {"br0:localhost:1890", [{topics, Topics},
                                              {restart_timeout, 5},
                                              {retry_interval, 1},
                                              {client_id, "bridge-test"},
                                              {max_outgoing_buffered_messages, Max},
                                              {try_private, false}, % enables that we can configure mqtt_version below
                                              {mqtt_version, MqttVersion}]}
                         ],
                         [
                          %% SSL Bridges
                         ]
                        }),
    application:ensure_all_started(vmq_bridge),
    Config = [{vmq_bridge, application:get_all_env(vmq_bridge)}],
    vmq_bridge_sup:change_config(Config).

stop_bridge_plugin() ->
    application:stop(vmq_bridge),
    application:unload(vmq_bridge).

bridge_rec(Msg, Timeout) ->
    receive
        Msg ->
            ok
    after
        Timeout ->
            throw({error, {message_not_received, Msg}})
    end.

get_bridge_pid() ->
    [{{vmq_bridge, _Name, "localhost",1890},Pid,worker,[vmq_bridge]}] =
        supervisor:which_children(vmq_bridge_sup),
    Pid.

pub_to_bridge(BridgePid, Topic, Payload, QoS) ->
    BridgePid ! {deliver, Topic, Payload, QoS, false, false},
    ok.

pub_to_bridge(BridgePid, Payload, QoS) ->
    BridgePid ! {deliver, [<<"bridge">>, <<"topic">>], Payload, QoS, false, false}.

brigdge_reg(ReportProc) ->
    bridge_reg(ReportProc, []).
bridge_reg(ReportProc, _Opts) ->

    RegisterFun = fun() ->
                          ok
                  end,
    PublishFun = fun(Topic, Payload, _Opts2 = #{retain := IsRetain}) when is_boolean(IsRetain) ->
                         ReportProc ! {publish, Topic, Payload},
                         ok
                 end,
    SubscribeFun = fun(_Topic) ->
                           ReportProc ! {subscribe, self()},
                           {ok, [1]}
                   end,
    UnsubscribeFun = fun(_Topic) ->
                             ReportProc ! {unsubscribe, self()},
                             ok
                     end,
{ok, #{publish_fun => PublishFun, register_fun => RegisterFun, 
                     subscribe_fun => SubscribeFun, unsubscribe_fun => UnsubscribeFun}}.