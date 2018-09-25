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

init_per_group(_GroupName, Config) ->
    Config.

end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    application:stop(vmq_bridge),
    ok.

groups() ->
    [
    {mqttv4, [],
     [
      buffer_outgoing
      ]
    }].

all() ->
    [{group, mqttv4}].

get_bridge_pid() ->
    [{{vmq_bridge,"localhost",1890},Pid,worker,[vmq_bridge]}] =
        supervisor:which_children(vmq_bridge_sup),
    Pid.

pub_to_bridge(BridgePid, Payload, QoS) ->
    BridgePid ! {deliver, [<<"bridge">>, <<"topic">>], Payload, QoS, false, false}.

buffer_outgoing(_Cfg) ->
    %% start bridge
    start_bridge_plugin(#{qos => 0,
                          max_outgoing_buffered_messages => 10}),
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
                                            {proto_ver, 128+3}]),
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

start_bridge_plugin(Opts) ->
    QoS = maps:get(qos, Opts),
    Max = maps:get(max_outgoing_buffered_messages, Opts),
    application:load(vmq_bridge),
    application:set_env(vmq_bridge, registry_mfa,
                        {?MODULE, bridge_reg, [self()]}),
    application:set_env(vmq_bridge, config,
                        {[
                          %% TCP Bridges
                          {"localhost:1890", [{topics, [{"bridge/#", both, QoS, "", ""}]},
                                              {restart_timeout, 5},
                                              {client_id, "bridge-test"},
                                              {max_outgoing_buffered_messages, Max}]}
                         ],
                         [
                          %% SSL Bridges
                         ]
                        }),
    application:ensure_all_started(vmq_bridge),
    Config = [{vmq_bridge, application:get_all_env(vmq_bridge)}],
    vmq_bridge_sup:change_config(Config).

bridge_reg(ReportProc) ->
    RegisterFun = fun() ->
                          ok
                  end,
    PublishFun = fun(Topic, Payload, _Opts) ->
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
    {RegisterFun, PublishFun, {SubscribeFun, UnsubscribeFun}}.
