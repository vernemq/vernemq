%% Copyright 2018 Erlio GmbH Basel Switzerland (http://erl.io)
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(vmq_bridge_tests).
-include_lib("eunit/include/eunit.hrl").

-define(setup(F), {setup, fun setup/0, fun teardown/1, F}).
-export([bridge_plugin/1]).
-export([b2br_disconnect_qos2/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Tests Descriptions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
subscribe_test_() ->
    [
    {"Broker to Bridge Disconnect QoS1",
      ?setup(fun b2br_disconnect_qos1/1)}
     ,{"Broker to Bridge Disconnect QoS2",
       ?setup(fun b2br_disconnect_qos2/1)}
     ,{"Broker to Bridge Disconnect QoS1 adv",
       ?setup(fun br2b_disconnect_qos1/1)}
     ,{"Broker to Bridge Disconnect QoS2 adv",
       ?setup(fun br2b_disconnect_qos2/1)}
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Setup Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
setup() ->
    ok.
teardown(_) ->
    flush(),
    application:stop(vmq_bridge).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
b2br_disconnect_qos1(_) ->
    start_bridge_plugin(1),
    Connect = packet:gen_connect("bridge-test", [{keepalive,60}, {clean_session, false},
                                            {proto_ver, 128+3}]),
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
    ?_assertEqual(ok, gen_tcp:close(SSocket)).

b2br_disconnect_qos2(_) ->
    start_bridge_plugin(2),
    Connect = packet:gen_connect("bridge-test", [{keepalive,60}, {clean_session, false},
                                            {proto_ver, 128+3}]),
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
    ?_assertEqual(ok, gen_tcp:close(SSocket)).

br2b_disconnect_qos1(_) ->
    start_bridge_plugin(1),
    Connect = packet:gen_connect("bridge-test", [{keepalive,60}, {clean_session, false},
                                            {proto_ver, 128+3}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(1, "bridge/#", 1),
    Suback = packet:gen_suback(1, 1),
    Publish = packet:gen_publish("bridge/disconnect/test", 1, <<"disconnect-message">>, [{mid, 2}]),
    PublishDup = packet:gen_publish("bridge/disconnect/test", 1, <<"disconnect-message">>, [{mid, 2}, {dup, true}]),
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
    br2b_disconnect_helper(BridgeProc),
    ok = packet:expect_packet(Bridge, "publish", Publish),
    ok = gen_tcp:close(Bridge),
    {ok, Bridge2} = gen_tcp:accept(SSocket),
    ok = packet:expect_packet(Bridge2, "2nd connect", Connect),
    ok = gen_tcp:send(Bridge2, Connack),
    ok = packet:expect_packet(Bridge2, "2nd subscribe", Subscribe2),
    ok = gen_tcp:send(Bridge2, Suback2),
    ok = packet:expect_packet(Bridge2, "2nd publish", PublishDup),
    ok = gen_tcp:close(Bridge2),
    ?_assertEqual(ok, gen_tcp:close(SSocket)).

br2b_disconnect_qos2(_) ->
    start_bridge_plugin(2),
    Connect = packet:gen_connect("bridge-test", [{keepalive,60}, {clean_session, false},
                                            {proto_ver, 128+3}]),
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
    br2b_disconnect_helper(BridgeProc),
    ok = packet:expect_packet(Bridge, "publish", Publish),
    ok = gen_tcp:close(Bridge),
    {ok, Bridge2} = gen_tcp:accept(SSocket),
    ok = packet:expect_packet(Bridge2, "2nd connect", Connect),
    ok = gen_tcp:send(Bridge2, Connack),
    ok = packet:expect_packet(Bridge2, "2nd subscribe", Subscribe2),
    ok = gen_tcp:send(Bridge2, Suback2),
    ok = packet:expect_packet(Bridge2, "2nd publish", PublishDup),
    ok = gen_tcp:send(Bridge2, Pubrec),
    ok = packet:expect_packet(Bridge2, "pubrel", Pubrel),
    ok = gen_tcp:close(Bridge2),
    {ok, Bridge3} = gen_tcp:accept(SSocket),
    ok = packet:expect_packet(Bridge3, "3rd connect", Connect),
    ok = gen_tcp:send(Bridge3, Connack),
    ok = packet:expect_packet(Bridge3, "3rd subscribe", Subscribe3),
    ok = gen_tcp:send(Bridge3, Suback3),
    ok = packet:expect_packet(Bridge3, "2nd pubrel", Pubrel),
    ok = gen_tcp:send(Bridge3, Pubcomp),
    ok = gen_tcp:close(Bridge3),
    ?_assertEqual(ok, gen_tcp:close(SSocket)).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Helper
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
br2b_disconnect_helper(BridgeProc) ->
    BridgeProc ! {deliver, [<<"bridge">>, <<"disconnect">>, <<"test">>], <<"disconnect-message">>, 0, false, false}.

bridge_plugin(ReportProc) ->
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

start_bridge_plugin(QoS) ->
    application:load(vmq_bridge),
    application:set_env(vmq_bridge, registry_mfa,
                        {?MODULE, bridge_plugin, [self()]}),
    application:set_env(vmq_bridge, config,
                        {[
                          %% TCP Bridges
                          {"localhost:1890", [{topics, [{"bridge/#", both, QoS, "", ""}]},
                                                 {restart_timeout, 5},
                                                 {client_id, "bridge-test"}]}
                         ],
                         [
                          %% SSL Bridges
                         ]
                        }),
    application:ensure_all_started(vmq_bridge),
    Config = [{vmq_bridge, application:get_all_env(vmq_bridge)}],
    vmq_bridge_sup:change_config(Config).

flush() ->
    receive
        _ -> flush()
    after
        0 -> ok
    end.
