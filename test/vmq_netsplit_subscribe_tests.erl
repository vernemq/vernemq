-module(vmq_netsplit_subscribe_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqtt_commons/include/emqtt_frame.hrl").
-define(NET_TICK_TIME, 10).

-compile(export_all).
-ifdef(NETSPLIT_TESTS).
run_test_() ->
    NetTickTime = ?NET_TICK_TIME,
    vmq_netsplit_utils:test(NetTickTime, NetTickTime * 10,
                            fun(Nodes) ->
                                    {timeout, NetTickTime * 10,
                                     [?_test(subscribe_clean_session(Nodes))]}
                            end).
-endif.

subscribe_clean_session(Nodes) ->
    vmq_netsplit_utils:reset_tables(Nodes),
    ok = vmq_netsplit_utils:check_connected(Nodes),
    Connect = packet:gen_connect("test-netsplit-client", [{clean_session, false},
                                                          {keepalive, 10}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(53, "netsplit/0/test", 0),
    Suback = packet:gen_suback(53, 0),
    Port = vmq_netsplit_utils:get_port(Nodes),
    {ok, Socket} = packet:do_client_connect(Connect, Connack,
                                            [{port, Port}]),

    %% Create Partitions
    {Island1, Island2} = vmq_netsplit_utils:partition_network(Nodes),

    %%================================%%
    %%     Window of Uncertanity      %%
    %%                                %%
    %% Erlang thinks everything is ok %%
    %%          ..outch..             %%
    %%================================%%

    %% this Subscription will only be visible in one partition
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    Island1Res = vmq_netsplit_utils:proxy_multicall(Island1, vmq_reg,
                                                    subscriptions_for_subscriber,
                                                    [{"", "test-netsplit-client"}]),
    Island1Res = [[{"netsplit/0/test", 0}] || _ <- lists:seq(1, length(Island1))],
    Island2Res = vmq_netsplit_utils:proxy_multicall(Island2, vmq_reg,
                                                    subscriptions_for_subscriber,
                                                    [{"", "test-netsplit-client"}]),
    ?assertEqual(Island2Res, [[] || _ <- lists:seq(1, length(Island2))]),

    %% SLEEP until cluster knows about net split
    io:format(user, "Sleep 2x ~p seconds (net_ticktime)~n", [?NET_TICK_TIME]),
    timer:sleep(?NET_TICK_TIME * 2000), % sleep 2x net_tick_time

    %%================================%%
    %%     Window of Uncertanity      %%
    %%                                %%
    %%        -- CLOSED --            %%
    %%================================%%

    vmq_netsplit_utils:check_connected(Island1),
    vmq_netsplit_utils:check_connected(Island2),
    true = vmq_netsplit_utils:ensure_not_ready(Nodes),


    %% fix the network
    vmq_netsplit_utils:fix_network(Island1, Island2),
    timer:sleep(?NET_TICK_TIME * 2000),
    vmq_netsplit_utils:check_connected(Nodes),

    %% unsplit should have merged the tables
    NodesRes = vmq_netsplit_utils:proxy_multicall(Nodes, vmq_reg,
                                                  subscriptions_for_subscriber,
                                                  [{"", "test-netsplit-client"}]),
    ?assertEqual(NodesRes, [[{"netsplit/0/test", 0}]
                            || _ <- lists:seq(1, length(Nodes))]),
    ok.
