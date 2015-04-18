-module(vmq_netsplit_subscribe_SUITE).
-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([subscribe_clean_session_test/1]).

-define(NET_TICK_TIME, 5).

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(_Config) ->
    cover:start(),
    _Config.

end_per_suite(_Config) ->
    _Config.

init_per_testcase(_Case, Config) ->
    Nodes = vmq_netsplit_utils:setup(?NET_TICK_TIME),
    [{nodes, Nodes}|Config].

end_per_testcase(_, Config) ->
    vmq_netsplit_utils:teardown(proplists:get_value(nodes, Config, [])),
    Config.

all() ->
    [subscribe_clean_session_test].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
subscribe_clean_session_test(Config) ->
    Nodes = proplists:get_value(nodes, Config, []),
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
    timer:sleep(100),
    Island1Res = vmq_netsplit_utils:proxy_multicall(Island1, vmq_reg,
                                                    subscriptions_for_subscriber_id,
                                                    [{"", "test-netsplit-client"}]),
    ct:pal("subscrptions on island 1: ~p", [Island1Res]),
    Island1Res = [[{"netsplit/0/test", 0}] || _ <- lists:seq(1, length(Island1))],
    Island2Res = vmq_netsplit_utils:proxy_multicall(Island2, vmq_reg,
                                                    subscriptions_for_subscriber_id,
                                                    [{"", "test-netsplit-client"}]),
    Island2Res =  [[] || _ <- lists:seq(1, length(Island2))],

    %% SLEEP until cluster knows about net split
    true = vmq_netsplit_utils:ensure_not_ready(Nodes),

    %%================================%%
    %%     Window of Uncertanity      %%
    %%                                %%
    %%        -- CLOSED --            %%
    %%================================%%

    vmq_netsplit_utils:check_connected(Island1),
    vmq_netsplit_utils:check_connected(Island2),


    %% fix the network
    vmq_netsplit_utils:fix_network(Island1, Island2),
    timer:sleep(?NET_TICK_TIME * 2000),
    vmq_netsplit_utils:check_connected(Nodes),

    %% unsplit should have merged the tables
    NodesRes = vmq_netsplit_utils:proxy_multicall(Nodes, vmq_reg,
                                                  subscriptions_for_subscriber_id,
                                                  [{"", "test-netsplit-client"}]),
    NodesRes = [[{"netsplit/0/test", 0}] || _ <- lists:seq(1, length(Nodes))].
