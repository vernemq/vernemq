-module(vmq_netsplit_register_not_ready_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqtt_commons/include/emqtt_frame.hrl").
-define(NET_TICK_TIME, 10).

-compile(export_all).
-ifdef(NETSPLIT_TESTS).
run_test_() ->
    NetTickTime = ?NET_TICK_TIME,
    vmq_netsplit_utils:test(NetTickTime, NetTickTime * 10,
                            fun(Nodes) ->
                                    {timeout, NetTickTime * 5,
                                     [?_test(register_not_ready(Nodes))]}
                            end).
-endif.

register_not_ready(Nodes) ->
    ok = vmq_netsplit_utils:check_connected(Nodes),

    %% Connect a test-client
    Connect = packet:gen_connect("test-client", [{clean_session, true},
                                                 {keepalive, 10}]),
    Connack = packet:gen_connack(0),
    Port = vmq_netsplit_utils:get_port(Nodes),
    {ok, _Socket} = packet:do_client_connect(Connect, Connack,
                                             [{port, Port}]),

    %% Create Partitions
    {Island1, Island2} = vmq_netsplit_utils:partition_network(Nodes),

    %%================================%%
    %%     Window of Uncertanity      %%
    %%                                %%
    %% Erlang thinks everything is ok %%
    %%          ..outch..             %%
    %%================================%%


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

    %% we are now on a partitioned network and SHOULD NOT allow new connections
    ConnNack = packet:gen_connack(3), %% server unavailable
    [begin
         P = vmq_netsplit_utils:get_port([N]),
         {ok, S} = packet:do_client_connect(Connect, ConnNack, [{port, P}]),
         gen_tcp:close(S)
     end || N <- Nodes],

    %% we configure the nodes to trade consistency for availability
    ok = vmq_netsplit_utils:configure_trade_consistency(Nodes),
    [begin
         P = vmq_netsplit_utils:get_port([N]),
         {ok, S} = packet:do_client_connect(Connect, Connack, [{port, P}]),
         gen_tcp:close(S)
     end || N <- Nodes],

    %% fix cables
    vmq_netsplit_utils:fix_network(Island1, Island2),
    timer:sleep(1000),
    vmq_netsplit_utils:check_connected(Nodes),
    ok.
