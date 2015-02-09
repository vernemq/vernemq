-module(vmq_netsplit_register_multiple_session_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqtt_commons/include/emqtt_frame.hrl").

run_test_() ->
    NetTickTime = 10,
    vmq_netsplit_utils:test(NetTickTime, NetTickTime * 10,
                            fun(Nodes) ->
                                    {timeout, NetTickTime * 5,
                                     [?_test(register_consistency(Nodes))]}
                            end).

register_consistency(Nodes) ->
    ok = vmq_netsplit_utils:check_connected(Nodes),
    %% we configure the nodes to trade consistency for availability
    ok = vmq_netsplit_utils:configure_trade_consistency(Nodes),

    %% Create Partitions
    {Island1, Island2} = vmq_netsplit_utils:partition_network(Nodes),

    %%================================%%
    %%     Window of Uncertanity      %%
    %%                                %%
    %% Erlang thinks everything is ok %%
    %%          ..outch..             %%
    %%================================%%

    PortInIsland1 = vmq_netsplit_utils:get_port(Island1),
    PortInIsland2 = vmq_netsplit_utils:get_port(Island2),

    Connect = packet:gen_connect("test-client", [{clean_session, true},
                                                 {keepalive, 10}]),
    Connack = packet:gen_connack(0),
    {ok, Socket1} = packet:do_client_connect(Connect, Connack,
                                             [{port, PortInIsland1}]),

    {ok, Socket2} = packet:do_client_connect(Connect, Connack,
                                               [{port, PortInIsland2}]),
    vmq_netsplit_utils:fix_network(Island1, Island2),
    gen_tcp:close(Socket1),
    gen_tcp:close(Socket2),
    ok.


