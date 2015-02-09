-module(vmq_netsplit_register_consistency_tests).
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

    %% Create Partitions
    {Island1, Island2} = vmq_netsplit_utils:partition_network(Nodes),

    %%================================%%
    %%     Window of Uncertanity      %%
    %%                                %%
    %% Erlang thinks everything is ok %%
    %%          ..outch..             %%
    %%================================%%

    %% the registration is synchronized using vmq_reg_leader, depending if
    %% we connect to a node sitting in the same partition as the leader we
    %% get disconnected earlier in the registration process.
    PortInIsland1 = vmq_netsplit_utils:get_port(Island1),
    PortInIsland2 = vmq_netsplit_utils:get_port(Island2),

    Connect = packet:gen_connect("test-client", [{clean_session, true},
                                                 {keepalive, 10}]),
    Connack = packet:gen_connack(0),
    {error, closed} = packet:do_client_connect(Connect, Connack,
                                             [{port, PortInIsland1}]),

    {error, closed} = packet:do_client_connect(Connect, Connack,
                                               [{port, PortInIsland2}]),
    vmq_netsplit_utils:fix_network(Island1, Island2),
    ok.
