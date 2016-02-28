-module(vmq_netsplit_register_not_ready_SUITE).
-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([register_not_ready_test/1]).

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
    [register_not_ready_test].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
register_not_ready_test(Config) ->
    Nodes = proplists:get_value(nodes, Config, []),
    ok = vmq_netsplit_utils:check_connected(Nodes),

    %% Connect a test-client
    Connect = packet:gen_connect("test-client-not-ready", [{clean_session, true},
                                                 {keepalive, 10}]),
    Connack1 = packet:gen_connack(0),
    Port = vmq_netsplit_utils:get_port(Nodes),
    {ok, _Socket} = packet:do_client_connect(Connect, Connack1,
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
    true = vmq_netsplit_utils:ensure_not_ready(Nodes),
    %%================================%%
    %%     Window of Uncertanity      %%
    %%                                %%
    %%        -- CLOSED --            %%
    %%================================%%

    vmq_netsplit_utils:check_connected(Island1),
    vmq_netsplit_utils:check_connected(Island2),

    %% we are now on a partitioned network and SHOULD NOT allow new connections
    ConnNack = packet:gen_connack(3), %% server unavailable
    [begin
         P = vmq_netsplit_utils:get_port([N]),
         {ok, S} = packet:do_client_connect(Connect, ConnNack, [{port, P}]),
         gen_tcp:close(S)
     end || N <- Nodes],

    %% we configure the nodes to trade consistency for availability,
    %% this call alsow sets allow_multiple_sessions=true
    ok = vmq_netsplit_utils:configure_trade_consistency(Nodes),
    %% in this consitency mode (incl. allow_multiple_sessions) we ignore the
    %% clean_session flag and return SessionPresent=true if we discover that a
    %% queue already exists on the connecting node.
    [begin
         P = vmq_netsplit_utils:get_port([N]),
         Connack2 =
         case P == Port of
             true ->
                 % this node has a running queue, which triggers a
                 % sessionPresent=true
                 packet:gen_connack(true, 0);
             false ->
                 packet:gen_connack(false, 0)
         end,
         {ok, S} = packet:do_client_connect(Connect, Connack2, [{port, P}]),
         gen_tcp:close(S)
     end || N <- Nodes],

    %% fix cables
    vmq_netsplit_utils:fix_network(Island1, Island2),
    timer:sleep(1000),
    vmq_netsplit_utils:check_connected(Nodes),
    ok.
