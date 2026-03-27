%% Copyright 2024 Octavo Labs/VerneMQ (https://vernemq.com/)
%% and Individual Contributors.
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

-module(vmq_balance_cluster_SUITE).
-include_lib("kernel/include/logger.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

%% ===================================================================
%% common_test callbacks
%% ===================================================================

init_per_suite(Config) ->
    S = vmq_test_utils:get_suite_rand_seed(),
    Config0 = vmq_cluster_test_utils:init_distribution(Config),
    [S | Config0].

end_per_suite(_Config) ->
    _Config.

init_per_testcase(Case, Config) ->
    vmq_test_utils:seed_rand(Config),
    NodeWithPorts =
        [vmq_cluster_test_utils:random_node_with_port(Case) || _I <- lists:seq(1, 3)],
    Nodes =
        vmq_cluster_test_utils:pmap(
            fun({N, Port}) ->
                {ok, Peer, Node} =
                    vmq_cluster_test_utils:start_node(N, Config, Case),
                {ok, _} =
                    rpc:call(Node, vmq_server_cmd, listener_start, [Port, []]),
                ok = rpc:call(Node, vmq_auth, register_hooks, []),
                {Peer, Node, Port}
            end,
            NodeWithPorts),
    {_, CoverNodes, _} = lists:unzip3(Nodes),
    {ok, _} = cover:start([node() | CoverNodes]),
    [{nodes, Nodes} | Config].

end_per_testcase(_, Config) ->
    {_, NodeList} = lists:keyfind(nodes, 1, Config),
    {Peers, Nodes, _} = lists:unzip3(NodeList),
    vmq_cluster_test_utils:pmap(
        fun({Peer, Node}) ->
            vmq_cluster_test_utils:stop_peer(Peer, Node)
        end,
        lists:zip(Peers, Nodes)),
    ok.

all() ->
    [
     balance_srv_running_on_all_nodes_test,
     balance_disabled_all_nodes_accept_test,
     balance_enabled_cluster_stats_test,
     balance_enabled_even_distribution_accepts_test,
     balance_http_endpoint_on_cluster_test,
     balance_metrics_on_cluster_test,
     balance_skewed_node_rejects_test
    ].

%% ===================================================================
%% Tests
%% ===================================================================

balance_srv_running_on_all_nodes_test(Config) ->
    %% Verify vmq_balance_srv is running on every node in the cluster
    ok = ensure_cluster(Config),
    Nodes = proplists:get_value(nodes, Config),
    {_, NodeNames, _} = lists:unzip3(Nodes),
    lists:foreach(fun(Node) ->
        Pid = rpc:call(Node, erlang, whereis, [vmq_balance_srv]),
        ?assert(is_pid(Pid)),
        ct:pal("vmq_balance_srv running on ~p: ~p", [Node, Pid])
    end, NodeNames).

balance_disabled_all_nodes_accept_test(Config) ->
    %% When balance is disabled (default), all nodes should accept
    ok = ensure_cluster(Config),
    Nodes = proplists:get_value(nodes, Config),
    {_, NodeNames, _} = lists:unzip3(Nodes),
    lists:foreach(fun(Node) ->
        Result = rpc:call(Node, vmq_balance_srv, is_accepting, []),
        ?assert(Result),
        ct:pal("Node ~p is_accepting=~p (disabled)", [Node, Result])
    end, NodeNames).

balance_enabled_cluster_stats_test(Config) ->
    %% Enable balance on all nodes, verify they can see each other's counts
    ok = ensure_cluster(Config),
    Nodes = proplists:get_value(nodes, Config),
    {_, NodeNames, _} = lists:unzip3(Nodes),
    %% Enable balance on all nodes
    lists:foreach(fun(Node) ->
        ok = rpc:call(Node, application, set_env, [vmq_server, balance_enabled, true]),
        ok = rpc:call(Node, application, set_env, [vmq_server, balance_check_interval, 500]),
        restart_remote_balance_srv(Node)
    end, NodeNames),
    %% Wait for a couple check cycles
    timer:sleep(2000),
    %% All nodes should report stats with 3 nodes
    lists:foreach(fun(Node) ->
        {IsAccepting, LocalConns, ClusterAvg, IsEnabled} =
            rpc:call(Node, vmq_balance_srv, balance_stats, []),
        ct:pal("Node ~p: accepting=~p local=~p avg=~p enabled=~p",
               [Node, IsAccepting, LocalConns, ClusterAvg, IsEnabled]),
        ?assertEqual(1, IsEnabled),
        ?assertEqual(1, IsAccepting),  %% 0 connections, should accept
        ?assertEqual(0, LocalConns),
        ?assertEqual(0, ClusterAvg)
    end, NodeNames).

balance_enabled_even_distribution_accepts_test(Config) ->
    %% With balance enabled and connections evenly distributed,
    %% all nodes should accept
    ok = ensure_cluster(Config),
    Nodes = proplists:get_value(nodes, Config),
    {_, NodeNames, Ports} = lists:unzip3(Nodes),
    %% Enable balance on all nodes with low min_connections
    lists:foreach(fun(Node) ->
        ok = rpc:call(Node, application, set_env, [vmq_server, balance_enabled, true]),
        ok = rpc:call(Node, application, set_env, [vmq_server, balance_min_connections, 0]),
        ok = rpc:call(Node, application, set_env, [vmq_server, balance_check_interval, 500]),
        restart_remote_balance_srv(Node)
    end, NodeNames),
    %% Connect same number of clients to each node
    ClientsPerNode = 5,
    Sockets = lists:flatmap(fun({_Node, Port, Idx}) ->
        [begin
            ClientId = "bal-even-" ++ integer_to_list(Idx) ++ "-" ++ integer_to_list(I),
            Connect = packet:gen_connect(ClientId, [{keepalive, 60}]),
            Connack = packet:gen_connack(0),
            {ok, Socket} = packet:do_client_connect(Connect, Connack,
                                                     [{port, Port}]),
            Socket
        end || I <- lists:seq(1, ClientsPerNode)]
    end, lists:zip3(NodeNames, Ports, lists:seq(1, length(NodeNames)))),
    %% Wait for balance check
    timer:sleep(2000),
    %% All nodes should still be accepting (even distribution)
    lists:foreach(fun(Node) ->
        Result = rpc:call(Node, vmq_balance_srv, is_accepting, []),
        ct:pal("Node ~p is_accepting=~p (even distribution)", [Node, Result]),
        ?assert(Result)
    end, NodeNames),
    %% Cleanup
    lists:foreach(fun(S) -> gen_tcp:close(S) end, Sockets).

balance_http_endpoint_on_cluster_test(Config) ->
    %% Verify the HTTP endpoint works on cluster nodes
    ok = ensure_cluster(Config),
    Nodes = proplists:get_value(nodes, Config),
    {_, NodeNames, _} = lists:unzip3(Nodes),
    application:ensure_all_started(inets),
    %% Start HTTP listeners on each node
    HttpPorts = lists:map(fun(Node) ->
        HttpPort = vmq_test_utils:get_free_port(),
        {ok, _} = rpc:call(Node, vmq_server_cmd, listener_start,
                           [HttpPort, [{http, true},
                                       {config_mod, vmq_balance_http},
                                       {config_fun, routes}]]),
        {Node, HttpPort}
    end, NodeNames),
    %% Query each node's balance-health endpoint
    lists:foreach(fun({Node, HttpPort}) ->
        Url = "http://127.0.0.1:" ++ integer_to_list(HttpPort) ++ "/api/balance-health",
        {ok, {{_, 200, _}, _Headers, Body}} = httpc:request(Url),
        Json = vmq_json:decode(list_to_binary(Body), [return_maps, {labels, binary}]),
        ?assertEqual(<<"accepting">>, maps:get(<<"status">>, Json)),
        ct:pal("Node ~p HTTP balance-health: ~p", [Node, Json])
    end, HttpPorts),
    %% Cleanup
    lists:foreach(fun({_Node, HttpPort}) ->
        %% Stop is best-effort in cluster tests
        catch vmq_server_cmd:listener_stop(HttpPort, "127.0.0.1", false)
    end, HttpPorts).

balance_metrics_on_cluster_test(Config) ->
    %% Verify balance metrics report correct default values on cluster nodes
    ok = ensure_cluster(Config),
    Nodes = proplists:get_value(nodes, Config),
    {_, NodeNames, _} = lists:unzip3(Nodes),
    lists:foreach(fun(Node) ->
        {IsAccepting, LocalConns, ClusterAvg, IsEnabled} =
            rpc:call(Node, vmq_balance_srv, balance_stats, []),
        ct:pal("Node ~p metrics: accepting=~p conns=~p avg=~p enabled=~p",
               [Node, IsAccepting, LocalConns, ClusterAvg, IsEnabled]),
        %% Disabled by default: accepting=1, enabled=0, no connections
        ?assertEqual(1, IsAccepting),
        ?assertEqual(0, LocalConns),
        ?assertEqual(0, ClusterAvg),
        ?assertEqual(0, IsEnabled)
    end, NodeNames).

balance_skewed_node_rejects_test(Config) ->
    %% Create an imbalanced cluster: load one node heavily, leave others empty.
    %% The overloaded node should eventually reject.
    ok = ensure_cluster(Config),
    Nodes = proplists:get_value(nodes, Config),
    [{_, _Node1, Port1}, {_, _Node2, _Port2}, {_, _Node3, _Port3}] = Nodes,
    {_, NodeNames, _} = lists:unzip3(Nodes),
    %% Enable balance on all nodes with aggressive settings
    lists:foreach(fun(Node) ->
        ok = rpc:call(Node, application, set_env, [vmq_server, balance_enabled, true]),
        ok = rpc:call(Node, application, set_env, [vmq_server, balance_threshold, "1.1"]),
        ok = rpc:call(Node, application, set_env, [vmq_server, balance_hysteresis, "0.05"]),
        ok = rpc:call(Node, application, set_env, [vmq_server, balance_min_connections, 0]),
        ok = rpc:call(Node, application, set_env, [vmq_server, balance_check_interval, 300]),
        restart_remote_balance_srv(Node)
    end, NodeNames),
    %% Connect many clients to node1 only, leave others at 0
    NumClients = 30,
    Sockets = [begin
        ClientId = "bal-skew-" ++ integer_to_list(I),
        Connect = packet:gen_connect(ClientId, [{keepalive, 60}]),
        Connack = packet:gen_connack(0),
        {ok, Socket} = packet:do_client_connect(Connect, Connack, [{port, Port1}]),
        Socket
    end || I <- lists:seq(1, NumClients)],
    %% Wait for several balance check cycles
    timer:sleep(3000),
    %% Node1 (overloaded) should be rejecting with correct stats
    [OverloadedNode | OtherNodes] = NodeNames,
    {OvAccepting, OvLocalConns, OvClusterAvg, OvEnabled} =
        rpc:call(OverloadedNode, vmq_balance_srv, balance_stats, []),
    ct:pal("Overloaded node ~p: accepting=~p local=~p avg=~p enabled=~p",
           [OverloadedNode, OvAccepting, OvLocalConns, OvClusterAvg, OvEnabled]),
    ?assertEqual(0, OvAccepting),  %% rejecting
    ?assertEqual(1, OvEnabled),
    ?assertEqual(NumClients, OvLocalConns),
    %% Avg should be NumClients / 3 nodes = 10
    ?assertEqual(NumClients div 3, OvClusterAvg),
    %% Other nodes (empty) should be accepting
    lists:foreach(fun(Node) ->
        Result = rpc:call(Node, vmq_balance_srv, is_accepting, []),
        ct:pal("Empty node ~p is_accepting=~p", [Node, Result]),
        ?assert(Result)
    end, OtherNodes),
    %% Verify HTTP endpoint reflects the rejection
    application:ensure_all_started(inets),
    HttpPort = vmq_test_utils:get_free_port(),
    {ok, _} = rpc:call(OverloadedNode, vmq_server_cmd, listener_start,
                       [HttpPort, [{http, true},
                                   {config_mod, vmq_balance_http},
                                   {config_fun, routes}]]),
    Url = "http://127.0.0.1:" ++ integer_to_list(HttpPort) ++ "/api/balance-health",
    {ok, {{_, StatusCode, _}, _Headers, Body}} = httpc:request(Url),
    ct:pal("Overloaded node HTTP status=~p body=~s", [StatusCode, Body]),
    ?assertEqual(503, StatusCode),
    Json = vmq_json:decode(list_to_binary(Body), [return_maps, {labels, binary}]),
    ?assertEqual(<<"rejecting">>, maps:get(<<"status">>, Json)),
    %% Cleanup
    lists:foreach(fun(S) -> gen_tcp:close(S) end, Sockets).

%% ===================================================================
%% Helpers
%% ===================================================================

ensure_cluster(Config) ->
    [{_, Node1, _} | OtherNodes] = Nodes = proplists:get_value(nodes, Config),
    [begin
         {ok, _} = rpc:call(Node, vmq_server_cmd, node_join, [Node1])
     end || {_Peer, Node, _} <- OtherNodes],
    {_, NodeNames, _} = lists:unzip3(Nodes),
    Expected = lists:sort(NodeNames),
    ok = vmq_cluster_test_utils:wait_until_joined(NodeNames, Expected),
    [?assertEqual({Node, Expected},
                  {Node, lists:sort(vmq_cluster_test_utils:get_cluster_members(Node))})
     || Node <- NodeNames],
    vmq_cluster_test_utils:wait_until_ready(NodeNames),
    ok.

restart_remote_balance_srv(Node) ->
    OldPid = rpc:call(Node, erlang, whereis, [vmq_balance_srv]),
    rpc:call(Node, supervisor, terminate_child, [vmq_server_sup, vmq_balance_srv]),
    rpc:call(Node, supervisor, restart_child, [vmq_server_sup, vmq_balance_srv]),
    vmq_cluster_test_utils:wait_until(fun() ->
        case rpc:call(Node, erlang, whereis, [vmq_balance_srv]) of
            Pid when is_pid(Pid), Pid =/= OldPid -> true;
            _ -> false
        end
    end, 50, 100).
