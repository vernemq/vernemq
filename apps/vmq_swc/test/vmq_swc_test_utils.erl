-module(vmq_swc_test_utils).

-export([
    get_cluster_members/1,
    pmap/2,
    wait_until/3,
    wait_until_left/2,
    wait_until_joined/2,
    wait_until_offline/1,
    wait_until_disconnected/2,
    wait_until_connected/2,
    start_node/3,
    partition_cluster/2,
    heal_cluster/2
    ]).
get_cluster_members(Node) ->
    Config = rpc:call(Node, vmq_swc, config, [test]),
    lists:sort([Node|
                [binary_to_atom(M, utf8)
                 || M <- rpc:call(Node, vmq_swc_group_membership, get_alive_members, [Config])]]).

pmap(F, L) ->
    Parent = self(),
    lists:foldl(
        fun(X, N) ->
                spawn_link(fun() ->
                            Parent ! {pmap, N, F(X)}
                    end),
                N+1
        end, 0, L),
    L2 = [receive {pmap, N, R} -> {N,R} end || _ <- L],
    {_, L3} = lists:unzip(lists:keysort(1, L2)),
    L3.

wait_until(Fun, Retry, Delay) when Retry > 0 ->
    Res = Fun(),
    case Res of
        true ->
            ok;
        _ when Retry == 1 ->
            {fail, Res};
        _ ->
            timer:sleep(Delay),
            wait_until(Fun, Retry-1, Delay)
    end.

wait_until_left(Nodes, LeavingNode) ->
    wait_until(fun() ->
                lists:all(fun(X) -> X == true end,
                          pmap(fun(Node) ->
                                not
                                lists:member(LeavingNode,
                                             get_cluster_members(Node))
                        end, Nodes))
        end, 60*2, 500).

wait_until_joined(Nodes, ExpectedCluster) ->
    wait_until(fun() ->
                lists:all(fun(X) -> X == true end,
                          pmap(fun(Node) ->
                                M = lists:sort(get_cluster_members(Node)),
                                lists:sort(ExpectedCluster) == M
                        end, Nodes))
        end, 60*2, 500).

wait_until_offline(Node) ->
    wait_until(fun() ->
                pang == net_adm:ping(Node)
        end, 60*2, 500).

wait_until_disconnected(Node1, Node2) ->
    wait_until(fun() ->
                pang == rpc:call(Node1, net_adm, ping, [Node2])
        end, 60*2, 500).

wait_until_connected(Node1, Node2) ->
    wait_until(fun() ->
                pong == rpc:call(Node1, net_adm, ping, [Node2])
        end, 60*2, 500).

start_node(Name, Config, Case) ->
    CodePath = lists:filter(fun filelib:is_dir/1, code:get_path()),
    %% have the slave nodes monitor the runner node, so they can't outlive it
    NodeConfig = [
            {monitor_master, true},
            {erl_flags, "-smp"},
            {startup_functions, [
                    {code, set_path, [CodePath]}
                    ]}],
    case ct_slave:start(Name, NodeConfig) of
        {ok, Node} ->

            PrivDir = proplists:get_value(priv_dir, Config),
            NodeDir = filename:join([PrivDir, Node, Case]),
            ok = rpc:call(Node, application, load, [vmq_swc]),
            ok = rpc:call(Node, application, load, [lager]),
            ok = rpc:call(Node, application, set_env, [lager,
                                                       log_root,
                                                       NodeDir]),
            ok = rpc:call(Node, application, set_env, [vmq_swc,
                                                       data_dir,
                                                       NodeDir]),
            SyncInterval = proplists:get_value(sync_interval, Config, {100,50}),
            ok = rpc:call(Node, application, set_env, [vmq_swc, sync_interval,
                                                       SyncInterval]),
            AutoGC = proplists:get_value(auto_gc, Config, true),
            ok = rpc:call(Node, application, set_env, [vmq_swc, auto_gc, AutoGC]),

            ok = rpc:call(Node, application, set_env, [vmq_swc, listener_address, {"127.0.0.1",
                                                                                   20000 + (erlang:phash2(Node) rem 1000)}]),

            ok = rpc:call(Node, application, set_env, [vmq_swc, transport_mod, vmq_swc_proxy_diameter_transport]),
            SwcGroup = proplists:get_value(swc_group, Config, local),

            {ok, _} = rpc:call(Node, vmq_swc, start, [SwcGroup]),
            ok = wait_until(fun() ->
                            case rpc:call(Node, plumtree_peer_service_manager, get_local_state, []) of
                                {ok, _Res} -> true;
                                _ -> false
                            end
                    end, 60, 500),
            Node;
        {error, already_started, Node} ->
            ct_slave:stop(Name),
            wait_until_offline(Node),
            start_node(Name, Config, Case)
    end.

partition_cluster(ANodes, BNodes) ->
    vmq_swc_proxy_diameter_transport:partition_cluster(ANodes, BNodes).

heal_cluster(ANodes, BNodes) ->
    vmq_swc_proxy_diameter_transport:heal_partitioned_cluster(ANodes, BNodes).
