%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Helium Systems, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(vmq_cluster_test_utils).

-export([
    get_cluster_members/1,
    pmap/2,
    wait_until/3,
    wait_until_left/2,
    wait_until_joined/2,
    wait_until_ready/1,
    wait_until_offline/1,
    wait_until_disconnected/2,
    wait_until_connected/2,
    start_node/3,
    partition_cluster/2,
    heal_cluster/2,
    ensure_cluster/1
    ]).

-include_lib("eunit/include/eunit.hrl").

get_cluster_members(Node) ->
    rpc:call(Node, vmq_plugin, only, [cluster_members, []]).

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
                                lists:sort(ExpectedCluster) ==
                                lists:sort(get_cluster_members(Node))
                        end, Nodes))
        end, 60*2, 500).

wait_until_ready(Nodes) ->
    wait_until(fun() ->
                       NodeStates = [rpc:call(N, vmq_cluster, is_ready, []) || N <- Nodes],
                       lists:all(fun(Bool) -> Bool end, NodeStates)
               end, 60*100, 100).

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
    ct:pal("Start Node ~p for Case ~p~n", [Name, Case]),
    CodePath = lists:filter(fun filelib:is_dir/1, code:get_path()),
    %% have the slave nodes monitor the runner node, so they can't outlive it
    NodeConfig = [
        {monitor_master, true},
        {erl_flags, "-smp"} %% smp for the eleveldb god
    ],
    VmqServerPrivDir = code:priv_dir(vmq_server),
    ct:log("Starting node ~p with Opts = ~p", [Name, NodeConfig]),
    case ct_slave:start(Name, NodeConfig) of
        {ok, Node} ->
            true = rpc:block_call(Node, code, set_path, [CodePath]),
            PrivDir = proplists:get_value(priv_dir, Config),
            NodeDir = filename:join([PrivDir, Node, Case]),
            ok = rpc:call(Node, application, load, [vmq_server]),
            ok = rpc:call(Node, application, load, [vmq_plugin]),
            ok = rpc:call(Node, application, load, [vmq_generic_msg_store]),
           % ok = rpc:call(Node, application, load, [plumtree]),
            ok = rpc:call(Node, application, load, [vmq_swc]),
            ok = rpc:call(Node, application, load, [lager]),
            ok = rpc:call(Node, application, set_env, [vmq_server, metadata_impl, vmq_swc]),
            ok = rpc:call(Node, application, set_env, [vmq_server, max_drain_time, 5000]),
            ok = rpc:call(Node, application, set_env, [vmq_server, max_msgs_per_drain_step, 40]),
            ok = rpc:call(Node, application, set_env, [vmq_server, mqtt_connect_timeout, 12000]),
            ok = rpc:call(Node, application, set_env, [vmq_server, coordinate_registrations, true]),
            ok = rpc:call(Node, application, set_env, [vmq_server, allow_register_during_netsplit, true]),
            ok = rpc:call(Node, application, set_env, [lager,
                                                       log_root,
                                                       NodeDir]),
            ok = rpc:call(Node, application, set_env, [vmq_swc,
                                                       data_dir,
                                                       NodeDir]),
            ok = rpc:call(Node, application, set_env, [vmq_swc,
                                                       metadata_root,
                                                       NodeDir ++ "/meta/"]),
            ok = rpc:call(Node, application, set_env, [vmq_server,
                                                       listeners,
                                                       [{vmq, [{{{127,0,0,1},
                                                                 random_port(Node)},
                                                                []}]}
                                                       ]]),
            ok = rpc:call(Node, application, set_env, [vmq_generic_msg_store,
                                                       msg_store_opts,
                                                       [{store_dir,
                                                         NodeDir++"/msgstore"}]
                                                      ]),
            ok = rpc:call(Node, application, set_env, [vmq_plugin,
                                                       wait_for_proc,
                                                       vmq_server_sup]),
            ok = rpc:call(Node, application, set_env, [vmq_plugin,
                                                       plugin_dir,
                                                       NodeDir]),
            ok = rpc:call(Node, application, set_env, [vmq_plugin,
                                                       default_schema_dir,
                                                       [VmqServerPrivDir]]),
            {ok, _} = rpc:call(Node, application, ensure_all_started,
                               [vmq_server]),
            %{ok, _} = rpc:call(Node, application, ensure_all_started, [vmq_swc]),
            ok = wait_until(fun() ->
                            case rpc:call(Node, vmq_plugin, only, [cluster_members, []]) of
                                {error, no_matching_hook} ->
                                    false;
                                Members when is_list(Members) ->
                                ct:pal("CLUSTER MEMBERS: ~p~n", [Members]),
                                    case rpc:call(Node, erlang, whereis,
                                                  [vmq_server_sup]) of
                                        undefined ->
                                            false;
                                        P when is_pid(P) ->
                                            true
                                    end
                            end
                    end, 60, 500),
            Node;
        {error, already_started, Node} ->
            {ok, _NodeName} = ct_slave:stop(Name),
            wait_until_offline(Node),
            start_node(Name, Config, Case)
    end.

partition_cluster(ANodes, BNodes) ->
    pmap(fun({Node1, Node2}) ->
                true = rpc:call(Node1, erlang, set_cookie, [Node2, canttouchthis]),
                true = rpc:call(Node1, erlang, disconnect_node, [Node2]),
                ok = wait_until_disconnected(Node1, Node2)
        end,
         [{Node1, Node2} || Node1 <- ANodes, Node2 <- BNodes]),
    ok.

heal_cluster(ANodes, BNodes) ->
    GoodCookie = erlang:get_cookie(),
    pmap(fun({Node1, Node2}) ->
                true = rpc:call(Node1, erlang, set_cookie, [Node2, GoodCookie]),
                ok = wait_until_connected(Node1, Node2)
        end,
         [{Node1, Node2} || Node1 <- ANodes, Node2 <- BNodes]),
    ok.

ensure_cluster(Config) ->
    [{Node1, _}|OtherNodes] = Nodes = proplists:get_value(nodes, Config),
    [begin
         {ok, _} = rpc:call(Node, vmq_server_cmd, node_join, [Node1])
     end || {Node, _} <- OtherNodes],
    {NodeNames, _} = lists:unzip(Nodes),
    Expected = lists:sort(NodeNames),
    ok = vmq_cluster_test_utils:wait_until_joined(NodeNames, Expected),
    [?assertEqual({Node, Expected}, {Node,
                                     lists:sort(vmq_cluster_test_utils:get_cluster_members(Node))})
     || Node <- NodeNames],
    vmq_cluster_test_utils:wait_until_ready(NodeNames),
    ok.


random_port(Node) ->
    10000 + (erlang:phash2(Node) rem 10000).