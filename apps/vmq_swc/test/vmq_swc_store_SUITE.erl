-module(vmq_swc_store_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([
         read_write_delete_test/1,
         partitioned_cluster_test/1,
         partitioned_delete_test/1,
         siblings_test/1,
         cluster_join_test/1,
         cluster_leave_test/1,
         events_test/1,
         full_sync_test/1
        ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

-define(SWC_GROUP, test).
-define(STORE_NAME, list_to_atom("vmq_swc_store_" ++ atom_to_list(?SWC_GROUP))).

%% ===================================================================
%% common_test callbacks
%% ===================================================================

init_per_suite(Config) ->
    lager:start(),
    %% this might help, might not...
    os:cmd(os:find_executable("epmd")++" -daemon"),
    {ok, Hostname} = inet:gethostname(),
    case net_kernel:start([list_to_atom("runner@"++Hostname), shortnames]) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,
    lager:info("node name ~p", [node()]),
    [{swc_group, ?SWC_GROUP}|Config].

end_per_suite(_Config) ->
    application:stop(lager),
    _Config.

init_per_testcase(partitioned_delete_test = Case, Config0) ->
    Config1 = [{sync_interval, 0},{auto_gc, true}|Config0],
    init_per_testcase_(Case, Config1, [electra, flail]);
init_per_testcase(Case, Config) ->
    init_per_testcase_(Case, Config, [electra, katana, flail, gargoyle]).

init_per_testcase_(Case, Config, Nodenames) ->
    Nodes = vmq_swc_test_utils:pmap(fun(N) ->
                    vmq_swc_test_utils:start_node(N, Config, Case)
            end, Nodenames),
    {ok, _} = ct_cover:add_nodes(Nodes),
    [{nodes, Nodes}|Config].

end_per_testcase(_, _Config) ->
    vmq_swc_test_utils:pmap(fun(Node) ->ct_slave:stop(Node) end, [electra, katana, flail, gargoyle]),
    ok.

all() ->
    [read_write_delete_test,
     partitioned_cluster_test,
     siblings_test,
     cluster_leave_test,
     cluster_join_test,
     events_test,
     full_sync_test].

read_write_delete_test(Config) ->
    [Node1|OtherNodes] = Nodes = proplists:get_value(nodes, Config),
    [?assertEqual(ok, rpc:call(Node, plumtree_peer_service, join, [Node1]))
     || Node <- OtherNodes],
    Expected = lists:sort(Nodes),
    ok = vmq_swc_test_utils:wait_until_joined(Nodes, Expected),
    [?assertEqual({Node, Expected}, {Node,
                                     lists:sort(vmq_swc_test_utils:get_cluster_members(Node))})
     || Node <- Nodes],
    ?assertEqual(undefined, get_metadata(Node1, {foo, bar}, baz, [])),
    ok = put_metadata(Node1, {foo, bar}, baz, quux, []),
    ?assertEqual(quux, get_metadata(Node1, {foo, bar}, baz, [])),
    ok = wait_until_converged(Nodes, {foo, bar}, baz, quux),
    ok = put_metadata(Node1, {foo, bar}, baz, norf, []),
    ok = wait_until_converged(Nodes, {foo, bar}, baz, norf),
    ok = delete_metadata(Node1, {foo, bar}, baz),
    ok = wait_until_converged(Nodes, {foo, bar}, baz, undefined),
    ok.

partitioned_cluster_test(Config) ->
    [Node1|OtherNodes] = Nodes = proplists:get_value(nodes, Config),
    [?assertEqual(ok, rpc:call(Node, plumtree_peer_service, join, [Node1]))
     || Node <- OtherNodes],
    Expected = lists:sort(Nodes),
    ok = vmq_swc_test_utils:wait_until_joined(Nodes, Expected),
    [?assertEqual({Node, Expected}, {Node,
                                     lists:sort(vmq_swc_test_utils:get_cluster_members(Node))})
     || Node <- Nodes],
    ok = wait_until_converged(Nodes, {foo, bar}, baz, undefined),
    ok = put_metadata(Node1, {foo, bar}, baz, quux, []),
    ok = wait_until_converged(Nodes, {foo, bar}, baz, quux),
    {ANodes, BNodes} = lists:split(2, Nodes),
    vmq_swc_test_utils:partition_cluster(ANodes, BNodes),
    %% write to one side
    ok = put_metadata(Node1, {foo, bar}, baz, norf, []),
    %% check that whole side has the new value
    ok = wait_until_converged(ANodes, {foo, bar}, baz, norf),
    %% the far side should have the old value
    ok = wait_until_converged(BNodes, {foo, bar}, baz, quux),
    vmq_swc_test_utils:heal_cluster(ANodes, BNodes),
    %% all the nodes should see the new value
    ok = wait_until_converged(Nodes, {foo, bar}, baz, norf),
    ok.

partitioned_delete_test(Config) ->
    [Node1, Node2] = Nodes = proplists:get_value(nodes, Config),
    ?assertEqual(ok, rpc:call(Node1, plumtree_peer_service, join, [Node2])),
    Expected = lists:sort(Nodes),
    ok = vmq_swc_test_utils:wait_until_joined(Nodes, Expected),
    [?assertEqual({Node, Expected}, {Node,
                                     lists:sort(vmq_swc_test_utils:get_cluster_members(Node))})
     || Node <- Nodes],
    ok = wait_until_converged(Nodes, {foo, bar}, baz, undefined),
    ok = write(Node1, {foo, bar}, baz, quux),
    ok = wait_until_converged(Nodes, {foo, bar}, baz, quux),

    %% disable broadcasts
    disable_broadcast(Nodes),

    %% delete on one side
    {_, Context} = read(Node1, {foo, bar}, baz),
    ok = write(Node1, {foo, bar}, baz, '$deleted', Context),
    NoValuePred =
        fun(_, {[], [_|_]}) -> true;
           (Node, {_, _} = CC) ->
                io:format(user, "XXX ~p NoValuePred: ~p~n", [Node, CC]),
                false
        end,
    QuuxValuePred =
        fun(_, {[quux], _}) -> true;
           (_Node, _Val) ->
                false
        end,
    NoCCPred =
        fun(_Node, {[], []} = _Val) ->
                %% io:format(user, "XXX ~p NoCCPred: ~p~n", [Node, Val]),
                true;
           (_Node, _Val) ->
                %% io:format(user, "XXX ~p NoCCPred: ~p~n", [Node, Val]),
                false
        end,
    %% check that writer side has seen the delete
    ok = wait_until_causal_context([Node1], {foo, bar}, baz, NoValuePred),

    %% the far side should have the old value
    ok = wait_until_causal_context([Node2], {foo, bar}, baz, QuuxValuePred),

    %% replicate the delete using manual sync.
    rpc:call(Node1, erlang, send, [?STORE_NAME, {sync_with, Node2}]),

    %% all the nodes should see the delete and no value should be
    %% present.
    ok = wait_until_causal_context(Nodes, {foo, bar}, baz, NoCCPred),
    %% [dump(N) || N <- Nodes],
    %% io:format(user, "Nodes: ~p~n", [Nodes]),
    ok.

siblings_test(Config) ->
    [Node1|OtherNodes] = Nodes = proplists:get_value(nodes, Config),
    [?assertEqual(ok, rpc:call(Node, plumtree_peer_service, join, [Node1]))
     || Node <- OtherNodes],
    Expected = lists:sort(Nodes),
    ok = vmq_swc_test_utils:wait_until_joined(Nodes, Expected),
    [?assertEqual({Node, Expected}, {Node,
                                     lists:sort(vmq_swc_test_utils:get_cluster_members(Node))})
     || Node <- Nodes],
    ok = wait_until_converged(Nodes, {foo, bar}, baz, undefined),
    ok = put_metadata(Node1, {foo, bar}, baz, quux, []),
    ok = put_metadata(Node1, {foo, bar}, canary, 1, []),
    ok = wait_until_converged(Nodes, {foo, bar}, baz, quux),
    ok = wait_until_converged(Nodes, {foo, bar}, canary, 1),
    {ANodes, BNodes} = lists:split(2, Nodes),
    vmq_swc_test_utils:partition_cluster(ANodes, BNodes),
    %% write to one side
    ok = put_metadata(Node1, {foo, bar}, baz, norf, []),
    ok = put_metadata(Node1, {foo, bar}, canary, 2, []),
    %% check that whole side has the new value
    ok = wait_until_converged(ANodes, {foo, bar}, baz, norf),
    ok = wait_until_converged(ANodes, {foo, bar}, canary, 2),
    %% the far side should have the old value
    ok = wait_until_converged(BNodes, {foo, bar}, baz, quux),
    ok = wait_until_converged(BNodes, {foo, bar}, canary, 1),
    %% write a competing value to the other side
    [Node3|_] = BNodes,
    ok = put_metadata(Node3, {foo, bar}, baz, mork, []),
    ok = wait_until_converged(BNodes, {foo, bar}, baz, mork),
    vmq_swc_test_utils:heal_cluster(ANodes, BNodes),
    %% block until the canary key converges
    ok = wait_until_converged(Nodes, {foo, bar}, canary, 2),
    %% make sure we have siblings, but don't resolve them yet
    ok = wait_until_sibling(Nodes, {foo, bar}, baz),
    %% resolve the sibling
    spork = get_metadata(Node1, {foo, bar}, baz, [{resolver, fun(_Object) ->
                            spork end}, {allow_put, false}]),
    %% without allow_put set, all the siblings are still there...
    ok = wait_until_sibling(Nodes, {foo, bar}, baz),
    %% resolve the sibling and write it back
    spork = get_metadata(Node1, {foo, bar}, baz, [{resolver, fun(_Object) ->
                            spork end}, {allow_put, true}]),
    %% check all the nodes see the resolution
    ok = wait_until_converged(Nodes, {foo, bar}, baz, spork),
    ok.

cluster_join_test(Config) ->
    % we form two clusters, fill them with data, let the log GC cleanup
    % all history and let them join, as a result all nodes should have all the data.
    [OtherNode, Node1 | OtherNodes] = AllNodes = proplists:get_value(nodes, Config),
    [?assertEqual(ok, rpc:call(Node, plumtree_peer_service, join, [Node1]))
     || Node <- OtherNodes],
    Nodes0 = [Node1 | OtherNodes],
    Expected0 = lists:sort(Nodes0),
    ok = vmq_swc_test_utils:wait_until_joined(Nodes0, Expected0),
    [?assertEqual({Node, Expected0}, {Node,
                                     lists:sort(vmq_swc_test_utils:get_cluster_members(Node))})
     || Node <- Nodes0],
    % Fill Cluster 1
    ok = wait_until_converged(Nodes0, {foo, bar}, baz, undefined),
    ok = put_metadata(Node1, {foo, bar}, baz, quux, []),
    ok = put_metadata(Node1, {foo, bar}, canary, 1, []),
    ok = wait_until_converged(Nodes0, {foo, bar}, baz, quux),
    ok = wait_until_converged(Nodes0, {foo, bar}, canary, 1),

    % Wait until all the nodes have deleted the log
    ok = wait_until_log_gc(AllNodes),

    % join the two clusters
    ?assertEqual(ok, rpc:call(OtherNode, plumtree_peer_service, join, [Node1])),
    Expected1 = lists:sort(AllNodes),
    ok = vmq_swc_test_utils:wait_until_joined(AllNodes, Expected1),
    ok = wait_until_converged(AllNodes, {foo, bar}, baz, quux),
    ok = wait_until_converged(AllNodes, {foo, bar}, canary, 1),
    % Wait until all nodes have deleted the log
    ok = wait_until_log_gc(AllNodes).

cluster_leave_test(Config) ->
    [Node1|OtherNodes] = Nodes = proplists:get_value(nodes, Config),
    [?assertEqual(ok, rpc:call(Node, plumtree_peer_service, join, [Node1]))
     || Node <- OtherNodes],
    Expected = lists:sort(Nodes),
    ok = vmq_swc_test_utils:wait_until_joined(Nodes, Expected),
    [?assertEqual({Node, Expected}, {Node,
                                     lists:sort(vmq_swc_test_utils:get_cluster_members(Node))})
     || Node <- Nodes],
    ok = wait_until_converged(Nodes, {foo, bar}, baz, undefined),
    ok = put_metadata(Node1, {foo, bar}, baz, quux, []),
    ok = put_metadata(Node1, {foo, bar}, canary, 1, []),
    ok = wait_until_converged(Nodes, {foo, bar}, baz, quux),
    ok = wait_until_converged(Nodes, {foo, bar}, canary, 1),

    % remove Node1
    ok = rpc:call(Node1, plumtree_peer_service, leave, [[]]),
    [Node2|_] = Nodes1 = Nodes -- [Node1],

    % put some new data
    ok = put_metadata(Node2, {foo, bar}, hello, world, []),
    ok = wait_until_converged(Nodes1, {foo, bar}, hello, world),
    % Wait until all nodes have deleted the log, because without a proper
    % node leave we would keep the log so the Node can catch up later
    ok = wait_until_log_gc(Nodes1).

events_test(Config) ->
    [Node1|OtherNodes] = Nodes = proplists:get_value(nodes, Config),
    [?assertEqual(ok, rpc:call(Node, plumtree_peer_service, join, [Node1]))
     || Node <- OtherNodes],
    Expected = lists:sort(Nodes),
    ok = vmq_swc_test_utils:wait_until_joined(Nodes, Expected),
    [?assertEqual({Node, Expected}, {Node,
                                     lists:sort(vmq_swc_test_utils:get_cluster_members(Node))})
     || Node <- Nodes],

    Myself = node(),
    T = ets:new(?MODULE, [public, {write_concurrency, true}]),
    Fun = fun({updated, rand, Key, _OldValues, NewValues}) ->
                  rpc:call(Myself, ets, insert, [T, {{node(), Key}, NewValues}]);
             ({deleted, rand, Key, _OldValues}) ->
                  rpc:call(Myself, ets, delete, [T, {node(), Key}])
          end,
    [begin
         SwcGroupConfig = config(Node),
         rpc:call(Node, vmq_swc_store, subscribe, [SwcGroupConfig, rand, Fun])
     end || Node <- Nodes],

    lists:foreach(fun(I) ->
                          RandNode = lists:nth(rand:uniform(length(Nodes)), Nodes),
                          ok = put_metadata(RandNode, rand, I, I, [])
                  end, lists:seq(1, 1000)),
    ok = wait_until_log_gc(Nodes),
    ?assertEqual(length(Nodes) * 1000, ets:info(T, size)),
    lists:foreach(fun(I) ->
                          RandNode = lists:nth(rand:uniform(length(Nodes)), Nodes),
                          ok = delete_metadata(RandNode, rand, I)
                  end, lists:seq(1, 1000)),
    ok = wait_until_log_gc(Nodes),
    ?assertEqual(0, ets:info(T, size)),
    ok.

full_sync_test(Config) ->
    [LastNode|Nodes] = proplists:get_value(nodes, Config),
    [Node1|OtherNodes] = Nodes,
    [?assertEqual(ok, rpc:call(Node, plumtree_peer_service, join, [Node1]))
     || Node <- OtherNodes],

    Expected = lists:sort(Nodes),
    ok = vmq_swc_test_utils:wait_until_joined(Nodes, Expected),
    [?assertEqual({Node, Expected}, {Node,
                                     lists:sort(vmq_swc_test_utils:get_cluster_members(Node))})
     || Node <- Nodes],
    % at this point the cluster is fully clustered with the exception of LastNode
    lists:foreach(fun(I) ->
                          RandNode = lists:nth(rand:uniform(length(Nodes)), Nodes),
                          ok = put_metadata(RandNode, rand, I, I, [])
                  end, lists:seq(1, 10000)),
    ok = wait_until_log_gc(Nodes),

    io:format(user, "start full sync on node ~p~n", [LastNode]),
    % let's join the LastNode,
    ?assertEqual(ok, rpc:call(Node1, plumtree_peer_service, join, [LastNode])),

    % insert some more entries while joining the cluster
    lists:foreach(fun(I) ->
                          RandNode = lists:nth(rand:uniform(length([LastNode|Nodes])), [LastNode|Nodes]),
                          ok = put_metadata(RandNode, rand, I, I, [])
                  end, lists:seq(10001, 20000)),
    ok = wait_until_log_gc([LastNode|Nodes]),

    lists:foreach(fun(I) ->
                          wait_until_converged([LastNode|Nodes], rand, I, I)
                  end, lists:seq(1, 20000)).


disable_broadcast(Nodes) ->
    [ok = rpc:call(N, vmq_swc_store, set_broadcast, [config(N), false])
     || N <- Nodes].

broadcast(_Objects, _Peers) ->
    %% drop!
    ok.

config(Node) ->
    rpc:call(Node, vmq_swc, config, [?SWC_GROUP]).


%% ===================================================================
%% utility functions
%% ===================================================================

%% Raw api (no sibling merges)
write(Node, Prefix, Key, Val) ->
    rpc:call(Node, vmq_swc, raw_put, [?SWC_GROUP, Prefix, Key, Val, swc_vv:new()]).

write(Node, Prefix, Key, Val, Context) ->
    rpc:call(Node, vmq_swc, raw_put, [?SWC_GROUP, Prefix, Key, Val, Context]).

delete(Node, Prefix, Key) ->
    %% Metadata delete doesn't do any merges
    delete_metadata(Node, Prefix, Key).

read(Node, Prefix, Key) ->
    rpc:call(Node, vmq_swc, raw_get, [?SWC_GROUP, Prefix, Key]).

%% Metadata API
get_metadata(Node, Prefix, Key, Opts) ->
    rpc:call(Node, vmq_swc, get, [?SWC_GROUP, Prefix, Key, Opts]).

put_metadata(Node, Prefix, Key, ValueOrFun, Opts) ->
    rpc:call(Node, vmq_swc, put, [?SWC_GROUP, Prefix, Key, ValueOrFun, Opts]).

delete_metadata(Node, Prefix, Key) ->
    rpc:call(Node, vmq_swc, delete, [?SWC_GROUP, Prefix, Key]).

wait_until_converged(Nodes, Prefix, Key, ExpectedValue) ->
    vmq_swc_test_utils:wait_until(
      fun() ->
              lists:all(
                fun(X) -> X == true end,
                vmq_swc_test_utils:pmap(
                  fun(Node) ->
                          ExpectedValue == get_metadata(Node, Prefix,
                                                        Key,
                                                        [{allow_put,
                                                          false}])
                  end, Nodes))
      end, 60*2, 500).

wait_until_causal_context(Nodes, Prefix, Key, PredicateFun) ->
    vmq_swc_test_utils:wait_until(
      fun() ->
              lists:all(fun(X) -> X == true end,
                        vmq_swc_test_utils:pmap(
                          fun(Node) ->
                                  Val = read(Node, Prefix, Key),
                                  PredicateFun(Node, Val)
                          end, Nodes))
      end, 5*10, 100).

wait_until_sibling(Nodes, Prefix, Key) ->
    vmq_swc_test_utils:wait_until(fun() ->
                lists:all(fun(X) -> X == true end,
                          vmq_swc_test_utils:pmap(fun(Node) ->
                                case read(Node, Prefix, Key) of
                                    undefined -> false;
                                    {Values, _Context} ->
                                        length(Values) > 1
                                end
                        end, Nodes))
end, 60*2, 500).

wait_until_log_gc(Nodes) ->
    vmq_swc_test_utils:wait_until(
      fun() ->
              lists:all(fun(X) -> X == true end,
                        vmq_swc_test_utils:pmap(
                          fun(Node) ->
                                  case rpc:call(Node, vmq_swc_store, dump, [?STORE_NAME]) of
                                      #{log := #{n := 0, key_memory := 0, data_memory := 0, data := []}} -> true;
                                      _ ->
                                          false
                                  end
                          end, Nodes))
      end, 60*2, 500).

dump(Node) ->
    Dump =  rpc:call(Node, vmq_swc_store, dump, [?STORE_NAME]),
    State = rpc:call(Node, sys, get_state, [vmq_swc_store]),
    io:format(user, "~p NC: ~p~nstate: ~p~n", [Node, Dump, State]).
