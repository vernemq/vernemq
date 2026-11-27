-module(vmq_swc_store_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-define(SWC_GROUP, test).
-define(STORE_NAME, list_to_atom("vmq_swc_store_" ++ atom_to_list(?SWC_GROUP))).

%% ===================================================================
%% common_test callbacks
%% ===================================================================

suite() ->
    [
      %% If a test hangs, no need to wait for 30 minutes.
      {timetrap, {minutes, 15}}
    ].

init_per_suite(Config) ->
    Config0 = vmq_swc_test_utils:init_distribution(Config),
    [{swc_group, ?SWC_GROUP}|Config0].

end_per_suite(_Config) ->
    _Config.

init_per_group(rocksdb, Config) ->
    [{db_backend, rocksdb}|Config];
init_per_group(leveled, Config) ->
    [{db_backend, leveled}|Config];
init_per_group(leveldb, Config) ->
    [{db_backend, leveldb},{dkm_backend, vmq_swc_dkm_leveldb}, {dkm_dir, "swc_dkm"} | Config].

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(basic_store_test, Config) ->
    application:load(vmq_swc),
    application:set_env(vmq_swc, init_sync_procedure, false),
    application:set_env(vmq_swc, db_backend, proplists:get_value(db_backend, Config)),
    application:set_env(vmq_swc, dkm_backend, proplists:get_value(dkm_backend, Config)),
    application:set_env(vmq_swc, dkm_dir, proplists:get_value(dkm_dir, Config)),
    ok = vmq_swc_plugin:plugin_start([basic]),
    Config;
init_per_testcase(init_sync_procedure_on_test, Config) ->
    application:load(vmq_swc),
    application:set_env(vmq_swc, init_sync_procedure, true),
    application:set_env(vmq_swc, db_backend, proplists:get_value(db_backend, Config)),
    application:set_env(vmq_swc, dkm_backend, proplists:get_value(dkm_backend, Config)),
    application:set_env(vmq_swc, dkm_dir, proplists:get_value(dkm_dir, Config)),
    ok = vmq_swc_plugin:plugin_start([basic]),
    Config;
init_per_testcase(store_restart_test, Config) ->
    init_per_testcase(basic_store_test, Config);
init_per_testcase(standalone_dkm_gc_test, Config) ->
    init_per_testcase(basic_store_test, Config);
init_per_testcase(bad_convertfun_test, Config) ->
    init_per_testcase(basic_store_test, Config);
init_per_testcase(init_sync_procedure_off_test, Config) ->
    init_per_testcase(basic_store_test, Config);
init_per_testcase(init_sync_persistent_update_test, Config) ->
    init_per_testcase(basic_store_test, Config);
init_per_testcase(partitioned_delete_test = Case, Config0) ->
    Config1 = [{sync_interval, 0}, {auto_gc, true} | Config0],
    init_per_testcase_(Case, Config1, [electra, flail]);
init_per_testcase(normal_sync_tombstone_gc_test = Case, Config0) ->
    Config1 = [{sync_interval, 0}, {auto_gc, false} | Config0],
    init_per_testcase_(Case, Config1, [electra, flail]);
init_per_testcase(normal_sync_restart_test = Case, Config0) ->
    Config1 = [{sync_interval, 0}, {exchange_batch_size, 1} | Config0],
    init_per_testcase_(Case, Config1, [electra, flail]);
init_per_testcase(normal_sync_sparse_clock_test = Case, Config0) ->
    Config1 = [{sync_interval, 0}, {exchange_batch_size, 1} | Config0],
    init_per_testcase_(Case, Config1, [electra, flail]);
init_per_testcase(full_sync_test = Case, Config0) ->
    Config1 = [{exchange_batch_size, 1}|Config0],
    init_per_testcase_(Case, Config1, [electra, katana, flail, gargoyle]);
init_per_testcase(snapshot_bootstrap_large_clock_test = Case, Config0) ->
    Config1 = [{init_sync_procedure, false}, {exchange_batch_size, 1} | Config0],
    init_per_testcase_(Case, Config1, [electra, katana]);
init_per_testcase(snapshot_bootstrap_restart_test = Case, Config0) ->
    Config1 = [{init_sync_procedure, false}, {exchange_batch_size, 1}, {sync_interval, 0} | Config0],
    init_per_testcase_(Case, Config1, [electra, katana]);
init_per_testcase(snapshot_bootstrap_concurrent_writes_test = Case, Config0) ->
    Config1 = [{init_sync_procedure, false}, {exchange_batch_size, 1}, {sync_interval, 0} | Config0],
    init_per_testcase_(Case, Config1, [electra, katana]);
init_per_testcase(snapshot_bootstrap_empty_history_test = Case, Config0) ->
    Config1 = [{init_sync_procedure, false}, {exchange_batch_size, 1}, {sync_interval, 0} | Config0],
    init_per_testcase_(Case, Config1, [electra, katana]);
init_per_testcase(snapshot_bootstrap_local_write_test = Case, Config0) ->
    Config1 = [{init_sync_procedure, false}, {exchange_batch_size, 1}, {sync_interval, 0} | Config0],
    init_per_testcase_(Case, Config1, [electra, katana]);
init_per_testcase(snapshot_bootstrap_concurrent_joins_test = Case, Config0) ->
    Config1 = [{init_sync_procedure, false}, {exchange_batch_size, 1}, {sync_interval, 0} | Config0],
    init_per_testcase_(Case, Config1, [electra, katana, flail]);
init_per_testcase(snapshot_bootstrap_source_not_ready_test = Case, Config0) ->
    Config1 = [{init_sync_procedure, false}, {sync_interval, 0} | Config0],
    init_per_testcase_(Case, Config1, [electra, katana]);
init_per_testcase(Case, Config) ->
    init_per_testcase_(Case, Config, [electra, katana, flail, gargoyle]).

init_per_testcase_(Case, Config, Nodenames) ->
    PeerNodes = vmq_swc_test_utils:pmap(fun(N) ->
                    {ok, Peer, Node} = vmq_swc_test_utils:start_node(N, Config, Case),
                    {Peer, Node}
            end, Nodenames),
    {_Peers, Nodes} = lists:unzip(PeerNodes),
    {ok, _} = ct_cover:add_nodes(Nodes),
    [{nodes, Nodes}, {peer_nodes, PeerNodes}|Config].

end_per_testcase(basic_store_test, _Config) ->
    application:stop(vmq_swc);
end_per_testcase(store_restart_test, _Config) ->
    application:stop(vmq_swc);
end_per_testcase(standalone_dkm_gc_test, _Config) ->
    application:stop(vmq_swc);
end_per_testcase(bad_convertfun_test, _Config) ->
    application:stop(vmq_swc);
end_per_testcase(init_sync_procedure_off_test, _Config) ->
    application:stop(vmq_swc),
    application:unset_env(vmq_swc, init_sync_procedure);
end_per_testcase(init_sync_procedure_on_test, _Config) ->
    application:stop(vmq_swc),
    application:unset_env(vmq_swc, init_sync_procedure);
end_per_testcase(init_sync_persistent_update_test, _Config) ->
    application:stop(vmq_swc);
end_per_testcase(_, Config) ->
    PeerNodes = proplists:get_value(peer_nodes, Config),
    vmq_swc_test_utils:pmap(fun({Peer, Node}) -> vmq_swc_test_utils:stop_peer(Peer, Node) end,
                            PeerNodes),
    ok.

all() ->
    [
     %{group, rocksdb},
     %{group, leveled},
     {group, leveldb}
    ].

groups() ->
    AllTests = [basic_store_test,
                standalone_dkm_gc_test,
                init_sync_procedure_off_test,
                init_sync_procedure_on_test,
                init_sync_persistent_update_test,
                store_restart_test,
                bad_convertfun_test,
                read_write_delete_test,
                partitioned_cluster_test,
                partitioned_delete_test,
                normal_sync_tombstone_gc_test,
                normal_sync_restart_test,
                normal_sync_sparse_clock_test,
                siblings_test,
                cluster_leave_test,
                cluster_join_test,
                events_test,
                full_sync_test,
                snapshot_bootstrap_large_clock_test,
                snapshot_bootstrap_restart_test,
                snapshot_bootstrap_concurrent_writes_test,
                snapshot_bootstrap_empty_history_test,
                snapshot_bootstrap_local_write_test,
                snapshot_bootstrap_concurrent_joins_test,
                snapshot_bootstrap_source_not_ready_test],
    [{rocksdb, [shuffle], AllTests},
     {leveled, [shuffle], AllTests},
     {leveldb, [shuffle], AllTests}].



basic_store_test(_Config) ->
    Prefixes = [{a,a},{a,b},{a,c},{a,d}],
    KVPairsByPrefix =
    lists:foldl(
      fun(I, Acc) ->
              lists:foldl(
                fun(P, AccAcc) ->
                        undefined = vmq_swc:get(basic, P, I, []),
                        ok = vmq_swc:put(basic, P, I, I, []),
                        I = vmq_swc:get(basic, P, I, []),
                        maps:put(P, [{I, I} | maps:get(P, AccAcc, [])], AccAcc)
                end, Acc, Prefixes)
      end, #{}, lists:seq(1, 100)),

    lists:foreach(
      fun(P) ->
              KVsForPrefix = vmq_swc:fold(basic, fun(K,V, Acc) -> [{K, V}|Acc] end, [], P, []),
              ?assertEqual(KVsForPrefix, maps:get(P, KVPairsByPrefix))
        end, Prefixes).

standalone_dkm_gc_test(_Config) ->
    Group = basic,
    StoreName = list_to_atom("vmq_swc_store_" ++ atom_to_list(Group)),
    Prefix = {standalone, dkm_gc},
    Key = key1,
    SKey = sext:encode({Prefix, Key}),

    NodeClock = vmq_swc_store:node_clock_by_storename(StoreName),
    [Id] = swc_node:ids(NodeClock),
    Watermark = vmq_swc_store:watermark_by_storename(StoreName),
    ?assert(lists:member(Id, swc_watermark:peers(Watermark))),

    ok = vmq_swc:put(Group, Prefix, Key, value1, []),
    DKM0 = vmq_swc_store:get_dkm_by_storename(StoreName),
    ?assert(lists:keymember(SKey, 1, maps:get(latest, vmq_swc_dkm:dump(DKM0)))),

    ok = vmq_swc:delete(Group, Prefix, Key),
    ok = vmq_swc_store:do_gc(StoreName, node()),
    DKM1 = vmq_swc_store:get_dkm_by_storename(StoreName),
    ?assertNot(lists:keymember(SKey, 1, maps:get(latest, vmq_swc_dkm:dump(DKM1)))).

init_sync_procedure_off_test(_Config) ->
    Config = vmq_swc:config(basic),
    ?assertEqual(true, vmq_swc_store:get_init_sync(Config)).

init_sync_procedure_on_test(_Config) ->
    Config = vmq_swc:config(basic),
    ?assertEqual(false, vmq_swc_store:get_init_sync(Config)).

init_sync_persistent_update_test(_Config) ->
    Config = vmq_swc:config(basic),
    ?assertEqual(true, vmq_swc_store:get_init_sync(Config)),
    ?assertEqual(ok, vmq_swc_store:set_init_sync_by_groupname(basic, false)),
    ?assertEqual(false, vmq_swc_store:get_init_sync(Config)),
    ?assertEqual(ok, vmq_swc_store:set_init_sync_by_groupname(basic, true)),
    ?assertEqual(true, vmq_swc_store:get_init_sync(Config)).

store_restart_test(_Config) ->
    Group = basic,
    Config = vmq_swc:config(Group),
    StoreName = list_to_atom("vmq_swc_store_" ++ atom_to_list(Group)),
    Prefix = {test, subs},
    Key = key1,
    Value = val1,
    ConvertFun = fun(Evt) -> {swc_evt, Evt} end,
    ok = vmq_swc_store:subscribe(Config, Prefix, ConvertFun),
    StorePid0 = whereis(StoreName),
    ?assert(is_pid(StorePid0)),
    exit(StorePid0, kill),
    _StorePid1 = wait_until_pid(StoreName, StorePid0, 50),
    %% trigger an event, should still be delivered after restart
    ok = vmq_swc:put(Group, Prefix, Key, Value, []),
    receive
        {swc_evt, {updated, Prefix, Key, _OldVals, _NewVals}} -> ok
    after 5000 ->
        ct:fail(timeout)
    end.

bad_convertfun_test(_Config) ->
    Group = basic,
    Config = vmq_swc:config(Group),
    StoreName = list_to_atom("vmq_swc_store_" ++ atom_to_list(Group)),
    Prefix = {test, subs_crash},
    Key = key2,
    Value = val2,
    BadFun = fun(_Evt) -> erlang:error(bad_convertfun) end,
    GoodFun = fun(Evt) -> {good_evt, Evt} end,
    ok = vmq_swc_store:subscribe(Config, Prefix, BadFun),
    ok = vmq_swc_store:subscribe(Config, Prefix, GoodFun),
    StorePid0 = whereis(StoreName),
    MRef = monitor(process, StorePid0),
    ok = vmq_swc:put(Group, Prefix, Key, Value, []),
    receive
        {good_evt, {updated, Prefix, Key, _OldVals, _NewVals}} -> ok
    after 5000 ->
        ct:fail(timeout)
    end,
    receive
        {'DOWN', MRef, process, _, Reason} ->
            ct:fail({store_crashed, Reason})
    after 500 ->
        demonitor(MRef, [flush]),
        ok
    end.

wait_until_pid(Name, OldPid, 0) ->
    Pid = whereis(Name),
    case is_pid(Pid) andalso (Pid =/= OldPid) of
        true -> Pid;
        false -> ct:fail(store_not_restarted)
    end;
wait_until_pid(Name, OldPid, N) ->
    Pid = whereis(Name),
    case is_pid(Pid) andalso (Pid =/= OldPid) of
        true -> Pid;
        false -> ct:sleep(100), wait_until_pid(Name, OldPid, N - 1)
    end.

read_write_delete_test(Config) ->
    [Node1|OtherNodes] = Nodes = proplists:get_value(nodes, Config),    
    % We only have 1 SWC partition, so empty history is {1,0,true} before joining
    ?assertEqual({1,0,true}, rpc:call(Node1, vmq_swc_plugin, history, [[test]])),
    [?assertEqual({1,0,true}, rpc:call(Node, vmq_swc_plugin, history, [[test]]))
    || Node <- OtherNodes],
    [?assertEqual(ok, rpc:call(Node, vmq_swc_peer_service, join, [Node1]))
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
    [?assertEqual(ok, rpc:call(Node, vmq_swc_peer_service, join, [Node1]))
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
    ?assertEqual(ok, rpc:call(Node1, vmq_swc_peer_service, join, [Node2])),
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
        fun(_, {[], _}) -> true;
           (_Node, {_, _} = _CC) ->
                %% io:format(user, "XXX ~p NoValuePred: ~p~n", [Node, CC]),
                false
        end,
    QuuxValuePred =
        fun(_, {[quux], _}) -> true;
           (_Node, _Val) ->
                false
        end,
    NoCCPred =
        fun(_Node, {[], _} = _Val) ->
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
    rpc:call(Node2, erlang, send, [?STORE_NAME, {sync_with, Node1}]),

    %% all the nodes should see the delete and no value should be
    %% present.
    ok = wait_until_causal_context(Nodes, {foo, bar}, baz, NoCCPred),
    %% [dump(N) || N <- Nodes],
    %% io:format(user, "Nodes: ~p~n", [Nodes]),
    ok.

normal_sync_tombstone_gc_test(Config) ->
    [SourceNode, LaggingNode] = Nodes = proplists:get_value(nodes, Config),
    ?assertEqual(ok, rpc:call(LaggingNode, vmq_swc_peer_service, join, [SourceNode])),
    ok = vmq_swc_test_utils:wait_until_joined(Nodes, lists:sort(Nodes)),
    Prefix = normal_sync_gc,
    Key = deleted_key,
    SKey = sext:encode({Prefix, Key}),

    ok = put_metadata(SourceNode, Prefix, Key, value, []),
    ok = wait_until_converged(Nodes, Prefix, Key, value),
    disable_broadcast(Nodes),
    ok = delete_metadata(SourceNode, Prefix, Key),
    ?assertEqual(undefined,
                 get_metadata(SourceNode, Prefix, Key, [{allow_put, false}])),
    ?assertEqual(value,
                 get_metadata(LaggingNode, Prefix, Key, [{allow_put, false}])),

    %% The lagging member has not acknowledged the delete, so neither the
    %% tombstone nor its dot-to-key mapping may be garbage-collected.
    ok = rpc:call(SourceNode, vmq_swc_store, do_gc, [?STORE_NAME, SourceNode]),
    ?assertMatch({ok, _}, rpc:call(SourceNode, vmq_swc_db, get,
                                   [config(SourceNode), obj, SKey])),
    ?assert(dkm_has_key(SourceNode, SKey)),

    %% Pull the tombstone, then let the source observe the receiver's updated
    %% clock. Only after that acknowledgement is GC safe.
    rpc:call(LaggingNode, erlang, send, [?STORE_NAME, {sync_with, SourceNode}]),
    ok = wait_until_converged(Nodes, Prefix, Key, undefined),
    ok = vmq_swc_test_utils:wait_until(
           fun() ->
                   rpc:call(SourceNode, erlang, send,
                            [?STORE_NAME, {sync_with, LaggingNode}]),
                   rpc:call(SourceNode, vmq_swc_store, do_gc,
                            [?STORE_NAME, SourceNode]),
                   rpc:call(SourceNode, vmq_swc_db, get,
                            [config(SourceNode), obj, SKey]) =:= not_found
           end, 60*2, 100),
    ?assertNot(dkm_has_key(SourceNode, SKey)).

normal_sync_restart_test(Config) ->
    [SourceNode, ReceivingNode] = Nodes = proplists:get_value(nodes, Config),
    ?assertEqual(ok, rpc:call(ReceivingNode, vmq_swc_peer_service, join, [SourceNode])),
    ok = vmq_swc_test_utils:wait_until_joined(Nodes, lists:sort(Nodes)),
    disable_broadcast(Nodes),
    Prefix = normal_sync_restart,
    ObjectCount = 1000,
    lists:foreach(fun(I) ->
                          ok = put_metadata(SourceNode, Prefix, I, I, [])
                  end, lists:seq(1, ObjectCount)),
    SourceClock = rpc:call(SourceNode, vmq_swc_store, node_clock, [config(SourceNode)]),

    rpc:call(ReceivingNode, erlang, send, [?STORE_NAME, {sync_with, SourceNode}]),
    ok = vmq_swc_test_utils:wait_until(
           fun() ->
                   Count = metadata_count(ReceivingNode, Prefix),
                   Count > 0 andalso Count < ObjectCount
           end, 60*3, 10),
    restart_remote_store(ReceivingNode),
    rpc:call(ReceivingNode, erlang, send, [?STORE_NAME, {sync_with, SourceNode}]),
    ok = vmq_swc_test_utils:wait_until(
           fun() -> metadata_count(ReceivingNode, Prefix) =:= ObjectCount end,
           60*3, 100),
    ok = wait_until_converged(Nodes, Prefix, 1, 1),
    ok = wait_until_converged(Nodes, Prefix, ObjectCount div 2, ObjectCount div 2),
    ok = wait_until_converged(Nodes, Prefix, ObjectCount, ObjectCount),
    assert_clock_descends(ReceivingNode, SourceClock).

normal_sync_sparse_clock_test(Config) ->
    [SourceNode, ReceivingNode] = Nodes = proplists:get_value(nodes, Config),
    ?assertEqual(ok, rpc:call(ReceivingNode, vmq_swc_peer_service, join, [SourceNode])),
    ok = vmq_swc_test_utils:wait_until_joined(Nodes, lists:sort(Nodes)),
    disable_broadcast(Nodes),
    Prefix = normal_sync_sparse,
    Key = sparse_key,
    SKey = sext:encode({Prefix, Key}),
    SparseId = {'retired-normal-sync-actor', <<"sparse-actor">>},
    SparseCounter = 1_000,
    SparseEntry = {1, 1 bsl (SparseCounter - 2)},
    ExpectedReceivingEntry = {0, 1 bsl (SparseCounter - 1)},
    Dot = {SparseId, SparseCounter},
    SparseObject0 = swc_kv:add(swc_kv:new(), Dot, sparse_value),

    SourceConfig = config(SourceNode),
    SourceClock0 = rpc:call(SourceNode, vmq_swc_store, node_clock, [SourceConfig]),
    SourceClock1 = maps:put(SparseId, SparseEntry, SourceClock0),
    SparseObject = swc_kv:strip(SparseObject0, SourceClock1),
    ok = rpc:call(SourceNode, vmq_swc_db, put_many,
                  [SourceConfig,
                   [{obj, SKey, term_to_binary(SparseObject)},
                    {dkm, sext:encode(Dot), SKey}]]),
    ok = rpc:call(SourceNode, vmq_swc_store, set_nodeclock,
                  [?STORE_NAME, SourceClock1]),
    restart_remote_store(SourceNode),

    rpc:call(ReceivingNode, erlang, send, [?STORE_NAME, {sync_with, SourceNode}]),
    ok = wait_until_converged(Nodes, Prefix, Key, sparse_value),
    ReceivingClock = rpc:call(ReceivingNode, vmq_swc_store, node_clock,
                              [config(ReceivingNode)]),
    %% Normal repair may claim the explicitly transferred high dot, but unlike
    %% a complete bootstrap snapshot it must not claim the unrelated base dot
    %% for which no object was returned.
    ?assertEqual(ExpectedReceivingEntry, swc_node:get(SparseId, ReceivingClock)).

siblings_test(Config) ->
    [Node1|OtherNodes] = Nodes = proplists:get_value(nodes, Config),
    [?assertEqual(ok, rpc:call(Node, vmq_swc_peer_service, join, [Node1]))
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
    ResolverFun = fun ?MODULE:resolver/1,
    spork = get_metadata(Node1, {foo, bar}, baz, [{resolver, ResolverFun}, {allow_put, false}]),
    %% without allow_put set, all the siblings are still there...
    ok = wait_until_sibling(Nodes, {foo, bar}, baz),
    %% resolve the sibling and write it back
    spork = get_metadata(Node1, {foo, bar}, baz, [{resolver, ResolverFun}, {allow_put, true}]),
    %% check all the nodes see the resolution
    ok = wait_until_converged(Nodes, {foo, bar}, baz, spork),
    ok.

cluster_join_test(Config) ->
    % we form two clusters, fill them with data, let the log GC cleanup
    % all history and let them join, as a result all nodes should have all the data.
    [OtherNode, Node1 | OtherNodes] = AllNodes = proplists:get_value(nodes, Config),
    [?assertEqual(ok, rpc:call(Node, vmq_swc_peer_service, join, [Node1]))
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

    % join the two clusters
    ?assertEqual(ok, rpc:call(OtherNode, vmq_swc_peer_service, join, [Node1])),
    Expected1 = lists:sort(AllNodes),
    ok = vmq_swc_test_utils:wait_until_joined(AllNodes, Expected1),
    ok = wait_until_converged(AllNodes, {foo, bar}, baz, quux),
    ok = wait_until_converged(AllNodes, {foo, bar}, canary, 1).

cluster_leave_test(Config) ->
    [Node1|OtherNodes] = Nodes = proplists:get_value(nodes, Config),
    [?assertEqual(ok, rpc:call(Node, vmq_swc_peer_service, join, [Node1]))
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
    ok = rpc:call(Node1, vmq_swc_peer_service, leave, [[]]),
    [Node2|_] = Nodes1 = Nodes -- [Node1],

    % put some new data
    ok = put_metadata(Node2, {foo, bar}, hello, world, []),
    ok = wait_until_converged(Nodes1, {foo, bar}, hello, world).

events_test(Config) ->
    [Node1|OtherNodes] = Nodes = proplists:get_value(nodes, Config),
    [?assertEqual(ok, rpc:call(Node, vmq_swc_peer_service, join, [Node1]))
     || Node <- OtherNodes],
    Expected = lists:sort(Nodes),
    ok = vmq_swc_test_utils:wait_until_joined(Nodes, Expected),
    [?assertEqual({Node, Expected}, {Node,
                                     lists:sort(vmq_swc_test_utils:get_cluster_members(Node))})
     || Node <- Nodes],

    T = ets:new(test_swc_events, [named_table, public, {write_concurrency, true}]),

    Myself = node(),
    Fun = fun ?MODULE:event_subscribe/1,
    [begin
         SwcGroupConfig = config(Node),
         rpc:call(Node, persistent_term, put, [main_test_node, Myself]),
         rpc:call(Node, vmq_swc_store, subscribe, [SwcGroupConfig, rand, Fun])
     end || Node <- Nodes],

    lists:foreach(fun(I) ->
                          RandNode = lists:nth(rand:uniform(length(Nodes)), Nodes),
                          ok = put_metadata(RandNode, rand, I, I, [])
                  end, lists:seq(1, 1000)),

    ok =
        vmq_swc_test_utils:wait_until(fun() ->
                      ets:info(test_swc_events, size) == length(Nodes) * 1000
                   end, 60*2, 500),
    ct:pal("ETS table size after Put: ~p~n", [ets:info(test_swc_events, size)]),

    lists:foreach(fun(I) ->
                          RandNode = lists:nth(rand:uniform(length(Nodes)), Nodes),
                          ok = delete_metadata(RandNode, rand, I)
                  end, lists:seq(1, 1000)),
    ok = vmq_swc_test_utils:wait_until(fun() -> ets:info(T, size) == 0 end, 60*2, 500),
    ct:pal("ETS table size after Delete 2: ~p~n", [ets:info(T, size)]),
    ok.

full_sync_test(Config) ->
    [LastNode|Nodes] = proplists:get_value(nodes, Config),
    [Node1|OtherNodes] = Nodes,
    [?assertEqual(ok, rpc:call(Node, vmq_swc_peer_service, join, [Node1]))
     || Node <- OtherNodes],

    Expected = lists:sort(Nodes),
    ok = vmq_swc_test_utils:wait_until_joined(Nodes, Expected),
    [?assertEqual({Node, Expected}, {Node,
                                     lists:sort(vmq_swc_test_utils:get_cluster_members(Node))})
     || Node <- Nodes],
    % at this point the cluster is fully clustered with the exception of LastNode
    Objects = [{crypto:strong_rand_bytes(100), I} || I <- lists:seq(1,1000)], % use something where insertion order doesn't reflect key ordering.
    lists:foreach(fun({Key, Val}) ->
                          RandNode = lists:nth(rand:uniform(length(Nodes)), Nodes),
                          ok = put_metadata(RandNode, rand, Key, Val, [])
                  end, Objects),

    lists:foreach(fun({Key, Val}) ->
                           ok = wait_until_converged(Nodes, rand, Key, Val)
                   end, Objects),

    % let's join the LastNode,
    ?assertEqual(ok, rpc:call(LastNode, vmq_swc_peer_service, join, [Node1])),

    % insert some more entries while joining the cluster
    Objects1 = [{crypto:strong_rand_bytes(100), I + 100} || I <- lists:seq(1,100)], % use something where insertion order doesn't reflect key ordering.
    lists:foreach(fun({Key, Val}) ->
                          RandNode = lists:nth(rand:uniform(length(Nodes)), Nodes),
                          ok = put_metadata(RandNode, rand, Key, Val, [])
                  end, Objects1),

    lists:foreach(fun({Key, Val}) ->
                          ok = wait_until_converged([LastNode|Nodes], rand, Key, Val)
                  end, Objects ++ Objects1).

snapshot_bootstrap_large_clock_test(Config) ->
    [JoiningNode, SourceNode] = Nodes = proplists:get_value(nodes, Config),
    ok = rpc:call(SourceNode, vmq_swc_group_coordinator, flag_as_init_synced, []),

    %% Build the causal shape of a node joining millions of writes behind
    %% without making the test perform millions of writes.
    SourceId = set_local_counter(SourceNode, 5_000_000),
    ok = put_metadata(SourceNode, bootstrap, first, value1, []),
    ok = put_metadata(SourceNode, bootstrap, second, value2, []),

    %% With the deprecated eager setting off, the node starts as a normal
    %% standalone node. The join operation must activate bootstrap itself.
    ?assertEqual(true, rpc:call(JoiningNode, vmq_swc_store, get_init_sync,
                                [config(JoiningNode)])),
    ?assertEqual(ok, rpc:call(JoiningNode, vmq_swc_peer_service, join, [SourceNode])),
    Expected = lists:sort(Nodes),
    ok = vmq_swc_test_utils:wait_until_joined(Nodes, Expected),
    ok = wait_until_converged(Nodes, bootstrap, first, value1),
    ok = wait_until_converged(Nodes, bootstrap, second, value2),

    ?assert(is_pid(rpc:call(JoiningNode, erlang, whereis, [?STORE_NAME]))),
    JoiningClock = rpc:call(JoiningNode, vmq_swc_store, node_clock, [config(JoiningNode)]),
    {Counter, 0} = swc_node:get(SourceId, JoiningClock),
    ?assert(Counter >= 5_000_002),
    ?assertEqual(not_found, bootstrap_marker(JoiningNode)).

snapshot_bootstrap_restart_test(Config) ->
    [JoiningNode, SourceNode] = Nodes = proplists:get_value(nodes, Config),
    ok = rpc:call(SourceNode, vmq_swc_group_coordinator, flag_as_init_synced, []),
    lists:foreach(fun(I) ->
                          ok = put_metadata(SourceNode, bootstrap_restart, I, I, [])
                  end, lists:seq(1, 500)),

    Parent = self(),
    spawn(fun() ->
                  Parent ! {join_result,
                            rpc:call(JoiningNode, vmq_swc_peer_service, join, [SourceNode])}
          end),
    ok = vmq_swc_test_utils:wait_until(
           fun() ->
                   bootstrap_marker(JoiningNode) =/= not_found andalso
                   get_metadata(JoiningNode, bootstrap_restart, 250,
                                [{allow_put, false}]) =:= 250 andalso
                   rpc:call(JoiningNode, vmq_swc_store, get_init_sync,
                            [config(JoiningNode)]) =:= false
           end, 60*3, 10),

    StoreSupId = {vmq_swc_store_sup, ?SWC_GROUP},
    ok = rpc:call(JoiningNode, supervisor, terminate_child, [vmq_swc_sup, StoreSupId]),
    ok = vmq_swc_test_utils:wait_until(
           fun() -> undefined =:= rpc:call(JoiningNode, erlang, whereis, [?STORE_NAME]) end,
           60, 100),
    ok = rpc:call(JoiningNode, application, set_env,
                  [vmq_swc, init_sync_procedure, false]),
    {ok, _} = rpc:call(JoiningNode, supervisor, restart_child, [vmq_swc_sup, StoreSupId]),
    ok = vmq_swc_test_utils:wait_until(
           fun() -> is_pid(rpc:call(JoiningNode, erlang, whereis, [?STORE_NAME])) end,
           60, 100),

    %% The durable marker takes precedence over the now-disabled setting.
    ?assertEqual(false, rpc:call(JoiningNode, vmq_swc_store, get_init_sync,
                                 [config(JoiningNode)])),
    rpc:call(JoiningNode, erlang, send, [?STORE_NAME, {sync_with, SourceNode}]),
    receive
        {join_result, JoinResult} -> ?assertEqual(ok, JoinResult)
    after 300000 ->
        ct:fail(join_timeout)
    end,
    Expected = lists:sort(Nodes),
    ok = vmq_swc_test_utils:wait_until_joined(Nodes, Expected),
    ok = wait_until_converged(Nodes, bootstrap_restart, 1, 1),
    ok = wait_until_converged(Nodes, bootstrap_restart, 250, 250),
    ok = wait_until_converged(Nodes, bootstrap_restart, 500, 500),
    ?assertEqual(not_found, bootstrap_marker(JoiningNode)),
    ?assertEqual(true, rpc:call(JoiningNode, vmq_swc_store, get_init_sync,
                                [config(JoiningNode)])).

snapshot_bootstrap_concurrent_writes_test(Config) ->
    [JoiningNode, SourceNode] = Nodes = proplists:get_value(nodes, Config),
    ok = rpc:call(SourceNode, vmq_swc_group_coordinator, flag_as_init_synced, []),
    InitialObjects = [{I, I} || I <- lists:seq(1, 500)],
    lists:foreach(fun({Key, Val}) ->
                          ok = put_metadata(SourceNode, bootstrap_concurrent, Key, Val, [])
                  end, InitialObjects),

    Parent = self(),
    spawn(fun() ->
                  Parent ! {join_result,
                            rpc:call(JoiningNode, vmq_swc_peer_service, join, [SourceNode])}
          end),
    ok = vmq_swc_test_utils:wait_until(
           fun() ->
                   get_metadata(JoiningNode, bootstrap_concurrent, 250,
                                [{allow_put, false}]) =:= 250 andalso
                   rpc:call(JoiningNode, vmq_swc_store, get_init_sync,
                            [config(JoiningNode)]) =:= false
           end,
           60*3, 10),

    %% These writes happen after the snapshot clock was captured. Some can be
    %% observed by the scan and the rest must be collected by anti-entropy.
    ok = put_metadata(SourceNode, bootstrap_concurrent, 1, updated, []),
    ConcurrentObjects = [{I, I} || I <- lists:seq(501, 525)],
    lists:foreach(fun({Key, Val}) ->
                          ok = put_metadata(SourceNode, bootstrap_concurrent, Key, Val, [])
                  end, ConcurrentObjects),

    receive
        {join_result, JoinResult} -> ?assertEqual(ok, JoinResult)
    after 300000 ->
        ct:fail(join_timeout)
    end,
    Expected = lists:sort(Nodes),
    ok = vmq_swc_test_utils:wait_until_joined(Nodes, Expected),
    rpc:call(JoiningNode, erlang, send, [?STORE_NAME, {sync_with, SourceNode}]),
    ok = wait_until_converged(Nodes, bootstrap_concurrent, 1, updated),
    lists:foreach(fun({Key, Val}) ->
                          ok = wait_until_converged(Nodes, bootstrap_concurrent, Key, Val)
                  end, ConcurrentObjects),
    ok = wait_until_converged(Nodes, bootstrap_concurrent, 250, 250),
    ?assertEqual(true, rpc:call(JoiningNode, vmq_swc_store, get_init_sync,
                                [config(JoiningNode)])).

snapshot_bootstrap_empty_history_test(Config) ->
    [JoiningNode, SourceNode] = Nodes = proplists:get_value(nodes, Config),
    ok = rpc:call(SourceNode, vmq_swc_group_coordinator, flag_as_init_synced, []),
    Prefix = bootstrap_empty_history,
    Key = deleted_key,
    SKey = sext:encode({Prefix, Key}),

    ok = put_metadata(SourceNode, Prefix, Key, value, []),
    ok = delete_metadata(SourceNode, Prefix, Key),
    ok = rpc:call(SourceNode, vmq_swc_store, do_gc, [?STORE_NAME, SourceNode]),
    ?assertEqual(not_found,
                 rpc:call(SourceNode, vmq_swc_db, get,
                          [config(SourceNode), obj, SKey])),
    SourceClock = rpc:call(SourceNode, vmq_swc_store, node_clock, [config(SourceNode)]),

    Parent = self(),
    spawn(fun() ->
                  Parent ! {join_result,
                            rpc:call(JoiningNode, vmq_swc_peer_service, join, [SourceNode])}
          end),
    InitResult = vmq_swc_test_utils:wait_until(
                   fun() ->
                           rpc:call(JoiningNode, persistent_term, get,
                                    [{vmq_swc_store, init_sync, ?SWC_GROUP}, false]) =:= true
                   end, 100, 100),
    case InitResult of
        ok -> ok;
        _ ->
            StorePid = rpc:call(JoiningNode, erlang, whereis, [?STORE_NAME]),
            StoreInfo = rpc:call(JoiningNode, erlang, process_info,
                                 [StorePid, [current_function, current_stacktrace,
                                             message_queue_len, messages]]),
            ExchangeSup = list_to_atom("vmq_swc_exchange_sup_" ++ atom_to_list(?SWC_GROUP)),
            Exchanges = rpc:call(JoiningNode, supervisor, which_children, [ExchangeSup]),
            ct:fail({bootstrap_not_completed, bootstrap_marker(JoiningNode),
                     bootstrap_peer_marker(JoiningNode), StoreInfo, Exchanges})
    end,
    receive
        {join_result, JoinResult} -> ?assertEqual(ok, JoinResult)
    after 300000 ->
        ct:fail(join_timeout)
    end,
    ok = vmq_swc_test_utils:wait_until_joined(Nodes, lists:sort(Nodes)),
    ?assertEqual(undefined, get_metadata(JoiningNode, Prefix, Key, [{allow_put, false}])),
    assert_clock_descends(JoiningNode, SourceClock),
    ?assertEqual(not_found, bootstrap_marker(JoiningNode)),
    ?assertEqual(not_found, bootstrap_peer_marker(JoiningNode)),

    StorePid0 = rpc:call(JoiningNode, erlang, whereis, [?STORE_NAME]),
    true = rpc:call(JoiningNode, erlang, exit, [StorePid0, kill]),
    _StorePid1 = wait_until_remote_pid(JoiningNode, ?STORE_NAME, StorePid0, 100),
    ?assertEqual(true, rpc:call(JoiningNode, vmq_swc_store, get_init_sync,
                                [config(JoiningNode)])),
    assert_clock_descends(JoiningNode, SourceClock),
    ?assertEqual(not_found, bootstrap_marker(JoiningNode)),
    ?assertEqual(not_found, bootstrap_peer_marker(JoiningNode)).

snapshot_bootstrap_local_write_test(Config) ->
    [JoiningNode, SourceNode] = Nodes = proplists:get_value(nodes, Config),
    ok = rpc:call(SourceNode, vmq_swc_group_coordinator, flag_as_init_synced, []),
    lists:foreach(fun(I) ->
                          ok = put_metadata(SourceNode, bootstrap_local_write, I, I, [])
                  end, lists:seq(1, 1000)),

    Parent = self(),
    spawn(fun() ->
                  Parent ! {join_result,
                            rpc:call(JoiningNode, vmq_swc_peer_service, join, [SourceNode])}
          end),
    ok = vmq_swc_test_utils:wait_until(
           fun() ->
                   get_metadata(JoiningNode, bootstrap_local_write, 10,
                                [{allow_put, false}]) =:= 10 andalso
                   rpc:call(JoiningNode, vmq_swc_store, get_init_sync,
                            [config(JoiningNode)]) =:= false
           end, 60*3, 10),

    LocalClock0 = rpc:call(JoiningNode, vmq_swc_store, node_clock, [config(JoiningNode)]),
    LocalId = local_id(JoiningNode, LocalClock0),
    ok = put_metadata(JoiningNode, bootstrap_local_write, local_only, local_value, []),
    LocalClock1 = rpc:call(JoiningNode, vmq_swc_store, node_clock, [config(JoiningNode)]),
    LocalEntry = swc_node:get(LocalId, LocalClock1),
    %% Make the interleaving explicit: the write completed while bootstrap was
    %% still active, after at least one snapshot batch had been imported.
    ?assertEqual(false, rpc:call(JoiningNode, vmq_swc_store, get_init_sync,
                                 [config(JoiningNode)])),

    receive
        {join_result, JoinResult} -> ?assertEqual(ok, JoinResult)
    after 300000 ->
        ct:fail(join_timeout)
    end,
    ok = vmq_swc_test_utils:wait_until_joined(Nodes, lists:sort(Nodes)),
    FinalClock = rpc:call(JoiningNode, vmq_swc_store, node_clock, [config(JoiningNode)]),
    ?assertEqual(LocalEntry, swc_node:get(LocalId, FinalClock)),
    rpc:call(SourceNode, erlang, send, [?STORE_NAME, {sync_with, JoiningNode}]),
    ok = wait_until_converged(Nodes, bootstrap_local_write, local_only, local_value).

snapshot_bootstrap_concurrent_joins_test(Config) ->
    [SourceNode, JoiningNodeX, JoiningNodeZ] = Nodes = proplists:get_value(nodes, Config),
    ok = rpc:call(SourceNode, vmq_swc_group_coordinator, flag_as_init_synced, []),
    lists:foreach(fun(I) ->
                          ok = put_metadata(SourceNode, concurrent_joins, I, I, [])
                  end, lists:seq(1, 500)),

    Parent = self(),
    lists:foreach(fun(JoiningNode) ->
                          spawn(fun() ->
                                        Result = rpc:call(JoiningNode, vmq_swc_peer_service,
                                                          join, [SourceNode]),
                                        Parent ! {join_result, JoiningNode, Result}
                                end)
                  end, [JoiningNodeX, JoiningNodeZ]),

    lists:foreach(fun(JoiningNode) ->
                          ok = vmq_swc_test_utils:wait_until(
                                 fun() ->
                                         case bootstrap_peer_marker(JoiningNode) of
                                             {ok, PeerBin} ->
                                                 binary_to_term(PeerBin) =:= SourceNode;
                                             _ ->
                                                 false
                                         end
                                 end, 60*3, 10)
                  end, [JoiningNodeX, JoiningNodeZ]),

    lists:foreach(fun(JoiningNode) ->
                          receive
                              {join_result, JoiningNode, JoinResult} ->
                                  ?assertEqual(ok, JoinResult)
                          after 300000 ->
                              ct:fail({join_timeout, JoiningNode})
                          end
                  end, [JoiningNodeX, JoiningNodeZ]),
    Expected = lists:sort(Nodes),
    ok = vmq_swc_test_utils:wait_until_joined(Nodes, Expected),
    ok = wait_until_converged(Nodes, concurrent_joins, 1, 1),
    ok = wait_until_converged(Nodes, concurrent_joins, 250, 250),
    ok = wait_until_converged(Nodes, concurrent_joins, 500, 500).

snapshot_bootstrap_source_not_ready_test(Config) ->
    [SourceNode, JoiningNode] = proplists:get_value(nodes, Config),
    ok = vmq_swc_test_utils:wait_until(
        fun() ->
            rpc:call(SourceNode, vmq_swc_group_coordinator, is_ready, []) =:= true
        end,
        60,
        10
    ),
    ?assertEqual(1, swc_initialized_metric(SourceNode)),
    ?assertEqual(
        ok,
        rpc:call(
            SourceNode,
            vmq_swc_store,
            prepare_snapshot_bootstrap,
            [config(SourceNode), JoiningNode]
        )
    ),
    ok = vmq_swc_test_utils:wait_until(
        fun() ->
            SyncState = rpc:call(SourceNode, vmq_swc_group_coordinator, sync_state, []),
            maps:get(?SWC_GROUP, SyncState) =:= false
        end,
        60,
        10
    ),
    ?assertEqual(false, rpc:call(SourceNode, vmq_swc_group_coordinator, is_ready, [])),
    ?assertEqual(0, swc_initialized_metric(SourceNode)),

    ?assertEqual(
        {error, bootstrap_source_not_ready},
        rpc:call(JoiningNode, vmq_swc_peer_service, join, [SourceNode])
    ),
    ?assertEqual(not_found, bootstrap_marker(JoiningNode)),
    ?assertEqual(not_found, bootstrap_peer_marker(JoiningNode)),
    ?assertEqual(
        true,
        rpc:call(JoiningNode, vmq_swc_store, get_init_sync, [config(JoiningNode)])
    ),
    ?assertEqual(true, rpc:call(JoiningNode, vmq_swc_group_coordinator, is_ready, [])),
    ?assertEqual(1, swc_initialized_metric(JoiningNode)),
    ?assertEqual(
        [JoiningNode],
        rpc:call(
            JoiningNode,
            vmq_swc_group_membership,
            get_members,
            [config(JoiningNode)]
        )
    ).

disable_broadcast(Nodes) ->
    [ok = rpc:call(N, vmq_swc_store, set_broadcast, [config(N), false])
     || N <- Nodes].

broadcast(_Objects, _Peers) ->
    %% drop!
    ok.

config(Node) ->
    rpc:call(Node, vmq_swc, config, [?SWC_GROUP]).

metadata_count(Node, Prefix) ->
    rpc:call(Node, vmq_swc, fold,
             [?SWC_GROUP, fun(_Key, _Value, Acc) -> Acc + 1 end, 0, Prefix,
              [{allow_put, false}]]).

dkm_has_key(Node, SKey) ->
    DKM = rpc:call(Node, vmq_swc_store, get_dkm_by_storename, [?STORE_NAME]),
    Dump = rpc:call(Node, vmq_swc_dkm, dump, [DKM]),
    lists:any(
      fun(Table) -> lists:keymember(SKey, 1, maps:get(Table, Dump)) end,
      [latest, latest_candidates, gc_candidates]).

restart_remote_store(Node) ->
    StoreSupId = {vmq_swc_store_sup, ?SWC_GROUP},
    ok = rpc:call(Node, supervisor, terminate_child, [vmq_swc_sup, StoreSupId]),
    ok = vmq_swc_test_utils:wait_until(
           fun() -> undefined =:= rpc:call(Node, erlang, whereis, [?STORE_NAME]) end,
           60, 100),
    {ok, _} = rpc:call(Node, supervisor, restart_child, [vmq_swc_sup, StoreSupId]),
    ok = vmq_swc_test_utils:wait_until(
           fun() -> is_pid(rpc:call(Node, erlang, whereis, [?STORE_NAME])) end,
           60, 100).

set_local_counter(Node, Counter) ->
    Config = config(Node),
    Clock0 = rpc:call(Node, vmq_swc_store, node_clock, [Config]),
    Id = local_id(Node, Clock0),
    Clock1 = swc_node:store_entry(Id, {Counter, 0}, Clock0),
    ok = rpc:call(Node, vmq_swc_store, set_nodeclock, [?STORE_NAME, Clock1]),
    Id.

local_id(Node, Clock) ->
    [Id] = [Id || {Peer, _Actor} = Id <- swc_node:ids(Clock), Peer =:= Node],
    Id.

assert_clock_descends(Node, ExpectedClock) ->
    Clock = rpc:call(Node, vmq_swc_store, node_clock, [config(Node)]),
    ?assertEqual(Clock, swc_node:merge(Clock, ExpectedClock)).

wait_until_remote_pid(_Node, _Name, _OldPid, 0) ->
    ct:fail(store_not_restarted);
wait_until_remote_pid(Node, Name, OldPid, N) ->
    Pid = rpc:call(Node, erlang, whereis, [Name]),
    case is_pid(Pid) andalso (Pid =/= OldPid) of
        true -> Pid;
        false ->
            ct:sleep(100),
            wait_until_remote_pid(Node, Name, OldPid, N - 1)
    end.

bootstrap_marker(Node) ->
    rpc:call(Node, vmq_swc_db, get, [config(Node), default, <<"IBS">>]).

bootstrap_peer_marker(Node) ->
    rpc:call(Node, vmq_swc_db, get, [config(Node), default, <<"IBP">>]).

swc_initialized_metric(Node) ->
    Metrics = rpc:call(Node, vmq_swc_metrics, metrics, []),
    {gauge, [], swc_initialized, swc_initialized, _Description, Value} =
        lists:keyfind(swc_initialized, 4, Metrics),
    Value.


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
                          Tmp = get_metadata(Node, Prefix,
                                                        Key,
                                                        [{allow_put,
                                                          false}]),
                          case ExpectedValue == Tmp of
                              true -> true;
                              false ->
                                  false
                          end
                  end, Nodes))
      end, 60*3, 500).

wait_until_causal_context(Nodes, Prefix, Key, PredicateFun) ->
    vmq_swc_test_utils:wait_until(
      fun() ->
              lists:all(fun(X) -> X == true end,
                        vmq_swc_test_utils:pmap(
                          fun(Node) ->
                                  Val = read(Node, Prefix, Key),
                                  PredicateFun(Node, Val)
                          end, Nodes))
      end, 60*2, 500).

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

event_subscribe({updated, rand, Key, _OldValues, NewValues}) ->
    MainNode = persistent_term:get(main_test_node),
    rpc:call(MainNode, ets, insert, [test_swc_events, {{node(), Key}, NewValues}]);
event_subscribe({deleted, rand, Key, _OldValues}) ->
    MainNode = persistent_term:get(main_test_node),
    rpc:call(MainNode, ets, delete, [test_swc_events, {node(), Key}]).

resolver(_Object) ->
    spork.
