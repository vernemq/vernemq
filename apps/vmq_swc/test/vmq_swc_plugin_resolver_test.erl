-module(vmq_swc_plugin_resolver_test).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test cases for subscription_resolver/1
%%====================================================================

single_sibling_passthrough_test() ->
    Val = {{1000, 0, 0}, [{node1, true, [{<<"topic/a">>, {1, 0}}]}]},
    ?assertEqual(Val, vmq_swc_plugin:subscription_resolver([Val])).

empty_list_returns_undefined_test() ->
    ?assertEqual(undefined, vmq_swc_plugin:subscription_resolver([])).

disjoint_topics_same_node_test() ->
    V1 = {{1000, 0, 0}, [{node1, true, [{<<"topic/a">>, {1, 0}}]}]},
    V2 = {{1001, 0, 0}, [{node1, true, [{<<"topic/b">>, {0, 0}}]}]},
    {Ts, Subs} = vmq_swc_plugin:subscription_resolver([V1, V2]),
    ?assertEqual({1001, 0, 0}, Ts),
    [{node1, true, Topics}] = Subs,
    ?assertEqual(
        [{<<"topic/a">>, {1, 0}}, {<<"topic/b">>, {0, 0}}],
        lists:sort(Topics)
    ).

overlapping_topics_dedup_test() ->
    V1 = {{1000, 0, 0}, [{node1, true, [{<<"topic/a">>, {1, 0}}, {<<"topic/b">>, {0, 0}}]}]},
    V2 = {{1001, 0, 0}, [{node1, true, [{<<"topic/a">>, {1, 0}}, {<<"topic/c">>, {2, 0}}]}]},
    {_Ts, Subs} = vmq_swc_plugin:subscription_resolver([V1, V2]),
    [{node1, true, Topics}] = Subs,
    ?assertEqual(3, length(Topics)),
    ?assertMatch({_, _}, lists:keyfind(<<"topic/a">>, 1, Topics)),
    ?assertMatch({_, _}, lists:keyfind(<<"topic/b">>, 1, Topics)),
    ?assertMatch({_, _}, lists:keyfind(<<"topic/c">>, 1, Topics)).

different_nodes_both_preserved_test() ->
    V1 = {{1000, 0, 0}, [{node1, true, [{<<"topic/a">>, {1, 0}}]}]},
    V2 = {{1001, 0, 0}, [{node2, false, [{<<"topic/b">>, {0, 0}}]}]},
    {_Ts, Subs} = vmq_swc_plugin:subscription_resolver([V1, V2]),
    ?assertEqual(2, length(Subs)),
    ?assertMatch({_, _, _}, lists:keyfind(node1, 1, Subs)),
    ?assertMatch({_, _, _}, lists:keyfind(node2, 1, Subs)).

clean_session_false_wins_test() ->
    V1 = {{1000, 0, 0}, [{node1, false, [{<<"topic/a">>, {1, 0}}]}]},
    V2 = {{1001, 0, 0}, [{node1, true, [{<<"topic/b">>, {0, 0}}]}]},
    {_Ts, Subs} = vmq_swc_plugin:subscription_resolver([V1, V2]),
    [{node1, CS, _}] = Subs,
    ?assertEqual(false, CS).

clean_session_both_true_test() ->
    V1 = {{1000, 0, 0}, [{node1, true, [{<<"topic/a">>, {1, 0}}]}]},
    V2 = {{1001, 0, 0}, [{node1, true, [{<<"topic/b">>, {0, 0}}]}]},
    {_Ts, Subs} = vmq_swc_plugin:subscription_resolver([V1, V2]),
    [{node1, CS, _}] = Subs,
    ?assertEqual(true, CS).

tombstone_sibling_filtering_test() ->
    V1 = {{1000, 0, 0}, [{node1, true, [{<<"topic/a">>, {1, 0}}]}]},
    V2 = {{1001, 0, 0}, '$deleted'},
    {_Ts, Subs} = vmq_swc_plugin:subscription_resolver([V1, V2]),
    ?assertEqual([{node1, true, [{<<"topic/a">>, {1, 0}}]}], Subs).

all_tombstones_fallback_to_lww_test() ->
    V1 = {{1000, 0, 0}, '$deleted'},
    V2 = {{1001, 0, 0}, '$deleted'},
    Result = vmq_swc_plugin:subscription_resolver([V1, V2]),
    ?assertEqual(V2, Result).

%%====================================================================
%% Test cases for feature flag toggle
%%====================================================================

feature_flag_lww_test() ->
    application:set_env(vmq_swc, subscription_resolver, lww),
    try
        Resolver = vmq_swc_plugin:resolver_for_prefix({vmq, subscriber}),
        V1 = {{1000, 0, 0}, [{node1, true, [{<<"topic/a">>, {1, 0}}]}]},
        V2 = {{1001, 0, 0}, [{node1, true, [{<<"topic/b">>, {0, 0}}]}]},
        Result = Resolver([V1, V2]),
        %% LWW should pick the newest timestamp, not merge
        ?assertEqual(V2, Result)
    after
        application:unset_env(vmq_swc, subscription_resolver)
    end.

feature_flag_set_union_test() ->
    application:set_env(vmq_swc, subscription_resolver, set_union),
    try
        Resolver = vmq_swc_plugin:resolver_for_prefix({vmq, subscriber}),
        V1 = {{1000, 0, 0}, [{node1, true, [{<<"topic/a">>, {1, 0}}]}]},
        V2 = {{1001, 0, 0}, [{node1, true, [{<<"topic/b">>, {0, 0}}]}]},
        {_Ts, Subs} = Resolver([V1, V2]),
        [{node1, true, Topics}] = Subs,
        ?assertEqual(2, length(Topics))
    after
        application:unset_env(vmq_swc, subscription_resolver)
    end.

non_subscriber_prefix_uses_lww_test() ->
    Resolver = vmq_swc_plugin:resolver_for_prefix({vmq, config}),
    V1 = {{1000, 0, 0}, some_value},
    V2 = {{1001, 0, 0}, other_value},
    ?assertEqual(V2, Resolver([V1, V2])).

%%====================================================================
%% Test cases for three or more siblings
%%====================================================================

three_siblings_merge_test() ->
    V1 = {{1000, 0, 0}, [{node1, true, [{<<"topic/a">>, {1, 0}}]}]},
    V2 = {{1001, 0, 0}, [{node1, true, [{<<"topic/b">>, {0, 0}}]}]},
    V3 = {{1002, 0, 0}, [{node1, true, [{<<"topic/c">>, {2, 0}}]}]},
    {Ts, Subs} = vmq_swc_plugin:subscription_resolver([V1, V2, V3]),
    ?assertEqual({1002, 0, 0}, Ts),
    [{node1, true, Topics}] = Subs,
    ?assertEqual(3, length(Topics)).

%%====================================================================
%% Test cases for complex merge scenarios
%%====================================================================

multi_node_values_in_both_siblings_test() ->
    V1 = {{1000, 0, 0}, [
        {node1, true, [{<<"t/1">>, {1, 0}}]},
        {node2, false, [{<<"t/2">>, {0, 0}}]}
    ]},
    V2 = {{1001, 0, 0}, [
        {node1, true, [{<<"t/3">>, {1, 0}}]},
        {node3, true, [{<<"t/4">>, {2, 0}}]}
    ]},
    {_Ts, Subs} = vmq_swc_plugin:subscription_resolver([V1, V2]),
    ?assertEqual(3, length(Subs)),
    ?assertMatch({_, _, _}, lists:keyfind(node1, 1, Subs)),
    ?assertMatch({_, _, _}, lists:keyfind(node2, 1, Subs)),
    ?assertMatch({_, _, _}, lists:keyfind(node3, 1, Subs)),
    {node1, true, N1Topics} = lists:keyfind(node1, 1, Subs),
    ?assertEqual(2, length(N1Topics)).

empty_topics_node_merge_test() ->
    V1 = {{1000, 0, 0}, [{node1, true, []}]},
    V2 = {{1001, 0, 0}, [{node1, true, [{<<"topic/a">>, {1, 0}}]}]},
    {_Ts, Subs} = vmq_swc_plugin:subscription_resolver([V1, V2]),
    [{node1, true, Topics}] = Subs,
    ?assertEqual([{<<"topic/a">>, {1, 0}}], Topics).

same_topic_different_qos_test() ->
    V1 = {{1000, 0, 0}, [{node1, true, [{<<"topic/a">>, {0, 0}}]}]},
    V2 = {{1001, 0, 0}, [{node1, true, [{<<"topic/a">>, {2, 0}}]}]},
    {_Ts, Subs} = vmq_swc_plugin:subscription_resolver([V1, V2]),
    [{node1, true, Topics}] = Subs,
    %% ukeymerge keeps one entry per topic key; exact QoS is deterministic
    ?assertEqual(1, length(Topics)).

unsorted_nodes_merge_test() ->
    S1 = [{node_z, true, [{<<"a">>, {0, 0}}]}, {node_a, true, [{<<"b">>, {1, 0}}]}],
    S2 = [{node_m, false, [{<<"c">>, {2, 0}}]}],
    Result = vmq_swc_plugin:merge_subs(S1, S2),
    ?assertEqual(3, length(Result)),
    ?assertMatch({_, _, _}, lists:keyfind(node_a, 1, Result)),
    ?assertMatch({_, _, _}, lists:keyfind(node_m, 1, Result)),
    ?assertMatch({_, _, _}, lists:keyfind(node_z, 1, Result)).

%%====================================================================
%% Test cases for merge_subs/2
%%====================================================================

merge_subs_empty_left_test() ->
    Subs = [{node1, true, [{<<"t">>, {0, 0}}]}],
    ?assertEqual(Subs, vmq_swc_plugin:merge_subs([], Subs)).

merge_subs_empty_right_test() ->
    Subs = [{node1, true, [{<<"t">>, {0, 0}}]}],
    ?assertEqual(Subs, vmq_swc_plugin:merge_subs(Subs, [])).

merge_subs_both_empty_test() ->
    ?assertEqual([], vmq_swc_plugin:merge_subs([], [])).
