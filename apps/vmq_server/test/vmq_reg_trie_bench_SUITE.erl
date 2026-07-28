-module(vmq_reg_trie_bench_SUITE).

%% Note: This directive should only be used in test suites.
-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

suite() ->
    [{timetrap,{minutes,10}}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_GroupName, Config) ->
    Config.

end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

groups() ->
    [].

all() ->
    [
     fanout_compaction_keeps_topic_key,
     sharded_fanout_keeps_all_matches,
     async_sharded_fanout_dispatches_all_matches
    ].


%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

fanout_compaction_keeps_topic_key(_Config) ->
    ok = vmq_test_utils:setup(),
    Topic = [{[<<"some">>, <<"topic">>], 0}],
    Hour = 1000 * 3600,
    ok = gen_server:call(vmq_reg_trie, {event, updated_event("a", 1, Topic)}, Hour),
    ok = gen_server:call(vmq_reg_trie, {event, updated_event("a", 2, Topic)}, Hour),
    [_, _] = lists:sort(vmq_reg_trie:fold(
        {"a", <<"publisher">>},
        [<<"some">>, <<"topic">>],
        fun(E, _, Acc) -> [E | Acc] end,
        []
    )),

    ok = gen_server:call(vmq_reg_trie, {event, deleted_event("a", 1, Topic)}, Hour),
    [{{"a", <<"2">>}, 0, _QPid}] = vmq_reg_trie:fold(
        {"a", <<"publisher">>},
        [<<"some">>, <<"topic">>],
        fun(E, _, Acc) -> [E | Acc] end,
        []
    ),
    [{_, {{"a", <<"2">>}, 0, _}}] = ets:tab2list(vmq_trie_subs),
    [] = ets:tab2list(vmq_trie_subs_fanout),
    ok = vmq_test_utils:teardown(),
    ok.

sharded_fanout_keeps_all_matches(_Config) ->
    application:set_env(vmq_server, fanout_shard_count, 8),
    try
        ok = vmq_test_utils:setup(),
        persistent_term:put({vmq_reg_trie, fanout_shard_count}, 8),
        Topic = [{[<<"some">>, <<"topic">>], 0}],
        Hour = 1000 * 3600,
        lists:foreach(
          fun(I) ->
                  ok = gen_server:call(vmq_reg_trie, {event, updated_event("a", I, Topic)}, Hour)
          end,
          lists:seq(1, 16)),
        16 = length(vmq_reg_trie:fold(
                      {"a", <<"publisher">>},
                      [<<"some">>, <<"topic">>],
                      fun(E, _, Acc) -> [E | Acc] end,
                      [])),
        Shards = lists:usort([
            Shard
         || {{Shard, _Key}, _Val} <- ets:tab2list(vmq_trie_subs_fanout)
        ]),
        true = length(Shards) > 1
    after
        catch vmq_test_utils:teardown(),
        persistent_term:erase({vmq_reg_trie, fanout_shard_count}),
        application:unset_env(vmq_server, fanout_shard_count)
    end,
    ok.

async_sharded_fanout_dispatches_all_matches(_Config) ->
    application:set_env(vmq_server, fanout_shard_count, 8),
    application:set_env(vmq_server, fanout_async_handoff, true),
    try
        ok = vmq_test_utils:setup(),
        persistent_term:put({vmq_reg_trie, fanout_shard_count}, 8),
        Topic = [{[<<"some">>, <<"topic">>], 0}],
        Hour = 1000 * 3600,
        lists:foreach(
          fun(I) ->
                  ok = gen_server:call(vmq_reg_trie, {event, updated_event("a", I, Topic)}, Hour)
          end,
          lists:seq(1, 16)),
        TestPid = self(),
        [] = vmq_reg_trie:fold(
               {"a", <<"publisher">>},
               [<<"some">>, <<"topic">>],
               fun(E, _, Acc) -> TestPid ! {fanout_match, E}, Acc end,
               []),
        16 = receive_fanout_matches(16)
    after
        catch vmq_test_utils:teardown(),
        persistent_term:erase({vmq_reg_trie, fanout_shard_count}),
        application:unset_env(vmq_server, fanout_shard_count),
        application:unset_env(vmq_server, fanout_async_handoff)
    end,
    ok.

receive_fanout_matches(Count) ->
    receive_fanout_matches(Count, 0).

receive_fanout_matches(0, Acc) ->
    Acc;
receive_fanout_matches(Count, Acc) ->
    receive
        {fanout_match, _} ->
            receive_fanout_matches(Count - 1, Acc + 1)
    after 1000 ->
        Acc
    end.

bench_ets(_Config) ->
    %%bench_ets_(5).
    [bench_ets_(Num) || Num <- [1000,2000,4000,8000,16000,32000,64000]].


bench_ets_(Num) ->
    %% Key = {"MP", [<<"some">>,<<"topic">>]},
    %% Value = {{"MP", <<"client_id">>}, QoSOrSubInfo}
    %% Entry = {Key, Value}
    %% example:
    %%   {{"a",[<<"some">>,<<"topic">>]},{{"a",<<"10">>},0}}

    Events =
        [{{"mp", [<<"some">>, <<"topic">>]},
         {{"mp", integer_to_binary(I)}, 0}} || I <- lists:seq(1,Num)],

    %% bag [{Key,Val1}, {Key, Val2},...]
    DBag = ets:new(table, [duplicate_bag]),

    %% set [{Key, #{V1 => V11, V2 => V22}}]
    Bag = ets:new(table, [bag]),

    TS1 = erlang:monotonic_time(millisecond),
    [ets:insert(Bag, E) || E <- Events],
    TS2 = erlang:monotonic_time(millisecond),
    iotime(Num, bag, TS1, TS2),


    TS3 = erlang:monotonic_time(millisecond),
    [ets:insert(DBag, E) || E <- Events],
    TS4 = erlang:monotonic_time(millisecond),
    iotime(Num, duplicate_bag, TS3, TS4),

    %% io:format(user, "Baag: ~p~n", [ets:tab2list(Bag)]),
    %% io:format(user, "Seet: ~p~n", [ets:tab2list(Set)]),


    T7 = erlang:monotonic_time(millisecond),
    [ets:delete_object(Bag, E) || E <- Events],
    T8 = erlang:monotonic_time(millisecond),
    iotime(Num, duplicate_bag_del_o, T7, T8),

    T5 = erlang:monotonic_time(millisecond),
    [ets:delete_object(DBag, E) || E <- Events],
    T6 = erlang:monotonic_time(millisecond),
    iotime(Num, duplicate_bag_del_o, T5, T6),

    ets:delete(Bag),
    ets:delete(DBag).


bench_vmq_trie_single_lookups_test(_Config) ->
    bench_single_lookups(1000),
    bench_single_lookups(2000),
    bench_single_lookups(4000),
    bench_single_lookups(8000),
    bench_single_lookups(16000),
    bench_single_lookups(32000),
    bench_single_lookups(64000),
    bench_single_lookups(128000),
    bench_single_lookups(256000),
    bench_single_lookups(512000),
    bench_single_lookups(1024000),
    bench_single_lookups(2048000),
    bench_single_lookups(4096000),
    ok.


bench_single_lookups(Num) ->
    ok = vmq_test_utils:setup(),
    %% one subscriber / topic
    InsertTopicsF = fun(I) ->
                           [{[<<"unique">>,<<"topic">>,integer_to_binary(I)], 0}]
                   end,
    LookupTopicF = fun(I) ->
                           [<<"unique">>,<<"topic">>,integer_to_binary(I)]
                   end,
    InsertEvents =
        [updated_event("a", I, InsertTopicsF(I)) || I <- lists:seq(1,Num-1)],

    lists:foreach(
      fun(Event) ->
              vmq_reg_trie ! Event
      end, InsertEvents),
    Hour = 1000*3600,
    ok = gen_server:call(vmq_reg_trie, {event, updated_event("a", Num, InsertTopicsF(Num))}, Hour),

    TS1 = erlang:monotonic_time(millisecond),

    %% io:format(user, "XXX ~p~n", [ets:tab2list(vmq_trie_subs)]),
    %% io:format(user, "XXX ~p~n", [ets:tab2list(vmq_trie_subs_fanout)]),
    [
     begin
         IB = integer_to_binary(I),
         [{{"a", IB}, 0, _QPid}] =
              vmq_reg_trie:fold({"a", <<"whatever">>}, LookupTopicF(I),
                                fun(E, _, Acc) -> [E|Acc] end,
                                [])
     end
     || I <- lists:seq(1,Num)
    ],
    TS2 = erlang:monotonic_time(millisecond),
    iotime(Num, single_lookup, TS1, TS2),
    ok = vmq_test_utils:teardown(),
    ok.

bench_vmq_trie_fanout_subs_test(_Config) ->
    bench_fanout_subs(1000),
    bench_fanout_subs(2000),
    bench_fanout_subs(4000),
    bench_fanout_subs(8000),
    bench_fanout_subs(16000),
    bench_fanout_subs(32000),
    bench_fanout_subs(64000),
    bench_fanout_subs(128000),
    bench_fanout_subs(256000),
    bench_fanout_subs(512000),
    bench_fanout_subs(1024000),
    bench_fanout_subs(2048000),
    bench_fanout_subs(4096000),
    ok.

bench_fanout_subs(Num) ->
    ok = vmq_test_utils:setup(),

    Topic = [{[<<"some">>,<<"topic">>],0}],
    %% insert fanout subscriptions
    InsertEvents = [updated_event("a", I, Topic) || I <- lists:seq(1,Num-1)],
    TS1 = erlang:monotonic_time(millisecond),
    lists:foreach(
      fun(Event) ->
              vmq_reg_trie ! Event
      end, InsertEvents),
    Hour = 1000*3600,
    ok = gen_server:call(vmq_reg_trie, {event, updated_event("a", Num, Topic)}, Hour),
    TS2 = erlang:monotonic_time(millisecond),
    iotime(Num, fanout_insert, TS1, TS2),

    %% io:format(user, "XXX : ~p~n", [ets:tab2list(vmq_trie_subs)]),
    %% io:format(user, "XXX : ~p~n", [ets:tab2list(vmq_trie_subs_fanout)]),

    %% fold and receive all subscribers in the fanout.
    TS5 = erlang:monotonic_time(millisecond),
    [_|_] =
        vmq_reg_trie:fold({"a", <<"whatever">>}, [<<"some">>, <<"topic">>],
                          fun(E, _, Acc) -> [E|Acc] end,
                          []),
    TS6 = erlang:monotonic_time(millisecond),
    iotime(Num, fanout_lookup, TS5, TS6),


    %% delete fanout subscriptions
    DeleteEvents = [deleted_event("a", I, Topic) || I <- lists:seq(1,Num-1)],
    TS3 = erlang:monotonic_time(millisecond),
    lists:foreach(
      fun(Event) ->
              vmq_reg_trie ! Event
      end, DeleteEvents),
    Hour = 1000*3600,
    ok = gen_server:call(vmq_reg_trie, {event, deleted_event("a", Num, Topic)}, Hour),
    TS4 = erlang:monotonic_time(millisecond),
    iotime(Num, fanout_delete, TS3, TS4),

    %% sanity check
    [] = ets:tab2list(vmq_trie_subs),
    [] = ets:tab2list(vmq_trie_subs_fanout),

    ok = vmq_test_utils:teardown(),
    ok.



updated_event(MP, ClientIdInt, Topics) ->
    {updated, {vmq, subscriber},
     {MP,integer_to_binary(ClientIdInt)},
     undefined,
     [{node(),true,Topics}]
    }.

deleted_event(MP, ClientIdInt, Topics) ->
    {deleted,{vmq,subscriber},
     {MP, integer_to_binary(ClientIdInt)},
     [{node(),true,Topics}]
    }.

iotime(Num, Type,  T1, T2) ->
    io:format(user, "~p ~p: Elapsed time ~ps~n", [Num, Type, (T2 - T1)/1000]).
