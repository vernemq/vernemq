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
    [%% bench_vmq_trie
    ].


%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

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
         [{{"a", IB},0}] =
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
