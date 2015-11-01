-module(vmq_diversity_ets).

-export([install/1]).

-import(luerl_lib, [badarg_error/3]).


install(St) ->
    luerl_emul:alloc_table(table(), St).

table() ->
    [
     {<<"insert">>, {function, fun insert/2}},
     {<<"insert_new">>, {function, fun insert_new/2}},
     {<<"lookup">>, {function, fun lookup/2}},
     {<<"delete">>, {function, fun delete/2}},
     {<<"delete_all">>, {function, fun delete_all/2}}
    ].

insert([BTableId, ObjectOrObjects] = As, St) when is_binary(BTableId) ->
    TableId = table_id(BTableId, As, St),
    case luerl:decode(ObjectOrObjects, St) of
        [{K, _}|_] = OObjects when is_binary(K) ->
            {[ets:insert(TableId, OObjects)], St}
    end.

insert_new([BTableId, ObjectOrObjects] = As, St) when is_binary(BTableId) ->
    TableId = table_id(BTableId, As, St),
    case luerl:decode(ObjectOrObjects, St) of
        [{K, _}|_] = OObjects when is_binary(K) ->
            {[ets:insert_new(TableId, OObjects)], St}
    end.

lookup([BTableId, Key] = As, St) when is_binary(BTableId) ->
    TableId = table_id(BTableId, As, St),
    KKey = luerl:decode(Key, St),
    Result0 = ets:lookup(TableId, KKey),
    {_, Vals} = lists:unzip(Result0),
    {Result1, NewSt} = luerl:encode(Vals, St),
    {[Result1], NewSt}.

delete([BTableId, Key] = As, St) when is_binary(BTableId) ->
    TableId = table_id(BTableId, As, St),
    KKey = luerl:decode(Key, St),
    {[ets:delete(TableId, KKey)], St}.

delete_all([BTableId] = As, St) when is_binary(BTableId) ->
    TableId = table_id(BTableId, As, St),
    {[ets:delete_all_objects(TableId)], St}.

table_id(BPoolId, As, St) ->
    try list_to_existing_atom(binary_to_list(BPoolId)) of
        APoolId -> APoolId
    catch
        _:_ ->
            lager:error("unknown pool ~p", [BPoolId]),
            badarg_error(unknown_pool, As, St)
    end.
