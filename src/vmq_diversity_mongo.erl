-module(vmq_diversity_mongo).

-export([install/1]).

-import(luerl_lib, [badarg_error/3]).


insert(PoolName, Collection, DocOrDocs) ->
    poolboy:transaction(PoolName, fun(Worker) ->
                                          mongo:insert(Worker, Collection, DocOrDocs)
                                  end).

update(PoolName, Collection, Selector, Command) ->
    poolboy:transaction(PoolName, fun(Worker) ->
                                          mongo:update(Worker, Collection, Selector, Command)
                                  end).

delete(PoolName, Collection, Selector) ->
    poolboy:transaction(PoolName, fun(Worker) ->
                                          mongo:delete(Worker, Collection, Selector)
                                  end).

find(PoolName, Collection, Selector, Args) ->
    poolboy:transaction(PoolName, fun(Worker) ->
                                          mongo:find(Worker, Collection, Selector, Args)
                                  end).

find_one(PoolName, Collection, Selector, Args) ->
    poolboy:transaction(PoolName, fun(Worker) ->
                                          mongo:find_one(Worker, Collection, Selector, Args)
                                  end).

install(St) ->
    luerl_emul:alloc_table(table(), St).

table() ->
    [
     {<<"insert">>, {function, fun insert/2}},
     {<<"update">>, {function, fun update/2}},
     {<<"delete">>, {function, fun delete/2}},
     {<<"find">>, {function, fun find/2}},
     {<<"find_one">>, {function, fun find_one/2}},
     {<<"next">>, {function, fun cursor_next/2}},
     {<<"take">>, {function, fun cursor_take/2}},
     {<<"close">>, {function, fun cursor_close/2}}
    ].

insert(As, St) ->
    case As of
        [BPoolId, Collection, DocOrDocs] when is_binary(BPoolId)
                                             and is_binary(Collection) ->
            case luerl:decode(DocOrDocs, St) of
                [{K, V}|_] = TableOrTables when is_binary(K)
                                                or (is_integer(K) and is_list(V)) ->
                    PoolId = pool_id(BPoolId, As, St),
                    try insert(PoolId, Collection, map(TableOrTables, [])) of
                        Result1 ->
                            {Result2, NewSt} = luerl:encode(unmap(Result1, []), St),
                            {[Result2], NewSt}
                    catch
                        E:R ->
                            lager:error("can't execute insert ~p due to ~p:~p",
                                        [TableOrTables, E, R]),
                            badarg_error(execute_insert, As, St)
                    end
            end;
        _ ->
            badarg_error(execute_parse, As, St)
    end.

update(As, St) ->
    case As of
        [BPoolId, Collection, Selector0, Doc] when is_binary(BPoolId)
                                                       and is_binary(Collection) ->
            case {luerl:decode(Selector0, St),
                  luerl:decode(Doc, St)} of
                {[{SelectKey, _}|_] = Selector, [{K,_}|_] = Command}
                  when is_binary(SelectKey) and is_binary(K) ->
                    PoolId = pool_id(BPoolId, As, St),
                    try update(PoolId, Collection, map(Selector),
                               #{<<"$set">> => map(Command)}) of
                        ok ->
                            {[true], St}
                    catch
                        E:R ->
                            lager:error("can't execute update ~p due to ~p:~p",
                                        [{Selector, Command}, E, R]),
                            badarg_error(execute_update, As, St)
                    end
            end;
        _ ->
            badarg_error(execute_parse, As, St)
    end.

delete(As, St) ->
    case As of
        [BPoolId, Collection, Selector0] when is_binary(BPoolId)
                                             and is_binary(Collection) ->
            case luerl:decode(Selector0, St) of
                Selector when is_list(Selector) ->
                    PoolId = pool_id(BPoolId, As, St),
                    delete(PoolId, Collection, map(Selector)),
                    {[true], St}
            end;
        _ ->
            badarg_error(execute_parse, As, St)
    end.

find(As, St) ->
    case As of
        [BPoolId, Collection, Selector0|Args] when is_binary(BPoolId)
                                             and is_binary(Collection) ->
            case luerl:decode(Selector0, St) of
                Selector when is_list(Selector) ->
                    PoolId = pool_id(BPoolId, As, St),
                    try find(PoolId, Collection, map(Selector), parse_args(Args, St)) of
                        CursorPid when is_pid(CursorPid) ->
                            %% we re passing the cursor pid as a binary to the Lua Script
                            BinPid = list_to_binary(pid_to_list(CursorPid)),
                            {[<<"mongo-cursor-", BinPid/binary>>], St}
                    catch
                        E:R ->
                            lager:error("can't execute find ~p due to ~p:~p",
                                        [Selector, E, R]),
                            badarg_error(execute_find, As, St)
                    end
            end;
        _ ->
            badarg_error(execute_parse, As, St)
    end.

find_one(As, St) ->
    case As of
        [BPoolId, Collection, Selector0|Args] when is_binary(BPoolId)
                                             and is_binary(Collection) ->
            case luerl:decode(Selector0, St) of
                Selector when is_list(Selector) ->
                    PoolId = pool_id(BPoolId, As, St),
                    try find_one(PoolId, Collection, map(Selector), parse_args(Args, St)) of
                        Result1 when map_size(Result1) > 0 ->
                            {Result2, NewSt} = luerl:encode(unmap(Result1), St),
                            {[Result2], NewSt};
                        _ ->
                            {[false], St}
                    catch
                        E:R ->
                            lager:error("can't execute find_one ~p due to ~p:~p",
                                        [Selector, E, R]),
                            badarg_error(execute_find_one, As, St)
                    end
            end;
        _ ->
            badarg_error(execute_parse, As, St)
    end.


cursor_next(As, St) ->
    case As of
        [<<"mongo-cursor-", BinPid/binary>>] ->
            CursorPid = list_to_pid(binary_to_list(BinPid)),
            case mc_cursor:next(CursorPid) of
                error ->
                    {[false], St};
                {Result1} ->
                    {Result2, NewSt} = luerl:encode(unmap(Result1), St),
                    {[Result2], NewSt}
            end;
        _ ->
            badarg_error(execute_parse, As, St)
    end.

cursor_take(As, St) ->
    case As of
        [<<"mongo-cursor-", BinPid/binary>>, N] when is_number(N) ->
            CursorPid = list_to_pid(binary_to_list(BinPid)),
            case mc_cursor:take(CursorPid, round(N)) of
                error ->
                    {[false], St};
                Result1 ->
                    {Result2, NewSt} = luerl:encode(unmap(Result1, []), St),
                    {[Result2], NewSt}
            end;
        _ ->
            badarg_error(execute_parse, As, St)
    end.


cursor_close(As, St) ->
    case As of
        [<<"mongo-cursor-", BinPid/binary>>] ->
            CursorPid = list_to_pid(binary_to_list(BinPid)),
            mc_cursor:close(CursorPid),
            {[true], St};
        _ ->
            badarg_error(execute_parse, As, St)
    end.

map([], []) -> [];
map([{I, [{K, _}|_] = Doc}|Rest], Acc) when is_integer(I) and is_binary(K) ->
    %% list of docs
    map(Rest, [ensure_id(map(Doc))|Acc]);
map([{K, _}|_] = Doc, Acc) when is_binary(K) ->
    %% one doc
    [ensure_id(map(Doc))|Acc];
map([], Acc) -> lists:reverse(Acc).

map([]) -> #{};
map(Proplist) ->
    lists:foldl(fun
                    ({K, [{_, _}|_] = V}, AccIn) ->
                        maps:put(K, map(V), AccIn);
                    ({K, V}, AccIn) ->
                        maps:put(K, V, AccIn)
                end, #{}, Proplist).

unmap([], []) -> [];
unmap([Map|Rest], Acc) when is_map(Map) ->
    unmap(Rest, [unmap(Map)|Acc]);
unmap([], Acc) -> lists:reverse(Acc).

unmap(Map) when map_size(Map) == 0 -> [];
unmap(Map) ->
    maps:fold(fun
                  (K,V, AccIn) when is_map(V) ->
                      [{K, unmap(V)}|AccIn];
                  (K,V, AccIn) ->
                      [{K, V}|AccIn]
              end, [], Map).

ensure_id(#{<<"_id">> := _} = Map) -> Map;
ensure_id(Map) ->
    %% we use our own _id because the autogenerated one
    %% has the unuseful format {Key}
    maps:put(<<"_id">>, term_to_binary(erlang:phash2(Map)), Map).

pool_id(BPoolId, As, St) ->
    try list_to_existing_atom(binary_to_list(BPoolId)) of
        APoolId -> APoolId
    catch
        _:_ ->
            lager:error("unknown pool ~p", [BPoolId]),
            badarg_error(unknown_pool, As, St)
    end.

parse_args([], _) -> [];
parse_args([Table], St) when is_tuple(Table) ->
    case luerl:decode(Table, St) of
        [{K, _}|_] = Args when is_binary(K) ->
            parse_args_(Args, []);
        _ ->
            []
    end;
parse_args(_, _) ->
    % everything else gets ignored
    [].

parse_args_([{K, V}|Rest], Acc) ->
    try list_to_existing_atom(binary_to_list(K)) of
        AK when is_list(V) ->
            parse_args_(Rest, [{AK, map(V)}|Acc]);
        AK ->
            parse_args_(Rest, [{AK, V}|Acc])
    catch
        _:_ ->
            parse_args_(Rest, Acc)
    end;
parse_args_([], Acc) -> Acc.


