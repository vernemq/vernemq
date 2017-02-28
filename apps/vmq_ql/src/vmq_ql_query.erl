%% Copyright 2016 Erlio GmbH Basel Switzerland (http://erl.io)
%%
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

-module(vmq_ql_query).
-include("vmq_ql.hrl").

-behaviour(gen_server).

%% API
-export([start_link/2,
         fetch/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-callback fields_config() -> [info_table()].
-callback fold_init_rows(function(), any()) -> any().

-record(state, {mgr, query, next, result_table}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%    _   ____  _______    __
%%%   | | / /  |/  / __ \  / /
%%%   | |/ / /|_/ / /_/ / / /__
%%%   |___/_/  /_/\___\_\/____/
%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_link(Mgr, QueryString) ->
    gen_server:start_link(?MODULE, [Mgr, QueryString], []).

fetch(Pid, Ordering, Limit) ->
    try
        gen_server:call(Pid, {fetch, Ordering, Limit}, infinity)
    catch
        exit:{noproc, _} ->
            []
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([MgrPid, QueryString]) ->
    monitor(process, MgrPid),
    {ok, #state{mgr=MgrPid, query=QueryString}, 0}.

handle_call({fetch, Ordering, Limit}, _From,
            #state{next=Next, result_table=Results} = State) ->
    case collect_results(Ordering, Limit, Next, Results) of
        {no_more_rows, Rows} ->
            {stop, normal, Rows, cleanup_result(State)};
        {NextKey, Rows} ->
            {reply, Rows, State#state{next=NextKey}}
    end.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _, process, Mgr, _}, #state{mgr=Mgr} = State) ->
    {stop, normal, State};
handle_info(timeout, #state{mgr=Mgr, query=Query} = State) ->
    case internal_query(Query) of
        {ok, {select, ResultTable}} ->
            Mgr ! {results_ready, node(), self(), ets:info(ResultTable, size)},
            {noreply, State#state{result_table=ResultTable}};
        {error, Reason} ->
            lager:debug("can't run query ~p due to ~p", [Query, Reason]),
            Mgr ! {query_error, node(), self(), Reason},
            {stop, normal, State}
    end.

terminate(_Reason, State) ->
    cleanup_result(State),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

module(Table) ->
    TableMap = application:get_env(vmq_ql, table_map, []),
    case proplists:get_value(Table, TableMap) of
        undefined ->
            exit({no_table_mod_found, Table});
        TableMod ->
            TableMod
    end.

collect_results(desc, Limit, undefined, Tab) ->
    collect_results(desc, Limit, ets:first(Tab), Tab, []);
collect_results(asc, Limit, undefined, Tab) ->
    collect_results(asc, Limit, ets:last(Tab), Tab, []);
collect_results(Ordering, Limit, Next, Tab) ->
    collect_results(Ordering, Limit, Next, Tab, []).

collect_results(_, _, '$end_of_table', _, Acc) -> {no_more_rows, lists:reverse(lists:flatten(Acc))};
collect_results(_, 0, NextKey, _, Acc) -> {NextKey, lists:reverse(lists:flatten(Acc))};
collect_results(desc, Limit, Key, Tab, Acc) ->
    collect_results(desc, Limit - 1, ets:next(Tab, Key), Tab,
                    [ets:lookup(Tab, Key)|Acc]);
collect_results(asc, Limit, Key, Tab, Acc) ->
    collect_results(asc, Limit - 1, ets:prev(Tab, Key), Tab,
                    [ets:lookup(Tab, Key)|Acc]).

cleanup_result(#state{result_table=undefined} = State) -> State;
cleanup_result(#state{result_table=Tab} = State) ->
    ets:delete(Tab),
    State#state{result_table=undefined}.

internal_query(Str) ->
    try vmq_ql_parser:parse(Str) of
        {fail, E} ->
            {error, E};
        Parsed when is_list(Parsed) ->
            eval(proplists:get_value(type, Parsed), Parsed)
    catch
        E:R ->
            {error, {E, R}}
    end.

eval("SELECT", Query) ->
    From = proplists:get_value(from, Query),
    Fields = proplists:get_value(fields, Query),
    Where = proplists:get_value(where, Query),
    OrderBy = proplists:get_value(orderby, Query),
    Limit = proplists:get_value(limit, Query),
    select(Fields, module(From), Where, OrderBy, Limit).

select(Fields, Module, Where, OrderBy, Limit) ->
    FieldsConfig = Module:fields_config(),
    RequiredFields = lists:usort((required_fields(Where)
                                  ++ include_fields(FieldsConfig, Fields)
                                  ++ OrderBy)) -- [all],
    RowInitializer = get_row_initializer(FieldsConfig, RequiredFields),
    EmptyResultRow = empty_result_row(FieldsConfig, Fields),
    Results = ets:new(?MODULE, [ordered_set]),
    try
        Module:fold_init_rows(
          fun(InitRow, Idx) ->
                  PreparedRows = initialize_row(RowInitializer, InitRow),
                  lists:foldl(
                    fun(Row, AccIdx) ->
                            put({?MODULE, row_data}, Row),
                            case eval_query(Where) of
                                true ->
                                    raise_enough_data(AccIdx, OrderBy, Limit, enough_data),
                                    Key = order_by_key(AccIdx, OrderBy, Row),
                                    ets:insert(Results,
                                               {Key, filter_row(Fields, EmptyResultRow, Row)}),
                                    AccIdx + 1;
                                false -> AccIdx
                            end
                    end, Idx, PreparedRows)
          end, 1)
    catch
        exit:enough_data ->
            %% Raising inside an ets:fold allows us to stop the fold
            ok;
        E1:R1 ->
            ets:delete(Results),
            lager:error("Select query terminated due to ~p ~p", [E1, R1]),
            exit({E1, R1})
    end,
    case is_integer(Limit) of
        false ->
            ok;
        true ->
            case ets:info(Results, size) - Limit of
                V when V > 0 ->
                    try
                        ets:foldl(fun(_, 0) ->
                                          exit(trimmed_table);
                                     ({Key, _}, I) ->
                                          ets:delete(Results, Key),
                                          I - 1
                                  end, V, Results)
                    catch
                        exit:trimmed_table ->
                            ok
                    end;
                _ ->
                    ok
            end
    end,
    {ok, {select, Results}}.

get_row_initializer(FieldConfig, RequiredFields) ->
    DependsWithDuplicates =
    lists:foldl(
      fun(Field, Acc) ->
              Acc ++ depends(lists:filter(
                               fun(#vmq_ql_table{provides=Fields}) ->
                                       lists:member(Field, Fields)
                               end, FieldConfig), [])
      end, [], RequiredFields),
    % DependsWithDuplicates can look like [a,b,a,b,c,c]
    % and has to be transformed to [a,b,c]
    % Note lists:usort doesn't work here
    lists:reverse(
      lists:foldl(
        fun(InitFun, Acc) ->
                case lists:member(InitFun, Acc) of
                    false ->
                        [InitFun|Acc];
                    true ->
                        Acc
                end
        end, [], DependsWithDuplicates)).

empty_result_row(FieldsConfig, [all]) ->
    Fields = lists:flatten([Fs || {Fs, _} <- FieldsConfig]),
    empty_result_row(FieldsConfig, Fields);
empty_result_row(_FieldsConfig, Fields) ->
    maps:from_list([{F, null} || F <- Fields]).

depends([#vmq_ql_table{depends_on=Depends, init_fun=InitFun}|Rest], Acc) ->
    depends(Rest, [depends(Depends, [InitFun])|Acc]);
depends([], Acc) -> lists:flatten(Acc).

include_fields(FieldConfig, Fields) ->
    include_fields(FieldConfig, Fields, []).
include_fields(FieldConfig, [all|Rest], Acc) ->
    include_fields(FieldConfig, Rest,
                   lists:foldl(fun(#vmq_ql_table{include_if_all=true,
                                                   provides=Fields}, AccAcc) ->
                                       Fields ++ AccAcc;
                                  (_, AccAcc) ->
                                       AccAcc
                               end, Acc, FieldConfig));
include_fields(FieldConfig, [F|Rest], Acc) ->
    include_fields(FieldConfig, Rest, [F|Acc]);
include_fields(_, [], Acc) -> Acc.

initialize_row([InitFun], Row) ->
    InitFun(Row);
initialize_row([InitFun|Rest], Row) ->
    Rows = InitFun(Row),
    initialize_rows(Rest, Rows, []).

initialize_rows([], _, Acc) -> lists:flatten(Acc);
initialize_rows(_, [], Acc) -> lists:flatten(Acc);
initialize_rows(Initializer, [Row|Rows], Acc) ->
    initialize_rows(Initializer, Rows,
                    [initialize_row(Initializer, Row)|Acc]).

order_by_key(Idx, [], _) -> Idx;
order_by_key(Idx, OrderBy, Row) ->
    {lists:reverse(lists:foldl(fun(Field, OrderByAcc) ->
                                       [maps:get(Field, Row, null)|OrderByAcc]
                               end, [], OrderBy)), Idx}.

raise_enough_data(Cnt, [], Limit, Exit) when is_integer(Limit) and (Cnt > Limit) ->
    exit(Exit);
raise_enough_data(_, _, _, _) -> ignore.


required_fields(Where) ->
    lists:foldl(fun(A, Atoms) when is_atom(A) ->
                        [A|Atoms];
                   (_, Atoms) -> Atoms
                end, [], prepare(Where, [])).

prepare([], Acc) -> lists:usort(Acc);

prepare([{op, V1, _Op, V2}|Rest], Acc) ->
    prepare(Rest, [V1, V2|Acc]);
prepare([{_, {op, V1, _Op, V2}}|Rest], Acc) ->
    prepare(Rest, [V1, V2|Acc]);
prepare([{_, Ops}|Rest], Acc) when is_list(Ops) ->
    prepare(Rest, prepare(Ops, Acc)).

filter_row([all], EmptyResultRow, Row) ->
    maps:merge(EmptyResultRow, Row);
filter_row(Fields, EmptyResultRow, Row) ->
    maps:merge(EmptyResultRow, maps:with(Fields, Row)).



eval_query([]) ->
    %% No WHERE clause was specified
    true;
eval_query([{op, V1, Op, V2}|Rest]) ->
    eval_query(Rest, eval_op(Op, V1, V2));
eval_query([Op|Rest]) when is_list(Op) ->
    eval_query(Rest, eval_query(Op)).

eval_query([], Bool) -> Bool;

eval_query([{'and', {op, V1, Op, V2}}|Rest], true) ->
    case eval_op(Op, V1, V2) of
        true -> eval_query(Rest, true);
        false -> false
    end;
eval_query([{'and', Ops}|Rest], true) when is_list(Ops) ->
   case eval_query(Ops) of
       true -> eval_query(Rest, true);
       false -> false
   end;
eval_query([{'or', {op, V1, Op, V2}}|Rest], false) ->
    case eval_op(Op, V1, V2) of
        false -> eval_query(Rest, false);
        true -> true
    end;
eval_query([{'or', Ops}|Rest], false) when is_list(Ops) ->
   case eval_query(Ops) of
       false -> eval_query(Rest, false);
       true -> true
   end;
eval_query([{'and', _}|_], false) -> false; % Always false
eval_query([{'or', _}|_], true) -> true. % Always true

eval_op(equals, V1, V2) -> v(V1) == v(V2);
eval_op(not_equals, V1, V2) -> v(V1) /= v(V2);
eval_op(lesser, V1, V2) -> v(V1) < v(V2);
eval_op(lesser_equals, V1, V2) -> v(V1) =< v(V2);
eval_op(greater, V1, V2) -> v(V1) > v(V2);
eval_op(greater_equals, V1, V2) -> v(V1) >= v(V2);
eval_op(like, V, V) -> true;
eval_op(like, V1, V2) ->
    case {v(V1), v(V2)} of
        {V, P} when (is_list(V) or is_binary(V))
                and (is_list(P) or is_binary(P)) ->
            case get({?MODULE, P}) of
                undefined ->
                    case re:compile(P) of
                        {ok, MP} ->
                            put({?MODULE, P}, MP),
                            re:run(V, MP) =/= nomatch;
                        {error, ErrSpec} ->
                            lager:error("can't compile regexp ~p due to ~p", [P, ErrSpec]),
                            false
                    end;
                MP ->
                    re:run(V, MP) =/= nomatch
            end;
        _ ->
            false
    end.

v(true) -> true;
v(false) -> false;
v(undefined) -> null;
v(V) when is_atom(V) ->
    lookup_ident(V);
v(V) when is_pid(V) ->
    list_to_binary(pid_to_list(V));
v(V) ->
    try
        binary_to_existing_atom(V, utf8)
    catch
        _:_ -> V
    end.

lookup_ident(Ident) ->
    Row = get({?MODULE, row_data}),
    case maps:find(Ident, Row) of
        error -> undefined;
        {ok, V} -> V
    end.
