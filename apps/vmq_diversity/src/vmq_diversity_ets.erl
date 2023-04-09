%% Copyright 2018 Erlio GmbH Basel Switzerland (http://erl.io)
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

-module(vmq_diversity_ets).
-include_lib("luerl/include/luerl.hrl").

-export([install/1]).

-import(luerl_lib, [badarg_error/3]).

install(St) ->
    luerl_emul:alloc_table(table(), St).

table() ->
    [
        {<<"insert">>, #erl_func{code = fun insert/2}},
        {<<"insert_new">>, #erl_func{code = fun insert_new/2}},
        {<<"lookup">>, #erl_func{code = fun lookup/2}},
        {<<"delete">>, #erl_func{code = fun delete/2}},
        {<<"delete_all">>, #erl_func{code = fun delete_all/2}},
        {<<"ensure_table">>, #erl_func{code = fun ensure_table/2}}
    ].

insert([BTableId, ObjectOrObjects] = As, St) when is_binary(BTableId) ->
    TableId = table_id(BTableId, As, St),
    case luerl:decode(ObjectOrObjects, St) of
        [{K, _} | _] = OObjects when is_binary(K) ->
            {[ets:insert(TableId, OObjects)], St}
    end.

insert_new([BTableId, ObjectOrObjects] = As, St) when is_binary(BTableId) ->
    TableId = table_id(BTableId, As, St),
    case luerl:decode(ObjectOrObjects, St) of
        [{K, _} | _] = OObjects when is_binary(K) ->
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

ensure_table(As, St) ->
    case As of
        [Config0 | _] ->
            case luerl:decode(Config0, St) of
                Config when is_list(Config) ->
                    Options = vmq_diversity_utils:map(Config),
                    Name = vmq_diversity_utils:str(
                        maps:get(
                            <<"name">>,
                            Options,
                            "simple_kv"
                        )
                    ),
                    Type = vmq_diversity_utils:atom(
                        maps:get(
                            <<"type">>,
                            Options,
                            set
                        )
                    ),
                    AName = list_to_atom("vmq-diversity-ets" ++ Name),
                    NewOptions = [Type],
                    vmq_diversity_sup:start_all_pools(
                        [{kv, [{id, AName}, {opts, NewOptions}]}], []
                    ),

                    % return to lua
                    {[true], St};
                _ ->
                    badarg_error(execute_parse, As, St)
            end;
        _ ->
            badarg_error(execute_parse, As, St)
    end.

table_id(BTableName, As, St) ->
    try list_to_existing_atom("vmq-diversity-ets" ++ binary_to_list(BTableName)) of
        ATableName -> ATableName
    catch
        _:_ ->
            lager:error("unknown pool ~p", [BTableName]),
            badarg_error(unknown_pool, As, St)
    end.
