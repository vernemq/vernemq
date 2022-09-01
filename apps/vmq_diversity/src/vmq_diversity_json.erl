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

-module(vmq_diversity_json).
-include_lib("luerl/include/luerl.hrl").

-export([install/1]).

-define(JSON_EMPTY_OBJECT, [{}]).
-define(LUA_EMPTY_TABLE, []).

install(St) ->
    luerl_emul:alloc_table(table(), St).

table() ->
    [
        {<<"decode">>, #erl_func{code = fun decode/2}},
        {<<"encode">>, #erl_func{code = fun encode/2}}
    ].

decode([Bin | _], St) when is_binary(Bin) ->
    try vmq_json:decode(Bin) of
        Result0 ->
            {Result1, NewSt} = luerl:encode(json_to_lua(Result0), St),
            {[Result1], NewSt}
    catch
        _:_ ->
            {[nil], St}
    end.

encode([T | _], St) when is_tuple(T) ->
    try vmq_json:encode(lua_to_json(luerl:decode(T, St))) of
        Result0 ->
            {[Result0], St}
    catch
        _:_ ->
            {[nil], St}
    end.

lua_to_json(?LUA_EMPTY_TABLE) ->
    ?JSON_EMPTY_OBJECT;
lua_to_json(V) when is_list(V) ->
    lua_to_json_list(V, []);
lua_to_json(V) ->
    V.

lua_to_json_list([{K, V} | Rest], Acc) when is_integer(K) ->
    lua_to_json_list(Rest, [lua_to_json(V) | Acc]);
lua_to_json_list([{K, V} | Rest], Acc) when is_binary(K) ->
    lua_to_json_list(Rest, [{K, lua_to_json(V)} | Acc]);
lua_to_json_list([], Acc) ->
    lists:reverse(Acc).

json_to_lua(?JSON_EMPTY_OBJECT) ->
    ?LUA_EMPTY_TABLE;
json_to_lua(Result) when is_list(Result) ->
    lists:map(
        fun
            ({K, V}) ->
                {K, json_to_lua(V)};
            (V) ->
                json_to_lua(V)
        end,
        Result
    );
json_to_lua(Result) ->
    Result.
