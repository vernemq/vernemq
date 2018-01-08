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

-export([install/1]).


install(St) ->
    luerl_emul:alloc_table(table(), St).

table() ->
    [
     {<<"decode">>, {function, fun decode/2}},
     {<<"encode">>, {function, fun encode/2}}
    ].

decode([Bin|_], St) when is_binary(Bin) ->
    try jsx:decode(Bin) of
        Result0 ->
            {Result1, NewSt} = luerl:encode(Result0, St),
            {[Result1], NewSt}
    catch
        _:_ ->
            {[nil], St}
    end.

encode([T|_], St) when is_tuple(T) ->
    try jsx:encode(encode_value(luerl:decode(T, St))) of
        Result0 ->
            {[Result0], St}
    catch
        _:_ ->
            {[nil], St}
    end.

encode_value(V) when is_list(V) ->
    encode_list(V, []);
encode_value(V) ->
    V.

encode_list([{K, V}|Rest], Acc) when is_integer(K) ->
    encode_list(Rest, [encode_value(V)|Acc]);
encode_list([{K, V}|Rest], Acc) when is_binary(K) ->
    encode_list(Rest, [{K, encode_value(V)}|Acc]);
encode_list([], Acc) -> lists:reverse(Acc).
