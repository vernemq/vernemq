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

-module(vmq_diversity_utils).
-compile(export_all).

map(TableOrTables) ->
    case map(TableOrTables, []) of
        [Map] -> Map;
        Maps -> Maps
    end.


map([], []) -> map_([]);
map([{I, [{K, _}|_] = Doc}|Rest], Acc) when is_integer(I) and is_binary(K) ->
    %% list of docs
    map(Rest, [map_(Doc)|Acc]);
map([{K, _}|_] = Doc, Acc) when is_binary(K) ->
    %% one doc
    [map_(Doc)|Acc];
map([], Acc) -> lists:reverse(Acc).

map_([]) -> #{};
map_(Proplist) ->
    lists:foldl(fun
                    ({K, [{_, _}|_] = V}, AccIn) ->
                        maps:put(K, map_(V), AccIn);
                    ({K, V}, AccIn) ->
                        maps:put(K, V, AccIn)
                end, #{}, Proplist).

unmap(MapOrMaps) ->
    unmap(MapOrMaps, []).

unmap([], []) -> [];
unmap([Map|Rest], Acc) when is_map(Map) ->
    unmap(Rest, [unmap_(Map)|Acc]);
unmap([], Acc) -> lists:reverse(Acc);
unmap(Map, []) when is_map(Map) ->
    unmap_(Map).

unmap_(Map) when map_size(Map) == 0 -> [];
unmap_(Map) ->
    maps:fold(fun
                  (K,V, AccIn) when is_map(V) ->
                      [{K, unmap_(V)}|AccIn];
                  (K,V, AccIn) ->
                      [{K, V}|AccIn]
              end, [], Map).

int(I) when is_integer(I) -> I;
int(I) when is_number(I) -> round(I).

str(S) when is_list(S) -> S;
str(S) when is_binary(S) -> binary_to_list(S).

ustr(undefined) -> undefined;
ustr(S) -> str(S).

atom(A) when is_atom(A) -> A;
atom(A) -> list_to_atom(str(A)).
