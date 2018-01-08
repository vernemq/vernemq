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

-module(vmq_diversity_utils).
-compile([nowarn_export_all, export_all]).

convert(Val) when is_list(Val) ->
    convert_list(Val, []);
convert(Val) when is_number(Val) ->
    case round(Val) of
        RVal when RVal == Val -> RVal;
        _ -> Val
    end;
convert(Val) when is_binary(Val) -> Val;
convert(Val) when is_boolean(Val) -> Val;
convert(nil) -> undefined.

convert_list([ListItem|Rest], Acc) ->
    convert_list(Rest, [convert_list_item(ListItem)|Acc]);
convert_list([], Acc) -> lists:reverse(Acc).

convert_list_item({Idx, Val}) when is_integer(Idx) ->
    %% lua array
    convert(Val);
convert_list_item({BinKey, Val}) when is_binary(BinKey) ->
    try list_to_existing_atom(binary_to_list(BinKey)) of
        Key -> {Key, convert(Val)}
    catch
        _:_ ->
            {BinKey, convert(Val)}
    end.

%% map / unmap is currently only used by the mongodb
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

unmap(Map) when is_map(Map) ->
    unmap(maps:to_list(Map), []);
unmap([Map|_] = Maps) when is_map(Map) ->
    {_, Ret} =
    lists:foldl(fun(M, {I, Acc}) ->
                        NextI = I + 1,
                        {NextI, [{NextI, unmap(M)}|Acc]}
                end, {0, []}, Maps),
    Ret.

unmap([{K, Map}|Rest], Acc) when is_map(Map) ->
    unmap(Rest, [{K, unmap(Map)}|Acc]);
unmap([{K, [Map|_] = Maps}|Rest], Acc) when is_map(Map) ->
    unmap(Rest, [{K, unmap(Maps)}|Acc]);
unmap([{K, V}|Rest], Acc) ->
    unmap(Rest, [{K, V}|Acc]);
unmap([], Acc) -> lists:reverse(Acc).

int(I) when is_integer(I) -> I;
int(I) when is_number(I) -> round(I).

str(S) when is_list(S) -> S;
str(S) when is_binary(S) -> binary_to_list(S).

ustr(undefined) -> undefined;
ustr(S) -> str(S).

atom(A) when is_atom(A) -> A;
atom(A) -> list_to_atom(str(A)).
