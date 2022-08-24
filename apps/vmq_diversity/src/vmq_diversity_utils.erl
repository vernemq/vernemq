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
-include_lib("vernemq_dev/include/vernemq_dev.hrl").
-export([
    convert_modifiers/2,
    normalize_subscribe_topics/1,
    convert/1,
    map/1,
    unmap/1,
    int/1,
    str/1,
    ustr/1,
    atom/1
]).

%% @doc convert modifiers returned from lua to the canonical form so
%% it can be run throught the modifier checker and returned to the
%% mqtt fsms.
convert_modifiers(auth_on_subscribe, Mods0) ->
    normalize_subscribe_topics(convert(Mods0));
convert_modifiers(on_unsubscribe, Mods0) ->
    convert(Mods0);
convert_modifiers(Hook, Mods0) ->
    Mods1 = atomize_keys(Mods0),
    Converted = lists:map(
        fun(Mod) ->
            convert_modifier(Hook, Mod)
        end,
        Mods1
    ),
    case
        lists:member(Hook, [
            auth_on_register_m5,
            auth_on_subscribe_m5,
            auth_on_unsubscribe_m5,
            on_unsubscribe_m5,
            auth_on_publish_m5,
            on_deliver_m5,
            on_auth_m5
        ])
    of
        true ->
            maps:from_list(Converted);
        _ ->
            Converted
    end.

atomize_keys(Mods) ->
    lists:map(
        fun
            ({K, V}) when is_binary(K) ->
                {binary_to_existing_atom(K, utf8), V};
            ({K, V}) when is_atom(K) ->
                {K, V}
        end,
        Mods
    ).

convert_modifier(_, {properties, Props}) ->
    {properties, convert_properties(Props)};
convert_modifier(Hook, {subscriber_id, SID}) when
    Hook =:= auth_on_register_m5;
    Hook =:= auth_on_register
->
    {subscriber_id, atomize_keys(SID)};
convert_modifier(auth_on_subscribe_m5, {topics, Topics}) ->
    {topics, normalize_subscribe_topics(convert(Topics))};
convert_modifier(_, {Key, Val}) ->
    %% fall back to automatic conversion,
    {Key, convert(Val)}.

convert_properties(Props) ->
    NewProps = lists:map(
        fun convert_property/1, atomize_keys(Props)
    ),
    maps:from_list(NewProps).

convert_property({?P_USER_PROPERTY, Val}) ->
    {?P_USER_PROPERTY,
        lists:map(
            fun
                ({_, [T]}) -> T;
                (V) -> V
            end,
            Val
        )};
%% TODO: currently vmq_plugin_utils:check_modifiers splits the topic.
%% convert_property({?P_RESPONSE_TOPIC, Topic}) ->
%%     {?P_RESPONSE_TOPIC, vmq_topic:word(Topic)};
convert_property({?P_PAYLOAD_FORMAT_INDICATOR, <<"undefined">>}) ->
    {?P_PAYLOAD_FORMAT_INDICATOR, undefined};
convert_property({?P_PAYLOAD_FORMAT_INDICATOR, <<"utf8">>}) ->
    {?P_PAYLOAD_FORMAT_INDICATOR, utf8};
convert_property({K, V}) ->
    {K, convert(V)}.

normalize_subscribe_topics(Topics0) ->
    lists:map(
        fun
            ([T, [Q, SubOpts]]) ->
                {T, {convert(Q), maps:from_list(SubOpts)}};
            ([T, Q]) ->
                {T, convert(Q)}
        end,
        Topics0
    ).

%% @doc recursively converts a value returned from lua to an erlang
%% data structure.
convert(Val) when is_list(Val) ->
    convert_list(Val, []);
convert(Val) when is_number(Val) ->
    case round(Val) of
        RVal when RVal == Val -> RVal;
        _ -> Val
    end;
convert(Val) when is_binary(Val) -> Val;
convert(Val) when is_boolean(Val) -> Val;
convert(nil) ->
    undefined.

convert_list([ListItem | Rest], Acc) ->
    convert_list(Rest, [convert_list_item(ListItem) | Acc]);
convert_list([], Acc) ->
    lists:reverse(Acc).

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

%% @doc used to convert the config map value (table) from the lua
%% scripts to an erlang map.
%%
%% Also used to convert mongodb return values (selectors etc) to an
%% erlang map. The mongodb specific code should be moved to the
%% mongodb adapter.
-spec map(any()) -> map().
map(TableOrTables) ->
    case map(TableOrTables, []) of
        [Map] -> Map;
        Maps -> Maps
    end.

map([], []) ->
    map_([]);
map([{I, [{K, _} | _] = Doc} | Rest], Acc) when is_integer(I) and is_binary(K) ->
    %% list of docs
    map(Rest, [map_(Doc) | Acc]);
map([{K, _} | _] = Doc, Acc) when is_binary(K) ->
    %% one doc
    [map_(Doc) | Acc];
map([], Acc) ->
    lists:reverse(Acc).

map_([]) ->
    #{};
map_(Proplist) ->
    lists:foldl(
        fun
            ({K, [{_, _} | _] = V}, AccIn) ->
                maps:put(K, map_(V), AccIn);
            ({K, V}, AccIn) ->
                maps:put(K, V, AccIn)
        end,
        #{},
        Proplist
    ).

%% @doc convert an erlang map into a an erlang representation of a lua
%% table. Contains mongo-specific code to convert an erlang timstamp
%% to unix millisecs which is currently mongodb specific. TODO: Move
%% the mongodb specific stuff should to the mongodb adapter.
-spec unmap(map() | [map()]) -> [any()].
unmap(Map) when is_map(Map) ->
    unmap(maps:to_list(Map), []);
unmap([Map | _] = Maps) when is_map(Map) ->
    {_, Ret} =
        lists:foldl(
            fun(M, {I, Acc}) ->
                NextI = I + 1,
                {NextI, [{NextI, unmap(M)} | Acc]}
            end,
            {0, []},
            Maps
        ),
    Ret.

unmap([{K, Map} | Rest], Acc) when is_map(Map) ->
    unmap(Rest, [{K, unmap(Map)} | Acc]);
unmap([{K, [Map | _] = Maps} | Rest], Acc) when is_map(Map) ->
    unmap(Rest, [{K, unmap(Maps)} | Acc]);
unmap([{K, {A, B, C} = TS} | Rest], Acc) when
    is_integer(A), is_integer(B), is_integer(C)
->
    unmap(Rest, [{K, to_unixtime_millisecs(TS)} | Acc]);
unmap([{K, V} | Rest], Acc) ->
    unmap(Rest, [{K, V} | Acc]);
unmap([], Acc) ->
    lists:reverse(Acc).

to_unixtime_millisecs({MegaSecs, Secs, MicroSecs}) ->
    MegaSecs * 1000000000 + Secs * 1000 + MicroSecs div 1000.

int(I) when is_integer(I) -> I;
int(I) when is_number(I) -> round(I).

str(S) when is_list(S) -> S;
str(S) when is_binary(S) -> binary_to_list(S).

ustr(undefined) -> undefined;
ustr(S) -> str(S).

atom(A) when is_atom(A) -> A;
atom(A) -> list_to_atom(str(A)).
