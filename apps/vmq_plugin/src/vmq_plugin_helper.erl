%% Copyright 2018 Erlio GmbH Basel Switzerland (http://erl.io)
%% Copyright 2018-2024 Octavo Labs/VerneMQ (https://vernemq.com/)
%% and Individual Contributors.
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

-module(vmq_plugin_helper).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
    all/2,
    all_till_ok/2,
    all_till_ok/3,
    all_till_ok/4,
    filter_hooks/2
]).

all(Hooks, Params) ->
    all(Hooks, Params, []).

all([{compat, Hook, CompatMod, CompatFun, Module, Fun} | Rest], Params, Acc) ->
    Res = apply(CompatMod, CompatFun, [Hook, Module, Fun, Params]),
    all(Rest, Params, [Res | Acc]);
all([{Module, Fun} | Rest], Params, Acc) ->
    Res = apply(Module, Fun, Params),
    all(Rest, Params, [Res | Acc]);
all([], _, Acc) ->
    lists:reverse(Acc).

all_till_ok([{Module, Fun} | Rest], Params) ->
    case apply(Module, Fun, Params) of
        ok -> ok;
        {ok, V} -> {ok, V};
        {error, Error} -> {error, Error};
        next -> all_till_ok(Rest, Params);
        E -> {error, E}
    end;
all_till_ok([{compat, Hook, CompatMod, CompatFun, Module, Fun} | Rest], Params) ->
    case apply(CompatMod, CompatFun, [Hook, Module, Fun, Params]) of
        ok -> ok;
        {ok, V} -> {ok, V};
        {error, Error} -> {error, Error};
        next -> all_till_ok(Rest, Params);
        E -> {error, E}
    end;
all_till_ok([{_Plugin, Module, Fun} | Rest], Params) ->
    case apply(Module, Fun, Params) of
        ok -> ok;
        {ok, V} -> {ok, V};
        {error, Error} -> {error, Error};
        next -> all_till_ok(Rest, Params);
        E -> {error, E}
    end;
all_till_ok([{compat, Hook, CompatMod, CompatFun, _Plugin, Module, Fun} | Rest], Params) ->
    case apply(CompatMod, CompatFun, [Hook, Module, Fun, Params]) of
        ok -> ok;
        {ok, V} -> {ok, V};
        {error, Error} -> {error, Error};
        next -> all_till_ok(Rest, Params);
        E -> {error, E}
    end;
all_till_ok([], _) ->
    {error, plugin_chain_exhausted}.

all_till_ok(Hooks, Params, Plugins) ->
    all_till_ok(filter_hooks(Hooks, Plugins), Params).

all_till_ok(Hooks, Params, Plugins, NonMatchingResponse) ->
    case all_till_ok(Hooks, Params, Plugins) of
        {error, plugin_chain_exhausted} -> NonMatchingResponse;
        Res -> Res
    end.

filter_hooks(Hooks, Plugins) ->
    lists:flatmap(
        fun(Plugin) ->
            lists:filter(
                fun
                    ({Plugin0, _, _}) -> Plugin0 =:= Plugin;
                    ({compat, _, _, _, Plugin0, _, _}) -> Plugin0 =:= Plugin;
                    (_) -> false
                end,
                Hooks
            )
        end,
        Plugins
    ).

-ifdef(TEST).
filter_hooks_preserves_configured_plugin_order_test() ->
    Hooks = [
        {plugin_a, mod_a, fun_a},
        {plugin_b, mod_b, fun_b},
        {plugin_a, mod_a, fun_a2}
    ],
    ?assertEqual(
        [
            {plugin_b, mod_b, fun_b},
            {plugin_a, mod_a, fun_a},
            {plugin_a, mod_a, fun_a2}
        ],
        filter_hooks(Hooks, [plugin_b, plugin_a])
    ).

all_till_ok_filtered_returns_non_matching_response_test() ->
    ?assertEqual(
        {error, no_match},
        all_till_ok([], [], [plugin_a], {error, no_match})
    ).
-endif.
