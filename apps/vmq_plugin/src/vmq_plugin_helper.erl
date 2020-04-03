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

-module(vmq_plugin_helper).
-export([all/2,
         all_till_ok/2]).

all(Hooks, Params) ->
    all(Hooks, Params, []).

all([{compat, Hook, CompatMod, CompatFun, Module, Fun}|Rest], Params, Acc) ->
    Res = apply(CompatMod, CompatFun, [Hook, Module, Fun, Params]),
    all(Rest, Params, [Res|Acc]);
all([{Module, Fun}|Rest], Params, Acc) ->
    Res = apply(Module, Fun, Params),
    all(Rest, Params, [Res|Acc]);
all([], _, Acc) -> lists:reverse(Acc).

all_till_ok([{Module, Fun}|Rest], Params) ->
    case apply(Module, Fun, Params) of
        ok -> ok;
        {ok, V} -> {ok, V};
        {error, Error} -> {error, Error};
        next -> all_till_ok(Rest, Params);
        E -> {error, E}
    end;
all_till_ok([{compat, Hook, CompatMod, CompatFun, Module, Fun}|Rest], Params) ->
    case apply(CompatMod, CompatFun, [Hook, Module, Fun, Params]) of
        ok -> ok;
        {ok, V} -> {ok, V};
        {error, Error} -> {error, Error};
        next -> all_till_ok(Rest, Params);
        E -> {error, E}
    end;
all_till_ok([], _) ->
    {error, plugin_chain_exhausted}.
