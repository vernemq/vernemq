%% Copyright 2018 Octavo Labs AG Zurich Switzerland (https://octavolabs.com)
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

-module(vmq_swc).

-export([start/0, start/1, stop/0, stop/1]).
-export([config/1,
         members/1,

         get/4,
         put/5,
         put_many/4,
         delete/3,
         delete_many/3,
         fold/5]).

-export([raw_get/3,
         raw_put/5]).


start() ->
    vmq_swc_plugin:plugin_start().

stop(SwcGroup) when is_atom(SwcGroup) ->
    SwcGroupStr = atom_to_list(SwcGroup),
    StoreSup = list_to_atom("vmq_swc_store_sup_" ++ SwcGroupStr),
    supervisor:terminate_child(vmq_swc_sup, StoreSup).

start(SwcGroup) when is_atom(SwcGroup) ->
    _ = application:ensure_all_started(vmq_swc),
    supervisor:start_child(vmq_swc_sup,
                           #{id => {vmq_swc_store_sup, SwcGroup},
                             start => {vmq_swc_store_sup, start_link, [SwcGroup]},
                             type => supervisor}).

stop() ->
    application:stop(vmq_swc).

members(SwcGroup) ->
    vmq_swc_group_membership:get_members(config(SwcGroup)).

get(SwcGroup, Prefix, Key, Opts) ->
    PKey = {Prefix, Key},
    Default = get_option(default, Opts, undefined),
    % TODO: use something different than max_resolver by default, e.g.
    % wrapping the value with a TS and provide lWW...
    ResolveMethod = get_option(resolver, Opts, fun max_resolver/1),
    AllowPut = get_option(allow_put, Opts, true),

    Config = config(SwcGroup),

    case vmq_swc_store:read(Config, PKey) of
        {[], _} ->
            Default;
        Existing ->
            maybe_tombstone(maybe_resolve(Config, PKey, Existing, ResolveMethod, AllowPut), Default)
    end.

raw_get(SwcGroup, Prefix, Key) ->
    PKey = {Prefix, Key},
    Config = config(SwcGroup),
    vmq_swc_store:read(Config, PKey).

raw_put(SwcGroup, Prefix, Key, Value, Context) ->
    PKey = {Prefix, Key},
    Config = config(SwcGroup),
    vmq_swc_store:write(Config, PKey, Value, Context).

put(SwcGroup, Prefix, Key, ValueOrFun, _Opts) when not is_function(ValueOrFun) ->
    PKey = {Prefix, Key},
    Config = config(SwcGroup),
    Context = current_context(Config, PKey),
    vmq_swc_store:write(Config, PKey, ValueOrFun, Context).

delete_many(SwcGroup, Prefix, [_Key|_] = Keys) ->
    put_many(SwcGroup, Prefix, [{Key, '$deleted'} || Key <- Keys], []).

put_many(SwcGroup, Prefix, [{_Key, _ValueOrFun}|_] = Ops, _Opts) ->
    Config = config(SwcGroup),
    WriteBatch =
    lists:foldl(fun({Key, ValueOrFun}, Acc) ->
                        PKey = {Prefix, Key},
                        Context = current_context(Config, PKey),
                        [{PKey, ValueOrFun, Context}|Acc]
                end, [], Ops),
    vmq_swc_store:write_batch(Config, WriteBatch).

delete(SwcGroup, Prefix, Key) ->
    put(SwcGroup, Prefix, Key, '$deleted', []).

fold(SwcGroup, Fun, Acc, Prefix, Opts) ->
    Default = get_option(default, Opts, undefined),
    ResolveMethod = get_option(resolver, Opts, fun max_resolver/1),
    AllowPut = get_option(allow_put, Opts, true),
    Config = config(SwcGroup),

    vmq_swc_store:fold_values(Config,
      fun (_PKey, {[], _}, AccAcc) ->
              AccAcc;
          ({_Prefix, Key} = PKey, Existing, AccAcc) ->
              Fun(Key, maybe_tombstone(maybe_resolve(Config, PKey, Existing, ResolveMethod, AllowPut), Default), AccAcc)
      end, Acc, Prefix).

maybe_resolve(_Config, _PKey, {[Value], _Context}, _Method, _AllowPut) ->
    Value;
maybe_resolve(Config, PKey, {Values, Context}, ResolverFun, AllowPut) ->
    ResolvedValue = ResolverFun(Values),
    case AllowPut of
        true ->
            vmq_swc_store:write(Config, PKey, ResolvedValue, Context);
        false ->
            ok
    end,
    ResolvedValue.

current_context(Config, PKey) ->
    {_Values, Context} = vmq_swc_store:read(Config, PKey),
    Context.

max_resolver(Values) -> lists:max(Values).

maybe_tombstone('$deleted', Default) ->
    Default;
maybe_tombstone(Value, _Default) ->
    Value.

get_option(Key, Opts, Default) ->
    case lists:keyfind(Key, 1, Opts) of
        false -> Default;
        {_, Val} -> Val
    end.

config(SwcGroup) ->
    [{_, Config}] = ets:lookup(vmq_swc_group_config, SwcGroup),
    Config.
