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

-module(vmq_diversity_vmq_api).
-include_lib("luerl/include/luerl.hrl").

-export([install/1]).

install(St) ->
    luerl_emul:alloc_table(table(), St).

table() ->
    [
        {<<"disconnect_by_subscriber_id">>, #erl_func{code = fun disconnect/2}},
        {<<"reauthorize_subscriptions">>, #erl_func{code = fun reauthorize_subscriptions/2}}
    ].

disconnect([LuaSubId, LuaOpts], St) ->
    SubId = luerl:decode(LuaSubId, St),
    %% mountpoints from lua are restricted to the list/string type.
    MP = to_list(mp, proplists:get_value(<<"mountpoint">>, SubId)),
    ClientId = proplists:get_value(<<"client_id">>, SubId),
    case ClientId of
        undefined -> throw({missing_parameter, client_id});
        _ -> ok
    end,

    Opts = conv_opts(luerl:decode(LuaOpts, St)),
    Res = try_fun(
        fun() -> vernemq_dev_api:disconnect_by_subscriber_id({MP, ClientId}, Opts) end, not_found
    ),
    {[atom_to_binary(Res, utf8)], St}.

reauthorize_subscriptions([LuaUser, LuaSubId, LuaOpts], St) ->
    User = luerl:decode(LuaUser, St),
    SubId = luerl:decode(LuaSubId, St),
    %% mountpoints from lua are restricted to the list/string type.
    MP = to_list(mp, proplists:get_value(<<"mountpoint">>, SubId)),
    ClientId = proplists:get_value(<<"client_id">>, SubId),
    case ClientId of
        undefined -> throw({missing_parameter, client_id});
        _ -> ok
    end,

    Opts = conv_opts(luerl:decode(LuaOpts, St)),
    _ = try_fun(
        fun() -> vernemq_dev_api:reauthorize_subscriptions(User, {MP, ClientId}, Opts) end, ignored
    ),
    {[atom_to_binary(ok, utf8)], St}.

try_fun(Fun, CatchVal) ->
    %% wrap it in a try-catch to make it possible to verify in
    %% tests, syntactically, that we can call the function. To
    %% test this for real, we'd need a running `vmq_server`.
    try
        Fun()
    catch
        _:_ ->
            CatchVal
    end.

conv_opts(Opts) ->
    lists:map(
        fun
            ({<<"do_cleanup">>, true}) ->
                do_cleanup;
            ({<<"do_cleanup">>, false}) ->
                {do_cleanup, false};
            (E) ->
                E
        end,
        Opts
    ).

to_list(Name, undefined) ->
    throw({missing_parameter, Name});
to_list(_, Binary) ->
    binary_to_list(Binary).
