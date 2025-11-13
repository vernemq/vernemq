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

-module(vmq_http_config).
-include_lib("kernel/include/logger.hrl").

-export([config/1, auth_mode/2]).

-callback routes() -> [{string(), atom(), any()}].
config(Opts) ->
    HttpLocal = element(2, lists:keyfind(http_modules, 1, Opts)),
    HttpLocalList = [
        list_to_atom(string:strip(Token))
     || Token <- string:tokens(string:trim(HttpLocal, both, "[]"), ",")
    ],

    HttpModules =
        case {HttpLocalList, vmq_config:get_env(http_modules)} of
            {[], _} -> vmq_config:get_env(http_modules);
            {LocalModules, _} -> LocalModules
        end,
    config(HttpModules, []).

config([HttpModule | Rest], Routes) when is_atom(HttpModule) ->
    try
        ModRoutes = apply(HttpModule, routes, []),
        config(Rest, Routes ++ ModRoutes)
    catch
        E:R ->
            ?LOG_ERROR("can't call ~p:routes() due to ~p ~p", [HttpModule, E, R]),
            config(Rest, Routes)
    end;
config([], Routes) ->
    Routes.

auth_mode(Req, Module) ->
    Reg = maps:get(ref, Req),
    Scheme = binary_to_atom(maps:get(scheme, Req)),
    Opts = ranch:get_protocol_options(Reg),
    Global = maps:get(
        Module,
        vmq_config:get_env(vmq_server, http_modules_auth, #{}),
        undefined
    ),
    ListenerName =
        case lists:keyfind(listener_name, 1, maps:get(opts, Opts)) of
            false -> undefined;
            Tuple -> list_to_atom(element(2, Tuple))
        end,
    Local = maps:get(
        {Scheme, ListenerName, Module},
        vmq_config:get_env(vmq_server, http_listener_modules_auth, #{}),
        undefined
    ),

    case {Local, Global} of
        {undefined, undefined} -> undefined;
        {undefined, G} -> G;
        {L, _} -> L
    end.
