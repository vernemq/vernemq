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

-module(vmq_server_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

-spec start(_, _) -> {'error', _} | {'ok', pid()} | {'ok', pid(), _}.
start(_StartType, _StartArgs) ->
    ok = vmq_metadata:start(),
    ok = vmq_message_store:start(),

    case vmq_server_sup:start_link() of
        {error, _} = E ->
            E;
        R ->
            %% we'll wait for some millis, this
            %% enables the vmq_plugin mechanism to be prepared...
            %% vmq_plugin_mgr waits for the 'vmq_server_sup' process
            %% to be registered.
            timer:sleep(500),
            vmq_server_cli:init_registry(),
            start_user_plugins(),
            vmq_config:configure_node(),
            R
    end.

start_user_plugins() ->
    Plugins = application:get_env(vmq_server, user_plugins, []),
    [ start_user_plugin(P) || P <- Plugins ].

start_user_plugin({_Order, #{path := Path,
                             name := PluginName}}) ->
    Res = case Path of
              undefined ->
                  vmq_plugin_mgr:enable_plugin(PluginName);
              _ ->
                  vmq_plugin_mgr:enable_plugin(PluginName, [{path, Path}])
          end,
    case Res of
        ok ->
            ok;
        {error, Reason} ->
            lager:warning("could not start plugin ~p due to ~p", [PluginName, Reason])
    end.

-spec stop(_) -> 'ok'.
stop(_State) ->
    _ = vmq_message_store:stop(),
    _ = vmq_metadata:stop(),
    ok.
