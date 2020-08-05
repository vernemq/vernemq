%% Copyright 2019 Octavo Labs AG Zurich Switzerland (http://octavolabs.com)
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

-module(vmq_message_store).
-include("vmq_server.hrl").
-export([start/0,
         stop/0,
         write/2,
         read/2,
         delete/2,
         find/2]).

start() ->
    Impl = application:get_env(vmq_server, message_store_impl, vmq_generic_msg_store),
    Ret = vmq_plugin_mgr:enable_system_plugin(Impl, [internal]),
    lager:info("Try to start ~p: ~p", [Impl, Ret]),
    Ret.

stop() ->
    % vmq_message_store:stop is typically called when stopping the vmq_server
    % OTP application. As vmq_plugin_mgr:disable_plugin is likely stopping
    % another OTP application too we might block the OTP application
    % controller. Wrapping the disable_plugin in its own process would
    % enable to stop the involved applications. Moreover, because an
    % application:stop is actually a gen_server:call to the application
    % controller the order of application termination is still provided.
    % Nevertheless, this is of course only a workaround and the problem
    % needs to be addressed when reworking the plugin system.
    Impl = application:get_env(vmq_server, message_store_impl, vmq_generic_msg_store),
    _ = spawn(fun() ->
                      Ret = vmq_plugin_mgr:disable_plugin(Impl),
                      lager:info("Try to stop ~p: ~p", [Impl, Ret])
              end),
    ok.

write(SubscriberId, Msg) ->
    vmq_plugin:only(msg_store_write, [SubscriberId, Msg]).

read(SubscriberId, MsgRef) ->
    vmq_plugin:only(msg_store_read, [SubscriberId, MsgRef]).

delete(SubscriberId, MsgRef) ->
    vmq_plugin:only(msg_store_delete, [SubscriberId, MsgRef]).

find(SubscriberId, Type) when Type =:= queue_init;
                              Type =:= other ->
    vmq_plugin:only(msg_store_find, [SubscriberId, Type]).
