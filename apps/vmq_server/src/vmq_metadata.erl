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

-module(vmq_metadata).
-export([start/0,
         stop/0,
         put/3,
         get/2,
         delete/2,
         fold/3,
         subscribe/1]).

start() ->
    Impl = application:get_env(vmq_server, metadata_impl, vmq_plumtree),
    Ret = vmq_plugin_mgr:enable_system_plugin(Impl, []),
    lager:info("Try to start ~p: ~p", [Impl, Ret]),
    Ret.

stop() ->
    Impl = application:get_env(vmq_server, metadata_impl, vmq_plumtree),
    Ret = vmq_plugin_mgr:disable_plugin(Impl, []),
    lager:info("Try to start ~p: ~p", [Impl, Ret]),
    Ret.


put(FullPrefix, Key, Value) ->
    vmq_plugin:only(metadata_put, [FullPrefix, Key, Value]).

get(FullPrefix, Key) ->
    vmq_plugin:only(metadata_get, [FullPrefix, Key]).

delete(FullPrefix, Key) ->
    vmq_plugin:only(metadata_delete, [FullPrefix, Key]).

fold(FullPrefix, Fun, Acc) ->
    vmq_plugin:only(metadata_fold, [FullPrefix, Fun, Acc]).

subscribe(FullPrefix) ->
    vmq_plugin:only(metadata_subscribe, [FullPrefix]).


