%% Copyright 2014 Erlio GmbH Basel Switzerland (http://erl.io)
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

-module(vmq_server).
-export([start/0, start_no_auth/0, start_no_auth/1, stop/0]).

-spec start() -> 'ok'.
start_no_auth() ->
    application:load(mnesia_cluster),
    application:set_env(mnesia_cluster, table_definition_mod,
                        {vmq_reg, vmq_table_defs, []}),
    application:set_env(mnesia_cluster, cluster_monitor_callbacks,
                        [vmq_cluster]),
    application:set_env(mnesia_cluster, app_process, vmq_cluster),
    application:set_env(mnesia_cluster, cluster_partition_handling,
                        ignore), % we use unsplit
    application:ensure_all_started(vmq_server),
    ok.

start_no_auth(ClusterNode) ->
    application:load(mnesia_cluster),
    application:set_env(mnesia_cluster, table_definition_mod,
                        {vmq_reg, vmq_table_defs, []}),
    application:set_env(mnesia_cluster, cluster_monitor_callbacks,
                        [vmq_cluster]),
    application:set_env(mnesia_cluster, app_process, vmq_cluster),
    application:set_env(mnesia_cluster, cluster_partition_handling,
                        ignore), % we use unsplit
    application:set_env(mnesia_cluster, cluster_nodes, {[ClusterNode], ram}),
    application:ensure_all_started(vmq_server),
    ok.


start() ->
    start_no_auth(),
    vmq_auth:register_hooks().


-spec stop() -> 'ok' | {'error',_}.
stop() ->
    application:stop(vmq_server),
    application:stop(mnesia_cluster),
    application:stop(unsplit),
    application:stop(ranch),
    application:stop(emqtt_commons),
    application:stop(cowlib),
    application:stop(vmq_server),
    application:stop(asn1),
    application:stop(public_key),
    application:stop(cowboy),
    application:stop(bitcask),
    application:stop(mnesia),
    application:stop(crypto),
    application:stop(ssl),
    application:stop(os_mon),
    application:stop(jobs),
    application:stop(lager).

