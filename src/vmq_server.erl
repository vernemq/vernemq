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
-export([start/0,
         start_no_auth/0,
         start_no_auth/1,
         stop/0,
         table_defs/0]).

-spec start() -> 'ok'.
start_no_auth() ->
    maybe_start_distribution(),

    application:load(vmq_plugin),
    application:set_env(vmq_plugin, wait_for_proc, vmq_server_sup),

    application:load(mnesia_cluster),
    application:set_env(mnesia_cluster, table_definition_mod,
                        {?MODULE, table_defs, []}),
    application:set_env(mnesia_cluster, cluster_monitor_callbacks,
                        [vmq_cluster]),
    application:set_env(mnesia_cluster, app_process, vmq_server_sup),
    application:set_env(mnesia_cluster, cluster_partition_handling,
                        ignore), % we use unsplit
    %application:load(sasl),
    %application:set_env(sasl, sasl_error_logger, false),
    application:ensure_all_started(vmq_server),
    ok.

start_no_auth(ClusterNode) ->
    maybe_start_distribution(),

    application:load(vmq_plugin),
    application:set_env(vmq_plugin, wait_for_proc, vmq_server_sup),

    application:load(mnesia_cluster),
    application:set_env(mnesia_cluster, table_definition_mod,
                        {?MODULE, table_defs, []}),
    application:set_env(mnesia_cluster, cluster_monitor_callbacks,
                        [vmq_cluster]),
    application:set_env(mnesia_cluster, app_process, vmq_server_sup),
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
    application:stop(emqtt_commons),
    application:stop(cowlib),
    application:stop(vmq_server),
    application:stop(asn1),
    application:stop(public_key),
    application:stop(vmq_plugin),
    application:stop(mnesia),
    application:stop(crypto),
    application:stop(ssl),
    application:stop(riak_sysmon),
    application:stop(os_mon),
    application:stop(jobs),
    application:stop(lager).

maybe_start_distribution() ->
    case ets:info(sys_dist) of
        undefined ->
            %% started without -sname or -name arg
            {ok, _} = net_kernel:start([vmq_server, shortnames]);
        _ ->
            ok
    end.

table_defs() ->
    VmqRegTables = vmq_reg:table_defs(),
    VmqConfigTables = vmq_config:table_defs(),
    VmqMsgStoreTables = vmq_msg_store:table_defs(),
    VmqRegTables ++ VmqConfigTables ++ VmqMsgStoreTables.
