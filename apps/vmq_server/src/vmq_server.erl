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

-module(vmq_server).
-export([start/0,
         start_no_auth/0,
         start_no_auth/1,
         stop/0]).

-spec start_no_auth() -> 'ok'.
start_no_auth() ->
    maybe_start_distribution(),

    _ = application:load(vmq_plugin),
    application:set_env(vmq_plugin, wait_for_proc, vmq_server_sup),
    %application:load(sasl),
    %application:set_env(sasl, sasl_error_logger, false),
    _ = application:ensure_all_started(vmq_server),
    ok.

-spec start_no_auth(node()) -> any().
start_no_auth(ClusterNode) ->
    maybe_start_distribution(),

    _ = application:load(vmq_plugin),
    application:set_env(vmq_plugin, wait_for_proc, vmq_server_sup),
    _ = application:ensure_all_started(vmq_server),
    vmq_peer_service:join(ClusterNode).

-spec start() -> 'ok'.
start() ->
    _ = application:load(plumtree),
    application:set_env(plumtree, plumtree_data_dir, "./data/" ++ atom_to_list(node())),
    application:set_env(plumtree, storage_mod, plumtree_leveldb_metadata_manager),
    start_no_auth(),
    vmq_auth:register_hooks().


-spec stop() -> 'ok'.
stop() ->
    application:stop(vmq_server),
    wait_until_metadata_has_stopped(),
    _ = [application:stop(App) || App <- [vmq_plugin,
                                          riak_sysmon,
                                          clique,
                                          asn1,
                                          public_key,
                                          cowboy,
                                          ranch,
                                          crypto,
                                          ssl,
                                          os_mon,
                                          lager]],
    ok.

-spec wait_until_metadata_has_stopped() -> 'ok'.
wait_until_metadata_has_stopped() ->
    Impl = application:get_env(vmq_server, metadata_impl, vmq_plumtree),
    case lists:keymember(Impl, 1, application:which_applications()) of
        true ->
            timer:sleep(100),
            wait_until_metadata_has_stopped();
        false ->
            ok
    end.

-spec maybe_start_distribution() -> 'ok'.
maybe_start_distribution() ->
    case ets:info(sys_dist) of
        undefined ->
            %% started without -sname or -name arg
            {ok, _} = net_kernel:start([vmq_server, shortnames]),
            ok;
        _ ->
            ok
    end.
