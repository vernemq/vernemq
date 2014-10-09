-module(vmq).
-export([start/0, start_no_auth/0, stop/0]).

-spec start() -> 'ok'.
start_no_auth() ->
    application:set_env(vmq_server, hipe_compile, false),
    application:load(mnesia_cluster),
    application:set_env(mnesia_cluster, table_definition_mod,
                        {vmq_reg, vmq_table_defs, []}),
    application:set_env(mnesia_cluster, cluster_monitor_callbacks,
                        [vmq_cluster]),
    application:set_env(mnesia_cluster, app_process, vmq_cluster),
    application:ensure_all_started(vmq_server),
    ok.

start() ->
    start_no_auth(),
    vmq_auth:register_hooks().


-spec stop() -> 'ok' | {'error',_}.
stop() ->
    application:stop(vmq_server),
    application:stop(mnesia_cluster),
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
    application:stop(lager).

