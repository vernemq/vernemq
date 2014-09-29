-module(vmq).
-export([start/0, stop/0]).

-define(DEFAULT_NR_OF_ACCEPTORS, 10).
-define(LOCALHOST, {127, 0, 0, 1}).

-spec start() -> 'ok'.
start() ->
    application:load(mnesia_cluster),
    application:set_env(mnesia_cluster, table_definition_mod,
                        {vmq_reg, vmq_table_defs, []}),
    application:set_env(mnesia_cluster, cluster_monitor_callbacks,
                        [vmq_cluster]),
    application:set_env(mnesia_cluster, app_process, vmq_cluster),
    application:ensure_all_started(vmq_server),

    vmq_auth:register_hooks().


-spec stop() -> 'ok' | {'error',_}.
stop() ->
    application:stop(vmq_server),
    application:stop(emqtt_commons),
    application:stop(mnesia_cluster),
    application:stop(locks),
    application:stop(mnesia),
    application:stop(bitcask),
    application:stop(ranch).

