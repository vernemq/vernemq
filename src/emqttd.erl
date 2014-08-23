-module(emqttd).
-export([start/0, stop/0]).

start() ->
    application:load(mnesia_cluster),
    application:set_env(mnesia_cluster, table_definition_mod,
                        {emqttd_trie, emqttd_table_defs, []}),
    application:set_env(mnesia_cluster, cluster_monitor_callbacks, [emqttd_trie]),
    application:set_env(mnesia_cluster, app_process, emqttd_trie),
    application:ensure_all_started(emqttd).

stop() ->
    application:stop(emqttd),
    application:stop(emqtt_commons),
    application:stop(mnesia_cluster),
    application:stop(locks),
    application:stop(mnesia),
    application:stop(bitcask),
    application:stop(ranch).

