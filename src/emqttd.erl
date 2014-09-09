-module(emqttd).
-export([start/0, stop/0]).

-define(DEFAULT_NR_OF_ACCEPTORS, 10).
-define(LOCALHOST, {127,0,0,1}).

start() ->
    application:load(mnesia_cluster),
    application:set_env(mnesia_cluster, table_definition_mod,
                        {emqttd_reg, emqttd_table_defs, []}),
    application:set_env(mnesia_cluster, cluster_monitor_callbacks, [emqttd_cluster]),
    application:set_env(mnesia_cluster, app_process, emqttd_cluster),
    application:ensure_all_started(emqttd_server),

    emqttd_auth:register_hooks(),

    case proplists:get_value(emqttd_port, init:get_arguments()) of
        [StringPort] ->
            Endpoint = {?LOCALHOST, list_to_integer(StringPort), ?DEFAULT_NR_OF_ACCEPTORS},
            emqttd_endpoint_sup:add_endpoint(Endpoint);
        undefined ->
            ok
    end,
    case proplists:get_value(emqttd_ws_port, init:get_arguments()) of
        [StringPortWS] ->
            EndpointWS = {?LOCALHOST, list_to_integer(StringPortWS), ?DEFAULT_NR_OF_ACCEPTORS},
            emqttd_endpoint_sup:add_ws_endpoint(EndpointWS);
        undefined ->
            ok
    end.

stop() ->
    application:stop(emqttd_server),
    application:stop(emqtt_commons),
    application:stop(mnesia_cluster),
    application:stop(locks),
    application:stop(mnesia),
    application:stop(bitcask),
    application:stop(ranch).

