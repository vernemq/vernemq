-module(vmq_test_utils).
-export([setup/0,
         setup_use_default_auth/0,
         teardown/0]).

setup() ->
    start_server(true).
setup_use_default_auth() ->
    start_server(false).

start_server(StartNoAuth) ->
    ok = maybe_start_distribution(),
    mnesia_cluster_utils:force_reset(),
    application:load(vmq_server),
    application:set_env(vmq_server, schema_dirs, ["../priv"]),
    application:set_env(vmq_server, listeners, []),
    application:set_env(vmq_server, ignore_mnesia_config, true),
    start_server_(StartNoAuth),
    wait_til_ready(),
    disable_all_plugins().

start_server_(_StartNoAuth = true) ->
    vmq_server:start_no_auth();
start_server_(_StartNoAuth = false) ->
    vmq_server:start().

teardown() ->
    disable_all_plugins(),
    vmq_server:stop(),
    mnesia_cluster_utils:force_reset().

disable_all_plugins() ->
    _ = [vmq_plugin_mgr:disable_plugin(P) || P <- vmq_plugin:info(all)],
    ok.

wait_til_ready() ->
    wait_til_ready(vmq_cluster:is_ready(), 100).
wait_til_ready(true, _) -> ok;
wait_til_ready(false, I) when I > 0 ->
    timer:sleep(5),
    wait_til_ready(vmq_cluster:is_ready(), I - 1);
wait_til_ready(_, _) -> exit(not_ready).

maybe_start_distribution() ->
    case ets:info(sys_dist) of
        undefined ->
            %% started without -sname or -name arg
            {ok, _} = net_kernel:start([vmq_server, shortnames]),
            ok;
        _ ->
            ok
    end.
