-module(vmq_test_utils).
-export([setup/0,
         setup_use_default_auth/0,
         teardown/0,
         reset_tables/0,
         maybe_start_distribution/1]).

setup() ->
    start_server(true).
setup_use_default_auth() ->
    start_server(false).

start_server(StartNoAuth) ->
    os:cmd(os:find_executable("epmd")++" -daemon"),
    ok = maybe_start_distribution(vmq_server),
    Datadir = "/tmp/vernemq-test/data/" ++ atom_to_list(node()),
    os:cmd("rm -rf " ++ Datadir),
    application:load(plumtree),
    application:set_env(plumtree, plumtree_data_dir, Datadir),
    application:set_env(plumtree, metadata_root, Datadir ++ "/meta/"),
    application:load(vmq_server),
    PrivDir = code:priv_dir(vmq_server),
    application:set_env(vmq_server, schema_dirs, [PrivDir]),
    application:set_env(vmq_server, listeners, [{vmq, [{{{0,0,0,0}, random_port()}, []}]}]),
    application:set_env(vmq_server, ignore_db_config, true),
    application:set_env(vmq_server, msg_store_opts, [
                                                     {store_dir, Datadir ++ "/msgstore"},
                                                     {open_retries, 30},
                                                     {open_retry_delay, 2000}
                                                    ]),
    LogDir = "log." ++ atom_to_list(node()),
    application:load(lager),
    application:set_env(lager, handlers, [
                                          {lager_file_backend,
                                           [{file, LogDir ++ "/console.log"},
                                            {level, info},
                                            {size,10485760},
                                            {date,"$D0"},
                                            {count,5}]},
                                          {lager_file_backend,
                                           [{file, LogDir ++ "/error.log"},
                                            {level, error},
                                            {size,10485760},
                                            {date,"$D0"},
                                            {count,5}]}]),
    start_server_(StartNoAuth),
    disable_all_plugins().

start_server_(_StartNoAuth = true) ->
    vmq_server:start_no_auth();
start_server_(_StartNoAuth = false) ->
    vmq_server:start().

random_port() ->
    10000 + (erlang:phash2(node()) rem 10000).

teardown() ->
    disable_all_plugins(),
    vmq_metrics:reset_counters(),
    vmq_server:stop(),
    Datadir = "/tmp/vernemq-test/data/" ++ atom_to_list(node()),
    _ = [eleveldb:destroy(Datadir ++ "/meta/" ++ integer_to_list(I), [])
         || I <- lists:seq(0, 11)],
    _ = [eleveldb:destroy(Datadir ++ "/msgstore/" ++ integer_to_list(I), [])
         || I <- lists:seq(0, 11)],
    eleveldb:destroy(Datadir ++ "/trees", []),
    ok.

disable_all_plugins() ->
    _ = [vmq_plugin_mgr:disable_plugin(P) || P <- vmq_plugin:info(all)],
    ok.

maybe_start_distribution(Name) ->
    case ets:info(sys_dist) of
        undefined ->
            %% started without -sname or -name arg
            {ok, _} = net_kernel:start([Name, shortnames]),
            ok;
        _ ->
            ok
    end.

reset_tables() ->
    _ = [reset_tab(T) || T <- [subscriber, config, retain]],
    %% it might be possible that a cached retained message
    %% isn't yet persisted in plumtree
    ets:delete_all_objects(vmq_retain_srv),
    ok.

reset_tab(Tab) ->
    plumtree_metadata:fold(
      fun({Key, V}, _) ->
              case V =/= '$deleted' of
                  true ->
                      plumtree_metadata:delete({vmq, Tab}, Key);
                  _ ->
                      ok
              end
      end, ok, {vmq, Tab}).
