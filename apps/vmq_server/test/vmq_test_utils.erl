-module(vmq_test_utils).
-export([setup/0,
         teardown/0,
         reset_tables/0,
         maybe_start_distribution/1,
         get_suite_rand_seed/0,
         seed_rand/1,
         rand_bytes/1]).

setup() ->
    os:cmd(os:find_executable("epmd")++" -daemon"),
    NodeName = list_to_atom("vmq_server-" ++ integer_to_list(erlang:phash2(os:timestamp()))),
    ok = maybe_start_distribution(NodeName),
    Datadir = "/tmp/vernemq-test/data/" ++ atom_to_list(node()),
    os:cmd("rm -rf " ++ Datadir),
   % application:load(plumtree),
   % application:set_env(plumtree, plumtree_data_dir, Datadir),
   % application:set_env(plumtree, metadata_root, Datadir ++ "/meta/"),
    application:load(vmq_swc),
    application:set_env(vmq_swc, db_backend, leveldb),
    application:set_env(vmq_swc, data_dir, Datadir),
    application:set_env(vmq_swc, metadata_root, Datadir),
    application:load(vmq_server),
    PrivDir = code:priv_dir(vmq_server),
    application:set_env(vmq_server, listeners, [{vmq, [{{{0,0,0,0}, random_port()}, []}]}]),
    application:set_env(vmq_server, ignore_db_config, true),
    application:load(vmq_plugin),
    application:set_env(vmq_plugin, default_schema_dir, [PrivDir]),
    application:set_env(vmq_server, metadata_impl, vmq_swc),
    application:load(vmq_generic_msg_store),
    % application:set_env(vmq_generic_msg_store, msg_store_opts, [
    %                                                  {store_dir, Datadir ++ "/msgstore"},
    %                                                  {open_retries, 30},
    %                                                  {open_retry_delay, 2000}
    %                                                 ]),
    application:set_env(vmq_generic_msg_store, msg_store_engine, vmq_storage_engine_leveldb),
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
    vmq_server:start_no_auth(),
    disable_all_plugins().

random_port() ->
    10000 + (erlang:phash2(node()) rem 10000).

teardown() ->
    disable_all_plugins(),
    vmq_metrics:reset_counters(),
    vmq_server:stop(),
    vmq_swc:stop(),
    application:unload(vmq_swc),
    application:unload(vmq_server),
    Datadir = "/tmp/vernemq-test/data/" ++ atom_to_list(node()),
    _ = [eleveldb:destroy(Datadir ++ "/meta/" ++ integer_to_list(I), [])
         || I <- lists:seq(0, 11)],
    _ = [eleveldb:destroy(Datadir ++ "/msgstore/" ++ integer_to_list(I), [])
         || I <- lists:seq(0, 11)],
    eleveldb:destroy(Datadir ++ "/trees", []),
    ok.

disable_all_plugins() ->
    {ok, Plugins} = vmq_plugin_mgr:get_plugins(),
    %% Disable App Pluginns
    lists:foreach(fun %({application, vmq_plumtree, _}) ->
                          % don't disable metadata plugin
                       %   ignore;
                       ({application, vmq_swc, _}) ->
                                ignore;
                        ({application, vmq_generic_msg_store, _}) ->
                          % don't disable message store plugin
                          ignore;
                        ({application, App, _Hooks}) ->
                          vmq_plugin_mgr:disable_plugin(App);
                        (_ModPlugins) ->
                          ignore
                  end, Plugins),
    %% Disable Mod Plugins
    lists:foreach(fun ({_, vmq_lvldb_store, _, _}) ->
                          ignore;
                      (P) ->
                          vmq_plugin_mgr:disable_plugin(P)
                  end, vmq_plugin:info(all)).

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
    vmq_metadata:fold({vmq, Tab},
      fun({Key, V}, _) ->
              case V =/= '$deleted' of
                  true ->
                      vmq_metadata:delete({vmq, Tab}, Key);
                  _ ->
                      ok
              end
      end, ok).

get_suite_rand_seed() ->
    %% To set the seed when running a test from the command line,
    %% create a config.cfg file containing `{seed, {x,y,z}}.` with the
    %% desired seed in there. Then run the tests like this:
    %%
    %% `./rebar3 ct --config config.cfg ...`
    Seed =
        case ct:get_config(seed, undefined) of
            undefined ->
                os:timestamp();
            X ->
                X
        end,
    io:format(user, "Suite random seed: ~p~n", [Seed]),
    {seed, Seed}.

seed_rand(Config) ->
    Seed =
        case proplists:get_value(seed, Config, undefined) of
            undefined ->
                throw("No seed found in Config");
            S -> S
        end,
    rand:seed(exsplus, Seed).

rand_bytes(N) ->
    L = [ rand:uniform(256)-1 || _ <- lists:seq(1,N)],
    list_to_binary(L).
