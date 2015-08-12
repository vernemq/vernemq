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
    Datadir = "data/" ++ atom_to_list(node()),
    application:load(plumtree),
    application:set_env(plumtree, plumtree_data_dir, Datadir),
    application:set_env(plumtree, metadata_root, Datadir ++ "/meta/"),
    application:load(vmq_server),
    PrivDir = code:priv_dir(vmq_server),
    application:set_env(vmq_server, schema_dirs, [PrivDir]),
    application:set_env(vmq_server, listeners, [{vmq, [{{{0,0,0,0}, random_port()}, []}]}]),
    application:set_env(vmq_server, ignore_db_config, true),
    application:set_env(vmq_server, lvldb_store_dir, Datadir ++ "/msgstore"),
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
    reset_all(),
    start_server_(StartNoAuth),
    %wait_til_ready(),
    disable_all_plugins().

start_server_(_StartNoAuth = true) ->
    vmq_server:start_no_auth();
start_server_(_StartNoAuth = false) ->
    vmq_server:start().

random_port() ->
    10000 + (erlang:phash2(node()) rem 10000).

teardown() ->
    disable_all_plugins(),
    vmq_server:stop().

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

reset_all() ->
    {ok, PlumtreeDir} = application:get_env(plumtree, plumtree_data_dir),
    filelib:ensure_dir(PlumtreeDir ++ "/ptmp"),
    del_dir(PlumtreeDir),
    {ok, MsgStoreDir} = application:get_env(vmq_server, lvldb_store_dir),
    filelib:ensure_dir(MsgStoreDir ++ "/ptmp"),
    del_dir(MsgStoreDir).

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

del_dir(Dir) ->
    lists:foreach(fun(D) ->
                          ok = file:del_dir(D)
                  end, del_all_files([Dir], [])).

del_all_files([], EmptyDirs) ->
    EmptyDirs;
del_all_files([Dir | T], EmptyDirs) ->
    {ok, FilesInDir} = file:list_dir(Dir),
    {Files, Dirs} = lists:foldl(fun(F, {Fs, Ds}) ->
                                        Path = Dir ++ "/" ++ F,
                                        case filelib:is_dir(Path) of
                                            true ->
                                                {Fs, [Path | Ds]};
                                            false ->
                                                {[Path | Fs], Ds}
                                        end
                                end, {[],[]}, FilesInDir),
    lists:foreach(fun(F) ->
                          ok = file:delete(F)
                  end, Files),
    del_all_files(T ++ Dirs, [Dir | EmptyDirs]).
