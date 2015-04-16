-module(vmq_test_utils).
-export([setup/0,
         setup_use_default_auth/0,
         teardown/0,
         reset_tables/0]).

setup() ->
    start_server(true).
setup_use_default_auth() ->
    start_server(false).

start_server(StartNoAuth) ->
    ok = maybe_start_distribution(),
    application:load(plumtree),
    application:set_env(plumtree, plumtree_data_dir, "data/" ++ atom_to_list(node())),
    application:load(vmq_server),
    %% CWD using rebar3 is _build/logs/ct_run.nodename.YYYY-MM-DD_hh.mm.ss
    PrivDir =
    case {filelib:is_dir("./priv"),
          filelib:is_dir("../../priv")} of
        {true, _} ->
            "./priv";
        {_, true} ->
            "../../priv"
    end,

    application:set_env(vmq_server, schema_dirs, [PrivDir]),
    application:set_env(vmq_server, listeners, []),
    application:set_env(vmq_server, ignore_db_config, true),
    reset_all(),
    start_server_(StartNoAuth),
    %wait_til_ready(),
    disable_all_plugins().

start_server_(_StartNoAuth = true) ->
    vmq_server:start_no_auth();
start_server_(_StartNoAuth = false) ->
    vmq_server:start().

teardown() ->
    disable_all_plugins(),
    vmq_server:stop().

disable_all_plugins() ->
    _ = [vmq_plugin_mgr:disable_plugin(P) || P <- vmq_plugin:info(all)],
    ok.

maybe_start_distribution() ->
    case ets:info(sys_dist) of
        undefined ->
            %% started without -sname or -name arg
            {ok, _} = net_kernel:start([vmq_server, shortnames]),
            ok;
        _ ->
            ok
    end.

reset_all() ->
    {ok, Dir} = application:get_env(plumtree, plumtree_data_dir),
    filelib:ensure_dir(Dir ++ "/ptmp"),
    del_dir(Dir).

reset_tables() ->
    _ = [reset_tab(T) || T <- [subscriber, config, retain]],
    ok.

reset_tab(Tab) ->
    plumtree_metadata:fold(
      fun({Key, _}, _) ->
              plumtree_metadata:delete({vmq, Tab}, Key)
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
