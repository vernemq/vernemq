-module(vmq_plugin_SUITE).
-include_lib("common_test/include/ct.hrl").

-compile(export_all).
-compile(nowarn_export_all).

all() ->
    [app_hook_without_proplist,
     bad_plugin_does_not_get_saved,
     good_plugin_gets_saved,
     cannot_enable_duplicate_module_plugin,
     cannot_enable_duplicate_app_plugin,
     module_plugin_mod_does_not_exist,
     module_plugin_function_does_not_exist,
     app_plugin_mod_does_not_exist,
     app_plugin_function_does_not_exist,
     cannot_load_plugin_with_existing_mod,
     plugin_with_compat_hooks,
     module_plugin_with_compat_hooks
    ].

init_per_suite(Config) ->
    application:ensure_all_started(lager),
    %%lager:set_loglevel(lager_console_backend, debug),
    Config.

init_per_testcase(_, Config) ->
    application:load(vmq_plugin),
    Config.

end_per_testcase(_, _Config) ->
    application:stop(vmq_plugin),
    application:unload(vmq_plugin),
    ok.

end_per_suite(_Config) ->
    ok.

%% Tests
app_hook_without_proplist(_Config) ->
    {ok, _} = application:ensure_all_started(vmq_plugin),
    Hooks = [{?MODULE, sample_hook_function, 0},
             {hookname, ?MODULE, sample_hook_function, 0}],
    application:set_env(vmq_plugin, vmq_plugin_hooks, Hooks),
    ok = vmq_plugin_mgr:enable_plugin(vmq_plugin, [{path, code:lib_dir(vmq_plugin)}]).

bad_plugin_does_not_get_saved(_Config) ->
    {ok, _} = application:ensure_all_started(vmq_plugin),
    {error, _} = vmq_plugin_mgr:enable_plugin(bad_app,  [{path, "bad_dir"}]),
    %% Expect that the bad plugin has not been written to file.
    {ok, []} = vmq_plugin_mgr:get_plugins().

good_plugin_gets_saved(_Config) ->
    {ok, _} = application:ensure_all_started(vmq_plugin),
    %% `vmq_plugin` is an application, so we can cheat and use that as
    %% a plugin.
    Path = code:lib_dir(vmq_plugin),
    ok = vmq_plugin_mgr:enable_plugin(vmq_plugin, [{path, code:lib_dir(vmq_plugin)}]),
    {ok, [{application, vmq_plugin, [{hooks, []}, {path, Path}]}]} = vmq_plugin_mgr:get_plugins().

cannot_enable_duplicate_module_plugin(_Config) ->
    {ok, _} = application:ensure_all_started(vmq_plugin),
    ok = vmq_plugin_mgr:enable_module_plugin(hookname, ?MODULE, sample_hook_function, 0),
    %% Enabling a module twice does not throw an error.
    ok = vmq_plugin_mgr:enable_module_plugin(hookname, ?MODULE, sample_hook_function, 0),
    %% but it does not get written to the config file again.
    {ok, [{module, ?MODULE, [{hooks, [{hook, hookname, ?MODULE, sample_hook_function, 0, undefined, []}]}]}]} =
        vmq_plugin_mgr:get_plugins().

cannot_enable_duplicate_app_plugin(_Config) ->
    {ok, _} = application:ensure_all_started(vmq_plugin),
    ok = vmq_plugin_mgr:enable_plugin(vmq_plugin, [{path, code:lib_dir(vmq_plugin)}]),
    {error,already_enabled} = vmq_plugin_mgr:enable_plugin(vmq_plugin, [{path, code:lib_dir(vmq_plugin)}]).

module_plugin_mod_does_not_exist(_Config) ->
    {ok, _} = application:ensure_all_started(vmq_plugin),
    {error, {unknown_module,nonexistent_mod}} =
        vmq_plugin_mgr:enable_module_plugin(hookname, nonexistent_mod, sample_hook_function, 0).

module_plugin_function_does_not_exist(_Config) ->
    {ok, _} = application:ensure_all_started(vmq_plugin),
    {error,{no_matching_fun_in_module,vmq_plugin_SUITE,nonexistent_fun,0}} =
        vmq_plugin_mgr:enable_module_plugin(hookname, ?MODULE, nonexistent_fun, 0).

app_plugin_mod_does_not_exist(_Config) ->
    {ok, _} = application:ensure_all_started(vmq_plugin),
    Hooks = [{nonexistent_mod,sample_hook,0, []}],
    application:set_env(vmq_plugin, vmq_plugin_hooks, Hooks),
    {error, {unknown_module,nonexistent_mod}} =
        vmq_plugin_mgr:enable_plugin(vmq_plugin, [{path, code:lib_dir(vmq_plugin)}]).

app_plugin_function_does_not_exist(_Config) ->
    {ok, _} = application:ensure_all_started(vmq_plugin),
    Hooks = [{?MODULE,nonexistent_fun,0, []}],
    application:set_env(vmq_plugin, vmq_plugin_hooks, Hooks),
    {error, {no_matching_fun_in_module,vmq_plugin_SUITE,nonexistent_fun,0}} =
        vmq_plugin_mgr:enable_plugin(vmq_plugin, [{path, code:lib_dir(vmq_plugin)}]).

cannot_load_plugin_with_existing_mod(_Config) ->
    {ok, _} = application:ensure_all_started(vmq_plugin),
    {error, {module_conflict, {stdlib, lists}}}
        = vmq_plugin_mgr:enable_plugin(app_conflicting_mod,
                                       [{path, code:lib_dir(vmq_plugin)
                                         ++ "/test/app_conflicting_mod"}]).

plugin_with_compat_hooks(_Config) ->
    {ok, _} = application:ensure_all_started(vmq_plugin),
    Hooks =
        [{sample_hook_name_v0, test_compat_mod, sample_hook_v0, 2}],
    application:set_env(vmq_plugin, vmq_plugin_hooks, Hooks),
    ok = vmq_plugin_mgr:enable_plugin(vmq_plugin, [{path, code:lib_dir(vmq_plugin)},
                                                   {compat, {sample_hook_name_m5,
                                                             test_compat_mod,
                                                             sample_hook_v1_to_v0,
                                                             3}}]),
    {ok, {1,2,3}} = vmq_plugin:all_till_ok(sample_hook_name_m5, [1,2,3]),
    [{ok, {1,2,3}}] = vmq_plugin:all(sample_hook_name_m5, [1,2,3]),
    {ok, {1,2,3}} = vmq_plugin:only(sample_hook_name_m5, [1,2,3]).

module_plugin_with_compat_hooks(_Config) ->
    {ok, _} = application:ensure_all_started(vmq_plugin),

    %% register a normal hook
    ok = vmq_plugin_mgr:enable_module_plugin(sample_hook_name_v0,
                                             test_compat_mod,
                                             sample_hook_v0,
                                             2,
                                             []),

    %% then register the same hook with compat code
    ok = vmq_plugin_mgr:enable_module_plugin(sample_hook_name_v0,
                                             test_compat_mod,
                                             sample_hook_v0,
                                             2,
                                             [{compat, {sample_hook_name_m5,
                                                        test_compat_mod,
                                                        sample_hook_v1_to_v0,
                                                        3}}
                                             ]),

    %% check the normal hook works
    {ok, {1,2}} = vmq_plugin:all_till_ok(sample_hook_name_v0, [1,2]),
    [{ok, {1,2}}] = vmq_plugin:all(sample_hook_name_v0, [1,2]),
    {ok, {1,2}} = vmq_plugin:only(sample_hook_name_v0, [1,2]),

    %% check that the compat hook works as well.
    {ok, {1,2,3}} = vmq_plugin:all_till_ok(sample_hook_name_m5, [1,2,3]),
    [{ok, {1,2,3}}] = vmq_plugin:all(sample_hook_name_m5, [1,2,3]),
    {ok, {1,2,3}} = vmq_plugin:only(sample_hook_name_m5, [1,2,3]),

    ok = vmq_plugin_mgr:disable_module_plugin(sample_hook_name_v0,
                                              test_compat_mod,
                                              sample_hook_v0,
                                              2,
                                              [{compat, {sample_hook_name_m5,
                                                         test_compat_mod,
                                                         sample_hook_v1_to_v0,
                                                         3}}
                                              ]),

    ok = vmq_plugin_mgr:disable_module_plugin(sample_hook_name_v0,
                                              test_compat_mod,
                                              sample_hook_v0,
                                              2,
                                              []),

    %% check the normal hook is gone
    {error, no_matching_hook_found} = vmq_plugin:all_till_ok(sample_hook_name_v0, [1,2]),
    {error, no_matching_hook_found} = vmq_plugin:all(sample_hook_name_v0, [1,2]),
    {error, no_matching_hook_found} = vmq_plugin:only(sample_hook_name_v0, [1,2]),

    %% check that the compat hook is gone as well.
    {error, no_matching_hook_found} = vmq_plugin:all_till_ok(sample_hook_name_m5, [1,2,3]),
    {error, no_matching_hook_found} = vmq_plugin:all(sample_hook_name_m5, [1,2,3]),
    {error, no_matching_hook_found} = vmq_plugin:only(sample_hook_name_m5, [1,2,3]).



sample_hook_function() ->
    ok.

sample_hook_function(_) ->
    ok.

sample_hook_function(_, _) ->
    ok.
