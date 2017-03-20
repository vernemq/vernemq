-module(vmq_plugin_SUITE).
-include_lib("common_test/include/ct.hrl").

-export([init_per_testcase/2, end_per_testcase/2,
         init_per_suite/1, end_per_suite/1,
         all/0]).

-export([app_hook_without_proplist/1,
         bad_plugin_does_not_get_saved/1,
         good_plugin_gets_saved/1,
         cannot_enable_duplicate_module_plugin/1,
         cannot_enable_duplicate_app_plugin/1,
         module_plugin_mod_does_not_exist/1,
         module_plugin_function_does_not_exist/1,
         app_plugin_mod_does_not_exist/1,
         app_plugin_function_does_not_exist/1,
         cannot_load_plugin_with_existing_mod/1]).

-export([sample_hook_function/0,
         sample_hook_function/1,
         sample_hook_function/2]).

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
     cannot_load_plugin_with_existing_mod].

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
    ok = vmq_plugin_mgr:enable_plugin(vmq_plugin, [code:lib_dir(vmq_plugin)]).

bad_plugin_does_not_get_saved(_Config) ->
    {ok, _} = application:ensure_all_started(vmq_plugin),
    {error, _} = vmq_plugin_mgr:enable_plugin(bad_app,  ["bad_dir"]),
    %% Expect that the bad plugin has not been written to file.
    {ok, []} = vmq_plugin_mgr:get_plugins().

good_plugin_gets_saved(_Config) ->
    {ok, _} = application:ensure_all_started(vmq_plugin),
    %% `vmq_plugin` is an application, so we can cheat and use that as
    %% a plugin.
    Path = code:lib_dir(vmq_plugin),
    ok = vmq_plugin_mgr:enable_plugin(vmq_plugin, [code:lib_dir(vmq_plugin)]),
    {ok, [{application, vmq_plugin, [{hooks, []}, {paths, [Path]}]}]} = vmq_plugin_mgr:get_plugins().

cannot_enable_duplicate_module_plugin(_Config) ->
    {ok, _} = application:ensure_all_started(vmq_plugin),
    ok = vmq_plugin_mgr:enable_module_plugin(hookname, ?MODULE, sample_hook_function, 0),
    %% Enabling a module twice does not throw an error.
    ok = vmq_plugin_mgr:enable_module_plugin(hookname, ?MODULE, sample_hook_function, 0),
    %% but it does not get written to the config file again.
    {ok, [{module, ?MODULE, [{hooks, [{hookname, sample_hook_function, 0}]}]}]} =
        vmq_plugin_mgr:get_plugins().

cannot_enable_duplicate_app_plugin(_Config) ->
    {ok, _} = application:ensure_all_started(vmq_plugin),
    ok = vmq_plugin_mgr:enable_plugin(vmq_plugin, [code:lib_dir(vmq_plugin)]),
    {error,already_enabled} = vmq_plugin_mgr:enable_plugin(vmq_plugin, [code:lib_dir(vmq_plugin)]).

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
        vmq_plugin_mgr:enable_plugin(vmq_plugin, [code:lib_dir(vmq_plugin)]).

app_plugin_function_does_not_exist(_Config) ->
    {ok, _} = application:ensure_all_started(vmq_plugin),
    Hooks = [{?MODULE,nonexistent_fun,0, []}],
    application:set_env(vmq_plugin, vmq_plugin_hooks, Hooks),
    {error, {no_matching_fun_in_module,vmq_plugin_SUITE,nonexistent_fun,0}} =
        vmq_plugin_mgr:enable_plugin(vmq_plugin, [code:lib_dir(vmq_plugin)]).

cannot_load_plugin_with_existing_mod(_Config) ->
    {ok, _} = application:ensure_all_started(vmq_plugin),
    {error, {module_conflict, {stdlib, lists}}}
        = vmq_plugin_mgr:enable_plugin(app_conflicting_mod,
                                       [code:lib_dir(vmq_plugin)
                                        ++ "/test/app_conflicting_mod"]).

sample_hook_function() ->
    ok.

sample_hook_function(_) ->
    ok.

sample_hook_function(_, _) ->
    ok.
