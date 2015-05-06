-module(vmq_plugin_SUITE).
-include_lib("common_test/include/ct.hrl").

-export([init_per_testcase/2, end_per_testcase/2,
         init_per_suite/1,
         all/0]).

-export([an_empty_config_file/1,
         load_plugin_config_file/1,
         bad_plugin_does_not_get_saved/1,
         good_plugin_gets_saved/1,
         cannot_enable_duplicate_module_plugin/1,
         cannot_enable_duplicate_app_plugin/1,
         module_plugin_mod_does_not_exist/1,
         module_plugin_function_does_not_exist/1,
         app_plugin_mod_does_not_exist/1,
         app_plugin_function_does_not_exist/1]).

-export([sample_hook_function/0,
         sample_hook_function/1,
         sample_hook_function/2]).

all() ->
    [an_empty_config_file,
     load_plugin_config_file,
     bad_plugin_does_not_get_saved,
     good_plugin_gets_saved,
     cannot_enable_duplicate_module_plugin,
     cannot_enable_duplicate_app_plugin,
     module_plugin_mod_does_not_exist,
     module_plugin_function_does_not_exist,
     app_plugin_mod_does_not_exist,
     app_plugin_function_does_not_exist].

init_per_suite(Config) ->
    application:ensure_all_started(lager),
    %%lager:set_loglevel(lager_console_backend, debug),
    Config.

init_per_testcase(_, Config) ->
    application:load(vmq_plugin),
    application:set_env(vmq_plugin, plugin_dir, config_dir(Config)),
    application:set_env(vmq_plugin, plugin_config, config_file()),
    Config.

end_per_testcase(_, _Config) ->
    application:stop(vmq_plugin),
    application:unload(vmq_plugin),
    ok.

%% Tests
load_plugin_config_file(Config) ->
    Contents =
        {plugins,
         [{application,vmq_elixir,
           [{paths,
             ["/home/lhc/dev/erl.io/vmq_elixir"]}]},
          {module,?MODULE,
           [{hooks,
             [{hook_number_1,sample_hook_function,0}]}]},
          {module,?MODULE,
           [{hooks,
             [{hook_number_1,sample_hook_function,1}]}]},
          {module,?MODULE,
           [{hooks,
             [{hook_number_1,sample_hook_function,2}]}]}]},
    ok = write_config(Config, Contents),
    {ok, _} = application:ensure_all_started(vmq_plugin),
    ok = vmq_plugin_mgr:enable_plugin(vmq_plugin, [code:lib_dir(vmq_plugin)]).


an_empty_config_file(Config) ->
    ok = write_config(Config, empty_plugin_config()),
    {ok, _} = application:ensure_all_started(vmq_plugin),
    %% Expect that no new plugins has been written to file.
    {plugins, []} = read_config(Config).

bad_plugin_does_not_get_saved(Config) ->
    ok = write_config(Config, empty_plugin_config()),
    {ok, _} = application:ensure_all_started(vmq_plugin),
    {error, _} = vmq_plugin_mgr:enable_plugin(bad_app,  ["bad_dir"]),
    %% Expect that the bad plugin has not been written to file.
    {plugins, []} = read_config(Config).

good_plugin_gets_saved(Config) ->
    ok = write_config(Config, empty_plugin_config()),
    {ok, _} = application:ensure_all_started(vmq_plugin),
    %% `vmq_plugin` is an application, so we can cheat and use that as
    %% a plugin.
    ok = vmq_plugin_mgr:enable_plugin(vmq_plugin, [code:lib_dir(vmq_plugin)]),
    %% Expect that the bad plugin has not been written to file.
    Path = code:lib_dir(vmq_plugin),
    {plugins, [{application, vmq_plugin, [{paths, [Path]}]}]} = read_config(Config).

cannot_enable_duplicate_module_plugin(Config) ->
    ok = write_config(Config ,empty_plugin_config()),
    {ok, _} = application:ensure_all_started(vmq_plugin),
    ok = vmq_plugin_mgr:enable_module_plugin(hookname, ?MODULE, sample_hook_function, 0),
    %% Enabling a module twice does not throw an error.
    ok = vmq_plugin_mgr:enable_module_plugin(hookname, ?MODULE, sample_hook_function, 0),
    %% but it does not get written to the config file again.
    {plugins, [{module, ?MODULE, [{hooks, [{hookname, sample_hook_function, 0}]}]}]} = read_config(Config).

cannot_enable_duplicate_app_plugin(Config) ->
    ok = write_config(Config ,empty_plugin_config()),
    {ok, _} = application:ensure_all_started(vmq_plugin),
    ok = vmq_plugin_mgr:enable_plugin(vmq_plugin, [code:lib_dir(vmq_plugin)]),
    {error,already_enabled} = vmq_plugin_mgr:enable_plugin(vmq_plugin, [code:lib_dir(vmq_plugin)]).

module_plugin_mod_does_not_exist(Config) ->
    ok = write_config(Config ,empty_plugin_config()),
    {ok, _} = application:ensure_all_started(vmq_plugin),
    {error, {unknown_module,nonexistent_mod}} =
        vmq_plugin_mgr:enable_module_plugin(hookname, nonexistent_mod, sample_hook_function, 0).

module_plugin_function_does_not_exist(Config) ->
    ok = write_config(Config ,empty_plugin_config()),
    {ok, _} = application:ensure_all_started(vmq_plugin),
    {error,{no_matching_fun_in_module,vmq_plugin_SUITE,nonexistent_fun,0}} =
        vmq_plugin_mgr:enable_module_plugin(hookname, ?MODULE, nonexistent_fun, 0).

app_plugin_mod_does_not_exist(Config) ->
    ok = write_config(Config, empty_plugin_config()),
    {ok, _} = application:ensure_all_started(vmq_plugin),
    Hooks = [{nonexistent_mod,sample_hook,0}],
    application:set_env(vmq_plugin, vmq_plugin_hooks, Hooks),
    {error, {unknown_module,nonexistent_mod}} =
        vmq_plugin_mgr:enable_plugin(vmq_plugin, [code:lib_dir(vmq_plugin)]).

app_plugin_function_does_not_exist(Config) ->
    ok = write_config(Config, empty_plugin_config()),
    {ok, _} = application:ensure_all_started(vmq_plugin),
    Hooks = [{?MODULE,nonexistent_fun,0}],
    application:set_env(vmq_plugin, vmq_plugin_hooks, Hooks),
    {error, {no_matching_fun_in_module,vmq_plugin_SUITE,nonexistent_fun,0}} =
        vmq_plugin_mgr:enable_plugin(vmq_plugin, [code:lib_dir(vmq_plugin)]).

sample_hook_function() ->
    ok.

sample_hook_function(_) ->
    ok.

sample_hook_function(_, _) ->
    ok.

%% Helpers
empty_plugin_config() ->
    "{plugins,[]}.".

config_file() ->
    "ct_test_plugin.config".

config_dir(CTConfig) ->
    proplists:get_value(priv_dir, CTConfig).

read_config(CTConfig) ->
    Name = filename:join(config_dir(CTConfig), config_file()),
    {ok, [Plugins]} = file:consult(Name),
    Plugins.

write_config(CTConfig, PluginConfig) when is_list(PluginConfig) ->
    Name = filename:join(config_dir(CTConfig), config_file()),
    file:write_file(Name, PluginConfig);
write_config(CTConfig, PluginConfig) ->
    String = io_lib:format("~p.", [PluginConfig]),
    write_config(CTConfig, String).
