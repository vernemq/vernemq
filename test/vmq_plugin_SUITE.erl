-module(vmq_plugin_SUITE).
-include_lib("common_test/include/ct.hrl").

-export([init_per_testcase/2, end_per_testcase/2,
         init_per_suite/1,
         all/0]).

-export([an_empty_config_file/1,
         load_plugin_config_file/1,
         bad_plugin_does_not_get_saved/1,
         good_plugin_gets_saved/1]).

all() ->
    [an_empty_config_file,
     load_plugin_config_file,
     bad_plugin_does_not_get_saved,
     good_plugin_gets_saved].

init_per_suite(Config) ->
    application:ensure_all_started(lager),
    lager:set_loglevel(lager_console_backend, debug),
    %%lager:set_loglevel(lager_file_backend, “error.log”, debug).
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
         [{application,vmq_elixir_plugin,
           [{paths,
             ["/home/lhc/dev/erl.io/vmq_elixir_plugin/_build/dev/lib/vmq_elixir_plugin/"]}]},
          {module,vmq_lvldb_store,
           [{hooks,
             [{msg_store_delete_async,msg_store_delete_async,1}]}]},
          {module,vmq_lvldb_store,
           [{hooks,[{msg_store_delete_sync,msg_store_delete_sync,1}]}]},
          {module,vmq_lvldb_store,
           [{hooks,[{msg_store_fold,msg_store_fold,2}]}]},
          {module,vmq_lvldb_store,
           [{hooks,[{msg_store_read,msg_store_read,1}]}]},
          {module,vmq_lvldb_store,
           [{hooks,[{msg_store_write_async,msg_store_write_async,2}]}]},
          {module,vmq_lvldb_store,
           [{hooks,[{msg_store_write_sync,msg_store_write_sync,2}]}]},
          {module,vmq_config,
           [{hooks,[{change_config,change_config,1}]}]},
          {application,vmq_systree,[]},
          {application,vmq_passwd,[]},
          {application,vmq_acl,[]}]},
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
    {error, _} = vmq_plugin_mgr:enable_plugin(bad_app, "bad_dir"),
    %% Expect that the bad plugin has not been written to file.
    {plugins, []} = read_config(Config).

good_plugin_gets_saved(Config) ->
    ok = write_config(Config, empty_plugin_config()),
    {ok, _} = application:ensure_all_started(vmq_plugin),
    %% `vmq_plugin` is an application, so we can cheat and use that as
    %% a plugin.
    ok = vmq_plugin_mgr:enable_plugin(vmq_plugin, code:lib_dir(vmq_plugin)),
    %% Expect that the bad plugin has not been written to file.
    Path = code:lib_dir(vmq_plugin),
    {plugins, [{application, vmq_plugin, Path}]} = read_config(Config).

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
