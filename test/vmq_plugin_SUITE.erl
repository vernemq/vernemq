-module(vmq_plugin_SUITE).
-include_lib("common_test/include/ct.hrl").

-export([init_per_testcase/2, end_per_testcase/2,
         all/0]).

-export([an_empty_config_file/1,
         bad_plugin_does_not_get_saved/1,
         good_plugin_gets_saved/1]).

all() ->
    [an_empty_config_file,
     bad_plugin_does_not_get_saved,
     good_plugin_gets_saved].

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
an_empty_config_file(Config) ->
    write_config(Config, empty_plugin_config()),
    {ok, _} = application:ensure_all_started(vmq_plugin),
    %% Expect that no new plugins has been written to file.
    {plugins, []} = read_config(Config).

bad_plugin_does_not_get_saved(Config) ->
    write_config(Config, empty_plugin_config()),
    {ok, _} = application:ensure_all_started(vmq_plugin),
    {error, _} = vmq_plugin_mgr:enable_plugin(bad_app, "bad_dir"),
    %% Expect that the bad plugin has not been written to file.
    {plugins, []} = read_config(Config).

good_plugin_gets_saved(Config) ->
    write_config(Config, empty_plugin_config()),
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

write_config(CTConfig, PluginConfig) ->
    Name = filename:join(config_dir(CTConfig), config_file()),
    file:write_file(Name, PluginConfig).

