-module(vmq_diversity_provider_SUITE).
-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([mysql_test/1,
         postgres_test/1,
         mongodb_test/1,
         redis_test/1,
         http_test/1,
         kv_test/1,
         json_test/1,
         logger_test/1,
         memcached_test/1]).

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(_Config) ->
    application:load(vmq_plugin),
    ok = file:write_file("vmq_plugin.conf", <<"{plugins, []}.">>),
    application:ensure_all_started(vmq_plugin),
    vmq_plugin_mgr:enable_plugin(vmq_diversity),
    cover:start(),
    _Config.

end_per_suite(_Config) ->
    vmq_plugin_mgr:disable_plugin(vmq_diversity),
    application:stop(vmq_plugin),
    _Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_, Config) ->
    Config.

-ifdef('DO_TEST').
all() ->
    [mysql_test,
     postgres_test,
     mongodb_test,
     redis_test,
     http_test,
     kv_test,
     json_test,
     logger_test,
     memcached_test].
-else.
all() -> [].
-endif.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

mysql_test(_) ->
    {ok, _} = vmq_diversity:load_script("../../../../test/mysql_test.lua").

postgres_test(_) ->
    {ok, _} = vmq_diversity:load_script("../../../../test/postgres_test.lua").

mongodb_test(_) ->
    {ok, _} = vmq_diversity:load_script("../../../../test/mongodb_test.lua").

redis_test(_) ->
    {ok, _} = vmq_diversity:load_script("../../../../test/redis_test.lua").

http_test(_) ->
    {ok, _} = vmq_diversity:load_script("../../../../test/http_test.lua").

kv_test(_) ->
    {ok, _} = vmq_diversity:load_script("../../../../test/ets_test.lua").

json_test(_) ->
    {ok, _} = vmq_diversity:load_script("../../../../test/json_test.lua").

logger_test(_) ->
    {ok, _} = vmq_diversity:load_script("../../../../test/log_test.lua").

memcached_test(_) ->
    {ok, _} = vmq_diversity:load_script("../../../../test/memcached_test.lua").
