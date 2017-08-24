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
         postgres_error_test/1,
         mongodb_test/1,
         mongodb_error_test/1,
         redis_test/1,
         http_test/1,
         kv_test/1,
         json_test/1,
         bcrypt_test/1,
         logger_test/1,
         memcached_test/1,
         auth_cache_test/1]).

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(_Config) ->
    application:load(vmq_plugin),
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

-ifndef(run_all_tests).
-define(run_all_tests, false).
-endif.

all() ->
    case ?run_all_tests of
        true ->
            [mysql_test,
             postgres_test,
             mongodb_test,
             redis_test,
             http_test,
             kv_test,
             json_test,
             bcrypt_test,
             logger_test,
             memcached_test,
             auth_cache_test];
        _ ->
            [http_test,
             kv_test,
             json_test,
             bcrypt_test,
             logger_test,
             auth_cache_test]
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

test_script(File) ->
    code:lib_dir(vmq_diversity) ++ "/test/" ++ File.

mysql_test(_) ->
    {ok, _} = vmq_diversity:load_script(test_script("mysql_test.lua")).

postgres_test(_) ->
    {ok, _} = vmq_diversity:load_script(test_script("postgres_test.lua")).

postgres_error_test(_) ->
    {ok, _} = vmq_diversity:load_script(test_script("postgres_error_test.lua")),
    error = vmq_diversity_plugin:auth_on_register({{127,0,0,1}, 1234}, {"", <<"clientid">>}, <<"username">>, <<"password">>, true).

mongodb_test(_) ->
    {ok, _} = vmq_diversity:load_script(test_script("mongodb_test.lua")).

mongodb_error_test(_) ->
    {ok, _} = vmq_diversity:load_script(test_script("mongodb_error_test.lua")),
    error = vmq_diversity_plugin:auth_on_register({{127,0,0,1}, 1234}, {"", <<"clientid">>}, <<"username">>, <<"password">>, true).

redis_test(_) ->
    {ok, _} = vmq_diversity:load_script(test_script("redis_test.lua")).

http_test(_) ->
    {ok, _} = vmq_diversity:load_script(test_script("http_test.lua")).

kv_test(_) ->
    {ok, _} = vmq_diversity:load_script(test_script("ets_test.lua")).

json_test(_) ->
    {ok, _} = vmq_diversity:load_script(test_script("json_test.lua")).

bcrypt_test(_) ->
    {ok, _} = vmq_diversity:load_script(test_script("bcrypt_test.lua")).

logger_test(_) ->
    {ok, _} = vmq_diversity:load_script(test_script("log_test.lua")).

memcached_test(_) ->
    {ok, _} = vmq_diversity:load_script(test_script("memcached_test.lua")).

auth_cache_test(_) ->
    {ok, _} = vmq_diversity:load_script(test_script("cache_test.lua")).
