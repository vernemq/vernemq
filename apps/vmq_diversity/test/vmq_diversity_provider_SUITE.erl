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
         mongodb_auth_source_test/1,
         mongodb_error_test/1,
         redis_test/1,
         http_test/1,
         kv_test/1,
         json_test/1,
         bcrypt_test/1,
         logger_test/1,
         memcached_test/1,
         auth_cache_test/1,
         vmq_api_test/1,
         crypto_test/1]).

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

init_per_testcase(mongodb_auth_source_test, Config) ->
    case os:find_executable("mongo") of
       false ->
        {skipped, "no mongo client to setup auth source in MongoDB"};
       _ ->
        DataDir = proplists:get_value(data_dir, Config),
        JSPath = filename:join(DataDir, "create_auth_source_user.js"),
        os:cmd("mongo -u vmq_test_user -p vmq_test_password < script.js < " ++ JSPath),
        Config
    end;
init_per_testcase(_TestCase, Config) ->
   Config.

end_per_testcase(_TestCase, Config) ->
   Config.

-ifndef(run_all_tests).
-define(run_all_tests, false).
-endif.

-ifndef(run_ci_tests).
-define(run_ci_tests, false).
-endif.

integration_tests() ->
    [postgres_test,
     mongodb_test,
     mongodb_auth_source_test,
     mysql_test,
     redis_test,
     memcached_test].

integration_tests_that_pass_ci() ->
    [postgres_test,
     mongodb_test,
     mongodb_auth_source_test].

other_tests() -> [http_test,
                  kv_test,
                  json_test,
                  bcrypt_test,
                  logger_test,
                  auth_cache_test,
                  vmq_api_test].
all() ->
    case {?run_all_tests, ?run_ci_tests} of
        {true, _} -> integration_tests() ++ other_tests();
        {_, true} -> integration_tests_that_pass_ci() ++ other_tests();
        _ -> other_tests()
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

mongodb_auth_source_test(_) ->
    {ok, _} = vmq_diversity:load_script(test_script("mongodb_auth_source_test.lua")).

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

vmq_api_test(_) ->
    {ok, _} = vmq_diversity:load_script(test_script("vmq_api_test.lua")).

crypto_test(_) ->
    {ok, _} = vmq_diversity:load_script(test_script("crypto_test.lua")).
