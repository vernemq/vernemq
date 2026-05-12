-module(vmq_enhanced_auth_rate_limiter_SUITE).

-include_lib("vmq_enhanced_auth/src/vmq_enhanced_auth.hrl").

-export([
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2,
    all/0
]).

-export([
    undefined_acl_name_allowed_test/1,
    no_config_allowed_test/1,
    under_limit_allowed_test/1,
    at_limit_allowed_test/1,
    over_limit_dropped_test/1,
    multiple_acl_names_independent_test/1,
    set_rate_test/1,
    update_rate_test/1,
    delete_rate_test/1,
    delete_nonexistent_rate_test/1,
    list_rates_test/1,
    counter_reset_test/1,
    drop_metric_incremented_test/1,
    rate_limit_metrics_format_test/1,
    metrics_empty_when_no_drops_test/1,
    config_loaded_on_start_test/1,
    delete_rate_cleans_metrics_test/1
]).

all() ->
    [
        undefined_acl_name_allowed_test,
        no_config_allowed_test,
        under_limit_allowed_test,
        at_limit_allowed_test,
        over_limit_dropped_test,
        multiple_acl_names_independent_test,
        set_rate_test,
        update_rate_test,
        delete_rate_test,
        delete_nonexistent_rate_test,
        list_rates_test,
        counter_reset_test,
        drop_metric_incremented_test,
        rate_limit_metrics_format_test,
        metrics_empty_when_no_drops_test,
        config_loaded_on_start_test,
        delete_rate_cleans_metrics_test
    ].

init_per_suite(Config) ->
    cover:start(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_testcase(config_loaded_on_start_test, Config) ->
    application:set_env(vmq_enhanced_auth, publish_rate_limit, [
        {"configacl", 50}
    ]),
    {ok, Pid} = vmq_enhanced_auth_rate_limiter:start_link(),
    [{pid, Pid} | Config];
init_per_testcase(_Case, Config) ->
    application:set_env(vmq_enhanced_auth, publish_rate_limit, []),
    {ok, Pid} = vmq_enhanced_auth_rate_limiter:start_link(),
    [{pid, Pid} | Config].

end_per_testcase(_Case, Config) ->
    Pid = proplists:get_value(pid, Config),
    unlink(Pid),
    exit(Pid, shutdown),
    wait_for_exit(Pid),
    Config.

wait_for_exit(Pid) ->
    MRef = erlang:monitor(process, Pid),
    receive
        {'DOWN', MRef, process, Pid, _} -> ok
    after 5000 ->
        ct:fail(rate_limiter_did_not_stop)
    end.

undefined_acl_name_allowed_test(_Config) ->
    allow = vmq_enhanced_auth_rate_limiter:check_publish_rate(undefined, 0).

no_config_allowed_test(_Config) ->
    allow = vmq_enhanced_auth_rate_limiter:check_publish_rate(<<"unknown_acl">>, 0).

under_limit_allowed_test(_Config) ->
    ok = vmq_enhanced_auth_rate_limiter:set_rate(<<"acl1">>, 5),
    allow = vmq_enhanced_auth_rate_limiter:check_publish_rate(<<"acl1">>, 0),
    allow = vmq_enhanced_auth_rate_limiter:check_publish_rate(<<"acl1">>, 1),
    allow = vmq_enhanced_auth_rate_limiter:check_publish_rate(<<"acl1">>, 2).

at_limit_allowed_test(_Config) ->
    ok = vmq_enhanced_auth_rate_limiter:set_rate(<<"acl1">>, 3),
    allow = vmq_enhanced_auth_rate_limiter:check_publish_rate(<<"acl1">>, 0),
    allow = vmq_enhanced_auth_rate_limiter:check_publish_rate(<<"acl1">>, 1),
    allow = vmq_enhanced_auth_rate_limiter:check_publish_rate(<<"acl1">>, 0).

over_limit_dropped_test(_Config) ->
    ok = vmq_enhanced_auth_rate_limiter:set_rate(<<"acl1">>, 2),
    allow = vmq_enhanced_auth_rate_limiter:check_publish_rate(<<"acl1">>, 0),
    allow = vmq_enhanced_auth_rate_limiter:check_publish_rate(<<"acl1">>, 1),
    drop = vmq_enhanced_auth_rate_limiter:check_publish_rate(<<"acl1">>, 0),
    drop = vmq_enhanced_auth_rate_limiter:check_publish_rate(<<"acl1">>, 1).

multiple_acl_names_independent_test(_Config) ->
    ok = vmq_enhanced_auth_rate_limiter:set_rate(<<"acl_a">>, 1),
    ok = vmq_enhanced_auth_rate_limiter:set_rate(<<"acl_b">>, 2),
    allow = vmq_enhanced_auth_rate_limiter:check_publish_rate(<<"acl_a">>, 0),
    drop = vmq_enhanced_auth_rate_limiter:check_publish_rate(<<"acl_a">>, 0),
    allow = vmq_enhanced_auth_rate_limiter:check_publish_rate(<<"acl_b">>, 1),
    allow = vmq_enhanced_auth_rate_limiter:check_publish_rate(<<"acl_b">>, 1),
    drop = vmq_enhanced_auth_rate_limiter:check_publish_rate(<<"acl_b">>, 1).

set_rate_test(_Config) ->
    ok = vmq_enhanced_auth_rate_limiter:set_rate(<<"acl1">>, 100),
    [{<<"acl1">>, 100}] = vmq_enhanced_auth_rate_limiter:list_rates().

update_rate_test(_Config) ->
    ok = vmq_enhanced_auth_rate_limiter:set_rate(<<"acl1">>, 100),
    ok = vmq_enhanced_auth_rate_limiter:set_rate(<<"acl1">>, 200),
    [{<<"acl1">>, 200}] = vmq_enhanced_auth_rate_limiter:list_rates().

delete_rate_test(_Config) ->
    ok = vmq_enhanced_auth_rate_limiter:set_rate(<<"acl1">>, 100),
    ok = vmq_enhanced_auth_rate_limiter:delete_rate(<<"acl1">>),
    [] = vmq_enhanced_auth_rate_limiter:list_rates().

delete_nonexistent_rate_test(_Config) ->
    {error, not_found} = vmq_enhanced_auth_rate_limiter:delete_rate(<<"nonexistent">>).

list_rates_test(_Config) ->
    ok = vmq_enhanced_auth_rate_limiter:set_rate(<<"acl1">>, 10),
    ok = vmq_enhanced_auth_rate_limiter:set_rate(<<"acl2">>, 20),
    Rates = lists:sort(vmq_enhanced_auth_rate_limiter:list_rates()),
    [{<<"acl1">>, 10}, {<<"acl2">>, 20}] = Rates.

counter_reset_test(_Config) ->
    ok = vmq_enhanced_auth_rate_limiter:set_rate(<<"acl1">>, 2),
    allow = vmq_enhanced_auth_rate_limiter:check_publish_rate(<<"acl1">>, 0),
    allow = vmq_enhanced_auth_rate_limiter:check_publish_rate(<<"acl1">>, 0),
    drop = vmq_enhanced_auth_rate_limiter:check_publish_rate(<<"acl1">>, 0),
    Pid = whereis(vmq_enhanced_auth_rate_limiter),
    Pid ! reset_counters,
    timer:sleep(50),
    allow = vmq_enhanced_auth_rate_limiter:check_publish_rate(<<"acl1">>, 0),
    allow = vmq_enhanced_auth_rate_limiter:check_publish_rate(<<"acl1">>, 0).

drop_metric_incremented_test(_Config) ->
    ok = vmq_enhanced_auth_rate_limiter:set_rate(<<"acl1">>, 1),
    allow = vmq_enhanced_auth_rate_limiter:check_publish_rate(<<"acl1">>, 0),
    drop = vmq_enhanced_auth_rate_limiter:check_publish_rate(<<"acl1">>, 0),
    drop = vmq_enhanced_auth_rate_limiter:check_publish_rate(<<"acl1">>, 1),
    Metrics = lists:sort(ets:tab2list(?RATE_LIMIT_METRICS_TBL)),
    [{{<<"acl1">>, 0}, 1}, {{<<"acl1">>, 1}, 1}] = Metrics.

rate_limit_metrics_format_test(_Config) ->
    ok = vmq_enhanced_auth_rate_limiter:set_rate(<<"acl1">>, 1),
    allow = vmq_enhanced_auth_rate_limiter:check_publish_rate(<<"acl1">>, 1),
    drop = vmq_enhanced_auth_rate_limiter:check_publish_rate(<<"acl1">>, 1),
    [Metric] = vmq_enhanced_auth_metrics:rate_limit_metrics(),
    {counter, [{acl_name, "acl1"}, {qos, "1"}], {?PUBLISH_RATE_LIMIT_EXCEEDED, <<"acl1">>, 1},
     ?PUBLISH_RATE_LIMIT_EXCEEDED, Desc, 1} = Metric,
    true = is_binary(Desc).

metrics_empty_when_no_drops_test(_Config) ->
    [] = vmq_enhanced_auth_metrics:rate_limit_metrics().

config_loaded_on_start_test(_Config) ->
    Rates = vmq_enhanced_auth_rate_limiter:list_rates(),
    [{<<"configacl">>, 50}] = Rates,
    allow = vmq_enhanced_auth_rate_limiter:check_publish_rate(<<"configacl">>, 0).

delete_rate_cleans_metrics_test(_Config) ->
    ok = vmq_enhanced_auth_rate_limiter:set_rate(<<"acl1">>, 1),
    allow = vmq_enhanced_auth_rate_limiter:check_publish_rate(<<"acl1">>, 0),
    drop = vmq_enhanced_auth_rate_limiter:check_publish_rate(<<"acl1">>, 0),
    drop = vmq_enhanced_auth_rate_limiter:check_publish_rate(<<"acl1">>, 1),
    2 = length(ets:tab2list(?RATE_LIMIT_METRICS_TBL)),
    ok = vmq_enhanced_auth_rate_limiter:delete_rate(<<"acl1">>),
    [] = ets:tab2list(?RATE_LIMIT_METRICS_TBL),
    [] = vmq_enhanced_auth_metrics:rate_limit_metrics().
