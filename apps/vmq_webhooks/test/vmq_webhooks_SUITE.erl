-module(vmq_webhooks_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("vernemq_dev/include/vernemq_dev.hrl").
-include("vmq_webhooks_test.hrl").


-export([
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-compile([export_all]).
-compile([nowarn_export_all]).

-define(HTTP_PORT, 34567).
-define(HTTPS_PORT, 45678).

init_per_suite(Config) ->
    {ok, StartedApps} = application:ensure_all_started(vmq_server),
    ok = vmq_plugin_mgr:enable_plugin(vmq_webhooks),
    {ok, _} = application:ensure_all_started(cowboy),
    cover:start(),
    CertsDir = filename:join([?config(data_dir, Config)]),
    [{started_apps, StartedApps},
     {certs_dir, CertsDir}] ++ Config.

end_per_suite(Config) ->
    vmq_plugin_mgr:disable_plugin(vmq_webhooks),
    application:stop(cowboy),
    application:stop(vmq_server),
    [ application:stop(App) || App <- proplists:get_value(started_apps, Config, []) ],
    Config.

init_per_group(http, Config) ->
    webhooks_handler:start_endpoint_clear(?HTTP_PORT),
    Config;
init_per_group(https, Config) ->
    %% each HTTPS test starts its own webhook handler because they need different options
    Config.

end_per_group(http, Config) ->
    webhooks_handler:stop_endpoint_clear(),
    Config;
end_per_group(https, Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    vmq_webhooks_cache:purge_all(),
    Config.

end_per_testcase(_, Config) ->
    Config.

all() ->
    [
     {group, http},
     {group, https}
    ].

groups() ->
    [
     {http, http()},
     {https, https()}
    ].

http() ->
    [
     auth_on_register_m5_test,
     auth_on_publish_m5_test,
     auth_on_publish_m5_no_payload_test,
     auth_on_publish_m5_modify_props_test,
     auth_on_subscribe_m5_test,
     on_register_m5_test,
     on_publish_m5_test,
     on_subscribe_m5_test,
     on_unsubscribe_m5_test,
     on_deliver_m5_test,
     on_deliver_m5_modify_props_test,
     on_auth_m5_test,


     auth_on_register_test,
     auth_on_publish_test,
     auth_on_publish_no_payload_test,
     auth_on_subscribe_test,
     on_register_test,
     on_publish_test,
     on_subscribe_test,
     on_unsubscribe_test,
     on_deliver_test,
     on_offline_message_test,
     on_client_wakeup_test,
     on_client_offline_test,
     on_client_gone_test,
     on_session_expired_test,
     base64payload_test,
     auth_on_register_undefined_creds_test,
     cache_auth_on_register,
     cache_auth_on_publish,
     cache_auth_on_subscribe,
     cache_expired_entry,
     cli_allow_query_parameters_test
    ].

https() ->
    [
     https_ca_test,
     https_wrong_ca_test,
     https_client_cert_test,
     https_missing_client_cert_fail_test,
     https_expired_server_cert_test,
     https_fails_with_no_crl_test,
     https_bad_server_cn_test
    ].

start_endpoint_clear() ->
    webhooks_handler:start_endpoint_clear(?HTTP_PORT).

%% Test cases
cache_expired_entry(_) ->
    Endpoint = ?ENDPOINT ++ "/cache1s",
    Self = pid_to_bin(self()),
    register_hook(auth_on_register, Endpoint),
    ok = vmq_plugin:all_till_ok(auth_on_register,
                                      [?PEER, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, Self, ?PASSWORD, true]),
    exp_response(cache_auth_on_register_ok),
    %% wait until the entry was expired
    timer:sleep(1100),
    ok = vmq_plugin:all_till_ok(auth_on_register,
                                      [?PEER, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, Self, ?PASSWORD, true]),
    exp_response(cache_auth_on_register_ok),
    ok = vmq_plugin:all_till_ok(auth_on_register,
                                      [?PEER, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, Self, ?PASSWORD, true]),
    exp_response(cache_auth_on_register_ok),
    ok = exp_nothing(200),
    #{{entries,<<"http://localhost:34567/cache1s">>,
       auth_on_register} := 1,
      {hits,<<"http://localhost:34567/cache1s">>,
       auth_on_register} := 1,
      {misses,<<"http://localhost:34567/cache1s">>,
       auth_on_register} := 2} = vmq_webhooks_cache:stats(),
    deregister_hook(auth_on_register, Endpoint).

cache_auth_on_register(_) ->
    Endpoint = ?ENDPOINT ++ "/cache",
    Self = pid_to_bin(self()),
    register_hook(auth_on_register, Endpoint),
    ok = vmq_plugin:all_till_ok(auth_on_register,
                                      [?PEER, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, Self, ?PASSWORD, true]),
    exp_response(cache_auth_on_register_ok),
    ok = vmq_plugin:all_till_ok(auth_on_register,
                                      [?PEER, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, Self, ?PASSWORD, true]),
    ok = exp_nothing(200),
    #{{entries,<<"http://localhost:34567/cache">>,
       auth_on_register} := 1,
      {hits,<<"http://localhost:34567/cache">>,
       auth_on_register} := 1,
      {misses,<<"http://localhost:34567/cache">>,
       auth_on_register} := 1} = vmq_webhooks_cache:stats(),
    deregister_hook(auth_on_register, Endpoint).

cache_auth_on_publish(_) ->
    Endpoint = ?ENDPOINT ++ "/cache",
    Self = pid_to_bin(self()),
    register_hook(auth_on_publish, Endpoint),
    ok = vmq_plugin:all_till_ok(auth_on_publish,
                      [Self, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, 1, ?TOPIC, ?PAYLOAD, false]),
    exp_response(cache_auth_on_publish_ok),
    ok = vmq_plugin:all_till_ok(auth_on_publish,
                      [Self, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, 1, ?TOPIC, ?PAYLOAD, false]),
    ok = exp_nothing(200),
    #{{entries,<<"http://localhost:34567/cache">>,
       auth_on_publish} := 1,
      {hits,<<"http://localhost:34567/cache">>,
       auth_on_publish} := 1,
      {misses,<<"http://localhost:34567/cache">>,
       auth_on_publish} := 1} = vmq_webhooks_cache:stats(),
    deregister_hook(auth_on_publish, Endpoint).

cache_auth_on_subscribe(_) ->
    Endpoint = ?ENDPOINT ++ "/cache",
    Self = pid_to_bin(self()),
    register_hook(auth_on_subscribe, Endpoint),
    ok = vmq_plugin:all_till_ok(auth_on_subscribe,
                      [Self, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, [{?TOPIC, 1}]]),
    exp_response(cache_auth_on_subscribe_ok),
    ok = vmq_plugin:all_till_ok(auth_on_subscribe,
                      [Self, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, [{?TOPIC, 1}]]),
    ok = exp_nothing(200),
    #{{entries,<<"http://localhost:34567/cache">>,
       auth_on_subscribe} := 1,
      {hits,<<"http://localhost:34567/cache">>,
       auth_on_subscribe} := 1,
      {misses,<<"http://localhost:34567/cache">>,
       auth_on_subscribe} := 1} = vmq_webhooks_cache:stats(),
    deregister_hook(auth_on_subscribe, Endpoint).

auth_on_register_test(_) ->
    register_hook(auth_on_register, ?ENDPOINT),
    ok = vmq_plugin:all_till_ok(auth_on_register,
                      [?PEER, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, ?USERNAME, ?PASSWORD, true]),
    {error, <<"not_allowed">>} = vmq_plugin:all_till_ok(auth_on_register,
                      [?PEER, {?MOUNTPOINT, ?NOT_ALLOWED_CLIENT_ID}, ?USERNAME, ?PASSWORD, true]),
    {error, plugin_chain_exhausted} = vmq_plugin:all_till_ok(auth_on_register,
                      [?PEER, {?MOUNTPOINT, ?IGNORED_CLIENT_ID}, ?USERNAME, ?PASSWORD, true]),
    {ok, [{subscriber_id,
           {"mynewmount", <<"changed_client_id">>}}]} = vmq_plugin:all_till_ok(auth_on_register,
                      [?PEER, {?MOUNTPOINT, ?CHANGED_CLIENT_ID}, ?USERNAME, ?PASSWORD, true]),
    {ok, [{username, <<"changed_username">>}]} = vmq_plugin:all_till_ok(auth_on_register,
                      [?PEER, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, ?CHANGED_USERNAME, ?PASSWORD, true]),
    deregister_hook(auth_on_register, ?ENDPOINT).

auth_on_publish_test(_) ->
    register_hook(auth_on_publish, ?ENDPOINT),
    ok = vmq_plugin:all_till_ok(auth_on_publish,
                      [?USERNAME, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, 1, ?TOPIC, ?PAYLOAD, false]),
    {error, <<"not_allowed">>} = vmq_plugin:all_till_ok(auth_on_publish,
                      [?USERNAME, {?MOUNTPOINT, ?NOT_ALLOWED_CLIENT_ID}, 1, ?TOPIC, ?PAYLOAD, false]),
    {error, plugin_chain_exhausted} = vmq_plugin:all_till_ok(auth_on_publish,
                      [?USERNAME, {?MOUNTPOINT, ?IGNORED_CLIENT_ID}, 1, ?TOPIC, ?PAYLOAD, false]),
    {ok, [{topic, [<<"rewritten">>, <<"topic">>]}]} = vmq_plugin:all_till_ok(auth_on_publish,
                      [?USERNAME, {?MOUNTPOINT, ?CHANGED_CLIENT_ID}, 1, ?TOPIC, ?PAYLOAD, false]),
    deregister_hook(auth_on_publish, ?ENDPOINT).

auth_on_subscribe_test(_) ->
    register_hook(auth_on_subscribe, ?ENDPOINT),
    ok = vmq_plugin:all_till_ok(auth_on_subscribe,
                      [?USERNAME, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, [{?TOPIC, 1}]]),
    {error, <<"not_allowed">>} = vmq_plugin:all_till_ok(auth_on_subscribe,
                      [?USERNAME, {?MOUNTPOINT, ?NOT_ALLOWED_CLIENT_ID}, [{?TOPIC, 1}]]),
    {error, plugin_chain_exhausted} = vmq_plugin:all_till_ok(auth_on_subscribe,
                      [?USERNAME, {?MOUNTPOINT, ?IGNORED_CLIENT_ID}, [{?TOPIC, 1}]]),
    {ok, [{[<<"rewritten">>, <<"topic">>], 2},
          {[<<"forbidden">>, <<"topic">>], not_allowed}]} = vmq_plugin:all_till_ok(auth_on_subscribe,
                      [?USERNAME, {?MOUNTPOINT, ?CHANGED_CLIENT_ID}, [{?TOPIC, 1}]]),
    deregister_hook(auth_on_subscribe, ?ENDPOINT).

on_register_test(_) ->
    register_hook(on_register, ?ENDPOINT),
    Self = pid_to_bin(self()),
    [next] = vmq_plugin:all(on_register,
                            [?PEER, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, Self]),
    ok = exp_response(on_register_ok),
    deregister_hook(on_register, ?ENDPOINT).

on_publish_test(_) ->
    register_hook(on_publish, ?ENDPOINT),
    Self = pid_to_bin(self()),
    [next] = vmq_plugin:all(on_publish,
                           [Self, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, 1, ?TOPIC, ?PAYLOAD, false]),
    ok = exp_response(on_publish_ok),
    deregister_hook(on_publish, ?ENDPOINT).

on_subscribe_test(_) ->
    register_hook(on_subscribe, ?ENDPOINT),
    Self = pid_to_bin(self()),
    [next] = vmq_plugin:all(on_subscribe,
                            [Self, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, [{?TOPIC, 1},
                                                                       {?TOPIC, not_allowed}]]),
    ok = exp_response(on_subscribe_ok),
    deregister_hook(on_subscribe, ?ENDPOINT).

on_unsubscribe_test(_) ->
    register_hook(on_unsubscribe, ?ENDPOINT),
    ok = vmq_plugin:all_till_ok(on_unsubscribe,
                                [?USERNAME, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, [?TOPIC]]),
    {ok, [[<<"rewritten">>, <<"topic">>],
          [<<"anotherrewrittentopic">>]]} = vmq_plugin:all_till_ok(on_unsubscribe,
                      [?USERNAME, {?MOUNTPOINT, ?CHANGED_CLIENT_ID}, [?TOPIC]]),
    deregister_hook(on_unsubscribe, ?ENDPOINT).

on_deliver_test(_) ->
    register_hook(on_deliver, ?ENDPOINT),
    Self = pid_to_bin(self()),
    ok = vmq_plugin:all_till_ok(on_deliver,
                                [Self, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, 1, ?TOPIC, ?PAYLOAD, false]),
    ok = exp_response(on_deliver_ok),
    deregister_hook(on_deliver, ?ENDPOINT).

%%%%%%%%%%%%%%%%%%%%%%%%% MQTT 5.0 tests %%%%%%%%%%%%%%%%%%%%%%%%%
auth_on_register_m5_test(_) ->
    register_hook(auth_on_register_m5, ?ENDPOINT),
    ok = vmq_plugin:all_till_ok(auth_on_register_m5,
                      [?PEER, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, ?USERNAME, ?PASSWORD, true, #{} ]),
    {error, <<"not_allowed">>} = vmq_plugin:all_till_ok(auth_on_register_m5,
                      [?PEER, {?MOUNTPOINT, ?NOT_ALLOWED_CLIENT_ID}, ?USERNAME, ?PASSWORD, true, #{}]),
    {error, plugin_chain_exhausted} = vmq_plugin:all_till_ok(auth_on_register_m5,
                      [?PEER, {?MOUNTPOINT, ?IGNORED_CLIENT_ID}, ?USERNAME, ?PASSWORD, true, #{}]),
    {ok, #{subscriber_id :=
           {"mynewmount", <<"changed_client_id">>}}} = vmq_plugin:all_till_ok(auth_on_register_m5,
                      [?PEER, {?MOUNTPOINT, ?CHANGED_CLIENT_ID}, ?USERNAME, ?PASSWORD, true, #{}]),
    {ok, #{username := <<"changed_username">>}} = vmq_plugin:all_till_ok(auth_on_register_m5,
                      [?PEER, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, ?CHANGED_USERNAME, ?PASSWORD, true, #{}]),
    WantUserProps = [{<<"k1">>, <<"v1">>},
                     {<<"k1">>, <<"v2">>},
                     {<<"k2">>, <<"v2">>}],
    {ok, #{properties := #{?P_USER_PROPERTY := GotUserProps}}}
        = vmq_plugin:all_till_ok(auth_on_register_m5,
                      [?PEER, {?MOUNTPOINT, ?WITH_PROPERTIES}, ?USERNAME, ?PASSWORD, true,
                       #{?P_SESSION_EXPIRY_INTERVAL => 5,
                         ?P_RECEIVE_MAX => 10,
                         ?P_TOPIC_ALIAS_MAX => 15,
                         ?P_REQUEST_RESPONSE_INFO => true,
                         ?P_REQUEST_PROBLEM_INFO => true,
                         ?P_USER_PROPERTY => WantUserProps}]),
    [] = WantUserProps -- GotUserProps,
    deregister_hook(auth_on_register_m5, ?ENDPOINT).


auth_on_publish_m5_test(_) ->
    register_hook(auth_on_publish_m5, ?ENDPOINT),
    ok = vmq_plugin:all_till_ok(auth_on_publish_m5,
                      [?USERNAME, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, 1, ?TOPIC, ?PAYLOAD, false, #{}]),
    {error, <<"not_allowed">>} = vmq_plugin:all_till_ok(auth_on_publish_m5,
                      [?USERNAME, {?MOUNTPOINT, ?NOT_ALLOWED_CLIENT_ID}, 1, ?TOPIC, ?PAYLOAD, false, #{}]),
    {error, plugin_chain_exhausted} = vmq_plugin:all_till_ok(auth_on_publish_m5,
                      [?USERNAME, {?MOUNTPOINT, ?IGNORED_CLIENT_ID}, 1, ?TOPIC, ?PAYLOAD, false, #{}]),
    {ok, #{topic := [<<"rewritten">>, <<"topic">>]}} = vmq_plugin:all_till_ok(auth_on_publish_m5,
                      [?USERNAME, {?MOUNTPOINT, ?CHANGED_CLIENT_ID}, 1, ?TOPIC, ?PAYLOAD, false, #{}]),
    deregister_hook(auth_on_publish_m5, ?ENDPOINT).

auth_on_publish_m5_modify_props_test(_) ->
    register_hook(auth_on_publish_m5, ?ENDPOINT),
    Args = [?USERNAME, {?MOUNTPOINT, <<"modify_props">>}, 1, ?TOPIC, ?PAYLOAD, false,
            #{?P_USER_PROPERTY =>
                  [{<<"k1">>, <<"v1">>},
                   {<<"k2">>, <<"v2">>}],
              ?P_CORRELATION_DATA => <<"correlation_data">>,
              ?P_RESPONSE_TOPIC => [<<"response">>,<<"topic">>],
              ?P_PAYLOAD_FORMAT_INDICATOR => utf8,
              ?P_CONTENT_TYPE => <<"content_type">>}],
    {ok, #{properties :=
          #{?P_USER_PROPERTY :=
                [{<<"k1">>, <<"v1">>},
                 {<<"k2">>, <<"v2">>},
                 {<<"k3">>, <<"v3">>}],
            ?P_CORRELATION_DATA := <<"modified_correlation_data">>,
            ?P_RESPONSE_TOPIC := [<<"modified_responsetopic">>],
            ?P_PAYLOAD_FORMAT_INDICATOR := undefined,
            ?P_CONTENT_TYPE := <<"modified_content_type">>}}}
        = vmq_plugin:all_till_ok(auth_on_publish_m5, Args),
    deregister_hook(auth_on_publish_m5, ?ENDPOINT).

auth_on_subscribe_m5_test(_) ->
    register_hook(auth_on_subscribe_m5, ?ENDPOINT),
    ok = vmq_plugin:all_till_ok(auth_on_subscribe_m5,
                      [?USERNAME, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID},
                       [{?TOPIC, {1,#{no_local => false,rap => false,
                                      retain_handling => send_retain}}}],
                       #{?P_USER_PROPERTY =>
                             [{<<"k1">>, <<"v1">>}],
                         ?P_SUBSCRIPTION_ID => [1,2,3]}]),
    {error, <<"not_allowed">>} = vmq_plugin:all_till_ok(auth_on_subscribe_m5,
                      [?USERNAME, {?MOUNTPOINT, ?NOT_ALLOWED_CLIENT_ID}, [{?TOPIC, 1}], #{}]),
    {error, plugin_chain_exhausted} = vmq_plugin:all_till_ok(auth_on_subscribe_m5,
                      [?USERNAME, {?MOUNTPOINT, ?IGNORED_CLIENT_ID}, [{?TOPIC, 1}], #{}]),
    {ok, #{topics := [{[<<"rewritten">>, <<"topic">>], {2, #{no_local := false,
                                                             rap := false,
                                                             retain_handling := send_retain}}},
                      {[<<"forbidden">>, <<"topic">>], 135}]}} = vmq_plugin:all_till_ok(auth_on_subscribe_m5,
                      [?USERNAME, {?MOUNTPOINT, ?CHANGED_CLIENT_ID}, [{?TOPIC, 1}], #{}]),
    deregister_hook(auth_on_subscribe_m5, ?ENDPOINT).

on_register_m5_test(_) ->
    register_hook(on_register_m5, ?ENDPOINT),
    Self = pid_to_bin(self()),
    UserProps = [{<<"k1">>, <<"v1">>}],
    Args = [?PEER, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, Self,
                       #{?P_SESSION_EXPIRY_INTERVAL => 5,
                         ?P_RECEIVE_MAX => 10,
                         ?P_TOPIC_ALIAS_MAX => 15,
                         ?P_REQUEST_RESPONSE_INFO => true,
                         ?P_REQUEST_PROBLEM_INFO => true,
                         ?P_USER_PROPERTY => UserProps}],
    [next] = vmq_plugin:all(on_register_m5, Args),
    ok = exp_response(on_register_m5_ok),
    deregister_hook(on_register_m5, ?ENDPOINT).

on_publish_m5_test(_) ->
    register_hook(on_publish_m5, ?ENDPOINT),
    Self = pid_to_bin(self()),
    Args = [Self, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, 1, ?TOPIC, ?PAYLOAD, false,
            #{?P_USER_PROPERTY =>
                  [{<<"k1">>, <<"v1">>},
                   {<<"k2">>, <<"v2">>}],
              ?P_CORRELATION_DATA => <<"correlation_data">>,
              ?P_RESPONSE_TOPIC => [<<"response">>,<<"topic">>],
              ?P_PAYLOAD_FORMAT_INDICATOR => utf8,
              ?P_CONTENT_TYPE => <<"content_type">>}],
    [next] = vmq_plugin:all(on_publish_m5, Args),
    ok = exp_response(on_publish_m5_ok),
    deregister_hook(on_publish_m5, ?ENDPOINT).

on_subscribe_m5_test(_) ->
    register_hook(on_subscribe_m5, ?ENDPOINT),
    Self = pid_to_bin(self()),
    Args =
        [Self, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, [{?TOPIC, {1, #{no_local => false,
                                                                  rap => false,
                                                                  retain_handling => send_retain}}},
                                                   {?TOPIC, not_allowed}],
         #{?P_USER_PROPERTY =>
               [{<<"k1">>, <<"v1">>}],
           ?P_SUBSCRIPTION_ID => [1,2,3]}],
    [next] = vmq_plugin:all(on_subscribe_m5, Args),
    ok = exp_response(on_subscribe_m5_ok),
    deregister_hook(on_subscribe_m5, ?ENDPOINT).

on_unsubscribe_m5_test(_) ->
    register_hook(on_unsubscribe_m5, ?ENDPOINT),
    Args = [?USERNAME, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, [?TOPIC],
            #{?P_USER_PROPERTY =>
                  [{<<"k1">>, <<"v1">>}]}],
    ok = vmq_plugin:all_till_ok(on_unsubscribe_m5, Args),
    {ok, #{topics := [[<<"rewritten">>, <<"topic">>]]}} = vmq_plugin:all_till_ok(on_unsubscribe_m5,
                      [?USERNAME, {?MOUNTPOINT, ?CHANGED_CLIENT_ID}, [?TOPIC], #{}]),
    deregister_hook(on_unsubscribe_m5, ?ENDPOINT).

on_deliver_m5_test(_) ->
    register_hook(on_deliver_m5, ?ENDPOINT),
    Self = pid_to_bin(self()),
    ok = vmq_plugin:all_till_ok(on_deliver_m5,
                                [Self, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, 1, ?TOPIC, ?PAYLOAD, false, #{}]),
    ok = exp_response(on_deliver_m5_ok),
    deregister_hook(on_deliver_m5, ?ENDPOINT).

on_deliver_m5_modify_props_test(_) ->
    register_hook(on_deliver_m5, ?ENDPOINT),
    Self = pid_to_bin(self()),
    Args = [Self, {?MOUNTPOINT, <<"modify_props">>}, 1,  ?TOPIC, ?PAYLOAD, false,
            #{?P_USER_PROPERTY =>
                  [{<<"k1">>, <<"v1">>},
                   {<<"k2">>, <<"v2">>}],
              ?P_CORRELATION_DATA => <<"correlation_data">>,
              ?P_RESPONSE_TOPIC => [<<"response">>,<<"topic">>],
              ?P_PAYLOAD_FORMAT_INDICATOR => utf8,
              ?P_CONTENT_TYPE => <<"content_type">>}],
    {ok, #{properties :=
          #{?P_USER_PROPERTY :=
                [{<<"k1">>, <<"v1">>},
                 {<<"k2">>, <<"v2">>},
                 {<<"k3">>, <<"v3">>}],
            ?P_CORRELATION_DATA := <<"modified_correlation_data">>,
            ?P_RESPONSE_TOPIC := [<<"modified_responsetopic">>],
            ?P_PAYLOAD_FORMAT_INDICATOR := undefined,
            ?P_CONTENT_TYPE := <<"modified_content_type">>}}}
        = vmq_plugin:all_till_ok(on_deliver_m5, Args),

    ok = exp_response(on_deliver_m5_ok),
    deregister_hook(on_deliver_m5, ?ENDPOINT).

on_auth_m5_test(_) ->
    register_hook(on_auth_m5, ?ENDPOINT),
    {ok,
     #{properties := #{?P_AUTHENTICATION_METHOD := <<"AUTH_METHOD">>,
                       ?P_AUTHENTICATION_DATA := <<"AUTH_DATA1">>}}}
        = vmq_plugin:all_till_ok(on_auth_m5,
                                 [?USERNAME, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID},
                                  #{?P_AUTHENTICATION_METHOD => <<"AUTH_METHOD">>,
                                    ?P_AUTHENTICATION_DATA => <<"AUTH_DATA0">>}]),
    deregister_hook(on_auth_m5, ?ENDPOINT).

on_offline_message_test(_) ->
    register_hook(on_offline_message, ?ENDPOINT),
    Self = pid_to_bin(self()),
    [next] = vmq_plugin:all(on_offline_message, [{?MOUNTPOINT, Self}, 1, ?TOPIC, ?PAYLOAD, false]),
    ok = exp_response(on_offline_message_ok),
    deregister_hook(on_offline_message, ?ENDPOINT).

on_client_wakeup_test(_) ->
    register_hook(on_client_wakeup, ?ENDPOINT),
    Self = pid_to_bin(self()),
    [next] = vmq_plugin:all(on_client_wakeup, [{?MOUNTPOINT, Self}]),
    ok = exp_response(on_client_wakeup_ok),
    deregister_hook(on_client_wakeup, ?ENDPOINT).

on_client_offline_test(_) ->
    register_hook(on_client_offline, ?ENDPOINT),
    Self = pid_to_bin(self()),
    [next] = vmq_plugin:all(on_client_offline, [{?MOUNTPOINT, Self}]),
    ok = exp_response(on_client_offline_ok),
    deregister_hook(on_client_offline, ?ENDPOINT).

on_client_gone_test(_) ->
    register_hook(on_client_gone, ?ENDPOINT),
    Self = pid_to_bin(self()),
    [next] = vmq_plugin:all(on_client_gone, [{?MOUNTPOINT, Self}]),
    ok = exp_response(on_client_gone_ok),
    deregister_hook(on_client_gone, ?ENDPOINT).

on_session_expired_test(_) ->
    register_hook(on_session_expired, ?ENDPOINT),
    Self = pid_to_bin(self()),
    [next] = vmq_plugin:all(on_session_expired, [{?MOUNTPOINT, Self}]),
    ok = exp_response(on_session_expired_ok),
    deregister_hook(on_session_expired, ?ENDPOINT).

base64payload_test(_) ->
    ok = clique:run(["vmq-admin", "webhooks", "register",
                     "hook=auth_on_publish", "endpoint=" ++ ?ENDPOINT, "--base64payload=true"]),
    {ok, [{payload, ?PAYLOAD}]} =
        vmq_plugin:all_till_ok(
          auth_on_publish,
          [?USERNAME, {?MOUNTPOINT, ?BASE64_PAYLOAD_CLIENT_ID}, 1, ?TOPIC, ?PAYLOAD, false]),
    deregister_hook(auth_on_publish, ?ENDPOINT).


auth_on_publish_no_payload_test(_) ->
    ok = clique:run(["vmq-admin", "webhooks", "register",
                     "hook=auth_on_publish", "endpoint=" ++ ?ENDPOINT, "--no_payload=true", "--base64payload=true"]),
    ok = vmq_plugin:all_till_ok(
          auth_on_publish,
          [?USERNAME, {?MOUNTPOINT, ?NO_PAYLOAD_CLIENT_ID}, 1, ?TOPIC, ?PAYLOAD, false]),
    deregister_hook(auth_on_publish, ?ENDPOINT).

auth_on_publish_m5_no_payload_test(_) ->
    ok = clique:run(["vmq-admin", "webhooks", "register",
                     "hook=auth_on_publish_m5", "endpoint=" ++ ?ENDPOINT, "--no_payload=true"]),
    ok = vmq_plugin:all_till_ok(
          auth_on_publish_m5,
          [?USERNAME, {?MOUNTPOINT, ?NO_PAYLOAD_CLIENT_ID}, 1, ?TOPIC, ?PAYLOAD, false, #{}]),
    deregister_hook(auth_on_publish_m5, ?ENDPOINT).

auth_on_register_undefined_creds_test(_) ->
    register_hook(auth_on_register, ?ENDPOINT),
    Username = undefined,
    Password = undefined,
    ok = vmq_plugin:all_till_ok(auth_on_register,
                      [?PEER, {?MOUNTPOINT, <<"undefined_creds">>}, Username, Password, true]),
    deregister_hook(auth_on_register, ?ENDPOINT).

%% Test for https://github.com/vernemq/vernemq/issues/740
cli_allow_query_parameters_test(_) ->
    EndpointWithParams = ?ENDPOINT ++ "/hook?key=value",
    ok = register_hook(auth_on_register, EndpointWithParams),
    [] = deregister_hook(auth_on_register, EndpointWithParams).

%% HTTPS Tests

%% Given a CA that signed the endpoint's server certificate, the webhook works
https_ca_test(Config) ->
    should_succeed(Config, #{}, #{cafile => cert_path(Config, "all-ca.crt")}).

%% Given a CA that dit not sign the endpoint's server certificate, the webhook fails
https_wrong_ca_test(Config) ->
    should_fail(Config, #{}, #{cafile => cert_path(Config, "test-fake-root-ca.crt")}).

%% Given a server (endpoint) certificate that has expired, the webhook fails
https_expired_server_cert_test(Config) ->
    should_fail(Config,
                #{certfile => cert_path(Config, "server-expired.crt")},
                #{cafile => cert_path(Config, "all-ca.crt")}).

%% Given a valid server (endpoint) certificate with a CN that does not match its hostname, the webhook fails
https_bad_server_cn_test(Config) ->
    should_fail(Config,
                #{certfile => cert_path(Config, "bad-cn-server.crt"),
                  keyfile => cert_path(Config, "bad-cn-server.key")},
                #{cafile => cert_path(Config, "all-ca.crt")}).

%% Authenticating to an endpoint with a client TLS certificate and the webhook works
https_client_cert_test(Config) ->
    ServerOpts = #{verify => verify_peer, fail_if_no_peer_cert => true},
    ClientOpts = #{cafile => cert_path(Config, "all-ca.crt"),
                   keyfile => cert_path(Config, "client.key"),
                   certfile => cert_path(Config, "client.crt")},
    should_succeed(Config, ServerOpts, ClientOpts).

%% Failing to provide a client certificate when required by the endpoint makes the webhook fail
https_missing_client_cert_fail_test(Config) ->
    should_fail(Config, #{verify => verify_peer, fail_if_no_peer_cert => true}, #{}).

%% An endpoint without a CRL available fails when requiring CRL checks
https_fails_with_no_crl_test(Config) ->
    should_fail(Config, #{}, #{use_crls => true}).

should_fail(Config, ServerOpts, ClientOpts) ->
    {didnt_receive_response, on_deliver_ok} = base_https_test(Config, ServerOpts, ClientOpts).

should_succeed(Config, ServerOpts, ClientOpts) ->
    ok = base_https_test(Config, ServerOpts, ClientOpts).

base_https_test(Config, ServerOpts, ClientSSLEnv) ->
    Opts = maps:merge(default_https_server_opts(Config), ServerOpts),
    ok = start_endpoint_tls(Opts),
    set_ssl_app_env(ClientSSLEnv),
    register_hook(on_deliver, ?HTTPS_ENDPOINT),
    Self = pid_to_bin(self()),
    _ = vmq_plugin:all_till_ok(on_deliver,
                                [Self, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, 1, ?TOPIC, ?PAYLOAD, false]),
    ExpResponse = exp_response(on_deliver_ok),
    clear_ssl_app_env(),
    deregister_hook(on_deliver, ?HTTPS_ENDPOINT),
    webhooks_handler:stop_endpoint_tls(),
    ExpResponse.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% If a test fails, we don't want subsequent tests
%% failing because the handler is already started.
start_endpoint_tls(Opts) ->
    webhooks_handler:stop_endpoint_tls(),
    webhooks_handler:start_endpoint_tls(Opts).

https_port(Config) -> ?config(https_port, Config).

%% takes a default options map and adds those not set
default_https_server_opts(Config) ->
    default_https_server_opts(Config, #{}).

default_https_server_opts(Config, Opts) ->
    Defaults = #{port => ?HTTPS_PORT,
                 keyfile => cert_path(Config, "server.key"),
                 certfile => cert_path(Config, "server.crt"),
                 cacertfile => cert_path(Config, "all-ca.crt")},
    maps:merge(Defaults, Opts).

set_ssl_app_env(Opts) ->
    Defaults = #{use_crls => false,
                 tls_version => 'tlsv1.2',
                 depth => 100},
    SSLEnv = maps:merge(Defaults, Opts),
    maps:map(fun (K,V) -> application:set_env(vmq_webhooks, K, V) end, SSLEnv).

clear_ssl_app_env() ->
    Keys = [cafile, certfile, keyfile, password, depth, tls_version],
    [application:unset_env(vmq_webhooks, Key)|| Key <- Keys].

cert_path(Config, Filename) ->
    CertsDir = ?config(certs_dir, Config),
    FullCertPath = filename:join([CertsDir, "ssl", Filename]),
    true = vmq_schema_util:file_is_pem_content(FullCertPath),
    FullCertPath.

register_hook(Hook, Endpoint) ->
    ok = clique:run(["vmq-admin", "webhooks", "register",
                     "hook=" ++ atom_to_list(Hook), "endpoint=" ++ Endpoint, "--base64payload=false"]).

deregister_hook(Hook, Endpoint) ->
    ok = clique:run(["vmq-admin", "webhooks", "deregister",
                     "hook=" ++ atom_to_list(Hook), "endpoint=" ++ Endpoint]),
    [] = vmq_webhooks_plugin:all_hooks().

pid_to_bin(Pid) ->
    list_to_binary(lists:flatten(io_lib:format("~p", [Pid]))).

exp_response(Exp) ->
    receive
        Exp -> ok;
        Got -> {received, Got, expected, Exp}
    after
        1000 ->
            {didnt_receive_response, Exp}
    end.

exp_nothing(Timeout) ->
    receive
        Got ->
            {received, Got, expected, nothing}
    after
        Timeout ->
            ok
    end.
