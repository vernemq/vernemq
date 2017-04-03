-module(vmq_webhooks_SUITE).
-include("vmq_webhooks_test.hrl").

-export([
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([
         auth_on_register_test/1,
         auth_on_publish_test/1,
         auth_on_subscribe_test/1,
         on_register_test/1,
         on_publish_test/1,
         on_subscribe_test/1,
         on_unsubscribe_test/1,
         on_deliver_test/1,
         on_offline_message_test/1,
         on_client_wakeup_test/1,
         on_client_offline_test/1,
         on_client_gone_test/1,
         base64payload_test/1,
         auth_on_register_undefined_creds_test/1,
         cache_auth_on_register/1,
         cache_auth_on_publish/1,
         cache_auth_on_subscribe/1,
         cache_expired_entry/1
        ]).

init_per_suite(_Config) ->
    {ok, StartedApps} = application:ensure_all_started(vmq_server),
    ok = vmq_plugin_mgr:enable_plugin(vmq_webhooks),
    {ok, _} = application:ensure_all_started(cowboy),
    start_endpoint(),
    cover:start(),
    [{started_apps, StartedApps} |_Config].

end_per_suite(_Config) ->
    vmq_plugin_mgr:disable_plugin(vmq_webhooks),
    stop_endpoint(),
    application:stop(cowboy),
    application:stop(vmq_server),
    [ application:stop(App) || App <- proplists:get_value(started_apps, _Config, []) ],
   _Config.

init_per_testcase(_Case, Config) ->
    vmq_webhooks_cache:purge_all(),
    Config.

end_per_testcase(_, Config) ->
    Config.

all() ->
    [
     auth_on_register_test,
     auth_on_publish_test,
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
     base64payload_test,
     auth_on_register_undefined_creds_test,
     cache_auth_on_register,
     cache_auth_on_publish,
     cache_auth_on_subscribe,
     cache_expired_entry
    ].


start_endpoint() ->
    webhooks_handler:start_endpoint().

stop_endpoint() ->
    webhooks_handler:stop_endpoint().

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
    {error, error} = vmq_plugin:all_till_ok(auth_on_register,
                      [?PEER, {?MOUNTPOINT, ?NOT_ALLOWED_CLIENT_ID}, ?USERNAME, ?PASSWORD, true]),
    {error, chain_exhausted} = vmq_plugin:all_till_ok(auth_on_register,
                      [?PEER, {?MOUNTPOINT, ?IGNORED_CLIENT_ID}, ?USERNAME, ?PASSWORD, true]),
    {ok, [{subscriber_id,
           {"mynewmount", <<"changed_client_id">>}}]} = vmq_plugin:all_till_ok(auth_on_register,
                      [?PEER, {?MOUNTPOINT, ?CHANGED_CLIENT_ID}, ?USERNAME, ?PASSWORD, true]),
    deregister_hook(auth_on_register, ?ENDPOINT).

auth_on_publish_test(_) ->
    register_hook(auth_on_publish, ?ENDPOINT),
    ok = vmq_plugin:all_till_ok(auth_on_publish,
                      [?USERNAME, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, 1, ?TOPIC, ?PAYLOAD, false]),
    {error, error} = vmq_plugin:all_till_ok(auth_on_publish,
                      [?USERNAME, {?MOUNTPOINT, ?NOT_ALLOWED_CLIENT_ID}, 1, ?TOPIC, ?PAYLOAD, false]),
    {error, chain_exhausted} = vmq_plugin:all_till_ok(auth_on_publish,
                      [?USERNAME, {?MOUNTPOINT, ?IGNORED_CLIENT_ID}, 1, ?TOPIC, ?PAYLOAD, false]),
    {ok, [{topic, [<<"rewritten">>, <<"topic">>]}]} = vmq_plugin:all_till_ok(auth_on_publish,
                      [?USERNAME, {?MOUNTPOINT, ?CHANGED_CLIENT_ID}, 1, ?TOPIC, ?PAYLOAD, false]),
    deregister_hook(auth_on_publish, ?ENDPOINT).

auth_on_subscribe_test(_) ->
    register_hook(auth_on_subscribe, ?ENDPOINT),
    ok = vmq_plugin:all_till_ok(auth_on_subscribe,
                      [?USERNAME, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, [{?TOPIC, 1}]]),
    {error, error} = vmq_plugin:all_till_ok(auth_on_subscribe,
                      [?USERNAME, {?MOUNTPOINT, ?NOT_ALLOWED_CLIENT_ID}, [{?TOPIC, 1}]]),
    {error, chain_exhausted} = vmq_plugin:all_till_ok(auth_on_subscribe,
                      [?USERNAME, {?MOUNTPOINT, ?IGNORED_CLIENT_ID}, [{?TOPIC, 1}]]),
    {ok, [{[<<"forbidden">>, <<"topic">>], not_allowed},
          {[<<"rewritten">>, <<"topic">>], 2}]} = vmq_plugin:all_till_ok(auth_on_subscribe,
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
    {ok, [[<<"rewritten">>, <<"topic">>]]} = vmq_plugin:all_till_ok(on_unsubscribe,
                      [?USERNAME, {?MOUNTPOINT, ?CHANGED_CLIENT_ID}, [?TOPIC]]),
    deregister_hook(on_unsubscribe, ?ENDPOINT).

on_deliver_test(_) ->
    register_hook(on_deliver, ?ENDPOINT),
    Self = pid_to_bin(self()),
    ok = vmq_plugin:all_till_ok(on_deliver,
                                [Self, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, ?TOPIC, ?PAYLOAD]),
    ok = exp_response(on_deliver_ok),
    deregister_hook(on_deliver, ?ENDPOINT).

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

base64payload_test(_) ->
    ok = clique:run(["vmq-admin", "webhooks", "register",
                     "hook=auth_on_publish", "endpoint=" ++ ?ENDPOINT, "--base64payload=true"]),
    {ok, [{payload, ?PAYLOAD}]} =
        vmq_plugin:all_till_ok(
          auth_on_publish,
          [?USERNAME, {?MOUNTPOINT, ?BASE64_PAYLOAD_CLIENT_ID}, 1, ?TOPIC, ?PAYLOAD, false]),
    deregister_hook(auth_on_publish, ?ENDPOINT).

auth_on_register_undefined_creds_test(_) ->
    register_hook(auth_on_register, ?ENDPOINT),
    Username = undefined,
    Password = undefined,
    ok = vmq_plugin:all_till_ok(auth_on_register,
                      [?PEER, {?MOUNTPOINT, <<"undefined_creds">>}, Username, Password, true]),
    deregister_hook(auth_on_register, ?ENDPOINT).
    
%% helper functions
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
                              
                             
