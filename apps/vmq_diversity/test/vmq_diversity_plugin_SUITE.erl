-module(vmq_diversity_plugin_SUITE).
-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([auth_on_register_test/1,
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
         auth_on_register_undefined_creds_test/1
        ]).

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(_Config) ->
    application:load(vmq_plugin),
    application:ensure_all_started(vmq_plugin),
    vmq_plugin_mgr:enable_plugin(vmq_diversity),
    {ok, _} = vmq_diversity:load_script(code:lib_dir(vmq_diversity) ++ "/test/plugin_test.lua"),
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

all() ->
    [auth_on_register_test,
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
     auth_on_register_undefined_creds_test
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
auth_on_register_test(_) ->
    ok = vmq_plugin:all_till_ok(auth_on_register,
                      [peer(), allowed_subscriber_id(), username(), password(), true]),
    {error, error} = vmq_plugin:all_till_ok(auth_on_register,
                      [peer(), not_allowed_subscriber_id(), username(), password(), true]),
    {error, chain_exhausted} = vmq_plugin:all_till_ok(auth_on_register,
                      [peer(), ignored_subscriber_id(), username(), password(), true]),
    {ok, [{subscriber_id, {"override-mountpoint", <<"override-client-id">>}}]} = vmq_plugin:all_till_ok(auth_on_register,
                      [peer(), changed_subscriber_id(), username(), password(), true]).

auth_on_publish_test(_) ->
    ok = vmq_plugin:all_till_ok(auth_on_publish,
                      [username(), allowed_subscriber_id(), 1, topic(), payload(), false]),
    {error, error} = vmq_plugin:all_till_ok(auth_on_publish,
                      [username(), not_allowed_subscriber_id(), 1, topic(), payload(), false]),
    {error, chain_exhausted} = vmq_plugin:all_till_ok(auth_on_publish,
                      [username(), ignored_subscriber_id(), 1, topic(), payload(), false]),
    {ok, [{topic, [<<"hello">>, <<"world">>]}]} = vmq_plugin:all_till_ok(auth_on_publish,
                      [username(), changed_subscriber_id(), 1, topic(), payload(), false]).
auth_on_subscribe_test(_) ->
    ok = vmq_plugin:all_till_ok(auth_on_subscribe,
                      [username(), allowed_subscriber_id(), [{topic(), 1}]]),
    {error, error} = vmq_plugin:all_till_ok(auth_on_subscribe,
                      [username(), not_allowed_subscriber_id(), [{topic(), 1}]]),
    {error, chain_exhausted} = vmq_plugin:all_till_ok(auth_on_subscribe,
                      [username(), ignored_subscriber_id(), [{topic(), 1}]]),
    {ok, [{[<<"hello">>, <<"world">>], 2}]} = vmq_plugin:all_till_ok(auth_on_subscribe,
                      [username(), changed_subscriber_id(), [{topic(), 1}]]).
on_register_test(_) ->
    [next] = vmq_plugin:all(on_register,
                            [peer(), allowed_subscriber_id(), username()]).
on_publish_test(_) ->
    [next] = vmq_plugin:all(on_publish,
                            [username(), allowed_subscriber_id(), 1, topic(), payload(), false]).
on_subscribe_test(_) ->
    [next] = vmq_plugin:all(on_subscribe,
                            [username(), allowed_subscriber_id(), [{topic(), 1}]]).

on_unsubscribe_test(_) ->
    {error, chain_exhausted} = vmq_plugin:all_till_ok(on_unsubscribe,
                                             [username(), allowed_subscriber_id(), [topic()]]),
    {ok, [[<<"hello">>, <<"world">>]]} = vmq_plugin:all_till_ok(on_unsubscribe,
                      [username(), changed_subscriber_id(), [topic()]]).

on_deliver_test(_) ->
    ok = vmq_plugin:all_till_ok(on_deliver,
                                [username(), allowed_subscriber_id(), topic(), payload()]).

on_offline_message_test(_) ->
    [next] = vmq_plugin:all(on_offline_message, [allowed_subscriber_id(), 2,
                                                 topic(), payload(), false]).
on_client_wakeup_test(_) ->
    [next] = vmq_plugin:all(on_client_wakeup, [allowed_subscriber_id()]).
on_client_offline_test(_) ->
    [next] = vmq_plugin:all(on_client_offline, [allowed_subscriber_id()]).
on_client_gone_test(_) ->
    [next] = vmq_plugin:all(on_client_gone, [allowed_subscriber_id()]).

auth_on_register_undefined_creds_test(_) ->
    Username = undefined,
    Password = undefined,
    ok = vmq_plugin:all_till_ok(auth_on_register,
                      [peer(), {"", <<"undefined_creds">>}, Username, Password, true]).

peer() -> {{192, 168, 123, 123}, 12345}.

ignored_subscriber_id() ->
    {"", <<"ignored-subscriber-id">>}.

allowed_subscriber_id() ->
    {"", <<"allowed-subscriber-id">>}.

not_allowed_subscriber_id() ->
    {"", <<"not-allowed-subscriber-id">>}.

changed_subscriber_id() ->
    {"", <<"changed-subscriber-id">>}.


username() -> <<"test-user">>.
password() -> <<"test-password">>.
topic() -> [<<"test">>, <<"topic">>].
payload() -> <<"hello world">>.
