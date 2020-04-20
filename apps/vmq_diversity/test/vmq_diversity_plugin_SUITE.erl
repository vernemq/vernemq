-module(vmq_diversity_plugin_SUITE).
-include_lib("vernemq_dev/include/vernemq_dev.hrl").
-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-compile(export_all).
-compile(nowarn_export_all).

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
     on_session_expired_test,
     auth_on_register_undefined_creds_test,
     invalid_modifiers_test,

     auth_on_register_m5_test,
     auth_on_register_m5_modify_props_test,
     on_register_m5_test,
     on_publish_m5_test,
     auth_on_subscribe_m5_test,
     on_subscribe_m5_test,
     on_unsubscribe_m5_test,
     auth_on_publish_m5_test,
     auth_on_publish_m5_modify_props_test,
     on_auth_m5_test
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
auth_on_register_test(_) ->
    ok = vmq_plugin:all_till_ok(auth_on_register,
                      [peer(), allowed_subscriber_id(), username(), password(), true]),
    {error,invalid_credentials} = vmq_plugin:all_till_ok(auth_on_register,
                      [peer(), not_allowed_subscriber_id(), username(), password(), true]),
    {error, plugin_chain_exhausted} = vmq_plugin:all_till_ok(auth_on_register,
                      [peer(), ignored_subscriber_id(), username(), password(), true]),
    {ok, [{subscriber_id, {"override-mountpoint", <<"override-client-id">>}}]} = vmq_plugin:all_till_ok(auth_on_register,
                      [peer(), changed_subscriber_id(), username(), password(), true]),
    {ok, [{username, <<"override-username">>}]} = vmq_plugin:all_till_ok(auth_on_register,
                      [peer(), changed_username(), username(), password(), true]).

props() ->
    #{p_user_property => [{<<"key1">>, <<"val1">>}]}.

auth_on_publish_test(_) ->
    ok = vmq_plugin:all_till_ok(auth_on_publish,
                      [username(), allowed_subscriber_id(), 1, topic(), payload(), false]),
    {error, not_authorized} = vmq_plugin:all_till_ok(auth_on_publish,
                      [username(), not_allowed_subscriber_id(), 1, topic(), payload(), false]),
    {error, plugin_chain_exhausted} = vmq_plugin:all_till_ok(auth_on_publish,
                      [username(), ignored_subscriber_id(), 1, topic(), payload(), false]),
    {ok, [{topic, [<<"hello">>, <<"world">>]}]} = vmq_plugin:all_till_ok(auth_on_publish,
                      [username(), changed_subscriber_id(), 1, topic(), payload(), false]).

invalid_modifiers_test(_) ->
    {error,{invalid_modifiers,#{topic := 5}}} =
        vmq_plugin:all_till_ok(auth_on_publish_m5,
                               [username(), {"", <<"invalid_topic_mod">>}, 1, topic(), payload(), false, props()]),
    {error,{invalid_modifiers,#{unknown := 5}}} =
        vmq_plugin:all_till_ok(auth_on_publish_m5,
                               [username(), {"", <<"unknown_mod">>}, 1, topic(), payload(), false, props()]).

auth_on_subscribe_test(_) ->
    ok = vmq_plugin:all_till_ok(auth_on_subscribe,
                      [username(), allowed_subscriber_id(), [{topic(), 1}]]),
    {error, not_authorized} = vmq_plugin:all_till_ok(auth_on_subscribe,
                      [username(), not_allowed_subscriber_id(), [{topic(), 1}]]),
    {error, plugin_chain_exhausted} = vmq_plugin:all_till_ok(auth_on_subscribe,
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
    {error, plugin_chain_exhausted} = vmq_plugin:all_till_ok(on_unsubscribe,
                                             [username(), allowed_subscriber_id(), [topic()]]),
    {ok, [[<<"hello">>, <<"world">>]]} = vmq_plugin:all_till_ok(on_unsubscribe,
                      [username(), changed_subscriber_id(), [topic()]]).

on_deliver_test(_) ->
    ok = vmq_plugin:all_till_ok(on_deliver,
                                [username(), allowed_subscriber_id(), 1, topic(), payload(), false]).

on_offline_message_test(_) ->
    [next] = vmq_plugin:all(on_offline_message, [allowed_subscriber_id(), 2,
                                                 topic(), payload(), false]).
on_client_wakeup_test(_) ->
    [next] = vmq_plugin:all(on_client_wakeup, [allowed_subscriber_id()]).
on_client_offline_test(_) ->
    [next] = vmq_plugin:all(on_client_offline, [allowed_subscriber_id()]).
on_client_gone_test(_) ->
    [next] = vmq_plugin:all(on_client_gone, [allowed_subscriber_id()]).
on_session_expired_test(_) ->
    [next] = vmq_plugin:all(on_session_expired, [allowed_subscriber_id()]).

auth_on_register_undefined_creds_test(_) ->
    Username = undefined,
    Password = undefined,
    ok = vmq_plugin:all_till_ok(auth_on_register,
                      [peer(), {"", <<"undefined_creds">>}, Username, Password, true]).

auth_on_publish_m5_test(_) ->
    ok = vmq_plugin:all_till_ok(auth_on_publish_m5,
                      [username(), allowed_subscriber_id(), 1, topic(), payload(), false, props()]),
    {error, not_authorized} = vmq_plugin:all_till_ok(auth_on_publish_m5,
                      [username(), not_allowed_subscriber_id(), 1, topic(), payload(), false, props()]),
    {error, plugin_chain_exhausted} = vmq_plugin:all_till_ok(auth_on_publish_m5,
                      [username(), ignored_subscriber_id(), 1, topic(), payload(), false, props()]),
    {ok, #{topic := [<<"hello">>, <<"world">>]}} = vmq_plugin:all_till_ok(auth_on_publish_m5,
                      [username(), changed_subscriber_id(), 1, topic(), payload(), false, props()]).

auth_on_register_m5_test(_) ->
    ok = vmq_plugin:all_till_ok(auth_on_register_m5,
                      [peer(), allowed_subscriber_id(), username(), password(), true, props()]),
    {error,invalid_credentials} = vmq_plugin:all_till_ok(auth_on_register_m5,
                      [peer(), not_allowed_subscriber_id(), username(), password(), true, #{}]),
    {error, plugin_chain_exhausted} = vmq_plugin:all_till_ok(auth_on_register_m5,
                      [peer(), ignored_subscriber_id(), username(), password(), true, #{}]),
    {ok, #{subscriber_id := {"override-mountpoint", <<"override-client-id">>}}} = vmq_plugin:all_till_ok(auth_on_register_m5,
                      [peer(), changed_subscriber_id(), username(), password(), true, #{}]),
    {ok, #{username := <<"override-username">>}} = vmq_plugin:all_till_ok(auth_on_register_m5,
                      [peer(), changed_username(), username(), password(), true, #{}]).

auth_on_register_m5_modify_props_test(_) ->
    WantUserProps = [{<<"k1">>, <<"v1">>},
                     {<<"k1">>, <<"v2">>},
                     {<<"k2">>, <<"v2">>}],
    Args = [peer(), {"", <<"modify_props">>}, username(), password(), true,
            #{?P_SESSION_EXPIRY_INTERVAL => 5,
              ?P_RECEIVE_MAX => 10,
              ?P_TOPIC_ALIAS_MAX => 15,
              ?P_REQUEST_RESPONSE_INFO => true,
              ?P_REQUEST_PROBLEM_INFO => true,
              ?P_USER_PROPERTY => WantUserProps}],
    {ok, #{properties :=
               #{?P_USER_PROPERTY :=
                     [{<<"k3">>, <<"v3">>}],
                 ?P_SESSION_EXPIRY_INTERVAL := 10}}}
        = vmq_plugin:all_till_ok(auth_on_register_m5, Args).

on_register_m5_test(_) ->
    UserProps = [{<<"k1">>, <<"v1">>},
                 {<<"k1">>, <<"v2">>},
                 {<<"k2">>, <<"v2">>}],
    Args = [peer(), allowed_subscriber_id(), username(),
            #{?P_SESSION_EXPIRY_INTERVAL => 5,
              ?P_RECEIVE_MAX => 10,
              ?P_TOPIC_ALIAS_MAX => 15,
              ?P_REQUEST_RESPONSE_INFO => true,
              ?P_REQUEST_PROBLEM_INFO => true,
              ?P_USER_PROPERTY => UserProps}],
    [next] = vmq_plugin:all(on_register_m5, Args).

auth_on_publish_m5_modify_props_test(_) ->
    Args = [username(), {"", <<"modify_props">>}, 1, topic(), payload(), false,
            #{?P_USER_PROPERTY =>
                  [{<<"k1">>, <<"v1">>},
                   {<<"k2">>, <<"v2">>}],
              ?P_CORRELATION_DATA => <<"correlation_data">>,
              ?P_RESPONSE_TOPIC => [<<"response">>,<<"topic">>],
              ?P_PAYLOAD_FORMAT_INDICATOR => utf8,
              ?P_CONTENT_TYPE => <<"content_type">>}],
    ExpProps =
        #{?P_USER_PROPERTY =>
              [{<<"k1">>, <<"v1">>},
               {<<"k2">>, <<"v2">>},
               {<<"k3">>, <<"v3">>}],
          ?P_CORRELATION_DATA => <<"modified_correlation_data">>,
          ?P_RESPONSE_TOPIC => [<<"modified">>, <<"response">>, <<"topic">>],
          ?P_PAYLOAD_FORMAT_INDICATOR => undefined,
          ?P_CONTENT_TYPE => <<"modified_content_type">>},
    {ok, #{properties := ExpProps}} = vmq_plugin:all_till_ok(auth_on_publish_m5, Args).

on_publish_m5_test(_) ->
    Args = [username(), {"", <<"modify_props">>}, 1, topic(), payload(), false,
            #{?P_USER_PROPERTY =>
                  [{<<"k1">>, <<"v1">>},
                   {<<"k2">>, <<"v2">>}],
              ?P_CORRELATION_DATA => <<"correlation_data">>,
              ?P_RESPONSE_TOPIC => [<<"response">>,<<"topic">>],
              ?P_PAYLOAD_FORMAT_INDICATOR => utf8,
              ?P_CONTENT_TYPE => <<"content_type">>}],
    [next] = vmq_plugin:all(on_publish_m5, Args).

on_deliver_m5_test(_) ->
    Args = [username(), allowed_subscriber_id(), 1, topic(), payload(), false,
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
            ?P_RESPONSE_TOPIC := [<<"modified">>, <<"response">>,<<"topic">>],
            ?P_PAYLOAD_FORMAT_INDICATOR := undefined,
            ?P_CONTENT_TYPE := <<"modified_content_type">>}}}
        = vmq_plugin:all_till_ok(on_deliver_m5, Args).

auth_on_subscribe_m5_test(_) ->
    Props = #{?P_USER_PROPERTY =>
                  [{<<"k1">>, <<"v1">>}],
              ?P_SUBSCRIPTION_ID => [1,2,3]},
    ok = vmq_plugin:all_till_ok(auth_on_subscribe_m5,
                      [username(), allowed_subscriber_id(), [{topic(), {1, subopts()}}], Props]),
    {error, not_authorized} = vmq_plugin:all_till_ok(auth_on_subscribe_m5,
                      [username(), not_allowed_subscriber_id(), [{topic(), {1, subopts()}}], props()]),
    {error, plugin_chain_exhausted} = vmq_plugin:all_till_ok(auth_on_subscribe_m5,
                      [username(), ignored_subscriber_id(), [{topic(), {1, subopts()}}], props()]),
    {ok, #{topics := [{[<<"hello">>, <<"world">>], {2, #{rap := true}}}]}} = vmq_plugin:all_till_ok(auth_on_subscribe_m5,
                      [username(), changed_subscriber_id(), [{topic(), {1, subopts()}}], props()]).

on_subscribe_m5_test(_) ->
    Props = #{?P_USER_PROPERTY =>
                  [{<<"k1">>, <<"v1">>}],
              ?P_SUBSCRIPTION_ID => [1,2,3]},
    [next] = vmq_plugin:all(on_subscribe_m5,
                          [username(), allowed_subscriber_id(), [{topic(), {1, subopts()}}], Props]).


on_unsubscribe_m5_test(_) ->
    Args = [username(), changed_subscriber_id(), [topic()],
            #{?P_USER_PROPERTY =>
                  [{<<"k1">>, <<"v1">>}]}],
    {ok, #{topics := [[<<"hello">>, <<"world">>]]}}
        = vmq_plugin:all_till_ok(on_unsubscribe_m5, Args).

on_auth_m5_test(_) ->
    {ok, #{properties := #{?P_AUTHENTICATION_METHOD := <<"AUTH_METHOD">>,
                           ?P_AUTHENTICATION_DATA := <<"AUTH_DATA1">>}}}
        = vmq_plugin:all_till_ok(on_auth_m5,
                                 [username(), allowed_subscriber_id(),
                                  #{?P_AUTHENTICATION_METHOD => <<"AUTH_METHOD">>,
                                    ?P_AUTHENTICATION_DATA => <<"AUTH_DATA0">>}]).

%%%%%%%%%%%%%%%%%%%%%%%%% helpers %%%%%%%%%%%%%%%%%%%%%%%%%
peer() -> {{192, 168, 123, 123}, 12345}.

ignored_subscriber_id() ->
    {"", <<"ignored-subscriber-id">>}.

allowed_subscriber_id() ->
    {"", <<"allowed-subscriber-id">>}.

not_allowed_subscriber_id() ->
    {"", <<"not-allowed-subscriber-id">>}.

changed_subscriber_id() ->
    {"", <<"changed-subscriber-id">>}.

changed_username() ->
    {"", <<"changed-username">>}.

username() -> <<"test-user">>.
password() -> <<"test-password">>.
topic() -> [<<"test">>, <<"topic">>].
payload() -> <<"hello world">>.
subopts() ->
    #{rap => true,
      no_local => false}.
