-module(vmq_diversity_auth_cache_SUITE).
-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([auth_cache_test/1,
         auth_cache_reject_test/1,
         auth_cache_pass_through_test/1,
         auth_cache_cleanup_test/1]).

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(_Config) ->
    application:load(vmq_plugin),
    application:ensure_all_started(vmq_plugin),
    vmq_plugin_mgr:enable_plugin(vmq_diversity),
    {ok, _} = vmq_diversity:load_script(code:lib_dir(vmq_diversity) ++ "/test/cache_test.lua"),
    cover:start(),
    _Config.

end_per_suite(_Config) ->
    vmq_plugin_mgr:disable_plugin(vmq_diversity),
    application:stop(vmq_plugin),
    _Config.

init_per_testcase(_Case, Config) ->
    vmq_diversity_cache:clear_cache(),
    Config.

end_per_testcase(_, Config) ->
    Config.

all() ->
    [auth_cache_test,
     auth_cache_reject_test,
     auth_cache_pass_through_test,
     auth_cache_cleanup_test].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
auth_cache_test(_) ->
    %% This test verifies that an action is granted if a cached ACL is found
    %% for this MP/ClientId
    ok = vmq_plugin:all_till_ok(auth_on_register,
                                [peer(), allowed_subscriber_id(), username(), password(), true]),
    ok = vmq_plugin:all_till_ok(auth_on_publish,
                                [username(), allowed_subscriber_id(), 0,
                                 [<<"a">>,<<"b">>, <<"c">>], payload(), false]),
    ok = vmq_plugin:all_till_ok(auth_on_publish,
                      [username(), allowed_subscriber_id(), 0,
                       [<<"a">>, <<>>, <<"test-user">>,
                        <<"allowed-subscriber-id">>, <<"hello">>, <<"world">>],
                       payload(), false]),
    ok = vmq_plugin:all_till_ok(auth_on_subscribe,
                                [username(), allowed_subscriber_id(),
                                 [{[<<"a">>, <<"b">>, <<"c">>], 0},
                                  {[<<"a">>, <<>>, <<"test-user">>,
                                    <<"allowed-subscriber-id">>, <<"+">>, <<"world">>], 0}]]),
    {ok, PubMods1} =
        vmq_plugin:all_till_ok(auth_on_publish,
                               [username(), allowed_subscriber_id(), 0,
                                [<<"modifiers">>], payload(), false]),
    [] = PubMods1 -- [{topic,[<<"hello">>,<<"world">>]},
                      {retain,true},
                      {qos,1},
                      {payload,<<"hello world">>},
                      {mountpoint,"override-mountpoint2"}],

    {ok, [{[<<"hello">>,<<"world">>],2}]} =
        vmq_plugin:all_till_ok(auth_on_subscribe,
                               [username(), allowed_subscriber_id(),
                                [{[<<"modifiers">>], 0}]]),

    {ok, #{topic := [<<"hello">>,<<"world">>],
           retain := true,
           qos := 1,
           payload := <<"hello world">>,
           mountpoint := "override-mountpoint2"}} =
        vmq_plugin:all_till_ok(auth_on_publish_m5,
                               [username(), allowed_subscriber_id(), 0,
                                [<<"modifiers">>], payload(), false, #{}]),

    {ok, #{topics := [{[<<"hello">>,<<"world">>],2}]}} =
        vmq_plugin:all_till_ok(auth_on_subscribe_m5,
                               [username(), allowed_subscriber_id(),
                                [{[<<"modifiers">>], 0}], #{}]),
    ok.

auth_cache_reject_test(_) ->
    %% This test verifies that an action is rejected if a cached ACL is found
    %% for this MP/ClientId but wrong publish/subscribe Topics are used
    ok = vmq_plugin:all_till_ok(auth_on_register,
                      [peer(), allowed_subscriber_id(), username(), password(), true]),
    {error, _} = vmq_plugin:all_till_ok(auth_on_publish,
                      [username(), allowed_subscriber_id(), 0,
                       [<<"c">>, <<"b">>, <<"a">>], payload(), false]),
    {error, _} = vmq_plugin:all_till_ok(auth_on_publish,
                      [username(), allowed_subscriber_id(), 0,
                       [<<"a">>, <<>>, <<"not-my-user">>,
                        <<"allowed-subscriber-id">>, <<"hello">>, <<"world">>],
                       payload(), false]),
    {error, _} = vmq_plugin:all_till_ok(auth_on_subscribe,
                      [username(), allowed_subscriber_id(),
                       [{[<<"c">>, <<"b">>, <<"a">>], 0},
                        {[<<"a">>, <<>>, <<"test-user">>,
                          <<"allowed-subscriber-id">>, <<"+">>, <<"world">>], 0}]]).

auth_cache_pass_through_test(_) ->
    %% This test verifies that the Lua function gets called if no
    %% cached ACL is found for this MP/ClientId
    ok = vmq_plugin:all_till_ok(auth_on_register,
                      [peer(), ignored_subscriber_id(), username(), password(), true]),
    ok = vmq_plugin:all_till_ok(auth_on_publish,
                      [username(), ignored_subscriber_id(), 0,
                       [<<"c">>, <<"b">>, <<"a">>], payload(), false]),
    ok = vmq_plugin:all_till_ok(auth_on_publish,
                      [username(), ignored_subscriber_id(), 0,
                       [<<"a">>, <<>>, <<"not-my-user">>,
                        <<"ignored-subscriber-id">>, <<"hello">>, <<"world">>],
                       payload(), false]),
    ok = vmq_plugin:all_till_ok(auth_on_subscribe,
                      [username(), ignored_subscriber_id(),
                       [{[<<"c">>, <<"b">>, <<"a">>], 0},
                        {[<<"a">>, <<>>, <<"test-user">>,
                          <<"ignored-subscriber-id">>, <<"+">>, <<"world">>], 0}]]).

auth_cache_cleanup_test(_) ->
    %% This test verifies that an action is rejected if a cached ACL is found
    %% for this MP/ClientId but wrong publish/subscribe Topics are used
    [] = vmq_diversity_cache:entries(<<"">>, <<"allowed-subscriber-id">>),
    ok = vmq_plugin:all_till_ok(auth_on_register,
                      [peer(), allowed_subscriber_id(), username(), password(), true]),
    [{publish, PublishAcls},
     {subscribe, SubscribeAcls}] = vmq_diversity_cache:entries(<<"">>, <<"allowed-subscriber-id">>),
    3 = length(PublishAcls),
    3 = length(SubscribeAcls),
    vmq_plugin:all(on_client_offline, [allowed_subscriber_id()]),
    [] = vmq_diversity_cache:entries(<<"">>, <<"allowed-subscriber-id">>).


peer() -> {{192, 168, 123, 123}, 12345}.

ignored_subscriber_id() ->
    {"", <<"ignored-subscriber-id">>}.

allowed_subscriber_id() ->
    {"", <<"allowed-subscriber-id">>}.

%not_allowed_subscriber_id() ->
%    {"", <<"not-allowed-subscriber-id">>}.
%
%changed_subscriber_id() ->
%    {"", <<"changed-subscriber-id">>}.
%

username() -> <<"test-user">>.
password() -> <<"test-password">>.
payload() -> <<"hello world">>.
