-module(vmq_fetch_matched_topic_subscribers_lua_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(_Config) ->
    cover:start(),
    [{ct_hooks, vmq_cth} |_Config].

end_per_suite(_Config) ->
    _Config.

init_per_testcase(_Case, Config) ->
    vmq_test_utils:setup(vmq_reg_redis_trie),
    eredis:q(whereis(redis_client), ["FLUSHDB"]),
    Config.

end_per_testcase(_, Config) ->
    vmq_test_utils:teardown(),
    Config.

all() ->
    [
     fetch_matched_topic_subscribers_test
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

fetch_matched_topic_subscribers_test(_) ->
    MP = "",
    NodeName = <<"Node-A">>,
    ClientId = <<"CID-1">>,
    CS = true,
    Group = <<"group1">>,
    Topic1 = <<"topic1/123">>,
    Topic2 = <<"topic2/123">>,
    SharedTopic = <<"$share/", Group/binary, "/topic1/123">>,
    QoS_0 = <<"0">>,
    QoS_1 = <<"1">>,
    QoS_2 = <<"2">>,
    Topic1WithQoS = [Topic1, QoS_2],
    Topic2WithQoS = [Topic2, QoS_1],
    SharedTopicWithQoS = [SharedTopic, QoS_0],

    remap_subscriber(MP, ClientId, NodeName, CS, os:system_time(nanosecond)),
    {ok, []} = eredis:q(whereis(redis_client), ["FCALL",
                                                fetch_matched_topic_subscribers,
                                                0,
                                                MP,
                                                1,
                                                Topic1]),
    {ok,[NodeName, <<"1">>,[]]} = eredis:q(whereis(redis_client), ["FCALL",
                                                                   subscribe,
                                                                   0,
                                                                   MP,
                                                                   ClientId,
                                                                   NodeName,
                                                                   os:system_time(nanosecond),
                                                                   1 | Topic1WithQoS]),
    {ok,[[NodeName, ClientId, QoS_2]]} = eredis:q(whereis(redis_client), ["FCALL",
                                                                          fetch_matched_topic_subscribers,
                                                                          0,
                                                                          MP,
                                                                          1,
                                                                          Topic1]),
    {ok,[NodeName,<<"1">>,[Topic1WithQoS]]} = eredis:q(whereis(redis_client), ["FCALL",
                                                                               subscribe,
                                                                               0,
                                                                               MP,
                                                                               ClientId,
                                                                               NodeName,
                                                                               os:system_time(nanosecond),
                                                                               1 | Topic2WithQoS]),
    {ok,[[NodeName, ClientId, QoS_1]]} = eredis:q(whereis(redis_client), ["FCALL",
                                                                          fetch_matched_topic_subscribers,
                                                                          0,
                                                                          MP,
                                                                          1,
                                                                          Topic2]),
    {ok,[NodeName,
         <<"1">>,
         [Topic1WithQoS, Topic2WithQoS]]} = eredis:q(whereis(redis_client), ["FCALL",
                                                                             subscribe,
                                                                             0,
                                                                             MP,
                                                                             ClientId,
                                                                             NodeName,
                                                                             os:system_time(nanosecond),
                                                                             1 | SharedTopicWithQoS]),
    {ok, [_, _]} = eredis:q(whereis(redis_client), ["FCALL",
                                                   fetch_matched_topic_subscribers,
                                                   0,
                                                   MP,
                                                   1,
                                                   Topic1]),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Helper
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

remap_subscriber(MP, ClientId, NodeName, CS, TS) ->
    {ok, _} = eredis:q(whereis(redis_client), ["FCALL",
                                               remap_subscriber,
                                               0,
                                               MP,
                                               ClientId,
                                               NodeName,
                                               CS,
                                               TS]),
    ok.
