-module(vmq_subscribe_lua_SUITE).

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
     subscribe_without_remap_test,
     stale_subscribe_test,
     subscribe_from_different_node_test,
     new_topics_subscribe_test,
     new_shared_topics_subscribe_test
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

subscribe_without_remap_test(_) ->
    MP = "",
    ClientId = "CID-123",
    NodeName = <<"Node-A">>,
    TopicWithQoS = [<<"topic1/123">>, <<"2">>],
    NumOfTopics = 1,

    {ok, []} = eredis:q(whereis(redis_client), ["FCALL",
                                                subscribe,
                                                0,
                                                MP,
                                                ClientId,
                                                NodeName,
                                                os:system_time(nanosecond),
                                                NumOfTopics | TopicWithQoS]),
    {ok, [NodeName, <<"1">>, [TopicWithQoS]]} = eredis:q(whereis(redis_client), ["FCALL",
                                                                                 fetch_subscriber,
                                                                                 0,
                                                                                 MP,
                                                                                 ClientId]),
    ok.

stale_subscribe_test(_) ->
    MP = "",
    ClientId = "CID-123",
    NodeName = <<"Node-A">>,
    CS = false,
    TS = os:system_time(nanosecond),
    TopicWithQoS = [<<"topic1/123">>, <<"2">>],
    NumOfTopics = 1,

    remap_subscriber(MP, ClientId, NodeName, CS, TS),
    {error, <<"ERR stale_request">>} = eredis:q(whereis(redis_client), ["FCALL",
                                                                        subscribe,
                                                                        0,
                                                                        MP,
                                                                        ClientId,
                                                                        NodeName,
                                                                        TS - 10000,
                                                                        NumOfTopics | TopicWithQoS]),
    ok.

subscribe_from_different_node_test(_) ->
    MP = "",
    ClientId = "CID-123",
    NodeName1 = <<"Node-A">>,
    NodeName2 = <<"Node-B">>,
    CS = false,
    TopicWithQoS = [<<"topic1/123">>, <<"2">>],
    NumOfTopics = 1,

    remap_subscriber(MP, ClientId, NodeName1, CS, os:system_time(nanosecond)),
    {error, <<"ERR unauthorized">>} = eredis:q(whereis(redis_client), ["FCALL",
                                                                       subscribe,
                                                                       0,
                                                                       MP,
                                                                       ClientId,
                                                                       NodeName2,
                                                                       os:system_time(nanosecond),
                                                                       NumOfTopics | TopicWithQoS]),
    ok.

new_topics_subscribe_test(_) ->
    MP = "",
    NodeName = <<"Node-A">>,
    ClientId = "CID-1",
    CS = true,
    TopicWithQoS1 = [<<"topic1/123">>, <<"2">>],
    TopicWithQoS2 = [<<"topic3/123">>, <<"1">>],
    TopicWithQoS3 = [<<"topic2/123">>, <<"1">>],
    TopicWithQoS4 = [<<"topic2/123">>, <<"0">>],

    remap_subscriber(MP, ClientId, NodeName, CS, os:system_time(nanosecond)),
    {ok, [NodeName, <<"1">>, []]} = eredis:q(whereis(redis_client), ["FCALL",
                                                                     subscribe,
                                                                     0,
                                                                     MP,
                                                                     ClientId,
                                                                     NodeName,
                                                                     os:system_time(nanosecond),
                                                                     1 | TopicWithQoS1]),
    {ok, [NodeName, <<"1">>, [TopicWithQoS1]]} = eredis:q(whereis(redis_client), ["FCALL",
                                                                                  subscribe,
                                                                                  0,
                                                                                  MP,
                                                                                  ClientId,
                                                                                  NodeName,
                                                                                  os:system_time(nanosecond),
                                                                                  0]),
    {ok, [NodeName, <<"1">>, [TopicWithQoS1]]} = eredis:q(whereis(redis_client), ["FCALL",
                                                                                  subscribe,
                                                                                  0,
                                                                                  MP,
                                                                                  ClientId,
                                                                                  NodeName,
                                                                                  os:system_time(nanosecond),
                                                                                  1 | TopicWithQoS2]),
    {ok,
     [NodeName, <<"1">>, [TopicWithQoS1, TopicWithQoS2]]} = eredis:q(whereis(redis_client), ["FCALL",
                                                                                             subscribe,
                                                                                             0,
                                                                                             MP,
                                                                                             ClientId,
                                                                                             NodeName,
                                                                                             os:system_time(nanosecond),
                                                                                             1 | TopicWithQoS3]),
    {ok,
     [NodeName,
      <<"1">>,
      [TopicWithQoS1, TopicWithQoS3, TopicWithQoS2]]} = eredis:q(whereis(redis_client), ["FCALL",
                                                                                         subscribe,
                                                                                         0,
                                                                                         MP,
                                                                                         ClientId,
                                                                                         NodeName,
                                                                                         os:system_time(nanosecond),
                                                                                         1 | TopicWithQoS4]),
    ok.

new_shared_topics_subscribe_test(_) ->
    MP = "",
    NodeName = <<"Node-A">>,
    ClientId = "CID-1",
    CS = true,
    Group = <<"group1">>,
    TopicWithQoS1 = [<<"$share/", Group/binary, "/topic1/123">>, <<"2">>],
    TopicWithQoS2 = [<<"$share/", Group/binary, "/topic3/123">>, <<"1">>],
    TopicWithQoS3 = [<<"$share/", Group/binary, "/topic2/123">>, <<"1">>],
    TopicWithQoS4 = [<<"$share/", Group/binary, "/topic2/123">>, <<"0">>],

    remap_subscriber(MP, ClientId, NodeName, CS, os:system_time(nanosecond)),
    {ok, [NodeName, <<"1">>, []]} = eredis:q(whereis(redis_client), ["FCALL",
                                                                     subscribe,
                                                                     0,
                                                                     MP,
                                                                     ClientId,
                                                                     NodeName,
                                                                     os:system_time(nanosecond),
                                                                     1 | TopicWithQoS1]),
    {ok, [NodeName, <<"1">>, [TopicWithQoS1]]} = eredis:q(whereis(redis_client), ["FCALL",
                                                                                  subscribe,
                                                                                  0,
                                                                                  MP,
                                                                                  ClientId,
                                                                                  NodeName,
                                                                                  os:system_time(nanosecond),
                                                                                  0]),
    {ok, [NodeName, <<"1">>, [TopicWithQoS1]]} = eredis:q(whereis(redis_client), ["FCALL",
                                                                                  subscribe,
                                                                                  0,
                                                                                  MP,
                                                                                  ClientId,
                                                                                  NodeName,
                                                                                  os:system_time(nanosecond),
                                                                                  1 | TopicWithQoS2]),
    {ok,
     [NodeName, <<"1">>, [TopicWithQoS1, TopicWithQoS2]]} = eredis:q(whereis(redis_client), ["FCALL",
                                                                                             subscribe,
                                                                                             0,
                                                                                             MP,
                                                                                             ClientId,
                                                                                             NodeName,
                                                                                             os:system_time(nanosecond),
                                                                                             1 | TopicWithQoS3]),
    {ok,
     [NodeName,
      <<"1">>,
      [TopicWithQoS1, TopicWithQoS3, TopicWithQoS2]]} = eredis:q(whereis(redis_client), ["FCALL",
                                                                                         subscribe,
                                                                                         0,
                                                                                         MP,
                                                                                         ClientId,
                                                                                         NodeName,
                                                                                         os:system_time(nanosecond),
                                                                                         1 | TopicWithQoS4]),
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
