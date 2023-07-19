-module(vmq_delete_subscriber_lua_SUITE).

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
    vmq_test_utils:setup(),
    Config.

end_per_testcase(_, Config) ->
    vmq_test_utils:teardown(),
    Config.

all() ->
    [
     delete_without_remap_test,
     stale_delete_test,
     delete_from_different_node_test,
     delete_subscriber_test
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

delete_without_remap_test(_) ->
    MP = "",
    ClientId = "CID-123",
    NodeName = <<"Node-A">>,

    {ok, <<"1">>} = eredis:q(whereis(vmq_redis_client), ["FCALL",
                                                     delete_subscriber,
                                                     0,
                                                     MP,
                                                     ClientId,
                                                     NodeName,
                                                     os:system_time(nanosecond)]),
    ok.

stale_delete_test(_) ->
    MP = "",
    ClientId = "CID-123",
    NodeName = <<"Node-A">>,
    CS = false,
    TS = os:system_time(nanosecond),

    remap_subscriber(MP, ClientId, NodeName, CS, TS),
    {error, <<"ERR stale_request">>} = eredis:q(whereis(vmq_redis_client), ["FCALL",
                                                                        delete_subscriber,
                                                                        0,
                                                                        MP,
                                                                        ClientId,
                                                                        NodeName,
                                                                        TS - 10000]),
    ok.

delete_from_different_node_test(_) ->
    MP = "",
    ClientId = "CID-123",
    NodeName1 = <<"Node-A">>,
    NodeName2 = <<"Node-B">>,
    CS = false,

    remap_subscriber(MP, ClientId, NodeName1, CS, os:system_time(nanosecond)),
    {error,<<"ERR unauthorized">>} = eredis:q(whereis(vmq_redis_client), ["FCALL",
                                                                      delete_subscriber,
                                                                      0,
                                                                      MP,
                                                                      ClientId,
                                                                      NodeName2,
                                                                      os:system_time(nanosecond)]),
    ok.


delete_subscriber_test(_) ->
    MP = "",
    NodeName = <<"Node-A">>,
    ClientId = "CID-1",
    CS = true,
    Group = <<"group1">>,
    TopicWithQoS1 = [<<"topic1/123">>, <<"2">>],
    TopicWithQoS2 = [<<"$share/", Group/binary, "/topic1/123">>, <<"1">>],
    TopicsWithQoS = TopicWithQoS1 ++ TopicWithQoS2,

    remap_subscriber(MP, ClientId, NodeName, CS, os:system_time(nanosecond)),
    {ok,  [NodeName,<<"1">>,[]]} = eredis:q(whereis(vmq_redis_client), ["FCALL",
                                                                    subscribe,
                                                                    0,
                                                                    MP,
                                                                    ClientId,
                                                                    NodeName,
                                                                    os:system_time(nanosecond),
                                                                    2 | TopicsWithQoS]),
    {ok,  [NodeName, <<"1">>, [TopicWithQoS1, TopicWithQoS2]]} = eredis:q(whereis(vmq_redis_client), ["FCALL",
                                                                                                  fetch_subscriber,
                                                                                                  0,
                                                                                                  MP,
                                                                                                  ClientId]),
    {ok, <<"1">>} = eredis:q(whereis(vmq_redis_client), ["FCALL",
                                                     delete_subscriber,
                                                     0,
                                                     MP,
                                                     ClientId,
                                                     NodeName,
                                                     os:system_time(nanosecond)]),
    {ok,  []} = eredis:q(whereis(vmq_redis_client), ["FCALL",
                                                 fetch_subscriber,
                                                 0,
                                                 MP,
                                                 ClientId]),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Helper
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

remap_subscriber(MP, ClientId, NodeName, CS, TS) ->
    {ok, _} = eredis:q(whereis(vmq_redis_client), ["FCALL",
                                               remap_subscriber,
                                               0,
                                               MP,
                                               ClientId,
                                               NodeName,
                                               CS,
                                               TS]),
    ok.
