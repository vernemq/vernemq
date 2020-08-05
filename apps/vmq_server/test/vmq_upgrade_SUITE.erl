-module(vmq_upgrade_SUITE).
-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([v0_to_v1_subscriber_format_test/1]).

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(_Config) ->
    cover:start(),
    _Config.

end_per_suite(_Config) ->
    _Config.

init_per_testcase(_Case, Config) ->
    vmq_test_utils:setup(),
    Config.

end_per_testcase(_, Config) ->
    vmq_test_utils:teardown(),
    Config.

all() ->
    [v0_to_v1_subscriber_format_test].

v0_to_v1_subscriber_format_test(_) ->
    Topic = [<<"a">>,<<"b">>,<<"c">>],
    SubscriberId = {"", <<"test-client">>},
    V0Subs = [{Topic, 1, node()}],
    vmq_metadata:put({vmq, subscriber}, SubscriberId, V0Subs),
    {ok, _, _} = vmq_queue_sup_sup:start_queue(SubscriberId, false),
    true = wait_until_true(
      fun() ->
              %% this should setup a queue
              C1 = vmq_reg:get_queue_pid(SubscriberId) =/= not_found,
              %% return proper subscription
              V1Subs = [{node(), false, [{Topic, 1}]}],
              C2 = V1Subs == vmq_reg:subscriptions_for_subscriber_id(SubscriberId),
              %% routing index must be updated
              C3 = [{SubscriberId, 1}] == vmq_reg_view:fold(vmq_reg_trie, SubscriberId, Topic,
                                                            fun(V, _, Acc) -> [V|Acc] end, []),
              C1 and C2 and C3
      end,
      10).

wait_until_true(_, 0) -> false;
wait_until_true(Fun, N) ->
    case Fun() of
        true -> true;
        false ->
            timer:sleep(100),
            wait_until_true(Fun, N -1)
    end.
