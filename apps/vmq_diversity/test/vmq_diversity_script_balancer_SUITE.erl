-module(vmq_diversity_script_balancer_SUITE).
-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([load_balancing_test_simple/1]).

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(_Config) ->
    cover:start(),
    application:ensure_all_started(vmq_diversity),
    _Config.

end_per_suite(_Config) ->
    application:stop(vmq_diversity),
    _Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_, Config) ->
    Config.

all() ->
    [
     %% disabled as travis routinely fails this test for some reason.
     %%load_balancing_test_simple
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
load_balancing_test_simple(_) ->
    %% The test script balance.lua contains one hook that returns the global
    %% __SCRIPT_INSTANCE_ID . The script is configured to `keep_state=false`
    %% and `num_states=100`. The execution time of the script is constant
    %% therefore we can assume that the round-robin script balancer will
    %% hit every instance exactly once if the hook is called 100 times.
    %% To test that this is the case we sum up the returned
    %% __SCRIPT_INSTANCE_ID__ which should be equivalent to
    %% `lists:sum(lists:seq(1, 100))
    {ok, ScriptMgrPid} = vmq_diversity_script:start_link(code:lib_dir(vmq_diversity) ++ "/test/balance.lua"),
    %% Script has `keep_state=false & num_states=100`
    %%101 = length(supervisor:which_children(ScriptSupPid)),
    NumCalls = 10000,
    Self = self(),
    lists:foreach(
      fun(_) ->
              spawn(
                fun() ->
                        I = vmq_diversity_script:call_function(
                              ScriptMgrPid, balanced_function, []),
                        Self ! {id, I}
                end)
      end, lists:seq(1, NumCalls)),
    Hist =
    lists:foldl(
      fun(_, Hist) ->
              receive
                  {id, I} ->
                      N = maps:get(I, Hist, 0),
                      maps:put(I, N + 1, Hist)
              end
      end, #{}, lists:seq(1, NumCalls)),
    E = chi_square_err(NumCalls, Hist),
    io:format(user, "ChiSquare Error ~p~n", [E]),
    true = E < 13.

chi_square_err(NumSamples, Observed) ->
    %% Use ChiSquare to check uniformness of histogram
    NumBins = length(maps:keys(Observed)),
    ExpectedNumSamplesPerBin = (NumSamples / NumBins) -9 ,

    math:sqrt(lists:foldl(fun(I, Acc) ->
                        ObservedNumSamplesInBinI = maps:get(I, Observed),
                        (math:pow(ExpectedNumSamplesPerBin - ObservedNumSamplesInBinI, 2)
                         /
                        ExpectedNumSamplesPerBin) + Acc
                end, 0, maps:keys(Observed))).
