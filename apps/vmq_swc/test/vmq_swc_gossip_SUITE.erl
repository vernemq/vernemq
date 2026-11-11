-module(vmq_swc_gossip_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

%% ===================================================================
%% common_test callbacks
%% ===================================================================

suite() ->
    [{timetrap, {minutes, 5}}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    %% Use very short intervals so tests run quickly
    application:load(vmq_swc),
    application:set_env(vmq_swc, gossip_interval, 500),
    application:set_env(vmq_swc, fast_gossip_interval, 50),
    application:set_env(vmq_swc, fast_gossip_duration, 300),
    %% Initialize the peer service manager ETS table with an empty ORSWOT
    %% so gossip can read local state without crashing.
    setup_peer_service_state(),
    {ok, Pid} = vmq_swc_peer_service_gossip:start_link(),
    [{gossip_pid, Pid} | Config].

end_per_testcase(_Case, _Config) ->
    catch vmq_swc_peer_service_gossip:stop(),
    cleanup_peer_service_state(),
    application:unset_env(vmq_swc, gossip_interval),
    application:unset_env(vmq_swc, fast_gossip_interval),
    application:unset_env(vmq_swc, fast_gossip_duration),
    ok.

all() ->
    [
        fast_mode_entry_on_membership_change_test,
        fast_mode_exit_after_duration_test,
        timer_duration_changes_in_fast_mode_test,
        gossip_all_peers_in_fast_mode_test
    ].

%% ===================================================================
%% Tests
%% ===================================================================

fast_mode_entry_on_membership_change_test(_Config) ->
    %% Verify that notify_membership_change triggers fast mode
    State0 = sys:get_state(vmq_swc_peer_service_gossip),
    ?assertEqual(normal, element(2, State0)),

    vmq_swc_peer_service_gossip:notify_membership_change(),
    %% Give the cast time to be processed
    timer:sleep(50),

    State1 = sys:get_state(vmq_swc_peer_service_gossip),
    ?assertEqual(fast, element(2, State1)),
    %% fast_deadline should be non-zero (monotonic_time can be negative)
    Deadline = element(4, State1),
    ?assert(Deadline =/= 0),
    ok.

fast_mode_exit_after_duration_test(_Config) ->
    %% Enter fast mode
    vmq_swc_peer_service_gossip:notify_membership_change(),
    timer:sleep(50),

    State1 = sys:get_state(vmq_swc_peer_service_gossip),
    ?assertEqual(fast, element(2, State1)),

    %% Wait for the fast gossip duration to expire (300ms + some margin).
    %% The fast_gossip_interval is 50ms, so the gossip timer will fire
    %% several times. After 300ms the deadline expires and mode returns to normal.
    timer:sleep(500),

    State2 = sys:get_state(vmq_swc_peer_service_gossip),
    ?assertEqual(normal, element(2, State2)),
    %% fast_deadline should be reset to 0
    ?assertEqual(0, element(4, State2)),
    ok.

timer_duration_changes_in_fast_mode_test(_Config) ->
    %% In normal mode, the timer interval is gossip_interval (500ms).
    %% In fast mode, it should be fast_gossip_interval (50ms).
    %% We verify by checking that the timer_ref changes when entering fast mode.

    %% Capture the timer ref in normal mode
    State0 = sys:get_state(vmq_swc_peer_service_gossip),
    TimerRef0 = element(3, State0),
    ?assert(is_reference(TimerRef0)),

    %% Enter fast mode
    vmq_swc_peer_service_gossip:notify_membership_change(),
    timer:sleep(10),

    State1 = sys:get_state(vmq_swc_peer_service_gossip),
    TimerRef1 = element(3, State1),
    ?assertEqual(fast, element(2, State1)),
    %% Timer ref should have changed (old one cancelled, new one created)
    ?assertNotEqual(TimerRef0, TimerRef1),

    %% Wait for one fast gossip cycle and check the timer ref changed again
    timer:sleep(100),
    State2 = sys:get_state(vmq_swc_peer_service_gossip),
    TimerRef2 = element(3, State2),
    ?assertNotEqual(TimerRef1, TimerRef2),
    ok.

gossip_all_peers_in_fast_mode_test(_Config) ->
    %% Verify that re-entering fast mode extends the deadline.
    %% In fast mode, do_gossip_all is called (sends to all peers).
    %% In normal mode, do_gossip sends to a single random peer.
    %% With a singleton cluster (no peers), gossip returns {error, singleton}.

    %% Start in normal mode
    State0 = sys:get_state(vmq_swc_peer_service_gossip),
    ?assertEqual(normal, element(2, State0)),

    %% Enter fast mode via membership change notification
    vmq_swc_peer_service_gossip:notify_membership_change(),
    timer:sleep(50),

    State1 = sys:get_state(vmq_swc_peer_service_gossip),
    ?assertEqual(fast, element(2, State1)),
    Deadline1 = element(4, State1),

    %% Wait a bit and re-trigger fast mode -- deadline should be pushed forward
    timer:sleep(100),
    vmq_swc_peer_service_gossip:notify_membership_change(),
    timer:sleep(50),

    State2 = sys:get_state(vmq_swc_peer_service_gossip),
    Deadline2 = element(4, State2),
    ?assert(Deadline2 >= Deadline1),
    ?assertEqual(fast, element(2, State2)),
    ok.

%% ===================================================================
%% Setup helpers for the peer service manager ETS table
%% ===================================================================

setup_peer_service_state() ->
    %% Create the ETS table that vmq_swc_peer_service_manager:get_local_state/0 reads.
    case ets:info(swc_cluster_state) of
        undefined ->
            ets:new(swc_cluster_state, [named_table, public, set, {keypos, 1}]);
        _ ->
            ok
    end,
    %% Insert an empty ORSWOT as the cluster state
    ets:insert(swc_cluster_state, {cluster_state, riak_dt_orswot:new()}),
    ok.

cleanup_peer_service_state() ->
    catch ets:delete(swc_cluster_state),
    ok.
