-module(vmq_queue_SUITE).
-include("src/vmq_server.hrl").

-ifdef(nowarn_gen_fsm).
-compile([{nowarn_deprecated_function,
          [{gen_fsm,sync_send_all_state_event,2}]}]).
-endif.

-define(RECEIVE_TIMEOUT, 30000).

-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([queue_crash_test/1,
         queue_fifo_test/1,
         queue_lifo_test/1,
         queue_fifo_offline_drop_test/1,
         queue_lifo_offline_drop_test/1,
         queue_offline_transition_test/1,
         queue_offline_online_transition_test_std/1,
         queue_offline_online_transition_test_ignore_max/1,
         queue_offline_online_transition_test_ignore_max_lifo/1,
         queue_online_takeover_keeps_extended_queue_size_test/1,
         queue_persistent_client_expiration_test/1,
         queue_force_disconnect_test/1,
         queue_force_disconnect_cleanup_test/1,
         queue_wait_for_offline_change_state_test/1]).

-export([hook_auth_on_publish/6,
         hook_auth_on_subscribe/3,
         hook_on_message_drop/3]).

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(_Config) ->
    cover:start(),
    _Config.

end_per_suite(_Config) ->
    _Config.

init_per_testcase(_Case, Config) ->
    ok = ensure_vmq_server_loaded(),
    reset_queue_test_env(),
    vmq_test_utils:setup(),
    persistent_term:put({vmq_reg_trie, fanout_shard_count}, 1),
    vmq_config:set_env(queue_deliver_mode, fanout, false),
    enable_hooks(),
    Config.

end_per_testcase(_, Config) ->
    vmq_test_utils:teardown(),
    persistent_term:erase({vmq_reg_trie, fanout_shard_count}),
    Config.

all() ->
    [queue_crash_test,
     queue_fifo_test,
     queue_lifo_test,
     queue_fifo_offline_drop_test,
     queue_lifo_offline_drop_test,
     queue_offline_transition_test,
     queue_offline_online_transition_test_std,
     queue_offline_online_transition_test_ignore_max,
     queue_offline_online_transition_test_ignore_max_lifo,
     queue_online_takeover_keeps_extended_queue_size_test,
     queue_persistent_client_expiration_test,
     queue_force_disconnect_test,
     queue_force_disconnect_cleanup_test,
     queue_wait_for_offline_change_state_test
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
queue_crash_test(_) ->
    Parent = self(),
    {_, ClientId} = SubscriberId = {"", <<"mock-client">>},
    QueueOpts = maps:merge(#{cleanup_on_disconnect => false}, vmq_queue:default_opts()),
    SessionPid1 = spawn(fun() -> mock_session(Parent) end),

    {ok, #{session_present := false,
           queue_pid := QPid1}} = vmq_reg:register_subscriber_(SessionPid1, SubscriberId, false, QueueOpts, 10),
    {ok, [1]} = vmq_reg:subscribe(false, SubscriberId,
                                  [{[<<"test">>, <<"topic">>], 1}]),
    %% at this point we've a working subscription
    timer:sleep(20),
    Msg = msg([<<"test">>, <<"topic">>], <<"test-message">>, 1),
    {ok, {1, 0}} = vmq_reg:publish(true, vmq_reg_trie, ClientId, Msg),
    receive_msg(QPid1, 1, Msg),

    %% teardown session
    SessionPid1 ! go_down,
    timer:sleep(20),
    {offline, fanout, 0, 0, false} = vmq_queue:status(QPid1),

    %% fill the offline queue
    {ok, {1, 0}} = vmq_reg:publish(true, vmq_reg_trie, ClientId, Msg),
    {offline, fanout, 1, 0, false} = vmq_queue:status(QPid1),

    %% crash the queue
    catch gen_fsm:sync_send_all_state_event(QPid1, byebye),
    false = is_process_alive(QPid1),
    timer:sleep(20),
    NewQPid = vmq_reg:get_queue_pid(SubscriberId),
    {offline, fanout, 1, 0, false} = vmq_queue:status(NewQPid),

    %% reconnect
    SessionPid2 = spawn(fun() -> mock_session(Parent) end),
    {ok, #{session_present := true,
           queue_pid := NewQPid}} = vmq_reg:register_subscriber_(SessionPid2, SubscriberId, false, QueueOpts, 10),
    receive_persisted_msg(NewQPid, 1, Msg),
    {online, fanout, 0, 1, false} = vmq_queue:status(NewQPid),
    {ok, []} = vmq_message_store:find(SubscriberId, other).

queue_fifo_test(_) ->
    Parent = self(),
    SubscriberId = {"", <<"mock-fifo-client">>},
    QueueOpts = maps:merge(#{cleanup_on_disconnect => false}, vmq_queue:default_opts()),
    SessionPid1 = spawn(fun() -> mock_session(Parent) end),

    {ok, #{session_present := false,
           queue_pid := QPid}} = vmq_reg:register_subscriber_(SessionPid1, SubscriberId, false, QueueOpts, 10),
    {ok, [1]} = vmq_reg:subscribe(false, SubscriberId,
                           [{[<<"test">>, <<"fifo">>, <<"topic">>], 1}]),
    %% teardown session
    SessionPid1 ! go_down,
    timer:sleep(20),

    Msgs = publish_multi(SubscriberId, [<<"test">>, <<"fifo">>, <<"topic">>]),

    SessionPid2 = spawn(fun() -> mock_session(Parent) end),
    {ok, #{session_present := true,
           queue_pid := QPid}} = vmq_reg:register_subscriber_(SessionPid2, SubscriberId, false, QueueOpts, 10),

    ok = receive_multi(QPid, 1, Msgs),
    {ok, []} = vmq_message_store:find(SubscriberId, other).

queue_lifo_test(_) ->
    Parent = self(),
    SubscriberId = {"", <<"mock-lifo-client">>},
    QueueOpts = maps:merge(vmq_queue:default_opts(), #{cleanup_on_disconnect => false, queue_type => lifo}),
    SessionPid1 = spawn(fun() -> mock_session(Parent) end),

    {ok, #{session_present := false,
           queue_pid := QPid}} = vmq_reg:register_subscriber_(SessionPid1, SubscriberId, false, QueueOpts, 10),
    {ok, [1]} = vmq_reg:subscribe(false, SubscriberId, [{[<<"test">>, <<"lifo">>, <<"topic">>], 1}]),
    %% teardown session
    SessionPid1 ! go_down,
    timer:sleep(20),

    Msgs = publish_multi(SubscriberId, [<<"test">>, <<"lifo">>, <<"topic">>]),

    SessionPid2 = spawn(fun() -> mock_session(Parent) end),
    {ok, #{session_present := true,
           queue_pid := QPid}} = vmq_reg:register_subscriber_(SessionPid2, SubscriberId, false, QueueOpts, 10),

    ok = receive_multi(QPid, 1, lists:reverse(Msgs)), %% reverse list to get lifo
    {ok, []} = vmq_message_store:find(SubscriberId, other).

queue_fifo_offline_drop_test(_) ->
    Parent = self(),
    SubscriberId = {"", <<"mock-fifo-client">>},
    QueueOpts = maps:merge(vmq_queue:default_opts(), #{cleanup_on_disconnect => false,
                                                       max_offline_messages => 10}),
    SessionPid1 = spawn(fun() -> mock_session(Parent) end),

    {ok, #{session_present := false,
           queue_pid := QPid}} = vmq_reg:register_subscriber_(SessionPid1, SubscriberId, false, QueueOpts, 10),
    {ok, [1]} = vmq_reg:subscribe(false, SubscriberId, [{[<<"test">>, <<"fifo">>, <<"topic">>], 1}]),
    %% teardown session
    SessionPid1 ! go_down,
    timer:sleep(20),

    Msgs = publish_multi(SubscriberId, [<<"test">>, <<"fifo">>, <<"topic">>]), % publish 100, only the first 10 are kept
    {offline, fanout, 10, 0, false} = vmq_queue:status(QPid),

    SessionPid2 = spawn(fun() -> mock_session(Parent) end),
    {ok, #{session_present := true,
           queue_pid := QPid}} = vmq_reg:register_subscriber_(SessionPid2, SubscriberId, false, QueueOpts, 10),
    {KeptMsgs, _} = lists:split(10, Msgs),
    ok = receive_multi(QPid, 1, KeptMsgs),
    {ok, []} = vmq_message_store:find(SubscriberId, other).


queue_lifo_offline_drop_test(_) ->
    Parent = self(),
    SubscriberId = {"", <<"mock-lifo-client">>},
    QueueOpts = maps:merge(vmq_queue:default_opts(), #{cleanup_on_disconnect => false,
                                                       max_offline_messages => 10,
                                                       queue_type => lifo}),
    SessionPid1 = spawn(fun() -> mock_session(Parent) end),

    {ok, #{session_present := false,
           queue_pid := QPid}} = vmq_reg:register_subscriber_(SessionPid1, SubscriberId, false, QueueOpts, 10),
    {ok, [1]} = vmq_reg:subscribe(false, SubscriberId,
                           [{[<<"test">>, <<"lifo">>, <<"topic">>], 1}]),
    %% teardown session
    SessionPid1 ! go_down,
    timer:sleep(20),

    Msgs = publish_multi(SubscriberId, [<<"test">>, <<"lifo">>, <<"topic">>]), % publish 100, only the first 10 are kept
    {offline, fanout, 10, 0, false} = vmq_queue:status(QPid),

    SessionPid2 = spawn(fun() -> mock_session(Parent) end),
    {ok, #{session_present := true,
           queue_pid := QPid}} = vmq_reg:register_subscriber_(SessionPid2, SubscriberId, false, QueueOpts, 10),
    {KeptMsgs, _} = lists:split(10, lists:reverse(Msgs)),
    ok = receive_multi(QPid, 1, KeptMsgs),
    {ok, []} = vmq_message_store:find(SubscriberId, other).


queue_offline_transition_test(_) ->
    Parent = self(),
    SubscriberId = {"", <<"mock-trans-client">>},
    QueueOpts = maps:merge(vmq_queue:default_opts(), #{cleanup_on_disconnect => false,
                                                       max_offline_messages => 1000,
                                                       queue_type => fifo}),
    SessionPid1 = spawn(fun() -> mock_session(Parent) end),
    {ok, #{session_present := false,
           queue_pid := QPid}} = vmq_reg:register_subscriber_(SessionPid1, SubscriberId, false, QueueOpts, 10),
    {ok, [1]} = vmq_reg:subscribe(false, SubscriberId, [{[<<"test">>, <<"transition">>], 1}]),
    timer:sleep(20), % give some time to plumtree

    %% teardown session
    catch vmq_queue:set_last_waiting_acks(QPid, []), % simulate what real session does
    teardown_session(SessionPid1),
    Msgs = publish_multi(SubscriberId, [<<"test">>, <<"transition">>]), % publish 100

    SessionPid2 = spawn(fun() -> mock_session(Parent) end),
    {ok, #{session_present := true,
           queue_pid := QPid}} = vmq_reg:register_subscriber_(SessionPid2, SubscriberId, false, QueueOpts, 10),
    ok = receive_multi(QPid, 1, Msgs),
    {ok, []} = vmq_message_store:find(SubscriberId, other).

queue_offline_online_transition_test_std(_) ->
    Parent = self(),
    SubscriberId = {"", <<"mock-trans-client">>},
    application:set_env(vmq_server, override_max_online_messages, false),
    QueueOpts = maps:merge(vmq_queue:default_opts(), #{cleanup_on_disconnect => false,
                                                        max_offline_messages => 100,
                                                        max_online_messages => 10,
                                                        queue_type => fifo}),
    SessionPid1 = spawn(fun() -> mock_session(Parent) end),
    {ok, #{session_present := false,
            queue_pid := QPid}} = vmq_reg:register_subscriber_(SessionPid1, SubscriberId, false, QueueOpts, 10),
    {ok, [1]} = vmq_reg:subscribe(false, SubscriberId, [{[<<"test">>, <<"transition">>], 1}]),
    timer:sleep(20), % give some time to plumtree

    %% teardown session
    catch vmq_queue:set_last_waiting_acks(QPid, []), % simulate what real session does
    teardown_session(SessionPid1),
    Msgs = publish_multi(SubscriberId, [<<"test">>, <<"transition">>]), % publish 100

    SessionPid2 = spawn(fun() -> mock_session(Parent) end),
    {ok, #{session_present := true,
            queue_pid := QPid}} = vmq_reg:register_subscriber_(SessionPid2, SubscriberId, false, QueueOpts, 10),
    {KeptMsgs, _} = lists:split(10, Msgs),
    ok = receive_multi(QPid, 1, KeptMsgs),
    {ok, []} = vmq_message_store:find(SubscriberId, other).

queue_offline_online_transition_test_ignore_max(_) ->
    Parent = self(),
    SubscriberId = {"", <<"mock-trans-client">>},
    application:set_env(vmq_server, override_max_online_messages, true),
    QueueOpts = maps:merge(vmq_queue:default_opts(), #{cleanup_on_disconnect => false,
                                                       max_offline_messages => 100,
                                                       max_online_messages => 10,
                                                       queue_type => fifo}),
    SessionPid1 = spawn(fun() -> mock_session(Parent) end),
    {ok, #{session_present := false,
           queue_pid := QPid}} = vmq_reg:register_subscriber_(SessionPid1, SubscriberId, false, QueueOpts, 10),
    {ok, [1]} = vmq_reg:subscribe(false, SubscriberId, [{[<<"test">>, <<"transition">>], 1}]),
    timer:sleep(20), % give some time to plumtree

    %% teardown session
    catch vmq_queue:set_last_waiting_acks(QPid, []), % simulate what real session does
    teardown_session(SessionPid1),
    Msgs = publish_multi(SubscriberId, [<<"test">>, <<"transition">>]), % publish 100

    SessionPid2 = spawn(fun() -> mock_session(Parent) end),
    {ok, #{session_present := true,
           queue_pid := QPid}} = vmq_reg:register_subscriber_(SessionPid2, SubscriberId, false, QueueOpts, 10),
    ok = receive_multi(QPid, 1, Msgs),
    {ok, []} = vmq_message_store:find(SubscriberId, other).

queue_offline_online_transition_test_ignore_max_lifo(_) ->
    Parent = self(),
    SubscriberId = {"", <<"mock-trans-client">>},
    application:set_env(vmq_server, override_max_online_messages, true),
    QueueOpts = maps:merge(vmq_queue:default_opts(), #{cleanup_on_disconnect => false,
                                                        max_offline_messages => 100,
                                                        max_online_messages => 10,
                                                        queue_type => lifo}),
    SessionPid1 = spawn(fun() -> mock_session(Parent) end),
    {ok, #{session_present := false,
            queue_pid := QPid}} = vmq_reg:register_subscriber_(SessionPid1, SubscriberId, false, QueueOpts, 10),
    {ok, [1]} = vmq_reg:subscribe(false, SubscriberId, [{[<<"test">>, <<"transition">>], 1}]),
    timer:sleep(20), % give some time to plumtree

    %% teardown session
    catch vmq_queue:set_last_waiting_acks(QPid, []), % simulate what real session does
    teardown_session(SessionPid1),
    Msgs = publish_multi(SubscriberId, [<<"test">>, <<"transition">>]), % publish 100

    SessionPid2 = spawn(fun() -> mock_session(Parent) end),
    {ok, #{session_present := true,
            queue_pid := QPid}} = vmq_reg:register_subscriber_(SessionPid2, SubscriberId, false, QueueOpts, 10),
    ok = receive_multi(QPid, 1, lists:reverse(Msgs)),
    {ok, []} = vmq_message_store:find(SubscriberId, other).

queue_online_takeover_keeps_extended_queue_size_test(_) ->
    Parent = self(),
    SubscriberId = {"", <<"mock-online-takeover-client">>},
    application:set_env(vmq_server, override_max_online_messages, true),
    QueueOpts = maps:merge(vmq_queue:default_opts(), #{
        cleanup_on_disconnect => false,
        max_online_messages => 10,
        max_offline_messages => 100,
        queue_type => fifo,
        queue_to_session_batch_size => 100
    }),
    {ok, false, QPid} = vmq_queue_sup_sup:start_queue(SubscriberId),

    Msgs = [
        #deliver{qos = 1, msg = msg([<<"test">>, <<"takeover">>], payload(I), 1)}
     || I <- lists:seq(1, 100)
    ],
    ok = vmq_queue:enqueue_many(QPid, Msgs),
    {offline, fanout, 100, 0, false} = vmq_queue:status(QPid),

    SessionPid1 = spawn(fun() -> passive_session(Parent) end),
    {ok, _} = vmq_queue:add_session(QPid, SessionPid1, QueueOpts),
    {online, fanout, 0, 1, false} = vmq_queue:status(QPid),

    SessionPid2 = spawn(fun() -> passive_session(Parent) end),
    AddSessionPid = spawn(fun() ->
        Parent ! {add_session, vmq_queue:add_session(QPid, SessionPid2, QueueOpts)}
    end),
    receive
        {passive_received, SessionPid1, QPid, 100} -> ok
    after 1000 ->
        exit(waiting_for_first_session_messages)
    end,
    SessionPid1 ! go_down,
    receive
        {add_session, {ok, _}} -> ok
    after 1000 ->
        exit(waiting_for_add_session)
    end,
    false = is_process_alive(AddSessionPid),

    %% The takeover path should behave like offline wakeup and keep the extended online queue size.
    receive
        {passive_received, SessionPid2, QPid, 100} -> ok
    after 1000 ->
        exit(waiting_for_second_session_messages)
    end,
    SessionPid2 ! go_down.

queue_persistent_client_expiration_test(_) ->
    Parent = self(),
    SubscriberId = {"", <<"persistent-client-expiration">>},
    %% Set the persistent client to expire after 15 seconds
    application:set_env(vmq_server, persistent_client_expiration, 2),
    QueueOpts = maps:merge(vmq_queue:default_opts(), #{cleanup_on_disconnect => false,
                                                       max_offline_messages => 1000,
                                                       queue_type => fifo}),
    SessionPid1 = spawn(fun() -> mock_session(Parent) end),
    {ok, #{session_present := false,
           queue_pid := QPid}} = vmq_reg:register_subscriber_(SessionPid1, SubscriberId, false, QueueOpts, 10),
    {ok, [1]} = vmq_reg:subscribe(false, SubscriberId, [{[<<"test">>, <<"transition">>], 1}]),
    timer:sleep(200), % give some time to plumtree

    %% teardown session
    catch vmq_queue:set_last_waiting_acks(QPid, []), % simulate what real session does
    SessionPid1 ! {go_down_in, 1},
    Msgs = publish_multi(SubscriberId, [<<"test">>, <<"transition">>]),
    NumPubbedMsgs = length(Msgs),

    timer:sleep(200), % give some time to plumtree
    {ok, FoundMsgs} = vmq_message_store:find(SubscriberId, other),
    NumPubbedMsgs = length(FoundMsgs),

    %% let's wait for the persistent-client-expiration to kick in
    timer:sleep(3000),

    not_found = vmq_queue_sup_sup:get_queue_pid(SubscriberId),
    {ok, []} = vmq_message_store:find(SubscriberId, other).

queue_force_disconnect_test(_) ->
    Parent = self(),
    SubscriberId = {"", <<"force-client-disconnect">>},
    QueueOpts = maps:merge(vmq_queue:default_opts(), #{cleanup_on_disconnect => false,
                                                       max_offline_messages => 1000,
                                                       queue_type => fifo}),
    SessionPid1 = spawn(fun() -> mock_session(Parent) end),
    {ok, #{session_present := false,
           queue_pid := QPid0}} = vmq_reg:register_subscriber_(SessionPid1, SubscriberId, false, QueueOpts, 10),
    {ok, [1]} = vmq_reg:subscribe(false, SubscriberId, [{[<<"test">>, <<"disconnect">>], 1}]),
    timer:sleep(200), % give some time to plumtree

    monitor(process, SessionPid1),
    vmq_queue:force_disconnect(QPid0, ?ADMINISTRATIVE_ACTION),

    % ensure we got disconnected
    receive
        {'DOWN', _MRef, process, SessionPid1, _} -> ok
    end,

    % Reconnect and ensure SessionPresent, and same QueuePid
    {ok, #{session_present := true,
           queue_pid := QPid0}} = vmq_reg:register_subscriber_(Parent, SubscriberId, false, QueueOpts, 10).


queue_force_disconnect_cleanup_test(_) ->
    NonConsumingSessionPid = self(),
    SubscriberId = {"", <<"force-client-discleanup">>},
    QueueOpts = maps:merge(vmq_queue:default_opts(), #{cleanup_on_disconnect => false,
                                                       max_offline_messages => 1000,
                                                       queue_type => fifo}),
    SessionPresent = false,
    {ok, #{session_present := SessionPresent,
           queue_pid := QPid0}} = vmq_reg:register_subscriber_(NonConsumingSessionPid, SubscriberId, false, QueueOpts, 10),
    {ok, [1]} = vmq_reg:subscribe(false, SubscriberId, [{[<<"test">>, <<"discleanup">>], 1}]),
    timer:sleep(200), % give some time to plumtree

    Msgs = publish_multi(SubscriberId, [<<"test">>, <<"discleanup">>]),
    NumPubbedMsgs = length(Msgs),

    timer:sleep(200), % give some time to plumtree
    {ok, FoundMsgs} = vmq_message_store:find(SubscriberId, other),
    NumPubbedMsgs = length(FoundMsgs),

    vmq_queue:force_disconnect(QPid0, ?ADMINISTRATIVE_ACTION, true),

    % Ensure all Subscriptions are gone
    [] = vmq_reg:subscriptions_for_subscriber_id(SubscriberId),

    % SessionPresent should be again `false` and we should get a new Queue Pid
    {ok, #{session_present := SessionPresent,
           queue_pid := QPid1}} = vmq_reg:register_subscriber_(NonConsumingSessionPid, SubscriberId, false, QueueOpts, 10),
    true = (QPid0 =/= QPid1),
    false = is_process_alive(QPid0),

    {ok, []} = vmq_message_store:find(SubscriberId, other).

%% Regression test for the wait_for_offline session-takeover wedge (#571, #1369):
%% if the draining old session reports a state change (change_state via active/1)
%% instead of going 'DOWN', the takeover must still complete. Pre-fix the queue
%% wedges and add_session never returns (this test times out); post-fix it completes.
queue_wait_for_offline_change_state_test(_) ->
    Parent = self(),
    SubscriberId = {"", <<"takeover-client">>},
    QueueOpts = maps:merge(vmq_queue:default_opts(),
                           #{cleanup_on_disconnect => false,
                             max_offline_messages => 1000,
                             queue_type => fifo}),

    %% Old session: re-activates once on the takeover disconnect, then dies.
    OldSessionPid = spawn(fun() -> reactivating_session(Parent, undefined, 1) end),
    {ok, #{session_present := false,
           queue_pid := QPid0}} =
        vmq_reg:register_subscriber_(OldSessionPid, SubscriberId, false, QueueOpts, 10),
    %% Hand it the queue pid (arrives before the takeover disconnect; FIFO mailbox).
    OldSessionPid ! {queue_pid, QPid0},
    {online, _, _, _, _} = vmq_queue:status(QPid0),

    %% Take over from a separate process (add_session blocks until the old is gone).
    NewSessionPid = spawn(fun() -> mock_session(Parent) end),
    TakeoverRef = make_ref(),
    Tester = self(),
    _ = spawn(fun() ->
                  R = vmq_reg:register_subscriber_(
                        NewSessionPid, SubscriberId, false, QueueOpts, 10),
                  Tester ! {TakeoverRef, R}
              end),

    %% The takeover must complete (pre-fix this timed out: queue wedged).
    receive
        {TakeoverRef, {ok, #{session_present := true, queue_pid := QPid0}}} ->
            ok;
        {TakeoverRef, Other} ->
            exit({unexpected_takeover_result, Other})
    after 5000 ->
        exit(takeover_wedged_in_wait_for_offline)
    end,

    %% Queue online with the new session.
    {online, _, _, _, _} = vmq_queue:status(QPid0).

publish_multi({_, ClientId}, Topic) ->
    publish_multi(ClientId, Topic, []).

ensure_vmq_server_loaded() ->
    case application:load(vmq_server) of
        ok -> ok;
        {error, {already_loaded, vmq_server}} -> ok
    end.

reset_queue_test_env() ->
    application:set_env(vmq_server, fanout_shard_count, 1),
    application:set_env(vmq_server, fanout_async_handoff, false),
    application:set_env(vmq_server, override_max_online_messages, false),
    application:set_env(vmq_server, persistent_client_expiration, 0),
    application:set_env(vmq_server, max_online_messages, 30000),
    application:set_env(vmq_server, max_offline_messages, -1),
    application:set_env(vmq_server, queue_deliver_mode, fanout),
    application:set_env(vmq_server, queue_type, fifo),
    application:set_env(vmq_server, max_drain_time, 100),
    application:set_env(vmq_server, max_msgs_per_drain_step, 10).

publish_multi(ClientId, Topic, Acc) when length(Acc) < 100 ->
    Msg = msg(Topic, list_to_binary("test-message-"++ integer_to_list(length(Acc))), 1),
    {ok, {1, 0}} = vmq_reg:publish(true, vmq_reg_trie, ClientId, Msg),
    publish_multi(ClientId, Topic, [Msg|Acc]);
publish_multi(_, _, Acc) -> lists:reverse(Acc).

receive_multi(QPid, QoS, Msgs) ->
    PMsgs = [#deliver{qos=QoS, msg=Msg#vmq_msg{persisted=true, qos=1}} || Msg <- Msgs],
    receive_multi(QPid, PMsgs).

receive_multi(_, []) -> ok;
receive_multi(QPid, Msgs) ->
    receive
        {received, QPid, RecMsgs} ->
            case lists:split(length(RecMsgs), Msgs) of
                {RecMsgs, RestMsgs} ->
                    receive_multi(QPid, RestMsgs);
                _ ->
                    exit({wrong_messages, {RecMsgs, Msgs}})
            end;
        M ->
            exit({wrong_message, M})
    after 60000 ->
            exit({receive_multi_timeout, QPid, Msgs, vmq_queue:status(QPid)})
    end.

teardown_session(SessionPid) ->
    MRef = monitor(process, SessionPid),
    SessionPid ! {go_down_in, 1},
    receive
        {'DOWN', MRef, process, SessionPid, _} ->
            ok
    after 5000 ->
            demonitor(MRef, [flush]),
            exit({session_teardown_timeout, SessionPid})
    end.

mock_session(Parent) ->
    receive
        {to_session_fsm, {mail, QPid, new_data}} ->
            vmq_queue:active(QPid),
            mock_session(Parent);
        {to_session_fsm, {mail, QPid, Msgs, _, _}} ->
            vmq_queue:notify(QPid),
            timer:sleep(100),
            Parent ! {received, QPid, Msgs},
            mock_session(Parent);
        {go_down_in, Ms} ->
            timer:sleep(Ms);
        _ -> % go down
            ok
    end.

passive_session(Parent) ->
    receive
        {to_session_fsm, {mail, QPid, new_data}} ->
            vmq_queue:active(QPid),
            passive_session(Parent);
        {to_session_fsm, {mail, QPid, Msgs, _, _}} ->
            Parent ! {passive_received, self(), QPid, length(Msgs)},
            passive_session(Parent);
        go_down ->
            ok;
        _ ->
            passive_session(Parent)
    end.

payload(I) ->
    list_to_binary("test-message-" ++ integer_to_list(I)).

%% Mock session that, on the takeover disconnect, re-activates the queue
%% (vmq_queue:active/1) instead of dying -- Reactivations times -- then goes down.
reactivating_session(Parent, QPid, Reactivations) ->
    receive
        {queue_pid, NewQPid} ->
            reactivating_session(Parent, NewQPid, Reactivations);
        {to_session_fsm, {mail, MailQPid, new_data}} ->
            vmq_queue:active(MailQPid),
            reactivating_session(Parent, QPid, Reactivations);
        {to_session_fsm, {mail, MailQPid, Msgs, _, _}} ->
            vmq_queue:notify(MailQPid),
            Parent ! {received, MailQPid, Msgs},
            reactivating_session(Parent, QPid, Reactivations);
        {to_session_fsm, {disconnect, _Reason}} when Reactivations > 0, is_pid(QPid) ->
            %% re-activate instead of dying: casts {change_state, active, self()}
            vmq_queue:active(QPid),
            reactivating_session(Parent, QPid, Reactivations - 1);
        _ -> % any other message (incl. the final disconnect) -> go down
            ok
    end.

msg(Topic, Payload, QoS) ->
    #vmq_msg{msg_ref=vmq_mqtt_fsm_util:msg_ref(),
             mountpoint="",
             routing_key=Topic,
             payload=Payload,
             qos=QoS,
             properties=#{}}.

receive_msg(QPid, QoS, Msg) ->
    %% if we were able to persist the message
    %% we'll set the persist flag
    PMsg = Msg#vmq_msg{persisted=true},
    receive
        {received, QPid, [#deliver{qos=QoS, msg=PMsg}]} ->
            ok;
        M ->
            exit({wrong_message, M})
    after ?RECEIVE_TIMEOUT ->
            exit({timeout, receive_msg, QPid, QoS, Msg})
    end.

receive_persisted_msg(QPid, QoS, Msg) ->
    %% if we were able to persist the message
    %% we'll set the persist flag,
    %% BUT we've also set the qos of the message
    %% to the one of the subscription
    PMsg = Msg#vmq_msg{persisted=true, qos=QoS},
    receive
        {received, QPid, [#deliver{qos=QoS, msg=PMsg}]} ->
            ok;
        M ->
            exit({wrong_message, M})
    after ?RECEIVE_TIMEOUT ->
            exit({timeout, receive_persisted_msg, QPid, QoS, Msg})
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
enable_hooks() ->
    vmq_plugin_mgr:enable_module_plugin(auth_on_publish, ?MODULE, hook_auth_on_publish, 6),
    vmq_plugin_mgr:enable_module_plugin(auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3),
    vmq_plugin_mgr:enable_module_plugin(on_message_drop, ?MODULE, hook_on_message_drop, 3).

hook_auth_on_publish(_, _, _, _, _, _) -> ok.
hook_auth_on_subscribe(_, _, _) -> ok.
hook_on_message_drop(_, _, queue_full) -> ok.
