-module(vmq_queue_SUITE).
-include("src/vmq_server.hrl").
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
         queue_offline_transition_test/1]).

-export([hook_auth_on_publish/6,
         hook_auth_on_subscribe/3]).

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
    vmq_config:set_env(queue_deliver_mode, fanout, false),
    enable_hooks(),
    Config.

end_per_testcase(_, Config) ->
    vmq_test_utils:teardown(),
    Config.

all() ->
    [queue_crash_test,
     queue_fifo_test,
     queue_lifo_test,
     queue_fifo_offline_drop_test,
     queue_lifo_offline_drop_test,
     queue_offline_transition_test
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
queue_crash_test(_) ->
    Parent = self(),
    SubscriberId = {"", <<"mock-client">>},
    QueueOpts = maps:merge(#{clean_session => false}, vmq_queue:default_opts()),
    SessionPid1 = spawn(fun() -> mock_session(Parent) end),

    {ok, false, QPid1} = vmq_reg_leader:register_subscriber(SessionPid1, SubscriberId, QueueOpts),
    {ok, [1]} = vmq_reg:subscribe(false, <<"mock-user">>, SubscriberId,
                                  [{[<<"test">>, <<"topic">>], 1}]),
    %% at this point we've a working subscription
    timer:sleep(10),
    Msg = msg([<<"test">>, <<"topic">>], <<"test-message">>, 1),
    ok = vmq_reg:publish(Msg),
    receive_msg(QPid1, 1, Msg),

    %% teardown session
    SessionPid1 ! go_down,
    timer:sleep(10),
    {offline, fanout, 0, 0, false} = vmq_queue:status(QPid1),

    %% fill the offline queue
    ok = vmq_reg:publish(Msg),
    {offline, fanout, 1, 0, false} = vmq_queue:status(QPid1),

    %% crash the queue
    catch gen_fsm:sync_send_all_state_event(QPid1, byebye),
    false = is_process_alive(QPid1),
    timer:sleep(10),
    NewQPid = vmq_reg:get_queue_pid(SubscriberId),
    {offline, fanout, 1, 0, false} = vmq_queue:status(NewQPid),

    %% reconnect
    SessionPid2 = spawn(fun() -> mock_session(Parent) end),
    {ok, true, NewQPid} = vmq_reg_leader:register_subscriber(SessionPid2, SubscriberId, QueueOpts),
    receive_persisted_msg(NewQPid, 1, Msg),
    {online, fanout, 0, 1, false} = vmq_queue:status(NewQPid),
    {ok, []} = vmq_lvldb_store:msg_store_find(SubscriberId).

queue_fifo_test(_) ->
    Parent = self(),
    SubscriberId = {"", <<"mock-fifo-client">>},
    QueueOpts = maps:merge(#{clean_session => false}, vmq_queue:default_opts()),
    SessionPid1 = spawn(fun() -> mock_session(Parent) end),

    {ok, false, QPid} = vmq_reg_leader:register_subscriber(SessionPid1, SubscriberId, QueueOpts),
    {ok, [1]} = vmq_reg:subscribe(false, <<"mock-user">>, SubscriberId,
                           [{[<<"test">>, <<"fifo">>, <<"topic">>], 1}]),
    %% teardown session
    SessionPid1 ! go_down,
    timer:sleep(10),

    Msgs = publish_multi([<<"test">>, <<"fifo">>, <<"topic">>]),

    SessionPid2 = spawn(fun() -> mock_session(Parent) end),
    {ok, true, QPid} = vmq_reg_leader:register_subscriber(SessionPid2, SubscriberId, QueueOpts),

    ok = receive_multi(QPid, 1, Msgs),
    {ok, []} = vmq_lvldb_store:msg_store_find(SubscriberId).

queue_lifo_test(_) ->
    Parent = self(),
    SubscriberId = {"", <<"mock-lifo-client">>},
    QueueOpts = maps:merge(vmq_queue:default_opts(), #{clean_session => false, queue_type => lifo}),
    SessionPid1 = spawn(fun() -> mock_session(Parent) end),

    {ok, false, QPid} = vmq_reg_leader:register_subscriber(SessionPid1, SubscriberId, QueueOpts),
    {ok, [1]} = vmq_reg:subscribe(false, <<"mock-user">>, SubscriberId, [{[<<"test">>, <<"lifo">>, <<"topic">>], 1}]),
    %% teardown session
    SessionPid1 ! go_down,
    timer:sleep(10),

    Msgs = publish_multi([<<"test">>, <<"lifo">>, <<"topic">>]),

    SessionPid2 = spawn(fun() -> mock_session(Parent) end),
    {ok, true, QPid} = vmq_reg_leader:register_subscriber(SessionPid2, SubscriberId, QueueOpts),

    ok = receive_multi(QPid, 1, lists:reverse(Msgs)), %% reverse list to get lifo
    {ok, []} = vmq_lvldb_store:msg_store_find(SubscriberId).

queue_fifo_offline_drop_test(_) ->
    Parent = self(),
    SubscriberId = {"", <<"mock-fifo-client">>},
    QueueOpts = maps:merge(vmq_queue:default_opts(), #{clean_session => false,
                                                       max_offline_messages => 10}),
    SessionPid1 = spawn(fun() -> mock_session(Parent) end),

    {ok, false, QPid} = vmq_reg_leader:register_subscriber(SessionPid1, SubscriberId, QueueOpts),
    {ok, [1]} = vmq_reg:subscribe(false, <<"mock-user">>, SubscriberId, [{[<<"test">>, <<"fifo">>, <<"topic">>], 1}]),
    %% teardown session
    SessionPid1 ! go_down,
    timer:sleep(10),

    Msgs = publish_multi([<<"test">>, <<"fifo">>, <<"topic">>]), % publish 100, only the first 10 are kept
    {offline, fanout, 10, 0, false} = vmq_queue:status(QPid),

    SessionPid2 = spawn(fun() -> mock_session(Parent) end),
    {ok, true, QPid} = vmq_reg_leader:register_subscriber(SessionPid2, SubscriberId, QueueOpts),
    {KeptMsgs, _} = lists:split(10, Msgs),
    ok = receive_multi(QPid, 1, KeptMsgs),
    {ok, []} = vmq_lvldb_store:msg_store_find(SubscriberId).


queue_lifo_offline_drop_test(_) ->
    Parent = self(),
    SubscriberId = {"", <<"mock-lifo-client">>},
    QueueOpts = maps:merge(vmq_queue:default_opts(), #{clean_session => false,
                                                       max_offline_messages => 10,
                                                       queue_type => lifo}),
    SessionPid1 = spawn(fun() -> mock_session(Parent) end),

    {ok, false, QPid} = vmq_reg_leader:register_subscriber(SessionPid1, SubscriberId, QueueOpts),
    {ok, [1]} = vmq_reg:subscribe(false, <<"mock-user">>, SubscriberId,
                           [{[<<"test">>, <<"lifo">>, <<"topic">>], 1}]),
    %% teardown session
    SessionPid1 ! go_down,
    timer:sleep(10),

    Msgs = publish_multi([<<"test">>, <<"lifo">>, <<"topic">>]), % publish 100, only the first 10 are kept
    {offline, fanout, 10, 0, false} = vmq_queue:status(QPid),

    SessionPid2 = spawn(fun() -> mock_session(Parent) end),
    {ok, true, QPid} = vmq_reg_leader:register_subscriber(SessionPid2, SubscriberId, QueueOpts),
    {KeptMsgs, _} = lists:split(10, lists:reverse(Msgs)),
    ok = receive_multi(QPid, 1, KeptMsgs),
    {ok, []} = vmq_lvldb_store:msg_store_find(SubscriberId).


queue_offline_transition_test(_) ->
    Parent = self(),
    SubscriberId = {"", <<"mock-trans-client">>},
    QueueOpts = maps:merge(vmq_queue:default_opts(), #{clean_session => false,
                                                       max_offline_messages => 1000,
                                                       queue_type => fifo}),
    SessionPid1 = spawn(fun() -> mock_session(Parent) end),
    {ok, false, QPid} = vmq_reg_leader:register_subscriber(SessionPid1, SubscriberId, QueueOpts),
    {ok, [1]} = vmq_reg:subscribe(false, <<"mock-user">>, SubscriberId, [{[<<"test">>, <<"transition">>], 1}]),
    timer:sleep(10), % give some time to plumtree

    %% teardown session
    catch vmq_queue:set_last_waiting_acks(QPid, []), % simulate what real session does
    SessionPid1 ! {go_down_in, 1},
    Msgs = publish_multi([<<"test">>, <<"transition">>]), % publish 100

    SessionPid2 = spawn(fun() -> mock_session(Parent) end),
    {ok, true, QPid} = vmq_reg_leader:register_subscriber(SessionPid2, SubscriberId, QueueOpts),
    ok = receive_multi(QPid, 1, Msgs),
    {ok, []} = vmq_lvldb_store:msg_store_find(SubscriberId).


publish_multi(Topic) ->
    publish_multi(Topic, []).

publish_multi(Topic, Acc) when length(Acc) < 100 ->
    Msg = msg(Topic, list_to_binary("test-message-"++ integer_to_list(length(Acc))), 1),
    ok = vmq_reg:publish(Msg),
    publish_multi(Topic, [Msg|Acc]);
publish_multi(_, Acc) -> lists:reverse(Acc).

receive_multi(QPid, QoS, Msgs) ->
    PMsgs = [{deliver, QoS, Msg#vmq_msg{persisted=true, qos=1}} || Msg <- Msgs],
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
    end.

mock_session(Parent) ->
    receive
        {vmq_mqtt_fsm, {mail, QPid, new_data}} ->
            vmq_queue:active(QPid),
            mock_session(Parent);
        {vmq_mqtt_fsm, {mail, QPid, Msgs, _, _}} ->
            vmq_queue:notify(QPid),
            timer:sleep(100),
            Parent ! {received, QPid, Msgs},
            mock_session(Parent);
        {go_down_in, Ms} ->
            timer:sleep(Ms);
        _ -> % go down
            ok
    end.

msg(Topic, Payload, QoS) ->
    #vmq_msg{trade_consistency=false,
             reg_view=vmq_reg_trie,
             msg_ref=vmq_mqtt_fsm:msg_ref(),
             mountpoint="",
             routing_key=Topic,
             payload=Payload,
             qos=QoS}.

receive_msg(QPid, QoS, Msg) ->
    %% if we were able to persist the message
    %% we'll set the persist flag
    PMsg = Msg#vmq_msg{persisted=true},
    receive
        {received, QPid, [{deliver, QoS, PMsg}]} ->
            ok;
        M ->
            exit({wrong_message, M})
    end.

receive_persisted_msg(QPid, QoS, Msg) ->
    %% if we were able to persist the message
    %% we'll set the persist flag,
    %% BUT we've also set the qos of the message
    %% to the one of the subscription
    PMsg = Msg#vmq_msg{persisted=true, qos=QoS},
    receive
        {received, QPid, [{deliver, QoS, PMsg}]} ->
            ok;
        M ->
            exit({wrong_message, M})
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
enable_hooks() ->
    vmq_plugin_mgr:enable_module_plugin(auth_on_publish, ?MODULE, hook_auth_on_publish, 6),
    vmq_plugin_mgr:enable_module_plugin(auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3),
    vmq_plugin_mgr:enable_module_plugin(vmq_lvldb_store, msg_store_write, 2),
    vmq_plugin_mgr:enable_module_plugin(vmq_lvldb_store, msg_store_read, 2),
    vmq_plugin_mgr:enable_module_plugin(vmq_lvldb_store, msg_store_delete, 2),
    vmq_plugin_mgr:enable_module_plugin(vmq_lvldb_store, msg_store_find, 1).

hook_auth_on_publish(_, _, _, _, _, _) -> ok.
hook_auth_on_subscribe(_, _, _) -> ok.
