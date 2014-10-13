-module(vmq_systree).

-behaviour(gen_server).

%% API
-export([start_link/0,
         change_config_now/3]).

-export([incr_bytes_received/1,
         incr_bytes_sent/1,
         incr_inactive_clients/0,
         decr_inactive_clients/0,
         incr_expired_clients/0,
         incr_messages_received/0,
         incr_messages_sent/0,
         incr_publishes_dropped/0,
         incr_publishes_received/0,
         incr_publishes_sent/0,
         incr_subscription_count/0,
         decr_subscription_count/0,
         incr_socket_count/0,
         incr_connect_received/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {interval=10000, ref}).
-define(TABLE, vmq_systree).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec change_config_now(_,[any()],_) -> 'ok'.
change_config_now(_New, Changed, _Deleted) ->
    %% we are only interested if the config changes
    {_, NewInterval} = proplists:get_value(sys_interval, Changed, {undefined,10}),
    gen_server:cast(?MODULE, {new_interval, NewInterval}).

incr_bytes_received(V) ->
    incr_item(local_bytes_received, V).

incr_bytes_sent(V) ->
    incr_item(local_bytes_sent, V).

incr_expired_clients() ->
    update_item({local_expired_clients, counter}, {2, 1}).

incr_inactive_clients() ->
    update_item({local_inactive_clients, counter}, {2, 1}).

decr_inactive_clients() ->
    update_item({local_inactive_clients, counter}, {2, -1, 0, 0}).

incr_messages_received() ->
    incr_item(local_messages_received).

incr_messages_sent() ->
    incr_item(local_messages_sent).

incr_publishes_dropped() ->
    incr_item(local_publishes_dropped).

incr_publishes_received() ->
    incr_item(local_publishes_received).

incr_publishes_sent() ->
    incr_item(local_publishes_sent).

incr_subscription_count() ->
    update_item({local_subscription_count, counter}, {2, 1}).

decr_subscription_count() ->
    update_item({local_subscription_count, counter}, {2, -1, 0, 0}).

incr_socket_count() ->
    incr_item(local_socket_count).

incr_connect_received() ->
    incr_item(local_connect_received).

items() ->
    [{local_bytes_received,     mavg},
     {local_bytes_sent,         mavg},
     {local_active_clients,     gauge},
     {local_inactive_clients,   counter},
     {local_expired_clients,    counter},
     {local_messages_received,  mavg},
     {local_messages_sent,      mavg},
     {local_publishes_dropped,  mavg},
     {local_publishes_received, mavg},
     {local_publishes_sent,     mavg},
     {local_subscription_count, counter},
     {local_socket_count,       mavg},
     {local_connect_received,   mavg},
     {global_clients_total,     gauge},
     {erlang_vm_metrics,        gauge},
     {local_inflight_count,     gauge},
     {local_retained_count,     gauge},
     {local_stored_count,       gauge}].

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init([integer()]) -> {'ok',#state{}}.
init([]) ->
    Interval = application:get_env(vmq_server, interval, 10),
    ets:new(?TABLE, [public, named_table, {write_concurrency, true}]),
    init_table(items()),
    IntervalMS = Interval * 1000,
    TRef = erlang:send_after(IntervalMS, self(), publish),
    erlang:send_after(1000, self(), shift),
    {ok, #state{interval=IntervalMS, ref=TRef}}.

-spec handle_call(_, _, #state{}) -> {reply, ok, #state{}}.
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

-spec handle_cast(_, #state{}) -> {noreply, #state{}}.
handle_cast({new_interval, Interval}, State) ->
    IntervalMS = Interval * 1000,
    case State#state.ref of
        undefined -> ok;
        OldRef -> erlang:cancel_timer(OldRef)
    end,
    TRef =
    case Interval of
        0 -> undefined;
        _ ->
            erlang:send_after(IntervalMS, self(), publish)
    end,
    {noreply, State#state{interval=IntervalMS, ref=TRef}}.

-spec handle_info(_,_) -> {'noreply',_}.
handle_info(publish, State) ->
    #state{interval=Interval} = State,
    Snapshots = averages(items(), []),
    publish(Snapshots),
    TRef = erlang:send_after(Interval, self(), publish),
    {noreply, State#state{ref=TRef}};
handle_info(shift, State) ->
    shift(now_epoch(), items()),
    erlang:send_after(1000, self(), shift),
    {noreply, State}.

-spec terminate(_,#state{}) -> 'ok'.
terminate(_Reason, _State) ->
    ok.

-spec code_change(_,_,_) -> {'ok',_}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
publish([]) -> ok;
publish([{local_active_clients, gauge}|Rest]) ->
    Res = supervisor:count_children(vmq_session_sup),
    V = proplists:get_value(active, Res),
    publish("clients/active", V),
    publish(Rest);
publish([{local_inactive_clients, V}|Rest]) ->
    publish("clients/inactive", V),
    publish(Rest);
publish([{local_subscription_count, V}|Rest]) ->
    publish("subscriptions/count", V),
    publish(Rest);
publish([{local_bytes_received, All, Min1, Min5, Min15}|Rest]) ->
    publish("bytes/received", All),
    publish("load/bytes/received/1min", Min1),
    publish("load/bytes/received/5min", Min5 div 5),
    publish("load/bytes/received/15min", Min15 div 15),
    publish(Rest);
publish([{local_bytes_sent, All, Min1, Min5, Min15}|Rest]) ->
    publish("bytes/sent", All),
    publish("load/bytes/sent/1min", Min1),
    publish("load/bytes/sent/5min", Min5 div 5),
    publish("load/bytes/sent/15min", Min15 div 15),
    publish(Rest);
publish([{local_messages_received, All, Min1, Min5, Min15}|Rest]) ->
    publish("messages/received", All),
    publish("load/messages/received/1min", Min1),
    publish("load/messages/received/5min", Min5 div 5),
    publish("load/messages/received/15min", Min15 div 15),
    publish(Rest);
publish([{local_messages_sent, All, Min1, Min5, Min15}|Rest]) ->
    publish("messages/sent", All),
    publish("load/messages/sent/1min", Min1),
    publish("load/messages/sent/5min", Min5 div 5),
    publish("load/messages/sent/15min", Min15 div 15),
    publish(Rest);
publish([{local_publishes_received, All, Min1, Min5, Min15}|Rest]) ->
    publish("publish/messages/received", All),
    publish("load/publish/received/1min", Min1),
    publish("load/publish/received/5min", Min5 div 5),
    publish("load/publish/received/15min", Min15 div 15),
    publish(Rest);
publish([{local_publishes_sent, All, Min1, Min5, Min15}|Rest]) ->
    publish("publish/messages/sent", All),
    publish("load/publish/sent/1min", Min1),
    publish("load/publish/sent/5min", Min5 div 5),
    publish("load/publish/sent/15min", Min15 div 15),
    publish(Rest);
publish([{local_publishes_dropped, All, Min1, Min5, Min15}|Rest]) ->
    publish("publish/messages/dropped", All),
    publish("load/publish/dropped/1min", Min1),
    publish("load/publish/dropped/5min", Min5 div 5),
    publish("load/publish/dropped/15min", Min15 div 15),
    publish(Rest);
publish([{local_connect_received, _All, Min1, Min5, Min15}|Rest]) ->
    publish("load/connections/1min", Min1),
    publish("load/connections/5min", Min5 div 5),
    publish("load/connections/15min", Min15 div 15),
    publish(Rest);
publish([{local_socket_count, _All, Min1, Min5, Min15}|Rest]) ->
    publish("$SYS/broker/load/sockets/1min", Min1),
    publish("$SYS/broker/load/sockets/5min", Min5 div 5),
    publish("$SYS/broker/load/sockets/15min", Min15 div 15),
    publish(Rest);
publish([{global_clients_total, gauge}|Rest]) ->
    Total = vmq_reg:total_clients(),
    publish("clients/total", Total),
    publish(Rest);
publish([{erlang_vm_metrics, gauge}|Rest]) ->
    publish("vm/memory", erlang:memory(total)),
    publish("vm/port_count", erlang:system_info(port_count)),
    publish("vm/process_count", erlang:system_info(process_count)),
    publish(Rest);
publish([{local_inflight_count, gauge}|Rest]) ->
    publish("messages/inflight", vmq_msg_store:in_flight()),
    publish(Rest);
publish([{local_retained_count, gauge}|Rest]) ->
    publish("retained messages/count", vmq_msg_store:retained()),
    publish(Rest);
publish([{local_stored_count, gauge}|Rest]) ->
    publish("messages/stored", vmq_msg_store:stored()),
    publish(Rest);
publish([_|Rest]) ->
    publish(Rest).

publish(Topic, Val) ->
    TTopic = lists:flatten(["$SYS/", atom_to_list(node()), "/", Topic]),
    vmq_reg:publish(undefined, undefined, undefined,
                    TTopic, erlang:integer_to_binary(Val), false).

init_table([]) -> ok;
init_table([{MetricName, mavg}|Rest]) ->
    Epoch = now_epoch(),
    ets:insert_new(?TABLE, {{MetricName, total}, 0, Epoch}),
    Min1 = list_to_tuple([{MetricName, min1} | [0 || _ <- lists:seq(1,61)]]),
    ets:insert_new(?TABLE, Min1),
    Min5 = list_to_tuple([{MetricName, min5} | [0 || _ <- lists:seq(1,301)]]),
    ets:insert_new(?TABLE, Min5),
    Min15 = list_to_tuple([{MetricName, min15} | [0 || _ <- lists:seq(1,901)]]),
    ets:insert_new(?TABLE, Min15),
    init_table(Rest);
init_table([{MetricName, counter}|Rest]) ->
    ets:insert(?TABLE, {{MetricName, counter}, 0}),
    init_table(Rest);
init_table([_|Rest]) ->
    init_table(Rest).

incr_item(MetricName) ->
    incr_item(MetricName, 1).
incr_item(MetricName, V) ->
    Epoch = now_epoch(),
    PosMin1 = 2 + (Epoch rem 61),
    PosMin5 = 2 + (Epoch rem 301),
    PosMin15 = 2 + (Epoch rem 901),
    update_item({MetricName, total}, [{2, V}, {3, 1, 0, Epoch}]),
    update_item({MetricName, min1}, [{PosMin1, V}, {PosMin1 + 1, 1, 0, 0}]),
    update_item({MetricName, min5}, [{PosMin5, V}, {PosMin5 + 1, 1, 0, 0}]),
    update_item({MetricName, min15}, [{PosMin15, V}, {PosMin15 + 1, 1, 0, 0}]).

shift(_, []) -> ok;
shift(Now, [{Key, mavg}|Rest]) ->
    [{_,_,LastIncr}] = ets:lookup(?TABLE, {Key, total}),
    case Now - LastIncr of
        0 -> shift(Now, Rest);
        Diff ->
            Diffs = [Now + I || I <- lists:seq(1, Diff)],
            ets:update_element(?TABLE, {Key, total}, {3, Now}),
            ets:update_element(?TABLE, {Key, min1}, [{2 + (I rem 61), 0} || I <- Diffs]),
            ets:update_element(?TABLE, {Key, min5}, [{2 + (I rem 301), 0} || I <- Diffs]),
            ets:update_element(?TABLE, {Key, min15}, [{2 + (I rem 901), 0} || I <- Diffs]),
            shift(Now, Rest)
    end;
shift(Now, [_|Rest]) ->
    shift(Now, Rest).

update_item(Key, UpdateOp) ->
    try
        ets:update_counter(?TABLE, Key, UpdateOp)
    catch error:badarg -> error
    end.

averages([], Acc) -> Acc;
averages([{Key, counter}|Rest], Acc) ->
    %% nothing to move, but we accumulate the item
    [{_, Val}] = ets:lookup(?TABLE, {Key, counter}),
    averages(Rest, [{Key, Val}|Acc]);
averages([{Key, mavg}|Rest], Acc) ->
    [{_, Count, _}] = ets:lookup(?TABLE, {Key, total}),
    Item = {Key, Count,
            sum({Key, min1}),
            sum({Key, min5}) div 5,
            sum({Key, min15}) div 15},
    averages(Rest, [Item|Acc]);
averages([{Key, gauge}|Rest], Acc) ->
    averages(Rest, [{Key, gauge}|Acc]).


sum(Key) ->
    [Item] = ets:lookup(?TABLE, Key),
    [_|Vals] = tuple_to_list(Item),
    lists:sum(Vals).

now_epoch() ->
    {Mega, Sec, _} = os:timestamp(),
    (Mega * 1000000 + Sec).
