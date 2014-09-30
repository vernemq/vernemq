-module(vmq_systree).

-behaviour(gen_server).

%% API
-export([start_link/1]).

-export([incr_bytes_received/1,
         incr_bytes_sent/1,
         incr_active_clients/0,
         decr_active_clients/0,
         incr_inactive_clients/0,
         decr_inactive_clients/0,
         incr_messages_received/0,
         incr_messages_sent/0,
         incr_publishes_dropped/0,
         incr_publishes_received/0,
         incr_publishes_sent/0,
         incr_inflight_count/0,
         decr_inflight_count/0,
         incr_retained_count/0,
         decr_retained_count/0,
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

-record(state, {interval=60000}).
-define(TABLE, vmq_systree).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Interval) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Interval], []).

incr_bytes_received(V) ->
    incr_item(local_bytes_received, V).

incr_bytes_sent(V) ->
    incr_item(local_bytes_sent, V).

incr_active_clients() ->
    update_item(local_active_clients, {2, 1}).

decr_active_clients() ->
    update_item(local_active_clients, {2,-1, 0, 0}).

incr_inactive_clients() ->
    update_item(local_inactive_clients, {2, 1}).

decr_inactive_clients() ->
    update_item(local_inactive_clients, {2, -1, 0, 0}).

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

incr_inflight_count() ->
    update_item(local_inflight_count, {2, 1}).

decr_inflight_count() ->
    update_item(local_inflight_count, {2, -1, 0, 0}).

incr_retained_count() ->
    update_item(local_retained_count, {2, 1}).

decr_retained_count() ->
    update_item(local_retained_count, {2, -1, 0, 0}).

incr_subscription_count() ->
    update_item(local_subscription_count, {2, 1}).

decr_subscription_count() ->
    update_item(local_subscription_count, {2, -1, 0, 0}).

incr_socket_count() ->
    incr_item(local_socket_count).

incr_connect_received() ->
    incr_item(local_connect_received).

items() ->
    [{local_bytes_received,     mavg},
     {local_bytes_sent,         mavg},
     {local_active_clients,     counter},
     {local_inactive_clients,   counter},
     {local_messages_received,  mavg},
     {local_messages_sent,      mavg},
     {local_publishes_dropped,  mavg},
     {local_publishes_received, mavg},
     {local_publishes_sent,     mavg},
     {local_inflight_count,     counter},
     {local_retained_count,     counter},
     {local_subscription_count, counter},
     {local_socket_count,       mavg},
     {local_connect_received,   mavg}].

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init([integer()]) -> {'ok',#state{}}.
init([Interval]) ->
    ets:new(?TABLE, [public, named_table, {write_concurrency, true}]),
    init_table(items()),
    erlang:send_after(Interval, self(), publish),
    erlang:send_after(1000, self(), shift),
    {ok, #state{interval=Interval}}.

-spec handle_call(_, _, #state{}) -> {reply, ok, #state{}}.
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

-spec handle_cast(_, #state{}) -> {noreply, #state{}}.
handle_cast(_Msg, State) ->
    {noreply, State}.

-spec handle_info(_,_) -> {'noreply',_}.
handle_info(publish, State) ->
    #state{interval=Interval} = State,
    Snapshots = averages(items(), []),
    publish(Snapshots),
    erlang:send_after(Interval, self(), publish),
    {noreply, State};
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
publish([{local_active_clients, V}|Rest]) ->
    publish("$SYS/broker/clients/active", V),
    publish(Rest);
publish([{local_inactive_clients, V}|Rest]) ->
    publish("$SYS/broker/clients/inactive", V),
    publish(Rest);
publish([{local_inflight_count, V}|Rest]) ->
    publish("$SYS/broker/messages/inflight", V),
    publish(Rest);
publish([{local_retained_count, V}|Rest]) ->
    publish("$SYS/broker/retained messages/count", V),
    publish(Rest);
publish([{local_subscription_count, V}|Rest]) ->
    publish("$SYS/broker/subscriptions/count", V),
    publish(Rest);
publish([{local_bytes_received, All, Min1, Min5, Min15}|Rest]) ->
    publish("$SYS/broker/bytes/received", All),
    publish("$SYS/broker/load/bytes/received/1min", Min1),
    publish("$SYS/broker/load/bytes/received/5min", Min5 div 5),
    publish("$SYS/broker/load/bytes/received/15min", Min15 div 15),
    publish(Rest);
publish([{local_bytes_sent, All, Min1, Min5, Min15}|Rest]) ->
    publish("$SYS/broker/bytes/sent", All),
    publish("$SYS/broker/load/bytes/sent/1min", Min1),
    publish("$SYS/broker/load/bytes/sent/5min", Min5 div 5),
    publish("$SYS/broker/load/bytes/sent/15min", Min15 div 15),
    publish(Rest);
publish([{local_messages_received, All, Min1, Min5, Min15}|Rest]) ->
    publish("$SYS/broker/messages/received", All),
    publish("$SYS/broker/load/messages/received/1min", Min1),
    publish("$SYS/broker/load/messages/received/5min", Min5 div 5),
    publish("$SYS/broker/load/messages/received/15min", Min15 div 15),
    publish(Rest);
publish([{local_messages_sent, All, Min1, Min5, Min15}|Rest]) ->
    publish("$SYS/broker/messages/sent", All),
    publish("$SYS/broker/load/messages/sent/1min", Min1),
    publish("$SYS/broker/load/messages/sent/5min", Min5 div 5),
    publish("$SYS/broker/load/messages/sent/15min", Min15 div 15),
    publish(Rest);
publish([{local_publishes_received, All, Min1, Min5, Min15}|Rest]) ->
    publish("$SYS/broker/publish/messages/received", All),
    publish("$SYS/broker/load/publish/received/1min", Min1),
    publish("$SYS/broker/load/publish/received/5min", Min5 div 5),
    publish("$SYS/broker/load/publish/received/15min", Min15 div 15),
    publish(Rest);
publish([{local_publishes_sent, All, Min1, Min5, Min15}|Rest]) ->
    publish("$SYS/broker/publish/messages/sent", All),
    publish("$SYS/broker/load/publish/sent/1min", Min1),
    publish("$SYS/broker/load/publish/sent/5min", Min5 div 5),
    publish("$SYS/broker/load/publish/sent/15min", Min15 div 15),
    publish(Rest);
publish([{local_publishes_dropped, All, Min1, Min5, Min15}|Rest]) ->
    publish("$SYS/broker/publish/messages/dropped", All),
    publish("$SYS/broker/load/publish/dropped/1min", Min1),
    publish("$SYS/broker/load/publish/dropped/5min", Min5 div 5),
    publish("$SYS/broker/load/publish/dropped/15min", Min15 div 15),
    publish(Rest);
publish([{local_connect_received, _All, Min1, Min5, Min15}|Rest]) ->
    publish("$SYS/broker/load/connections/1min", Min1),
    publish("$SYS/broker/load/connections/5min", Min5 div 5),
    publish("$SYS/broker/load/connections/15min", Min15 div 15),
    publish(Rest);
publish([{local_socket_count, _All, Min1, Min5, Min15}|Rest]) ->
    publish("$SYS/broker/load/sockets/1min", Min1),
    publish("$SYS/broker/load/sockets/5min", Min5 div 5),
    publish("$SYS/broker/load/sockets/15min", Min15 div 15),
    publish(Rest);
publish([_|Rest]) ->
    publish(Rest).


publish(Topic, Val) ->
    vmq_reg:publish(undefined, undefined, undefined,
                    Topic, erlang:integer_to_binary(Val), false).

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
    [Item] = ets:lookup(?TABLE, {Key, counter}),
    averages(Rest, [Item|Acc]);
averages([{Key, mavg}|Rest], Acc) ->
    [{_, Count, _}] = ets:lookup(?TABLE, {Key, total}),
    Item = {Key, Count,
            sum({Key, min1}),
            sum({Key, min5}) div 5,
            sum({Key, min15}) div 15},
    averages(Rest, [Item|Acc]).

sum(Key) ->
    [Item] = ets:lookup(?TABLE, Key),
    [_|Vals] = tuple_to_list(Item),
    lists:sum(Vals).

now_epoch() ->
    {Mega, Sec, _} = os:timestamp(),
    (Mega * 1000000 + Sec).
