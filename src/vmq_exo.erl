%% Copyright 2014 Erlio GmbH Basel Switzerland (http://erl.io)
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(vmq_exo).
-behaviour(gen_server).
-export([incr_bytes_received/1,
         incr_bytes_sent/1,
         incr_expired_clients/0,
         incr_active_clients/0,
         decr_active_clients/0,
         incr_inactive_clients/0,
         decr_inactive_clients/0,
         incr_messages_received/1,
         incr_messages_sent/1,
         incr_publishes_dropped/1,
         incr_publishes_received/1,
         incr_publishes_sent/1,
         incr_subscription_count/0,
         decr_subscription_count/0,
         incr_socket_count/0,
         decr_socket_count/0,
         incr_connect_received/0,
         incr_cluster_bytes_sent/1,
         incr_cluster_bytes_received/1,
         incr_cluster_bytes_dropped/1,
         entries/0,
         entries/1]).

%% API functions
-export([start_link/0,
         system_statistics/0,
         scheduler_utilization/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {history=[]}).

%%%===================================================================
%%% API functions
%%%===================================================================

incr_bytes_received(V) ->
    incr_item([bytes, received], V).

incr_bytes_sent(V) ->
    incr_item([bytes, sent], V).

incr_expired_clients() ->
    exometer:update_or_create([clients, expired], 1).

incr_active_clients() ->
    exometer:update_or_create([clients, active], 1),
    exometer:update_or_create([clients, total], 1).

decr_active_clients() ->
    exometer:update_or_create([clients, active], -1),
    exometer:update_or_create([clients, total], -1).

incr_inactive_clients() ->
    exometer:update_or_create([clients, inactive], 1).

decr_inactive_clients() ->
    exometer:update_or_create([clients, inactive], -1).

incr_messages_received(V) ->
    incr_item([messages, received], V).

incr_messages_sent(V) ->
    incr_item([messages, sent], V).

incr_publishes_dropped(V) ->
    incr_item([publishes, dropped], V).

incr_publishes_received(V) ->
    incr_item([publishes, received], V).

incr_publishes_sent(V) ->
    incr_item([publishes, sent], V).

incr_subscription_count() ->
    incr_item([subscriptions], 1).

decr_subscription_count() ->
    incr_item([subscriptions], -1).

incr_socket_count() ->
    incr_item([sockets], 1).

decr_socket_count() ->
    incr_item([sockets], -1).

incr_connect_received() ->
    incr_item([connects, received], 1).

incr_cluster_bytes_received(V) ->
    incr_item([cluster, bytes, received], V).

incr_cluster_bytes_sent(V) ->
    incr_item([cluster, bytes, sent], V).

incr_cluster_bytes_dropped(V) ->
    incr_item([cluster, bytes, dropped], V).

incr_item(Entry, Val) ->
    exometer:update_or_create(Entry ++ ['last_sec'], Val),
    exometer:update_or_create(Entry ++ ['last_10sec'], Val),
    exometer:update_or_create(Entry ++ ['last_30sec'], Val),
    exometer:update_or_create(Entry ++ ['last_min'], Val),
    exometer:update_or_create(Entry ++ ['last_5min'], Val).

entries() ->
    {ok, entries(undefined)}.

entries(undefined) ->
    [
     {[memory], {function, erlang, memory, [], proplist,
                 [total, processes, system,
                  atom, binary, code, ets]}, [{snmp, []}]},
     {[system_stats], {function, ?MODULE, system_statistics, [], proplist,
                      system_statistics_items()}, [{snmp, []}]},
     {[cpuinfo], {function, ?MODULE, scheduler_utilization, [], proplist,
                 scheduler_utilization_items()}, [{snmp, []}]},
     {[subscriptions], {function, vmq_reg, total_subscriptions, [], proplist,
                        [total]}, [{snmp, []}]},
     {[clients, expired], counter, [{snmp, []}]},
     {[clients, active], counter, [{snmp, []}]},
     {[clients, inactive], counter, [{snmp, []}]},
     {[clients, total], counter, [{snmp, []}]}
     | counter_entries()];
entries({ReporterMod, Interval}) ->
    subscribe(ReporterMod, entries(undefined), Interval).

counter_entries() ->
    [
     {[bytes, received, last_sec], counter, [{snmp, []}]},
     {[bytes, received, last_10sec], counter, [{snmp, []}]},
     {[bytes, received, last_30sec], counter, [{snmp, []}]},
     {[bytes, received, last_min], counter, [{snmp, []}]},
     {[bytes, received, last_5min], counter, [{snmp, []}]},

     {[bytes, sent, last_sec], counter, [{snmp, []}]},
     {[bytes, sent, last_10sec], counter, [{snmp, []}]},
     {[bytes, sent, last_30sec], counter, [{snmp, []}]},
     {[bytes, sent, last_min], counter, [{snmp, []}]},
     {[bytes, sent, last_5min], counter, [{snmp, []}]},

     {[messages, received, last_sec], counter, [{snmp, []}]},
     {[messages, received, last_10sec], counter, [{snmp, []}]},
     {[messages, received, last_30sec], counter, [{snmp, []}]},
     {[messages, received, last_min], counter, [{snmp, []}]},
     {[messages, received, last_5min], counter, [{snmp, []}]},

     {[messages, sent, last_sec], counter, [{snmp, []}]},
     {[messages, sent, last_10sec], counter, [{snmp, []}]},
     {[messages, sent, last_30sec], counter, [{snmp, []}]},
     {[messages, sent, last_min], counter, [{snmp, []}]},
     {[messages, sent, last_5min], counter, [{snmp, []}]},

     {[publishes, dropped, last_sec], counter, [{snmp, []}]},
     {[publishes, dropped, last_10sec], counter, [{snmp, []}]},
     {[publishes, dropped, last_30sec], counter, [{snmp, []}]},
     {[publishes, dropped, last_min], counter, [{snmp, []}]},
     {[publishes, dropped, last_5min], counter, [{snmp, []}]},

     {[publishes, received, last_sec], counter, [{snmp, []}]},
     {[publishes, received, last_10sec], counter, [{snmp, []}]},
     {[publishes, received, last_30sec], counter, [{snmp, []}]},
     {[publishes, received, last_min], counter, [{snmp, []}]},
     {[publishes, received, last_5min], counter, [{snmp, []}]},

     {[publishes, sent, last_sec], counter, [{snmp, []}]},
     {[publishes, sent, last_10sec], counter, [{snmp, []}]},
     {[publishes, sent, last_30sec], counter, [{snmp, []}]},
     {[publishes, sent, last_min], counter, [{snmp, []}]},
     {[publishes, sent, last_5min], counter, [{snmp, []}]},

     {[connects, received, last_sec], counter, [{snmp, []}]},
     {[connects, received, last_10sec], counter, [{snmp, []}]},
     {[connects, received, last_30sec], counter, [{snmp, []}]},
     {[connects, received, last_min], counter, [{snmp, []}]},
     {[connects, received, last_5min], counter, [{snmp, []}]},

     {[sockets, last_sec], counter, [{snmp, []}]},
     {[sockets, last_10sec], counter, [{snmp, []}]},
     {[sockets, last_30sec], counter, [{snmp, []}]},
     {[sockets, last_min], counter, [{snmp, []}]},
     {[sockets, last_5min], counter, [{snmp, []}]},

     {[cluster, bytes, received, last_sec], counter, [{snmp, []}]},
     {[cluster, bytes, received, last_10sec], counter, [{snmp, []}]},
     {[cluster, bytes, received, last_30sec], counter, [{snmp, []}]},
     {[cluster, bytes, received, last_min], counter, [{snmp, []}]},
     {[cluster, bytes, received, last_5min], counter, [{snmp, []}]},

     {[cluster, bytes, sent, last_sec], counter, [{snmp, []}]},
     {[cluster, bytes, sent, last_10sec], counter, [{snmp, []}]},
     {[cluster, bytes, sent, last_30sec], counter, [{snmp, []}]},
     {[cluster, bytes, sent, last_min], counter, [{snmp, []}]},
     {[cluster, bytes, sent, last_5min], counter, [{snmp, []}]},

     {[cluster, bytes, dropped, last_sec], counter, [{snmp, []}]},
     {[cluster, bytes, dropped, last_10sec], counter, [{snmp, []}]},
     {[cluster, bytes, dropped, last_30sec], counter, [{snmp, []}]},
     {[cluster, bytes, dropped, last_min], counter, [{snmp, []}]},
     {[cluster, bytes, dropped, last_5min], counter, [{snmp, []}]}

    ].

system_statistics() ->
    {ContextSwitches, _} = erlang:statistics(context_switches),
    {TotalExactReductions, ExactReductions} = erlang:statistics(exact_reductions),
    {Number_of_GCs, Words_Reclaimed, 0} = erlang:statistics(garbage_collection),
    {{input, Input}, {output, Output}} = erlang:statistics(io),
    {Total_Reductions, Reductions_Since_Last_Call} = erlang:statistics(reductions),
    RunQueueLen = erlang:statistics(run_queue),
    {Total_Run_Time, Time_Since_Last_Call} = erlang:statistics(runtime),
    {Total_Wallclock_Time, Wallclock_Time_Since_Last_Call} = erlang:statistics(wall_clock),

    [{context_switches, ContextSwitches},
     {total_exact_reductions, TotalExactReductions},
     {exact_reductions, ExactReductions},
     {gc_count, Number_of_GCs},
     {words_reclaimed_by_gc, Words_Reclaimed},
     {total_io_in, Input},
     {total_io_out, Output},
     {total_reductions, Total_Reductions},
     {reductions, Reductions_Since_Last_Call},
     {run_queue, RunQueueLen},
     {total_runtime, Total_Run_Time},
     {runtime, Time_Since_Last_Call},
     {total_wallclock, Total_Wallclock_Time},
     {wallclock, Wallclock_Time_Since_Last_Call}].

system_statistics_items() ->
    [context_switches,
     total_exact_reductions,
     exact_reductions,
     gc_count,
     words_reclaimed_by_gc,
     total_io_in,
     total_io_out,
     total_reductions,
     reductions,
     run_queue,
     total_runtime,
     runtime,
     total_wallclock,
     wallclock].

scheduler_utilization() ->
    WallTimeTs0 =
    case erlang:get(vmq_exo_scheduler_wall_time) of
        undefined ->
            Ts0 = lists:sort(erlang:statistics(scheduler_wall_time)),
            erlang:put(vmq_exo_scheduler_wall_time, Ts0),
            Ts0;
        Ts0 -> Ts0
    end,
    WallTimeTs1 = lists:sort(erlang:statistics(scheduler_wall_time)),
    SchedulerUtilization = lists:map(fun({{I, A0, T0}, {I, A1, T1}}) ->
                                             Id = list_to_atom("scheduler_" ++ integer_to_list(I)),
                                             {Id, round((100 * (A1 - A0)/(T1 - T0)))}
                                     end, lists:zip(WallTimeTs0, WallTimeTs1)),
    TotalSum = lists:foldl(fun({_, UsagePerSchedu}, Sum) ->
                                   Sum + UsagePerSchedu
                           end, 0, SchedulerUtilization),
    TotalUtilization = round(TotalSum / length(SchedulerUtilization)),
    [{total, TotalUtilization}|SchedulerUtilization].

scheduler_utilization_items() ->
    erlang:system_flag(scheduler_wall_time, true),
    [total | [list_to_atom("scheduler_" ++ integer_to_list(I))
              || {I, _, _} <- lists:sort(erlang:statistics(scheduler_wall_time))]].

subscribe(ReporterMod, [{Metric, {function, _, _, _, proplist, Items}, _}|Rest],
          Interval) ->
    subscribe(ReporterMod, Metric, Items, Interval),
    subscribe(ReporterMod, Rest, Interval);
subscribe(ReporterMod, [{Metric, {function, _M, _F, _A, value, _}, _}|Rest], Interval) ->
    subscribe(ReporterMod, Metric, default, Interval),
    subscribe(ReporterMod, Rest, Interval);
subscribe(ReporterMod, [{Metric, _, _}|Rest], Interval) ->
    subscribe(ReporterMod, Metric, value, Interval),
    subscribe(ReporterMod, Rest, Interval);
subscribe(_, [], _) -> ok.

subscribe(ReporterMod, Metric, [Datapoint|Rest], Interval) when is_atom(Datapoint) ->
    subscribe(ReporterMod, Metric, Datapoint, Interval),
    subscribe(ReporterMod, Metric, Rest, Interval);
subscribe(ReporterMod, Metric, Datapoint, Interval) when is_atom(Datapoint) ->
    case exometer_report:subscribe(ReporterMod, Metric, Datapoint, Interval) of
        ok ->
            ok;
        E ->
            exit({exometer_report_subscribe, E, ReporterMod, Metric, Datapoint})
    end;
subscribe(_, _, [], _) -> ok.

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    erlang:system_flag(scheduler_wall_time, true),
    timer:send_interval(1000, calc_stats),
    {ok, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Req, _From, #state{} = State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(calc_stats, #state{history=History} = State) ->
    %% this is (MUST be) called every second!
    NewHistory =
    lists:foldl(
      fun({Entry,_,_}, AccHistory) ->
              case lists:reverse(Entry) of
                  [last_sec|Tail] ->
                     %% only call this if we hit the 'last_sec' counter
                      case exometer:get_value(Entry, [value]) of
                          {error, not_found} ->
                              ignore;
                          {ok, [{value, Val}]} ->
                              exometer:update(Entry, -Val),
                              H1 = update_sliding_windows(last_10sec, Tail, Val, AccHistory),
                              H2 = update_sliding_windows(last_30sec, Tail, Val, H1),
                              H3 = update_sliding_windows(last_min, Tail, Val, H2),
                              update_sliding_windows(last_5min, Tail, Val, H3)
                      end;
                  _ ->
                      AccHistory
              end
      end, History, counter_entries()),
    {noreply, State#state{history=NewHistory}}.

update_sliding_windows(last_10sec, Base, Val, History) ->
    update_sliding_windows(10, lists:reverse([last_10sec | Base]), Val, History);
update_sliding_windows(last_30sec, Base, Val, History) ->
    update_sliding_windows(30, lists:reverse([last_30sec | Base]), Val, History);
update_sliding_windows(last_min, Base, Val, History) ->
    update_sliding_windows(60, lists:reverse([last_min | Base]), Val, History);
update_sliding_windows(last_5min, Base, Val, History) ->
    update_sliding_windows(300, lists:reverse([last_5min | Base]), Val, History);

update_sliding_windows(Size, MetricName, SecVal, History) ->
    case lists:keyfind(MetricName, 1, History) of
        false ->
            InitSlidingWindow = [0 || _ <- lists:seq(1, Size - 1)],
            [{MetricName, [SecVal|InitSlidingWindow]}|History];
        {_, SlidingWindow} ->
            case lists:last(SlidingWindow) of
                0 ->
                    lists:keyreplace(MetricName, 1, History ,
                                     {MetricName, [SecVal|lists:droplast(SlidingWindow)]});
                OldestVal ->
                    exometer:update(MetricName, -OldestVal),
                    lists:keyreplace(MetricName, 1, History ,
                                     {MetricName, [SecVal|lists:droplast(SlidingWindow)]})
            end
    end.




%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
