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

-module(vmq_metrics).
-behaviour(gen_server).
-export([
         incr_socket_open/0,
         incr_socket_close/0,
         incr_socket_error/0,
         incr_bytes_received/1,
         incr_bytes_sent/1,

         incr_mqtt_connect_received/0,
         incr_mqtt_publish_received/0,
         incr_mqtt_puback_received/0,
         incr_mqtt_pubrec_received/0,
         incr_mqtt_pubrel_received/0,
         incr_mqtt_pubcomp_received/0,
         incr_mqtt_subscribe_received/0,
         incr_mqtt_unsubscribe_received/0,
         incr_mqtt_pingreq_received/0,
         incr_mqtt_disconnect_received/0,

         incr_mqtt_publish_sent/0,
         incr_mqtt_publishes_sent/1,
         incr_mqtt_puback_sent/0,
         incr_mqtt_pubrec_sent/0,
         incr_mqtt_pubrel_sent/0,
         incr_mqtt_pubcomp_sent/0,
         incr_mqtt_suback_sent/0,
         incr_mqtt_unsuback_sent/0,
         incr_mqtt_pingresp_sent/0,

         incr_mqtt_error_auth_connect/0,
         incr_mqtt_error_auth_publish/0,
         incr_mqtt_error_auth_subscribe/0,
         incr_mqtt_error_invalid_msg_size/0,
         incr_mqtt_error_invalid_puback/0,
         incr_mqtt_error_invalid_pubrec/0,
         incr_mqtt_error_invalid_pubcomp/0,

         incr_mqtt_error_connect/0,
         incr_mqtt_error_publish/0,
         incr_mqtt_error_subscribe/0,
         incr_mqtt_error_unsubscribe/0,

         incr_queue_setup/0,
         incr_queue_teardown/0,
         incr_queue_drop/0,
         incr_queue_unhandled/1,
         incr_queue_in/0,
         incr_queue_in/1,
         incr_queue_out/1,

         incr_client_expired/0,

         incr_cluster_bytes_sent/1,
         incr_cluster_bytes_received/1,
         incr_cluster_bytes_dropped/1
        ]).

-export([metrics/0,
         check_rate/2,
         reset_counters/0,
         reset_counter/1,
         reset_counter/2,
         counter_val/1]).

%% API functions
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).

%%%===================================================================
%%% API functions
%%%===================================================================

%%% Socket Totals
incr_socket_open() ->
    incr_item('socket_open', 1).

incr_socket_close() ->
    incr_item('socket_close', 1).

incr_socket_error() ->
    incr_item('socket_error', 1).

incr_bytes_received(V) ->
    incr_item('bytes_received', V).

incr_bytes_sent(V) ->
    incr_item('bytes_sent', V).


incr_mqtt_connect_received() ->
    incr_item(mqtt_connect_received, 1).

incr_mqtt_publish_received() ->
    incr_item(mqtt_publish_received, 1).

incr_mqtt_puback_received() ->
    incr_item(mqtt_puback_received, 1).

incr_mqtt_pubrec_received() ->
    incr_item(mqtt_pubrec_received, 1).

incr_mqtt_pubrel_received() ->
    incr_item(mqtt_pubrel_received, 1).

incr_mqtt_pubcomp_received() ->
    incr_item(mqtt_pubcomp_received, 1).

incr_mqtt_subscribe_received() ->
    incr_item(mqtt_subscribe_received, 1).

incr_mqtt_unsubscribe_received() ->
    incr_item(mqtt_unsubscribe_received, 1).

incr_mqtt_pingreq_received() ->
    incr_item(mqtt_pingreq_received, 1).

incr_mqtt_disconnect_received() ->
    incr_item(mqtt_disconnect_received, 1).

incr_mqtt_publish_sent() ->
    incr_item(mqtt_publish_sent, 1).
incr_mqtt_publishes_sent(N) ->
    incr_item(mqtt_publish_sent, N).

incr_mqtt_puback_sent() ->
    incr_item(mqtt_puback_sent, 1).

incr_mqtt_pubrec_sent() ->
    incr_item(mqtt_pubrec_sent, 1).

incr_mqtt_pubrel_sent() ->
    incr_item(mqtt_pubrel_sent, 1).

incr_mqtt_pubcomp_sent() ->
    incr_item(mqtt_pubcomp_sent, 1).

incr_mqtt_suback_sent() ->
    incr_item(mqtt_suback_sent, 1).

incr_mqtt_unsuback_sent() ->
    incr_item(mqtt_unsuback_sent, 1).

incr_mqtt_pingresp_sent() ->
    incr_item(mqtt_pingresp_sent, 1).

incr_mqtt_error_auth_connect() ->
    incr_item(mqtt_connect_auth_error, 1).

incr_mqtt_error_auth_publish() ->
    incr_item(mqtt_publish_auth_error, 1).

incr_mqtt_error_auth_subscribe() ->
    incr_item(mqtt_subscribe_auth_error, 1).

incr_mqtt_error_invalid_msg_size() ->
    incr_item(mqtt_publish_invalid_msg_size_error, 1).

incr_mqtt_error_invalid_puback() ->
    incr_item(mqtt_puback_invalid_error, 1).

incr_mqtt_error_invalid_pubrec() ->
    incr_item(mqtt_pubrec_invalid_error, 1).

incr_mqtt_error_invalid_pubcomp() ->
    incr_item(mqtt_pubcomp_invalid_error, 1).

incr_mqtt_error_connect() ->
    incr_item(mqtt_connect_error, 1).

incr_mqtt_error_publish() ->
    incr_item(mqtt_publish_error, 1).

incr_mqtt_error_subscribe() ->
    incr_item(mqtt_subscribe_error, 1).

incr_mqtt_error_unsubscribe() ->
    incr_item(mqtt_unsubscribe_error, 1).

incr_queue_setup() ->
    incr_item(queue_setup, 1).

incr_queue_teardown() ->
    incr_item(queue_teardown, 1).

incr_queue_drop() ->
    incr_item(queue_message_drop, 1).

incr_queue_unhandled(N) ->
    incr_item(queue_message_unhandled, N).

incr_queue_in() ->
    incr_item(queue_message_in, 1).
incr_queue_in(N) ->
    incr_item(queue_message_in, N).

incr_queue_out(N) ->
    incr_item(queue_message_out, N).

incr_client_expired() ->
    incr_item(client_expired, 1).


incr_cluster_bytes_received(V) ->
    incr_item('cluster_bytes_received', V).

incr_cluster_bytes_sent(V) ->
    incr_item('cluster_bytes_sent', V).

incr_cluster_bytes_dropped(V) ->
    incr_item('cluster_bytes_dropped', V).

incr_item(_, 0) -> ok; %% don't do the update
incr_item(Entry, Val) when Val > 0->
    case get(Entry) of
        undefined ->
            try ets:lookup(?MODULE, Entry) of
               [{_, CntRef}] ->
                    put(Entry, CntRef),
                    incr_item(Entry, Val);
                [] ->
                    lager:error("Invalid Counter ~p", [Entry])
            catch
               _:_ ->
                    %% we don't want to crash a session/queue
                    %% due to an unavailable counter
                    ok
            end;
        CntRef when Val == 1 ->
            mzmetrics:incr_resource_counter(CntRef, 0);
        CntRef ->
            mzmetrics:update_resource_counter(CntRef, 0, Val)
    end.

%% true means current rate is ok.
check_rate(_, 0) -> true; % 0 means unlimited
check_rate(RateEntry, MaxRate) ->
    case get(RateEntry) of
        undefined ->
            try ets:lookup(?MODULE, RateEntry) of
                [{_, CntRef}] ->
                    put(RateEntry, CntRef),
                    check_rate(RateEntry, MaxRate)
            catch
                _:_ ->
                    true
            end;
        CntRef ->
            mzmetrics:get_resource_counter(CntRef, 0) < MaxRate
    end.

counter_val(Entry) ->
    [{_, CntRef}] = ets:lookup(?MODULE, Entry),
    mzmetrics:get_resource_counter(CntRef, 0).

reset_counters() ->
    lists:foreach(
      fun(Entry) ->
              reset_counter(Entry)
      end, counter_entries()).

reset_counter(Entry) ->
    [{_, CntRef}] = ets:lookup(?MODULE, Entry),
    mzmetrics:reset_resource_counter(CntRef, 0).

reset_counter(Entry, InitVal) ->
    reset_counter(Entry),
    incr_item(Entry, InitVal).


metrics() ->
    lists:foldl(fun(Entry, Acc) ->
                        [{counter, Entry,
                          try counter_val(Entry) of
                              Value -> Value
                          catch
                              _:_ -> 0
                          end} | Acc]
                end, system_statistics() ++ misc_statistics(), counter_entries()).

counter_entries() ->
    [socket_open, socket_close, socket_error,
     bytes_received, bytes_sent,

     mqtt_connect_received, mqtt_publish_received,
     mqtt_puback_received, mqtt_pubrec_received,
     mqtt_pubrel_received, mqtt_pubcomp_received,
     mqtt_subscribe_received, mqtt_unsubscribe_received,
     mqtt_pingreq_received, mqtt_disconnect_received,

     mqtt_publish_sent, mqtt_puback_sent,
     mqtt_pubrec_sent, mqtt_pubrel_sent, mqtt_pubcomp_sent,
     mqtt_suback_sent, mqtt_unsuback_sent, mqtt_pingresp_sent,

     mqtt_connect_auth_error,
     mqtt_publish_auth_error,
     mqtt_subscribe_auth_error,
     mqtt_publish_invalid_msg_size_error,
     mqtt_puback_invalid_error, mqtt_pubrec_invalid_error,
     mqtt_pubcomp_invalid_error,

     mqtt_connect_error,
     mqtt_publish_error,
     mqtt_subscribe_error,
     mqtt_unsubscribe_error,

     queue_setup, queue_teardown,
     queue_message_drop, queue_message_unhandled,
     queue_message_in, queue_message_out,

     client_expired,

     cluster_bytes_received, cluster_bytes_sent, cluster_bytes_dropped
    ].

rate_entries() ->
    [{msg_in_rate, mqtt_publish_received},
     {byte_in_rate, bytes_received},
     {msg_out_rate, mqtt_publish_sent},
     {byte_out_rate, bytes_sent}].

misc_statistics() ->
    {NrOfSubs, NrOfTopics, SMemory} = vmq_reg_trie:stats(),
    {NrOfRetain, RMemory} = vmq_retain_srv:stats(),
    [{gauge, router_subscriptions, NrOfSubs},
     {gauge, router_topics, NrOfTopics},
     {gauge, router_memory, SMemory},
     {gauge, retain_messages, NrOfRetain},
     {gauge, retain_memory, RMemory},
     {gauge, queue_processes, vmq_queue_sup:nr_of_queues()}].

system_statistics() ->
    {ContextSwitches, _} = erlang:statistics(context_switches),
    {TotalExactReductions, _} = erlang:statistics(exact_reductions),
    {Number_of_GCs, Words_Reclaimed, 0} = erlang:statistics(garbage_collection),
    {{input, Input}, {output, Output}} = erlang:statistics(io),
    {Total_Reductions, _} = erlang:statistics(reductions),
    RunQueueLen = erlang:statistics(run_queue),
    {Total_Run_Time, _} = erlang:statistics(runtime),
    {Total_Wallclock_Time, _} = erlang:statistics(wall_clock),

    [{counter, system_context_switches, ContextSwitches},
     {counter, system_exact_reductions, TotalExactReductions},
     {counter, system_gc_count, Number_of_GCs},
     {counter, system_words_reclaimed_by_gc, Words_Reclaimed},
     {counter, system_io_in, Input},
     {counter, system_io_out, Output},
     {counter, system_reductions, Total_Reductions},
     {gauge,   system_run_queue, RunQueueLen},
     {counter, system_runtime, Total_Run_Time},
     {counter, system_wallclock, Total_Wallclock_Time}|
     scheduler_utilization()].

scheduler_utilization() ->
    WallTimeTs0 =
    case erlang:get(vmq_metrics_scheduler_wall_time) of
        undefined ->
            erlang:system_flag(scheduler_wall_time, true),
            Ts0 = lists:sort(erlang:statistics(scheduler_wall_time)),
            erlang:put(vmq_metrics_scheduler_wall_time, Ts0),
            Ts0;
        Ts0 -> Ts0
    end,
    WallTimeTs1 = lists:sort(erlang:statistics(scheduler_wall_time)),
    erlang:put(vmq_metrics_scheduler_wall_time, WallTimeTs1),
    SchedulerUtilization = lists:map(fun({{I, A0, T0}, {I, A1, T1}}) ->
                                             Id =
                                             list_to_atom("system_utilization_scheduler_" ++ integer_to_list(I)),
                                             {gauge, Id, round((100 * (A1 - A0)/(T1 - T0)))}
                                     end, lists:zip(WallTimeTs0, WallTimeTs1)),
    TotalSum = lists:foldl(fun({_, _, UsagePerSchedu}, Sum) ->
                                   Sum + UsagePerSchedu
                           end, 0, SchedulerUtilization),
    AvgUtilization = round(TotalSum / length(SchedulerUtilization)),
    [{gauge, system_utilization, AvgUtilization}|SchedulerUtilization].

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
    timer:send_interval(1000, calc_rates),
    ets:new(?MODULE, [public, named_table, {read_concurrency, true}]),
    {RateEntries, _} = lists:unzip(rate_entries()),
    lists:foreach(
      fun(Entry) ->
              Ref = mzmetrics:alloc_resource(0, atom_to_list(Entry), 8),
              ets:insert(?MODULE, {Entry, Ref})
      end, RateEntries ++ counter_entries()),
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
handle_info(calc_rates, State) ->
    %% this is (MUST be) called every second!
    SocketOpen = counter_val(socket_open),
    SocketClose = counter_val(socket_close),
    case SocketOpen - SocketClose of
        V when V >= 0 ->
            %% in theory this should always be positive
            %% but intermediate counter resets or counter overflows
            %% could occur
            lists:foreach(
              fun({RateEntry, Entry}) -> calc_rate_per_conn(RateEntry, Entry, V) end,
              rate_entries());
        _ ->
            lager:warning("Can't calculate message rates", [])
    end,
    {noreply, State}.

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
calc_rate_per_conn(REntry, _Entry, 0) ->
    reset_counter(REntry);
calc_rate_per_conn(REntry, Entry, N) ->
    case counter_val_since_last_call(Entry) of
        Val when Val >= 0 ->
            reset_counter(REntry, Val div N);
        _ ->
            reset_counter(REntry)
    end.

counter_val_since_last_call(Entry) ->
    ActVal = counter_val(Entry),
    case get({rate, Entry}) of
        undefined ->
            put({rate, Entry}, ActVal),
            ActVal;
        V ->
            put({rate, Entry}, ActVal),
            ActVal - V
    end.
