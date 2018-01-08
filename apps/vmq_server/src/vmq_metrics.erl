%% Copyright 2018 Erlio GmbH Basel Switzerland (http://erl.io)
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
-include("vmq_server.hrl").

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

         incr_mqtt_connack_sent/1,
         incr_mqtt_publish_sent/0,
         incr_mqtt_publishes_sent/1,
         incr_mqtt_puback_sent/0,
         incr_mqtt_pubrec_sent/0,
         incr_mqtt_pubrel_sent/0,
         incr_mqtt_pubcomp_sent/0,
         incr_mqtt_suback_sent/0,
         incr_mqtt_unsuback_sent/0,
         incr_mqtt_pingresp_sent/0,

         incr_mqtt_error_auth_publish/0,
         incr_mqtt_error_auth_subscribe/0,
         incr_mqtt_error_invalid_msg_size/0,
         incr_mqtt_error_invalid_puback/0,
         incr_mqtt_error_invalid_pubrec/0,
         incr_mqtt_error_invalid_pubcomp/0,

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
         metrics/1,
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

incr_mqtt_connack_sent(?CONNACK_ACCEPT) ->
    incr_item(mqtt_connack_accepted_sent, 1);
incr_mqtt_connack_sent(?CONNACK_PROTO_VER) ->
    incr_item(mqtt_connack_unacceptable_protocol_sent, 1);
incr_mqtt_connack_sent(?CONNACK_INVALID_ID) ->
    incr_item(mqtt_connack_identifier_rejected_sent, 1);
incr_mqtt_connack_sent(?CONNACK_SERVER) ->
    incr_item(mqtt_connack_server_unavailable_sent, 1);
incr_mqtt_connack_sent(?CONNACK_CREDENTIALS) ->
    incr_item(mqtt_connack_bad_credentials_sent, 1);
incr_mqtt_connack_sent(?CONNACK_AUTH) ->
    incr_item(mqtt_connack_not_authorized_sent, 1).

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

incr_mqtt_error_auth_publish() ->
    incr_item(mqtt_publish_auth_error, 1).

incr_mqtt_error_auth_subscribe() ->
    incr_item(mqtt_subscribe_auth_error, 1).

incr_mqtt_error_invalid_msg_size() ->
    incr_item(mqtt_invalid_msg_size_error, 1).

incr_mqtt_error_invalid_puback() ->
    incr_item(mqtt_puback_invalid_error, 1).

incr_mqtt_error_invalid_pubrec() ->
    incr_item(mqtt_pubrec_invalid_error, 1).

incr_mqtt_error_invalid_pubcomp() ->
    incr_item(mqtt_pubcomp_invalid_error, 1).

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
                    lager:error("invalid counter ~p", [Entry])
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
    metrics(false).

metrics(WithDescriptions) ->
    Metrics =
        lists:foldl(fun(Entry, Acc) ->
                            [{counter, Entry,
                              try counter_val(Entry) of
                                  Value -> Value
                              catch
                                  _:_ -> 0
                              end} | Acc]
                    end, system_statistics() ++ misc_statistics(), counter_entries()),
    case WithDescriptions of
        true ->
            lists:map(
              fun({Type, Metric, Val}) ->
                      {Type, Metric, Val, describe({Type, Metric})}
              end,
              Metrics);
        _ -> Metrics
    end.

counter_entries() ->
    [socket_open, socket_close, socket_error,
     bytes_received, bytes_sent,

     mqtt_connect_received, mqtt_publish_received,
     mqtt_puback_received, mqtt_pubrec_received,
     mqtt_pubrel_received, mqtt_pubcomp_received,
     mqtt_subscribe_received, mqtt_unsubscribe_received,
     mqtt_pingreq_received, mqtt_disconnect_received,

     mqtt_connack_accepted_sent, mqtt_connack_unacceptable_protocol_sent,
     mqtt_connack_identifier_rejected_sent,mqtt_connack_server_unavailable_sent,
     mqtt_connack_bad_credentials_sent,mqtt_connack_not_authorized_sent,

     mqtt_publish_sent, mqtt_puback_sent,
     mqtt_pubrec_sent, mqtt_pubrel_sent, mqtt_pubcomp_sent,
     mqtt_suback_sent, mqtt_unsuback_sent, mqtt_pingresp_sent,

     mqtt_publish_auth_error,
     mqtt_subscribe_auth_error,
     mqtt_invalid_msg_size_error,
     mqtt_puback_invalid_error, mqtt_pubrec_invalid_error,
     mqtt_pubcomp_invalid_error,

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
    {NrOfSubs, SMemory} = vmq_reg_trie:stats(),
    {NrOfRetain, RMemory} = vmq_retain_srv:stats(),
    [{gauge, router_subscriptions, NrOfSubs},
     {gauge, router_memory, SMemory},
     {gauge, retain_messages, NrOfRetain},
     {gauge, retain_memory, RMemory},
     {gauge, queue_processes, vmq_queue_sup_sup:nr_of_queues()}].

system_statistics() ->
    {ContextSwitches, _} = erlang:statistics(context_switches),
    {TotalExactReductions, _} = erlang:statistics(exact_reductions),
    {Number_of_GCs, Words_Reclaimed, 0} = erlang:statistics(garbage_collection),
    {{input, Input}, {output, Output}} = erlang:statistics(io),
    {Total_Reductions, _} = erlang:statistics(reductions),
    RunQueueLen = erlang:statistics(run_queue),
    {Total_Run_Time, _} = erlang:statistics(runtime),
    {Total_Wallclock_Time, _} = erlang:statistics(wall_clock),
    #{total := ErlangMemTotal,
      processes := ErlangMemProcesses,
      processes_used := ErlangMemProcessesUsed,
      system := ErlangMemSystem,
      atom := ErlangMemAtom,
      atom_used := ErlangMemAtomUsed,
      binary := ErlangMemBinary,
      code := ErlangMemCode,
      ets := ErlangMemEts} = maps:from_list(erlang:memory()),
    
    [{counter, system_context_switches, ContextSwitches},
     {counter, system_exact_reductions, TotalExactReductions},
     {counter, system_gc_count, Number_of_GCs},
     {counter, system_words_reclaimed_by_gc, Words_Reclaimed},
     {counter, system_io_in, Input},
     {counter, system_io_out, Output},
     {counter, system_reductions, Total_Reductions},
     {gauge,   system_run_queue, RunQueueLen},
     {counter, system_runtime, Total_Run_Time},
     {counter, system_wallclock, Total_Wallclock_Time},
     {gauge, vm_memory_total, ErlangMemTotal},
     {gauge, vm_memory_processes, ErlangMemProcesses},
     {gauge, vm_memory_processes_used, ErlangMemProcessesUsed},
     {gauge, vm_memory_system, ErlangMemSystem},
     {gauge, vm_memory_atom, ErlangMemAtom},
     {gauge, vm_memory_atom_used, ErlangMemAtomUsed},
     {gauge, vm_memory_binary, ErlangMemBinary},
     {gauge, vm_memory_code, ErlangMemCode},
     {gauge, vm_memory_ets, ErlangMemEts}|
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
            lager:warning("can't calculate message rates", [])
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

describe({counter,cluster_bytes_dropped}) ->
    <<"The number of bytes dropped while sending data to other cluster nodes.">>;
describe({counter,cluster_bytes_sent}) ->
    <<"The number of bytes send to other cluster nodes.">>;
describe({counter,cluster_bytes_received}) ->
    <<"The number of bytes recevied from other cluster nodes.">>;
describe({counter,client_expired}) ->
    <<"Not in use (deprecated)">>;
describe({counter,queue_message_out}) ->
    <<"The number of PUBLISH packets sent from MQTT queue processes.">>;
describe({counter,queue_message_in}) ->
    <<"The number of PUBLISH packets received by MQTT queue processes.">>;
describe({counter,queue_message_unhandled}) ->
    <<"The number of unhandled messages when connecting with clean session=true.">>;
describe({counter,queue_message_drop}) ->
    <<"The number of messages dropped due to full queues.">>;
describe({counter,queue_teardown}) ->
    <<"The number of times a MQTT queue process has been terminated.">>;
describe({counter,queue_setup}) ->
    <<"The number of times a MQTT queue process has been started.">>;
describe({counter,mqtt_unsubscribe_error}) ->
    <<"The number of times an UNSUBSCRIBE operation failed due to a netsplit.">>;
describe({counter,mqtt_subscribe_error}) ->
    <<"The number of times a SUBSCRIBE operation failed due to a netsplit.">>;
describe({counter,mqtt_publish_error}) ->
    <<"The number of times a PUBLISH operation failed due to a netsplit.">>;
describe({counter,mqtt_pubcomp_invalid_error}) ->
    <<"The number of unexpected PUBCOMP messages received.">>;
describe({counter,mqtt_pubrec_invalid_error}) ->
    <<"The number of unexpected PUBCOMP messages received.">>;
describe({counter,mqtt_puback_invalid_error}) ->
    <<"The number of unexpected PUBACK messages received.">>;
describe({counter,mqtt_invalid_msg_size_error}) ->
    <<"The number of packges exceeding the maximum allowed size.">>;
describe({counter,mqtt_subscribe_auth_error}) ->
    <<"The number of unauthorized subscription attempts.">>;
describe({counter,mqtt_publish_auth_error}) ->
    <<"The number of unauthorized publish attempts.">>;
describe({counter,mqtt_pingresp_sent}) ->
    <<"The number of PINGRESP packets sent.">>;
describe({counter,mqtt_unsuback_sent}) ->
    <<"The number of UNSUBACK packets sent.">>;
describe({counter,mqtt_suback_sent}) ->
    <<"The number of SUBACK packets sent.">>;
describe({counter,mqtt_pubcomp_sent}) ->
    <<"The number of PUBCOMP packets sent.">>;
describe({counter,mqtt_pubrel_sent}) ->
    <<"The number of PUBREL packets sent.">>;
describe({counter,mqtt_pubrec_sent}) ->
    <<"The number of PUBREC packets sent.">>;
describe({counter,mqtt_puback_sent}) ->
    <<"The number of PUBACK packets sent.">>;
describe({counter,mqtt_publish_sent}) ->
    <<"The number of PUBLISH packets sent.">>;
describe({counter,mqtt_connack_not_authorized_sent}) ->
    <<"The number of times a client was rejected due to insufficient authorization.">>;
describe({counter,mqtt_connack_bad_credentials_sent}) ->
    <<"The number of times a client sent bad credentials.">>;
describe({counter,mqtt_connack_server_unavailable_sent}) ->
    <<"The number of times a client was rejected due the the broker being unavailable.">>;
describe({counter,mqtt_connack_identifier_rejected_sent}) ->
    <<"The number of times a client was rejected due to a unacceptable identifier.">>;
describe({counter,mqtt_connack_unacceptable_protocol_sent}) ->
    <<"The number of times the broker is not able to support the requested protocol.">>;
describe({counter,mqtt_connack_accepted_sent}) ->
    <<"The number of times a connection has been accepted.">>;
describe({counter,mqtt_disconnect_received}) ->
    <<"The number of DISCONNECT packets sent.">>;
describe({counter,mqtt_pingreq_received}) ->
    <<"The number of PINGREQ packets received.">>;
describe({counter,mqtt_unsubscribe_received}) ->
    <<"The number of UNSUBSCRIBE packets received.">>;
describe({counter,mqtt_subscribe_received}) ->
    <<"The number of SUBSCRIBE packets received.">>;
describe({counter,mqtt_pubcomp_received}) ->
    <<"The number of PUBCOMP packets received.">>;
describe({counter,mqtt_pubrel_received}) ->
    <<"The number of PUBREL packets received.">>;
describe({counter,mqtt_pubrec_received}) ->
    <<"The number of PUBREC packets received.">>;
describe({counter,mqtt_puback_received}) ->
    <<"The number of PUBACK packets received.">>;
describe({counter,mqtt_publish_received}) ->
    <<"The number of PUBLISH packets received.">>;
describe({counter,mqtt_connect_received}) ->
    <<"The number of CONNECT packets received.">>;
describe({counter,bytes_sent}) ->
    <<"The total number of bytes sent.">>;
describe({counter,bytes_received}) ->
    <<"The total number of bytes received.">>;
describe({counter,socket_error}) ->
    <<"The total number of socket errors that have occurred.">>;
describe({counter,socket_close}) ->
    <<"The number of times an MQTT socket has been closed.">>;
describe({counter,socket_open}) ->
    <<"The number of times an MQTT socket has been opened.">>;
describe({counter,system_context_switches}) ->
    <<"The total number of context switches.">>;
describe({counter,system_exact_reductions}) ->
    <<"The exact number of reductions performed.">>;
describe({counter,system_gc_count}) ->
    <<"The number of garbage collections performed.">>;
describe({counter,system_words_reclaimed_by_gc}) ->
    <<"The number of words reclaimed by the garbage collector.">>;
describe({counter,system_io_in}) ->
    <<"The total number of bytes received through ports.">>;
describe({counter,system_io_out}) ->
    <<"The total number of bytes sent through ports.">>;
describe({counter,system_reductions}) ->
    <<"The number of reductions performed in the VM since the node was started.">>;
describe({gauge,system_run_queue}) ->
    <<"The total number of processes and ports ready to run on all run-queues.">>;
describe({counter,system_runtime}) ->
    <<"The sum of the runtime for all threads in the Erlang runtime system.">>;
describe({counter,system_wallclock}) ->
    <<"The number of milli-seconds passed since the node was started.">>;
describe({gauge,system_utilization}) ->
    <<"The average system (scheduler) utilization (percentage).">>;
describe({gauge,router_subscriptions}) ->
    <<"The number of subscriptions in the routing table.">>;
describe({gauge,router_memory}) ->
    <<"The number of bytes used by the routing table.">>;
describe({gauge,retain_messages}) ->
    <<"The number of currently stored retained messages.">>;
describe({gauge,retain_memory}) ->
    <<"The number of bytes used for storing retained messages.">>;
describe({gauge,queue_processes}) ->
    <<"The number of MQTT queue processes.">>;
describe({gauge, vm_memory_total}) ->
    <<"The total amount of memory allocated.">>;
describe({gauge, vm_memory_processes}) ->
    <<"The amount of memory allocated for processes.">>;
describe({gauge, vm_memory_processes_used}) ->
    <<"The amount of memory used by processes.">>;
describe({gauge, vm_memory_system}) ->
    <<"The amount of memory allocated for the emulator.">>;
describe({gauge, vm_memory_atom}) ->
    <<"The amount of memory allocated for atoms.">>;
describe({gauge, vm_memory_atom_used}) ->
    <<"The amount of memory used by atoms.">>;
describe({gauge, vm_memory_binary}) ->
    <<"The amount of memory allocated for binaries.">>;
describe({gauge, vm_memory_code}) ->
    <<"The amount of memory allocated for code.">>;
describe({gauge, vm_memory_ets}) ->
    <<"The amount of memory allocated for ETS tables.">>;
describe({Type, Metric}) ->
    describe_dynamic({Type, atom_to_binary(Metric, utf8)}).

describe_dynamic({gauge,<<"system_utilization_scheduler_", Number/binary>>}) ->
    <<"Scheduler ", Number/binary, " utilization (percentage)">>.
