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
         incr_cluster_bytes_dropped/1,
         entries/0,
         reset/0,
         metrics/0]).

-export([message_rate/0]).

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

-record(state, {msg_rate_ref, last_pub_recv=0}).

-define(COUNTER_TAB, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

message_rate() ->
    counter(mqtt_msg_rate).

counter(Name) ->
    CntRef =
    case get(Name) of
        undefined ->
            Ref = mzmetrics:alloc_resource(0, atom_to_list(Name), 8),
            put(Name, Ref),
            Ref;
        Ref ->
            Ref
    end,
    mzmetrics:get_resource_counter(CntRef, 0).

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
            CntRef = mzmetrics:alloc_resource(0, atom_to_list(Entry), 8),
            put(Entry, CntRef),
            incr_item(Entry, Val);
        CntRef when Val == 1 ->
            mzmetrics:incr_resource_counter(CntRef, 0);
        CntRef ->
            mzmetrics:update_resource_counter(CntRef, 0, Val)
    end.

entries() ->
    {ok, []}.

reset() ->
    lists:foreach(fun(M) ->
                        case get(M) of
                            undefined ->
                                ignore;
                            CntRef ->
                                mzmetrics:reset_resource_counter(CntRef, 0)
                        end
                end, mzmetrics()).

metrics() ->
    lists:foldl(fun(M, Acc) ->
                        CntRef =
                        case get(M) of
                            undefined ->
                                Ref = mzmetrics:alloc_resource(0, atom_to_list(M), 8),
                                put(M, Ref),
                                Ref;
                            Ref ->
                                Ref
                        end,
                        [{counter, M, mzmetrics:get_resource_counter(CntRef,
                                                                     0)} | Acc]
                end, system_statistics(), mzmetrics()).


mzmetrics() ->
    [socket_open, socket_close, socket_error,
     bytes_received, bytes_sent,

     mqtt_connect_received, mqtt_publish_received,
     mqtt_puback_received, mqtt_pubrec_received,
     mqtt_pubrel_received, mqtt_pubcomp_received,
     mqtt_subscribe_received, mqtt_unsubscribe_received,
     mqtt_pingreq_received, mqtt_disconnect_received,

     mqtt_publish_sent, mqtt_puback_sent,
     mqtt_pubrec_sent, mqtt_pubrel_sent, mqtt_pubcomp_sent,
     mqtt_suback_sent, mqtt_suback_sent, mqtt_pingresp_sent,

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
     {counter, system_wallclock, Total_Wallclock_Time}|scheduler_utilization()].

scheduler_utilization() ->
    WallTimeTs0 =
    case erlang:get(vmq_exo_scheduler_wall_time) of
        undefined ->
            erlang:system_flag(scheduler_wall_time, true),
            Ts0 = lists:sort(erlang:statistics(scheduler_wall_time)),
            erlang:put(vmq_exo_scheduler_wall_time, Ts0),
            Ts0;
        Ts0 -> Ts0
    end,
    WallTimeTs1 = lists:sort(erlang:statistics(scheduler_wall_time)),
    erlang:put(vmq_exo_scheduler_wall_time, WallTimeTs1),
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
    timer:send_interval(1000, calc_stats),
    Ref = mzmetrics:alloc_resource(0, "mqtt_msg_rate", 8),
    {ok, #state{msg_rate_ref=Ref}}.

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
handle_info(calc_stats, #state{msg_rate_ref=Ref, last_pub_recv=LastPubRecv} = State) ->
    %% this is (MUST be) called every second!
    PubRecv = counter(mqtt_publish_received),
    SocketOpen = counter(socket_open),
    SocketClose = counter(socket_close),
    MsgRate =
    case abs(SocketOpen - SocketClose) of
        0 ->
            0;
        NrOfConns ->
            abs(PubRecv - LastPubRecv) div NrOfConns
    end,
    mzmetrics:reset_resource_counter(Ref, 0),
    mzmetrics:update_resource_counter(Ref, 0, MsgRate),
    {noreply, State#state{last_pub_recv=PubRecv}}.

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
