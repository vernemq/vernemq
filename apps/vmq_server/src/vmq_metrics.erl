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
-include_lib("vmq_commons/include/vmq_types.hrl").
-include("vmq_server.hrl").
-include("vmq_metrics.hrl").

-behaviour(gen_server).
-export([
         incr/1,
         incr/2,

         incr_socket_open/0,
         incr_socket_close/0,
         incr_socket_close_timeout/0,
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
         incr_queue_initialized_from_storage/0,
         incr_queue_teardown/0,
         incr_queue_drop/0,
         incr_queue_msg_expired/1,
         incr_queue_unhandled/1,
         incr_queue_in/0,
         incr_queue_in/1,
         incr_queue_out/1,

         incr_cluster_bytes_sent/1,
         incr_cluster_bytes_received/1,
         incr_cluster_bytes_dropped/1,

         incr_router_matches_local/1,
         incr_router_matches_remote/1
        ]).

-export([metrics/0,
         metrics/1,
         check_rate/2,
         reset_counters/0,
         reset_counter/1,
         reset_counter/2,
         counter_val/1,
         register/1,
         get_label_info/0]).

%% API functions
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
          info = #{}
         }).

-ifdef(TEST).
-export([clear_stored_rates/0,
         start_calc_rates_interval/0,
         cancel_calc_rates_interval/0]).
-endif.

%%%===================================================================
%%% API functions
%%%===================================================================

%%% Socket Totals
incr_socket_open() ->
    incr_item(?METRIC_SOCKET_OPEN, 1).

incr_socket_close() ->
    incr_item(?METRIC_SOCKET_CLOSE, 1).

incr_socket_close_timeout() ->
    incr_item(?METRIC_SOCKET_CLOSE_TIMEOUT, 1).

incr_socket_error() ->
    incr_item(?METRIC_SOCKET_ERROR, 1).

incr_bytes_received(V) ->
    incr_item(?METRIC_BYTES_RECEIVED, V).

incr_bytes_sent(V) ->
    incr_item(?METRIC_BYTES_SENT, V).

incr_mqtt_connect_received() ->
    incr_item(?MQTT4_CONNECT_RECEIVED, 1).

incr_mqtt_publish_received() ->
    incr_item(?MQTT4_PUBLISH_RECEIVED, 1).

incr_mqtt_puback_received() ->
    incr_item(?MQTT4_PUBACK_RECEIVED, 1).

incr_mqtt_pubrec_received() ->
    incr_item(?MQTT4_PUBREC_RECEIVED, 1).

incr_mqtt_pubrel_received() ->
    incr_item(?MQTT4_PUBREL_RECEIVED, 1).

incr_mqtt_pubcomp_received() ->
    incr_item(?MQTT4_PUBCOMP_RECEIVED, 1).

incr_mqtt_subscribe_received() ->
    incr_item(?MQTT4_SUBSCRIBE_RECEIVED, 1).

incr_mqtt_unsubscribe_received() ->
    incr_item(?MQTT4_UNSUBSCRIBE_RECEIVED, 1).

incr_mqtt_pingreq_received() ->
    incr_item(?MQTT4_PINGREQ_RECEIVED, 1).

incr_mqtt_disconnect_received() ->
    incr_item(?MQTT4_DISCONNECT_RECEIVED, 1).

incr_mqtt_publish_sent() ->
    incr_item(?MQTT4_PUBLISH_SENT, 1).
incr_mqtt_publishes_sent(N) ->
    incr_item(?MQTT4_PUBLISH_SENT, N).

incr_mqtt_puback_sent() ->
    incr_item(?MQTT4_PUBACK_SENT, 1).

incr_mqtt_pubrec_sent() ->
    incr_item(?MQTT4_PUBREC_SENT, 1).

incr_mqtt_pubrel_sent() ->
    incr_item(?MQTT4_PUBREL_SENT, 1).

incr_mqtt_pubcomp_sent() ->
    incr_item(?MQTT4_PUBCOMP_SENT, 1).

incr_mqtt_suback_sent() ->
    incr_item(?MQTT4_SUBACK_SENT, 1).

incr_mqtt_unsuback_sent() ->
    incr_item(?MQTT4_UNSUBACK_SENT, 1).

incr_mqtt_pingresp_sent() ->
    incr_item(?MQTT4_PINGRESP_SENT, 1).

incr_mqtt_error_auth_publish() ->
    incr_item(?MQTT4_PUBLISH_AUTH_ERROR, 1).

incr_mqtt_error_auth_subscribe() ->
    incr_item(?MQTT4_SUBSCRIBE_AUTH_ERROR, 1).

incr_mqtt_error_invalid_msg_size() ->
    incr_item(?MQTT4_INVALID_MSG_SIZE_ERROR, 1).

incr_mqtt_error_invalid_puback() ->
    incr_item(?MQTT4_PUBACK_INVALID_ERROR, 1).

incr_mqtt_error_invalid_pubrec() ->
    incr_item(?MQTT4_PUBREC_INVALID_ERROR, 1).

incr_mqtt_error_invalid_pubcomp() ->
    incr_item(?MQTT4_PUBCOMP_INVALID_ERROR, 1).

incr_mqtt_error_publish() ->
    incr_item(?MQTT4_PUBLISH_ERROR, 1).

incr_mqtt_error_subscribe() ->
    incr_item(?MQTT4_SUBSCRIBE_ERROR, 1).

incr_mqtt_error_unsubscribe() ->
    incr_item(?MQTT4_UNSUBSCRIBE_ERROR, 1).

incr_queue_setup() ->
    incr_item(?METRIC_QUEUE_SETUP, 1).

incr_queue_initialized_from_storage() ->
    incr_item(?METRIC_QUEUE_INITIALIZED_FROM_STORAGE, 1).

incr_queue_teardown() ->
    incr_item(?METRIC_QUEUE_TEARDOWN, 1).

incr_queue_drop() ->
    incr_item(?METRIC_QUEUE_MESSAGE_DROP, 1).

incr_queue_msg_expired(N) ->
    incr_item(?METRIC_QUEUE_MESSAGE_EXPIRED, N).

incr_queue_unhandled(N) ->
    incr_item(?METRIC_QUEUE_MESSAGE_UNHANDLED, N).

incr_queue_in() ->
    incr_item(?METRIC_QUEUE_MESSAGE_IN, 1).
incr_queue_in(N) ->
    incr_item(?METRIC_QUEUE_MESSAGE_IN, N).

incr_queue_out(N) ->
    incr_item(?METRIC_QUEUE_MESSAGE_OUT, N).

incr_cluster_bytes_received(V) ->
    incr_item(?METRIC_CLUSTER_BYTES_RECEIVED, V).

incr_cluster_bytes_sent(V) ->
    incr_item(?METRIC_CLUSTER_BYTES_SENT, V).

incr_cluster_bytes_dropped(V) ->
    incr_item(?METRIC_CLUSTER_BYTES_DROPPED, V).

incr_router_matches_local(V) ->
    incr_item(?METRIC_ROUTER_MATCHES_LOCAL, V).

incr_router_matches_remote(V) ->
    incr_item(?METRIC_ROUTER_MATCHES_REMOTE, V).

incr(Entry) ->
    incr_item(Entry, 1).

incr(Entry, N) ->
    incr_item(Entry, N).

incr_item(_, 0) -> ok; %% don't do the update
incr_item(Entry, Val) when Val > 0->
    ARef = case get(atomics_ref) of
               undefined ->
                   Ref = persistent_term:get(?MODULE),
                   put(atomics_ref, Ref),
                   Ref;
               Ref ->
                   Ref
           end,
    atomics:add(ARef, met2idx(Entry), Val).

%% true means current rate is ok.
check_rate(_, 0) -> true; % 0 means unlimited
check_rate(RateEntry, MaxRate) ->
    ARef = persistent_term:get(?MODULE),
    atomics:get(ARef, met2idx(RateEntry)) < MaxRate.

counter_val(Entry) ->
    ARef = persistent_term:get(?MODULE),
    atomics:get(ARef, met2idx(Entry)).

reset_counters() ->
    lists:foreach(
      fun(#metric_def{id = Entry}) ->
              reset_counter(Entry)
      end, internal_defs()).

reset_counter(Entry) ->
    ARef = persistent_term:get(?MODULE),
    atomics:put(ARef, met2idx(Entry), 0).

reset_counter(Entry, InitVal) ->
    reset_counter(Entry),
    incr_item(Entry, InitVal).

internal_values(MetricDefs) ->
    lists:map(
      fun(#metric_def{id=Id}) ->
              try counter_val(Id) of
                  Value -> {Id, Value}
              catch
                  _:_ -> {Id, 0}
              end
      end,
      MetricDefs).

-spec metrics() -> [{metric_def(), non_neg_integer()}].
metrics() ->
    metrics(#{aggregate => true}).

metrics(Opts) ->
    WantLabels = maps:get(labels, Opts, []),

    {PluggableMetricDefs, PluggableMetricValues} = pluggable_metrics(),

    MetricDefs = metric_defs() ++ PluggableMetricDefs,
    MetricValues = metric_values() ++ PluggableMetricValues,

    %% Create id->metric def map
    IdDef = lists:foldl(
      fun(#metric_def{id=Id}=MD, Acc) ->
              maps:put(Id, MD, Acc)
      end, #{}, MetricDefs),

    %% Merge metrics definitions with values and filter based on labels.
    Metrics = lists:filtermap(
      fun({Id, Val}) ->
              case maps:find(Id, IdDef) of
                  {ok, #metric_def{labels = GotLabels} = Def} ->
                      Keep = has_label(WantLabels, GotLabels),
                      case Keep of
                          true ->
                              {true, {Def, Val}};
                          _ ->
                              false
                      end;
                  error ->
                      %% this could happen if metric definitions does
                      %% not correspond to the ids returned with the
                      %% metrics values.
                      lager:warning("unknown metrics id: ~p", [Id]),
                      false
              end
      end, MetricValues),

    case Opts of
        #{aggregate := true} ->
            aggregate_by_name(Metrics);
        _ ->
            Metrics
    end.

pluggable_metric_defs() ->
    {Defs, _} = pluggable_metrics(),
    Defs.

pluggable_metrics() ->
    lists:foldl(
      fun({App, _Name, _Version}, Acc) ->
              case application:get_env(App, vmq_metrics_mfa, undefined) of
                  undefined -> Acc;
                  {Mod, Fun, Args}
                    when is_atom(Mod) and is_atom(Fun) and is_list(Args) ->
                      try
                          Metrics = apply(Mod, Fun, Args),
                          lists:foldl(
                            fun({Type, Labels, UniqueId, Name, Description, Value}, {DefsAcc, ValsAcc}) ->
                                    {[m(Type, Labels, UniqueId, Name, Description) | DefsAcc],
                                     [{UniqueId, Value} | ValsAcc]}
                            end, Acc, Metrics)
                      catch
                          _:_ ->
                              Acc
                      end
              end
      end, {[], []}, application:which_applications()).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec register(metric_def()) -> ok | {error, any()}.
register(MetricDef) ->
    gen_server:call(?MODULE, {register, MetricDef}).

get_label_info() ->
    LabelInfo=
    lists:foldl(
      fun(#metric_def{labels = Labels}, Acc0) ->
              lists:foldl(
                fun({LabelName, Val}, AccAcc) ->
                        case AccAcc of
                            #{LabelName := Vals} ->
                                case lists:member(Val, Vals) of
                                           true ->
                                        AccAcc;
                                    false ->
                                        maps:put(LabelName, [Val|Vals], AccAcc)
                                end;
                            _ ->
                                maps:put(LabelName, [Val], AccAcc)
                        end
                end, Acc0, Labels)
      end, #{}, metric_defs() ++ pluggable_metric_defs()),
    maps:to_list(LabelInfo).

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
    {ok, TRef} = timer:send_interval(1000, calc_rates),
    put(calc_rates_interval, TRef),
    {RateEntries, _} = lists:unzip(rate_entries()),
    AllEntries = RateEntries ++
          [Id || #metric_def{id = Id} <- internal_defs()],
    NumEntries = length(AllEntries),

    %% Sanity check where it is checked that there is a one-to-one
    %% mapping between atomics indexes and metrics identifiers by 1)
    %% checking that all metric identifiers have an atomics index and
    %% 2) that there are as many indexes as there are metrics and 3)
    %% that there are no index duplicates.
    Idxs = lists:map(fun(Id) -> met2idx(Id) end, AllEntries),
    NumEntries = length(lists:sort(Idxs)),
    NumEntries = length(lists:usort(Idxs)),

    %% only alloc a new atomics array if one doesn't already exist!
    case catch persistent_term:get(?MODULE) of
        {'EXIT', {badarg,_}} ->
            %% allocate twice the number of entries to make it possible to add
            %% new metrics during a hot code upgrade.
            ARef = atomics:new(2*NumEntries, [{signed, false}]),
            persistent_term:put(?MODULE, ARef);
        _ExistingRef ->
            ok
    end,
    MetricsInfo = register_metrics(#{}),
    {ok, #state{info=MetricsInfo}}.

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
-ifdef(TEST).
handle_call({register, Metric}, _From, #state{info=Metrics0} = State) ->
    case register_metric(Metric, Metrics0) of
        {ok, Metrics1} ->
            {reply, ok, State#state{info=Metrics1}};
        {error, _} = Err ->
            {reply, Err, State}
    end;
handle_call(clear_rates, _From, #state{} = State) ->
    %% clear stored rates in process dictionary
    lists:foreach(fun({Key, _}) ->
        case Key of
            {rate, _} = V -> erase(V);
            _ -> ok
        end
    end, get()),
    %% clear rate entries in atomics
    lists:foreach(
        fun({RateEntry, _Entries}) -> reset_counter(RateEntry) end,
        rate_entries()),
    {reply, ok, State};
handle_call(start_calc_rates, _From, #state{} = State) ->
    {ok, TRef} = timer:send_interval(1000, calc_rates),
    put(calc_rates_interval, TRef),
    {reply, ok, State};
handle_call(cancel_calc_rates, _From, #state{} = State) ->
    Interval = erase(calc_rates_interval),
    timer:cancel(Interval),
    {reply, ok, State};
handle_call(_Req, _From, #state{} = State) ->
    Reply = ok,
    {reply, Reply, State}.
-else.
handle_call({register, Metric}, _From, #state{info=Metrics0} = State) ->
    case register_metric(Metric, Metrics0) of
        {ok, Metrics1} ->
            {reply, ok, State#state{info=Metrics1}};
        {error, _} = Err ->
            {reply, Err, State}
    end;
handle_call(_Req, _From, #state{} = State) ->
    Reply = ok,
    {reply, Reply, State}.
-endif.
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
              fun({RateEntry, Entries}) -> calc_rate_per_conn(RateEntry, Entries, V) end,
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
%
%%===================================================================
%%% Internal functions
%%%===================================================================
calc_rate_per_conn(REntry, _Entries, 0) ->
    reset_counter(REntry);
calc_rate_per_conn(REntry, Entries, N) ->
    Val = lists:sum(
            [counter_val_since_last_call(Entry) || Entry <- Entries]),
    case Val of
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

m(Type, Labels, UniqueId, Name, Description) ->
    #metric_def{
       type = Type,
       labels = Labels,
       id = UniqueId,
       name = Name,
       description = Description}.

register_metric(#metric_def{id=Id}=Metric, Metrics) ->
    case Metrics of
        #{Id := _} ->
            {error, already_registered};
        _ ->
            {ok, maps:put(Id, Metric, Metrics)}
    end.

register_metrics(Metrics) ->
    lists:foldl(
      fun(#metric_def{} = MetricDef, Acc) ->
              {ok, Acc0} = register_metric(MetricDef, Acc),
              Acc0
      end,
      Metrics,
      metric_defs()). % no need to register pluggable_metric_defs

has_label([], _) ->
    true;
has_label(WantLabels, GotLabels) when is_list(WantLabels), is_list(GotLabels) ->
    lists:all(
      fun(T1) ->
              lists:member(T1, GotLabels)
      end, WantLabels).

aggregate_by_name(Metrics) ->
    AggrMetrics = lists:foldl(
      fun({#metric_def{name = Name} = D1, V1}, Acc) ->
              case maps:find(Name, Acc) of
                  {ok, {_D2, V2}} ->
                      Acc#{Name => {D1#metric_def{labels=[]}, V1 + V2}};
                  error ->
                      %% Remove labels when aggregating.
                      Acc#{Name => {D1#metric_def{labels=[]}, V1}}
              end
      end, #{}, Metrics),
    maps:values(AggrMetrics).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%% VerneMQ metrics definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

metric_defs() ->
    flatten([system_stats_def(), misc_stats_def(),internal_defs()], []).

metric_values() ->
    flatten([system_statistics(),
             misc_statistics(),internal_values(internal_defs())], []).

internal_defs() ->
    flatten([counter_entries_def(), mqtt4_connack_sent_def(),
             mqtt5_disconnect_recv_def(), mqtt5_disconnect_sent_def(),
             mqtt5_connack_sent_def(),
             mqtt5_puback_sent_def(), mqtt5_puback_received_def(),
             mqtt5_pubrec_sent_def(), mqtt5_pubrec_received_def(),
             mqtt5_pubrel_sent_def(), mqtt5_pubrel_received_def(),
             mqtt5_pubcomp_sent_def(), mqtt5_pubcomp_received_def(),
             mqtt5_auth_sent_def(), mqtt5_auth_received_def()
            ], []).

counter_entries_def() ->
    [
     m(counter, [], socket_open, socket_open, <<"The number of times an MQTT socket has been opened.">>),
     m(counter, [], socket_close, socket_close, <<"The number of times an MQTT socket has been closed.">>),
     m(counter, [], socket_close_timeout, socket_close_timeout, <<"The number of times VerneMQ closed an MQTT socket due to no CONNECT frame has been received on time.">>),
     m(counter, [], socket_error, socket_error, <<"The total number of socket errors that have occurred.">>),
     m(counter, [], bytes_received, bytes_received, <<"The total number of bytes received.">>),
     m(counter, [], bytes_sent, bytes_sent, <<"The total number of bytes sent.">>),

     m(counter, [{mqtt_version,"4"}], mqtt_connect_received, mqtt_connect_received, <<"The number of CONNECT packets received.">>),
     m(counter, [{mqtt_version,"4"}], mqtt_publish_received, mqtt_publish_received, <<"The number of PUBLISH packets received.">>),
     m(counter, [{mqtt_version,"4"}], mqtt_puback_received, mqtt_puback_received, <<"The number of PUBACK packets received.">>),
     m(counter, [{mqtt_version,"4"}], mqtt_pubrec_received, mqtt_pubrec_received, <<"The number of PUBREC packets received.">>),
     m(counter, [{mqtt_version,"4"}], mqtt_pubrel_received, mqtt_pubrel_received, <<"The number of PUBREL packets received.">>),
     m(counter, [{mqtt_version,"4"}], mqtt_pubcomp_received, mqtt_pubcomp_received, <<"The number of PUBCOMP packets received.">>),
     m(counter, [{mqtt_version,"4"}], mqtt_subscribe_received, mqtt_subscribe_received, <<"The number of SUBSCRIBE packets received.">>),
     m(counter, [{mqtt_version,"4"}], mqtt_unsubscribe_received, mqtt_unsubscribe_received, <<"The number of UNSUBSCRIBE packets received.">>),
     m(counter, [{mqtt_version,"4"}], mqtt_pingreq_received, mqtt_pingreq_received, <<"The number of PINGREQ packets received.">>),
     m(counter, [{mqtt_version,"4"}], mqtt_disconnect_received, mqtt_disconnect_received, <<"The number of DISCONNECT packets received.">>),
     m(counter, [{mqtt_version,"4"}], mqtt_connack_accepted_sent, mqtt_connack_accepted_sent, <<"The number of times a connection has been accepted.">>),
     m(counter, [{mqtt_version,"4"}], mqtt_connack_unacceptable_protocol_sent, mqtt_connack_unacceptable_protocol_sent, <<"The number of times the broker is not able to support the requested protocol.">>),
     m(counter, [{mqtt_version,"4"}], mqtt_connack_identifier_rejected_sent, mqtt_connack_identifier_rejected_sent, <<"The number of times a client was rejected due to a unacceptable identifier.">>),
     m(counter, [{mqtt_version,"4"}], mqtt_connack_server_unavailable_sent, mqtt_connack_server_unavailable_sent, <<"The number of times a client was rejected due the the broker being unavailable.">>),
     m(counter, [{mqtt_version,"4"}], mqtt_connack_bad_credentials_sent, mqtt_connack_bad_credentials_sent, <<"The number of times a client sent bad credentials.">>),
     m(counter, [{mqtt_version,"4"}], mqtt_connack_not_authorized_sent, mqtt_connack_not_authorized_sent, <<"The number of times a client was rejected due to insufficient authorization.">>),
     m(counter, [{mqtt_version,"4"}], mqtt_publish_sent, mqtt_publish_sent, <<"The number of PUBLISH packets sent.">>),
     m(counter, [{mqtt_version,"4"}], mqtt_puback_sent, mqtt_puback_sent, <<"The number of PUBACK packets sent.">>),
     m(counter, [{mqtt_version,"4"}], mqtt_pubrec_sent, mqtt_pubrec_sent, <<"The number of PUBREC packets sent.">>),
     m(counter, [{mqtt_version,"4"}], mqtt_pubrel_sent, mqtt_pubrel_sent, <<"The number of PUBREL packets sent.">>),
     m(counter, [{mqtt_version,"4"}], mqtt_pubcomp_sent, mqtt_pubcomp_sent, <<"The number of PUBCOMP packets sent.">>),
     m(counter, [{mqtt_version,"4"}], mqtt_suback_sent, mqtt_suback_sent, <<"The number of SUBACK packets sent.">>),
     m(counter, [{mqtt_version,"4"}], mqtt_unsuback_sent, mqtt_unsuback_sent, <<"The number of UNSUBACK packets sent.">>),
     m(counter, [{mqtt_version,"4"}], mqtt_pingresp_sent, mqtt_pingresp_sent, <<"The number of PINGRESP packets sent.">>),
     m(counter, [{mqtt_version,"4"}], mqtt_publish_auth_error, mqtt_publish_auth_error, <<"The number of unauthorized publish attempts.">>),
     m(counter, [{mqtt_version,"4"}], mqtt_subscribe_auth_error, mqtt_subscribe_auth_error, <<"The number of unauthorized subscription attempts.">>),
     m(counter, [{mqtt_version,"4"}], mqtt_invalid_msg_size_error, mqtt_invalid_msg_size_error, <<"The number of packages exceeding the maximum allowed size.">>),
     m(counter, [{mqtt_version,"4"}], mqtt_puback_invalid_error, mqtt_puback_invalid_error, <<"The number of unexpected PUBACK messages received.">>),
     m(counter, [{mqtt_version,"4"}], mqtt_pubrec_invalid_error, mqtt_pubrec_invalid_error, <<"The number of unexpected PUBREC messages received.">>),
     m(counter, [{mqtt_version,"4"}], mqtt_pubcomp_invalid_error, mqtt_pubcomp_invalid_error, <<"The number of unexpected PUBCOMP messages received.">>),
     m(counter, [{mqtt_version,"4"}], mqtt_publish_error, mqtt_publish_error, <<"The number of times a PUBLISH operation failed due to a netsplit.">>),
     m(counter, [{mqtt_version,"4"}], mqtt_subscribe_error, mqtt_subscribe_error, <<"The number of times a SUBSCRIBE operation failed due to a netsplit.">>),
     m(counter, [{mqtt_version,"4"}], mqtt_unsubscribe_error, mqtt_unsubscribe_error, <<"The number of times an UNSUBSCRIBE operation failed due to a netsplit.">>),
     m(counter, [{mqtt_version,"4"}], ?MQTT4_CLIENT_KEEPALIVE_EXPIRED, client_keepalive_expired, <<"The number of clients which failed to communicate within the keepalive time period.">>),

     m(counter, [{mqtt_version,"5"}], ?MQTT5_CONNECT_RECEIVED, mqtt_connect_received, <<"The number of CONNECT packets received.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_INVALID_MSG_SIZE_ERROR, mqtt_invalid_msg_size_error, <<"The number of packages exceeding the maximum allowed size.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_PINGREQ_RECEIVED, mqtt_pingreq_received, <<"The number of PINGREQ packets received.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_PINGRESP_SENT, mqtt_pingresp_sent, <<"The number of PINGRESP packets sent.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_PUBACK_INVALID_ERROR, mqtt_puback_invalid_error, <<"The number of unexpected PUBACK messages received.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_PUBCOMP_INVALID_ERROR, mqtt_pubcomp_invalid_error, <<"The number of unexpected PUBCOMP messages received.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_PUBLISH_AUTH_ERROR, mqtt_publish_auth_error, <<"The number of unauthorized publish attempts.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_PUBLISH_ERROR, mqtt_publish_error, <<"The number of times a PUBLISH operation failed due to a netsplit.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_PUBLISH_RECEIVED, mqtt_publish_received, <<"The number of PUBLISH packets received.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_PUBLISH_SENT, mqtt_publish_sent, <<"The number of PUBLISH packets sent.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_SUBACK_SENT, mqtt_suback_sent, <<"The number of SUBACK packets sent.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_SUBSCRIBE_AUTH_ERROR, mqtt_subscribe_auth_error, <<"The number of unauthorized subscription attempts.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_SUBSCRIBE_ERROR, mqtt_subscribe_error, <<"The number of times a SUBSCRIBE operation failed due to a netsplit.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_SUBSCRIBE_RECEIVED, mqtt_subscribe_received, <<"The number of SUBSCRIBE packets received.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_UNSUBACK_SENT, mqtt_unsuback_sent, <<"The number of UNSUBACK packets sent.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_UNSUBSCRIBE_ERROR, mqtt_unsubscribe_error, <<"The number of times an UNSUBSCRIBE operation failed due to a netsplit.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_UNSUBSCRIBE_RECEIVED, mqtt_unsubscribe_received, <<"The number of UNSUBSCRIBE packets received.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_CLIENT_KEEPALIVE_EXPIRED, client_keepalive_expired, <<"The number of clients which failed to communicate within the keepalive time period.">>),

     m(counter, [], queue_setup, queue_setup, <<"The number of times a MQTT queue process has been started.">>),
     m(counter, [], queue_initialized_from_storage, queue_initialized_from_storage, <<"The number of times a MQTT queue process has been initialized from offline storage.">>),
     m(counter, [], queue_teardown, queue_teardown, <<"The number of times a MQTT queue process has been terminated.">>),
     m(counter, [], queue_message_drop, queue_message_drop, <<"The number of messages dropped due to full queues.">>),
     m(counter, [], queue_message_expired, queue_message_expired, <<"The number of messages which expired before delivery.">>),
     m(counter, [], queue_message_unhandled, queue_message_unhandled, <<"The number of unhandled messages when connecting with clean session=true.">>),
     m(counter, [], queue_message_in, queue_message_in, <<"The number of PUBLISH packets received by MQTT queue processes.">>),
     m(counter, [], queue_message_out, queue_message_out, <<"The number of PUBLISH packets sent from MQTT queue processes.">>),
     m(counter, [], client_expired, client_expired, <<"Not in use (deprecated)">>),
     m(counter, [], cluster_bytes_received, cluster_bytes_received, <<"The number of bytes received from other cluster nodes.">>),
     m(counter, [], cluster_bytes_sent, cluster_bytes_sent, <<"The number of bytes send to other cluster nodes.">>),
     m(counter, [], cluster_bytes_dropped, cluster_bytes_dropped, <<"The number of bytes dropped while sending data to other cluster nodes.">>),
     m(counter, [], router_matches_local, router_matches_local, <<"The number of matched local subscriptions.">>),
     m(counter, [], router_matches_remote, router_matches_remote, <<"The number of matched remote subscriptions.">>)
    ].


flatten([], Acc) -> Acc;
flatten([[H|[]]|T1], Acc) when is_tuple(H) ->
    flatten(T1, [H|Acc]);
flatten([[H|T0]|T1], Acc) when is_tuple(H) ->
    flatten([T0|T1], [H|Acc]);
flatten([H|T], Acc) ->
    flatten(T, [H|Acc]).

rcn_to_str(RNC) ->
    %% TODO: replace this with a real textual representation
    atom_to_list(RNC).

mqtt5_disconnect_recv_def() ->
    RCNs =
        [
         ?NORMAL_DISCONNECT,
         ?DISCONNECT_WITH_WILL_MSG,
         ?UNSPECIFIED_ERROR,
         ?MALFORMED_PACKET,
         ?PROTOCOL_ERROR,
         ?IMPL_SPECIFIC_ERROR,
         ?TOPIC_NAME_INVALID,
         ?RECEIVE_MAX_EXCEEDED,
         ?TOPIC_ALIAS_INVALID,
         ?PACKET_TOO_LARGE,
         ?MESSAGE_RATE_TOO_HIGH,
         ?QUOTA_EXCEEDED,
         ?ADMINISTRATIVE_ACTION,
         ?PAYLOAD_FORMAT_INVALID],
    [m(counter, [{mqtt_version,"5"},{reason_code, rcn_to_str(RCN)}],
       {?MQTT5_DISCONNECT_RECEIVED, RCN}, mqtt_disconnect_received,
       <<"The number of DISCONNECT packets received.">>) || RCN <- RCNs].

mqtt5_disconnect_sent_def() ->
    RCNs =
        [
         ?NORMAL_DISCONNECT,
         ?UNSPECIFIED_ERROR,
         ?MALFORMED_PACKET,
         ?PROTOCOL_ERROR,
         ?IMPL_SPECIFIC_ERROR,
         ?NOT_AUTHORIZED,
         ?SERVER_BUSY,
         ?SERVER_SHUTTING_DOWN,
         ?KEEP_ALIVE_TIMEOUT,
         ?SESSION_TAKEN_OVER,
         ?TOPIC_FILTER_INVALID,
         ?TOPIC_NAME_INVALID,
         ?RECEIVE_MAX_EXCEEDED,
         ?TOPIC_ALIAS_INVALID,
         ?PACKET_TOO_LARGE,
         ?MESSAGE_RATE_TOO_HIGH,
         ?QUOTA_EXCEEDED,
         ?ADMINISTRATIVE_ACTION,
         ?PAYLOAD_FORMAT_INVALID,
         ?RETAIN_NOT_SUPPORTED,
         ?QOS_NOT_SUPPORTED,
         ?USE_ANOTHER_SERVER,
         ?SERVER_MOVED,
         ?SHARED_SUBS_NOT_SUPPORTED,
         ?CONNECTION_RATE_EXCEEDED,
         ?MAX_CONNECT_TIME,
         ?SUBSCRIPTION_IDS_NOT_SUPPORTED,
         ?WILDCARD_SUBS_NOT_SUPPORTED],
    [m(counter, [{mqtt_version,"5"},{reason_code, rcn_to_str(RCN)}],
       {?MQTT5_DISCONNECT_SENT, RCN}, mqtt_disconnect_sent,
       <<"The number of DISCONNECT packets sent.">>) || RCN <- RCNs].

mqtt5_connack_sent_def() ->
    RCNs =
        [?SUCCESS,
         ?UNSPECIFIED_ERROR,
         ?MALFORMED_PACKET,
         ?PROTOCOL_ERROR,
         ?IMPL_SPECIFIC_ERROR,
         ?UNSUPPORTED_PROTOCOL_VERSION,
         ?CLIENT_IDENTIFIER_NOT_VALID,
         ?BAD_USERNAME_OR_PASSWORD,
         ?NOT_AUTHORIZED,
         ?SERVER_UNAVAILABLE,
         ?SERVER_BUSY,
         ?BANNED,
         ?BAD_AUTHENTICATION_METHOD,
         ?TOPIC_NAME_INVALID,
         ?PACKET_TOO_LARGE,
         ?QUOTA_EXCEEDED,
         ?PAYLOAD_FORMAT_INVALID,
         ?RETAIN_NOT_SUPPORTED,
         ?QOS_NOT_SUPPORTED,
         ?USE_ANOTHER_SERVER,
         ?SERVER_MOVED,
         ?CONNECTION_RATE_EXCEEDED],
    [m(counter, [{mqtt_version,"5"},{reason_code, rcn_to_str(RCN)}],
       {?MQTT5_CONNACK_SENT, RCN}, mqtt_connack_sent,
       <<"The number of CONNACK packets sent.">>) || RCN <- RCNs].

mqtt5_puback_sent_def() ->
    RCNs = [?SUCCESS,
            ?NO_MATCHING_SUBSCRIBERS,
            ?UNSPECIFIED_ERROR,
            ?IMPL_SPECIFIC_ERROR,
            ?NOT_AUTHORIZED,
            ?TOPIC_NAME_INVALID,
            ?PACKET_ID_IN_USE,
            ?QUOTA_EXCEEDED,
            ?PAYLOAD_FORMAT_INVALID],
    [m(counter, [{mqtt_version,"5"},{reason_code, rcn_to_str(RCN)}],
       {?MQTT5_PUBACK_SENT, RCN}, mqtt_puback_sent,
       <<"The number of PUBACK packets sent.">>) || RCN <- RCNs].

mqtt5_puback_received_def() ->
    RCNs = [?SUCCESS,
            ?NO_MATCHING_SUBSCRIBERS,
            ?UNSPECIFIED_ERROR,
            ?IMPL_SPECIFIC_ERROR,
            ?NOT_AUTHORIZED,
            ?TOPIC_NAME_INVALID,
            ?PACKET_ID_IN_USE,
            ?QUOTA_EXCEEDED,
            ?PAYLOAD_FORMAT_INVALID],
    [m(counter, [{mqtt_version,"5"},{reason_code, rcn_to_str(RCN)}],
       {?MQTT5_PUBACK_RECEIVED, RCN}, mqtt_puback_received,
       <<"The number of PUBACK packets received.">>) || RCN <- RCNs].

mqtt5_pubrec_sent_def() ->
    RCNs = [?SUCCESS,
            ?NO_MATCHING_SUBSCRIBERS,
            ?UNSPECIFIED_ERROR,
            ?IMPL_SPECIFIC_ERROR,
            ?NOT_AUTHORIZED,
            ?TOPIC_NAME_INVALID,
            ?PACKET_ID_IN_USE,
            ?QUOTA_EXCEEDED,
            ?PAYLOAD_FORMAT_INVALID],
    [m(counter, [{mqtt_version,"5"},{reason_code, rcn_to_str(RCN)}],
       {?MQTT5_PUBREC_SENT, RCN}, mqtt_pubrec_sent,
       <<"The number of PUBREC packets sent.">>) || RCN <- RCNs].

mqtt5_pubrec_received_def() ->
    RCNs = [?SUCCESS,
            ?NO_MATCHING_SUBSCRIBERS,
            ?UNSPECIFIED_ERROR,
            ?IMPL_SPECIFIC_ERROR,
            ?NOT_AUTHORIZED,
            ?TOPIC_NAME_INVALID,
            ?PACKET_ID_IN_USE,
            ?QUOTA_EXCEEDED,
            ?PAYLOAD_FORMAT_INVALID],
    [m(counter, [{mqtt_version,"5"},{reason_code, rcn_to_str(RCN)}],
       {?MQTT5_PUBREC_RECEIVED, RCN}, mqtt_pubrec_received,
       <<"The number of PUBREC packets received.">>) || RCN <- RCNs].

mqtt5_pubrel_sent_def() ->
    RCNs = [?SUCCESS,
            ?PACKET_ID_NOT_FOUND],
    [m(counter, [{mqtt_version,"5"},{reason_code, rcn_to_str(RCN)}],
       {?MQTT5_PUBREL_SENT, RCN}, mqtt_pubrel_sent,
       <<"The number of PUBREL packets sent.">>) || RCN <- RCNs].

mqtt5_pubrel_received_def() ->
    RCNs = [?SUCCESS,
            ?PACKET_ID_NOT_FOUND],
    [m(counter, [{mqtt_version,"5"},{reason_code, rcn_to_str(RCN)}],
       {?MQTT5_PUBREL_RECEIVED, RCN}, mqtt_pubrel_received,
       <<"The number of PUBREL packets received.">>) || RCN <- RCNs].

mqtt5_pubcomp_sent_def() ->
    RCNs = [?SUCCESS,
            ?PACKET_ID_NOT_FOUND],
    [m(counter, [{mqtt_version,"5"},{reason_code, rcn_to_str(RCN)}],
       {?MQTT5_PUBCOMP_SENT, RCN}, mqtt_pubcomp_sent,
       <<"The number of PUBCOMP packets sent.">>) || RCN <- RCNs].

mqtt5_pubcomp_received_def() ->
    RCNs = [?SUCCESS,
            ?PACKET_ID_NOT_FOUND],
    [m(counter, [{mqtt_version,"5"},{reason_code, rcn_to_str(RCN)}],
       {?MQTT5_PUBCOMP_RECEIVED, RCN}, mqtt_pubcomp_received,
       <<"The number of PUBCOMP packets received.">>) || RCN <- RCNs].

mqtt5_auth_sent_def() ->
    RCNs = [?SUCCESS,
            ?CONTINUE_AUTHENTICATION,
            ?REAUTHENTICATE],
    [m(counter, [{mqtt_version,"5"},{reason_code, rcn_to_str(RCN)}],
       {?MQTT5_AUTH_SENT, RCN}, mqtt_auth_sent,
       <<"The number of AUTH packets sent.">>) || RCN <- RCNs].

mqtt5_auth_received_def() ->
    RCNs = [?SUCCESS,
            ?CONTINUE_AUTHENTICATION,
            ?REAUTHENTICATE],
    [m(counter, [{mqtt_version,"5"},{reason_code, rcn_to_str(RCN)}],
       {?MQTT5_AUTH_RECEIVED, RCN}, mqtt_auth_received,
       <<"The number of AUTH packets received.">>) || RCN <- RCNs].

m4_connack_labels(?CONNACK_ACCEPT) ->
    rcn_to_str(?SUCCESS);
m4_connack_labels(?CONNACK_PROTO_VER) ->
    rcn_to_str(?UNSUPPORTED_PROTOCOL_VERSION);
m4_connack_labels(?CONNACK_INVALID_ID) ->
    rcn_to_str(?CLIENT_IDENTIFIER_NOT_VALID);
m4_connack_labels(?CONNACK_SERVER) ->
    rcn_to_str(?SERVER_UNAVAILABLE);
m4_connack_labels(?CONNACK_CREDENTIALS) ->
    rcn_to_str(?BAD_USERNAME_OR_PASSWORD);
m4_connack_labels(?CONNACK_AUTH) ->
    rcn_to_str(?NOT_AUTHORIZED).

mqtt4_connack_sent_def() ->
    RCNs = [?CONNACK_ACCEPT,
            ?CONNACK_PROTO_VER,
            ?CONNACK_INVALID_ID,
            ?CONNACK_SERVER,
            ?CONNACK_CREDENTIALS,
            ?CONNACK_AUTH],
    [m(counter, [{mqtt_version,"4"},{return_code, m4_connack_labels(RCN)}],
       {?MQTT4_CONNACK_SENT, RCN}, mqtt_connack_sent,
       <<"The number of CONNACK packets sent.">>) || RCN <- RCNs].

rate_entries() ->
    [{?METRIC_MSG_IN_RATE, [?MQTT4_PUBLISH_RECEIVED, ?MQTT5_PUBLISH_RECEIVED]},
     {?METRIC_BYTE_IN_RATE, [?METRIC_BYTES_RECEIVED]},
     {?METRIC_MSG_OUT_RATE, [?MQTT4_PUBLISH_SENT, ?MQTT5_PUBLISH_SENT]},
     {?METRIC_BYTE_OUT_RATE, [?METRIC_BYTES_SENT]}].

fetch_external_metric(Mod, Fun, Default) ->
    % safe-guard call to external metric provider
    % as it it possible that the metric provider
    % isn't ready yet.
    try
        apply(Mod, Fun, [])
    catch
        ErrorClass:Reason ->
            lager:warning("can't fetch metrics from ~p", [Mod]),
            lager:debug("fetching metrics from ~p resulted in ~p with reason ~p", [Mod, ErrorClass, Reason]),
            Default
    end.

-spec misc_statistics() -> [{metric_id(), any()}].
misc_statistics() ->
    {NrOfSubs, SMemory} = fetch_external_metric(vmq_reg_trie, stats, {0, 0}),
    {NrOfRetain, RMemory} = fetch_external_metric(vmq_retain_srv, stats, {0, 0}),
    {NetsplitDetectedCount, NetsplitResolvedCount}
        = fetch_external_metric(vmq_cluster, netsplit_statistics, {0, 0}),
    [{netsplit_detected, NetsplitDetectedCount},
     {netsplit_resolved, NetsplitResolvedCount},
     {router_subscriptions, NrOfSubs},
     {router_memory, SMemory},
     {retain_messages, NrOfRetain},
     {retain_memory, RMemory},
     {queue_processes, fetch_external_metric(vmq_queue_sup_sup, nr_of_queues, 0)}].

-spec misc_stats_def() -> [metric_def()].
misc_stats_def() ->
    [m(counter, [], netsplit_detected, netsplit_detected, <<"The number of detected netsplits.">>),
     m(counter, [], netsplit_resolved, netsplit_resolved, <<"The number of resolved netsplits.">>),
     m(gauge, [], router_subscriptions, router_subscriptions, <<"The number of subscriptions in the routing table.">>),
     m(gauge, [], router_memory, router_memory, <<"The number of bytes used by the routing table.">>),
     m(gauge, [], retain_messages, retain_messages, <<"The number of currently stored retained messages.">>),
     m(gauge, [], retain_memory, retain_memory, <<"The number of bytes used for storing retained messages.">>),
     m(gauge, [], queue_processes, queue_processes, <<"The number of MQTT queue processes.">>)].

-spec system_statistics() -> [{metric_id(), any()}].
system_statistics() ->
    {ContextSwitches, _} = erlang:statistics(context_switches),
    {TotalExactReductions, _} = erlang:statistics(exact_reductions),
    {Number_of_GCs, Words_Reclaimed, 0} = erlang:statistics(garbage_collection),
    {{input, Input}, {output, Output}} = erlang:statistics(io),
    {Total_Reductions, _} = erlang:statistics(reductions),
    RunQueueLen = erlang:statistics(run_queue),
    {Total_Run_Time, _} = erlang:statistics(runtime),
    {Total_Wallclock_Time, _} = erlang:statistics(wall_clock),
    ProcessCount = erlang:system_info(process_count),
    #{total := ErlangMemTotal,
      processes := ErlangMemProcesses,
      processes_used := ErlangMemProcessesUsed,
      system := ErlangMemSystem,
      atom := ErlangMemAtom,
      atom_used := ErlangMemAtomUsed,
      binary := ErlangMemBinary,
      code := ErlangMemCode,
      ets := ErlangMemEts} = maps:from_list(erlang:memory()),
    [{system_context_switches, ContextSwitches},
     {system_exact_reductions, TotalExactReductions},
     {system_gc_count, Number_of_GCs},
     {system_words_reclaimed_by_gc, Words_Reclaimed},
     {system_io_in, Input},
     {system_io_out, Output},
     {system_reductions, Total_Reductions},
     {system_run_queue, RunQueueLen},
     {system_runtime, Total_Run_Time},
     {system_wallclock, Total_Wallclock_Time},
     {system_process_count, ProcessCount},

     {vm_memory_total, ErlangMemTotal},
     {vm_memory_processes, ErlangMemProcesses},
     {vm_memory_processes_used, ErlangMemProcessesUsed},
     {vm_memory_system, ErlangMemSystem},
     {vm_memory_atom, ErlangMemAtom},
     {vm_memory_atom_used, ErlangMemAtomUsed},
     {vm_memory_binary, ErlangMemBinary},
     {vm_memory_code, ErlangMemCode},
     {vm_memory_ets, ErlangMemEts}|
     scheduler_utilization()].

system_stats_def() ->
    [
     m(counter, [], system_context_switches, system_context_switches, <<"The total number of context switches.">>),
     m(counter, [], system_exact_reductions, system_exact_reductions, <<"The exact number of reductions performed.">>),
     m(counter, [], system_gc_count, system_gc_count, <<"The number of garbage collections performed.">>),
     m(counter, [], system_words_reclaimed_by_gc, system_words_reclaimed_by_gc, <<"The number of words reclaimed by the garbage collector.">>),
     m(counter, [], system_io_in, system_io_in, <<"The total number of bytes received through ports.">>),
     m(counter, [], system_io_out, system_io_out, <<"The total number of bytes sent through ports.">>),
     m(counter, [], system_reductions, system_reductions, <<"The number of reductions performed in the VM since the node was started.">>),
     m(gauge, [], system_run_queue, system_run_queue, <<"The total number of processes and ports ready to run on all run-queues.">>),
     m(counter, [], system_runtime, system_runtime, <<"The sum of the runtime for all threads in the Erlang runtime system.">>),
     m(counter, [], system_wallclock, system_wallclock, <<"The number of milli-seconds passed since the node was started.">>),
     m(gauge, [], system_process_count, system_process_count, <<"The number of Erlang processes.">>),

     m(gauge, [], vm_memory_total, vm_memory_total, <<"The total amount of memory allocated.">>),
     m(gauge, [], vm_memory_processes, vm_memory_processes, <<"The amount of memory allocated for processes.">>),
     m(gauge, [], vm_memory_processes_used, vm_memory_processes_used, <<"The amount of memory used by processes.">>),
     m(gauge, [], vm_memory_system, vm_memory_system, <<"The amount of memory allocated for the emulator.">>),
     m(gauge, [], vm_memory_atom, vm_memory_atom, <<"The amount of memory allocated for atoms.">>),
     m(gauge, [], vm_memory_atom_used, vm_memory_atom_used, <<"The amount of memory used by atoms.">>),
     m(gauge, [], vm_memory_binary, vm_memory_binary, <<"The amount of memory allocated for binaries.">>),
     m(gauge, [], vm_memory_code, vm_memory_code, <<"The amount of memory allocated for code.">>),
     m(gauge, [], vm_memory_ets, vm_memory_ets, <<"The amount of memory allocated for ETS tables.">>)|
     scheduler_utilization_def()].

-spec scheduler_utilization() -> [{metric_id(), any()}].
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
                                             StrName = "system_utilization_scheduler_" ++ integer_to_list(I),
                                             Id = list_to_atom(StrName),
                                             Val = round((100 * (A1 - A0)/(T1 - T0))),
                                             {Id, Val}
                                     end, lists:zip(WallTimeTs0, WallTimeTs1)),
    TotalSum = lists:foldl(fun({_MetricDef, UsagePerSchedu}, Sum) ->
                                   Sum + UsagePerSchedu
                           end, 0, SchedulerUtilization),
    AvgUtilization = round(TotalSum / length(SchedulerUtilization)),
    [{system_utilization, AvgUtilization}|
     SchedulerUtilization].

-spec scheduler_utilization_def() -> [metric_def()].
scheduler_utilization_def() ->
    DirtySchedulers =
        try
            %% not supported by default on OTP versions before 20.
            erlang:system_info(dirty_cpu_schedulers)
        catch
            error:badarg -> 0
        end,
    NumSchedulers = erlang:system_info(schedulers) + DirtySchedulers,
    SchedUtilDefs = lists:map(
      fun(I) ->
              StrName = "system_utilization_scheduler_" ++ integer_to_list(I),
              Number = integer_to_binary(I),
              Id = list_to_atom(StrName),
              Description = <<"Scheduler ", Number/binary, " utilization (percentage)">>,
              m(gauge, [], Id, Id, Description)
      end,
      lists:seq(1, NumSchedulers)),
    [m(gauge, [], system_utilization, system_utilization,
       <<"The average system (scheduler) utilization (percentage).">>)
     |SchedUtilDefs].

met2idx(?MQTT5_CONNECT_RECEIVED)                                  -> 1;
met2idx({?MQTT5_CONNACK_SENT, ?SUCCESS})                          -> 2;
met2idx({?MQTT5_CONNACK_SENT, ?UNSPECIFIED_ERROR})                -> 3;
met2idx({?MQTT5_CONNACK_SENT, ?MALFORMED_PACKET})                 -> 4;
met2idx({?MQTT5_CONNACK_SENT, ?PROTOCOL_ERROR})                   -> 5;
met2idx({?MQTT5_CONNACK_SENT, ?IMPL_SPECIFIC_ERROR})              -> 6;
met2idx({?MQTT5_CONNACK_SENT, ?UNSUPPORTED_PROTOCOL_VERSION})     -> 7;
met2idx({?MQTT5_CONNACK_SENT, ?CLIENT_IDENTIFIER_NOT_VALID})      -> 8;
met2idx({?MQTT5_CONNACK_SENT, ?BAD_USERNAME_OR_PASSWORD})         -> 9;
met2idx({?MQTT5_CONNACK_SENT, ?NOT_AUTHORIZED})                   -> 10;
met2idx({?MQTT5_CONNACK_SENT, ?SERVER_UNAVAILABLE})               -> 11;
met2idx({?MQTT5_CONNACK_SENT, ?SERVER_BUSY})                      -> 12;
met2idx({?MQTT5_CONNACK_SENT, ?BANNED})                           -> 13;
met2idx({?MQTT5_CONNACK_SENT, ?BAD_AUTHENTICATION_METHOD})        -> 14;
met2idx({?MQTT5_CONNACK_SENT, ?TOPIC_NAME_INVALID})               -> 15;
met2idx({?MQTT5_CONNACK_SENT, ?PACKET_TOO_LARGE})                 -> 16;
met2idx({?MQTT5_CONNACK_SENT, ?QUOTA_EXCEEDED})                   -> 17;
met2idx({?MQTT5_CONNACK_SENT, ?PAYLOAD_FORMAT_INVALID})           -> 18;
met2idx({?MQTT5_CONNACK_SENT, ?RETAIN_NOT_SUPPORTED})             -> 19;
met2idx({?MQTT5_CONNACK_SENT, ?QOS_NOT_SUPPORTED})                -> 20;
met2idx({?MQTT5_CONNACK_SENT, ?USE_ANOTHER_SERVER})               -> 21;
met2idx({?MQTT5_CONNACK_SENT, ?SERVER_MOVED})                     -> 22;
met2idx({?MQTT5_CONNACK_SENT, ?CONNECTION_RATE_EXCEEDED})         -> 23;
met2idx({?MQTT5_DISCONNECT_RECEIVED, ?NORMAL_DISCONNECT})         -> 24;
met2idx({?MQTT5_DISCONNECT_RECEIVED, ?DISCONNECT_WITH_WILL_MSG})  -> 25;
met2idx({?MQTT5_DISCONNECT_RECEIVED, ?UNSPECIFIED_ERROR})         -> 26;
met2idx({?MQTT5_DISCONNECT_RECEIVED, ?MALFORMED_PACKET})          -> 27;
met2idx({?MQTT5_DISCONNECT_RECEIVED, ?PROTOCOL_ERROR})            -> 28;
met2idx({?MQTT5_DISCONNECT_RECEIVED, ?IMPL_SPECIFIC_ERROR})       -> 29;
met2idx({?MQTT5_DISCONNECT_RECEIVED, ?TOPIC_NAME_INVALID})        -> 30;
met2idx({?MQTT5_DISCONNECT_RECEIVED, ?RECEIVE_MAX_EXCEEDED})      -> 31;
met2idx({?MQTT5_DISCONNECT_RECEIVED, ?TOPIC_ALIAS_INVALID})       -> 32;
met2idx({?MQTT5_DISCONNECT_RECEIVED, ?PACKET_TOO_LARGE})          -> 33;
met2idx({?MQTT5_DISCONNECT_RECEIVED, ?MESSAGE_RATE_TOO_HIGH})     -> 34;
met2idx({?MQTT5_DISCONNECT_RECEIVED, ?QUOTA_EXCEEDED})            -> 35;
met2idx({?MQTT5_DISCONNECT_RECEIVED, ?ADMINISTRATIVE_ACTION})     -> 36;
met2idx({?MQTT5_DISCONNECT_RECEIVED, ?PAYLOAD_FORMAT_INVALID})    -> 37;
met2idx({?MQTT5_DISCONNECT_SENT,?NORMAL_DISCONNECT})              -> 38;
met2idx({?MQTT5_DISCONNECT_SENT,?UNSPECIFIED_ERROR})              -> 39;
met2idx({?MQTT5_DISCONNECT_SENT,?MALFORMED_PACKET})               -> 40;
met2idx({?MQTT5_DISCONNECT_SENT,?PROTOCOL_ERROR})                 -> 41;
met2idx({?MQTT5_DISCONNECT_SENT,?IMPL_SPECIFIC_ERROR})            -> 42;
met2idx({?MQTT5_DISCONNECT_SENT,?NOT_AUTHORIZED})                 -> 43;
met2idx({?MQTT5_DISCONNECT_SENT,?SERVER_BUSY})                    -> 44;
met2idx({?MQTT5_DISCONNECT_SENT,?SERVER_SHUTTING_DOWN})           -> 45;
met2idx({?MQTT5_DISCONNECT_SENT,?KEEP_ALIVE_TIMEOUT})             -> 46;
met2idx({?MQTT5_DISCONNECT_SENT,?SESSION_TAKEN_OVER})             -> 47;
met2idx({?MQTT5_DISCONNECT_SENT,?TOPIC_FILTER_INVALID})           -> 48;
met2idx({?MQTT5_DISCONNECT_SENT,?TOPIC_NAME_INVALID})             -> 49;
met2idx({?MQTT5_DISCONNECT_SENT,?RECEIVE_MAX_EXCEEDED})           -> 50;
met2idx({?MQTT5_DISCONNECT_SENT,?TOPIC_ALIAS_INVALID})            -> 51;
met2idx({?MQTT5_DISCONNECT_SENT,?PACKET_TOO_LARGE})               -> 52;
met2idx({?MQTT5_DISCONNECT_SENT,?MESSAGE_RATE_TOO_HIGH})          -> 53;
met2idx({?MQTT5_DISCONNECT_SENT,?QUOTA_EXCEEDED})                 -> 54;
met2idx({?MQTT5_DISCONNECT_SENT,?ADMINISTRATIVE_ACTION})          -> 55;
met2idx({?MQTT5_DISCONNECT_SENT,?PAYLOAD_FORMAT_INVALID})         -> 56;
met2idx({?MQTT5_DISCONNECT_SENT,?RETAIN_NOT_SUPPORTED})           -> 57;
met2idx({?MQTT5_DISCONNECT_SENT,?QOS_NOT_SUPPORTED})              -> 58;
met2idx({?MQTT5_DISCONNECT_SENT,?USE_ANOTHER_SERVER})             -> 59;
met2idx({?MQTT5_DISCONNECT_SENT,?SERVER_MOVED})                   -> 60;
met2idx({?MQTT5_DISCONNECT_SENT,?SHARED_SUBS_NOT_SUPPORTED})      -> 61;
met2idx({?MQTT5_DISCONNECT_SENT,?CONNECTION_RATE_EXCEEDED})       -> 62;
met2idx({?MQTT5_DISCONNECT_SENT,?MAX_CONNECT_TIME})               -> 63;
met2idx({?MQTT5_DISCONNECT_SENT,?SUBSCRIPTION_IDS_NOT_SUPPORTED}) -> 64;
met2idx({?MQTT5_DISCONNECT_SENT,?WILDCARD_SUBS_NOT_SUPPORTED})    -> 65;
met2idx(?MQTT5_PUBLISH_AUTH_ERROR)                                -> 66;
met2idx(?MQTT5_SUBSCRIBE_AUTH_ERROR)                              -> 67;
met2idx(?MQTT5_INVALID_MSG_SIZE_ERROR)                            -> 68;
met2idx(?MQTT5_PUBACK_INVALID_ERROR)                              -> 69;
met2idx(?MQTT5_PUBCOMP_INVALID_ERROR)                             -> 70;
met2idx(?MQTT5_PUBLISH_ERROR)                                     -> 71;
met2idx(?MQTT5_SUBSCRIBE_ERROR)                                   -> 72;
met2idx(?MQTT5_UNSUBSCRIBE_ERROR)                                 -> 73;
met2idx(?MQTT5_PINGREQ_RECEIVED)                                  -> 74;
met2idx(?MQTT5_PINGRESP_SENT)                                     -> 75;
met2idx({?MQTT5_PUBACK_RECEIVED, ?SUCCESS})                       -> 76;
met2idx({?MQTT5_PUBACK_RECEIVED, ?NO_MATCHING_SUBSCRIBERS})       -> 77;
met2idx({?MQTT5_PUBACK_RECEIVED, ?UNSPECIFIED_ERROR})             -> 78;
met2idx({?MQTT5_PUBACK_RECEIVED, ?IMPL_SPECIFIC_ERROR})           -> 79;
met2idx({?MQTT5_PUBACK_RECEIVED, ?NOT_AUTHORIZED})                -> 80;
met2idx({?MQTT5_PUBACK_RECEIVED, ?TOPIC_NAME_INVALID})            -> 81;
met2idx({?MQTT5_PUBACK_RECEIVED, ?PACKET_ID_IN_USE})              -> 82;
met2idx({?MQTT5_PUBACK_RECEIVED, ?QUOTA_EXCEEDED})                -> 83;
met2idx({?MQTT5_PUBACK_RECEIVED, ?PAYLOAD_FORMAT_INVALID})        -> 84;
met2idx({?MQTT5_PUBACK_SENT, ?SUCCESS})                           -> 85;
met2idx({?MQTT5_PUBACK_SENT, ?NO_MATCHING_SUBSCRIBERS})           -> 86;
met2idx({?MQTT5_PUBACK_SENT, ?UNSPECIFIED_ERROR})                 -> 87;
met2idx({?MQTT5_PUBACK_SENT, ?IMPL_SPECIFIC_ERROR})               -> 88;
met2idx({?MQTT5_PUBACK_SENT, ?NOT_AUTHORIZED})                    -> 89;
met2idx({?MQTT5_PUBACK_SENT, ?TOPIC_NAME_INVALID})                -> 90;
met2idx({?MQTT5_PUBACK_SENT, ?PACKET_ID_IN_USE})                  -> 91;
met2idx({?MQTT5_PUBACK_SENT, ?QUOTA_EXCEEDED})                    -> 92;
met2idx({?MQTT5_PUBACK_SENT, ?PAYLOAD_FORMAT_INVALID})            -> 93;
met2idx({?MQTT5_PUBCOMP_RECEIVED,?SUCCESS})                       -> 94;
met2idx({?MQTT5_PUBCOMP_RECEIVED,?PACKET_ID_NOT_FOUND})           -> 95;
met2idx({?MQTT5_PUBCOMP_SENT, ?SUCCESS})                          -> 96;
met2idx({?MQTT5_PUBCOMP_SENT, ?PACKET_ID_NOT_FOUND})              -> 97;
met2idx(?MQTT5_PUBLISH_RECEIVED)                                  -> 98;
met2idx(?MQTT5_PUBLISH_SENT)                                      -> 99;
met2idx({?MQTT5_PUBREC_RECEIVED, ?SUCCESS})                       -> 100;
met2idx({?MQTT5_PUBREC_RECEIVED, ?NO_MATCHING_SUBSCRIBERS})       -> 101;
met2idx({?MQTT5_PUBREC_RECEIVED, ?UNSPECIFIED_ERROR})             -> 102;
met2idx({?MQTT5_PUBREC_RECEIVED, ?IMPL_SPECIFIC_ERROR})           -> 103;
met2idx({?MQTT5_PUBREC_RECEIVED, ?NOT_AUTHORIZED})                -> 104;
met2idx({?MQTT5_PUBREC_RECEIVED, ?TOPIC_NAME_INVALID})            -> 105;
met2idx({?MQTT5_PUBREC_RECEIVED, ?PACKET_ID_IN_USE})              -> 106;
met2idx({?MQTT5_PUBREC_RECEIVED, ?QUOTA_EXCEEDED})                -> 107;
met2idx({?MQTT5_PUBREC_RECEIVED, ?PAYLOAD_FORMAT_INVALID})        -> 108;
met2idx({?MQTT5_PUBREC_SENT, ?SUCCESS})                           -> 109;
met2idx({?MQTT5_PUBREC_SENT, ?NO_MATCHING_SUBSCRIBERS})           -> 110;
met2idx({?MQTT5_PUBREC_SENT, ?UNSPECIFIED_ERROR})                 -> 111;
met2idx({?MQTT5_PUBREC_SENT, ?IMPL_SPECIFIC_ERROR})               -> 112;
met2idx({?MQTT5_PUBREC_SENT, ?NOT_AUTHORIZED})                    -> 113;
met2idx({?MQTT5_PUBREC_SENT, ?TOPIC_NAME_INVALID})                -> 114;
met2idx({?MQTT5_PUBREC_SENT, ?PACKET_ID_IN_USE})                  -> 115;
met2idx({?MQTT5_PUBREC_SENT, ?QUOTA_EXCEEDED})                    -> 116;
met2idx({?MQTT5_PUBREC_SENT, ?PAYLOAD_FORMAT_INVALID})            -> 117;
met2idx({?MQTT5_PUBREL_RECEIVED, ?SUCCESS})                       -> 118;
met2idx({?MQTT5_PUBREL_RECEIVED, ?PACKET_ID_NOT_FOUND})           -> 119;
met2idx({?MQTT5_PUBREL_SENT, ?SUCCESS})                           -> 120;
met2idx({?MQTT5_PUBREL_SENT, ?PACKET_ID_NOT_FOUND})               -> 121;
met2idx(?MQTT5_SUBACK_SENT)                                       -> 122;
met2idx(?MQTT5_SUBSCRIBE_RECEIVED)                                -> 123;
met2idx(?MQTT5_UNSUBACK_SENT)                                     -> 124;
met2idx(?MQTT5_UNSUBSCRIBE_RECEIVED)                              -> 125;
met2idx({?MQTT5_AUTH_SENT,?SUCCESS})                              -> 126;
met2idx({?MQTT5_AUTH_SENT,?CONTINUE_AUTHENTICATION})              -> 127;
met2idx({?MQTT5_AUTH_SENT,?REAUTHENTICATE})                       -> 128;
met2idx({?MQTT5_AUTH_RECEIVED, ?SUCCESS})                         -> 129;
met2idx({?MQTT5_AUTH_RECEIVED, ?CONTINUE_AUTHENTICATION})         -> 130;
met2idx({?MQTT5_AUTH_RECEIVED, ?REAUTHENTICATE})                  -> 131;
met2idx({?MQTT4_CONNACK_SENT, ?CONNACK_ACCEPT})                   -> 132;
met2idx({?MQTT4_CONNACK_SENT, ?CONNACK_PROTO_VER})                -> 133;
met2idx({?MQTT4_CONNACK_SENT, ?CONNACK_INVALID_ID})               -> 134;
met2idx({?MQTT4_CONNACK_SENT, ?CONNACK_SERVER})                   -> 135;
met2idx({?MQTT4_CONNACK_SENT, ?CONNACK_CREDENTIALS})              -> 136;
met2idx({?MQTT4_CONNACK_SENT, ?CONNACK_AUTH})                     -> 137;
met2idx(?MQTT4_CONNECT_RECEIVED)                                  -> 138;
met2idx(?MQTT4_PUBLISH_RECEIVED)                                  -> 139;
met2idx(?MQTT4_PUBACK_RECEIVED)                                   -> 140;
met2idx(?MQTT4_PUBREC_RECEIVED)                                   -> 141;
met2idx(?MQTT4_PUBREL_RECEIVED)                                   -> 142;
met2idx(?MQTT4_PUBCOMP_RECEIVED)                                  -> 143;
met2idx(?MQTT4_SUBSCRIBE_RECEIVED)                                -> 144;
met2idx(?MQTT4_UNSUBSCRIBE_RECEIVED)                              -> 145;
met2idx(?MQTT4_PINGREQ_RECEIVED)                                  -> 146;
met2idx(?MQTT4_DISCONNECT_RECEIVED)                               -> 147;
met2idx(?MQTT4_PUBLISH_SENT)                                      -> 148;
met2idx(?MQTT4_PUBACK_SENT)                                       -> 149;
met2idx(?MQTT4_PUBREC_SENT)                                       -> 150;
met2idx(?MQTT4_PUBREL_SENT)                                       -> 151;
met2idx(?MQTT4_PUBCOMP_SENT)                                      -> 152;
met2idx(?MQTT4_SUBACK_SENT)                                       -> 153;
met2idx(?MQTT4_UNSUBACK_SENT)                                     -> 154;
met2idx(?MQTT4_PINGRESP_SENT)                                     -> 155;
met2idx(?MQTT4_PUBLISH_AUTH_ERROR)                                -> 156;
met2idx(?MQTT4_SUBSCRIBE_AUTH_ERROR)                              -> 157;
met2idx(?MQTT4_INVALID_MSG_SIZE_ERROR)                            -> 158;
met2idx(?MQTT4_PUBACK_INVALID_ERROR)                              -> 159;
met2idx(?MQTT4_PUBREC_INVALID_ERROR)                              -> 160;
met2idx(?MQTT4_PUBCOMP_INVALID_ERROR)                             -> 161;
met2idx(?MQTT4_PUBLISH_ERROR)                                     -> 162;
met2idx(?MQTT4_SUBSCRIBE_ERROR)                                   -> 163;
met2idx(?MQTT4_UNSUBSCRIBE_ERROR)                                 -> 164;
met2idx(?METRIC_QUEUE_SETUP)                                      -> 165;
met2idx(?METRIC_QUEUE_INITIALIZED_FROM_STORAGE)                   -> 166;
met2idx(?METRIC_QUEUE_TEARDOWN)                                   -> 167;
met2idx(?METRIC_QUEUE_MESSAGE_DROP)                               -> 168;
met2idx(?METRIC_QUEUE_MESSAGE_EXPIRED)                            -> 169;
met2idx(?METRIC_QUEUE_MESSAGE_UNHANDLED)                          -> 170;
met2idx(?METRIC_QUEUE_MESSAGE_IN)                                 -> 171;
met2idx(?METRIC_QUEUE_MESSAGE_OUT)                                -> 172;
met2idx(?METRIC_CLIENT_EXPIRED)                                   -> 173;
met2idx(?METRIC_CLUSTER_BYTES_RECEIVED)                           -> 174;
met2idx(?METRIC_CLUSTER_BYTES_SENT)                               -> 175;
met2idx(?METRIC_CLUSTER_BYTES_DROPPED)                            -> 176;
met2idx(?METRIC_SOCKET_OPEN)                                      -> 177;
met2idx(?METRIC_SOCKET_CLOSE)                                     -> 178;
met2idx(?METRIC_SOCKET_ERROR)                                     -> 179;
met2idx(?METRIC_BYTES_RECEIVED)                                   -> 180;
met2idx(?METRIC_BYTES_SENT)                                       -> 181;
met2idx(?METRIC_MSG_IN_RATE)                                      -> 182;
met2idx(?METRIC_MSG_OUT_RATE)                                     -> 183;
met2idx(?METRIC_BYTE_IN_RATE)                                     -> 184;
met2idx(?METRIC_BYTE_OUT_RATE)                                    -> 185;
met2idx(?METRIC_ROUTER_MATCHES_LOCAL)                             -> 186;
met2idx(?METRIC_ROUTER_MATCHES_REMOTE)                            -> 187;
met2idx(mqtt_connack_not_authorized_sent)                         -> 188;
met2idx(mqtt_connack_bad_credentials_sent)                        -> 189;
met2idx(mqtt_connack_server_unavailable_sent)                     -> 190;
met2idx(mqtt_connack_identifier_rejected_sent)                    -> 191;
met2idx(mqtt_connack_unacceptable_protocol_sent)                  -> 192;
met2idx(mqtt_connack_accepted_sent)                               -> 193;
met2idx(?METRIC_SOCKET_CLOSE_TIMEOUT)                             -> 194;
met2idx(?MQTT5_CLIENT_KEEPALIVE_EXPIRED)                          -> 195;
met2idx(?MQTT4_CLIENT_KEEPALIVE_EXPIRED)                          -> 196.

-ifdef(TEST).
clear_stored_rates() ->
    gen_server:call(?MODULE, clear_rates).

start_calc_rates_interval() ->
    gen_server:call(?MODULE, start_calc_rates).

cancel_calc_rates_interval() ->
    gen_server:call(?MODULE, cancel_calc_rates).
-endif.
