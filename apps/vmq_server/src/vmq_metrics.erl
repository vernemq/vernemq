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
         incr_queue_teardown/0,
         incr_queue_drop/0,
         incr_queue_msg_expired/1,
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

incr_queue_msg_expired(N) ->
    incr_item(queue_message_expired, N).

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

incr(Entry) ->
    incr_item(Entry, 1).

incr(Entry, N) ->
    incr_item(Entry, N).

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
      fun(#metric_def{id = Entry}) ->
              reset_counter(Entry)
      end, internal_defs()).

reset_counter(Entry) ->
    [{_, CntRef}] = ets:lookup(?MODULE, Entry),
    mzmetrics:reset_resource_counter(CntRef, 0).

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

    MetricDefs = metric_defs(),
    MetricValues = metric_values(),

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
      end, #{}, metric_defs()),
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
    timer:send_interval(1000, calc_rates),
    ets:new(?MODULE, [public, named_table, {read_concurrency, true}]),
    {RateEntries, _} = lists:unzip(rate_entries()),
    lists:foreach(
      fun(Entry) ->
              Str = lists:flatten(io_lib:format("~p", [Entry])),
              Ref = mzmetrics:alloc_resource(0, Str, 8),
              ets:insert(?MODULE, {Entry, Ref})
      end, RateEntries ++
          [Id || #metric_def{id = Id} <- internal_defs()]),
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
%
%%===================================================================
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
      metric_defs()).

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
             mqtt5_disconnect_def(), mqtt5_connack_sent_def(),
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

     m(counter, [{mqtt_version,"5"}], ?MQTT5_CONNECT_RECEIVED, mqtt_connect_received, <<"The number of CONNECT packets received.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_INVALID_MSG_SIZE_ERROR, mqtt_invalid_msg_size_error, <<"The number of packages exceeding the maximum allowed size.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_PINGREQ_RECEIVED, mqtt_pingreq_received, <<"The number of PINGREQ packets received.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_PINGRESP_SENT, mqtt_pingresp_sent, <<"The number of PINGRESP packets sent.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_PUBACK_INVALID_ERROR, mqtt_puback_invalid_error, <<"The number of unexpected PUBACK messages received.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_PUBACK_RECEIVED, mqtt_puback_received, <<"The number of PUBACK packets received.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_PUBACK_SENT, mqtt_puback_sent, <<"The number of PUBACK packets sent.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_PUBCOMP_INVALID_ERROR, mqtt_pubcomp_invalid_error, <<"The number of unexpected PUBCOMP messages received.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_PUBCOMP_RECEIVED, mqtt_pubcomp_received, <<"The number of PUBCOMP packets received.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_PUBCOMP_SENT, mqtt_pubcomp_sent, <<"The number of PUBCOMP packets sent.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_PUBLISH_AUTH_ERROR, mqtt_publish_auth_error, <<"The number of unauthorized publish attempts.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_PUBLISH_ERROR, mqtt_publish_error, <<"The number of times a PUBLISH operation failed due to a netsplit.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_PUBLISH_RECEIVED, mqtt_publish_received, <<"The number of PUBLISH packets received.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_PUBLISH_SENT, mqtt_publish_sent, <<"The number of PUBLISH packets sent.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_PUBREC_RECEIVED, mqtt_pubrec_received, <<"The number of PUBREC packets received.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_PUBREC_SENT, mqtt_pubrec_sent, <<"The number of PUBREC packets sent.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_PUBREL_RECEIVED, mqtt_pubrel_received, <<"The number of PUBREL packets received.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_PUBREL_SENT, mqtt_pubrel_sent, <<"The number of PUBREL packets sent.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_SUBACK_SENT, mqtt_suback_sent, <<"The number of SUBACK packets sent.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_SUBSCRIBE_AUTH_ERROR, mqtt_subscribe_auth_error, <<"The number of unauthorized subscription attempts.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_SUBSCRIBE_ERROR, mqtt_subscribe_error, <<"The number of times a SUBSCRIBE operation failed due to a netsplit.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_SUBSCRIBE_RECEIVED, mqtt_subscribe_received, <<"The number of SUBSCRIBE packets received.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_UNSUBACK_SENT, mqtt_unsuback_sent, <<"The number of UNSUBACK packets sent.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_UNSUBSCRIBE_ERROR, mqtt_unsubscribe_error, <<"The number of times an UNSUBSCRIBE operation failed due to a netsplit.">>),
     m(counter, [{mqtt_version,"5"}], ?MQTT5_UNSUBSCRIBE_RECEIVED, mqtt_unsubscribe_received, <<"The number of UNSUBSCRIBE packets received.">>),

     m(counter, [], queue_setup, queue_setup, <<"The number of times a MQTT queue process has been started.">>),
     m(counter, [], queue_teardown, queue_teardown, <<"The number of times a MQTT queue process has been terminated.">>),
     m(counter, [], queue_message_drop, queue_message_drop, <<"The number of messages dropped due to full queues.">>),
     m(counter, [], queue_message_expired, queue_message_expired, <<"The number of messages which expired before delivery.">>),
     m(counter, [], queue_message_unhandled, queue_message_unhandled, <<"The number of unhandled messages when connecting with clean session=true.">>),
     m(counter, [], queue_message_in, queue_message_in, <<"The number of PUBLISH packets received by MQTT queue processes.">>),
     m(counter, [], queue_message_out, queue_message_out, <<"The number of PUBLISH packets sent from MQTT queue processes.">>),
     m(counter, [], client_expired, client_expired, <<"Not in use (deprecated)">>),
     m(counter, [], cluster_bytes_received, cluster_bytes_received, <<"The number of bytes received from other cluster nodes.">>),
     m(counter, [], cluster_bytes_sent, cluster_bytes_sent, <<"The number of bytes send to other cluster nodes.">>),
     m(counter, [], cluster_bytes_dropped, cluster_bytes_dropped, <<"The number of bytes dropped while sending data to other cluster nodes.">>)
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

mqtt5_disconnect_def() ->
    RCNs =
        [
         ?NORMAL_DISCONNECT,
         ?DISCONNECT_WITH_WILL_MSG,
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
       {?MQTT5_DISCONNECT_RECEIVED, RCN}, mqtt_disconnect_received,
       <<"The number of DISCONNECT packets received.">>) || RCN <- RCNs].

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
    [{msg_in_rate, mqtt_publish_received},
     {byte_in_rate, bytes_received},
     {msg_out_rate, mqtt_publish_sent},
     {byte_out_rate, bytes_sent}].

-spec misc_statistics() -> [{metric_id(), any()}].
misc_statistics() ->
    {NrOfSubs, SMemory} = vmq_reg_trie:stats(),
    {NrOfRetain, RMemory} = vmq_retain_srv:stats(),
    {NetsplitDetectedCount, NetsplitResolvedCount} = vmq_cluster:netsplit_statistics(),
    [{netsplit_detected, NetsplitDetectedCount},
     {netsplit_resolved, NetsplitResolvedCount},
     {router_subscriptions, NrOfSubs},
     {router_memory, SMemory},
     {retain_messages, NrOfRetain},
     {retain_memory, RMemory},
     {queue_processes, vmq_queue_sup_sup:nr_of_queues()}].

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
