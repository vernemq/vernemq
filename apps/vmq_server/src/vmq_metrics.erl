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
    incr_web_socket_open/0,
    incr_web_socket_close/0,
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
    incr_qos1_opts/1,

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

    incr_qos1_non_retry_message_dropped/0,
    incr_qos1_non_persistence_message_dropped/0,

    incr_mqtt_error_publish/0,
    incr_mqtt_error_subscribe/0,
    incr_mqtt_error_unsubscribe/0,

    incr_sidecar_events/1,
    incr_sidecar_events_error/1,

    incr_queue_setup/0,
    incr_queue_initialized_from_storage/0,
    incr_queue_teardown/0,
    incr_queue_drop/0,
    incr_queue_msg_expired/1,
    incr_queue_unhandled/1,
    incr_queue_in/0,
    incr_queue_in/1,
    incr_queue_out/1,

    incr_router_matches_local/1,
    incr_router_matches_remote/1,
    pretimed_measurement/2,

    incr_msg_store_ops_error/1,
    incr_msg_store_retry_exhausted/1,

    incr_redis_cmd/1,
    incr_redis_cmd_miss/1,
    incr_redis_cmd_err/1,
    incr_redis_stale_cmd/1,
    incr_unauth_redis_cmd/1,

    incr_cache_insert/1,
    incr_cache_delete/1,
    incr_cache_hit/1,
    incr_cache_miss/1,

    incr_msg_enqueue_subscriber_not_found/0,
    incr_topic_counter/1,
    incr_matched_topic/3,
    incr_shared_subscription_group_publish_attempt_failed/0,

    incr_events_sampled/2,
    incr_events_dropped/2,
    update_config_version_metric/2
]).

-export([
    metrics/0,
    metrics/1,
    check_rate/2,
    reset_counters/0,
    reset_counter/1,
    reset_counter/2,
    counter_val/1,
    register/1,
    get_label_info/0
]).

%% API functions
-export([start_link/0]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(TIMER_TABLE, vmq_metrics_timers).
-define(TOPIC_LABEL_TABLE, topic_labels).
-define(EVENTS_SAMPLING_TABLE, vmq_metrics_events_sampling).
-define(CONFIG_VERION_TABLE, config_version_table).

-record(state, {
    info = #{}
}).

-ifdef(TEST).
-export([
    clear_stored_rates/0,
    start_calc_rates_interval/0,
    cancel_calc_rates_interval/0
]).
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

incr_web_socket_open() ->
    incr_item(?METRIC_WEB_SOCKET_OPEN, 1).

incr_web_socket_close() ->
    incr_item(?METRIC_WEB_SOCKET_CLOSE, 1).

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

incr_qos1_non_retry_message_dropped() ->
    incr_item(?QOS1_NON_RETRY_DROPPED, 1).

incr_qos1_non_persistence_message_dropped() ->
    incr_item(?QOS1_NON_PERSISTENCE_DROPPED, 1).

incr_sidecar_events(HookName) ->
    incr_item({?SIDECAR_EVENTS, HookName}, 1).

incr_sidecar_events_error(HookName) ->
    incr_item({?SIDECAR_EVENTS_ERROR, HookName}, 1).

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

incr_router_matches_local(V) ->
    incr_item(?METRIC_ROUTER_MATCHES_LOCAL, V).

incr_router_matches_remote(V) ->
    incr_item(?METRIC_ROUTER_MATCHES_REMOTE, V).

incr_msg_store_ops_error(Op) ->
    incr_item({?METRIC_MSG_STORE_OPS_ERRORS, Op}, 1).

incr_msg_store_retry_exhausted(Op) ->
    incr_item({?METRIC_MSG_STORE_RETRY_EXHAUSTED, Op}, 1).

incr_redis_cmd({CMD, OPERATION}) ->
    incr_item({?REDIS_CMD, CMD, OPERATION}, 1).

incr_redis_cmd_miss({CMD, OPERATION}) ->
    incr_item({?REDIS_CMD_MISS, CMD, OPERATION}, 1).

incr_redis_cmd_err({CMD, OPERATION}) ->
    incr_item({?REDIS_CMD_ERROR, CMD, OPERATION}, 1).

incr_redis_stale_cmd({CMD, OPERATION}) ->
    incr_item({?REDIS_STALE_CMD, CMD, OPERATION}, 1).

incr_unauth_redis_cmd({CMD, OPERATION}) ->
    incr_item({?UNAUTH_REDIS_CMD, CMD, OPERATION}, 1).

incr_qos1_opts({NON_RETRY, NON_PERSISTENCE}) ->
    incr_item({?QOS1_SUBSCRIPTION_OPTS, NON_RETRY, NON_PERSISTENCE}, 1).

incr_cache_insert(NAME) ->
    incr_item({?CACHE_INSERT, NAME}, 1).

incr_cache_delete(NAME) ->
    incr_item({?CACHE_DELETE, NAME}, 1).

incr_cache_hit(NAME) ->
    incr_item({?CACHE_HIT, NAME}, 1).

incr_cache_miss(NAME) ->
    incr_item({?CACHE_MISS, NAME}, 1).

incr_msg_enqueue_subscriber_not_found() ->
    incr_item(msg_enqueue_subscriber_not_found, 1).

incr_shared_subscription_group_publish_attempt_failed() ->
    incr_item(shared_subscription_group_publish_attempt_failed, 1).

-spec incr_events_sampled(hook_name(), Criterion :: binary() | undefined) -> ok.
incr_events_sampled(_, undefined) -> ok;
incr_events_sampled(H, C) -> incr_events_sampled_item(H, C, "sampled").

-spec incr_events_dropped(hook_name(), Criterion :: binary() | undefined) -> ok.
incr_events_dropped(_, undefined) -> ok;
incr_events_dropped(H, C) -> incr_events_sampled_item(H, C, "dropped").

incr_events_sampled_item(H, C, SDType) ->
    try
        ets:update_counter(?EVENTS_SAMPLING_TABLE, {H, C, SDType}, 1)
    catch
        _:_ ->
            try
                ets:insert_new(?EVENTS_SAMPLING_TABLE, {{H, C, SDType}, 0}),
                incr_events_sampled_item(H, C, SDType)
            catch
                _:_ ->
                    lager:warning("couldn't initialize tables", [])
            end
    end,
    ok.

incr(Entry) ->
    incr_item(Entry, 1).

incr(Entry, N) ->
    incr_item(Entry, N).

%% don't do the update
incr_item(_, 0) ->
    ok;
incr_item(Entry, Val) when Val > 0 ->
    ARef =
        case get(atomics_ref) of
            undefined ->
                Ref = persistent_term:get(?MODULE),
                put(atomics_ref, Ref),
                Ref;
            Ref ->
                Ref
        end,
    atomics:add(ARef, met2idx(Entry), Val).

%% true means current rate is ok.

% 0 means unlimited
check_rate(_, 0) ->
    true;
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
        end,
        internal_defs()
    ).

reset_counter(Entry) ->
    ARef = persistent_term:get(?MODULE),
    atomics:put(ARef, met2idx(Entry), 0).

reset_counter(Entry, InitVal) ->
    reset_counter(Entry),
    incr_item(Entry, InitVal).

internal_values(MetricDefs) ->
    lists:map(
        fun(#metric_def{id = Id}) ->
            try counter_val(Id) of
                Value -> {Id, Value}
            catch
                _:_ -> {Id, 0}
            end
        end,
        MetricDefs
    ).

-spec metrics() -> [{metric_def(), non_neg_integer()}].
metrics() ->
    metrics(#{aggregate => true}).

metrics(Opts) ->
    WantLabels = maps:get(labels, Opts, []),

    {PluggableMetricDefs, PluggableMetricValues} = pluggable_metrics(),
    {HistogramMetricDefs, HistogramMetricValues} = histogram_metrics(),
    {TopicMetricsDefs, TopicMetricsValues} = topic_metrics(),
    {EventsSamplingMetricsDefs, EventsSamplingMetricsValues} = events_sampling_metrics(),
    {ConfigVersionMetricsDefs, ConfigVersionMetricsValues} = config_version_metrics(),

    MetricDefs =
        metric_defs() ++ PluggableMetricDefs ++ HistogramMetricDefs ++ TopicMetricsDefs ++
            EventsSamplingMetricsDefs ++ ConfigVersionMetricsDefs,
    MetricValues =
        metric_values() ++ PluggableMetricValues ++ HistogramMetricValues ++ TopicMetricsValues ++
            EventsSamplingMetricsValues ++ ConfigVersionMetricsValues,

    %% Create id->metric def map
    IdDef = lists:foldl(
        fun(#metric_def{id = Id} = MD, Acc) ->
            maps:put(Id, MD, Acc)
        end,
        #{},
        MetricDefs
    ),

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
        end,
        MetricValues
    ),

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
                undefined ->
                    Acc;
                {Mod, Fun, Args} when
                    is_atom(Mod) and is_atom(Fun) and is_list(Args)
                ->
                    try
                        Metrics = apply(Mod, Fun, Args),
                        lists:foldl(
                            fun(
                                {Type, Labels, UniqueId, Name, Description, Value},
                                {DefsAcc, ValsAcc}
                            ) ->
                                {[m(Type, Labels, UniqueId, Name, Description) | DefsAcc], [
                                    {UniqueId, Value} | ValsAcc
                                ]}
                            end,
                            Acc,
                            Metrics
                        )
                    catch
                        _:_ ->
                            Acc
                    end
            end
        end,
        {[], []},
        application:which_applications()
    ).

histogram_metric_defs() ->
    {Defs, _} = histogram_metrics(),
    Defs.

histogram_metrics() ->
    Histogram = ets:foldl(
        fun(
            {Metric, TotalCount, LE10, LE100, LE1K, LE10K, LE100K, LE1M, LE2M, LE4M, INF, TotalSum},
            Acc
        ) ->
            {UniqueId, MetricName, Description, Labels} = metric_name(Metric),
            Buckets =
                #{
                    10 => LE10,
                    100 => LE100,
                    1000 => LE1K,
                    10000 => LE10K,
                    100000 => LE100K,
                    1000000 => LE1M,
                    2000000 => LE2M,
                    4000000 => LE4M,
                    infinity => INF
                },
            [
                {histogram, Labels, UniqueId, MetricName, Description,
                    {TotalCount, TotalSum, Buckets}}
                | Acc
            ]
        end,
        [],
        ?TIMER_TABLE
    ),
    lists:foldl(
        fun({Type, Labels, UniqueId, Name, Description, Value}, {DefsAcc, ValsAcc}) ->
            {[m(Type, Labels, UniqueId, Name, Description) | DefsAcc], [{UniqueId, Value} | ValsAcc]}
        end,
        {[], []},
        Histogram
    ).

topic_metric_defs() ->
    {Defs, _} = topic_metrics(),
    Defs.

topic_metrics() ->
    ets:foldl(
        fun({Metric, TotalCount}, {DefsAcc, ValsAcc}) ->
            {UniqueId, MetricName, Description, Labels} = topic_metric_name(Metric),
            {[m(counter, Labels, UniqueId, MetricName, Description) | DefsAcc], [
                {UniqueId, TotalCount} | ValsAcc
            ]}
        end,
        {[], []},
        ?TOPIC_LABEL_TABLE
    ).

events_sampling_metric_defs() ->
    {Defs, _} = events_sampling_metrics(),
    Defs.

events_sampling_metrics() ->
    ets:foldl(
        fun({{Hook, Criterion, SDType}, TotalCount}, {DefsAcc, ValsAcc}) ->
            {UniqueId, MetricName, Description, Labels} = events_sampled_metric_name(
                Hook, Criterion, SDType
            ),
            {[m(counter, Labels, UniqueId, MetricName, Description) | DefsAcc], [
                {UniqueId, TotalCount} | ValsAcc
            ]}
        end,
        {[], []},
        ?EVENTS_SAMPLING_TABLE
    ).

incr_bucket_ops(V) when V =< 10 ->
    [{2, 1}, {3, 1}, {4, 1}, {5, 1}, {6, 1}, {7, 1}, {8, 1}, {9, 1}, {10, 1}, {11, 1}, {12, V}];
incr_bucket_ops(V) when V =< 100 ->
    [{2, 1}, {4, 1}, {5, 1}, {6, 1}, {7, 1}, {8, 1}, {9, 1}, {10, 1}, {11, 1}, {12, V}];
incr_bucket_ops(V) when V =< 1000 ->
    [{2, 1}, {5, 1}, {6, 1}, {7, 1}, {8, 1}, {9, 1}, {10, 1}, {11, 1}, {12, V}];
incr_bucket_ops(V) when V =< 10000 ->
    [{2, 1}, {6, 1}, {7, 1}, {8, 1}, {9, 1}, {10, 1}, {11, 1}, {12, V}];
incr_bucket_ops(V) when V =< 100000 ->
    [{2, 1}, {7, 1}, {8, 1}, {9, 1}, {10, 1}, {11, 1}, {12, V}];
incr_bucket_ops(V) when V =< 1000000 ->
    [{2, 1}, {8, 1}, {9, 1}, {10, 1}, {11, 1}, {12, V}];
incr_bucket_ops(V) when V =< 2000000 ->
    [{2, 1}, {9, 1}, {10, 1}, {11, 1}, {12, V}];
incr_bucket_ops(V) when V =< 4000000 ->
    [{2, 1}, {10, 1}, {11, 1}, {12, V}];
incr_bucket_ops(V) ->
    [{2, 1}, {11, 1}, {12, V}].

pretimed_measurement(Metric, Val) ->
    BucketOps = incr_bucket_ops(Val),
    incr_histogram_buckets(Metric, BucketOps).

incr_histogram_buckets(Metric, BucketOps) ->
    try
        ets:update_counter(?TIMER_TABLE, Metric, BucketOps)
    catch
        _:_ ->
            try
                ets:insert_new(?TIMER_TABLE, {Metric, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
                incr_histogram_buckets(Metric, BucketOps)
            catch
                _:_ ->
                    lager:warning("couldn't initialize tables", [])
            end
    end.

-spec incr_topic_counter(
    Metric ::
        {topic_matches, subscribe | publish | deliver | delivery_complete,
            Labels :: [{atom(), atom() | list() | binary()}]}
) -> ok.
incr_topic_counter(Metric) ->
    try
        ets:update_counter(?TOPIC_LABEL_TABLE, Metric, 1)
    catch
        _:_ ->
            try
                ets:insert_new(?TOPIC_LABEL_TABLE, {Metric, 0}),
                incr_topic_counter(Metric)
            catch
                _:_ ->
                    lager:warning("couldn't initialize tables", [])
            end
    end.

-spec incr_matched_topic(binary() | undefined, atom(), integer()) -> ok.
incr_matched_topic(<<>>, _Type, _Qos) ->
    ok;
incr_matched_topic(undefined, _Type, _Qos) ->
    ok;
incr_matched_topic(Name, Type, Qos) ->
    OperationName =
        case Type of
            read -> subscribe;
            write -> publish;
            _ -> Type
        end,
    incr_topic_counter(
        {topic_matches, OperationName, [
            {acl_matched, Name}, {qos, integer_to_list(Qos)}
        ]}
    ).

config_version_metric_defs() ->
    {Defs, _} = config_version_metrics(),
    Defs.

config_version_metrics() ->
    ets:foldl(
        fun({Metric, TotalCount}, {DefsAcc, ValsAcc}) ->
            {UniqueId, MetricName, Description, Labels} = config_version_metric_name(Metric),
            {[m(gauge, Labels, UniqueId, MetricName, Description) | DefsAcc], [
                {UniqueId, TotalCount} | ValsAcc
            ]}
        end,
        {[], []},
        ?CONFIG_VERION_TABLE
    ).

-spec update_config_version_metric(
    MetricName :: acl_version | complex_trie_version, Version :: list()
) -> ok.
update_config_version_metric(MetricName, Version) ->
    Metric = {MetricName, [{version, Version}]},
    case ets:lookup(?CONFIG_VERION_TABLE, Metric) of
        [_] ->
            ok;
        _ ->
            try
                ets:match_delete(?CONFIG_VERION_TABLE, {{MetricName, '_'}, 1}),
                ets:insert_new(?CONFIG_VERION_TABLE, {Metric, 1})
            catch
                _:_ ->
                    lager:warning("couldn't initialize tables", [])
            end
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
    LabelInfo =
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
                                        maps:put(LabelName, [Val | Vals], AccAcc)
                                end;
                            _ ->
                                maps:put(LabelName, [Val], AccAcc)
                        end
                    end,
                    Acc0,
                    Labels
                )
            end,
            #{},
            metric_defs() ++ pluggable_metric_defs() ++ histogram_metric_defs() ++
                topic_metric_defs() ++ events_sampling_metric_defs() ++ config_version_metric_defs()
        ),
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
    AllEntries =
        RateEntries ++
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

    ets:new(?TIMER_TABLE, [named_table, public, {write_concurrency, true}]),
    ets:new(?TOPIC_LABEL_TABLE, [named_table, public, {write_concurrency, true}]),
    ets:new(?EVENTS_SAMPLING_TABLE, [named_table, public, {write_concurrency, true}]),
    ets:new(?CONFIG_VERION_TABLE, [named_table, public, {write_concurrency, true}]),

    %% only alloc a new atomics array if one doesn't already exist!
    case catch persistent_term:get(?MODULE) of
        {'EXIT', {badarg, _}} ->
            %% allocate twice the number of entries to make it possible to add
            %% new metrics during a hot code upgrade.
            ARef = atomics:new(2 * NumEntries, [{signed, false}]),
            persistent_term:put(?MODULE, ARef);
        _ExistingRef ->
            ok
    end,
    MetricsInfo = register_metrics(#{}),
    {ok, #state{info = MetricsInfo}}.

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
handle_call({register, Metric}, _From, #state{info = Metrics0} = State) ->
    case register_metric(Metric, Metrics0) of
        {ok, Metrics1} ->
            {reply, ok, State#state{info = Metrics1}};
        {error, _} = Err ->
            {reply, Err, State}
    end;
handle_call(clear_rates, _From, #state{} = State) ->
    %% clear stored rates in process dictionary
    lists:foreach(
        fun({Key, _}) ->
            case Key of
                {rate, _} = V -> erase(V);
                _ -> ok
            end
        end,
        get()
    ),
    %% clear rate entries in atomics
    lists:foreach(
        fun({RateEntry, _Entries}) -> reset_counter(RateEntry) end,
        rate_entries()
    ),
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
handle_call({register, Metric}, _From, #state{info = Metrics0} = State) ->
    case register_metric(Metric, Metrics0) of
        {ok, Metrics1} ->
            {reply, ok, State#state{info = Metrics1}};
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
                rate_entries()
            );
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
        [counter_val_since_last_call(Entry) || Entry <- Entries]
    ),
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
        description = Description
    }.

register_metric(#metric_def{id = Id} = Metric, Metrics) ->
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
        % no need to register pluggable_metric_defs
        metric_defs()
    ).

has_label([], _) ->
    true;
has_label(WantLabels, GotLabels) when is_list(WantLabels), is_list(GotLabels) ->
    lists:all(
        fun(T1) ->
            lists:member(T1, GotLabels)
        end,
        WantLabels
    ).

aggregate_by_name(Metrics) ->
    AggrMetrics = lists:foldl(
        fun({#metric_def{name = Name} = D1, V1}, Acc) ->
            case maps:find(Name, Acc) of
                {ok, {_D2, V2}} ->
                    Acc#{Name => {D1#metric_def{labels = []}, V1 + V2}};
                error ->
                    %% Remove labels when aggregating.
                    Acc#{Name => {D1#metric_def{labels = []}, V1}}
            end
        end,
        #{},
        Metrics
    ),
    maps:values(AggrMetrics).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%% VerneMQ metrics definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

metric_defs() ->
    flatten([system_stats_def(), misc_stats_def(), internal_defs()], []).

metric_values() ->
    flatten(
        [
            system_statistics(),
            misc_statistics(),
            internal_values(internal_defs())
        ],
        []
    ).

internal_defs() ->
    flatten(
        [
            counter_entries_def(),
            mqtt4_connack_sent_def(),
            mqtt5_disconnect_recv_def(),
            mqtt5_disconnect_sent_def(),
            mqtt5_connack_sent_def(),
            mqtt5_puback_sent_def(),
            mqtt5_puback_received_def(),
            mqtt5_pubrec_sent_def(),
            mqtt5_pubrec_received_def(),
            mqtt5_pubrel_sent_def(),
            mqtt5_pubrel_received_def(),
            mqtt5_pubcomp_sent_def(),
            mqtt5_pubcomp_received_def(),
            mqtt5_auth_sent_def(),
            mqtt5_auth_received_def(),
            sidecar_events_def(),
            redis_def(),
            msg_store_ops_def(),
            mqtt_disconnect_def()
        ],
        []
    ).

msg_store_ops_def() ->
    Ops = [
        ?WRITE,
        ?DELETE,
        ?DELETE_ALL,
        ?READ,
        ?FIND
    ],
    [
        m(
            counter,
            [{operation, rcn_to_str(Op)}],
            {?METRIC_MSG_STORE_OPS_ERRORS, Op},
            ?METRIC_MSG_STORE_OPS_ERRORS,
            <<"The number of times msg store operation failed.">>
        )
     || Op <- Ops
    ] ++
        [
            m(
                counter,
                [{operation, rcn_to_str(Op)}],
                {?METRIC_MSG_STORE_RETRY_EXHAUSTED, Op},
                ?METRIC_MSG_STORE_RETRY_EXHAUSTED,
                <<"The number of times msg store operation retry exhausted.">>
            )
         || Op <- Ops
        ].

redis_def() ->
    OPERATIONs =
        [
            ?REMAP_SUBSCRIBER,
            ?SUBSCRIBE,
            ?UNSUBSCRIBE,
            ?DELETE_SUBSCRIBER,
            ?FETCH_SUBSCRIBER,
            ?FETCH_MATCHED_TOPIC_SUBSCRIBERS,
            ?ENQUEUE_MSG,
            ?POLL_MAIN_QUEUE,
            ?GET_LIVE_NODES,
            ?MIGRATE_OFFLINE_QUEUE,
            ?REAP_SUBSCRIBERS
        ],
    REDIS_DEF_1 =
        [
            m(
                counter,
                [{cmd, rcn_to_str(?FCALL)}, {operation, rcn_to_str(OPERATION)}],
                {?REDIS_CMD, ?FCALL, OPERATION},
                redis_cmd_total,
                <<"The number of redis cmd calls.">>
            )
         || OPERATION <- OPERATIONs
        ] ++
            [
                m(
                    counter,
                    [{cmd, rcn_to_str(?FCALL)}, {operation, rcn_to_str(OPERATION)}],
                    {?REDIS_CMD_ERROR, ?FCALL, OPERATION},
                    redis_cmd_errors_total,
                    <<"The number of times redis cmd call failed.">>
                )
             || OPERATION <- OPERATIONs
            ] ++
            [
                m(
                    counter,
                    [{cmd, rcn_to_str(?FCALL)}, {operation, rcn_to_str(OPERATION)}],
                    {?REDIS_CMD_MISS, ?FCALL, OPERATION},
                    redis_cmd_miss_total,
                    <<"The number of times redis cmd returned empty/undefined due to entry not exists.">>
                )
             || OPERATION <- OPERATIONs
            ] ++
            [
                m(
                    counter,
                    [{cmd, rcn_to_str(?FCALL)}, {operation, rcn_to_str(OPERATION)}],
                    {?REDIS_STALE_CMD, ?FCALL, OPERATION},
                    redis_stale_cmd_total,
                    <<"The number of times redis fcall execution failed due to staleness(redis entry timestamp > redis request timestamp).">>
                )
             || OPERATION <- OPERATIONs
            ] ++
            [
                m(
                    counter,
                    [{cmd, rcn_to_str(?FCALL)}, {operation, rcn_to_str(OPERATION)}],
                    {?UNAUTH_REDIS_CMD, ?FCALL, OPERATION},
                    unauth_redis_cmd_total,
                    <<"The number of times redis fcall execution failed due to client connection on different node.">>
                )
             || OPERATION <- OPERATIONs
            ],
    REDIS_DEF_2 =
        [
            m(
                counter,
                [{cmd, rcn_to_str(?FUNCTION_LOAD)}, {operation, rcn_to_str(OPERATION)}],
                {?REDIS_CMD, ?FUNCTION_LOAD, OPERATION},
                redis_cmd_total,
                <<"The number of redis cmd calls.">>
            )
         || OPERATION <- OPERATIONs
        ] ++
            [
                m(
                    counter,
                    [{cmd, rcn_to_str(?FUNCTION_LOAD)}, {operation, rcn_to_str(OPERATION)}],
                    {?REDIS_CMD_ERROR, ?FUNCTION_LOAD, OPERATION},
                    redis_cmd_errors_total,
                    <<"The number of times redis cmd call failed.">>
                )
             || OPERATION <- OPERATIONs
            ],
    REDIS_DEF_3 =
        [
            m(
                counter,
                [{cmd, rcn_to_str(?RPUSH)}, {operation, rcn_to_str(?MSG_STORE_WRITE)}],
                {?REDIS_CMD, ?RPUSH, ?MSG_STORE_WRITE},
                redis_cmd_total,
                <<"The number of redis cmd calls.">>
            ),
            m(
                counter,
                [{cmd, rcn_to_str(?RPUSH)}, {operation, rcn_to_str(?MSG_STORE_WRITE)}],
                {?REDIS_CMD_ERROR, ?RPUSH, ?MSG_STORE_WRITE},
                redis_cmd_errors_total,
                <<"The number of times redis cmd call failed.">>
            ),
            m(
                counter,
                [{cmd, rcn_to_str(?RPUSH)}, {operation, rcn_to_str(?MSG_STORE_WRITE)}],
                {?REDIS_CMD_MISS, ?RPUSH, ?MSG_STORE_WRITE},
                redis_cmd_miss_total,
                <<"The number of times redis cmd returned empty/undefined due to entry not exists.">>
            ),
            m(
                counter,
                [{cmd, rcn_to_str(?DEL)}, {operation, rcn_to_str(?MSG_STORE_DELETE)}],
                {?REDIS_CMD, ?DEL, ?MSG_STORE_DELETE},
                redis_cmd_total,
                <<"The number of redis cmd calls.">>
            ),
            m(
                counter,
                [{cmd, rcn_to_str(?DEL)}, {operation, rcn_to_str(?MSG_STORE_DELETE)}],
                {?REDIS_CMD_ERROR, ?DEL, ?MSG_STORE_DELETE},
                redis_cmd_errors_total,
                <<"The number of times redis cmd call failed.">>
            ),
            m(
                counter,
                [{cmd, rcn_to_str(?DEL)}, {operation, rcn_to_str(?MSG_STORE_DELETE)}],
                {?REDIS_CMD_MISS, ?DEL, ?MSG_STORE_DELETE},
                redis_cmd_miss_total,
                <<"The number of times redis cmd returned empty/undefined due to entry not exists.">>
            ),
            m(
                counter,
                [{cmd, rcn_to_str(?LPOP)}, {operation, rcn_to_str(?MSG_STORE_DELETE)}],
                {?REDIS_CMD, ?LPOP, ?MSG_STORE_DELETE},
                redis_cmd_total,
                <<"The number of redis cmd calls.">>
            ),
            m(
                counter,
                [{cmd, rcn_to_str(?LPOP)}, {operation, rcn_to_str(?MSG_STORE_DELETE)}],
                {?REDIS_CMD_ERROR, ?LPOP, ?MSG_STORE_DELETE},
                redis_cmd_errors_total,
                <<"The number of times redis cmd call failed.">>
            ),
            m(
                counter,
                [{cmd, rcn_to_str(?LPOP)}, {operation, rcn_to_str(?MSG_STORE_DELETE)}],
                {?REDIS_CMD_MISS, ?LPOP, ?MSG_STORE_DELETE},
                redis_cmd_miss_total,
                <<"The number of times redis cmd returned empty/undefined due to entry not exists.">>
            ),
            m(
                counter,
                [{cmd, rcn_to_str(?FIND)}, {operation, rcn_to_str(?MSG_STORE_FIND)}],
                {?REDIS_CMD, ?FIND, ?MSG_STORE_FIND},
                redis_cmd_total,
                <<"The number of redis cmd calls.">>
            ),
            m(
                counter,
                [{cmd, rcn_to_str(?FIND)}, {operation, rcn_to_str(?MSG_STORE_FIND)}],
                {?REDIS_CMD_ERROR, ?FIND, ?MSG_STORE_FIND},
                redis_cmd_errors_total,
                <<"The number of times redis cmd call failed.">>
            ),
            m(
                counter,
                [{cmd, rcn_to_str(?FIND)}, {operation, rcn_to_str(?MSG_STORE_FIND)}],
                {?REDIS_CMD_MISS, ?FIND, ?MSG_STORE_FIND},
                redis_cmd_miss_total,
                <<"The number of times redis cmd returned empty/undefined due to entry not exists.">>
            ),
            m(
                counter,
                [{cmd, rcn_to_str(?SCARD)}, {operation, rcn_to_str(?ENSURE_NO_LOCAL_CLIENT)}],
                {?REDIS_CMD, ?SCARD, ?ENSURE_NO_LOCAL_CLIENT},
                redis_cmd_total,
                <<"The number of redis cmd calls.">>
            ),
            m(
                counter,
                [{cmd, rcn_to_str(?SCARD)}, {operation, rcn_to_str(?ENSURE_NO_LOCAL_CLIENT)}],
                {?REDIS_CMD_ERROR, ?SCARD, ?ENSURE_NO_LOCAL_CLIENT},
                redis_cmd_error_total,
                <<"The number of times redis cmd call failed.">>
            ),
            m(
                counter,
                [{cmd, rcn_to_str(?SCARD)}, {operation, rcn_to_str(?ENSURE_NO_LOCAL_CLIENT)}],
                {?REDIS_CMD_MISS, ?SCARD, ?ENSURE_NO_LOCAL_CLIENT},
                redis_cmd_miss_total,
                <<"The number of times redis cmd returned empty/undefined due to entry not exists.">>
            )
        ],
    REDIS_DEF_1 ++ REDIS_DEF_2 ++ REDIS_DEF_3.

sidecar_events_def() ->
    HOOKs =
        [
            ?ON_REGISTER,
            ?ON_PUBLISH,
            ?ON_SUBSCRIBE,
            ?ON_UNSUBSCRIBE,
            ?ON_DELIVER,
            ?ON_DELIVERY_COMPLETE,
            ?ON_OFFLINE_MESSAGE,
            ?ON_CLIENT_WAKEUP,
            ?ON_CLIENT_OFFLINE,
            ?ON_CLIENT_GONE,
            ?ON_SESSION_EXPIRED
        ],
    [
        m(
            counter,
            [{mqtt_version, "4"}, {hook, rcn_to_str(HOOK)}],
            {?SIDECAR_EVENTS, HOOK},
            sidecar_events,
            <<"The number of events sidecar hook attempts.">>
        )
     || HOOK <- HOOKs
    ] ++
        [
            m(
                counter,
                [{mqtt_version, "4"}, {hook, rcn_to_str(HOOK)}],
                {?SIDECAR_EVENTS_ERROR, HOOK},
                sidecar_events_error,
                <<"The number of times events sidecar hook call failed.">>
            )
         || HOOK <- HOOKs
        ].

counter_entries_def() ->
    [
        m(
            counter,
            [],
            socket_open,
            socket_open,
            <<"The number of times an MQTT socket has been opened.">>
        ),
        m(
            counter,
            [],
            socket_close,
            socket_close,
            <<"The number of times an MQTT socket has been closed.">>
        ),
        m(
            counter,
            [],
            web_socket_open,
            web_socket_open,
            <<"The number of times an MQTT connection over web-socket has been opened.">>
        ),
        m(
            counter,
            [],
            web_socket_close,
            web_socket_close,
            <<"The number of times an MQTT connection over web-socket has been closed.">>
        ),
        m(
            counter,
            [],
            socket_close_timeout,
            socket_close_timeout,
            <<"The number of times VerneMQ closed an MQTT socket due to no CONNECT frame has been received on time.">>
        ),
        m(
            counter,
            [],
            socket_error,
            socket_error,
            <<"The total number of socket errors that have occurred.">>
        ),
        m(counter, [], bytes_received, bytes_received, <<"The total number of bytes received.">>),
        m(counter, [], bytes_sent, bytes_sent, <<"The total number of bytes sent.">>),

        m(
            counter,
            [{mqtt_version, "4"}],
            mqtt_connect_received,
            mqtt_connect_received,
            <<"The number of CONNECT packets received.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            mqtt_publish_received,
            mqtt_publish_received,
            <<"The number of PUBLISH packets received.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            mqtt_puback_received,
            mqtt_puback_received,
            <<"The number of PUBACK packets received.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            mqtt_pubrec_received,
            mqtt_pubrec_received,
            <<"The number of PUBREC packets received.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            mqtt_pubrel_received,
            mqtt_pubrel_received,
            <<"The number of PUBREL packets received.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            mqtt_pubcomp_received,
            mqtt_pubcomp_received,
            <<"The number of PUBCOMP packets received.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            mqtt_subscribe_received,
            mqtt_subscribe_received,
            <<"The number of SUBSCRIBE packets received.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            mqtt_unsubscribe_received,
            mqtt_unsubscribe_received,
            <<"The number of UNSUBSCRIBE packets received.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            mqtt_pingreq_received,
            mqtt_pingreq_received,
            <<"The number of PINGREQ packets received.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            mqtt_disconnect_received,
            mqtt_disconnect_received,
            <<"The number of DISCONNECT packets received.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            mqtt_connack_accepted_sent,
            mqtt_connack_accepted_sent,
            <<"The number of times a connection has been accepted.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            mqtt_connack_unacceptable_protocol_sent,
            mqtt_connack_unacceptable_protocol_sent,
            <<"The number of times the broker is not able to support the requested protocol.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            mqtt_connack_identifier_rejected_sent,
            mqtt_connack_identifier_rejected_sent,
            <<"The number of times a client was rejected due to a unacceptable identifier.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            mqtt_connack_server_unavailable_sent,
            mqtt_connack_server_unavailable_sent,
            <<"The number of times a client was rejected due the the broker being unavailable.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            mqtt_connack_bad_credentials_sent,
            mqtt_connack_bad_credentials_sent,
            <<"The number of times a client sent bad credentials.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            mqtt_connack_not_authorized_sent,
            mqtt_connack_not_authorized_sent,
            <<"The number of times a client was rejected due to insufficient authorization.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            mqtt_publish_sent,
            mqtt_publish_sent,
            <<"The number of PUBLISH packets sent.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            mqtt_puback_sent,
            mqtt_puback_sent,
            <<"The number of PUBACK packets sent.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            mqtt_pubrec_sent,
            mqtt_pubrec_sent,
            <<"The number of PUBREC packets sent.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            mqtt_pubrel_sent,
            mqtt_pubrel_sent,
            <<"The number of PUBREL packets sent.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            mqtt_pubcomp_sent,
            mqtt_pubcomp_sent,
            <<"The number of PUBCOMP packets sent.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            mqtt_suback_sent,
            mqtt_suback_sent,
            <<"The number of SUBACK packets sent.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            mqtt_unsuback_sent,
            mqtt_unsuback_sent,
            <<"The number of UNSUBACK packets sent.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            mqtt_pingresp_sent,
            mqtt_pingresp_sent,
            <<"The number of PINGRESP packets sent.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            mqtt_publish_auth_error,
            mqtt_publish_auth_error,
            <<"The number of unauthorized publish attempts.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            mqtt_subscribe_auth_error,
            mqtt_subscribe_auth_error,
            <<"The number of unauthorized subscription attempts.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            mqtt_invalid_msg_size_error,
            mqtt_invalid_msg_size_error,
            <<"The number of packages exceeding the maximum allowed size.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            mqtt_puback_invalid_error,
            mqtt_puback_invalid_error,
            <<"The number of unexpected PUBACK messages received.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            mqtt_pubrec_invalid_error,
            mqtt_pubrec_invalid_error,
            <<"The number of unexpected PUBREC messages received.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            mqtt_pubcomp_invalid_error,
            mqtt_pubcomp_invalid_error,
            <<"The number of unexpected PUBCOMP messages received.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            mqtt_publish_error,
            mqtt_publish_error,
            <<"The number of times a PUBLISH operation failed due to a netsplit.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            mqtt_subscribe_error,
            mqtt_subscribe_error,
            <<"The number of times a SUBSCRIBE operation failed due to a netsplit.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            mqtt_unsubscribe_error,
            mqtt_unsubscribe_error,
            <<"The number of times an UNSUBSCRIBE operation failed due to a netsplit.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            ?MQTT4_CLIENT_KEEPALIVE_EXPIRED,
            client_keepalive_expired,
            <<"The number of clients which failed to communicate within the keepalive time period.">>
        ),
        m(
            counter,
            [
                {mqtt_version, "4"},
                {non_persistence, rcn_to_str(?NON_PERSISTENCE)},
                {non_retry, rcn_to_str(?NON_RETRY)}
            ],
            {?QOS1_SUBSCRIPTION_OPTS, ?NON_RETRY, ?NON_PERSISTENCE},
            qos1_subscription_opts,
            <<"QoS 1 opts in subscription.">>
        ),
        m(
            counter,
            [
                {mqtt_version, "4"},
                {non_persistence, rcn_to_str(?NON_PERSISTENCE)},
                {non_retry, rcn_to_str(?RETRY)}
            ],
            {?QOS1_SUBSCRIPTION_OPTS, ?RETRY, ?NON_PERSISTENCE},
            qos1_subscription_opts,
            <<"QoS 1 opts in subscription.">>
        ),
        m(
            counter,
            [
                {mqtt_version, "4"},
                {non_persistence, rcn_to_str(?PERSISTENCE)},
                {non_retry, rcn_to_str(?NON_RETRY)}
            ],
            {?QOS1_SUBSCRIPTION_OPTS, ?NON_RETRY, ?PERSISTENCE},
            qos1_subscription_opts,
            <<"QoS 1 opts in subscription.">>
        ),
        m(
            counter,
            [
                {mqtt_version, "4"},
                {non_persistence, rcn_to_str(?PERSISTENCE)},
                {non_retry, rcn_to_str(?RETRY)}
            ],
            {?QOS1_SUBSCRIPTION_OPTS, ?RETRY, ?PERSISTENCE},
            qos1_subscription_opts,
            <<"QoS 1 opts in subscription.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            qos1_non_retry_dropped,
            qos1_non_retry_dropped,
            <<"QoS 1 non_retry messages dropped.">>
        ),
        m(
            counter,
            [{mqtt_version, "4"}],
            qos1_non_persistence_dropped,
            qos1_non_persistence_dropped,
            <<"QoS 1 non_persistence messages dropped.">>
        ),

        m(
            counter,
            [{mqtt_version, "5"}],
            ?MQTT5_CONNECT_RECEIVED,
            mqtt_connect_received,
            <<"The number of CONNECT packets received.">>
        ),
        m(
            counter,
            [{mqtt_version, "5"}],
            ?MQTT5_INVALID_MSG_SIZE_ERROR,
            mqtt_invalid_msg_size_error,
            <<"The number of packages exceeding the maximum allowed size.">>
        ),
        m(
            counter,
            [{mqtt_version, "5"}],
            ?MQTT5_PINGREQ_RECEIVED,
            mqtt_pingreq_received,
            <<"The number of PINGREQ packets received.">>
        ),
        m(
            counter,
            [{mqtt_version, "5"}],
            ?MQTT5_PINGRESP_SENT,
            mqtt_pingresp_sent,
            <<"The number of PINGRESP packets sent.">>
        ),
        m(
            counter,
            [{mqtt_version, "5"}],
            ?MQTT5_PUBACK_INVALID_ERROR,
            mqtt_puback_invalid_error,
            <<"The number of unexpected PUBACK messages received.">>
        ),
        m(
            counter,
            [{mqtt_version, "5"}],
            ?MQTT5_PUBCOMP_INVALID_ERROR,
            mqtt_pubcomp_invalid_error,
            <<"The number of unexpected PUBCOMP messages received.">>
        ),
        m(
            counter,
            [{mqtt_version, "5"}],
            ?MQTT5_PUBLISH_AUTH_ERROR,
            mqtt_publish_auth_error,
            <<"The number of unauthorized publish attempts.">>
        ),
        m(
            counter,
            [{mqtt_version, "5"}],
            ?MQTT5_PUBLISH_ERROR,
            mqtt_publish_error,
            <<"The number of times a PUBLISH operation failed due to a netsplit.">>
        ),
        m(
            counter,
            [{mqtt_version, "5"}],
            ?MQTT5_PUBLISH_RECEIVED,
            mqtt_publish_received,
            <<"The number of PUBLISH packets received.">>
        ),
        m(
            counter,
            [{mqtt_version, "5"}],
            ?MQTT5_PUBLISH_SENT,
            mqtt_publish_sent,
            <<"The number of PUBLISH packets sent.">>
        ),
        m(
            counter,
            [{mqtt_version, "5"}],
            ?MQTT5_SUBACK_SENT,
            mqtt_suback_sent,
            <<"The number of SUBACK packets sent.">>
        ),
        m(
            counter,
            [{mqtt_version, "5"}],
            ?MQTT5_SUBSCRIBE_AUTH_ERROR,
            mqtt_subscribe_auth_error,
            <<"The number of unauthorized subscription attempts.">>
        ),
        m(
            counter,
            [{mqtt_version, "5"}],
            ?MQTT5_SUBSCRIBE_ERROR,
            mqtt_subscribe_error,
            <<"The number of times a SUBSCRIBE operation failed due to a netsplit.">>
        ),
        m(
            counter,
            [{mqtt_version, "5"}],
            ?MQTT5_SUBSCRIBE_RECEIVED,
            mqtt_subscribe_received,
            <<"The number of SUBSCRIBE packets received.">>
        ),
        m(
            counter,
            [{mqtt_version, "5"}],
            ?MQTT5_UNSUBACK_SENT,
            mqtt_unsuback_sent,
            <<"The number of UNSUBACK packets sent.">>
        ),
        m(
            counter,
            [{mqtt_version, "5"}],
            ?MQTT5_UNSUBSCRIBE_ERROR,
            mqtt_unsubscribe_error,
            <<"The number of times an UNSUBSCRIBE operation failed due to a netsplit.">>
        ),
        m(
            counter,
            [{mqtt_version, "5"}],
            ?MQTT5_UNSUBSCRIBE_RECEIVED,
            mqtt_unsubscribe_received,
            <<"The number of UNSUBSCRIBE packets received.">>
        ),
        m(
            counter,
            [{mqtt_version, "5"}],
            ?MQTT5_CLIENT_KEEPALIVE_EXPIRED,
            client_keepalive_expired,
            <<"The number of clients which failed to communicate within the keepalive time period.">>
        ),

        m(
            counter,
            [],
            queue_setup,
            queue_setup,
            <<"The number of times a MQTT queue process has been started.">>
        ),
        m(
            counter,
            [],
            queue_initialized_from_storage,
            queue_initialized_from_storage,
            <<"The number of times a MQTT queue process has been initialized from offline storage.">>
        ),
        m(
            counter,
            [],
            queue_teardown,
            queue_teardown,
            <<"The number of times a MQTT queue process has been terminated.">>
        ),
        m(
            counter,
            [],
            queue_message_drop,
            queue_message_drop,
            <<"The number of messages dropped due to full queues.">>
        ),
        m(
            counter,
            [],
            queue_message_expired,
            queue_message_expired,
            <<"The number of messages which expired before delivery.">>
        ),
        m(
            counter,
            [],
            queue_message_unhandled,
            queue_message_unhandled,
            <<"The number of unhandled messages when connecting with clean session=true.">>
        ),
        m(
            counter,
            [],
            queue_message_in,
            queue_message_in,
            <<"The number of PUBLISH packets received by MQTT queue processes.">>
        ),
        m(
            counter,
            [],
            queue_message_out,
            queue_message_out,
            <<"The number of PUBLISH packets sent from MQTT queue processes.">>
        ),
        m(counter, [], client_expired, client_expired, <<"Not in use (deprecated)">>),
        m(
            counter,
            [],
            router_matches_local,
            router_matches_local,
            <<"The number of matched local subscriptions.">>
        ),
        m(
            counter,
            [],
            router_matches_remote,
            router_matches_remote,
            <<"The number of matched remote subscriptions.">>
        ),
        m(
            counter,
            [{cache, rcn_to_str(?LOCAL_SHARED_SUBS)}],
            {?CACHE_HIT, ?LOCAL_SHARED_SUBS},
            cache_hit,
            <<"The number of cache hit separate by cache name.">>
        ),
        m(
            counter,
            [{cache, rcn_to_str(?LOCAL_SHARED_SUBS)}],
            {?CACHE_MISS, ?LOCAL_SHARED_SUBS},
            cache_miss,
            <<"The number of cache miss separate by cache name.">>
        ),
        m(
            counter,
            [{cache, rcn_to_str(?LOCAL_SHARED_SUBS)}],
            {?CACHE_INSERT, ?LOCAL_SHARED_SUBS},
            cache_insert,
            <<"The number of cache insert separate by cache name.">>
        ),
        m(
            counter,
            [{cache, rcn_to_str(?LOCAL_SHARED_SUBS)}],
            {?CACHE_DELETE, ?LOCAL_SHARED_SUBS},
            cache_delete,
            <<"The number of cache delete separate by cache name.">>
        ),
        m(
            counter,
            [],
            msg_enqueue_subscriber_not_found,
            msg_enqueue_subscriber_not_found,
            <<"The number of times subscriber was not found when message had to be enqueued.">>
        ),
        m(
            counter,
            [],
            shared_subscription_group_publish_attempt_failed,
            shared_subscription_group_publish_attempt_failed,
            <<"The number of times publish attempt failed for a shared subscription group.">>
        )
    ].

flatten([], Acc) ->
    Acc;
flatten([[H | []] | T1], Acc) when is_tuple(H) ->
    flatten(T1, [H | Acc]);
flatten([[H | T0] | T1], Acc) when is_tuple(H) ->
    flatten([T0 | T1], [H | Acc]);
flatten([H | T], Acc) ->
    flatten(T, [H | Acc]).

rcn_to_str(RNC) ->
    %% TODO: replace this with a real textual representation
    atom_to_list(RNC).

mqtt_disconnect_def() ->
    RCNs =
        [
            ?REASON_NOT_AUTHORIZED,
            ?REASON_NORMAL_DISCONNECT,
            ?REASON_SESSION_TAKEN_OVER,
            ?REASON_ADMINISTRATIVE_ACTION,
            ?REASON_DISCONNECT_KEEP_ALIVE,
            ?REASON_DISCONNECT_MIGRATION,
            ?REASON_BAD_AUTHENTICATION_METHOD,
            ?REASON_REMOTE_SESSION_TAKEN_OVER,
            ?REASON_MQTT_CLIENT_DISCONNECT,
            ?REASON_RECEIVE_MAX_EXCEEDED,
            ?REASON_PROTOCOL_ERROR,
            ?REASON_PUBLISH_AUTH_ERROR,
            ?REASON_INVALID_PUBREC_ERROR,
            ?REASON_INVALID_PUBCOMP_ERROR,
            ?REASON_UNEXPECTED_FRAME_TYPE,
            ?REASON_EXIT_SIGNAL_RECEIVED,
            ?REASON_TCP_CLOSED,
            ?REASON_UNSPECIFIED
        ],
    [
        m(
            counter,
            [{mqtt_version, "4"}, {reason_code, rcn_to_str(RCN)}],
            {?MQTT_DISONNECT, RCN},
            ?MQTT_DISONNECT,
            <<"The number of client disconnects seperated by reason codes.">>
        )
     || RCN <- RCNs
    ].

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
            ?PAYLOAD_FORMAT_INVALID
        ],
    [
        m(
            counter,
            [{mqtt_version, "5"}, {reason_code, rcn_to_str(RCN)}],
            {?MQTT5_DISCONNECT_RECEIVED, RCN},
            mqtt_disconnect_received,
            <<"The number of DISCONNECT packets received.">>
        )
     || RCN <- RCNs
    ].

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
            ?WILDCARD_SUBS_NOT_SUPPORTED
        ],
    [
        m(
            counter,
            [{mqtt_version, "5"}, {reason_code, rcn_to_str(RCN)}],
            {?MQTT5_DISCONNECT_SENT, RCN},
            mqtt_disconnect_sent,
            <<"The number of DISCONNECT packets sent.">>
        )
     || RCN <- RCNs
    ].

mqtt5_connack_sent_def() ->
    RCNs =
        [
            ?SUCCESS,
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
            ?CONNECTION_RATE_EXCEEDED
        ],
    [
        m(
            counter,
            [{mqtt_version, "5"}, {reason_code, rcn_to_str(RCN)}],
            {?MQTT5_CONNACK_SENT, RCN},
            mqtt_connack_sent,
            <<"The number of CONNACK packets sent.">>
        )
     || RCN <- RCNs
    ].

mqtt5_puback_sent_def() ->
    RCNs = [
        ?SUCCESS,
        ?NO_MATCHING_SUBSCRIBERS,
        ?UNSPECIFIED_ERROR,
        ?IMPL_SPECIFIC_ERROR,
        ?NOT_AUTHORIZED,
        ?TOPIC_NAME_INVALID,
        ?PACKET_ID_IN_USE,
        ?QUOTA_EXCEEDED,
        ?PAYLOAD_FORMAT_INVALID
    ],
    [
        m(
            counter,
            [{mqtt_version, "5"}, {reason_code, rcn_to_str(RCN)}],
            {?MQTT5_PUBACK_SENT, RCN},
            mqtt_puback_sent,
            <<"The number of PUBACK packets sent.">>
        )
     || RCN <- RCNs
    ].

mqtt5_puback_received_def() ->
    RCNs = [
        ?SUCCESS,
        ?NO_MATCHING_SUBSCRIBERS,
        ?UNSPECIFIED_ERROR,
        ?IMPL_SPECIFIC_ERROR,
        ?NOT_AUTHORIZED,
        ?TOPIC_NAME_INVALID,
        ?PACKET_ID_IN_USE,
        ?QUOTA_EXCEEDED,
        ?PAYLOAD_FORMAT_INVALID
    ],
    [
        m(
            counter,
            [{mqtt_version, "5"}, {reason_code, rcn_to_str(RCN)}],
            {?MQTT5_PUBACK_RECEIVED, RCN},
            mqtt_puback_received,
            <<"The number of PUBACK packets received.">>
        )
     || RCN <- RCNs
    ].

mqtt5_pubrec_sent_def() ->
    RCNs = [
        ?SUCCESS,
        ?NO_MATCHING_SUBSCRIBERS,
        ?UNSPECIFIED_ERROR,
        ?IMPL_SPECIFIC_ERROR,
        ?NOT_AUTHORIZED,
        ?TOPIC_NAME_INVALID,
        ?PACKET_ID_IN_USE,
        ?QUOTA_EXCEEDED,
        ?PAYLOAD_FORMAT_INVALID
    ],
    [
        m(
            counter,
            [{mqtt_version, "5"}, {reason_code, rcn_to_str(RCN)}],
            {?MQTT5_PUBREC_SENT, RCN},
            mqtt_pubrec_sent,
            <<"The number of PUBREC packets sent.">>
        )
     || RCN <- RCNs
    ].

mqtt5_pubrec_received_def() ->
    RCNs = [
        ?SUCCESS,
        ?NO_MATCHING_SUBSCRIBERS,
        ?UNSPECIFIED_ERROR,
        ?IMPL_SPECIFIC_ERROR,
        ?NOT_AUTHORIZED,
        ?TOPIC_NAME_INVALID,
        ?PACKET_ID_IN_USE,
        ?QUOTA_EXCEEDED,
        ?PAYLOAD_FORMAT_INVALID
    ],
    [
        m(
            counter,
            [{mqtt_version, "5"}, {reason_code, rcn_to_str(RCN)}],
            {?MQTT5_PUBREC_RECEIVED, RCN},
            mqtt_pubrec_received,
            <<"The number of PUBREC packets received.">>
        )
     || RCN <- RCNs
    ].

mqtt5_pubrel_sent_def() ->
    RCNs = [
        ?SUCCESS,
        ?PACKET_ID_NOT_FOUND
    ],
    [
        m(
            counter,
            [{mqtt_version, "5"}, {reason_code, rcn_to_str(RCN)}],
            {?MQTT5_PUBREL_SENT, RCN},
            mqtt_pubrel_sent,
            <<"The number of PUBREL packets sent.">>
        )
     || RCN <- RCNs
    ].

mqtt5_pubrel_received_def() ->
    RCNs = [
        ?SUCCESS,
        ?PACKET_ID_NOT_FOUND
    ],
    [
        m(
            counter,
            [{mqtt_version, "5"}, {reason_code, rcn_to_str(RCN)}],
            {?MQTT5_PUBREL_RECEIVED, RCN},
            mqtt_pubrel_received,
            <<"The number of PUBREL packets received.">>
        )
     || RCN <- RCNs
    ].

mqtt5_pubcomp_sent_def() ->
    RCNs = [
        ?SUCCESS,
        ?PACKET_ID_NOT_FOUND
    ],
    [
        m(
            counter,
            [{mqtt_version, "5"}, {reason_code, rcn_to_str(RCN)}],
            {?MQTT5_PUBCOMP_SENT, RCN},
            mqtt_pubcomp_sent,
            <<"The number of PUBCOMP packets sent.">>
        )
     || RCN <- RCNs
    ].

mqtt5_pubcomp_received_def() ->
    RCNs = [
        ?SUCCESS,
        ?PACKET_ID_NOT_FOUND
    ],
    [
        m(
            counter,
            [{mqtt_version, "5"}, {reason_code, rcn_to_str(RCN)}],
            {?MQTT5_PUBCOMP_RECEIVED, RCN},
            mqtt_pubcomp_received,
            <<"The number of PUBCOMP packets received.">>
        )
     || RCN <- RCNs
    ].

mqtt5_auth_sent_def() ->
    RCNs = [
        ?SUCCESS,
        ?CONTINUE_AUTHENTICATION,
        ?REAUTHENTICATE
    ],
    [
        m(
            counter,
            [{mqtt_version, "5"}, {reason_code, rcn_to_str(RCN)}],
            {?MQTT5_AUTH_SENT, RCN},
            mqtt_auth_sent,
            <<"The number of AUTH packets sent.">>
        )
     || RCN <- RCNs
    ].

mqtt5_auth_received_def() ->
    RCNs = [
        ?SUCCESS,
        ?CONTINUE_AUTHENTICATION,
        ?REAUTHENTICATE
    ],
    [
        m(
            counter,
            [{mqtt_version, "5"}, {reason_code, rcn_to_str(RCN)}],
            {?MQTT5_AUTH_RECEIVED, RCN},
            mqtt_auth_received,
            <<"The number of AUTH packets received.">>
        )
     || RCN <- RCNs
    ].

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
    RCNs = [
        ?CONNACK_ACCEPT,
        ?CONNACK_PROTO_VER,
        ?CONNACK_INVALID_ID,
        ?CONNACK_SERVER,
        ?CONNACK_CREDENTIALS,
        ?CONNACK_AUTH
    ],
    [
        m(
            counter,
            [{mqtt_version, "4"}, {return_code, m4_connack_labels(RCN)}],
            {?MQTT4_CONNACK_SENT, RCN},
            mqtt_connack_sent,
            <<"The number of CONNACK packets sent.">>
        )
     || RCN <- RCNs
    ].

rate_entries() ->
    [
        {?METRIC_MSG_IN_RATE, [?MQTT4_PUBLISH_RECEIVED, ?MQTT5_PUBLISH_RECEIVED]},
        {?METRIC_BYTE_IN_RATE, [?METRIC_BYTES_RECEIVED]},
        {?METRIC_MSG_OUT_RATE, [?MQTT4_PUBLISH_SENT, ?MQTT5_PUBLISH_SENT]},
        {?METRIC_BYTE_OUT_RATE, [?METRIC_BYTES_SENT]}
    ].

fetch_external_metric(Mod, Fun, Default) ->
    % safe-guard call to external metric provider
    % as it it possible that the metric provider
    % isn't ready yet.
    try
        apply(Mod, Fun, [])
    catch
        ErrorClass:Reason ->
            lager:warning("can't fetch metrics from ~p", [Mod]),
            lager:debug("fetching metrics from ~p resulted in ~p with reason ~p", [
                Mod, ErrorClass, Reason
            ]),
            Default
    end.

-spec misc_statistics() -> [{metric_id(), any()}].
misc_statistics() ->
    {NrOfSubs, SMemory} = fetch_external_metric(vmq_reg_trie, stats, {0, 0}),
    {NrOfRetain, RMemory} = fetch_external_metric(vmq_retain_srv, stats, {0, 0}),
    [
        {router_subscriptions, NrOfSubs},
        {router_memory, SMemory},
        {retain_messages, NrOfRetain},
        {retain_memory, RMemory},
        {queue_processes, fetch_external_metric(vmq_queue_sup_sup, nr_of_queues, 0)}
    ].

-spec misc_stats_def() -> [metric_def()].
misc_stats_def() ->
    [
        m(
            gauge,
            [],
            router_subscriptions,
            router_subscriptions,
            <<"The number of subscriptions in the routing table.">>
        ),
        m(
            gauge,
            [],
            router_memory,
            router_memory,
            <<"The number of bytes used by the routing table.">>
        ),
        m(
            gauge,
            [],
            retain_messages,
            retain_messages,
            <<"The number of currently stored retained messages.">>
        ),
        m(
            gauge,
            [],
            retain_memory,
            retain_memory,
            <<"The number of bytes used for storing retained messages.">>
        ),
        m(gauge, [], queue_processes, queue_processes, <<"The number of MQTT queue processes.">>)
    ].

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
    #{
        total := ErlangMemTotal,
        processes := ErlangMemProcesses,
        processes_used := ErlangMemProcessesUsed,
        system := ErlangMemSystem,
        atom := ErlangMemAtom,
        atom_used := ErlangMemAtomUsed,
        binary := ErlangMemBinary,
        code := ErlangMemCode,
        ets := ErlangMemEts
    } = maps:from_list(erlang:memory()),
    [
        {system_context_switches, ContextSwitches},
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
        {vm_memory_ets, ErlangMemEts}
        | scheduler_utilization()
    ].

system_stats_def() ->
    [
        m(
            counter,
            [],
            system_context_switches,
            system_context_switches,
            <<"The total number of context switches.">>
        ),
        m(
            counter,
            [],
            system_exact_reductions,
            system_exact_reductions,
            <<"The exact number of reductions performed.">>
        ),
        m(
            counter,
            [],
            system_gc_count,
            system_gc_count,
            <<"The number of garbage collections performed.">>
        ),
        m(
            counter,
            [],
            system_words_reclaimed_by_gc,
            system_words_reclaimed_by_gc,
            <<"The number of words reclaimed by the garbage collector.">>
        ),
        m(
            counter,
            [],
            system_io_in,
            system_io_in,
            <<"The total number of bytes received through ports.">>
        ),
        m(
            counter,
            [],
            system_io_out,
            system_io_out,
            <<"The total number of bytes sent through ports.">>
        ),
        m(
            counter,
            [],
            system_reductions,
            system_reductions,
            <<"The number of reductions performed in the VM since the node was started.">>
        ),
        m(
            gauge,
            [],
            system_run_queue,
            system_run_queue,
            <<"The total number of processes and ports ready to run on all run-queues.">>
        ),
        m(
            counter,
            [],
            system_runtime,
            system_runtime,
            <<"The sum of the runtime for all threads in the Erlang runtime system.">>
        ),
        m(
            counter,
            [],
            system_wallclock,
            system_wallclock,
            <<"The number of milli-seconds passed since the node was started.">>
        ),
        m(
            gauge,
            [],
            system_process_count,
            system_process_count,
            <<"The number of Erlang processes.">>
        ),

        m(gauge, [], vm_memory_total, vm_memory_total, <<"The total amount of memory allocated.">>),
        m(
            gauge,
            [],
            vm_memory_processes,
            vm_memory_processes,
            <<"The amount of memory allocated for processes.">>
        ),
        m(
            gauge,
            [],
            vm_memory_processes_used,
            vm_memory_processes_used,
            <<"The amount of memory used by processes.">>
        ),
        m(
            gauge,
            [],
            vm_memory_system,
            vm_memory_system,
            <<"The amount of memory allocated for the emulator.">>
        ),
        m(
            gauge,
            [],
            vm_memory_atom,
            vm_memory_atom,
            <<"The amount of memory allocated for atoms.">>
        ),
        m(
            gauge,
            [],
            vm_memory_atom_used,
            vm_memory_atom_used,
            <<"The amount of memory used by atoms.">>
        ),
        m(
            gauge,
            [],
            vm_memory_binary,
            vm_memory_binary,
            <<"The amount of memory allocated for binaries.">>
        ),
        m(
            gauge,
            [],
            vm_memory_code,
            vm_memory_code,
            <<"The amount of memory allocated for code.">>
        ),
        m(
            gauge,
            [],
            vm_memory_ets,
            vm_memory_ets,
            <<"The amount of memory allocated for ETS tables.">>
        )
        | scheduler_utilization_def()
    ].

-spec scheduler_utilization() -> [{metric_id(), any()}].
scheduler_utilization() ->
    WallTimeTs0 =
        case erlang:get(vmq_metrics_scheduler_wall_time) of
            undefined ->
                erlang:system_flag(scheduler_wall_time, true),
                Ts0 = lists:sort(erlang:statistics(scheduler_wall_time)),
                erlang:put(vmq_metrics_scheduler_wall_time, Ts0),
                Ts0;
            Ts0 ->
                Ts0
        end,
    WallTimeTs1 = lists:sort(erlang:statistics(scheduler_wall_time)),
    erlang:put(vmq_metrics_scheduler_wall_time, WallTimeTs1),
    SchedulerUtilization = lists:map(
        fun({{I, A0, T0}, {I, A1, T1}}) ->
            StrName = "system_utilization_scheduler_" ++ integer_to_list(I),
            Id = list_to_atom(StrName),
            Val = round((100 * (A1 - A0) / (T1 - T0))),
            {Id, Val}
        end,
        lists:zip(WallTimeTs0, WallTimeTs1)
    ),
    TotalSum = lists:foldl(
        fun({_MetricDef, UsagePerSchedu}, Sum) ->
            Sum + UsagePerSchedu
        end,
        0,
        SchedulerUtilization
    ),
    AvgUtilization = round(TotalSum / length(SchedulerUtilization)),
    [
        {system_utilization, AvgUtilization}
        | SchedulerUtilization
    ].

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
        lists:seq(1, NumSchedulers)
    ),
    [
        m(
            gauge,
            [],
            system_utilization,
            system_utilization,
            <<"The average system (scheduler) utilization (percentage).">>
        )
        | SchedUtilDefs
    ].

met2idx(?MQTT5_CONNECT_RECEIVED) -> 1;
met2idx({?MQTT5_CONNACK_SENT, ?SUCCESS}) -> 2;
met2idx({?MQTT5_CONNACK_SENT, ?UNSPECIFIED_ERROR}) -> 3;
met2idx({?MQTT5_CONNACK_SENT, ?MALFORMED_PACKET}) -> 4;
met2idx({?MQTT5_CONNACK_SENT, ?PROTOCOL_ERROR}) -> 5;
met2idx({?MQTT5_CONNACK_SENT, ?IMPL_SPECIFIC_ERROR}) -> 6;
met2idx({?MQTT5_CONNACK_SENT, ?UNSUPPORTED_PROTOCOL_VERSION}) -> 7;
met2idx({?MQTT5_CONNACK_SENT, ?CLIENT_IDENTIFIER_NOT_VALID}) -> 8;
met2idx({?MQTT5_CONNACK_SENT, ?BAD_USERNAME_OR_PASSWORD}) -> 9;
met2idx({?MQTT5_CONNACK_SENT, ?NOT_AUTHORIZED}) -> 10;
met2idx({?MQTT5_CONNACK_SENT, ?SERVER_UNAVAILABLE}) -> 11;
met2idx({?MQTT5_CONNACK_SENT, ?SERVER_BUSY}) -> 12;
met2idx({?MQTT5_CONNACK_SENT, ?BANNED}) -> 13;
met2idx({?MQTT5_CONNACK_SENT, ?BAD_AUTHENTICATION_METHOD}) -> 14;
met2idx({?MQTT5_CONNACK_SENT, ?TOPIC_NAME_INVALID}) -> 15;
met2idx({?MQTT5_CONNACK_SENT, ?PACKET_TOO_LARGE}) -> 16;
met2idx({?MQTT5_CONNACK_SENT, ?QUOTA_EXCEEDED}) -> 17;
met2idx({?MQTT5_CONNACK_SENT, ?PAYLOAD_FORMAT_INVALID}) -> 18;
met2idx({?MQTT5_CONNACK_SENT, ?RETAIN_NOT_SUPPORTED}) -> 19;
met2idx({?MQTT5_CONNACK_SENT, ?QOS_NOT_SUPPORTED}) -> 20;
met2idx({?MQTT5_CONNACK_SENT, ?USE_ANOTHER_SERVER}) -> 21;
met2idx({?MQTT5_CONNACK_SENT, ?SERVER_MOVED}) -> 22;
met2idx({?MQTT5_CONNACK_SENT, ?CONNECTION_RATE_EXCEEDED}) -> 23;
met2idx({?MQTT5_DISCONNECT_RECEIVED, ?NORMAL_DISCONNECT}) -> 24;
met2idx({?MQTT5_DISCONNECT_RECEIVED, ?DISCONNECT_WITH_WILL_MSG}) -> 25;
met2idx({?MQTT5_DISCONNECT_RECEIVED, ?UNSPECIFIED_ERROR}) -> 26;
met2idx({?MQTT5_DISCONNECT_RECEIVED, ?MALFORMED_PACKET}) -> 27;
met2idx({?MQTT5_DISCONNECT_RECEIVED, ?PROTOCOL_ERROR}) -> 28;
met2idx({?MQTT5_DISCONNECT_RECEIVED, ?IMPL_SPECIFIC_ERROR}) -> 29;
met2idx({?MQTT5_DISCONNECT_RECEIVED, ?TOPIC_NAME_INVALID}) -> 30;
met2idx({?MQTT5_DISCONNECT_RECEIVED, ?RECEIVE_MAX_EXCEEDED}) -> 31;
met2idx({?MQTT5_DISCONNECT_RECEIVED, ?TOPIC_ALIAS_INVALID}) -> 32;
met2idx({?MQTT5_DISCONNECT_RECEIVED, ?PACKET_TOO_LARGE}) -> 33;
met2idx({?MQTT5_DISCONNECT_RECEIVED, ?MESSAGE_RATE_TOO_HIGH}) -> 34;
met2idx({?MQTT5_DISCONNECT_RECEIVED, ?QUOTA_EXCEEDED}) -> 35;
met2idx({?MQTT5_DISCONNECT_RECEIVED, ?ADMINISTRATIVE_ACTION}) -> 36;
met2idx({?MQTT5_DISCONNECT_RECEIVED, ?PAYLOAD_FORMAT_INVALID}) -> 37;
met2idx({?MQTT5_DISCONNECT_SENT, ?NORMAL_DISCONNECT}) -> 38;
met2idx({?MQTT5_DISCONNECT_SENT, ?UNSPECIFIED_ERROR}) -> 39;
met2idx({?MQTT5_DISCONNECT_SENT, ?MALFORMED_PACKET}) -> 40;
met2idx({?MQTT5_DISCONNECT_SENT, ?PROTOCOL_ERROR}) -> 41;
met2idx({?MQTT5_DISCONNECT_SENT, ?IMPL_SPECIFIC_ERROR}) -> 42;
met2idx({?MQTT5_DISCONNECT_SENT, ?NOT_AUTHORIZED}) -> 43;
met2idx({?MQTT5_DISCONNECT_SENT, ?SERVER_BUSY}) -> 44;
met2idx({?MQTT5_DISCONNECT_SENT, ?SERVER_SHUTTING_DOWN}) -> 45;
met2idx({?MQTT5_DISCONNECT_SENT, ?KEEP_ALIVE_TIMEOUT}) -> 46;
met2idx({?MQTT5_DISCONNECT_SENT, ?SESSION_TAKEN_OVER}) -> 47;
met2idx({?MQTT5_DISCONNECT_SENT, ?TOPIC_FILTER_INVALID}) -> 48;
met2idx({?MQTT5_DISCONNECT_SENT, ?TOPIC_NAME_INVALID}) -> 49;
met2idx({?MQTT5_DISCONNECT_SENT, ?RECEIVE_MAX_EXCEEDED}) -> 50;
met2idx({?MQTT5_DISCONNECT_SENT, ?TOPIC_ALIAS_INVALID}) -> 51;
met2idx({?MQTT5_DISCONNECT_SENT, ?PACKET_TOO_LARGE}) -> 52;
met2idx({?MQTT5_DISCONNECT_SENT, ?MESSAGE_RATE_TOO_HIGH}) -> 53;
met2idx({?MQTT5_DISCONNECT_SENT, ?QUOTA_EXCEEDED}) -> 54;
met2idx({?MQTT5_DISCONNECT_SENT, ?ADMINISTRATIVE_ACTION}) -> 55;
met2idx({?MQTT5_DISCONNECT_SENT, ?PAYLOAD_FORMAT_INVALID}) -> 56;
met2idx({?MQTT5_DISCONNECT_SENT, ?RETAIN_NOT_SUPPORTED}) -> 57;
met2idx({?MQTT5_DISCONNECT_SENT, ?QOS_NOT_SUPPORTED}) -> 58;
met2idx({?MQTT5_DISCONNECT_SENT, ?USE_ANOTHER_SERVER}) -> 59;
met2idx({?MQTT5_DISCONNECT_SENT, ?SERVER_MOVED}) -> 60;
met2idx({?MQTT5_DISCONNECT_SENT, ?SHARED_SUBS_NOT_SUPPORTED}) -> 61;
met2idx({?MQTT5_DISCONNECT_SENT, ?CONNECTION_RATE_EXCEEDED}) -> 62;
met2idx({?MQTT5_DISCONNECT_SENT, ?MAX_CONNECT_TIME}) -> 63;
met2idx({?MQTT5_DISCONNECT_SENT, ?SUBSCRIPTION_IDS_NOT_SUPPORTED}) -> 64;
met2idx({?MQTT5_DISCONNECT_SENT, ?WILDCARD_SUBS_NOT_SUPPORTED}) -> 65;
met2idx(?MQTT5_PUBLISH_AUTH_ERROR) -> 66;
met2idx(?MQTT5_SUBSCRIBE_AUTH_ERROR) -> 67;
met2idx(?MQTT5_INVALID_MSG_SIZE_ERROR) -> 68;
met2idx(?MQTT5_PUBACK_INVALID_ERROR) -> 69;
met2idx(?MQTT5_PUBCOMP_INVALID_ERROR) -> 70;
met2idx(?MQTT5_PUBLISH_ERROR) -> 71;
met2idx(?MQTT5_SUBSCRIBE_ERROR) -> 72;
met2idx(?MQTT5_UNSUBSCRIBE_ERROR) -> 73;
met2idx(?MQTT5_PINGREQ_RECEIVED) -> 74;
met2idx(?MQTT5_PINGRESP_SENT) -> 75;
met2idx({?MQTT5_PUBACK_RECEIVED, ?SUCCESS}) -> 76;
met2idx({?MQTT5_PUBACK_RECEIVED, ?NO_MATCHING_SUBSCRIBERS}) -> 77;
met2idx({?MQTT5_PUBACK_RECEIVED, ?UNSPECIFIED_ERROR}) -> 78;
met2idx({?MQTT5_PUBACK_RECEIVED, ?IMPL_SPECIFIC_ERROR}) -> 79;
met2idx({?MQTT5_PUBACK_RECEIVED, ?NOT_AUTHORIZED}) -> 80;
met2idx({?MQTT5_PUBACK_RECEIVED, ?TOPIC_NAME_INVALID}) -> 81;
met2idx({?MQTT5_PUBACK_RECEIVED, ?PACKET_ID_IN_USE}) -> 82;
met2idx({?MQTT5_PUBACK_RECEIVED, ?QUOTA_EXCEEDED}) -> 83;
met2idx({?MQTT5_PUBACK_RECEIVED, ?PAYLOAD_FORMAT_INVALID}) -> 84;
met2idx({?MQTT5_PUBACK_SENT, ?SUCCESS}) -> 85;
met2idx({?MQTT5_PUBACK_SENT, ?NO_MATCHING_SUBSCRIBERS}) -> 86;
met2idx({?MQTT5_PUBACK_SENT, ?UNSPECIFIED_ERROR}) -> 87;
met2idx({?MQTT5_PUBACK_SENT, ?IMPL_SPECIFIC_ERROR}) -> 88;
met2idx({?MQTT5_PUBACK_SENT, ?NOT_AUTHORIZED}) -> 89;
met2idx({?MQTT5_PUBACK_SENT, ?TOPIC_NAME_INVALID}) -> 90;
met2idx({?MQTT5_PUBACK_SENT, ?PACKET_ID_IN_USE}) -> 91;
met2idx({?MQTT5_PUBACK_SENT, ?QUOTA_EXCEEDED}) -> 92;
met2idx({?MQTT5_PUBACK_SENT, ?PAYLOAD_FORMAT_INVALID}) -> 93;
met2idx({?MQTT5_PUBCOMP_RECEIVED, ?SUCCESS}) -> 94;
met2idx({?MQTT5_PUBCOMP_RECEIVED, ?PACKET_ID_NOT_FOUND}) -> 95;
met2idx({?MQTT5_PUBCOMP_SENT, ?SUCCESS}) -> 96;
met2idx({?MQTT5_PUBCOMP_SENT, ?PACKET_ID_NOT_FOUND}) -> 97;
met2idx(?MQTT5_PUBLISH_RECEIVED) -> 98;
met2idx(?MQTT5_PUBLISH_SENT) -> 99;
met2idx({?MQTT5_PUBREC_RECEIVED, ?SUCCESS}) -> 100;
met2idx({?MQTT5_PUBREC_RECEIVED, ?NO_MATCHING_SUBSCRIBERS}) -> 101;
met2idx({?MQTT5_PUBREC_RECEIVED, ?UNSPECIFIED_ERROR}) -> 102;
met2idx({?MQTT5_PUBREC_RECEIVED, ?IMPL_SPECIFIC_ERROR}) -> 103;
met2idx({?MQTT5_PUBREC_RECEIVED, ?NOT_AUTHORIZED}) -> 104;
met2idx({?MQTT5_PUBREC_RECEIVED, ?TOPIC_NAME_INVALID}) -> 105;
met2idx({?MQTT5_PUBREC_RECEIVED, ?PACKET_ID_IN_USE}) -> 106;
met2idx({?MQTT5_PUBREC_RECEIVED, ?QUOTA_EXCEEDED}) -> 107;
met2idx({?MQTT5_PUBREC_RECEIVED, ?PAYLOAD_FORMAT_INVALID}) -> 108;
met2idx({?MQTT5_PUBREC_SENT, ?SUCCESS}) -> 109;
met2idx({?MQTT5_PUBREC_SENT, ?NO_MATCHING_SUBSCRIBERS}) -> 110;
met2idx({?MQTT5_PUBREC_SENT, ?UNSPECIFIED_ERROR}) -> 111;
met2idx({?MQTT5_PUBREC_SENT, ?IMPL_SPECIFIC_ERROR}) -> 112;
met2idx({?MQTT5_PUBREC_SENT, ?NOT_AUTHORIZED}) -> 113;
met2idx({?MQTT5_PUBREC_SENT, ?TOPIC_NAME_INVALID}) -> 114;
met2idx({?MQTT5_PUBREC_SENT, ?PACKET_ID_IN_USE}) -> 115;
met2idx({?MQTT5_PUBREC_SENT, ?QUOTA_EXCEEDED}) -> 116;
met2idx({?MQTT5_PUBREC_SENT, ?PAYLOAD_FORMAT_INVALID}) -> 117;
met2idx({?MQTT5_PUBREL_RECEIVED, ?SUCCESS}) -> 118;
met2idx({?MQTT5_PUBREL_RECEIVED, ?PACKET_ID_NOT_FOUND}) -> 119;
met2idx({?MQTT5_PUBREL_SENT, ?SUCCESS}) -> 120;
met2idx({?MQTT5_PUBREL_SENT, ?PACKET_ID_NOT_FOUND}) -> 121;
met2idx(?MQTT5_SUBACK_SENT) -> 122;
met2idx(?MQTT5_SUBSCRIBE_RECEIVED) -> 123;
met2idx(?MQTT5_UNSUBACK_SENT) -> 124;
met2idx(?MQTT5_UNSUBSCRIBE_RECEIVED) -> 125;
met2idx({?MQTT5_AUTH_SENT, ?SUCCESS}) -> 126;
met2idx({?MQTT5_AUTH_SENT, ?CONTINUE_AUTHENTICATION}) -> 127;
met2idx({?MQTT5_AUTH_SENT, ?REAUTHENTICATE}) -> 128;
met2idx({?MQTT5_AUTH_RECEIVED, ?SUCCESS}) -> 129;
met2idx({?MQTT5_AUTH_RECEIVED, ?CONTINUE_AUTHENTICATION}) -> 130;
met2idx({?MQTT5_AUTH_RECEIVED, ?REAUTHENTICATE}) -> 131;
met2idx({?MQTT4_CONNACK_SENT, ?CONNACK_ACCEPT}) -> 132;
met2idx({?MQTT4_CONNACK_SENT, ?CONNACK_PROTO_VER}) -> 133;
met2idx({?MQTT4_CONNACK_SENT, ?CONNACK_INVALID_ID}) -> 134;
met2idx({?MQTT4_CONNACK_SENT, ?CONNACK_SERVER}) -> 135;
met2idx({?MQTT4_CONNACK_SENT, ?CONNACK_CREDENTIALS}) -> 136;
met2idx({?MQTT4_CONNACK_SENT, ?CONNACK_AUTH}) -> 137;
met2idx(?MQTT4_CONNECT_RECEIVED) -> 138;
met2idx(?MQTT4_PUBLISH_RECEIVED) -> 139;
met2idx(?MQTT4_PUBACK_RECEIVED) -> 140;
met2idx(?MQTT4_PUBREC_RECEIVED) -> 141;
met2idx(?MQTT4_PUBREL_RECEIVED) -> 142;
met2idx(?MQTT4_PUBCOMP_RECEIVED) -> 143;
met2idx(?MQTT4_SUBSCRIBE_RECEIVED) -> 144;
met2idx(?MQTT4_UNSUBSCRIBE_RECEIVED) -> 145;
met2idx(?MQTT4_PINGREQ_RECEIVED) -> 146;
met2idx(?MQTT4_DISCONNECT_RECEIVED) -> 147;
met2idx(?MQTT4_PUBLISH_SENT) -> 148;
met2idx(?MQTT4_PUBACK_SENT) -> 149;
met2idx(?MQTT4_PUBREC_SENT) -> 150;
met2idx(?MQTT4_PUBREL_SENT) -> 151;
met2idx(?MQTT4_PUBCOMP_SENT) -> 152;
met2idx(?MQTT4_SUBACK_SENT) -> 153;
met2idx(?MQTT4_UNSUBACK_SENT) -> 154;
met2idx(?MQTT4_PINGRESP_SENT) -> 155;
met2idx(?MQTT4_PUBLISH_AUTH_ERROR) -> 156;
met2idx(?MQTT4_SUBSCRIBE_AUTH_ERROR) -> 157;
met2idx(?MQTT4_INVALID_MSG_SIZE_ERROR) -> 158;
met2idx(?MQTT4_PUBACK_INVALID_ERROR) -> 159;
met2idx(?MQTT4_PUBREC_INVALID_ERROR) -> 160;
met2idx(?MQTT4_PUBCOMP_INVALID_ERROR) -> 161;
met2idx(?MQTT4_PUBLISH_ERROR) -> 162;
met2idx(?MQTT4_SUBSCRIBE_ERROR) -> 163;
met2idx(?MQTT4_UNSUBSCRIBE_ERROR) -> 164;
met2idx(?METRIC_QUEUE_SETUP) -> 165;
met2idx(?METRIC_QUEUE_INITIALIZED_FROM_STORAGE) -> 166;
met2idx(?METRIC_QUEUE_TEARDOWN) -> 167;
met2idx(?METRIC_QUEUE_MESSAGE_DROP) -> 168;
met2idx(?METRIC_QUEUE_MESSAGE_EXPIRED) -> 169;
met2idx(?METRIC_QUEUE_MESSAGE_UNHANDLED) -> 170;
met2idx(?METRIC_QUEUE_MESSAGE_IN) -> 171;
met2idx(?METRIC_QUEUE_MESSAGE_OUT) -> 172;
met2idx(?METRIC_CLIENT_EXPIRED) -> 173;
met2idx(?METRIC_SOCKET_OPEN) -> 177;
met2idx(?METRIC_SOCKET_CLOSE) -> 178;
met2idx(?METRIC_SOCKET_ERROR) -> 179;
met2idx(?METRIC_BYTES_RECEIVED) -> 180;
met2idx(?METRIC_BYTES_SENT) -> 181;
met2idx(?METRIC_MSG_IN_RATE) -> 182;
met2idx(?METRIC_MSG_OUT_RATE) -> 183;
met2idx(?METRIC_BYTE_IN_RATE) -> 184;
met2idx(?METRIC_BYTE_OUT_RATE) -> 185;
met2idx(?METRIC_ROUTER_MATCHES_LOCAL) -> 186;
met2idx(?METRIC_ROUTER_MATCHES_REMOTE) -> 187;
met2idx(mqtt_connack_not_authorized_sent) -> 188;
met2idx(mqtt_connack_bad_credentials_sent) -> 189;
met2idx(mqtt_connack_server_unavailable_sent) -> 190;
met2idx(mqtt_connack_identifier_rejected_sent) -> 191;
met2idx(mqtt_connack_unacceptable_protocol_sent) -> 192;
met2idx(mqtt_connack_accepted_sent) -> 193;
met2idx(?METRIC_SOCKET_CLOSE_TIMEOUT) -> 194;
met2idx(?MQTT5_CLIENT_KEEPALIVE_EXPIRED) -> 195;
met2idx(?MQTT4_CLIENT_KEEPALIVE_EXPIRED) -> 196;
met2idx({?SIDECAR_EVENTS, ?ON_REGISTER}) -> 197;
met2idx({?SIDECAR_EVENTS, ?ON_PUBLISH}) -> 198;
met2idx({?SIDECAR_EVENTS, ?ON_SUBSCRIBE}) -> 199;
met2idx({?SIDECAR_EVENTS, ?ON_UNSUBSCRIBE}) -> 200;
met2idx({?SIDECAR_EVENTS, ?ON_DELIVER}) -> 201;
met2idx({?SIDECAR_EVENTS, ?ON_DELIVERY_COMPLETE}) -> 202;
met2idx({?SIDECAR_EVENTS, ?ON_OFFLINE_MESSAGE}) -> 203;
met2idx({?SIDECAR_EVENTS, ?ON_CLIENT_GONE}) -> 204;
met2idx({?SIDECAR_EVENTS, ?ON_CLIENT_WAKEUP}) -> 205;
met2idx({?SIDECAR_EVENTS, ?ON_CLIENT_OFFLINE}) -> 206;
met2idx({?SIDECAR_EVENTS, ?ON_SESSION_EXPIRED}) -> 207;
met2idx({?SIDECAR_EVENTS_ERROR, ?ON_REGISTER}) -> 208;
met2idx({?SIDECAR_EVENTS_ERROR, ?ON_PUBLISH}) -> 209;
met2idx({?SIDECAR_EVENTS_ERROR, ?ON_SUBSCRIBE}) -> 210;
met2idx({?SIDECAR_EVENTS_ERROR, ?ON_UNSUBSCRIBE}) -> 211;
met2idx({?SIDECAR_EVENTS_ERROR, ?ON_DELIVER}) -> 212;
met2idx({?SIDECAR_EVENTS_ERROR, ?ON_DELIVERY_COMPLETE}) -> 213;
met2idx({?SIDECAR_EVENTS_ERROR, ?ON_OFFLINE_MESSAGE}) -> 214;
met2idx({?SIDECAR_EVENTS_ERROR, ?ON_CLIENT_GONE}) -> 215;
met2idx({?SIDECAR_EVENTS_ERROR, ?ON_CLIENT_WAKEUP}) -> 216;
met2idx({?SIDECAR_EVENTS_ERROR, ?ON_CLIENT_OFFLINE}) -> 217;
met2idx({?SIDECAR_EVENTS_ERROR, ?ON_SESSION_EXPIRED}) -> 218;
met2idx({?REDIS_CMD, ?FUNCTION_LOAD, ?REMAP_SUBSCRIBER}) -> 219;
met2idx({?REDIS_CMD, ?FUNCTION_LOAD, ?SUBSCRIBE}) -> 220;
met2idx({?REDIS_CMD, ?FUNCTION_LOAD, ?UNSUBSCRIBE}) -> 221;
met2idx({?REDIS_CMD, ?FUNCTION_LOAD, ?DELETE_SUBSCRIBER}) -> 222;
met2idx({?REDIS_CMD, ?FUNCTION_LOAD, ?FETCH_SUBSCRIBER}) -> 223;
met2idx({?REDIS_CMD, ?FUNCTION_LOAD, ?FETCH_MATCHED_TOPIC_SUBSCRIBERS}) -> 224;
met2idx({?REDIS_CMD_ERROR, ?FUNCTION_LOAD, ?REMAP_SUBSCRIBER}) -> 225;
met2idx({?REDIS_CMD_ERROR, ?FUNCTION_LOAD, ?SUBSCRIBE}) -> 226;
met2idx({?REDIS_CMD_ERROR, ?FUNCTION_LOAD, ?UNSUBSCRIBE}) -> 227;
met2idx({?REDIS_CMD_ERROR, ?FUNCTION_LOAD, ?DELETE_SUBSCRIBER}) -> 228;
met2idx({?REDIS_CMD_ERROR, ?FUNCTION_LOAD, ?FETCH_SUBSCRIBER}) -> 229;
met2idx({?REDIS_CMD_ERROR, ?FUNCTION_LOAD, ?FETCH_MATCHED_TOPIC_SUBSCRIBERS}) -> 230;
met2idx({?REDIS_CMD, ?FCALL, ?REMAP_SUBSCRIBER}) -> 231;
met2idx({?REDIS_CMD_ERROR, ?FCALL, ?REMAP_SUBSCRIBER}) -> 232;
met2idx({?REDIS_CMD_MISS, ?FCALL, ?REMAP_SUBSCRIBER}) -> 233;
met2idx({?REDIS_STALE_CMD, ?FCALL, ?REMAP_SUBSCRIBER}) -> 234;
met2idx({?UNAUTH_REDIS_CMD, ?FCALL, ?REMAP_SUBSCRIBER}) -> 235;
met2idx({?REDIS_CMD, ?FCALL, ?SUBSCRIBE}) -> 236;
met2idx({?REDIS_CMD_ERROR, ?FCALL, ?SUBSCRIBE}) -> 237;
met2idx({?REDIS_CMD_MISS, ?FCALL, ?SUBSCRIBE}) -> 238;
met2idx({?REDIS_STALE_CMD, ?FCALL, ?SUBSCRIBE}) -> 239;
met2idx({?UNAUTH_REDIS_CMD, ?FCALL, ?SUBSCRIBE}) -> 240;
met2idx({?REDIS_CMD, ?FCALL, ?UNSUBSCRIBE}) -> 241;
met2idx({?REDIS_CMD_ERROR, ?FCALL, ?UNSUBSCRIBE}) -> 242;
met2idx({?REDIS_CMD_MISS, ?FCALL, ?UNSUBSCRIBE}) -> 243;
met2idx({?REDIS_STALE_CMD, ?FCALL, ?UNSUBSCRIBE}) -> 244;
met2idx({?UNAUTH_REDIS_CMD, ?FCALL, ?UNSUBSCRIBE}) -> 245;
met2idx({?REDIS_CMD, ?FCALL, ?DELETE_SUBSCRIBER}) -> 246;
met2idx({?REDIS_CMD_ERROR, ?FCALL, ?DELETE_SUBSCRIBER}) -> 247;
met2idx({?REDIS_CMD_MISS, ?FCALL, ?DELETE_SUBSCRIBER}) -> 248;
met2idx({?REDIS_STALE_CMD, ?FCALL, ?DELETE_SUBSCRIBER}) -> 249;
met2idx({?UNAUTH_REDIS_CMD, ?FCALL, ?DELETE_SUBSCRIBER}) -> 250;
met2idx({?REDIS_CMD, ?FCALL, ?FETCH_MATCHED_TOPIC_SUBSCRIBERS}) -> 251;
met2idx({?REDIS_CMD_ERROR, ?FCALL, ?FETCH_MATCHED_TOPIC_SUBSCRIBERS}) -> 252;
met2idx({?REDIS_CMD_MISS, ?FCALL, ?FETCH_MATCHED_TOPIC_SUBSCRIBERS}) -> 253;
met2idx({?REDIS_STALE_CMD, ?FCALL, ?FETCH_MATCHED_TOPIC_SUBSCRIBERS}) -> 254;
met2idx({?UNAUTH_REDIS_CMD, ?FCALL, ?FETCH_MATCHED_TOPIC_SUBSCRIBERS}) -> 255;
met2idx({?REDIS_CMD, ?FCALL, ?FETCH_SUBSCRIBER}) -> 256;
met2idx({?REDIS_CMD_ERROR, ?FCALL, ?FETCH_SUBSCRIBER}) -> 257;
met2idx({?REDIS_CMD_MISS, ?FCALL, ?FETCH_SUBSCRIBER}) -> 258;
met2idx({?REDIS_STALE_CMD, ?FCALL, ?FETCH_SUBSCRIBER}) -> 259;
met2idx({?UNAUTH_REDIS_CMD, ?FCALL, ?FETCH_SUBSCRIBER}) -> 260;
met2idx({?REDIS_CMD, ?FUNCTION_LOAD, ?ENQUEUE_MSG}) -> 261;
met2idx({?REDIS_CMD, ?FUNCTION_LOAD, ?POLL_MAIN_QUEUE}) -> 262;
met2idx({?REDIS_CMD_ERROR, ?FUNCTION_LOAD, ?ENQUEUE_MSG}) -> 263;
met2idx({?REDIS_CMD_ERROR, ?FUNCTION_LOAD, ?POLL_MAIN_QUEUE}) -> 264;
met2idx({?REDIS_CMD, ?FCALL, ?ENQUEUE_MSG}) -> 265;
met2idx({?REDIS_CMD, ?FCALL, ?POLL_MAIN_QUEUE}) -> 266;
met2idx({?REDIS_CMD_ERROR, ?FCALL, ?ENQUEUE_MSG}) -> 267;
met2idx({?REDIS_CMD_ERROR, ?FCALL, ?POLL_MAIN_QUEUE}) -> 268;
met2idx({?REDIS_CMD_MISS, ?FCALL, ?ENQUEUE_MSG}) -> 269;
met2idx({?REDIS_CMD_MISS, ?FCALL, ?POLL_MAIN_QUEUE}) -> 270;
met2idx({?REDIS_STALE_CMD, ?FCALL, ?ENQUEUE_MSG}) -> 271;
met2idx({?REDIS_STALE_CMD, ?FCALL, ?POLL_MAIN_QUEUE}) -> 272;
met2idx({?UNAUTH_REDIS_CMD, ?FCALL, ?ENQUEUE_MSG}) -> 273;
met2idx({?UNAUTH_REDIS_CMD, ?FCALL, ?POLL_MAIN_QUEUE}) -> 274;
met2idx({?METRIC_MSG_STORE_OPS_ERRORS, ?WRITE}) -> 275;
met2idx({?METRIC_MSG_STORE_OPS_ERRORS, ?DELETE}) -> 276;
met2idx({?METRIC_MSG_STORE_OPS_ERRORS, ?DELETE_ALL}) -> 277;
met2idx({?METRIC_MSG_STORE_OPS_ERRORS, ?READ}) -> 278;
met2idx({?METRIC_MSG_STORE_OPS_ERRORS, ?FIND}) -> 279;
met2idx({?METRIC_MSG_STORE_RETRY_EXHAUSTED, ?WRITE}) -> 280;
met2idx({?METRIC_MSG_STORE_RETRY_EXHAUSTED, ?DELETE}) -> 281;
met2idx({?METRIC_MSG_STORE_RETRY_EXHAUSTED, ?DELETE_ALL}) -> 282;
met2idx({?METRIC_MSG_STORE_RETRY_EXHAUSTED, ?READ}) -> 283;
met2idx({?METRIC_MSG_STORE_RETRY_EXHAUSTED, ?FIND}) -> 284;
met2idx({?REDIS_CMD, ?RPUSH, ?MSG_STORE_WRITE}) -> 285;
met2idx({?REDIS_CMD, ?DEL, ?MSG_STORE_DELETE}) -> 286;
met2idx({?REDIS_CMD, ?FIND, ?MSG_STORE_FIND}) -> 287;
met2idx({?REDIS_CMD_ERROR, ?RPUSH, ?MSG_STORE_WRITE}) -> 288;
met2idx({?REDIS_CMD_ERROR, ?DEL, ?MSG_STORE_DELETE}) -> 289;
met2idx({?REDIS_CMD_ERROR, ?FIND, ?MSG_STORE_FIND}) -> 290;
met2idx({?REDIS_CMD_MISS, ?RPUSH, ?MSG_STORE_WRITE}) -> 291;
met2idx({?REDIS_CMD_MISS, ?DEL, ?MSG_STORE_DELETE}) -> 292;
met2idx({?REDIS_CMD_MISS, ?FIND, ?MSG_STORE_FIND}) -> 293;
met2idx({?REDIS_CMD, ?LPOP, ?MSG_STORE_DELETE}) -> 294;
met2idx({?REDIS_CMD_ERROR, ?LPOP, ?MSG_STORE_DELETE}) -> 295;
met2idx({?REDIS_CMD_MISS, ?LPOP, ?MSG_STORE_DELETE}) -> 296;
met2idx({?QOS1_SUBSCRIPTION_OPTS, ?NON_RETRY, ?NON_PERSISTENCE}) -> 297;
met2idx({?QOS1_SUBSCRIPTION_OPTS, ?RETRY, ?NON_PERSISTENCE}) -> 298;
met2idx({?QOS1_SUBSCRIPTION_OPTS, ?NON_RETRY, ?PERSISTENCE}) -> 299;
met2idx({?QOS1_SUBSCRIPTION_OPTS, ?RETRY, ?PERSISTENCE}) -> 300;
met2idx(?QOS1_NON_RETRY_DROPPED) -> 301;
met2idx(?QOS1_NON_PERSISTENCE_DROPPED) -> 302;
met2idx({?CACHE_HIT, ?LOCAL_SHARED_SUBS}) -> 303;
met2idx({?CACHE_MISS, ?LOCAL_SHARED_SUBS}) -> 304;
met2idx({?CACHE_INSERT, ?LOCAL_SHARED_SUBS}) -> 305;
met2idx({?CACHE_DELETE, ?LOCAL_SHARED_SUBS}) -> 306;
met2idx({?REDIS_CMD, ?FCALL, ?GET_LIVE_NODES}) -> 307;
met2idx({?REDIS_CMD_ERROR, ?FCALL, ?GET_LIVE_NODES}) -> 308;
met2idx({?REDIS_CMD_MISS, ?FCALL, ?GET_LIVE_NODES}) -> 309;
met2idx({?REDIS_STALE_CMD, ?FCALL, ?GET_LIVE_NODES}) -> 310;
met2idx({?UNAUTH_REDIS_CMD, ?FCALL, ?GET_LIVE_NODES}) -> 311;
met2idx({?REDIS_CMD, ?FUNCTION_LOAD, ?GET_LIVE_NODES}) -> 312;
met2idx({?REDIS_CMD_ERROR, ?FUNCTION_LOAD, ?GET_LIVE_NODES}) -> 313;
met2idx({?REDIS_CMD, ?FCALL, ?MIGRATE_OFFLINE_QUEUE}) -> 314;
met2idx({?REDIS_CMD_ERROR, ?FCALL, ?MIGRATE_OFFLINE_QUEUE}) -> 315;
met2idx({?REDIS_CMD_MISS, ?FCALL, ?MIGRATE_OFFLINE_QUEUE}) -> 316;
met2idx({?REDIS_STALE_CMD, ?FCALL, ?MIGRATE_OFFLINE_QUEUE}) -> 317;
met2idx({?UNAUTH_REDIS_CMD, ?FCALL, ?MIGRATE_OFFLINE_QUEUE}) -> 318;
met2idx({?REDIS_CMD, ?FUNCTION_LOAD, ?MIGRATE_OFFLINE_QUEUE}) -> 319;
met2idx({?REDIS_CMD_ERROR, ?FUNCTION_LOAD, ?MIGRATE_OFFLINE_QUEUE}) -> 320;
met2idx({?REDIS_CMD, ?FCALL, ?REAP_SUBSCRIBERS}) -> 321;
met2idx({?REDIS_CMD_ERROR, ?FCALL, ?REAP_SUBSCRIBERS}) -> 322;
met2idx({?REDIS_CMD_MISS, ?FCALL, ?REAP_SUBSCRIBERS}) -> 323;
met2idx({?REDIS_STALE_CMD, ?FCALL, ?REAP_SUBSCRIBERS}) -> 324;
met2idx({?UNAUTH_REDIS_CMD, ?FCALL, ?REAP_SUBSCRIBERS}) -> 325;
met2idx({?REDIS_CMD, ?FUNCTION_LOAD, ?REAP_SUBSCRIBERS}) -> 326;
met2idx({?REDIS_CMD_ERROR, ?FUNCTION_LOAD, ?REAP_SUBSCRIBERS}) -> 327;
met2idx({?REDIS_CMD, ?SCARD, ?ENSURE_NO_LOCAL_CLIENT}) -> 328;
met2idx({?REDIS_CMD_ERROR, ?SCARD, ?ENSURE_NO_LOCAL_CLIENT}) -> 329;
met2idx({?REDIS_CMD_MISS, ?SCARD, ?ENSURE_NO_LOCAL_CLIENT}) -> 330;
met2idx(msg_enqueue_subscriber_not_found) -> 331;
met2idx({?MQTT_DISONNECT, ?REASON_NOT_AUTHORIZED}) -> 332;
met2idx({?MQTT_DISONNECT, ?REASON_NORMAL_DISCONNECT}) -> 333;
met2idx({?MQTT_DISONNECT, ?REASON_SESSION_TAKEN_OVER}) -> 334;
met2idx({?MQTT_DISONNECT, ?REASON_ADMINISTRATIVE_ACTION}) -> 335;
met2idx({?MQTT_DISONNECT, ?REASON_DISCONNECT_KEEP_ALIVE}) -> 336;
met2idx({?MQTT_DISONNECT, ?REASON_DISCONNECT_MIGRATION}) -> 337;
met2idx({?MQTT_DISONNECT, ?REASON_BAD_AUTHENTICATION_METHOD}) -> 338;
met2idx({?MQTT_DISONNECT, ?REASON_REMOTE_SESSION_TAKEN_OVER}) -> 339;
met2idx({?MQTT_DISONNECT, ?REASON_MQTT_CLIENT_DISCONNECT}) -> 340;
met2idx({?MQTT_DISONNECT, ?REASON_RECEIVE_MAX_EXCEEDED}) -> 341;
met2idx({?MQTT_DISONNECT, ?REASON_PROTOCOL_ERROR}) -> 342;
met2idx({?MQTT_DISONNECT, ?REASON_PUBLISH_AUTH_ERROR}) -> 343;
met2idx({?MQTT_DISONNECT, ?REASON_INVALID_PUBREC_ERROR}) -> 344;
met2idx({?MQTT_DISONNECT, ?REASON_INVALID_PUBCOMP_ERROR}) -> 345;
met2idx({?MQTT_DISONNECT, ?REASON_UNEXPECTED_FRAME_TYPE}) -> 346;
met2idx({?MQTT_DISONNECT, ?REASON_EXIT_SIGNAL_RECEIVED}) -> 347;
met2idx({?MQTT_DISONNECT, ?REASON_TCP_CLOSED}) -> 348;
met2idx({?MQTT_DISONNECT, ?REASON_UNSPECIFIED}) -> 349;
met2idx(shared_subscription_group_publish_attempt_failed) -> 350;
met2idx(?METRIC_WEB_SOCKET_OPEN) -> 351;
met2idx(?METRIC_WEB_SOCKET_CLOSE) -> 352.

-ifdef(TEST).
clear_stored_rates() ->
    gen_server:call(?MODULE, clear_rates).

start_calc_rates_interval() ->
    gen_server:call(?MODULE, start_calc_rates).

cancel_calc_rates_interval() ->
    gen_server:call(?MODULE, cancel_calc_rates).
-endif.

metric_name({Metric, SubMetric, Labels}) ->
    LMetric = atom_to_list(Metric),
    LSubMetric = atom_to_list(SubMetric),
    Name = list_to_atom(LMetric ++ "_" ++ LSubMetric ++ "_microseconds"),
    Description = list_to_binary(
        "A histogram of the " ++ LMetric ++ " " ++ LSubMetric ++ " latency."
    ),
    {[Name | Labels], Name, Description, Labels};
metric_name({Metric, SubMetric}) ->
    LMetric = atom_to_list(Metric),
    LSubMetric = atom_to_list(SubMetric),
    Name = list_to_atom(LMetric ++ "_" ++ LSubMetric ++ "_microseconds"),
    Description = list_to_binary(
        "A histogram of the " ++ LMetric ++ " " ++ LSubMetric ++ " latency."
    ),
    {Name, Name, Description, []}.

topic_metric_name({Metric, SubMetric, Labels}) ->
    LMetric = atom_to_list(Metric),
    LSubMetric = atom_to_list(SubMetric),
    MetricName = list_to_atom(LSubMetric ++ "_" ++ LMetric),
    Description = list_to_binary(
        "The number of " ++ LSubMetric ++ " packets on ACL matched topics."
    ),
    {[MetricName | Labels], MetricName, Description, Labels}.

events_sampled_metric_name(H, C, SDType) ->
    Name = list_to_atom("vmq_events_" ++ SDType),
    Labels = [{hook, atom_to_list(H)}, {acl_name, C}],
    Description = list_to_binary(
        "The number of events " ++ SDType ++ " due to sampling enabled."
    ),
    {[Name | Labels], Name, Description, Labels}.

config_version_metric_name({MetricName, Labels}) ->
    Description = list_to_binary("The current version of the config file."),
    {[MetricName | Labels], MetricName, Description, Labels}.
