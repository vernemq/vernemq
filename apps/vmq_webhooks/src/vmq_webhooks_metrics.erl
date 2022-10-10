-module(vmq_webhooks_metrics).

-export([init/0, incr/2, incr/3, metrics/0]).

-include_lib("vmq_server/include/vmq_metrics.hrl").

-type counter_type() :: requests | errors | bytes_sent.
-type hook() ::
    on_register
    | on_register_m5
    | auth_on_publish
    | auth_on_publish_m5
    | auth_on_register
    | auth_on_register_m5
    | auth_on_subscribe
    | auth_on_subscribe_m5
    | on_auth_m5
    | on_deliver
    | on_deliver_m5
    | on_unsubscribe
    | on_unsubscribe_m5
    | on_publish
    | on_publish_m5
    | on_subscribe
    | on_subscribe_m5
    | on_message_drop
    | on_topic_unsubscribed
    | on_session_expired
    | on_offline_message
    | on_config_change
    | on_client_wakeup
    | on_client_offline
    | on_client_gone.

-spec metrics() -> [{Type, Labels, Id, Description, Name, Value}] when
    Type :: atom(),
    Labels :: [metric_label()],
    Id :: metric_id(),
    Description :: undefined | binary(),
    Name :: binary(),
    Value :: term().
metrics() ->
    MetricKeys = [{H, CT} || H <- all_hooks(), CT <- all_counter_types()],
    CounterRef = persistent_term:get(?MODULE),
    [
        hook_and_counter_type_to_metric(CounterRef, Hook, CounterType)
     || {Hook, CounterType} <- MetricKeys
    ].

hook_and_counter_type_to_metric(CounterRef, Hook, CounterType) ->
    Index = met2idx(Hook, CounterType),
    HookBin = atom_to_binary(Hook, utf8),
    CounterTypeBin = atom_to_binary(CounterType, utf8),
    Name = <<"webhooks_", HookBin/binary, "_", CounterTypeBin/binary>>,
    Id = binary_to_atom(Name, utf8),
    Description =
        case CounterType of
            requests ->
                <<"Number of requests to ", HookBin/binary, " webhooks">>;
            errors ->
                <<"Number of errors calling ", HookBin/binary, " webhooks">>;
            bytes_sent ->
                <<"Number of bytes sent to ", HookBin/binary, " webhooks">>
        end,
    Value = counters:get(CounterRef, Index),
    {counter, [], Id, Id, Description, Value}.

-spec init() -> ok.
init() ->
    NumCounters = length(all_hooks()) * length(all_counter_types()),
    CounterRef = counters:new(NumCounters, [write_concurrency]),
    persistent_term:put(?MODULE, CounterRef),
    application:set_env(vmq_webhooks, vmq_metrics_mfa, {?MODULE, metrics, []}).

-spec incr(hook(), counter_type()) -> ok.
incr(Hook, CounterType) ->
    incr(Hook, CounterType, 1).

-spec incr(hook(), counter_type(), non_neg_integer()) -> ok.
incr(Hook, CounterType, Value) ->
    CounterRef = persistent_term:get(?MODULE),
    Index = met2idx(Hook, CounterType),
    counters:add(CounterRef, Index, Value).

-spec all_hooks() -> [hook()].
all_hooks() ->
    [
        on_register,
        on_register_m5,
        auth_on_publish,
        auth_on_publish_m5,
        auth_on_register,
        auth_on_register_m5,
        auth_on_subscribe,
        auth_on_subscribe_m5,
        on_auth_m5,
        on_deliver,
        on_deliver_m5,
        on_unsubscribe,
        on_unsubscribe_m5,
        on_publish,
        on_publish_m5,
        on_subscribe,
        on_subscribe_m5,
        on_message_drop,
        on_topic_unsubscribed,
        on_session_expired,
        on_offline_message,
        on_config_change,
        on_client_wakeup,
        on_client_offline,
        on_client_gone
    ].

-spec all_counter_types() -> [counter_type()].
all_counter_types() ->
    [requests, errors, bytes_sent].

-spec met2idx(hook(), counter_type()) -> non_neg_integer().
met2idx(on_register, requests) ->
    1;
met2idx(on_register_m5, requests) ->
    2;
met2idx(auth_on_publish, requests) ->
    3;
met2idx(auth_on_publish_m5, requests) ->
    4;
met2idx(auth_on_register, requests) ->
    5;
met2idx(auth_on_register_m5, requests) ->
    6;
met2idx(auth_on_subscribe, requests) ->
    7;
met2idx(auth_on_subscribe_m5, requests) ->
    8;
met2idx(on_auth_m5, requests) ->
    9;
met2idx(on_deliver, requests) ->
    10;
met2idx(on_deliver_m5, requests) ->
    11;
met2idx(on_unsubscribe, requests) ->
    12;
met2idx(on_unsubscribe_m5, requests) ->
    13;
met2idx(on_publish, requests) ->
    14;
met2idx(on_publish_m5, requests) ->
    15;
met2idx(on_subscribe, requests) ->
    16;
met2idx(on_subscribe_m5, requests) ->
    17;
met2idx(on_message_drop, requests) ->
    18;
met2idx(on_topic_unsubscribed, requests) ->
    19;
met2idx(on_session_expired, requests) ->
    20;
met2idx(on_offline_message, requests) ->
    21;
met2idx(on_config_change, requests) ->
    22;
met2idx(on_client_wakeup, requests) ->
    23;
met2idx(on_client_offline, requests) ->
    24;
met2idx(on_client_gone, requests) ->
    25;
met2idx(on_register, errors) ->
    26;
met2idx(on_register_m5, errors) ->
    27;
met2idx(auth_on_publish, errors) ->
    28;
met2idx(auth_on_publish_m5, errors) ->
    29;
met2idx(auth_on_register, errors) ->
    30;
met2idx(auth_on_register_m5, errors) ->
    31;
met2idx(auth_on_subscribe, errors) ->
    32;
met2idx(auth_on_subscribe_m5, errors) ->
    33;
met2idx(on_auth_m5, errors) ->
    34;
met2idx(on_deliver, errors) ->
    35;
met2idx(on_deliver_m5, errors) ->
    36;
met2idx(on_unsubscribe, errors) ->
    37;
met2idx(on_unsubscribe_m5, errors) ->
    38;
met2idx(on_publish, errors) ->
    39;
met2idx(on_publish_m5, errors) ->
    40;
met2idx(on_subscribe, errors) ->
    41;
met2idx(on_subscribe_m5, errors) ->
    42;
met2idx(on_message_drop, errors) ->
    43;
met2idx(on_topic_unsubscribed, errors) ->
    44;
met2idx(on_session_expired, errors) ->
    45;
met2idx(on_offline_message, errors) ->
    46;
met2idx(on_config_change, errors) ->
    47;
met2idx(on_client_wakeup, errors) ->
    48;
met2idx(on_client_offline, errors) ->
    49;
met2idx(on_client_gone, errors) ->
    50;
met2idx(on_register, bytes_sent) ->
    51;
met2idx(on_register_m5, bytes_sent) ->
    52;
met2idx(auth_on_publish, bytes_sent) ->
    53;
met2idx(auth_on_publish_m5, bytes_sent) ->
    54;
met2idx(auth_on_register, bytes_sent) ->
    55;
met2idx(auth_on_register_m5, bytes_sent) ->
    56;
met2idx(auth_on_subscribe, bytes_sent) ->
    57;
met2idx(auth_on_subscribe_m5, bytes_sent) ->
    58;
met2idx(on_auth_m5, bytes_sent) ->
    59;
met2idx(on_deliver, bytes_sent) ->
    60;
met2idx(on_deliver_m5, bytes_sent) ->
    61;
met2idx(on_unsubscribe, bytes_sent) ->
    62;
met2idx(on_unsubscribe_m5, bytes_sent) ->
    63;
met2idx(on_publish, bytes_sent) ->
    64;
met2idx(on_publish_m5, bytes_sent) ->
    65;
met2idx(on_subscribe, bytes_sent) ->
    66;
met2idx(on_subscribe_m5, bytes_sent) ->
    67;
met2idx(on_message_drop, bytes_sent) ->
    68;
met2idx(on_topic_unsubscribed, bytes_sent) ->
    69;
met2idx(on_session_expired, bytes_sent) ->
    70;
met2idx(on_offline_message, bytes_sent) ->
    71;
met2idx(on_config_change, bytes_sent) ->
    72;
met2idx(on_client_wakeup, bytes_sent) ->
    73;
met2idx(on_client_offline, bytes_sent) ->
    74;
met2idx(on_client_gone, bytes_sent) ->
    75.
