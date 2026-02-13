%% Copyright Gojek

-module(vmq_metrics_plus_plugin).

-include_lib("vernemq_dev/include/vernemq_dev.hrl").
-include_lib("vmq_commons/src/vmq_types_common.hrl").

-behaviour(on_publish_hook).
-behaviour(on_subscribe_hook).
-behaviour(on_deliver_hook).
-behaviour(on_delivery_complete_hook).
-behaviour(on_message_drop_hook).

-export([
    on_subscribe/4,
    on_publish/8,
    on_deliver/9,
    on_delivery_complete/9,
    on_message_drop/4
]).

-export([
    start/0,
    stop/0
]).

-type reason() :: atom().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Plugin Callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start() ->
    {ok, _} = application:ensure_all_started(vmq_metrics_plus_plugin),
    ok.

stop() ->
    application:stop(vmq_metrics_plus_plugin).

%%--------------------------------------------------------------------
%%%===================================================================
%%% Hook functions
%%%===================================================================
%% called as an all_till_ok hook
%%
-spec on_subscribe(username(), subscriber_id(), [{topic(), qos(), matched_acl()}], session_id()) ->
    'ok'.
on_subscribe(_UserName, _SubscriberId, Topics, _SessionId) ->
    lists:foreach(
        fun(T) ->
            {_Topic, QoS, #matched_acl{name = Name}} = T,
            vmq_metrics_plus:incr_matched_topic(Name, subscribe, QoS)
        end,
        Topics
    ).

-spec on_publish(
    username(), subscriber_id(), qos(), topic(), payload(), flag(), matched_acl(), session_id()
) ->
    'ok'.
on_publish(
    _UserName,
    _SubscriberId,
    QoS,
    _Topic,
    _Payload,
    _IsRetain,
    #matched_acl{name = Name},
    _SessionId
) ->
    vmq_metrics_plus:incr_matched_topic(Name, publish, QoS),
    ok.

-spec on_deliver(
    username(),
    subscriber_id(),
    qos(),
    topic(),
    payload(),
    flag(),
    matched_acl(),
    flag(),
    session_id()
) -> 'ok'.
on_deliver(
    _UserName,
    _SubscriberId,
    QoS,
    _Topic,
    _Payload,
    _IsRetain,
    #matched_acl{name = Name},
    _Persisted,
    _SessionId
) ->
    vmq_metrics_plus:incr_matched_topic(Name, deliver, QoS),
    ok.

-spec on_delivery_complete(
    username(),
    subscriber_id(),
    qos(),
    topic(),
    payload(),
    flag(),
    matched_acl(),
    flag(),
    session_id()
) ->
    'ok'.
on_delivery_complete(
    _UserName,
    _SubscriberId,
    QoS,
    _Topic,
    _Payload,
    _IsRetain,
    #matched_acl{name = Name},
    _Persisted,
    _SessionId
) ->
    vmq_metrics_plus:incr_matched_topic(Name, delivery_complete, QoS),
    ok.

-spec on_message_drop(subscriber_id(), fun(), reason(), session_id()) -> 'next'.
on_message_drop(SubscriberId, Fun, Reason, _SessionId) ->
    case Fun() of
        {_Topic, QoS, _Payload, _Props, #matched_acl{name = Name}} ->
            vmq_metrics_plus:incr_matched_topic(Name, message_drop, QoS),
            ok;
        _ ->
            lager:error("unexpected pattern in on_message_drop hook for ~p due to reason ~p", [
                SubscriberId, Reason
            ]),
            next
    end.
