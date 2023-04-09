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
-module(vmq_shared_subscriptions).
-include("vmq_server.hrl").

-export([publish/5, publish_to_group/3]).

publish(Msg, Policy, SubscriberGroups, LocalMatches, RemoteMatches) ->
    publish(Msg, Policy, SubscriberGroups, {LocalMatches, RemoteMatches}).

publish(_, _, undefined, Acc) ->
    Acc;
publish(Msg, Policy, SubscriberGroups, Acc) when is_map(SubscriberGroups) ->
    publish(Msg, Policy, maps:to_list(SubscriberGroups), Acc);
publish(_, _, [], Acc) ->
    Acc;
publish(Msg, Policy, [{Group, []} | Rest], Acc) ->
    lager:debug("can't publish to shared subscription ~p due to no subscribers, msg: ~p", [
        Group, Msg
    ]),
    publish(Msg, Policy, Rest, Acc);
publish(Msg, Policy, [{Group, SubscriberGroup} | Rest], Acc0) ->
    Subscribers = filter_subscribers(SubscriberGroup, Policy),
    %% Randomize subscribers once.
    RandSubscribers = [S || {_, S} <- lists:sort([{rand:uniform(), N} || N <- Subscribers])],
    case publish_to_group(Msg, RandSubscribers, Acc0) of
        {ok, Acc1} ->
            publish(Msg, Policy, Rest, Acc1);
        {error, Reason} ->
            lager:debug(
                "can't publish to shared subscription ~p due to '~p', msg: ~p",
                [Group, Reason, Msg]
            ),
            publish(Msg, Policy, Rest, Acc0)
    end.

publish_to_group(_Msg, [], _Acc0) ->
    {error, no_subscribers};
publish_to_group(
    Msg, [{Node, _SId, _SubInfo} = Subscriber | Rest] = RandSubs, {Local, Remote} = Acc0
) ->
    case publish_online(Msg, Subscriber, Acc0) of
        {error, different_node} ->
            case vmq_redis_queue:enqueue(Node, term_to_binary(RandSubs), term_to_binary(Msg)) of
                ok ->
                    {ok, {Local, Remote + 1}};
                E ->
                    E
            end;
        {ok, Acc1} ->
            {ok, Acc1};
        _ when length(Rest) == 0 ->
            publish_any(Msg, [Subscriber], Acc0);
        _ ->
            publish_to_group(Msg, Rest, Acc0)
    end.

publish_online(Msg, {Node, SId, SubInfo}, Acc0) when Node == node() ->
    case publish_(Msg, {SId, SubInfo}, online, Acc0) of
        {ok, _Acc1} = Res ->
            Res;
        _ ->
            {error, cannot_publish}
    end;
publish_online(_, _, _) ->
    {error, different_node}.

publish_any(_Msg, [], _Acc) ->
    {error, no_subscribers};
publish_any(Msg, [{_Node, SId, SubInfo} | Subscribers], Acc0) ->
    case publish_(Msg, {SId, SubInfo}, any, Acc0) of
        {ok, Acc1} ->
            {ok, Acc1};
        {error, _} ->
            publish_any(Msg, Subscribers, Acc0)
    end.

publish_(Msg0, {SubscriberId, SubInfo}, QState, {Local, Remote}) ->
    case vmq_reg:get_queue_pid(SubscriberId) of
        not_found ->
            {error, not_found};
        QPid ->
            try
                {QoS, Msg1} = maybe_add_sub_id(SubInfo, Msg0),
                case vmq_queue:enqueue_many(QPid, [{deliver, QoS, Msg1}], #{states => [QState]}) of
                    ok ->
                        {ok, {Local + 1, Remote}};
                    E ->
                        E
                end
            catch
                _:_ ->
                    {error, cant_enqueue}
            end
    end.

filter_subscribers(Subscribers, random) ->
    Subscribers;
filter_subscribers(Subscribers, prefer_local) ->
    Node = node(),
    LocalSubscribers =
        lists:filter(
            fun
                ({N, _, _}) when N == Node -> true;
                (_) -> false
            end,
            Subscribers
        ),
    case LocalSubscribers of
        [] -> Subscribers;
        _ -> LocalSubscribers
    end;
filter_subscribers(Subscribers, local_only) ->
    %% filtered in `vmq_reg`.
    Subscribers.

maybe_add_sub_id({QoS, #{sub_id := SubId}}, #vmq_msg{properties = Props} = Msg) ->
    {QoS, Msg#vmq_msg{properties = Props#{p_subscription_id => [SubId]}}};
maybe_add_sub_id({QoS, _UnusedSubInfo}, Msg) ->
    {QoS, Msg};
maybe_add_sub_id(QoS, Msg) ->
    {QoS, Msg}.
