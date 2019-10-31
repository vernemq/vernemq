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

-export([publish/5]).

publish(Msg, Policy, SubscriberGroups, LocalMatches, RemoteMatches) ->
    publish(Msg, Policy, SubscriberGroups, {LocalMatches, RemoteMatches}).

publish(_,_, undefined, Acc) -> Acc;
publish(Msg, Policy, SubscriberGroups, Acc) when is_map(SubscriberGroups) ->
    publish(Msg, Policy, maps:to_list(SubscriberGroups), Acc);
publish(_,_, [], Acc) -> Acc;
publish(Msg, Policy, [{Group, []}|Rest], Acc) ->
    lager:debug("can't publish to shared subscription ~p due to no subscribers, msg: ~p", [Group, Msg]),
    publish(Msg, Policy, Rest, Acc);
publish(Msg, Policy, [{Group, SubscriberGroup}|Rest], Acc0) ->
    Subscribers = filter_subscribers(SubscriberGroup, Policy),
    %% Randomize subscribers once.
    RandSubscribers = [S||{_,S} <- lists:sort([{rand:uniform(), N} || N <- Subscribers])],
    case publish_to_group(Msg, RandSubscribers, Acc0) of
        {ok, Acc1} ->
            publish(Msg, Policy, Rest, Acc1);
        {error, Reason} ->
            lager:debug("can't publish to shared subscription ~p due to '~p', msg: ~p",
                        [Group, Reason, Msg]),
            publish(Msg, Policy, Rest, Acc0)
    end.

publish_to_group(Msg, Subscribers, Acc0) ->
    case publish_online(Msg, Subscribers, Acc0) of
        {ok, Acc1} ->
            {ok, Acc1};
        NotOnlineSubscribers ->
            publish_any(Msg, NotOnlineSubscribers, Acc0)
    end.

publish_online(Msg, Subscribers, Acc0) ->
    try
        lists:foldl(
          fun(Subscriber, SubAcc) ->
                  case publish_(Msg, Subscriber, online, Acc0) of
                      {ok, Acc1} ->
                          throw({done, Acc1});
                      {error, offline} ->
                          [Subscriber|SubAcc];
                      {error, draining} ->
                          [Subscriber|SubAcc];
                      _ ->
                          SubAcc
                  end
          end, [], Subscribers)
    catch
        {done, Acc2} -> {ok, Acc2}
    end.

publish_any(_Msg, [], _Acc) -> {error, no_subscribers};
publish_any(Msg, [Subscriber|Subscribers], Acc0) ->
    case publish_(Msg, Subscriber, any, Acc0) of
        {ok, Acc1} ->
            {ok, Acc1};
        {error, _} ->
            publish_any(Msg, Subscribers, Acc0)
    end.

publish_(Msg0, {Node, SubscriberId, SubInfo}, QState, {Local, Remote}) when Node == node() ->
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
    end;
publish_(Msg0, {Node, SubscriberId, SubInfo}, QState, {Local, Remote}) ->
    {QoS, Msg1} = maybe_add_sub_id(SubInfo, Msg0),
    Term = {enqueue_many, SubscriberId, [{deliver, QoS, Msg1}], #{states => [QState]}},
    case vmq_cluster:remote_enqueue(Node, Term, true) of
        ok ->
            {ok, {Local, Remote + 1}};
        E ->
            E
    end.

filter_subscribers(Subscribers, random) ->
    Subscribers;
filter_subscribers(Subscribers, prefer_local) ->
    Node = node(),
    LocalSubscribers =
        lists:filter(fun({N, _, _}) when N == Node -> true;
                        (_) -> false
                     end, Subscribers),
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
