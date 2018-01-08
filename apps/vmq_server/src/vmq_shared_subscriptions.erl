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

-export([publish/3]).

publish(_,_, undefined) -> ok;
publish(Msg, Policy, SubscriberGroups) when is_map(SubscriberGroups) ->
    publish(Msg, Policy, maps:to_list(SubscriberGroups));
publish(_,_, []) -> ok;
publish(Msg, Policy, [{Group, []}|Rest]) ->
    lager:debug("can't publish to shared subscription ~p due to no subscribers, msg: ~p", [Group, Msg]),
    publish(Msg, Policy, Rest);
publish(Msg, Policy, [{Group, SubscriberGroup}|Rest]) ->
    Subscribers = filter_subscribers(SubscriberGroup, Policy),
    %% Randomize subscribers once.
    RandSubscribers = [S||{_,S} <- lists:sort([{rand:uniform(), N} || N <- Subscribers])],
    case publish_to_group(Msg, RandSubscribers) of
        ok ->
            publish(Msg, Policy, Rest);
        {error, Reason} ->
            lager:debug("can't publish to shared subscription ~p due to '~p', msg: Msg",
                        [Group, Reason, Msg]),
            publish(Msg, Policy, Rest)
    end.

publish_to_group(Msg, Subscribers) ->
    case publish_online(Msg, Subscribers) of
        ok ->
            ok;
        NotOnlineSubscribers ->
            publish_any(Msg, NotOnlineSubscribers)
    end.

publish_online(Msg, Subscribers) ->
    try
        lists:foldl(
          fun(Subscriber, Acc) ->
                  case publish_(Msg, Subscriber, online) of
                      ok ->
                          throw(done);
                      {error, offline} ->
                          [Subscriber|Acc];
                      {error, draining} ->
                          [Subscriber|Acc];
                      _ ->
                          Acc
                  end
          end, Subscribers, [])
    catch
        done -> ok
    end.

publish_any(_Msg, []) -> {error, no_subscribers};
publish_any(Msg, [Subscriber|Subscribers]) ->
    case publish_(Msg, Subscriber, any) of
        ok ->
            ok;
        {error, _} ->
            publish_any(Msg, Subscribers)
    end.

publish_(Msg, {Node, SubscriberId, QoS}, QState) when Node == node() ->
    case vmq_reg:get_queue_pid(SubscriberId) of
        not_found ->
            {error, not_found};
        QPid ->
            try
                vmq_queue:enqueue_many(QPid, [{deliver, QoS, Msg}], #{states => [QState]})
            catch
                _:_ ->
                    {error, cant_enqueue}
            end
    end;
publish_(Msg, {Node, SubscriberId, QoS}, QState) ->
    Term = {enqueue_many, SubscriberId, [{deliver, QoS, Msg}], #{states => [QState]}},
    vmq_cluster:remote_enqueue(Node, Term).

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
    Node = node(),
    lists:filter(fun({N, _, _}) when N == Node -> true;
                    (_) -> false
                 end, Subscribers).

