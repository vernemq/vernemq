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

-export([publish/5]).

publish(Msg, Policy, SubscriberGroups, LocalMatches, RemoteMatches) ->
    publish(Msg, Policy, SubscriberGroups, {LocalMatches, RemoteMatches}).

publish(_,_, undefined, Acc) -> Acc;
publish(Msg, Policy, SubscriberGroups, Acc) when is_map(SubscriberGroups) ->
    maps:fold(fun(Group, Subscribers, MAcc) ->
                      publish_(Msg, Policy, Group, Subscribers, MAcc)
              end, Acc, SubscriberGroups).

publish_(Msg, prefer_local, Group, {LocalSubs, RemoteSubs}, Acc0) ->
    case publish_to_group(Msg, LocalSubs, Acc0) of
        {ok, Acc1} ->
            Acc1;
        {error, no_subscribers} ->
            publish(Msg, Group, RemoteSubs, Acc0)
    end;
publish_(Msg, local_only, Group, {LocalSubs, _}, Acc0) ->
    publish_(Msg, Group, LocalSubs, Acc0);
publish_(Msg, random, Group, {LocalSubs, RemoteSubs}, Acc0) ->
    publish_(Msg, Group, LocalSubs ++ RemoteSubs, Acc0).


publish_(Msg, Group, Subs, Acc0) ->
    case publish_to_group(Msg, Subs, Acc0) of
        {ok, Acc1} ->
            Acc1;
        {error, Reason} ->
            lager:debug("can't publish to shared subscription ~p due to '~p', msg: ~p",
                        [Group, Reason, Msg]),
            Acc0
    end.

publish_to_group(Msg, Subscribers, Acc0) ->
    case publish_online(Msg, Subscribers, Acc0) of
        {ok, Acc1} ->
            {ok, Acc1};
        NotOnlineSubscribers ->
            publish_any(Msg, NotOnlineSubscribers, Acc0)
    end.

publish_online(Msg, Subscribers, Acc0) ->
    Ret =
    pick_random:pick_random(
      fun(Subscriber, SubAcc) ->
              case publish__(Msg, Subscriber, online, Acc0) of
                  {ok, Acc1} ->
                      {ok, {has_result, Acc1}};
                  {error, offline} ->
                      [Subscriber|SubAcc];
                  {error, draining} ->
                      [Subscriber|SubAcc];
                  _ ->
                      SubAcc
              end
      end, [], Subscribers),
    case Ret of
        {has_result, Result} -> {ok, Result};
        Ret -> Ret
    end.

publish_any(Msg, Subscribers, Acc0) ->
    Ret =
    pick_random:pick_random(
      fun(Subscriber, SubAcc0) ->
              case publish__(Msg, Subscriber, any, SubAcc0) of
                  {ok, SubAcc1} ->
                      {ok, {has_result, SubAcc1}};
                  {error, _} ->
                      SubAcc0
              end
      end, Acc0, Subscribers),
    case Ret of
        {has_result, Acc1} -> {ok, Acc1};
        _ -> {error, no_subscribers}
    end.

publish__(Msg, {Node, SubscriberId, QoS}, QState, {Local, Remote}) when Node == node() ->
    case vmq_reg:get_queue_pid(SubscriberId) of
        not_found ->
            {error, not_found};
        QPid ->
            try
                case vmq_queue:enqueue_many(QPid, [{deliver, QoS, Msg}], #{states => [QState]}) of
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
publish__(Msg, {Node, SubscriberId, QoS}, QState, {Local, Remote}) ->
    Term = {enqueue_many, SubscriberId, [{deliver, QoS, Msg}], #{states => [QState]}},
    case vmq_cluster:remote_enqueue(Node, Term, true) of
        ok ->
            {ok, {Local, Remote + 1}};
        E ->
            E
    end.
