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
-module(vmq_plugin_util).

-export([check_modifiers/2]).

check_modifiers(auth_on_subscribe, Modifiers) ->
    %% auth_on_subscribe take a special form
    lists:foldl(fun (_, error) -> error;
                    ([T, Q], Acc) when is_binary(T) and is_number(Q) ->
                        case vmq_topic:validate_topic(subscribe, T) of
                            {ok, Topic} ->
                                [{Topic, to_internal_qos(round(Q))}|Acc];
                            {error, Reason} ->
                                lager:error("can't parse topic ~p in auth_on_subscribe", [{T, Q}, Reason]),
                                error
                        end;
                   (T, _) ->
                        lager:error("can't rewrite topic in auth_on_subscribe due to wrong format ~p", [T]),
                        error
                end, [], Modifiers);
check_modifiers(on_unsubscribe, Modifiers) ->
    %% on_unsubscribe take a special form
    lists:foldl(fun (_, error) -> error;
                    (T, Acc) when is_binary(T) ->
                        case vmq_topic:validate_topic(subscribe, T) of
                            {ok, Topic} ->
                                [Topic|Acc];
                            {error, Reason} ->
                                lager:error("can't parse topic ~p in on_unsubscribe", [T, Reason]),
                                error
                        end;
                   (T, _) ->
                        lager:error("can't rewrite topic in on_unsubscribe due to wrong format ~p", [T]),
                        error
                end, [], Modifiers);
check_modifiers(Hook, [{_,_}|_] = Modifiers) ->
    AllowedModifiers = modifiers(Hook),
    lists:foldl(fun (_, error) -> error;
                    ({ModKey, ModVal}, Acc) ->
                        case lists:keyfind(ModKey, 1, AllowedModifiers) of
                            false ->
                                error;
                            {_, ValidatorFun} ->
                                case ValidatorFun(ModVal) of
                                    true ->
                                        [{ModKey, ModVal}|Acc];
                                    false ->
                                        lager:error("can't validate modifier ~p ~p ~p", [Hook, ModKey, ModVal]),
                                        error;
                                    {ok, NewModVal} ->
                                        [{ModKey, NewModVal}|Acc]
                                end
                        end
                end, [], Modifiers);
check_modifiers(Hook, Modifiers) ->
    lager:error("can't check modifiers ~p for hook ~p", [Hook, Modifiers]),
    error.

to_internal_qos(128) ->
    not_allowed;
to_internal_qos(V) when is_integer(V) ->
    V.

modifiers(auth_on_register) ->
    [{allow_register, fun val_bool/1},
     {allow_publish, fun val_bool/1},
     {allow_subscribe, fun val_bool/1},
     {allow_unsubscribe, fun val_bool/1},
     {max_message_size, fun val_int/1},
     {subscriber_id, fun val_subscriber_id/1},
     {clean_session, fun val_bool/1},
     {max_message_rate, fun val_int/1},
     {max_inflight_messages, fun val_int/1},
     {shared_subscription_policy, fun val_atom/1},
     {retry_interval, fun val_int/1},
     {upgrade_qos, fun val_bool/1},
     {allow_multiple_sessions, fun val_bool/1},
     {max_online_messages, fun val_int/1},
     {max_offline_messages, fun val_int/1},
     {queue_deliver_mode, fun val_atom/1},
     {queue_type, fun val_atom/1},
     {max_drain_time, fun val_int/1},
     {max_msgs_per_drain_step, fun val_int/1}];
modifiers(auth_on_publish) ->
    [{topic, fun val_pub_topic/1},
     {payload, fun val_binary/1},
     {qos, fun val_qos/1},
     {retain, fun val_bool/1},
     {mountpoint, fun val_string/1}];
modifiers(on_deliver) ->
    [{topic, fun val_pub_topic/1},
     {payload, fun val_binary/1}];
modifiers(_) -> [].

%% Validators For the Modifiers

val_bool(B) -> is_boolean(B).

val_atom(B) when is_binary(B) -> {ok, binary_to_existing_atom(B, utf8)};
val_atom(_) -> false.

val_binary(B) -> is_binary(B).

val_string(B) when is_binary(B) -> {ok, binary_to_list(B)};
val_string(_) -> false.

val_qos(N) when is_number(N)
                and (N >= 0) and (N =< 2) -> {ok, round(N)};
val_qos(_) -> false.

val_int(I) when is_integer(I) -> true;
val_int(N) when is_number(N) -> {ok, round(N)};
val_int(_) -> false.

val_subscriber_id([{_, _}|_] = SubscriberIdModifier) ->
    case {lists:keyfind(client_id, 1, SubscriberIdModifier),
          lists:keyfind(mountpoint, 1, SubscriberIdModifier)} of
        {{_, ClientId}, {_, Mountpoint}} when is_binary(ClientId) and is_binary(Mountpoint) ->
            {ok, {binary_to_list(Mountpoint), ClientId}};
        _ ->
            false
    end;
val_subscriber_id(_) -> false.

val_pub_topic(B) when is_binary(B) ->
    case vmq_topic:validate_topic(publish, B) of
        {ok, T} -> {ok, T};
        _ -> false
    end;
val_pub_topic(_) -> false.
