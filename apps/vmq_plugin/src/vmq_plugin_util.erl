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

-include_lib("vernemq_dev/include/vernemq_dev.hrl").

-export([check_modifiers/2]).

-spec check_modifiers(atom(), list() | map()) -> list() | map() | error.
check_modifiers(auth_on_subscribe, Modifiers) ->
    case val_sub_topics(Modifiers) of
        false -> error;
        {ok, NewMods} -> NewMods
    end;
check_modifiers(on_unsubscribe, Modifiers) ->
    case val_unsub_topics(Modifiers) of
        false -> error;
        {ok, NewMods} -> NewMods
    end;
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
check_modifiers(Hook, Modifiers) when is_map(Modifiers) ->
    case check_modifiers(Hook, maps:to_list(Modifiers)) of
        NewModifiers when is_list(NewModifiers) ->
            maps:from_list(NewModifiers);
        Other -> Other
    end;
check_modifiers(Hook, Modifiers) ->
    lager:error("can't check modifiers ~p for hook ~p", [Hook, Modifiers]),
    error.

to_internal_qos_m5(V) when is_integer(V) ->
    V.

to_internal_qos(128) ->
    not_allowed;
to_internal_qos(V) when is_integer(V) ->
    V.

modifiers(auth_on_register_m5) ->
    [{properties,
      val_properties_fun([{?P_USER_PROPERTY, fun user_property/1},
                          {?P_SESSION_EXPIRY_INTERVAL, fun val_int/1}])}|
     modifiers(auth_on_register)];
modifiers(auth_on_publish_m5) ->
    [{properties,
      val_properties_fun([{?P_USER_PROPERTY, fun user_property/1},
                          {?P_MESSAGE_EXPIRY_INTERVAL, fun val_int/1},
                          {?P_CONTENT_TYPE, fun val_utf8/1},
                          {?P_PAYLOAD_FORMAT_INDICATOR, val_atoms_fun([utf8,undefined])},
                          {?P_RESPONSE_TOPIC, fun val_pub_topic/1},
                          {?P_CORRELATION_DATA, fun val_utf8/1}])}|
     modifiers(auth_on_publish)];
modifiers(auth_on_subscribe_m5) ->
    [{topics, fun val_sub_topics/1}];
modifiers(on_unsubscribe_m5) ->
    [{topics, fun val_unsub_topics/1}];
modifiers(on_deliver_m5) ->
    [{properties,
      val_properties_fun([{?P_CONTENT_TYPE, fun val_utf8/1},
                          {?P_PAYLOAD_FORMAT_INDICATOR, val_atoms_fun([utf8,undefined])},
                          {?P_USER_PROPERTY, fun user_property/1},
                          {?P_RESPONSE_TOPIC, fun val_pub_topic/1},
                          {?P_CORRELATION_DATA, fun val_utf8/1}])}|
    modifiers(on_deliver)];
modifiers(on_auth_m5) ->
    [{properties,
      val_properties_fun([{?P_AUTHENTICATION_METHOD, fun val_binary/1},
                          {?P_AUTHENTICATION_DATA, fun val_binary/1}])},
     {reason_code, fun val_int/1}];
modifiers(auth_on_register) ->
    [{allow_register, fun val_bool/1},
     {allow_publish, fun val_bool/1},
     {allow_subscribe, fun val_bool/1},
     {allow_unsubscribe, fun val_bool/1},
     {max_message_size, fun val_int/1},
     {subscriber_id, fun val_subscriber_id/1},
     {username, fun val_binary/1},
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
     {mountpoint, fun val_string/1},
     {throttle, fun val_int/1}];
modifiers(on_deliver) ->
    [{topic, fun val_pub_topic/1},
     {payload, fun val_binary/1}];
modifiers(_) -> [].

%% Validators For the Modifiers
val_bool(B) -> is_boolean(B).

val_atom(B) when is_binary(B) -> {ok, binary_to_existing_atom(B, utf8)};
val_atom(_) -> false.

val_utf8(B) -> val_binary(B).

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


-spec val_properties_fun([{atom(), fun()}]) -> fun().
val_properties_fun(AllowedProperties) ->
    fun(Properties) when is_map(Properties) ->
            case val_properties(maps:to_list(Properties), AllowedProperties) of
                true -> Properties;
                false -> false;
                {ok, NewProps} ->
                    {ok, maps:from_list(NewProps)}
            end
    end.

val_atoms_fun(Atoms) ->
    fun(Val) ->
            lists:member(Val, Atoms)
    end.

val_properties([], _AllowedProperties) ->
    true;
val_properties(Props, AllowedProperties) ->
    lists:foldl(fun(_, false) -> false;
                   ({PropKey, PropVal}, {ok, Acc}) ->
                        case lists:keyfind(PropKey, 1, AllowedProperties) of
                            false ->
                                lager:error("property not allowed ~p ~p", [PropKey, PropVal]),
                                false;
                            {_, ValidatorFun} ->
                                case ValidatorFun(PropVal) of
                                    true ->
                                        {ok, [{PropKey, PropVal}|Acc]};
                                    false ->
                                        lager:error("invalid property ~p ~p", [PropKey, PropVal]),
                                        false;
                                    {ok, NewPropVal} ->
                                        {ok, [{PropKey, NewPropVal}|Acc]}
                                end
                        end
                end, {ok, []}, Props).

user_property(Vals) ->
    %% check that val is a list of {binary(), binary()}.
    lists:all(fun({K,V}) when is_binary(K), is_binary(V) ->
                      true;
                 (_) -> false
              end, Vals).

val_pub_topic(B) when is_binary(B) ->
    case vmq_topic:validate_topic(publish, B) of
        {ok, T} -> {ok, T};
        _ -> false
    end;
val_pub_topic(_) -> false.

val_unsub_topics(Topics) when is_list(Topics) ->
    Res =
    lists:foldl(fun (_, false) -> false;
                    (T, {ok, Acc}) when is_binary(T) ->
                        case vmq_topic:validate_topic(subscribe, T) of
                            {ok, Topic} ->
                                {ok, [Topic|Acc]};
                            {error, Reason} ->
                                lager:error("can't parse topic ~p", [T, Reason]),
                                false
                        end;
                   (T, _) ->
                        lager:error("can't rewrite topic due to wrong format ~p", [T]),
                        false
                end, {ok, []}, Topics),
    maybe_reverse(Res).

val_sub_topics(Topics) when is_list(Topics)  ->
    Res =
    lists:foldl(fun (_, false) -> false;
                    ({T, {Q, SubOpts}}, {ok, Acc})
                      when is_binary(T),
                           is_number(Q),
                           is_map(SubOpts) ->
                        %% MQTTv5 style subscriptions with
                        %% subscription options
                        case vmq_topic:validate_topic(subscribe, T) of
                            {ok, Topic} ->
                                {ok, [{Topic, {to_internal_qos_m5(Q), SubOpts}}|Acc]};
                            {error, Reason} ->
                                lager:error("can't parse topic ~p", [{T, Q}, Reason]),
                                false
                        end;
                    ({T, Q}, {ok, Acc}) when is_binary(T) and is_number(Q) ->
                        %% topic format before subopts were introduced with MQTTv5
                        case vmq_topic:validate_topic(subscribe, T) of
                            {ok, Topic} ->
                                {ok, [{Topic, to_internal_qos(Q)}|Acc]};
                            {error, Reason} ->
                                lager:error("can't parse topic ~p", [{T, Q}, Reason]),
                                false
                        end;
                    (T, _) ->
                        lager:error("can't rewrite topic due to wrong format ~p", [T]),
                        false
                end, {ok, []}, Topics),
    maybe_reverse(Res).

maybe_reverse(false) ->
    false;
maybe_reverse({ok, L}) when is_list(L) ->
    {ok, lists:reverse(L)}.
