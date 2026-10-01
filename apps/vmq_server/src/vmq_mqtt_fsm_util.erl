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

-module(vmq_mqtt_fsm_util).
-include("vmq_server.hrl").
-include("vmq_metrics.hrl").
-include_lib("vmq_commons/include/vmq_types.hrl").

-export([
    send/2,
    send_after/2,
    msg_ref/0,
    plugin_receive_loop/2,
    to_vmq_subtopics/2,
    peertoa/1,
    terminate_reason/1,
    terminate_proto_reason/1,
    generate_session_id/0,
    maybe_send_puback/3,
    should_send_puback/1
]).

-define(TO_SESSION, to_session_fsm).
-define(DELAYED_PUBACK_TBL, vmq_delayed_puback_table).

-spec msg_ref() -> msg_ref().
msg_ref() ->
    GUID =
        case get(guid) of
            undefined ->
                {{node(), self(), erlang:timestamp()}, 0};
            {S, I} ->
                {S, I + 1}
        end,
    put(guid, GUID),
    erlang:md5(term_to_binary(GUID)).

-spec send(pid(), any()) -> ok.
send(SessionPid, Msg) ->
    SessionPid ! {?TO_SESSION, Msg},
    ok.

-spec send_after(non_neg_integer(), any()) -> reference().
send_after(Time, Msg) ->
    erlang:send_after(Time, self(), {?TO_SESSION, Msg}).

-spec plugin_receive_loop(pid(), atom()) -> no_return().
plugin_receive_loop(PluginPid, PluginMod) ->
    receive
        {?TO_SESSION, {mail, QPid, new_data}} ->
            vmq_queue:active(QPid),
            plugin_receive_loop(PluginPid, PluginMod);
        {?TO_SESSION, {mail, QPid, Msgs, _, _}} ->
            lists:foreach(
                fun
                    (
                        #deliver{
                            qos = QoS,
                            msg = #vmq_msg{
                                routing_key = RoutingKey,
                                payload = Payload,
                                retain = IsRetain,
                                dup = IsDup
                            }
                        }
                    ) ->
                        PluginPid ! {deliver, RoutingKey, Payload, QoS, IsRetain, IsDup};
                    (Msg) ->
                        lager:warning("dropped message ~p for plugin ~p", [Msg, PluginMod]),
                        ok
                end,
                Msgs
            ),
            vmq_queue:notify(QPid),
            plugin_receive_loop(PluginPid, PluginMod);
        {?TO_SESSION, {info_req, {Ref, CallerPid}, _}} ->
            CallerPid ! {Ref, {error, i_am_a_plugin}},
            plugin_receive_loop(PluginPid, PluginMod);
        disconnect ->
            ok;
        {'DOWN', _MRef, process, PluginPid, Reason} ->
            case (Reason == normal) or (Reason == shutdown) of
                true ->
                    ok;
                false ->
                    lager:warning("plugin queue loop for ~p stopped due to ~p", [PluginMod, Reason])
            end;
        Other ->
            exit({unknown_msg_in_plugin_loop, Other})
    end.

-spec to_vmq_subtopics(
    [mqtt5_subscribe_topic() | mqtt_subscribe_topic()], subscription_id() | undefined
) -> [subscription()].
to_vmq_subtopics(Topics, SubId) ->
    lists:map(
        fun
            ({T, QoS}) ->
                {T, QoS};
            (
                #mqtt_subscribe_topic{
                    topic = T, qos = QoS, non_persistence = NonPersistence, non_retry = Retry
                }
            ) ->
                %% MQTTv4 style topics
                SubOpts = #{non_persistence => NonPersistence, non_retry => Retry},
                {T, {QoS, SubOpts}};
            (
                #mqtt5_subscribe_topic{
                    topic = T, qos = QoS, rap = Rap, retain_handling = RH, no_local = NL
                }
            ) ->
                SubOpts = #{rap => Rap, retain_handling => RH, no_local => NL},
                case SubId of
                    undefined ->
                        {T, {QoS, SubOpts}};
                    _ ->
                        {T, {QoS, SubOpts#{sub_id => SubId}}}
                end
        end,
        Topics
    ).

%% Shared by both protocol FSMs: every session gets a UUIDv4 the hooks
%% can correlate on.
-spec generate_session_id() -> session_id().
generate_session_id() ->
    <<A:32, B:16, C:16, D:16, E:48>> = crypto:strong_rand_bytes(16),
    iolist_to_binary(
        io_lib:format(
            "~8.16.0b-~4.16.0b-4~3.16.0b-~4.16.0b-~12.16.0b",
            [A, B, C band 16#0fff, (D band 16#3fff) bor 16#8000, E]
        )
    ).

-spec peertoa(peer()) -> string().
peertoa({IP, Port}) ->
    case IP of
        {_, _, _, _} ->
            io_lib:format("~s:~p", [inet:ntoa(IP), Port]);
        {_, _, _, _, _, _, _, _} ->
            io_lib:format("[~s]:~p", [inet:ntoa(IP), Port]);
        local ->
            "local"
    end.

-spec terminate_reason(any()) -> any().
terminate_reason(?ADMINISTRATIVE_ACTION) -> normal;
terminate_reason(?CLIENT_DISCONNECT) -> normal;
terminate_reason(?DISCONNECT_KEEP_ALIVE) -> normal;
terminate_reason(?DISCONNECT_MIGRATION) -> normal;
terminate_reason(?NORMAL_DISCONNECT) -> normal;
terminate_reason(?SESSION_TAKEN_OVER) -> normal;
terminate_reason(?REMOTE_SESSION_TAKEN_OVER) -> normal;
terminate_reason(?INVALID_PUBREC_ERROR) -> normal;
terminate_reason(?INVALID_PUBCOMP_ERROR) -> normal;
terminate_reason(?TCP_CLOSED) -> normal;
terminate_reason(?EXIT_SIGNAL_RECEIVED) -> normal;
terminate_reason(?PUBLISH_AUTH_ERROR) -> normal;
terminate_reason(Reason) -> Reason.

-spec terminate_proto_reason(any()) -> any().
terminate_proto_reason(Reason) ->
    case Reason of
        ?NOT_AUTHORIZED -> ?REASON_NOT_AUTHORIZED;
        ?NORMAL_DISCONNECT -> ?REASON_NORMAL_DISCONNECT;
        ?SESSION_TAKEN_OVER -> ?REASON_SESSION_TAKEN_OVER;
        ?ADMINISTRATIVE_ACTION -> ?REASON_ADMINISTRATIVE_ACTION;
        ?DISCONNECT_KEEP_ALIVE -> ?REASON_DISCONNECT_KEEP_ALIVE;
        ?DISCONNECT_MIGRATION -> ?REASON_DISCONNECT_MIGRATION;
        ?BAD_AUTHENTICATION_METHOD -> ?REASON_BAD_AUTHENTICATION_METHOD;
        ?REMOTE_SESSION_TAKEN_OVER -> ?REASON_REMOTE_SESSION_TAKEN_OVER;
        ?CLIENT_DISCONNECT -> ?REASON_MQTT_CLIENT_DISCONNECT;
        ?RECEIVE_MAX_EXCEEDED -> ?REASON_RECEIVE_MAX_EXCEEDED;
        ?PROTOCOL_ERROR -> ?REASON_PROTOCOL_ERROR;
        ?PUBLISH_AUTH_ERROR -> ?REASON_PUBLISH_AUTH_ERROR;
        ?INVALID_PUBREC_ERROR -> ?REASON_INVALID_PUBREC_ERROR;
        ?INVALID_PUBCOMP_ERROR -> ?REASON_INVALID_PUBCOMP_ERROR;
        ?UNEXPECTED_FRAME_TYPE -> ?REASON_UNEXPECTED_FRAME_TYPE;
        ?EXIT_SIGNAL_RECEIVED -> ?REASON_EXIT_SIGNAL_RECEIVED;
        ?TCP_CLOSED -> ?REASON_TCP_CLOSED;
        ?NORMAL -> ?REASON_NORMAL_DISCONNECT;
        ?KEEP_ALIVE_TIMEOUT -> ?REASON_DISCONNECT_KEEP_ALIVE;
        ?WRONG_AUTH_METHOD -> ?REASON_WRONG_AUTH_METHOD;
        ?QUEUE_DOWN -> ?REASON_QUEUE_DOWN;
        ?DISCONNECT_WITH_WILL_MSG -> ?REASON_DISCONNECT_WITH_WILL_MSG;
        ?MALFORMED_PACKET -> ?REASON_MALFORMED_PACKET;
        ?IMPL_SPECIFIC_ERROR -> ?REASON_IMPL_SPECIFIC_ERROR;
        ?TOPIC_NAME_INVALID -> ?REASON_TOPIC_NAME_INVALID;
        ?TOPIC_ALIAS_INVALID -> ?REASON_TOPIC_ALIAS_INVALID;
        ?PACKET_TOO_LARGE -> ?REASON_PACKET_TOO_LARGE;
        ?MESSAGE_RATE_TOO_HIGH -> ?REASON_MESSAGE_RATE_TOO_HIGH;
        ?QUOTA_EXCEEDED -> ?REASON_QUOTA_EXCEEDED;
        ?PAYLOAD_FORMAT_INVALID -> ?REASON_PAYLOAD_FORMAT_INVALID;
        ?UNSPECIFIED_ERROR -> ?REASON_UNSPECIFIED_ERROR;
        _ -> ?REASON_UNSPECIFIED
    end.

-spec maybe_send_puback(binary() | undefined, pid() | undefined, msg_id()) -> ok.
maybe_send_puback(Name, PubPid, PubMsgId) ->
    case should_send_puback(Name) of
        true when is_pid(PubPid) ->
            vmq_ranch:send_puback(PubPid, PubMsgId);
        _ ->
            ok
    end.

-spec should_send_puback(binary() | undefined) -> boolean().
should_send_puback(undefined) ->
    false;
should_send_puback(AclName) ->
    ets:member(?DELAYED_PUBACK_TBL, AclName).
