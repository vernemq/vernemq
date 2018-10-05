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
-module(vmq_mqtt_pre_init).

-include_lib("vmq_commons/include/vmq_types.hrl").
-include_lib("vmq_commons/include/vmq_types_mqtt5.hrl").
-include_lib("vernemq_dev/include/vernemq_dev.hrl").

-export([init/2,
         data_in/2,
         msg_in/2]).

-define(CLOSE_AFTER, 5000).

-record(state, {
          peer         :: peer(),
          opts         :: list(),
          max_message_size :: non_neg_integer(),
          close_after  :: reference()
         }).

init(Peer, Opts) ->
    TRef = erlang:send_after(?CLOSE_AFTER, self(), close_timeout),
    State = #state{peer=Peer,
                   opts=Opts,
                   max_message_size=vmq_config:get_env(max_message_size, 0),
                   close_after=TRef},
    State.

data_in(Data, #state{peer = Peer,
                     opts = Opts,
                     close_after=TRef,
                     max_message_size=MaxMessageSize} = FsmState0) when is_binary(Data) ->
    case parse_connect_frame(Data, MaxMessageSize) of
        more ->
            {ok, FsmState0, Data, []};
        {error, packet_exceeds_max_size} = E ->
            _ = vmq_metrics:incr_mqtt_error_invalid_msg_size(),
            E;
        {error, Reason} ->
            {error, Reason, []};
        {#mqtt5_connect{} = ConnectFrame, Rest} ->
            erlang:cancel_timer(TRef),
            case vmq_mqtt5_fsm:init(Peer, Opts, ConnectFrame) of
                {stop, Reason, Out} ->
                    {stop, Reason, Out};
                {FsmState1, Out} ->
                    {switch_fsm, vmq_mqtt5_fsm, FsmState1, Rest, Out}
            end;
        {#mqtt_connect{} = ConnectFrame, Rest} ->
            erlang:cancel_timer(TRef),
            case vmq_mqtt_fsm:init(Peer, Opts, ConnectFrame) of
                {stop, Reason, Out} ->
                    {stop, Reason, Out};
                {FsmState1, Out} ->
                    {switch_fsm, vmq_mqtt_fsm, FsmState1, Rest, Out}
            end
    end.

parse_connect_frame(Data, MaxMessageSize) ->
    case determine_protocol_version(Data) of
        5 ->
            vmq_parser_mqtt5:parse(Data, MaxMessageSize);
        4 ->
            vmq_parser:parse(Data, MaxMessageSize);
        3 ->
            vmq_parser:parse(Data, MaxMessageSize);
        {error, _} = E ->
            E
    end.

determine_protocol_version(<<1:4, 0:4, Rest/binary>>) ->
    consume_var_header(Rest).

consume_var_header(<<0:1, _:7, Rest/binary>>) ->
    get_protocol_info(Rest);
consume_var_header(<<1:1, _:7, 0:1, _:7, Rest/binary>>) ->
    get_protocol_info(Rest);
consume_var_header(<<1:1, _:7, 1:1, _:7, 0:1, _:7, Rest/binary>>) ->
    get_protocol_info(Rest);
consume_var_header(<<1:1, _:7, 1:1, _:7, 1:1, _:7, 0:1, _:7, Rest/binary>>) ->
    get_protocol_info(Rest).

get_protocol_info(<<0:8, 4:8, "MQTT", 5:8, _/binary>>) ->
    5;
get_protocol_info(<<0:8, 4:8, "MQTT", 4:8, _/binary>>) ->
    4;
get_protocol_info(<<0:8, 6:8, "MQIsdp", 3:8, _/binary>>) ->
    3;
get_protocol_info(<<0:8, 6:8, "MQIsdp", _:8, _/binary>>) ->
    %% This case is needed to not break the vmq_connect_SUITE
    %% invalid_protonum_test test case.
    3;
get_protocol_info(_) ->
    {error, unknown_protocol_version}.


msg_in({disconnect, ?NORMAL_DISCONNECT}, _FsmState0) ->
    {stop, normal, []};
msg_in(disconnect, _FsmState0) ->
    ignore;
msg_in(close_timeout, _FsmState0) ->
    lager:debug("stop due to timeout", []),
    {stop, normal, []}.
