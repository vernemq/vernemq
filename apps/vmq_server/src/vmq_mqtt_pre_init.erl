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
                    {stop, Reason, serialize_mqtt5(Out)};
                {FsmState1, Out} ->
                    {switch_fsm, vmq_mqtt5_fsm, FsmState1, Rest, serialize_mqtt5(Out)}
            end;
        {#mqtt_connect{} = ConnectFrame, Rest} ->
            erlang:cancel_timer(TRef),
            case vmq_mqtt_fsm:init(Peer, Opts, ConnectFrame) of
                {stop, Reason, Out} ->
                    {stop, Reason, serialize(Out)};
                {FsmState1, Out} ->
                    {switch_fsm, vmq_mqtt_fsm, FsmState1, Rest, serialize(Out)}
            end
    end.

serialize([Out]) ->
    [vmq_parser:serialise(Out)].

serialize_mqtt5([Out]) ->
    [vmq_parser_mqtt5:serialise(Out)].

parse_connect_frame(Data, MaxMessageSize) ->
    case vmq_parser:parse(Data, MaxMessageSize) of
        more ->
            more;
        {{error, is_mqtt5}, _} ->
            vmq_parser_mqtt5:parse(Data, MaxMessageSize);
        {error, _} = E ->
            E;
        {Frame, Rest} ->
            {Frame, Rest}
    end.

msg_in(disconnect, _FsmState0) ->
    ignore;
msg_in(close_timeout, _FsmState0) ->
    lager:debug("stop due to timeout", []),
    {stop, normal, []}.


