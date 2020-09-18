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
-include_lib("vernemq_dev/include/vernemq_dev.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([init/2,
         data_in/2,
         msg_in/2]).

-record(state, {
          peer         :: peer(),
          opts         :: list(),
          max_message_size :: non_neg_integer(),
          close_after  :: reference()
         }).
-type state() :: #state{}.

-spec init(peer(),[any()]) -> state().
init(Peer, Opts) ->
    ConnectTimeout = vmq_config:get_env(mqtt_connect_timeout, 5000),
    TRef = erlang:send_after(ConnectTimeout, self(), close_timeout),
    State = #state{peer=Peer,
                   opts=Opts,
                   max_message_size=vmq_config:get_env(max_message_size, 0),
                   close_after=TRef},
    State.
-spec data_in(binary(),state()) -> {'error','packet_exceeds_max_size'} | 
                                    {'error',_,[]} | 
                                    {'stop','normal' | [{_,_,_},...],[any()]} |
                                    {'ok',state(),binary(),[]} | 
                                    {'switch_fsm','vmq_mqtt5_fsm' | 'vmq_mqtt_fsm', vmq_mqtt5_fsm:state() | vmq_mqtt_fsm:state(), _, _}.
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
            end;
        {{error, Reason}, _} ->
            {error, Reason, []}
    end.

-spec parse_connect_frame(binary(),non_neg_integer()) -> any().
parse_connect_frame(Data, MaxMessageSize) ->
    case determine_protocol_version(Data) of
        5 ->
            vmq_parser_mqtt5:parse(Data, MaxMessageSize);
        131 ->
            vmq_parser:parse(Data, MaxMessageSize);
        132 ->
            vmq_parser:parse(Data, MaxMessageSize);
        4 ->
            vmq_parser:parse(Data, MaxMessageSize);
        3 ->
            vmq_parser:parse(Data, MaxMessageSize);
        more ->
            more;
        {error, _} = E ->
            E
    end.
   
-spec determine_protocol_version(binary()) -> 'more' | 3 | 4 | 5 | 131 | 132 | 
    {'error','cant_parse_fixed_header' | 'unknown_protocol_version' | {'cant_parse_connect_fixed_header',binary()}}.
determine_protocol_version(<<1:4, 0:4, Rest/binary>>) ->
    consume_var_header(Rest);
determine_protocol_version(Unknown) ->
    {error, {cant_parse_connect_fixed_header, Unknown}}.

-spec consume_var_header(binary()) -> 'more' | 3 | 4 | 5 | 131 | 132 | {'error','cant_parse_fixed_header' | 'unknown_protocol_version'}.
consume_var_header(<<0:1, _:7, Rest/binary>>) ->
    get_protocol_info(Rest);
consume_var_header(<<1:1, _:7, 0:1, _:7, Rest/binary>>) ->
    get_protocol_info(Rest);
consume_var_header(<<1:1, _:7, 1:1, _:7, 0:1, _:7, Rest/binary>>) ->
    get_protocol_info(Rest);
consume_var_header(<<1:1, _:7, 1:1, _:7, 1:1, _:7, 0:1, _:7, Rest/binary>>) ->
    get_protocol_info(Rest);
consume_var_header(<<_:8/binary, _/binary>>) ->
    {error, cant_parse_fixed_header};
consume_var_header(_) ->
    more.

-spec get_protocol_info(binary()) -> 'more' | 3 | 4 | 5 | 131 | 132 | {'error','unknown_protocol_version'}.
get_protocol_info(<<0:8, 4:8, "MQTT", 5:8, _/binary>>) ->
    5;
get_protocol_info(<<0:8, 4:8, "MQTT", 4:8, _/binary>>) ->
    4;
get_protocol_info(<<0:8, 4:8, "MQTT", 131:8, _/binary>>) ->
    131;
get_protocol_info(<<0:8, 4:8, "MQTT", 132:8, _/binary>>) ->
    132;
get_protocol_info(<<0:8, 4:8, "MQTT", _:8, _/binary>>) ->
    {error, unknown_protocol_version};
get_protocol_info(<<0:8, 6:8, "MQIsdp", 3:8, _/binary>>) ->
    3;
get_protocol_info(<<0:8, 6:8, "MQIsdp", _:8, _/binary>>) ->
    %% This case is needed to not break the vmq_connect_SUITE
    %% invalid_protonum_test test case.
    3;
get_protocol_info(_) ->
    more.

-spec msg_in('close_timeout' | 'disconnect' | {'disconnect','normal_disconnect'},_) -> 'ignore' | {'stop','normal',[]}.
msg_in({disconnect, ?NORMAL_DISCONNECT}, _FsmState0) ->
    {stop, normal, []};
msg_in(disconnect, _FsmState0) ->
    ignore;
msg_in(close_timeout, _FsmState0) ->
    vmq_metrics:incr_socket_close_timeout(),
    lager:debug("stop due to timeout", []),
    {stop, normal, []}.

-ifdef(TEST).

parse_connect_frame_test() ->
    Mv3 = <<16#10,16#11,16#00,16#06,"MQIsdp",16#03,16#00,16#00,16#3c,16#00,16#03,16#78,16#78,16#78>>,
    ?assertMatch({#mqtt_connect{}, _}, parse_connect_frame(Mv3,0)),
    ok = test_prefixes(Mv3),

    Mv4 = <<16#10,16#0f,16#00,16#04,16#4d,16#51,16#54,16#54,16#04,16#00,16#00,16#3c,16#00,16#03,16#73,16#75,16#62>>,
    ?assertMatch({#mqtt_connect{}, _}, parse_connect_frame(Mv4,0)),
    ok = test_prefixes(Mv4),

    Mv5 = <<16#10,16#13,16#00,16#04,16#4d,16#51,16#54,16#54,16#05,16#02,16#00,16#0a,16#00,16#00,16#06,16#63,16#6c,16#69,16#65,16#6e,16#74>>,
    ?assertMatch({#mqtt5_connect{}, _}, parse_connect_frame(Mv5,0)),
    ok = test_prefixes(Mv5).

test_prefixes(Binary) ->
    lists:foreach(
      fun(Len) ->
              Prefix = binary:part(Binary, {0,Len}),
              ?assertMatch(more, parse_connect_frame(Prefix, 0))
      end,
      lists:seq(1, byte_size(Binary)-1)).

-endif.
