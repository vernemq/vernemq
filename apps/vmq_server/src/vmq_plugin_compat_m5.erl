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

%% @doc compatibility layer making it possible to reuse plugin hooks
%% written for MQTTv3/4 in an MQTTv5 context. The compatibility layer
%% maps MQTTv5 information to the MQTTv4 equivalents and vice versa.
-module(vmq_plugin_compat_m5).

-include_lib("vernemq_dev/include/vernemq_dev.hrl").

-export([convert/4]).

convert(auth_on_publish, Mod, Fun,
        [User, SubscriberId, QoS, Topic, Payload, IsRetain, _Properties]) ->
    case apply(Mod, Fun, [User, SubscriberId, QoS, Topic, Payload, IsRetain]) of
        {ok, Vals} when is_list(Vals) ->
            {ok, maps:from_list(Vals)};
        {error, Vals} when is_list(Vals) ->
            {ok, maps:from_list(Vals)};
        Other -> Other %% for instance {error, any()}
    end;
convert(auth_on_register, Mod, Fun,
        [Peer, SubscriberId, User, Password, CleanStart, _Properties]) ->
    CleanSession = CleanStart, %% TODOv5, can we do better than having this caveat?
    case apply(Mod, Fun, [Peer, SubscriberId, User, Password, CleanSession]) of
        {ok, Vals} when is_list(Vals) ->
            M0 = maps:from_list(Vals),
            case maps:take(clean_session, M0) of
                {Val, M1} ->
                    {ok, M1#{clean_start => Val}};
                error ->
                    {ok, M0}
            end;
        {error, Vals} when is_list(Vals) ->
            {ok, maps:from_list(Vals)};
        Other -> Other %% for instance {error, any()}
    end;
convert(on_publish, Mod, Fun,
        [User, SubscriberId, QoS, Topic, Payload, IsRetain, _Properties]) ->
    apply(Mod, Fun, [User, SubscriberId, QoS, Topic, Payload, IsRetain]);
convert(auth_on_subscribe, Mod, Fun, [Username, SubscriberId, Topics, _Properties]) ->
    case apply(Mod, Fun, [Username, SubscriberId, conv_m5_topics(Topics)]) of
        {ok, Topics} when is_list(Topics) ->
            {ok, #{topics => Topics}};
        Other -> Other
    end;
convert(_, Mod, Fun, Args) ->
    apply(Mod, Fun, Args).

conv_m5_topics(Topics) ->
    lists:map(
      fun({T, {QoS, _SubOpts}}) ->
              {T, QoS}
      end, Topics).
