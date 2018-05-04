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

%% @doc compatibility layer making it possible reuse plugin hooks (v0
%% hooks) written for MQTTv3/4 plugins in an MQTTv5 context. The
%% compatibility layer maps MQTTv5 information to the MQTTv4
%% equivalents and vice versa.
-module(vmq_plugin_compat_v1_v0).

-export([convert/4]).

convert(auth_on_subscribe, Mod, Fun, [Username, SubscriberId, Topics]) ->
    apply(Mod, Fun, [Username, SubscriberId, topics_v1_v0(Topics)]);
convert(_, Mod, Fun, Args) ->
    apply(Mod, Fun, Args).

topics_v1_v0(Topics) ->
    lists:map(
      fun({T, {QoS, _SubOpts}}) ->
              {T, QoS}
      end, Topics).
