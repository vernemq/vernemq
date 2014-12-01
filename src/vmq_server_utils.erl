%% Copyright 2014 Erlio GmbH Basel Switzerland (http://erl.io)
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

-module(vmq_server_utils).
-export([total_clients/0,
         in_flight/0,
         retained/0,
         stored/0,
         active_clients/0]).

total_clients() ->
    vmq_reg:total_clients().

in_flight() ->
    vmq_msg_store:in_flight().

retained() ->
    vmq_reg:retained().

stored() ->
    vmq_msg_store:stored().

active_clients() ->
    vmq_session_sup:active_clients().

