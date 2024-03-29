%% Copyright 2018 Erlio GmbH Basel Switzerland (http://erl.io)
%% Copyright 2018-2024 Octavo Labs/VerneMQ (https://vernemq.com/)
%% and Individual Contributors.
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

-module(vmq_peer_service).

-export([
    join/1,
    leave/1,
    members/0,
    rename_member/2,
    add_event_handler/2,
    delete_event_handler/2,
    call_event_handler/3
]).

join(DiscoveryNode) ->
    vmq_plugin:only(cluster_join, [DiscoveryNode]).

leave(Node) ->
    vmq_plugin:only(cluster_leave, [Node]).

members() ->
    vmq_plugin:only(cluster_members, []).

rename_member(OldName, NewName) ->
    vmq_plugin:only(cluster_rename_member, [OldName, NewName]).

add_event_handler(Module, Args) ->
    vmq_plugin:only(cluster_events_add_handler, [Module, Args]).

delete_event_handler(Module, Reason) ->
    vmq_plugin:only(cluster_events_delete_handler, [Module, Reason]).

call_event_handler(Module, Msg, Timeout) ->
    vmq_plugin:only(cluster_events_call_handler, [Module, Msg, Timeout]).
