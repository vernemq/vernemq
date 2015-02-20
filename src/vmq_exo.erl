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

-module(vmq_exo).
-export([incr_bytes_received/1,
         incr_bytes_sent/1,
         incr_inactive_clients/0,
         decr_inactive_clients/0,
         incr_expired_clients/0,
         incr_messages_received/1,
         incr_messages_sent/1,
         incr_publishes_dropped/1,
         incr_publishes_received/1,
         incr_publishes_sent/1,
         incr_subscription_count/0,
         decr_subscription_count/0,
         incr_socket_count/0,
         incr_connect_received/0,
         entries/0]).

incr_bytes_received(V) ->
    incr_item([bytes, received], V).

incr_bytes_sent(V) ->
    incr_item([bytes, sent], V).

incr_expired_clients() ->
    incr_item([expired_clients], 1).

incr_inactive_clients() ->
    incr_item([inactive_clients], 1).

decr_inactive_clients() ->
    incr_item([inactive_clients], -1).

incr_messages_received(V) ->
    incr_item([messages, received], V).

incr_messages_sent(V) ->
    incr_item([messages, sent], V).

incr_publishes_dropped(V) ->
    incr_item([publishes, dropped], V).

incr_publishes_received(V) ->
    incr_item([publishes, received], V).

incr_publishes_sent(V) ->
    incr_item([publishes, sent], V).

incr_subscription_count() ->
    incr_item([subscriptions], 1).

decr_subscription_count() ->
    incr_item([subscriptions], -1).

incr_socket_count() ->
    incr_item([sockets], 1).

incr_connect_received() ->
    incr_item([connects, received], 1).

incr_item(Entry, Val) ->
    exometer:update_or_create(Entry, Val).

entries() ->
    {ok, entries_()}.

entries_() ->
    [
    % {[memory], {function, erlang, memory, [], proplist,[total, processes]}, [{snmp, []}]},
     {[bytes, received], histogram, [{snmp, []}]},
     {[bytes, sent], histogram, [{snmp, []}]},
     {[messages, received], histogram, [{snmp, []}]},
     {[messages, sent], histogram, [{snmp, []}]},
     {[publishes, dropped], histogram, [{snmp, []}]},
     {[publishes, received], histogram, [{snmp, []}]},
     {[publishes, sent], histogram, [{snmp, []}]},
     {[connects, received], histogram, [{snmp, []}]},
     {[sockets], histogram, [{snmp, []}]},
     {[subscriptions], counter, [{snmp, []}]},
     {[expired_clients], counter, [{snmp, []}]},
     {[inactive_clients], counter, [{snmp, []}]}
    ].
