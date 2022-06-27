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

-module(vmq_plumtree).
-export([start/0, stop/0]).

-export([
    metadata_put/3,
    metadata_get/2,
    metadata_delete/2,
    metadata_fold/3,
    metadata_subscribe/1
]).

-export([
    cluster_join/1,
    cluster_leave/1,
    cluster_members/0,
    cluster_rename_member/2,
    cluster_events_add_handler/2,
    cluster_events_delete_handler/2,
    cluster_events_call_handler/3
]).

-define(TOMBSTONE, '$deleted').

start() ->
    application:ensure_all_started(vmq_plumtree).

stop() ->
    application:stop(vmq_plumtree),
    application:stop(plumtree),
    application:stop(eleveldb).

cluster_join(DiscoveryNode) ->
    plumtree_peer_service:join(DiscoveryNode).

cluster_leave(Node) ->
    {ok, Local} = plumtree_peer_service_manager:get_local_state(),
    {ok, Actor} = plumtree_peer_service_manager:get_actor(),
    case riak_dt_orswot:update({remove, Node}, Actor, Local) of
        {error, {precondition, {not_present, Node}}} ->
            {error, not_present};
        {ok, Merged} ->
            AllNodes = riak_dt_orswot:value(Local),
            % multi_cast so we don't need to wait for the next gossip round
            multi_cast(AllNodes, plumtree_peer_service_gossip, {receive_state, Merged}),
            {ok, Local2} = plumtree_peer_service_manager:get_local_state(),
            Local2List = riak_dt_orswot:value(Local2),
            case [P || P <- Local2List, P =:= Node] of
                [] ->
                    plumtree_peer_service_manager:delete_state(),
                    ok;
                _ ->
                    cluster_leave(Node)
            end
    end.

multi_cast([Node | Rest], RegName, Msg) ->
    _ = gen_server:cast({RegName, Node}, Msg),
    multi_cast(Rest, RegName, Msg);
multi_cast([], _, _) ->
    ok.

cluster_members() ->
    {ok, LocalState} = plumtree_peer_service_manager:get_local_state(),
    riak_dt_orswot:value(LocalState).

cluster_rename_member(OldName, NewName) ->
    {ok, LocalState} = plumtree_peer_service_manager:get_local_state(),
    {ok, Actor} = plumtree_peer_service_manager:get_actor(),
    {ok, Merged} = riak_dt_orswot:update(
        {update, [
            {remove, OldName},
            {add, NewName}
        ]},
        Actor,
        LocalState
    ),
    _ = gen_server:cast(plumtree_peer_service_gossip, {receive_state, Merged}).

cluster_events_add_handler(Module, Opts) ->
    plumtree_peer_service_events:add_sup_handler(Module, Opts).

cluster_events_delete_handler(Module, Reason) ->
    gen_event:delete_handler(plumtree_peer_service_events, Module, [Reason]).

cluster_events_call_handler(Module, Msg, Timeout) ->
    gen_event:call(plumtree_peer_service_events, Module, Msg, Timeout).

metadata_put(FullPrefix, Key, Value) ->
    plumtree_metadata:put(FullPrefix, Key, Value).

metadata_get(FullPrefix, Key) ->
    plumtree_metadata:get(FullPrefix, Key).

metadata_delete(FullPrefix, Key) ->
    plumtree_metadata:delete(FullPrefix, Key).

metadata_fold(FullPrefix, Fun, Acc) ->
    plumtree_metadata:fold(Fun, Acc, FullPrefix, [{resolver, lww}]).

metadata_subscribe(FullPrefix) ->
    plumtree_metadata_manager:subscribe(FullPrefix).
