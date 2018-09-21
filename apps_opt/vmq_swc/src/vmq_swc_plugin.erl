%% Copyright 2018 Octavo Labs AG Zurich Switzerland (https://octavolabs.com)
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

-module(vmq_swc_plugin).

-export([metadata_put/3,
         metadata_get/2,
         metadata_delete/2,
         metadata_fold/3,
         metadata_subscribe/1,

         cluster_join/1,
         cluster_leave/1,
         cluster_members/0,
         cluster_rename_member/2,
         cluster_events_add_handler/2,
         cluster_events_delete_handler/2,
         cluster_events_call_handler/3]).

-define(SWC_GROUP, metadata).

cluster_join(DiscoveryNode) ->
    vmq_swc_plumtree_peer_service:join(DiscoveryNode).

cluster_leave(Node) ->
    {ok, Local} = vmq_swc_plumtree_peer_service_manager:get_local_state(),
    {ok, Actor} = vmq_swc_plumtree_peer_service_manager:get_actor(),
    case riak_dt_orswot:update({remove, Node}, Actor, Local) of
        {error,{precondition,{not_present, Node}}} ->
            {error, not_present};
        {ok, Merged} ->
            AllNodes = riak_dt_orswot:value(Local),
            % multi_cast so we don't need to wait for the next gossip round
            multi_cast(AllNodes, vmq_swc_plumtree_peer_service_gossip, {receive_state, Merged}),
            {ok, Local2} = vmq_swc_plumtree_peer_service_manager:get_local_state(),
            Local2List = riak_dt_orswot:value(Local2),
            case [P || P <- Local2List, P =:= Node] of
                [] ->
                    ok;
                _ ->
                    cluster_leave(Node)
            end
    end.

multi_cast([Node|Rest], RegName, Msg) ->
    _ = gen_server:cast({RegName, Node}, Msg),
    multi_cast(Rest, RegName, Msg);
multi_cast([], _, _) ->
    timer:sleep(1000),
    ok.

cluster_members() ->
    Config = vmq_swc:config(metadata),
    vmq_swc_group_membership:get_members(Config).

cluster_rename_member(OldName, NewName) ->
    {ok, LocalState} = vmq_swc_plumtree_peer_service_manager:get_local_state(),
    {ok, Actor} = vmq_swc_plumtree_peer_service_manager:get_actor(),
    {ok, Merged} = riak_dt_orswot:update({update, [{remove, OldName},
                                                   {add, NewName}]}, Actor, LocalState),
    _ = gen_server:cast(vmq_swc_plumtree_peer_service_gossip, {receive_state, Merged}).

cluster_events_add_handler(Module, Opts) ->
    vmq_swc_plumtree_peer_service_events:add_sup_handler(Module, Opts).

cluster_events_delete_handler(Module, Reason) ->
    gen_event:delete_handler(vmq_swc_plumtree_peer_service_events, Module, [Reason]).

cluster_events_call_handler(Module, Msg, Timeout) ->
    gen_event:call(vmq_swc_plumtree_peer_service_events, Module, Msg, Timeout).

metadata_put(FullPrefix, Key, Value) ->
    TsValue = {os:timestamp(), Value},
    vmq_swc:put(?SWC_GROUP, FullPrefix, Key, TsValue, []).

metadata_get(FullPrefix, Key) ->
    case vmq_swc:get(?SWC_GROUP, FullPrefix, Key, [{resolver, fun lww_resolver/1}]) of
        undefined -> undefined;
        {_Ts, Value} -> Value
    end.

metadata_delete(FullPrefix, Key) ->
    vmq_swc:delete(?SWC_GROUP, FullPrefix, Key).

metadata_fold(FullPrefix, Fun, Acc) ->
    vmq_swc:fold(?SWC_GROUP,
      fun(K, {_Ts, V}, AccAcc) ->
              Fun({K, V}, AccAcc)
      end,
      Acc, FullPrefix, [{resolver, fun lww_resolver/1}]).

metadata_subscribe(FullPrefix) ->
    ConvertFun = fun ({deleted, FP, Key, OldValues}) ->
                         {deleted, FP, Key, extract_val(lww_resolver(OldValues))};
                     ({updated, FP, Key, OldValues, Values}) ->
                         {updated, FP, Key,
                          extract_val(lww_resolver(OldValues)),
                          extract_val(lww_resolver(Values))}
                 end,
    vmq_swc_store:subscribe(vmq_swc:config(?SWC_GROUP), FullPrefix, ConvertFun).

lww_resolver([]) -> undefined;
lww_resolver([V]) -> V;
lww_resolver(TimestampedVals) ->
    [Newest|_] = lists:reverse(lists:keysort(1, TimestampedVals)),
    Newest.

extract_val({_Ts, Val}) -> Val;
extract_val(undefined) -> undefined.

