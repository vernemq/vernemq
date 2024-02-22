%% Copyright 2018 Octavo Labs AG Zurich Switzerland (https://octavolabs.com)
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

-module(vmq_swc_plugin).

-export([
    metadata_put/3,
    metadata_get/2,
    metadata_get/3,
    metadata_delete/2,
    metadata_fold/3,
    metadata_subscribe/1,

    cluster_join/1,
    cluster_leave/1,
    cluster_members/0,
    cluster_rename_member/2,
    cluster_events_add_handler/2,
    cluster_events_delete_handler/2,
    cluster_events_call_handler/3,

    plugin_start/0, plugin_start/1,
    plugin_stop/0,
    summary/0, summary/1,
    history/0, history/1
]).

-define(METRIC, metadata).
-define(INFO_KEY, {?MODULE, swc}).
-define(NR_OF_GROUPS, (application:get_env(vmq_swc, swc_groups, 10))).
-define(SWC_GROUPS, (persistent_term:get(?INFO_KEY))).

plugin_start() ->
    SWCGroups = [list_to_atom("meta" ++ integer_to_list(X)) || X <- lists:seq(1, ?NR_OF_GROUPS)],
    ok = persistent_term:put(?INFO_KEY, {?NR_OF_GROUPS, SWCGroups}),
    _ = [vmq_swc:start(G) || G <- SWCGroups],
    ok.

plugin_stop() ->
    {_, SWCGroups} = ?SWC_GROUPS,
    _ = [vmq_swc:stop(G) || G <- SWCGroups],
    persistent_term:erase(?INFO_KEY),
    ok.
% for tests
plugin_start(SWCGroups) ->
    ok = persistent_term:put(?INFO_KEY, {length(SWCGroups), SWCGroups}),
    _ = [vmq_swc:start(G) || G <- SWCGroups],
    ok.

group_for_key(PKey) ->
    {NrOfGroups, SWCGroups} = ?SWC_GROUPS,
    lists:nth((erlang:phash2(PKey) rem NrOfGroups) + 1, SWCGroups).

cluster_join(DiscoveryNode) ->
    vmq_swc_peer_service:join(DiscoveryNode).

cluster_leave(Node) ->
    {ok, Local} = vmq_swc_peer_service_manager:get_local_state(),
    {ok, Actor} = vmq_swc_peer_service_manager:get_actor(),
    case riak_dt_orswot:update({remove, Node}, Actor, Local) of
        {error, {precondition, {not_present, Node}}} ->
            {error, not_present};
        {ok, Merged} ->
            AllNodes = riak_dt_orswot:value(Local),
            % multi_cast so we don't need to wait for the next gossip round
            multi_cast(AllNodes, vmq_swc_peer_service_gossip, {receive_state, Merged}),
            {ok, Local2} = vmq_swc_peer_service_manager:get_local_state(),
            Local2List = riak_dt_orswot:value(Local2),
            case [P || P <- Local2List, P =:= Node] of
                [] ->
                    vmq_swc_peer_service_manager:delete_state(),
                    ok;
                _ ->
                    cluster_leave(Node)
            end
    end.

multi_cast([Node | Rest], RegName, Msg) ->
    _ = gen_server:cast({RegName, Node}, Msg),
    multi_cast(Rest, RegName, Msg);
multi_cast([], _, _) ->
    timer:sleep(1000),
    ok.

cluster_members() ->
    {_, [FirstGroup | _]} = ?SWC_GROUPS,
    Config = vmq_swc:config(FirstGroup),
    vmq_swc_group_membership:get_members(Config).

cluster_rename_member(OldName, NewName) ->
    {ok, LocalState} = vmq_swc_peer_service_manager:get_local_state(),
    {ok, Actor} = vmq_swc_peer_service_manager:get_actor(),
    {ok, Merged} = riak_dt_orswot:update(
        {update, [
            {remove, OldName},
            {add, NewName}
        ]},
        Actor,
        LocalState
    ),
    _ = gen_server:cast(vmq_swc_peer_service_gossip, {receive_state, Merged}).

cluster_events_add_handler(Module, Opts) ->
    vmq_swc_peer_service_events:add_sup_handler(Module, Opts).

cluster_events_delete_handler(Module, Reason) ->
    gen_event:delete_handler(vmq_swc_peer_service_events, Module, [Reason]).

cluster_events_call_handler(Module, Msg, Timeout) ->
    gen_event:call(vmq_swc_peer_service_events, Module, Msg, Timeout).

metadata_put(FullPrefix, Key, Value) ->
    TsValue = {os:timestamp(), Value},
    vmq_swc_metrics:timed_measurement(
        {?METRIC, put},
        vmq_swc,
        put,
        [group_for_key({FullPrefix, Key}), FullPrefix, Key, TsValue, []]
    ).

metadata_get(FullPrefix, Key) ->
    metadata_get(FullPrefix, Key, [{resolver, fun lww_resolver/1}]).

metadata_get(FullPrefix, Key, Opts) ->
    case
        vmq_swc_metrics:timed_measurement(
            {?METRIC, get},
            vmq_swc,
            get,
            [
                group_for_key({FullPrefix, Key}),
                FullPrefix,
                Key,
                Opts
            ]
        )
    of
        {_Ts, Value} -> Value;
        Default -> Default
    end.

metadata_delete(FullPrefix, Key) ->
    vmq_swc_metrics:timed_measurement(
        {?METRIC, delete},
        vmq_swc,
        delete,
        [group_for_key({FullPrefix, Key}), FullPrefix, Key]
    ).

metadata_fold(FullPrefix, Fun, Acc) ->
    {_, SWCGroups} = ?SWC_GROUPS,
    vmq_swc_metrics:timed_measurement(
        {?METRIC, fold}, lists, foldl, [
            fun(Group, AccAcc) ->
                vmq_swc:fold(
                    Group,
                    fun(K, {_Ts, V}, AccAccAcc) ->
                        Fun({K, V}, AccAccAcc)
                    end,
                    AccAcc,
                    FullPrefix,
                    [{resolver, fun lww_resolver/1}]
                )
            end,
            Acc,
            SWCGroups
        ]
    ).

metadata_subscribe(FullPrefix) ->
    {_, SWCGroups} = ?SWC_GROUPS,
    ConvertFun = fun
        ({deleted, FP, Key, OldValues}) ->
            {deleted, FP, Key, extract_val(lww_resolver(OldValues))};
        ({updated, FP, Key, OldValues, Values}) ->
            {updated, FP, Key, extract_val(lww_resolver(OldValues)),
                extract_val(lww_resolver(Values))}
    end,
    lists:foreach(
        fun(Group) ->
            vmq_swc_store:subscribe(vmq_swc:config(Group), FullPrefix, ConvertFun)
        end,
        SWCGroups
    ).

lww_resolver([]) ->
    undefined;
lww_resolver([V]) ->
    V;
lww_resolver(TimestampedVals) ->
    [Newest | _] = lists:reverse(lists:keysort(1, TimestampedVals)),
    Newest.

extract_val({_Ts, Val}) -> Val;
extract_val(undefined) -> undefined.

summary() ->
    summary(?SWC_GROUPS).
summary(SWCGroups) ->
    {ok, Actor} = vmq_swc_peer_service_manager:get_actor(),
    Node = node(),
    NodeClocks = [
        vmq_swc_store:node_clock_by_storename(
            list_to_atom("vmq_swc_store_" ++ atom_to_list(SWCGroup))
        )
     || SWCGroup <- SWCGroups
    ],
    [{maps:get({Node, Actor}, NC), maps:size(NC)} || NC <- NodeClocks].

% The Node is empty when all local Nodeclocks in SWCGroups are
% 0 and we only have the local Node in the Nodeclocks.
% In other words: history/1 returns {0,0,true}, in case
% the node has no history.
history() ->
    SWCGroups = [list_to_atom("meta" ++ integer_to_list(X)) || X <- lists:seq(1, ?NR_OF_GROUPS)],
    history(SWCGroups).
history(SWCGroups) ->
    LocalClockList = summary(SWCGroups),
    NrOfGroups = length(SWCGroups),
    {{LocalDots, Gap}, TotalClocks} =
        lists:foldl(
            fun(X, {{A, B}, C}) ->
                {{N, M}, Z} = X,
                {{A + N, B + M}, C + Z}
            end,
            {{0, 0}, 0},
            LocalClockList
        ),
    NeverClustered = NrOfGroups == TotalClocks,
    {LocalDots, Gap, NeverClustered}.
