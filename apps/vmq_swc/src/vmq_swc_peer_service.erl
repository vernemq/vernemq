%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Helium Systems, Inc.  All Rights Reserved.
%% Copyright 2018-2024 Octavo Labs/VerneMQ (https://vernemq.com/)
%% and Individual Contributors.
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(vmq_swc_peer_service).
-include_lib("kernel/include/logger.hrl").

-export([
    join/1,
    join/2,
    join/3,
    attempt_join/1,
    attempt_join/2,
    leave/1,
    stop/0,
    stop/1
]).

%% @doc prepare node to join a cluster
join(Node) ->
    join(Node, true).

%% @doc Convert nodename to atom
join(NodeStr, Auto) when is_list(NodeStr) ->
    join(erlang:list_to_atom(lists:flatten(NodeStr)), Auto);
join(Node, Auto) when is_atom(Node) ->
    join(node(), Node, Auto).

%% @doc Initiate join. Nodes cannot join themselves.
join(Node, Node, _) ->
    {error, self_join};
join(_, Node, _Auto) ->
    attempt_join(Node).

attempt_join(Node) ->
    {_NrOfGroups, SwcGroups} = persistent_term:get({vmq_swc_plugin, swc}),
    PreventNonEmptyJoin = application:get_env(vmq_swc, prevent_nonempty_join, true),
    History = vmq_swc_plugin:history(SwcGroups),
    ?LOG_DEBUG("History before Join ~p~n", [History]),
    L = length(SwcGroups),
    case {History, PreventNonEmptyJoin} of
        {{L, 0, true}, _} ->
            bootstrap_join(Node, SwcGroups);
        {_, true} ->
            ?LOG_INFO("Cannot join a cluster, as local node ~p is non-empty.~n", [node()]),
            {error, non_empty_node};
        {_, false} ->
            connect_node(Node)
    end.

bootstrap_join(Node, Groups) ->
    ?LOG_INFO("Preparing snapshot bootstrap from discovery node: ~p~n", [Node]),
    case net_kernel:connect_node(Node) of
        false ->
            {error, not_reachable};
        true ->
            case bootstrap_source_ready(Node, Groups) of
                true ->
                    case prepare_snapshot_bootstrap(Groups, Node) of
                        ok ->
                            {ok, Local} = vmq_swc_peer_service_manager:get_local_state(),
                            ok = attempt_join(Node, Local),
                            wait_for_snapshot_bootstrap_and_transport(Groups);
                        {error, _} = Error ->
                            Error
                    end;
                false ->
                    {error, bootstrap_source_not_ready}
            end
    end.

bootstrap_source_ready(Node, Groups) ->
    case rpc:call(Node, vmq_swc_group_coordinator, sync_state, []) of
        SyncState when is_map(SyncState) ->
            lists:all(fun(Group) -> maps:get(Group, SyncState, false) end, Groups);
        _ ->
            false
    end.

prepare_snapshot_bootstrap([], _Peer) ->
    ok;
prepare_snapshot_bootstrap([Group | Rest], Peer) ->
    case vmq_swc_store:prepare_snapshot_bootstrap(vmq_swc:config(Group), Peer) of
        ok -> prepare_snapshot_bootstrap(Rest, Peer);
        {error, _} = Error -> Error
    end.

wait_for_snapshot_bootstrap_and_transport(Groups) ->
    Timeout = 5 * application:get_env(vmq_swc, sync_timeout, 60000),
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    case wait_for_snapshot_bootstrap(Groups, Deadline) of
        ok -> wait_for_cluster_transport(Deadline);
        Error -> Error
    end.

wait_for_snapshot_bootstrap(Groups, Deadline) ->
    case lists:all(
        fun(Group) -> vmq_swc_store:get_init_sync(vmq_swc:config(Group)) end, Groups
    ) of
        true ->
            ok;
        false ->
            case erlang:monotonic_time(millisecond) >= Deadline of
                true ->
                    {error, snapshot_bootstrap_timeout};
                false ->
                    timer:sleep(100),
                    wait_for_snapshot_bootstrap(Groups, Deadline)
            end
    end.

wait_for_cluster_transport(Deadline) ->
    case lists:keymember(vmq_server, 1, application:which_applications()) of
        false ->
            %% vmq_swc can run as a standalone application (including its CT
            %% suite), in which case no vmq_cluster transport stack exists.
            ok;
        true ->
            {ok, Membership} = vmq_swc_peer_service_manager:get_local_state(),
            Peers = riak_dt_orswot:value(Membership) -- [node()],
            case lists:all(fun cluster_transport_ready/1, Peers) of
                true ->
                    ok;
                false ->
                    case erlang:monotonic_time(millisecond) >= Deadline of
                        true ->
                            {error, cluster_transport_timeout};
                        false ->
                            timer:sleep(100),
                            wait_for_cluster_transport(Deadline)
                    end
            end
    end.

cluster_transport_ready(Peer) ->
    try
        %% Do not merely wait for vmq_cluster's periodic readiness check to
        %% notice the new membership. The join path promises a bidirectional
        %% transport barrier, so actively and idempotently establish both
        %% per-peer transport workers before checking their status.
        ok = vmq_cluster_node_sup:ensure_cluster_node(Peer),
        ok = rpc:call(Peer, vmq_cluster_node_sup, ensure_cluster_node, [node()]),
        vmq_cluster_node_sup:node_status(Peer) =:= up andalso
            rpc:call(Peer, vmq_cluster_node_sup, node_status, [node()]) =:= up
    catch
        _:_ -> false
    end.

connect_node(Node) ->
    ?LOG_INFO("Sent join request to: ~p~n", [Node]),
    case net_kernel:connect_node(Node) of
        false ->
            ?LOG_INFO("Unable to connect to ~p~n", [Node]),
            {error, not_reachable};
        true ->
            {ok, Local} = vmq_swc_peer_service_manager:get_local_state(),
            attempt_join(Node, Local)
    end.

attempt_join(Node, Local) ->
    {ok, Remote} = gen_server:call({vmq_swc_peer_service_gossip, Node}, send_state),
    Merged = riak_dt_orswot:merge(Remote, Local),
    %_ = vmq_swc_peer_service_manager:update_state(Merged),
    %% broadcast to all nodes
    %% get peer list
    Members = riak_dt_orswot:value(Merged),
    _ = [
        gen_server:cast({vmq_swc_peer_service_gossip, P}, {receive_state, Merged})
     || P <- Members
    ],
    ok.

leave(_Args) when is_list(_Args) ->
    {ok, Local} = vmq_swc_peer_service_manager:get_local_state(),
    {ok, Actor} = vmq_swc_peer_service_manager:get_actor(),
    {ok, Leave} = riak_dt_orswot:update({remove, node()}, Actor, Local),
    case random_peer(Leave) of
        {ok, Peer} ->
            {ok, Remote} = gen_server:call({vmq_swc_peer_service_gossip, Peer}, send_state),
            Merged = riak_dt_orswot:merge(Leave, Remote),
            _ = gen_server:cast({vmq_swc_peer_service_gossip, Peer}, {receive_state, Merged}),
            {ok, Remote2} = gen_server:call({vmq_swc_peer_service_gossip, Peer}, send_state),
            Remote2List = riak_dt_orswot:value(Remote2),
            case [P || P <- Remote2List, P =:= node()] of
                [] ->
                    %% leaving the cluster shuts down the node
                    vmq_swc_peer_service_manager:delete_state(),
                    stop("Leaving cluster");
                _ ->
                    leave([])
            end;
        {error, singleton} ->
            ?LOG_WARNING("Cannot leave, not a member of a cluster.")
    end;
leave(_Args) ->
    leave([]).

stop() ->
    stop("received stop request").

stop(Reason) ->
    ?LOG_NOTICE("~p", [Reason]),
    ok.

random_peer(Leave) ->
    Members = riak_dt_orswot:value(Leave),
    Peers = [P || P <- Members],
    case Peers of
        [] ->
            {error, singleton};
        _ ->
            Idx = rand:uniform(length(Peers)),
            Peer = lists:nth(Idx, Peers),
            {ok, Peer}
    end.
