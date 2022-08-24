%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Helium Systems, Inc.  All Rights Reserved.
%%
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
    lager:info("Sent join request to: ~p~n", [Node]),
    case net_kernel:connect_node(Node) of
        false ->
            lager:info("Unable to connect to ~p~n", [Node]),
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
            lager:warning("Cannot leave, not a member of a cluster.")
    end;
leave(_Args) ->
    leave([]).

stop() ->
    stop("received stop request").

stop(Reason) ->
    lager:notice("~p", [Reason]),
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
