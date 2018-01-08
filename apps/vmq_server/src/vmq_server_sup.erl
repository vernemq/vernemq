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

-module(vmq_server_sup).

-behaviour(supervisor).
%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type, Args), {I, {I, start_link, Args},
                               permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

-spec start_link() -> 'ignore' | {'error',_} | {'ok',pid()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

-spec init([]) -> {'ok', {{'one_for_one', 5, 10},
                         [{atom(), {atom(), atom(), list()},
                           permanent, pos_integer(), worker, [atom()]}]}}.
init([]) ->
    {ok, MsgStoreChildSpecs} = application:get_env(vmq_server, msg_store_childspecs),

    maybe_change_nodename(),

    {ok, { {one_for_one, 5, 10},
           [?CHILD(vmq_config, worker, []) | MsgStoreChildSpecs]
           ++ [
               ?CHILD(vmq_crl_srv, worker, []),
               ?CHILD(vmq_queue_sup_sup, supervisor, [infinity, 5, 10]),
               ?CHILD(vmq_reg_sup, supervisor, []),
               ?CHILD(vmq_cluster_node_sup, supervisor, []),
               ?CHILD(vmq_sysmon, worker, []),
               ?CHILD(vmq_metrics_sup, supervisor, [])
              ]} }.

maybe_change_nodename() ->
    {ok, LocalState} = plumtree_peer_service_manager:get_local_state(),
    case riak_dt_orswot:value(LocalState) of
        [Node] when Node =/= node() ->
            lager:info("rename VerneMQ node from ~p to ~p", [Node, node()]),
            {ok, Actor} = plumtree_peer_service_manager:get_actor(),
            {ok, Merged} = riak_dt_orswot:update({update, [{remove, Node},
                                                           {add, node()}]}, Actor, LocalState),
            _ = gen_server:cast(plumtree_peer_service_gossip, {receive_state, Merged}),
            vmq_reg:fold_subscribers(
              fun(SubscriberId, Subs, _) ->
                      {NewSubs, _} = vmq_subscriber:change_node_all(Subs, node(), false),
                      vmq_subscriber_db:store(SubscriberId, NewSubs)
              end, ignored);
        _ ->
            %% we ignore if the node has the same name
            %% or if more than one node is returned (clustered)
            ignore
    end.
