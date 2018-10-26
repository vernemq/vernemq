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

-module(vmq_swc_store_sup).
-include("vmq_swc.hrl").
-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(SwcGroup) ->
    SupName = list_to_atom("vmq_swc_store_sup_" ++ atom_to_list(SwcGroup)),
    supervisor:start_link({local, SupName}, ?MODULE, [SwcGroup]).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @spec init(Args) -> {ok, {SupFlags, [ChildSpec]}} |
%%                     ignore |
%%                     {error, Reason}
%% @end
%%--------------------------------------------------------------------
init([SwcGroup]) ->

    PeerName = node(),

    Config = config(PeerName, SwcGroup, vmq_swc_edist_srv),
    % this table is created by the root supervisor
    ets:insert(vmq_swc_group_config, {SwcGroup, Config}),

    ExchangeSupChildSpec = #{id => {vmq_swc_exchange_sup, SwcGroup},
                             start => {vmq_swc_exchange_sup, start_link, [Config]},
                             type => supervisor},

    TransportChildSpec = #{id => {vmq_swc_edist_srv, SwcGroup},
                           start => {vmq_swc_edist_srv, start_link, [Config]}},

    DBChildSpecs = vmq_swc_db:childspecs({backend, vmq_swc_db_rocksdb}, Config, []),

    BatcherChildSpec = #{id => {vmq_swc_store_batcher, SwcGroup},
                         start => {vmq_swc_store_batcher, start_link, [Config]}},

    IteratorChildSpec = #{id => {vmq_swc_db_iterator, SwcGroup},
                          start => {vmq_swc_db_iterator, start_link, [Config]}},

    StoreChildSpec = #{id => {vmq_swc_store, SwcGroup},
                       start => {vmq_swc_store, start_link, [Config]}},

    MembershipChildSpec = #{id => {vmq_swc_group_membership, SwcGroup},
                            start => {vmq_swc_group_membership, start_link,
                                      [Config, plumtree, {vmq_swc_edist_srv, []}]}},


    RestartStrategy = one_for_one,
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    {ok, {SupFlags, DBChildSpecs ++ [TransportChildSpec, ExchangeSupChildSpec, MembershipChildSpec, BatcherChildSpec, IteratorChildSpec, StoreChildSpec]}}.

config(PeerName, SwcGroup, TransportMod) when is_atom(SwcGroup) and is_atom(TransportMod) ->
    SwcGroupStr = atom_to_list(SwcGroup),
    DBName = list_to_atom("vmq_swc_db_rocksdb_" ++ SwcGroupStr),
    StoreName = list_to_atom("vmq_swc_store_" ++ SwcGroupStr),
    ItrName = list_to_atom("vmq_swc_db_iterator_" ++ SwcGroupStr),
    BatcherName = list_to_atom("vmq_swc_store_batcher_" ++ SwcGroupStr),
    MembershipName = list_to_atom("vmq_swc_group_membership_" ++ SwcGroupStr),
    #swc_config{
       peer=PeerName,
       group=SwcGroup,
       db=DBName,
       store=StoreName,
       itr=ItrName,
       batcher=BatcherName,
       membership=MembershipName,
       transport=TransportMod
      }.
