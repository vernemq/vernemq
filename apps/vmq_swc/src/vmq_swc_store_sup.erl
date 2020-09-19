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
-export([start_link/1, start_link/2]).

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
%% @end
%%--------------------------------------------------------------------
-spec start_link(SwcGroup::atom()) -> {ok, Pid::pid()} | ignore | {error, Error::term()}.
start_link(SwcGroup) ->
    start_link(SwcGroup, [{membership_strategy, auto}]).

start_link(SwcGroup, Opts) ->
    SupName = list_to_atom("vmq_swc_store_sup_" ++ atom_to_list(SwcGroup)),
    supervisor:start_link({local, SupName}, ?MODULE, [SwcGroup, Opts]).

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
%% @end
%%--------------------------------------------------------------------
init([SwcGroup, Opts]) ->

    MembershipStrategy = proplists:get_value(membership_strategy, Opts, auto),
%   {A,B,C} = erlang:timestamp(),
%   TS = integer_to_list(A) ++ integer_to_list(B) ++ integer_to_list(C),
   
%   PeerName = node(),
   {ok, Actor} = vmq_swc_peer_service_manager:get_actor(),
   SWC_ID = {node(), Actor},
%   list_to_atom(TS ++ atom_to_list(node())),

    DbBackend =
    case proplists:get_value(db_backend, Opts, application:get_env(vmq_swc, db_backend, leveldb)) of
        rocksdb -> vmq_swc_db_rocksdb;
        leveled -> vmq_swc_db_leveled;
        leveldb -> vmq_swc_db_leveldb
    end,

    DbOpts = proplists:get_value(db_opts, Opts, []),

    Config = config(SWC_ID, SwcGroup, DbBackend, vmq_swc_edist_srv),
    % this table is created by the root supervisor
    ets:insert(vmq_swc_group_config, {SwcGroup, Config}),

    ExchangeSupChildSpec = #{id => {vmq_swc_exchange_sup, SwcGroup},
                             start => {vmq_swc_exchange_sup, start_link, [Config]},
                             type => supervisor},

    TransportChildSpec = #{id => {vmq_swc_edist_srv, SwcGroup},
                           start => {vmq_swc_edist_srv, start_link, [Config]}},

    DBChildSpecs = vmq_swc_db:childspecs(DbBackend, Config, DbOpts),

    BatcherChildSpec = #{id => {vmq_swc_store_batcher, SwcGroup},
                         start => {vmq_swc_store_batcher, start_link, [Config]}},

    StoreChildSpec = #{id => {vmq_swc_store, SwcGroup},
                       start => {vmq_swc_store, start_link, [Config]}},

    MembershipChildSpec = #{id => {vmq_swc_group_membership, SwcGroup},
                            start => {vmq_swc_group_membership, start_link,
                                      [Config, MembershipStrategy, {vmq_swc_edist_srv, []}]}},


    RestartStrategy = one_for_one,
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    {ok, {SupFlags, DBChildSpecs ++ [TransportChildSpec, ExchangeSupChildSpec, MembershipChildSpec, BatcherChildSpec, StoreChildSpec]}}.

config(SWC_ID, SwcGroup, DbBackend, TransportMod) when is_atom(SwcGroup) and is_atom(TransportMod) ->
    SwcGroupStr = atom_to_list(SwcGroup),
    DBName = list_to_atom("vmq_swc_db_" ++ SwcGroupStr),
    StoreName = list_to_atom("vmq_swc_store_" ++ SwcGroupStr),
    CacheName = list_to_atom("vmq_swc_store_r_o_w_" ++ SwcGroupStr),
    BatcherName = list_to_atom("vmq_swc_store_batcher_" ++ SwcGroupStr),
    MembershipName = list_to_atom("vmq_swc_group_membership_" ++ SwcGroupStr),
    #swc_config{
       peer=SWC_ID,
       group=SwcGroup,
       db=DBName,
       db_backend=DbBackend,
       store=StoreName,
       r_o_w_cache=CacheName,
       batcher=BatcherName,
       membership=MembershipName,
       transport=TransportMod
      }.
