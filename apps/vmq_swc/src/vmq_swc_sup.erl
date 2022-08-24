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

%%%-------------------------------------------------------------------
%% @doc vmq_swc top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(vmq_swc_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    ets:new(vmq_swc_group_config, [named_table, public, {read_concurrency, true}]),
    MetricsWorker = #{
        id => vmq_swc_metrics,
        start => {vmq_swc_metrics, start_link, []}
    },
    GossipWorker = #{
        id => vmq_swc_peer_service_gossip,
        start => {vmq_swc_peer_service_gossip, start_link, []}
    },
    EventsWorker = #{
        id => vmq_swc_peer_service_events,
        start => {vmq_swc_peer_service_events, start_link, []}
    },

    _State = vmq_swc_peer_service_manager:init(),

    {ok, {{one_for_one, 1000, 3600}, [MetricsWorker, GossipWorker, EventsWorker]}}.

%%====================================================================
%% Internal functions
%%====================================================================
