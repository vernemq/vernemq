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

-module(vmq_swc_exchange_sup).
-include("vmq_swc.hrl").

-behaviour(supervisor).

%% API
-export([
    start_link/1,
    start_exchange/3
]).

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
-spec start_link(SwcConfig :: config()) -> {ok, Pid :: pid()} | ignore | {error, Error :: term()}.
start_link(#swc_config{group = SwcGroup} = _Config) ->
    SupName = sup_name(SwcGroup),
    supervisor:start_link({local, SupName}, ?MODULE, []).

start_exchange(#swc_config{group = SwcGroup} = Config, Peer, Timeout) ->
    SupName = sup_name(SwcGroup),
    supervisor:start_child(SupName, #{
        id => {vmq_swc_exchange_fsm, Peer},
        start => {vmq_swc_exchange_fsm, start_link, [Config, Peer, Timeout]},
        restart => temporary
    }).

sup_name(SwcGroup) ->
    list_to_atom("vmq_swc_exchange_sup_" ++ atom_to_list(SwcGroup)).

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
init([]) ->
    {ok, {{one_for_one, 1000, 3600}, []}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
