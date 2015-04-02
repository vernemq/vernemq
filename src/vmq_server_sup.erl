%% Copyright 2014 Erlio GmbH Basel Switzerland (http://erl.io)
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
    {ok, PlumtreeJobsOpts} = application:get_env(vmq_server, plumtree_jobs_opts),
    ok = jobs:add_queue(plumtree_queue, PlumtreeJobsOpts),
    {ok, { {one_for_one, 5, 10}, [
            ?CHILD(vmq_config, worker, []),
            ?CHILD(vmq_crl_srv, worker, []),
            ?CHILD(vmq_sysmon, worker, []),
            ?CHILD(vmq_session_proxy_sup, supervisor, []),
            ?CHILD(vmq_msg_store_sup, supervisor, []),
            ?CHILD(vmq_reg_sup, supervisor, []),
            ?CHILD(vmq_cluster_node_sup, supervisor, [])
                                 ]} }.

