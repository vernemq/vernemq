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
-define(CHILD(I, Type, Args), {I, {I, start_link, Args}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

-spec start_link() -> 'ignore' | {'error',_} | {'ok',pid()}.
start_link() ->
    {ok, Pid} = supervisor:start_link({local, ?MODULE}, ?MODULE, []),
    ok = vmq_endpoint:start_listeners(),
    {ok, Pid}.

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

-spec init([]) -> {'ok',{{'one_for_one',5,10},[{atom(),{atom(),atom(),list()},permanent,pos_integer(),worker,[atom()]}]}}.
init([]) ->
    %% we make sure the hooks for vmq_server are registered first
    vmq_hook:start(vmq_server),
    [vmq_hook:start(A) || {A, _, _}<- application:loaded_applications(),
                             A /= vmq_server],
    WorkerPoolSpec = poolboy:child_spec(vmq_worker_pool,
                                        [{name, {local, vmq_worker_pool}},
                                         {worker_module, vmq_worker},
                                         {size, erlang:system_info(schedulers)}],
                                        []),
    {ok, { {one_for_one, 5, 10}, [
            WorkerPoolSpec,
            ?CHILD(vmq_config, worker, []),
            ?CHILD(vmq_crl_srv, worker, []),
            ?CHILD(vmq_reg, worker, []),
            ?CHILD(vmq_session_expirer, worker, []),
            ?CHILD(vmq_cluster, worker, []),
            ?CHILD(vmq_systree, worker, []),
            ?CHILD(vmq_msg_store, worker, [])
                                 ]} }.

