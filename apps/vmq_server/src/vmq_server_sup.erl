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

-include("vmq_server.hrl").

-define(MaxR, application:get_env(vmq_server, max_r, 5)).
-define(MaxT, application:get_env(vmq_server, max_t, 10)).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type, Args), {I, {I, start_link, Args}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

-spec start_link() -> 'ignore' | {'error', _} | {'ok', pid()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

-spec init([]) ->
    {'ok',
        {{'one_for_one', 5, 10}, [
            {atom(), {atom(), atom(), list()}, permanent, pos_integer(), worker, [atom()]}
        ]}}.
init([]) ->
    persistent_term:put(subscribe_trie_ready, 0),

    SentinelEndpoints = vmq_schema_util:parse_list(
        application:get_env(vmq_server, redis_sentinel_endpoints, "[{\"127.0.0.1\", 26379}]")
    ),
    RedisDB = application:get_env(vmq_server, redis_sentinel_database, 0),

    {ok,
        {{one_for_one, 5, 10}, [
            ?CHILD(eredis, worker, [
                [
                    {sentinel, [{endpoints, SentinelEndpoints}]},
                    {database, RedisDB},
                    {name, {local, vmq_redis_client}}
                ]
            ]),
            ?CHILD(vmq_config, worker, []),
            ?CHILD(vmq_metrics_sup, supervisor, []),
            ?CHILD(vmq_crl_srv, worker, []),
            ?CHILD(vmq_queue_sup_sup, supervisor, [infinity, ?MaxR, ?MaxT]),
            ?CHILD(vmq_reg_sup, supervisor, []),
            ?CHILD(vmq_redis_queue_sup, supervisor, []),
            ?CHILD(vmq_redis_reaper_sup, supervisor, []),
            ?CHILD(vmq_cluster_mon, worker, []),
            ?CHILD(vmq_sysmon, worker, []),
            ?CHILD(vmq_ranch_sup, supervisor, [])
        ]}}.
