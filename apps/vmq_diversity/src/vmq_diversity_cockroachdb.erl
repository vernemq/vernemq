%% Copyright 2019 Octavo Labs AG Zurich Switzerland (https://octavolabs.com)
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

-module(vmq_diversity_cockroachdb).
-include_lib("luerl/include/luerl.hrl").

%% API functions
-export([
    install/1,
    squery/2,
    equery/3
]).

%%%===================================================================
%%% API functions
%%%===================================================================

install(St) ->
    luerl_emul:alloc_table(table(), St).

squery(PoolName, Sql) ->
    vmq_diversity_postgres:squery(PoolName, Sql).

equery(PoolName, Stmt, Params) ->
    vmq_diversity_postgres:equery(PoolName, Stmt, Params).

%%%===================================================================
%%% Internal functions
%%%===================================================================
table() ->
    [
        {<<"execute">>, #erl_func{code = fun execute/2}},
        {<<"ensure_pool">>, #erl_func{code = fun ensure_pool/2}},
        {<<"hash_method">>, #erl_func{code = fun hash_method/2}}
    ].

execute(As, St) ->
    vmq_diversity_postgres:execute(As, St).

ensure_pool(As, St) ->
    vmq_diversity_postgres:ensure_pool(As, St, cockroachdb, pool_cockroachdb).

hash_method(_, St) ->
    {ok, DBConfigs} = application:get_env(vmq_diversity, db_config),
    DefaultConf = proplists:get_value(cockroachdb, DBConfigs),
    HashMethod = proplists:get_value(password_hash_method, DefaultConf),
    {[atom_to_binary(HashMethod, utf8)], St}.
