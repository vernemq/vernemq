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
%%
-module(vmq_ql_query_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,
         start_query/3]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

start_query(MgrPid, QueryString, Opts) when is_pid(MgrPid) ->
    Nodes = proplists:get_value(nodes, Opts, vmq_cluster:nodes()),
    lists:foldl(fun(Node, {AccRes,AccBad}) ->
                        try start_query_(Node, MgrPid, QueryString) of
                            {ok, Pid} -> {[Pid|AccRes], AccBad};
                            {error, _} -> {AccRes, [Node|AccBad]}
                        catch
                            %% if a remote supervisor isn't running
                            exit:{noproc, _} ->
                                {AccRes, [Node|AccBad]}
                        end
                end, {[],[]}, Nodes).

start_query_(Node, MgrPid, QueryString) ->
    supervisor:start_child({?MODULE, Node}, [MgrPid, QueryString]).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([]) ->
    {ok, {{simple_one_for_one, 0, 1},
          [{vmq_ql_query, {vmq_ql_query, start_link, []},
           temporary, 1000, worker, [vmq_ql_query_action]}]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
