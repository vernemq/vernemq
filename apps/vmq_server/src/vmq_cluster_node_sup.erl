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
-module(vmq_cluster_node_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,
         ensure_cluster_node/1,
         get_cluster_node/1,
         del_cluster_node/1,
         node_status/1]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type, Args), {I, {I, start_link, Args},
                               permanent, 5000, Type, [I]}).
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
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

ensure_cluster_node(Node) when Node == node() ->
    %% cluster node not needed
    ok;
ensure_cluster_node(Node) ->
    case get_cluster_node(Node) of
        {error, not_found} ->
            {ok, _} = supervisor:start_child(?MODULE, child_spec(Node)),
            ok;
        {ok, _} ->
            ok
    end.

del_cluster_node(Node) ->
    ChildId = {vmq_cluster_node, Node},
    case supervisor:terminate_child(?MODULE, ChildId) of
        ok ->
            supervisor:delete_child(?MODULE, ChildId);
        {error, not_found} ->
            {error, not_found}
    end.

get_cluster_node(Node) ->
    ChildId = {vmq_cluster_node, Node},
    case lists:keyfind(ChildId, 1, supervisor:which_children(?MODULE)) of
        false ->
            {error, not_found};
        {_, undefined, _, _} ->
            %% child was stopped
            {error, not_found};
        {_, restarting, _, _} ->
            %% child is restarting
            timer:sleep(100),
            get_cluster_node(Node);
        {_, Pid, _, _} when is_pid(Pid) ->
            {ok, Pid}
    end.

-spec node_status(node()) -> init | up | down.
node_status(Node) when Node == node() ->
    up;
node_status(Node) ->
    case get_cluster_node(Node) of
        {ok, Pid} when is_pid(Pid) ->
            vmq_cluster_node:status(Pid);
        _ ->
            down
    end.

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
    {ok, {{one_for_one, 5, 10}, [
            ?CHILD(vmq_cluster_mon, worker, [])
                                ]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
child_spec(Node) ->
    {{vmq_cluster_node, Node}, {vmq_cluster_node, start_link, [Node]},
     permanent, 5000, worker, [vmq_cluster_node]}.


