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

-module(vmq_cluster).

-behaviour(gen_event).

%% gen_server callbacks
-export([init/1,
         handle_event/2,
         handle_call/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([nodes/0,
         recheck/0,
         status/0,
         is_ready/0,
         if_ready/2,
         if_ready/3,
         publish/2,
         remote_enqueue/2]).

-define(SERVER, ?MODULE).
-define(VMQ_CLUSTER_STATUS, vmq_status). %% table is owned by vmq_cluster_mon

-record(state, {}).
-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

recheck() ->
    case gen_event:call(plumtree_peer_service_events, ?MODULE, recheck, infinity) of
        ok -> ok;
        E ->
            lager:warning("error happened during cluster checkup ~p", [E]),
            E
    end.

-spec nodes() -> [any()].
nodes() ->
    [Node || [{Node, true}]
             <- ets:match(?VMQ_CLUSTER_STATUS, '$1'), Node /= ready].

status() ->
    [{Node, Ready} || [{Node, Ready}]
             <- ets:match(?VMQ_CLUSTER_STATUS, '$1'), Node /= ready].


-spec is_ready() -> boolean().
is_ready() ->
    ets:lookup(?VMQ_CLUSTER_STATUS, ready) == [{ready, true}].

-spec if_ready(_, _) -> any().
if_ready(Fun, Args) ->
    case is_ready() of
        true ->
            apply(Fun, Args);
        false ->
            {error, not_ready}
    end.
-spec if_ready(_, _, _) -> any().
if_ready(Mod, Fun, Args) ->
    case is_ready() of
        true ->
            apply(Mod, Fun, Args);
        false ->
            {error, not_ready}
    end.

publish(Node, Msg) ->
    case vmq_cluster_node_sup:get_cluster_node(Node) of
        {error, not_found} ->
            {error, not_found};
        {ok, Pid} ->
            vmq_cluster_node:publish(Pid, Msg)
    end.

remote_enqueue(Node, Term) ->
    case vmq_cluster_node_sup:get_cluster_node(Node) of
        {error, not_found} ->
            {error, not_found};
        {ok, Pid} ->
            vmq_cluster_node:enqueue(Pid, Term)
    end.

%%%===================================================================
%%% gen_event callbacks
%%%===================================================================
-spec init([]) -> {'ok', state()}.
init([]) ->
    check_ready(),
    lager:info("plumtree peer service event handler '~p' registered", [?MODULE]),
    {ok, #state{}}.

-spec handle_call(_, _) -> {'ok', 'ok', _}.
handle_call(recheck, State) ->
    _ = check_ready(),
    {ok, ok, State}.

-spec handle_event(_, _) -> {'ok', _}.
handle_event({update, _}, State) ->
    %% Plumtree event
    _ = check_ready(),
    {ok, State}.

handle_info(Info, State) ->
    lager:warning("got unhandled info ~p", [Info]),
    {ok, State}.

-spec terminate(_, _) -> 'ok'.
terminate(_Reason, _State) ->
    ok.

-spec code_change(_, _, _) -> {'ok', _}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
check_ready() ->
    {ok, LocalState} = plumtree_peer_service_manager:get_local_state(),
    Nodes = riak_dt_orswot:value(LocalState),
    check_ready(Nodes).

check_ready(Nodes) ->
    check_ready(Nodes, []),
    ets:foldl(fun({ready, _}, _) ->
                        ignore;
                   ({Node, _IsReady}, _) ->
                        case lists:member(Node, Nodes) of
                            true ->
                                ignore;
                            false ->
                                %% Node is not part of the cluster anymore
                                lager:warning("remove supervision for node ~p", [Node]),
                                _ = vmq_cluster_node_sup:del_cluster_node(Node),
                                ets:delete(?VMQ_CLUSTER_STATUS, Node)
                        end
                end, ok, ?VMQ_CLUSTER_STATUS),
    ok.

check_ready([Node|Rest], Acc) ->
    IsReady = case rpc:call(Node, erlang, whereis, [vmq_server_sup]) of
                       Pid when is_pid(Pid) -> true;
                       _ -> false
                   end,
    ok = vmq_cluster_node_sup:ensure_cluster_node(Node),
    check_ready(Rest, [{Node, IsReady}|Acc]);
check_ready([], Acc) ->
    ClusterReady =
    case lists:keyfind(false, 2, Acc) of
        false -> true;
        _ -> false
    end,
    ets:insert(?VMQ_CLUSTER_STATUS, [{ready, ClusterReady}|Acc]).
