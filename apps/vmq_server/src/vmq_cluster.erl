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

-module(vmq_cluster).

-include_lib("vmq_commons/include/vmq_types.hrl").

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
         netsplit_statistics/0,
         publish/2,
         remote_enqueue/3,
         remote_enqueue/4,
         remote_enqueue_async/3]).

-define(SERVER, ?MODULE).
-define(VMQ_CLUSTER_STATUS, vmq_status). %% table is owned by vmq_cluster_mon

-record(state, {}).
-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

recheck() ->
    case vmq_peer_service:call_event_handler(?MODULE, recheck, infinity) of
        ok -> ok;
        E ->
            lager:warning("error during cluster checkup due to ~p", [E]),
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
    [{ready, {Ready, _, _}}] = ets:lookup(?VMQ_CLUSTER_STATUS, ready),
    Ready.

-spec netsplit_statistics() -> {non_neg_integer(), non_neg_integer()}.
netsplit_statistics() ->
    case catch ets:lookup(?VMQ_CLUSTER_STATUS, ready) of
    [{ready, {_Ready, NetsplitDetectedCount, NetsplitResolvedCount}}] ->
    {NetsplitDetectedCount, NetsplitResolvedCount};
    {'EXIT', {badarg, _}} -> {error, vmq_status_table_down} % we don't have a vmq_status ETS table
    end.    

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

-spec remote_enqueue(node(), Term, BufferIfUnreachable)
        -> ok | {error, term()}
        when Term :: {enqueue_many, subscriber_id(), Msgs::term(), Opts::map()}
                   | {enqueue, Queue::term(), Msgs::term()},
             BufferIfUnreachable :: boolean().
remote_enqueue(Node, Term, BufferIfUnreachable) ->
    Timeout = vmq_config:get_env(remote_enqueue_timeout),
    remote_enqueue(Node, Term, BufferIfUnreachable, Timeout).

-spec remote_enqueue(node(), Term, BufferIfUnreachable, Timeout)
        -> ok | {error, term()}
        when Term :: {enqueue_many, subscriber_id(), Msgs::term(), Opts::map()}
                   | {enqueue, Queue::term(), Msgs::term()},
             BufferIfUnreachable :: boolean(),
             Timeout :: non_neg_integer() | infinity.
remote_enqueue(Node, Term, BufferIfUnreachable, Timeout) ->
    case vmq_cluster_node_sup:get_cluster_node(Node) of
        {error, not_found} ->
            {error, not_found};
        {ok, Pid} ->
            vmq_cluster_node:enqueue(Pid, Term, BufferIfUnreachable, Timeout)
    end.

remote_enqueue_async(Node, Term, BufferIfUnreachable) ->
    case vmq_cluster_node_sup:get_cluster_node(Node) of
        {error, not_found} ->
            {error, not_found};
        {ok, Pid} ->
            vmq_cluster_node:enqueue_async(Pid, Term, BufferIfUnreachable)
    end.

%%%===================================================================
%%% gen_event callbacks
%%%===================================================================
-spec init([]) -> {'ok', state()}.
init([]) ->
    check_ready(),
    lager:info("cluster event handler '~p' registered", [?MODULE]),
    {ok, #state{}}.

-spec handle_call(_, _) -> {'ok', 'ok', _}.
handle_call(recheck, State) ->
    _ = check_ready(),
    {ok, ok, State}.

-spec handle_event(_, _) -> {'ok', _}.
handle_event({update, _}, State) ->
    %% Cluster event
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
    Nodes = vmq_peer_service:members(),
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
    %% We should only say we're ready if we've established a
    %% connection to the remote node.
    Status = vmq_cluster_node_sup:node_status(Node),
    IsReady1 = IsReady andalso lists:member(Status, [up, init]),
    check_ready(Rest, [{Node, IsReady1}|Acc]);
check_ready([], Acc) ->
    OldObj =
    case ets:lookup(?VMQ_CLUSTER_STATUS, ready) of
        [] -> {true, 0, 0};
        [{ready, Obj}] -> Obj
    end,
    NewObj =
    case {all_nodes_alive(Acc), OldObj} of
        {true, {true, NetsplitDetectedCnt, NetsplitResolvedCnt}} ->
            % Cluster was consistent, is still consistent
            {true, NetsplitDetectedCnt, NetsplitResolvedCnt};
        {true, {false, NetsplitDetectedCnt, NetsplitResolvedCnt}} ->
            % Cluster was inconsistent, netsplit resolved
            {true, NetsplitDetectedCnt, NetsplitResolvedCnt + 1};
        {false, {true, NetsplitDetectedCnt, NetsplitResolvedCnt}} ->
            % Cluster was consistent, but isn't anymore
            {false, NetsplitDetectedCnt + 1, NetsplitResolvedCnt};
        {false, {false, NetsplitDetectedCnt, NetsplitResolvedCnt}} ->
            % Cluster was inconsistent, is still inconsistent
            {false, NetsplitDetectedCnt, NetsplitResolvedCnt}
    end,
    ets:insert(?VMQ_CLUSTER_STATUS, [{ready, NewObj}|Acc]).

-spec all_nodes_alive([{NodeName::atom(), IsReady::boolean()}]) -> boolean().
all_nodes_alive([{_NodeName, _IsReady = false}|_]) -> false;
all_nodes_alive([{_NodeName, _IsReady = true} |Rest]) ->
    all_nodes_alive(Rest);
all_nodes_alive([]) -> true.
