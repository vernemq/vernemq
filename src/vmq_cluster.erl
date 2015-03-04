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

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([nodes/0,
         is_ready/0,
         if_ready/2,
         if_ready/3,
         publish/2]).

-define(SERVER, ?MODULE).

-record(state, {}).
-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> 'ignore' | {'error',_} | {'ok',pid()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).


-spec nodes() -> [any()].
nodes() ->
    [Node || [{Node, true}]
             <- ets:match(vmq_status, '$1'), Node /= ready].

-spec is_ready() -> boolean().
is_ready() ->
    ets:lookup(vmq_status, ready) == [{ready, true}].

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

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec init([]) -> {'ok', state()}.
init([]) ->
    _ = ets:new(vmq_status, [{read_concurrency, true}, public, named_table]),
    _ = ets:insert(vmq_status, {ready, false}),
    Nodes = mnesia_cluster_utils:cluster_nodes(running),
    _ = [ets:insert(vmq_status, {Node, true}) ||Node <- Nodes],
    _ = check_ready(Nodes),
    net_kernel:monitor_nodes(true),
    {ok, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec handle_call(_, _, _) -> {'reply', 'ok', _}.
handle_call(_Req, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(_, _) -> {'noreply', _}.
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec handle_info(_, _) -> {'noreply', _}.
handle_info({nodedown, Node}, State) ->
    ets:delete(vmq_status, Node),
    _ = check_ready(),
    {noreply, State};
handle_info({nodeup, Node}, State) ->
    ets:insert(vmq_status, {Node, true}),
    _ = check_ready(),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec terminate(_, _) -> 'ok'.
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec code_change(_, _, _) -> {'ok', _}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
check_ready() ->
    Nodes = mnesia_cluster_utils:cluster_nodes(all),
    check_ready(Nodes).

check_ready(Nodes) ->
    check_ready(Nodes, [], ets:match(vmq_status, '$1')).

check_ready(MnesiaNodes, Acc, [[{ready, _}]|Rest]) ->
    check_ready(MnesiaNodes, Acc, Rest);
check_ready(MnesiaNodes, Acc, [[{Node, Status}]|Rest]) ->
    case lists:member(Node, MnesiaNodes) of
        true when Status == true ->
            ok = vmq_cluster_node_sup:ensure_cluster_node(Node),
            check_ready(MnesiaNodes -- [Node], Acc, Rest);
        true when Status == false ->
            ok = vmq_cluster_node_sup:ensure_cluster_node(Node),
            check_ready(MnesiaNodes -- [Node], [Node|Acc], Rest);
        false ->
            _ = vmq_cluster_node_sup:del_cluster_node(Node),
            ets:delete(vmq_status, Node),
            check_ready(MnesiaNodes -- [Node], Acc, Rest)
    end;
check_ready([], [], []) ->
    ets:insert(vmq_status, {ready, true});
check_ready(UnseenMnesiaNodes, _, []) ->
    _ = [vmq_cluster_node_sup:ensure_cluster_node(Node)
         || Node<- UnseenMnesiaNodes],
    ets:insert(vmq_status, {ready, false}).

