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
%%
-module(vmq_cluster_node).
-include("vmq_server.hrl").

-behaviour(gen_server).

%% API
-export([start_link/1,
         publish/2,
         publish_batch/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {node,
                reachable=true,
                queue = queue:new(),
                batch_size,
                max_queue_size}).
-define(REMONITOR, 5000).

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
start_link(RemoteNode) ->
    gen_server:start_link(?MODULE, [RemoteNode], []).

publish(Pid, Msg) ->
    case catch gen_server:call(Pid, {publish, Msg}, infinity) of
        ok -> ok;
        {'EXIT', Reason} ->
            % we are not allowed to crash, this would
            % teardown the 'decoupled' publisher process
            {error, Reason}
    end.

publish_batch([#vmq_msg{mountpoint=MP,
                        routing_key=Topic,
                        reg_view=RegView} = Msg|Msgs]) ->
    _ = RegView:fold(MP, Topic, fun publish_batch_single/2, Msg),
    publish_batch(Msgs);
publish_batch([]) -> ok.

publish_batch_single({_,_} = SubscriberIdAndQoS, Msg) ->
    vmq_reg:publish(SubscriberIdAndQoS, Msg);
publish_batch_single(_Node, Msg) ->
    %% we ignore remote subscriptions, they are already covered
    %% by original publisher
    Msg.

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
init([RemoteNode]) ->
    MaxQueueSize = vmq_config:get_env(max_outgoing_publish_queue_size),
    BatchSize = vmq_config:get_env(outgoing_publish_batch_size),
    erlang:monitor_node(RemoteNode, true),
    {ok, #state{node=RemoteNode, batch_size=BatchSize, max_queue_size=MaxQueueSize}}.

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
handle_call({publish, Msg}, From, #state{queue=Q, reachable=true} = State) ->
    gen_server:reply(From, ok),
    {noreply, process_queue(State#state{queue=queue:in(Msg, Q)})};
handle_call({publish, Msg}, _From, #state{queue=Q, max_queue_size=Max,
                                          reachable=false} = State) ->
    case queue:len(Q) < Max of
        true ->
            {reply, ok, State#state{queue=queue:in(Msg, Q)}};
        false ->
            {reply, {error, outgoing_queue_full}, State}
    end.
%%
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
handle_info({nodedown, Node}, #state{node=Node} = State) ->
    erlang:send_after(?REMONITOR, self(), remonitor),
    {noreply, State#state{reachable=false}};
handle_info(remonitor, #state{node=Node} = State) ->
    erlang:monitor_node(Node, true),
    case net_adm:ping(Node) of
        pong ->
            {noreply, process_queue(State#state{reachable=true})};
        _ ->
            {noreply, State#state{reachable=false}}
    end.

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
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
process_queue(#state{node=Node, queue=Q, batch_size=BatchSize} = State) ->
    case queue:is_empty(Q) of
        true -> State;
        false ->
            State#state{queue=batch(Node, BatchSize, Q)}
    end.

batch(Node, BatchSize, Q) ->
    batch(Node, BatchSize, queue:out(Q), []).

batch(Node, BatchSize, {{value, V}, Q}, Batch) when length(Batch) < BatchSize ->
    batch(Node, BatchSize, queue:out(Q), [V|Batch]);
batch(Node, BatchSize, {{value, V}, Q}, Batch) when length(Batch) == BatchSize ->
    Msgs = lists:reverse([V|Batch]),
    case rpc:call(Node, ?MODULE, publish_batch, [Msgs]) of
        ok ->
            batch(Node, BatchSize, queue:out(Q), []);
        {badrpc, _} ->
            lists:foldl(fun(Item, AccQ) ->
                                queue:in_r(Item, AccQ)
                        end, Q, Batch)
    end;
batch(Node, _, {empty, Q} , Batch) ->
    Msgs = lists:reverse(Batch),
    case rpc:call(Node, ?MODULE, publish_batch, [Msgs]) of
        ok ->
            Q;
        {badrpc, _} ->
            lists:foldl(fun(Item, AccQ) ->
                                queue:in_r(Item, AccQ)
                        end, Q, Batch)
    end.
