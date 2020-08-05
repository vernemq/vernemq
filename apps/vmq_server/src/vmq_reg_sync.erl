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
-module(vmq_reg_sync).

-behaviour(gen_server).

%% API
-export([start_link/0,
         sync/3, sync/4,
         async/3, async/4,
         done/3,
         status/1, status/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {sync_queues=maps:new(),
                running=maps:new()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% vmq_reg_sync:
%% -------------
%% The goal of vmq_reg_sync is to offer a generic solution to synchronize
%% tasks in an Erlang cluster.
%% The task scheduling algorithm ensures (in a consistent cluster) the
%% following properties:
%%   1. For a specific task-type only one task runs at a time
%%   2. If a task owner dies it's running task is killed
%%   3. If a task owner dies it's queued tasks are removed and wont be exectuted
%%
%% How does it work:
%% ~~~~~~~~~~~~~~~~~
%% Every cluster node runs a vmq_reg_sync gen_server process as well as
%% a vmq_reg_sync_action_sup supervisor process.
%% A vmq_reg_sync:sync(SyncKey, Fun, Timeout) calculates (using hashing)
%% which node is responsible for the provided SyncKey. A remote gen_server
%% call enqueues the Fun into the queue for SyncKey. While the scheduling
%% may take place on a different node than the Caller noder, the Fun is, once
%% dequeued, run on the caller node by adding a child to the Caller local
%% vmq_reg_action_sync_sup.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec sync(any(), fun(() -> any()), pos_integer()) ->
    {error, not_ready | action_timeout | action_shutdown | action_not_alive} | any().
sync(SyncKey, Fun, Timeout) ->
    sync(SyncKey, Fun, sync_node(SyncKey), Timeout).

sync(SyncKey, Fun, SyncNode, Timeout) ->
    Owner = self(),
    call(SyncNode, {sync, Owner, SyncKey, Fun, Timeout}).

-spec async(any(), fun(() -> any()), pos_integer()) -> ok | {error, not_ready}.
async(SyncKey, Fun, Timeout) ->
    async(SyncKey, Fun, sync_node(SyncKey), Timeout).

async(SyncKey, Fun, SyncNode, Timeout) ->
    Owner = self(),
    call(SyncNode, {async, Owner, SyncKey, Fun, Timeout}).

call(Node, Req) ->
    try gen_server:call({?SERVER, Node}, Req, infinity)
    catch
        _:_ ->
            %% mostly happens in case of a netsplit
            {error, not_ready}
    end.

done(SyncPid, ActionPid, Reply) ->
    gen_server:call({?SERVER, node(SyncPid)}, {done, ActionPid, Reply}, infinity).

status(SyncKey) ->
    status(SyncKey, sync_node(SyncKey)).

status(SyncKey, SyncNode) ->
    gen_server:call({?SERVER, SyncNode}, {status, SyncKey}).

sync_node(SyncKey) ->
    Nodes = vmq_cluster:nodes(),
    I = erlang:phash2(SyncKey) rem length(Nodes) + 1,
    lists:nth(I, lists:sort(Nodes)).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, #state{}}.

handle_call({sync, Owner, SyncKey, Fun, Timeout}, From, State) ->
    NewState1 = sync_queue_in(SyncKey, {Fun, Owner, From, Timeout}, State),
    {noreply, sync_action(SyncKey, NewState1)};
handle_call({async, Owner, SyncKey, Fun, Timeout}, _From, State) ->
    NewState1 = sync_queue_in(SyncKey, {Fun, Owner, undefined, Timeout}, State),
    {reply, ok, sync_action(SyncKey, NewState1)};
handle_call({status, SyncKey}, _From, #state{running=Running} = State) ->
    SyncQ = get_sync_queue(SyncKey, State),
    NumQueued = queue:len(SyncQ),
    Reply = {NumQueued, maps:is_key(SyncKey, Running)},
    {reply, Reply, State};
handle_call({done, ActionPid, Reply}, _From, State) ->
    {reply, ok, sync_action_cleanup(ActionPid, Reply, State)}.

handle_cast(_Req, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
        {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
get_sync_queue(SyncKey, #state{sync_queues=SyncQs}) ->
    case maps:find(SyncKey, SyncQs) of
        {ok, Q} ->
            Q;
        error ->
            queue:new()
    end.

update_sync_queue(SyncKey, SyncQ, #state{sync_queues=SyncQs} = State) ->
    case queue:is_empty(SyncQ) of
        true -> State#state{sync_queues=maps:remove(SyncKey, SyncQs)};
        false -> State#state{sync_queues=maps:put(SyncKey, SyncQ, SyncQs)}
    end.

sync_queue_in(SyncKey, QItem, State) ->
    SyncQ = get_sync_queue(SyncKey, State),
    NewSyncQ = queue:in(QItem, SyncQ),
    update_sync_queue(SyncKey, NewSyncQ, State).

sync_queue_out(SyncKey, State) ->
    SyncQ = get_sync_queue(SyncKey, State),
    case queue:out(SyncQ) of
        {{value, QItem}, NewSyncQ} ->
            {ok, {QItem, update_sync_queue(SyncKey, NewSyncQ, State)}};
        {empty, _} ->
            empty
    end.

sync_action(SyncKey, #state{running=Running} = State) ->
    case maps:find(SyncKey, Running) of
        {ok, ActionPid} ->
            case alive(ActionPid) of
                true ->
                    State;
                false ->
                    %% We missed a vmq_reg_sync:done call, maybe due to netsplit
                    %% or timeout
                    sync_action_cleanup(ActionPid, {error, action_not_alive}, State)
            end;
        error ->
            case sync_queue_out(SyncKey, State) of
                {ok, {{Fun, Owner, From, Timeout}, NewState}} ->
                    case alive(Owner) of
                        true ->
                            {ok, ActionPid} = vmq_reg_sync_action_sup:start_action(self(), Owner, Fun, Timeout),
                            NewState#state{
                              % #{SyncKey => Pid, Pid => {SyncKey, From}}
                              running=maps:put(SyncKey, ActionPid,
                                               maps:put(ActionPid, {SyncKey, From}, Running))};
                        false ->
                            %% cleanup if an owner terminated
                            %% while another SyncAction was running
                            sync_action(SyncKey, NewState)
                    end;
                empty ->
                    State
            end
    end.

sync_action_cleanup(Pid, Reply, #state{running=Running} = State) ->
    case maps:find(Pid, Running) of
        error ->
            %% Unknown task completion message - perhaps this node was
            %% restarted or the supervision tree was restarted due to
            %% some bug.
            State;
        {ok, {SyncKey, From}} ->
            case Reply of
                {error, owner_terminated} ->
                    ignore;
                _ ->
                    reply(From, Reply)
            end,
            NewRunning = maps:remove(Pid, maps:remove(SyncKey, Running)),
            sync_action(SyncKey, State#state{running=NewRunning})
    end.

alive(Pid) ->
    Node = node(),
    case node(Pid) of
        Node -> is_process_alive(Pid);
        _ ->
            case rpc:pinfo(Pid, [status]) of
                [{status, _}] -> true;
                _ -> false
            end
    end.

reply(undefined, _) ->
    ok;
reply(From, Reply) ->
    gen_server:reply(From, Reply).
