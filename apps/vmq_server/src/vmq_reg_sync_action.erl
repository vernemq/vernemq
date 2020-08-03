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
-module(vmq_reg_sync_action).

-behaviour(gen_server).

%% API
-export([start_link/4]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {sync, owner, action}).

%%%===================================================================
%%% API
%%%===================================================================


-spec start_link(SyncPid::pid(), Owner::pid(), Fun::function(), Timeout::timeout()) -> {ok, Pid::pid()} | ignore | {error, Error::term()}.
start_link(SyncPid, Owner, Fun, Timeout) ->
    gen_server:start_link(?MODULE, [SyncPid, Owner, Fun, Timeout], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([SyncPid, OwnerPid, Fun, Timeout]) ->
    process_flag(trap_exit, true),
    monitor(process, OwnerPid),
    ActionPid = spawn_link(fun() -> Ret = Fun(), exit(Ret) end),
    {ok, #state{sync=SyncPid, owner=OwnerPid, action=ActionPid}, Timeout}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(timeout, State) ->
    exit(State#state.action, action_timeout),
    done(State#state.sync, {error, action_timeout}),
    {stop, normal, State};
handle_info({'EXIT', _Pid, Reply}, State) ->
    %% Action has terminated
    done(State#state.sync, Reply),
    {stop, normal, State};
handle_info({'DOWN', _, process, _, _Reason}, State) ->
    %% Owner terminated in the meantime, no need to
    done(State#state.sync, {error, owner_terminated}),
    {stop, normal, State}.

terminate(shutdown, State) ->
    exit(State#state.action, action_shutdown),
    done(State#state.sync, {error, action_shutdown}),
    ok;
terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
done(SyncPid, Reply) ->
    vmq_reg_sync:done(SyncPid, self(), Reply).
