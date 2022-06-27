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

%% @doc Simple gen_server for poolboy used to wrap a client process
%% and restart (after a configurable timeout) it if it dies for some
%% reason. The purpose is to protect poolboy from dying if an external
%% resource is not available and all the client processes die.
%% @end.
-module(vmq_diversity_worker_wrapper).

-behaviour(poolboy_worker).
-behaviour(gen_server).

%% API
-export([start_link/1, apply/4]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER, ?MODULE).

-record(state, {
    %% Name used for logging purposes.
    name = undefined,

    %% fun of arity 0 to start the worker.
    start_fun = undefined,

    %% fun of arity 1 used to terminate the worker when shutting
    %% down.
    terminate_fun = undefined,

    %% The worker PID if running.
    worker = undefined :: undefined | {reference(), pid()},

    %% Is the client process ready?
    connected = false :: boolean(),

    %% Reconnect timeout if the connection is lost (the worker
    %% process dies).
    reconnect_timeout :: non_neg_integer(),

    %% Reconnect timer reference.
    reconnect_tref :: undefined | reference()
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec start_link(Opts :: list()) -> {ok, Pid :: pid()} | {error, Error :: term()}.
start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

apply(Pid, Mod, Fun, Args) ->
    gen_server:call(Pid, {apply, Mod, Fun, Args}, infinity).

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
init(Opts) ->
    process_flag(trap_exit, true),
    self() ! connect,
    Name = proplists:get_value(name, Opts),
    StartFun = proplists:get_value(start_fun, Opts),
    true = is_function(StartFun, 0),
    TerminateFun = proplists:get_value(terminate_fun, Opts),
    true = TerminateFun =:= undefined orelse is_function(TerminateFun, 1),
    ReconnectTimeout = proplists:get_value(reconnect_timeout, Opts, 1000),
    {ok, #state{
        name = Name,
        start_fun = StartFun,
        terminate_fun = TerminateFun,
        reconnect_timeout = ReconnectTimeout,
        connected = false,
        worker = undefined
    }}.

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
handle_call({apply, _, _, _}, _From, #state{connected = false} = State) ->
    {reply, {error, not_connected}, State};
handle_call({apply, Mod, Fun, Args}, _From, #state{worker = {_MRef, Pid}} = State) when
    is_pid(Pid)
->
    Reply =
        try
            erlang:apply(Mod, Fun, [Pid | Args])
        catch
            exit:{noproc, _Reason} ->
                %% We remove the worker pid only in handle_info where
                %% the attempt to restart the process will also be
                %% kicked off.
                {error, not_connected}
        end,
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
handle_info(
    connect,
    #state{start_fun = StartFun, name = Name, worker = undefined, connected = false} = State
) ->
    NewState =
        case StartFun() of
            {ok, Pid} ->
                MRef = monitor(process, Pid),
                State#state{worker = {MRef, Pid}, connected = true, reconnect_tref = undefined};
            {error, Reason} ->
                lager:warning(
                    "Could not connect to ~p due to ~p",
                    [Name, Reason]
                ),
                schedule_reconnect(State#state{reconnect_tref = undefined})
        end,
    {noreply, NewState};
handle_info({'DOWN', MRef, process, WorkerPid, _}, #state{worker = {MRef, WorkerPid}} = S) ->
    NewState = schedule_reconnect(S#state{worker = undefined, connected = false}),
    {noreply, NewState};
handle_info({'EXIT', WorkerPid, _}, #state{worker = {_MRef, WorkerPid}} = S) ->
    %% We got an exit - let's wait for the monitor signal to arrive
    %% before cleaning up and scheduling a reconnect, but mark the
    %% connection as down.
    {noreply, S#state{connected = false}};
handle_info({'EXIT', _, _}, #state{worker = undefined, connected = false} = S) ->
    %% Got an exit when starting the client process. schedule a
    %% reconnect.
    NewState = schedule_reconnect(S),
    {noreply, NewState};
handle_info(_, S) ->
    {noreply, S}.

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
terminate(_Reason, #state{terminate_fun = TFun, worker = {_Mref, Pid}}) when
    TFun =/= undefined
->
    TFun(Pid),
    ok;
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
schedule_reconnect(#state{reconnect_tref = undefined, reconnect_timeout = Timeout} = S) ->
    TRef = erlang:send_after(Timeout, self(), connect),
    S#state{reconnect_tref = TRef};
schedule_reconnect(S) ->
    %% reconnect timer already scheduled.
    S.
