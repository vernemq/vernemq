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

-module(vmq_diversity_script).

-behaviour(gen_server).

%% API functions
-export([
    start_link/1,
    stats/1,
    reload_script/1,
    call_function/3
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {
    luastates = [],
    working_luastates = [],
    queue = [],
    state_sup :: pid(),
    samples = #{}
}).

-define(MAX_SAMPLES, 100).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec start_link(ScriptPath :: list()) -> {ok, Pid :: pid()} | ignore | {error, Error :: term()}.
start_link(ScriptPath) ->
    gen_server:start_link(?MODULE, [ScriptPath], []).

stats(Pid) ->
    gen_server:call(Pid, stats, infinity).

reload_script(Pid) ->
    gen_server:call(Pid, reload_script, infinity).

call_function(Pid, Function, Args) ->
    %% Sandbox the gen_server call and ensure that we don't crash.
    %% As vmq_plugin calls are not executed within a try-catch an
    %% error in the plugin code can crash session/queue. As the Lua
    %% support should provide a sandboxed environment we saveguard
    %% calls into the Lua environment.
    case catch gen_server:call(Pid, {call_function, Function, Args}, infinity) of
        {'EXIT', Reason} ->
            lager:error("can't call into Lua sandbox for function ~p due to ~p", [Function, Reason]),
            {error, Reason};
        Ret ->
            Ret
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
init([ScriptPath]) ->
    {ok, StateSup} = vmq_diversity_script_state_sup:start_link(),
    ScriptMgrPid = self(),
    LuaStatePids = setup_lua_states(StateSup, ScriptPath, ScriptMgrPid),
    maybe_register_hooks(ScriptMgrPid, LuaStatePids),
    {ok, #state{
        luastates = [],
        state_sup = StateSup
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
handle_call({call_function, Function, Args}, From, State) ->
    NewState = queue_function_call(Function, Args, From, State),
    {noreply, schedule_function_call(NewState)};
handle_call(reload_script, _From, #state{state_sup = SupPid} = State) ->
    lists:foreach(
        fun
            ({{vmq_diversity_script_state, _}, Child, _, _}) when is_pid(Child) ->
                vmq_diversity_script_state:reload(Child);
            (_) ->
                ignore
        end,
        supervisor:which_children(SupPid)
    ),
    {reply, ok, State};
handle_call(stats, _From, #state{samples = Samples} = State) ->
    {reply, avg_t(Samples), State#state{samples = #{}}}.

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
handle_info({state_ready, StatePid}, #state{luastates = LuaStates} = State) ->
    _MRef = monitor(process, StatePid),
    {noreply, State#state{luastates = [StatePid | LuaStates]}};
handle_info(
    {'DOWN', _MRef, process, StatePid, _},
    #state{
        luastates = LuaStates,
        working_luastates = WLuaStates
    } = State
) ->
    {noreply, State#state{
        luastates = lists:delete(StatePid, LuaStates),
        working_luastates = lists:keydelete(StatePid, 2, WLuaStates)
    }};
handle_info(
    {call_function_response, Ref, Reply},
    #state{luastates = LuaStates, working_luastates = WorkingLuaStates} = State
) ->
    case lists:keyfind(Ref, 1, WorkingLuaStates) of
        false ->
            {noreply, State};
        {Ref, LuaStatePid, Item} ->
            {From, Function, _, Ts1} = Item,
            Ts2 = os:timestamp(),
            gen_server:reply(From, Reply),
            {noreply,
                schedule_function_call(
                    ch_state(
                        Function,
                        Ts1,
                        Ts2,
                        State#state{
                            %% round-robin: append instead of prepend
                            luastates = LuaStates ++ [LuaStatePid],
                            working_luastates = lists:keydelete(
                                Ref, 1, WorkingLuaStates
                            )
                        }
                    )
                )}
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

queue_function_call(Function, Args, From, #state{queue = Queue} = State) ->
    Item = {From, Function, Args, os:timestamp()},
    State#state{queue = [Item | Queue]}.

schedule_function_call(
    #state{
        queue = Queue,
        luastates = [LuaStatePid | LuaStatesRest],
        working_luastates = WorkingLuaStates
    } = State
) when
    length(Queue) > 0
->
    [{_From, Function, Args, _Ts} = Item | NewQueueRev] = lists:reverse(Queue),
    Ref = vmq_diversity_script_state:call_function(LuaStatePid, Function, Args),
    State#state{
        queue = lists:reverse(NewQueueRev),
        luastates = LuaStatesRest,
        working_luastates = [{Ref, LuaStatePid, Item} | WorkingLuaStates]
    };
schedule_function_call(State) ->
    %% All LuaStates currently occupied or no item in the queue
    State.

ch_state(Function, Ts1, Ts2, #state{samples = Samples} = State) ->
    State#state{samples = add_ts(Function, Ts1, Ts2, Samples)}.

add_ts(Function, Ts1, Ts2, Samples) ->
    T = timer:now_diff(Ts2, Ts1),
    case maps:find(Function, Samples) of
        {ok, FunSamples} when length(FunSamples) < ?MAX_SAMPLES ->
            maps:put(Function, [T | FunSamples], Samples);
        {ok, FunSamples} ->
            maps:put(Function, lists:droplast([T | FunSamples]), Samples);
        error ->
            maps:put(Function, [T], Samples)
    end.

avg_t(Samples) ->
    maps:fold(
        fun(K, V, Acc) ->
            [{K, lists:sum(V) / length(V)} | Acc]
        end,
        [],
        Samples
    ).

maybe_register_hooks(StateMgrPid, [FirstLuaStatePid | _]) ->
    case vmq_diversity_script_state:get_hooks(FirstLuaStatePid) of
        [] ->
            ok;
        Hooks when is_list(Hooks) ->
            lists:foreach(
                fun(Hook) ->
                    vmq_diversity_plugin:register_hook(StateMgrPid, Hook)
                end,
                Hooks
            )
    end.

setup_lua_states(StateSup, ScriptPath, ScriptMgrPid) ->
    {ok, FirstLuaStatePid} = vmq_diversity_script_state_sup:start_state(
        StateSup, 1, ScriptPath, ScriptMgrPid
    ),
    case vmq_diversity_script_state:get_num_states(FirstLuaStatePid) of
        1 ->
            [FirstLuaStatePid];
        NumLuaStates when NumLuaStates > 1 ->
            LuaStatePids =
                lists:foldl(
                    fun(Id, Acc) ->
                        {ok, LuaStatePid} = vmq_diversity_script_state_sup:start_state(
                            StateSup, Id, ScriptPath, ScriptMgrPid
                        ),
                        [LuaStatePid | Acc]
                    end,
                    [FirstLuaStatePid],
                    lists:seq(2, NumLuaStates)
                ),
            lists:reverse(LuaStatePids)
    end.
