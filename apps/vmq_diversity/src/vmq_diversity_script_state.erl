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

-module(vmq_diversity_script_state).
-include_lib("luerl/include/luerl.hrl").

-behaviour(gen_server).

%% API functions
-export([start_link/3,
         reload/1,
         get_hooks/1,
         get_num_states/1,
         call_function/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {id,
                luastate,
                script,
                keep,
                owner}).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec start_link(Id::list(), Script::list(), ScriptMgrPid::pid()) -> {ok, Pid::pid()} | ignore | {error, Error::term()}.
start_link(Id, Script, ScriptMgrPid) ->
    gen_server:start_link(?MODULE, [Id, Script, ScriptMgrPid], []).

reload(Pid) ->
    gen_server:call(Pid, reload, infinity).

get_hooks(Pid) ->
    gen_server:call(Pid, get_hooks, infinity).

get_num_states(Pid) ->
    gen_server:call(Pid, get_num_states, infinity).

call_function(Pid, Function, Args) ->
    Ref = make_ref(),
    Pid ! {call_function, Ref, self(), Function, Args},
    Ref.

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
init([Id, Script, ScriptMgrPid]) ->
    case load_script(Id, Script) of
        {ok, LuaState} ->
            KeepState =
            case lua_keep_state(LuaState) of
                undefined ->
                    application:get_env(vmq_diversity, keep_state, false);
                KS ->
                    KS
            end,
            ScriptMgrPid ! {state_ready, self()},
            {ok, #state{id=Id, luastate=LuaState,
                        script=Script, keep=KeepState}};
        {error, Reason} ->
            lager:error("can't load script ~p due to ~p", [Script, Reason]),
            %% normal stop as we don't want to exhaust the supervisor strategy
            {stop, normal}
    end.

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
handle_call(get_hooks, _From, #state{luastate=LuaState} = State) ->
    Hooks = lua_hooks(LuaState),
    {reply, Hooks, State};
handle_call(get_num_states, _From, #state{luastate=LuaState, keep=Keep} = State) ->
    NumStates =
    case lua_num_states(LuaState) of
        undefined when Keep -> 1;
        undefined ->
            {ok, N} = application:get_env(vmq_diversity, nr_lua_states),
            N;
        N when N > 0 -> N
    end,
    {reply, NumStates, State};
handle_call(reload, _From, #state{id=Id, script=Script} = State) ->
    case load_script(Id, Script) of
        {ok, LuaState} ->
            lager:info("successfully reloaded script ~p", [Script]),
            {reply, ok, State#state{luastate=LuaState}};
        {error, Reason} ->
            lager:error("can't reload script due to ~p, keeping old version", [Reason]),
            {reply, {error, Reason}, State}
    end.


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
handle_info({call_function, Ref, CallerPid, Function, Args}, State) ->
    {Reply, NewState} =
    try luerl:call_function([hooks, Function], [Args], State#state.luastate) of
        {[], NewLuaState} ->
            {undefined, ch_state(NewLuaState, State)};
        {[Val], NewLuaState} ->
            {Val, ch_state(NewLuaState, State)}
    catch
        error:{lua_error, Reason, _} ->
            lager:error("can't call function ~p with args ~p in ~p due to ~p",
                        [Function, Args, State#state.script, Reason]),
            {error, State};
        E:R ->
            lager:error("can't call function ~p with args ~p in ~p due to ~p",
                        [Function, Args, State#state.script, {E,R}]),
            {error, State}
    end,
    CallerPid ! {call_function_response, Ref, Reply},
    {noreply, NewState}.


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

ch_state(NewLuaState, #state{keep=true} = State) ->
    State#state{luastate=luerl:gc(NewLuaState)};
ch_state(_, #state{keep=false} = State) ->
    State.

load_script(Id, Script) ->
    Libs = [
            {vmq_diversity_mysql,       <<"mysql">>},
            {vmq_diversity_postgres,    <<"postgres">>},
            {vmq_diversity_cockroachdb, <<"cockroachdb">>},
            {vmq_diversity_mongo,       <<"mongodb">>},
            {vmq_diversity_redis,       <<"redis">>},
            {vmq_diversity_http,        <<"http">>},
            {vmq_diversity_json,        <<"json">>},
            {vmq_diversity_bcrypt,      <<"bcrypt">>},
            {vmq_diversity_ets,         <<"kv">>},
            {vmq_diversity_lager,       <<"log">>},
            {vmq_diversity_memcached,   <<"memcached">>},
            {vmq_diversity_cache,       <<"auth_cache">>},
            {vmq_diversity_vmq_api,     <<"vmq_api">>},
            {vmq_diversity_crypto,      <<"crypto">>}
           ],

    {ok, ScriptsDir} = application:get_env(vmq_diversity, script_dir),
    AbsScriptDir = filename:absname(ScriptsDir),
    Do1 = "package.path = package.path .. \";" ++ AbsScriptDir ++ "/?.lua\"",
    {_, InitState1} = luerl:do(Do1, luerl:init()),
    Do2 = "__SCRIPT_INSTANCE_ID__ = " ++ integer_to_list(Id),
    {_, InitState2} = luerl:do(Do2, InitState1),

    LuaState =
    lists:foldl(fun({Mod, NS}, LuaStateAcc) ->
                        luerl:load_module([<<"package">>, <<"loaded">>,
                                           <<"_G">>, NS], Mod, LuaStateAcc)
                end, InitState2, Libs),

    try luerl:dofile(Script, LuaState) of
        {_, NewLuaState} ->
            {ok, NewLuaState}
    catch
        error:{lua_error, Reason, _} ->
            {error, Reason};
        E:R ->
            {error, {E, R}}
    end.

lua_hooks(LuaState) ->
    case luerl:eval("return hooks", LuaState) of
        {ok, [nil]} -> [];
        {ok, [Hooks]} ->
            [Hook || {Hook, Fun} <- Hooks, is_function(Fun)]
    end.

lua_num_states(LuaState) ->
    case luerl:eval("return num_states", LuaState) of
        {ok, [nil]} -> undefined;
        {ok, [NumLuaState]} -> round(NumLuaState)
    end.

lua_keep_state(LuaState) ->
    case luerl:eval("return keep_state", LuaState) of
        {ok, [nil]} -> undefined;
        {ok, [KeepState]} when is_boolean(KeepState) -> KeepState
    end.
