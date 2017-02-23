%% Copyright 2016 Erlio GmbH Basel Switzerland (http://erl.io)
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
-export([start_link/1,
         reload/1,
         stats/1,
         call_function/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {luastate,
                samples= #{},
                script,
                keep}).

-define(MAX_SAMPLES, 100).


%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Script) ->
    gen_server:start_link(?MODULE, [Script], []).

reload(Pid) ->
    gen_server:call(Pid, reload).

stats(Pid) ->
    gen_server:call(Pid, stats, infinity).

call_function(Pid, Function, Args) ->
    gen_server:call(Pid, {call_function, Function, Args}).

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
init([Script]) ->
    case load_script(Script) of
        {ok, LuaState} ->
            KeepState = application:get_env(vmq_diversity, keep_state, false),
            {ok, #state{luastate=LuaState, script=Script, keep=KeepState}};
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
handle_call(reload, _From, #state{script=Script} = State) ->
    case load_script(Script) of
        {ok, LuaState} ->
            lager:info("successfully reloaded ~p", [Script]),
            {reply, ok, State#state{luastate=LuaState}};
        {error, Reason} ->
            lager:error("can't reload script due to ~p, keeping old version", [Reason]),
            {reply, {error, Reason}, State}
    end;
handle_call({call_function, Function, Args}, _From, State) ->
    Ts1 = os:timestamp(),
    try luerl:call_function([hooks, Function], [Args], State#state.luastate) of
        {[], NewLuaState} ->
            Ts2 = os:timestamp(),
            {reply, undefined, ch_state(NewLuaState, Function, Ts1, Ts2, State)};
        {[Val], NewLuaState} ->
            Ts2 = os:timestamp(),
            {reply, convert(Val), ch_state(NewLuaState, Function, Ts1, Ts2, State)}
    catch
        error:{lua_error, Reason, _} ->
            lager:error("can't call function ~p with args ~p in ~p due to ~p",
                        [Function, Args, State#state.script, Reason]),
            {reply, error, State};
        E:R ->
            lager:error("can't call function ~p with args ~p in ~p due to ~p",
                        [Function, Args, State#state.script, {E,R}]),
            {reply, error, State}
    end;
handle_call(stats, _From, #state{samples=Samples} = State) ->
    {reply, avg_t(Samples), State#state{samples=#{}}}.

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

ch_state(NewLuaState, Function, Ts1, Ts2, #state{keep=true, samples=Samples} = State) ->
    State#state{luastate=luerl:gc(NewLuaState),
                samples=add_ts(Function, Ts1, Ts2, Samples)};
ch_state(_, Function, Ts1, Ts2, #state{keep=false, samples=Samples} = State) ->
    State#state{samples=add_ts(Function, Ts1, Ts2, Samples)}.

load_script(Script) ->
    Libs = [
            {vmq_diversity_mysql,       <<"mysql">>},
            {vmq_diversity_postgres,    <<"postgres">>},
            {vmq_diversity_mongo,       <<"mongodb">>},
            {vmq_diversity_redis,       <<"redis">>},
            {vmq_diversity_http,        <<"http">>},
            {vmq_diversity_json,        <<"json">>},
            {vmq_diversity_bcrypt,      <<"bcrypt">>},
            {vmq_diversity_ets,         <<"kv">>},
            {vmq_diversity_lager,       <<"log">>},
            {vmq_diversity_memcached,   <<"memcached">>},
            {vmq_diversity_cache,       <<"auth_cache">>}
           ],

    {ok, ScriptsDir} = application:get_env(vmq_diversity, script_dir),
    AbsScriptDir = filename:absname(ScriptsDir),
    Do = "package.path = package.path .. \";" ++ AbsScriptDir ++ "/?.lua\"",
    {_, InitState} = luerl:do(Do),

    LuaState =
    lists:foldl(fun({Mod, NS}, LuaStateAcc) ->
                        luerl:load_module([<<"package">>, <<"loaded">>,
                                           <<"_G">>, NS], Mod, LuaStateAcc)
                end, InitState, Libs),

    try luerl:dofile(Script, LuaState) of
        {_, NewLuaState} ->
            case luerl:eval("return hooks", NewLuaState) of
                {ok, [nil]} ->
                    [];
                {ok, [Hooks]} ->
                    register_hooks(Hooks)
            end,
            {ok, NewLuaState}
    catch
        error:{lua_error, Reason, _} ->
            {error, Reason};
        E:R ->
            {error, {E, R}}
    end.

register_hooks([{Hook, {function, _}}|Rest]) ->
    vmq_diversity_plugin:register_hook(Hook),
    register_hooks(Rest);
register_hooks([_|Rest]) ->
    register_hooks(Rest);
register_hooks([]) -> ok.

convert(Val) when is_list(Val) ->
    convert_list(Val, []);
convert(Val) when is_number(Val) ->
    case round(Val) of
        RVal when RVal == Val -> RVal;
        _ -> Val
    end;
convert(Val) when is_binary(Val) -> Val;
convert(Val) when is_boolean(Val) -> Val;
convert(nil) -> undefined.

convert_list([ListItem|Rest], Acc) ->
    convert_list(Rest, [convert_list_item(ListItem)|Acc]);
convert_list([], Acc) -> lists:reverse(Acc).

convert_list_item({Idx, Val}) when is_integer(Idx) ->
    %% lua array
    convert(Val);
convert_list_item({BinKey, Val}) when is_binary(BinKey) ->
    try list_to_existing_atom(binary_to_list(BinKey)) of
        Key -> {Key, convert(Val)}
    catch
        _:_ ->
            {BinKey, convert(Val)}
    end.

add_ts(Function, Ts1, Ts2, Samples) ->
    T = timer:now_diff(Ts2, Ts1),
    case maps:find(Function, Samples) of
        {ok, FunSamples} when length(FunSamples) < ?MAX_SAMPLES ->
            maps:put(Function, [T|FunSamples], Samples);
        {ok, FunSamples} ->
            maps:put(Function, lists:droplast([T|FunSamples]), Samples);
        error ->
            maps:put(Function, [T], Samples)
    end.

avg_t(Samples) ->
    maps:fold(fun(K, V, Acc) ->
                      [{K, lists:sum(V) / length(V)}|Acc]
              end, [], Samples).
