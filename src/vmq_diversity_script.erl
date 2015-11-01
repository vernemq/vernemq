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
                script}).

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
            {ok, #state{luastate=LuaState, script=Script}};
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
handle_call({call_function, Function, Args}, _From, #state{samples=Samples} = State) ->
    Ts1 = os:timestamp(),
    try luerl:call_function([hooks, Function], [Args], State#state.luastate) of
        {[true], NewLuaState} ->
            Ts2 = os:timestamp(),
            {reply, ok, State#state{luastate=NewLuaState,
                                    samples=add_ts(Function, Ts1, Ts2, Samples)}};
        {[false], NewLuaState} ->
            Ts2 = os:timestamp(),
            {reply, error, State#state{luastate=NewLuaState,
                                       samples=add_ts(Function, Ts1, Ts2, Samples)}};
        {[], NewLuaState} ->
            Ts2 = os:timestamp(),
            {reply, next, State#state{luastate=NewLuaState,
                                      samples=add_ts(Function, Ts1, Ts2, Samples)}};
        {[[{BinKey, _}|_] = BinPropList], NewLuaState} when is_binary(BinKey) ->
            Ts2 = os:timestamp(),
            {reply, convert_to_proplist(BinPropList, []),
             State#state{luastate=NewLuaState,
                         samples=add_ts(Function, Ts1, Ts2, Samples)}}
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

load_script(Script) ->
    LuaState0 = luerl:load_module([<<"package">>, <<"loaded">>, <<"_G">>, <<"mysql">>],
                                  vmq_diversity_mysql, luerl:init()),
    LuaState1 = luerl:load_module([<<"package">>, <<"loaded">>, <<"_G">>, <<"pgsql">>],
                                  vmq_diversity_postgres, LuaState0),
    LuaState2 = luerl:load_module([<<"package">>, <<"loaded">>, <<"_G">>, <<"mongodb">>],
                                  vmq_diversity_mongo, LuaState1),
    LuaState3 = luerl:load_module([<<"package">>, <<"loaded">>, <<"_G">>, <<"redis">>],
                                  vmq_diversity_redis, LuaState2),
    LuaState4 = luerl:load_module([<<"package">>, <<"loaded">>, <<"_G">>, <<"http">>],
                                  vmq_diversity_http, LuaState3),
    LuaState5 = luerl:load_module([<<"package">>, <<"loaded">>, <<"_G">>, <<"json">>],
                                  vmq_diversity_json, LuaState4),
    LuaState6 = luerl:load_module([<<"package">>, <<"loaded">>, <<"_G">>, <<"kv">>],
                                  vmq_diversity_ets, LuaState5),

    try luerl:dofile(Script, LuaState6) of
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

convert_to_proplist([{<<"mountpoint">>, MP}|Rest], Acc) ->
    convert_to_proplist(Rest, [{mountpoint, binary_to_list(MP)}|Acc]);
convert_to_proplist([{BinKey, Val}|Rest], Acc) ->
    try list_to_existing_atom(binary_to_list(BinKey)) of
        Key ->
            convert_to_proplist(Rest, [{Key, Val}|Acc])
    catch
        _:_ ->
            convert_to_proplist(Rest, [{BinKey, Val}|Acc])
    end;
convert_to_proplist([], Acc) -> {ok, Acc}.

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
