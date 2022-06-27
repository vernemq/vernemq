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

-module(vmq_diversity_memcached).
-include_lib("luerl/include/luerl.hrl").

-behaviour(gen_server).
-behaviour(poolboy_worker).

%% API functions
-export([
    start_link/1,
    install/1
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

-import(luerl_lib, [badarg_error/3]).

-record(state, {conn}).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec start_link(Args :: list()) -> {ok, Pid :: pid()} | {error, Error :: term()}.
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

install(St) ->
    luerl_emul:alloc_table(table(), St).

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
init(Args) ->
    {ok, Conn} = mcd:start_link(Args),
    {ok, #state{conn = Conn}}.

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
handle_call({q, Command}, _From, #state{conn = Conn} = State) ->
    {reply, handle_command(Conn, Command), State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

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
table() ->
    [
        {<<"ensure_pool">>, #erl_func{code = fun ensure_pool/2}},
        {<<"flush_all">>, #erl_func{code = fun flush_all/2}},
        {<<"get">>, #erl_func{code = fun get/2}},
        {<<"set">>, #erl_func{code = fun set/2}},
        {<<"delete">>, #erl_func{code = fun delete/2}},
        {<<"add">>, #erl_func{code = fun add/2}},
        {<<"replace">>, #erl_func{code = fun replace/2}}
    ].

flush_all([_] = As, St) ->
    Cmd = [flush_all],
    do_cmd(Cmd, As, St).

get([_, Key] = As, St) when is_binary(Key) ->
    Cmd = [get, Key],
    do_cmd(Cmd, As, St).

set([_, Key, Val, Expiration] = As, St) when
    is_binary(Key), is_binary(Val), is_number(Expiration)
->
    Cmd = [{set, 16#abba, trunc(Expiration)}, Key, Val],
    do_cmd(Cmd, As, St);
set([_, Key, Val] = As, St) when is_binary(Key), is_binary(Val) ->
    Exp = 0,
    set(As ++ [Exp], St).

delete([_, Key] = As, St) when is_binary(Key) ->
    Cmd = [delete, Key],
    do_cmd(Cmd, As, St).

add([_, Key, Val, Expiration] = As, St) when
    is_binary(Key), is_binary(Val), is_number(Expiration)
->
    Cmd = [{add, 16#abba, trunc(Expiration)}, Key, Val],
    do_cmd(Cmd, As, St);
add([_, Key, Val] = As, St) when is_binary(Key), is_binary(Val) ->
    Exp = 0,
    add(As ++ [Exp], St).

replace([_, Key, Val, Expiration] = As, St) when
    is_binary(Key), is_binary(Val), is_number(Expiration)
->
    Cmd = [{replace, 16#abba, trunc(Expiration)}, Key, Val],
    do_cmd(Cmd, As, St);
replace([_, Key, Val] = As, St) when
    is_binary(Key), is_binary(Val)
->
    Exp = 0,
    replace(As ++ [Exp], St).

do_cmd(Cmd, [BPoolId | _] = As, St) ->
    PoolId = pool_id(BPoolId, As, St),
    Res = pbtrans(PoolId, Cmd),
    case Res of
        {ok, deleted} ->
            {[true], St};
        {ok, flushed} ->
            {[true], St};
        {ok, Ret0} ->
            {Ret1, NewSt} = luerl:encode(Ret0, St),
            {[Ret1], NewSt};
        {error, _} ->
            {[false], St};
        _ ->
            {[false], St}
    end.

pbtrans(PoolName, Command) ->
    poolboy:transaction(
        PoolName,
        fun(Worker) ->
            gen_server:call(Worker, {q, Command})
        end
    ).

handle_command(Conn, Cmd) when is_list(Cmd) ->
    erlang:apply(mcd, do, [Conn | Cmd]).

ensure_pool(As, St) ->
    case As of
        [Config0 | _] ->
            case luerl:decode(Config0, St) of
                Config when is_list(Config) ->
                    Options = vmq_diversity_utils:map(Config),
                    PoolId = vmq_diversity_utils:atom(
                        maps:get(
                            <<"pool_id">>,
                            Options,
                            pool_memcached
                        )
                    ),

                    Host = vmq_diversity_utils:str(
                        maps:get(
                            <<"host">>,
                            Options,
                            "127.0.0.1"
                        )
                    ),
                    Port = vmq_diversity_utils:int(
                        maps:get(
                            <<"port">>,
                            Options,
                            11211
                        )
                    ),
                    NewOptions = [{host, Host}, {port, Port}],
                    vmq_diversity_sup:start_all_pools(
                        [{memcached, [{id, PoolId}, {opts, NewOptions}]}], []
                    ),

                    %% mcd doesn't connect immediately, so we have to
                    %% wait for it to do so.
                    case wait_for_connection(PoolId, 1000) of
                        ok ->
                            %% return to lua
                            {[true], St};
                        _ ->
                            badarg_error(connect_error, As, St)
                    end;
                _ ->
                    badarg_error(execute_parse, As, St)
            end;
        _ ->
            badarg_error(execute_parse, As, St)
    end.

wait_for_connection(PoolId, Timeout) when Timeout > 0 ->
    case pbtrans(PoolId, [get, <<"anything">>]) of
        {error, noconn} ->
            timer:sleep(100),
            wait_for_connection(PoolId, Timeout - 100);
        _ ->
            ok
    end;
wait_for_connection(_, _) ->
    error.

pool_id(BPoolId, As, St) ->
    try list_to_existing_atom(binary_to_list(BPoolId)) of
        APoolId -> APoolId
    catch
        _:_ ->
            lager:error("unknown pool ~p", [BPoolId]),
            badarg_error(unknown_pool, As, St)
    end.
