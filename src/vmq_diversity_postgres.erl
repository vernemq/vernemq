-module(vmq_diversity_postgres).

-behaviour(gen_server).
-behaviour(poolboy_worker).

%% API functions
-export([start_link/1,
        install/1,
        squery/2,
        equery/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-import(luerl_lib, [badarg_error/3]).

-record(state, {conn}).

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
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

install(St) ->
    luerl_emul:alloc_table(table(), St).

squery(PoolName, Sql) ->
    poolboy:transaction(PoolName, fun(Worker) ->
                                          gen_server:call(Worker, {squery, Sql})
                                  end).

equery(PoolName, Stmt, Params) ->
    poolboy:transaction(PoolName, fun(Worker) ->
                                          gen_server:call(Worker, {equery, Stmt, Params})
                                  end).


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
    process_flag(trap_exit, true),
    Hostname = proplists:get_value(hostname, Args, "localhost"),
    Database = proplists:get_value(database, Args),
    Username = proplists:get_value(user, Args),
    Password = proplists:get_value(password, Args),
    {ok, Conn} = epgsql:connect(Hostname, Username, Password, [
        {database, Database}
    ]),
    {ok, #state{conn=Conn}}.

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
handle_call({squery, Sql}, _From, #state{conn=Conn}=State) ->
    {reply, epgsql:squery(Conn, Sql), State};
handle_call({equery, Stmt, Params}, _From, #state{conn=Conn}=State) ->
    {reply, epgsql:equery(Conn, Stmt, Params), State};
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
terminate(_Reason, State) ->
    ok = epgsql:close(State#state.conn),
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
     {<<"execute">>, {function, fun execute/2}}
    ].

execute(As, St) ->
    case As of
        [BPoolId, BQuery|Args] when is_binary(BPoolId)
                                    and is_binary(BQuery) ->
            PoolId =
            try list_to_existing_atom(binary_to_list(BPoolId)) of
                APoolId -> APoolId
            catch
                _:_ ->
                    lager:error("unknown pool ~p", [BPoolId]),
                    badarg_error(unknown_pool, As, St)
            end,

            try equery(PoolId, BQuery, Args) of
                {ok, Columns, Rows} ->
                    %% SELECT
                    {Table, NewSt} = luerl:encode(build_result(Rows, Columns), St),
                    {[Table], NewSt};
                {ok, 0} ->
                    %% UPDATE failed
                    {[false], St};
                {ok, _} ->
                    %% UPDATE success
                    {[true], St};
                {ok, 0, _Columns, _Rows} ->
                    %% INSERT failed
                    {[false], St};
                {ok, _, _Columns, _Rows} ->
                    %% INSERT success
                    {[true], St};
                {error, _Error} ->
                    {[false], St}
            catch
                E:R ->
                    lager:error("can't execute query ~p due to ~p", [BQuery, E, R]),
                    badarg_error(execute_equery, As, St)
            end;
        _ ->
            badarg_error(execute_parse, As, St)
    end.

build_result(Results, Columns) ->
    build_result(Results, [Name || {column, Name, _, _, _, _} <- Columns], []).

build_result([Result|Results], Names, Acc) ->
    build_result(Results, Names, [lists:zip(Names, tuple_to_list(Result))|Acc]);
build_result([], _, Acc) -> lists:reverse(Acc).
