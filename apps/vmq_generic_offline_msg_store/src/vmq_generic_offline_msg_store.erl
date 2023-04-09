-module(vmq_generic_offline_msg_store).

-behaviour(gen_server).

-include_lib("vmq_commons/src/vmq_types_common.hrl").

%% API
-export([
    start_link/0,
    msg_store_write/2,
    msg_store_delete/1,
    msg_store_delete/2,
    msg_store_read/2,
    msg_store_find/1
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
    engine,
    engine_module,
    query_timeout,
    options
}).

%%%===================================================================
%%% API
%%%===================================================================
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

msg_store_write(SubscriberId, Msg) ->
    safe_call({write, SubscriberId, Msg}).

msg_store_delete(SubscriberId) ->
    safe_call({delete, SubscriberId}).

msg_store_delete(SubscriberId, MsgRef) ->
    safe_call({delete, SubscriberId, MsgRef}).

msg_store_read(SubscriberId, MsgRef) ->
    safe_call({read, SubscriberId, MsgRef}).

msg_store_find(SubscriberId) ->
    safe_call({find, SubscriberId}).

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
init(_) ->
    {ok, EngineModule} = application:get_env(vmq_generic_offline_msg_store, msg_store_engine),
    {ok, Opts} = application:get_env(vmq_generic_offline_msg_store, msg_store_opts),
    Timeout = proplists:get_value(query_timeout, Opts, 2000),

    process_flag(trap_exit, true),
    case apply(EngineModule, open, [Opts]) of
        {ok, EngineState} ->
            {ok, #state{
                engine = EngineState,
                engine_module = EngineModule,
                query_timeout = Timeout,
                options = Opts
            }};
        {error, Reason} ->
            {stop, Reason}
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
handle_call(Request, _From, State) ->
    {reply, handle_req(Request, State), State}.

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
handle_cast(_Request, State) ->
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
    {'EXIT', _, _},
    #state{engine_module = EngineModule, options = Opts}
) ->
    reconnect(EngineModule, Opts);
handle_info(Info, State) ->
    lager:info("Unknown info: ~p", [Info]),
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
terminate(_Reason, #state{engine = EngineState, engine_module = EngineModule}) ->
    apply(EngineModule, close, [EngineState]),
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
reconnect(EngineModule, Opts) ->
    case apply(EngineModule, open, [Opts]) of
        {ok, EngineState} ->
            {noreply, #state{engine = EngineState}};
        {error, Reason} ->
            lager:debug("Reconnection Error: ~p", [Reason]),
            timer:sleep(2000),
            reconnect(EngineModule, Opts)
    end.

safe_call(Request) ->
    try
        gen_server:call(?MODULE, Request)
    catch
        Error:Reason -> {error, {Error, Reason}}
    end.

handle_req(
    {write, SId, Msg},
    #state{engine = Engine, engine_module = EngineModule, query_timeout = Timeout}
) ->
    apply(EngineModule, write, [
        Engine, term_to_binary(SId), Msg#vmq_msg.msg_ref, term_to_binary(Msg), Timeout
    ]);
handle_req(
    {delete, SId},
    #state{engine = Engine, engine_module = EngineModule, query_timeout = Timeout}
) ->
    apply(EngineModule, delete, [Engine, term_to_binary(SId), Timeout]);
handle_req(
    {delete, SId, MsgRef},
    #state{engine = Engine, engine_module = EngineModule, query_timeout = Timeout}
) ->
    apply(EngineModule, delete, [Engine, term_to_binary(SId), MsgRef, Timeout]);
handle_req(
    {read, SId, MsgRef},
    #state{engine = Engine, engine_module = EngineModule, query_timeout = Timeout}
) ->
    apply(EngineModule, read, [Engine, term_to_binary(SId), MsgRef, Timeout]);
handle_req(
    {find, SId},
    #state{engine = Engine, engine_module = EngineModule, query_timeout = Timeout}
) ->
    apply(EngineModule, find, [Engine, term_to_binary(SId), Timeout]).
