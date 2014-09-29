-module(emqttd_cluster).

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([nodes/0,
         is_ready/0,
         if_ready/2,
         if_ready/3,
         on_node_up/1,
         on_node_down/1]).

-define(SERVER, ?MODULE).

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> 'ignore' | {'error',_} | {'ok',pid()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec nodes() -> [any()].
nodes() ->
    [Node || [{Node, true}]
             <- ets:match(emqttd_status, '$1'), Node /= ready].

-spec on_node_up(_) -> 'ok' | 'true'.
on_node_up(Node) ->
    wait_for_table(fun() ->
                           Nodes = mnesia_cluster_utils:cluster_nodes(all),
                           ets:insert(emqttd_status, {Node, true}),
                           update_ready(Nodes)
                   end).

-spec on_node_down(_) -> 'ok' | 'true'.
on_node_down(Node) ->
    wait_for_table(fun() ->
                           Nodes = mnesia_cluster_utils:cluster_nodes(all),
                           ets:delete(emqttd_status, Node),
                           update_ready(Nodes)
                   end).

-spec wait_for_table(fun(() -> 'true')) -> 'ok' | 'true'.
wait_for_table(Fun) ->
    case lists:member(emqttd_status, ets:all()) of
        true -> Fun();
        false -> timer:sleep(100)
    end.

-spec update_ready([any()]) -> 'true'.
update_ready(Nodes) ->
    SortedNodes = lists:sort(Nodes),
    IsReady = lists:sort([Node || [{Node, true}]
                                  <- ets:match(emqttd_status, '$1'),
                                  Node /= ready]) == SortedNodes,
    ets:insert(emqttd_status, {ready, IsReady}).

-spec is_ready() -> boolean().
is_ready() ->
    ets:lookup(emqttd_status, ready) == [{ready, true}].

-spec if_ready(_,_) -> any().
if_ready(Fun, Args) ->
    case is_ready() of
        true ->
            apply(Fun, Args);
        false ->
            {error, not_ready}
    end.
-spec if_ready(_,_,_) -> any().
if_ready(Mod, Fun, Args) ->
    case is_ready() of
        true ->
            apply(Mod, Fun, Args);
        false ->
            {error, not_ready}
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
-spec init([]) -> {'ok',#state{}}.
init([]) ->
    ets:new(emqttd_status, [{read_concurrency, true}, public, named_table]),
    {ok, #state{}}.

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
-spec handle_call(_,_,_) -> {'reply','ok',_}.
handle_call(_Request, _From, State) ->
    Reply = ok,
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
-spec handle_cast(_,_) -> {'noreply',_}.
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
-spec handle_info(_,_) -> {'noreply',_}.
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
-spec terminate(_,_) -> 'ok'.
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
-spec code_change(_,_,_) -> {'ok',_}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

