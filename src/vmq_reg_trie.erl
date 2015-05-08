-module(vmq_reg_trie).

-behaviour(gen_server).

%% API
-export([start_link/0,
         fold/4]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {status=init,
                event_handler,
                event_queue=queue:new()}).

-record(trie, {edge, node_id}).
-record(trie_node, {node_id, edge_count=0, topic}).
-record(trie_edge, {node_id, word}).

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
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

fold(MP, Topic, FoldFun, Acc) when is_list(Topic) ->
    fold_(FoldFun, Acc, match(MP, Topic)).

fold_(FoldFun, Acc, [Topic|MatchedTopics]) ->
    fold_(FoldFun, FoldFun(Topic, Acc), MatchedTopics);
fold_(_, Acc, []) -> Acc.


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
init([]) ->
    DefaultETSOpts = [public, named_table,
                      {read_concurrency, true}],
    _ = ets:new(vmq_trie, [{keypos, 2}|DefaultETSOpts]),
    _ = ets:new(vmq_trie_node, [{keypos, 2}|DefaultETSOpts]),
    _ = ets:new(vmq_trie_topic, [bag, {keypos, 1}|DefaultETSOpts]),
    Self = self(),
    spawn_link(
      fun() ->
              ok = vmq_reg:fold_subscribers(fun initialize_trie/2, ok),
              Self ! subscribers_loaded
      end),
    EventHandler = vmq_reg:subscribe_subscriber_changes(),
    {ok, #state{event_handler=EventHandler}}.

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
handle_info(subscribers_loaded, #state{event_handler=Handler,
                                       event_queue=Q} = State) ->
    lists:foreach(fun(Event) ->
                          handle_event(Handler, Event)
                  end, queue:to_list(Q)),
    lager:info("all subscribers loaded into ~p", [?MODULE]),
    {noreply, State#state{status=ready, event_queue=undefined}};
handle_info(Event, #state{status=init, event_queue=Q} = State) ->
    {noreply, State#state{event_queue=queue:in(Event, Q)}};
handle_info(Event, #state{event_handler=Handler} = State) ->
    handle_event(Handler, Event),
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
handle_event(Handler, Event) ->
    case Handler(Event) of
        {subscribe, MP, Topic, {_, _, _}} ->
            %% subscribe on local node
            add_topic(MP, Topic, node());
        {subscribe, MP, Topic, Node} when is_atom(Node) ->
            %% subscribe on remote node
            add_topic(MP, Topic, Node);
        {unsubscribe, MP, Topic, {_, _}} ->
            %% unsubscribe on local node
            del_topic(MP, Topic, node());
        {unsubscribe, MP, Topic, Node} when is_atom(Node) ->
            %% unsubscribe on remote node
            del_topic(MP, Topic, Node);
        ignore ->
            ok
    end.

match(MP, Topic) when is_list(MP) and is_list(Topic) ->
    TrieNodes = trie_match(MP, Topic),
    match(MP, TrieNodes, []).

match(MP, [#trie_node{topic=Name}|Rest], Acc) when Name =/= undefined ->
    match(MP, Rest, match_(Name, ets:lookup(vmq_trie_topic, {MP, Name}), Acc));
match(MP, [_|Rest], Acc) ->
    match(MP, Rest, Acc);
match(_, [], Acc) -> Acc.

match_(Topic, [{_, Node}|Rest], Acc) ->
    match_(Topic, Rest, [{Topic, Node}|Acc]);
match_(_, [], Acc) -> Acc.

initialize_trie({MP, Topic, {_,_,_}}, Acc) ->
    add_topic(MP, Topic, node()),
    Acc;
initialize_trie({MP, Topic, Node}, Acc) when is_atom(Node) ->
    add_topic(MP, Topic, Node),
    Acc.

add_topic(MP, Topic, Node) ->
    MPTopic = {MP, Topic},
    ets:insert(vmq_trie_topic, {MPTopic, Node}),
    case ets:lookup(vmq_trie_node, MPTopic) of
        [#trie_node{topic=Topic}] ->
            ignore;
        _ ->
            %% add trie path
            _ = [trie_add_path(MP, Triple) || Triple <- vmq_topic:triples(Topic)],
            %% add last node
            ets:insert(vmq_trie_node, #trie_node{node_id=MPTopic, topic=Topic})
    end.

trie_add_path(MP, {Node, Word, Child}) ->
    NodeId = {MP, Node},
    Edge = #trie_edge{node_id=NodeId, word=Word},
    case ets:lookup(vmq_trie_node, NodeId) of
        [TrieNode = #trie_node{edge_count=Count}] ->
            case ets:lookup(vmq_trie, Edge) of
                [] ->
                    ets:insert(vmq_trie_node,
                               TrieNode#trie_node{edge_count=Count + 1}),
                    ets:insert(vmq_trie, #trie{edge=Edge, node_id=Child});
                [_] ->
                    ok
            end;
        [] ->
            ets:insert(vmq_trie_node, #trie_node{node_id=NodeId, edge_count=1}),
            ets:insert(vmq_trie, #trie{edge=Edge, node_id=Child})
    end.

trie_match(MP, Words) ->
    trie_match(MP, root, Words, []).

trie_match(MP, Node, [], ResAcc) ->
    NodeId = {MP, Node},
    ets:lookup(vmq_trie_node, NodeId) ++ 'trie_match_#'(NodeId, ResAcc);
trie_match(MP, Node, [W|Words], ResAcc) ->
    NodeId = {MP, Node},
    lists:foldl(
      fun(WArg, Acc) ->
              case ets:lookup(vmq_trie,
                              #trie_edge{node_id=NodeId, word=WArg}) of
                  [#trie{node_id=ChildId}] ->
                      trie_match(MP, ChildId, Words, Acc);
                  [] ->
                      Acc
              end
      end, 'trie_match_#'(NodeId, ResAcc), [W, "+"]).

'trie_match_#'({MP, _} = NodeId, ResAcc) ->
    case ets:lookup(vmq_trie, #trie_edge{node_id=NodeId, word="#"}) of
        [#trie{node_id=ChildId}] ->
            ets:lookup(vmq_trie_node, {MP, ChildId}) ++ ResAcc;
        [] ->
            ResAcc
    end.

del_topic(MP, Topic, Node) ->
    MPTopic = {MP, Topic},
    ets:delete_object(vmq_trie_topic, {MPTopic, Node}),
    case ets:lookup(vmq_trie_topic, MPTopic) of
        [] ->
            trie_delete(MP, Topic);
        _ ->
            ignore
    end.

trie_delete(MP, Topic) ->
    NodeId = {MP, Topic},
    case ets:lookup(vmq_trie_node, NodeId) of
        [#trie_node{edge_count=0}] ->
            ets:delete(vmq_trie_node, NodeId),
            trie_delete_path(MP, lists:reverse(vmq_topic:triples(Topic)));
        _ ->
            ignore
    end.

trie_delete_path(_, []) ->
    ok;
trie_delete_path(MP, [{Node, Word, _}|RestPath]) ->
    NodeId = {MP, Node},
    Edge = #trie_edge{node_id=NodeId, word=Word},
    ets:delete(vmq_trie, Edge),
    case ets:lookup(vmq_trie_node, NodeId) of
        [#trie_node{edge_count=1, topic=undefined}] ->
            ets:delete(vmq_trie_node, NodeId),
            trie_delete_path(MP, RestPath);
        [#trie_node{edge_count=Count} = TrieNode] ->
            ets:insert(vmq_trie_node, TrieNode#trie_node{edge_count=Count-1});
        [] ->
            throw({not_found, NodeId})
    end.
