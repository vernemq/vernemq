-module(vmq_reg_trie).

-behaviour(gen_server).

%% API
-export([start_link/0,
         match/1,
         fold/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {event_handler, event_prefix}).

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

-spec match(list()) -> [{list(), atom()}].
match(Topic) when is_list(Topic) ->
    TrieNodes = trie_match(emqtt_topic:words(Topic)),
    lists:flatten([ets:lookup(vmq_trie_topic, Name)
                   || #trie_node{topic=Name} <- TrieNodes,
                      Name =/= undefined]).

fold(Topic, FoldFun, Acc) when is_list(Topic) ->
    fold_(FoldFun, Acc, match(Topic)).

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
    ets:new(vmq_trie, [{keypos, 2}|DefaultETSOpts]),
    ets:new(vmq_trie_node, [{keypos, 2}|DefaultETSOpts]),
    ets:new(vmq_trie_topic, [bag, {keypos, 1}|DefaultETSOpts]),
    vmq_reg:fold_subscribers(fun initialize_trie/2, []),
    {EventPrefix, EventHandler} = vmq_reg:subscribe_subscriber_changes(),
    {ok, #state{event_prefix=EventPrefix, event_handler=EventHandler}}.

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
handle_info({Prefix, Event},
            #state{event_prefix=Prefix, event_handler=Handler} = State) ->
    case Handler(Event) of
        {subscribe, Topic, {_, _, _}} ->
            %% subscribe on local node
            add_topic(Topic, node());
        {subscribe, Topic, Node} when is_atom(Node) ->
            %% subscribe on remote node
            add_topic(Topic, Node);
        {unsubscribe, Topic, {_, _}} ->
            %% unsubscribe on local node
            del_topic(Topic, node());
        {unsubscribe, Topic, Node} when is_atom(Node) ->
            %% unsubscribe on remote node
            del_topic(Topic, Node);
        ignore ->
            ok

    end,
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
initialize_trie({Topic, {_,_,_}}, Acc) ->
    add_topic(Topic, node()),
    Acc;
initialize_trie({Topic, Node}, Acc) when is_atom(Node) ->
    add_topic(Topic, Node),
    Acc.

add_topic(Topic, Node) ->
    ets:insert(vmq_trie_topic, {Topic, Node}),
    case ets:lookup(vmq_trie_node, Topic) of
        [#trie_node{topic=Topic}] ->
            ignore;
        [] ->
            %% add trie path
            [trie_add_path(Triple) || Triple <- emqtt_topic:triples(Topic)],
            %% add last node
            ets:insert(vmq_trie_node, #trie_node{node_id=Topic, topic=Topic})
    end.

trie_add_path({Node, Word, Child}) ->
    Edge = #trie_edge{node_id=Node, word=Word},
    case ets:lookup(vmq_trie_node, Node) of
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
            ets:insert(vmq_trie_node, #trie_node{node_id=Node, edge_count=1}),
            ets:insert(vmq_trie, #trie{edge=Edge, node_id=Child})
    end.

trie_match(Words) ->
    trie_match(root, Words, []).

trie_match(NodeId, [], ResAcc) ->
    ets:lookup(vmq_trie_node, NodeId) ++ 'trie_match_#'(NodeId, ResAcc);
trie_match(NodeId, [W|Words], ResAcc) ->
    lists:foldl(
      fun(WArg, Acc) ->
              case ets:lookup(vmq_trie,
                              #trie_edge{node_id=NodeId, word=WArg}) of
                  [#trie{node_id=ChildId}] ->
                      trie_match(ChildId, Words, Acc);
                  [] ->
                      Acc
              end
      end, 'trie_match_#'(NodeId, ResAcc), [W, "+"]).

'trie_match_#'(NodeId, ResAcc) ->
    case ets:lookup(vmq_trie, #trie_edge{node_id=NodeId, word="#"}) of
        [#trie{node_id=ChildId}] ->
            ets:lookup(vmq_trie_node, ChildId) ++ ResAcc;
        [] ->
            ResAcc
    end.

del_topic(Topic, Node) ->
    ets:delete_object(vmq_trie_topic, {Topic, Node}),
    case ets:lookup(vmq_trie_topic, Topic) of
        [] ->
            trie_delete(Topic);
        _ ->
            ignore
    end.

trie_delete(Topic) ->
    case ets:lookup(vmq_trie_node, Topic) of
        [#trie_node{edge_count=0}] ->
            ets:delete(vmq_trie_node, Topic),
            trie_delete_path(lists:reverse(emqtt_topic:triples(Topic)));
        _ ->
            ignore
    end.

trie_delete_path([]) ->
    ok;
trie_delete_path([{NodeId, Word, _}|RestPath]) ->
    Edge = #trie_edge{node_id=NodeId, word=Word},
    ets:delete(vmq_trie, Edge),
    case ets:lookup(vmq_trie_node, NodeId) of
        [#trie_node{edge_count=1, topic=undefined}] ->
            ets:delete(vmq_trie_node, NodeId),
            trie_delete_path(RestPath);
        [#trie_node{edge_count=Count} = TrieNode] ->
            ets:insert(vmq_trie_node, TrieNode#trie_node{edge_count=Count-1});
        [] ->
            throw({not_found, NodeId})
    end.
