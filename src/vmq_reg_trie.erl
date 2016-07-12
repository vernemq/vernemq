%% Copyright 2014 Erlio GmbH Basel Switzerland (http://erl.io)
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

-module(vmq_reg_trie).

-behaviour(gen_server).
-behaviour(vmq_reg_view).

%% API
-export([start_link/0,
         fold/4,
         stats/0]).

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
    fold_(MP, FoldFun, Acc, match(MP, Topic)).

fold_(MP, FoldFun, Acc, [{Topic, Node}|MatchedTopics]) when Node == node() ->
    fold_(MP, FoldFun,
          fold__(FoldFun, Acc,
                 ets:lookup(vmq_trie_subs, {MP, Topic})),
          MatchedTopics);
fold_(MP, FoldFun, Acc, [{_Topic, Node}|MatchedTopics]) ->
    fold_(MP, FoldFun, FoldFun(Node, Acc), MatchedTopics);
fold_(_, _, Acc, []) -> Acc.

fold__(FoldFun, Acc, [{_, SubsIdQoS}|Rest]) ->
    fold__(FoldFun, FoldFun(SubsIdQoS, Acc), Rest);
fold__(_, Acc, []) -> Acc.


stats() ->
    NrOfSubs = info(vmq_trie_subs, size),
    NrOfTopics = info(vmq_trie_topic, size),
    Mem1 = info(vmq_trie_subs, memory),
    Mem2 = info(vmq_trie_topic, memory),
    Mem3 = info(vmq_trie, memory),
    Mem4 = info(vmq_trie_node, memory),
    Memory = Mem1 + Mem2 + Mem3 + Mem4,
    {NrOfSubs, NrOfTopics, Memory}.

info(T, What) ->
    case ets:info(T, What) of
        undefined -> 0;
        V -> V
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
init([]) ->
    DefaultETSOpts = [public, named_table,
                      {read_concurrency, true}],
    _ = ets:new(vmq_trie, [{keypos, 2}|DefaultETSOpts]),
    _ = ets:new(vmq_trie_node, [{keypos, 2}|DefaultETSOpts]),
    _ = ets:new(vmq_trie_topic, [{keypos, 1}|DefaultETSOpts]),
    _ = ets:new(vmq_trie_subs, [bag|DefaultETSOpts]),
    Self = self(),
    spawn_link(
      fun() ->
              ok = vmq_reg:fold_subscriptions(fun initialize_trie/2, ok),
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
    NrOfSubscribers = ets:info(vmq_trie_subs, size),
    lager:info("~p subscribers loaded into ~p", [NrOfSubscribers, ?MODULE]),
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
        {delete, SubscriberId, Subs} ->
            handle_delete_event(SubscriberId, Subs);
        {update, SubscriberId, ToRemove, ToAdd} ->
            handle_delete_event(SubscriberId, ToRemove),
            handle_add_event(SubscriberId, ToAdd);
        ignore ->
            ok
    end.

handle_delete_event({MP, _} = SubscriberId, [{Topic, QoS, Node}|Rest]) when Node == node() ->
    del_topic(MP, Topic, Node),
    del_subscriber(MP, Topic, SubscriberId, QoS),
    handle_delete_event(SubscriberId, Rest);
handle_delete_event({MP, _} = SubscriberId, [{Topic, _, Node}|Rest]) ->
    del_topic(MP, Topic, Node),
    handle_delete_event(SubscriberId, Rest);
handle_delete_event(_, []) -> ok.

handle_add_event({MP, _} = SubscriberId, [{Topic, QoS, Node}|Rest]) when Node == node() ->
    add_topic(MP, Topic, Node),
    add_subscriber(MP, Topic, SubscriberId, QoS),
    handle_add_event(SubscriberId, Rest);
handle_add_event({MP, _} = SubscriberId, [{Topic, _, Node}|Rest]) ->
    add_topic(MP, Topic, Node),
    handle_add_event(SubscriberId, Rest);
handle_add_event(_, []) -> ok.



match(MP, Topic) when is_list(MP) and is_list(Topic) ->
    TrieNodes = trie_match(MP, Topic),
    match(MP, Topic, TrieNodes, []).

%% [MQTT-4.7.2-1] The Server MUST NOT match Topic Filters starting with a
%% wildcard character (# or +) with Topic Names beginning with a $ character.
match(MP, [<<"$",_/binary>>|_] = Topic, [#trie_node{topic=[<<"#">>]}|Rest], Acc) ->
    match(MP, Topic, Rest, Acc);
match(MP, [<<"$",_/binary>>|_] = Topic, [#trie_node{topic=[<<"+">>|_]}|Rest], Acc) ->
    match(MP, Topic, Rest, Acc);

match(MP, Topic, [#trie_node{topic=Name}|Rest], Acc) when Name =/= undefined ->
    case ets:lookup(vmq_trie_topic, {MP, Name}) of
        [] ->
            match(MP, Topic, Rest, Acc);
        [{_, _, _, Nodes}] ->
            match(MP, Topic, Rest, match_(Name, Nodes, Acc))
    end;
match(MP, Topic, [_|Rest], Acc) ->
    match(MP, Topic, Rest, Acc);
match(_, _, [], Acc) -> Acc.

match_(Topic, [Node|Rest], Acc) ->
    match_(Topic, Rest, [{Topic, Node}|Acc]);
match_(_, [], Acc) -> Acc.

initialize_trie({MP, Topic, {SubscriberId, QoS, _}}, Acc) ->
    add_topic(MP, Topic, node()),
    add_subscriber(MP, Topic, SubscriberId, QoS),
    Acc;
initialize_trie({MP, Topic, Node}, Acc) when is_atom(Node) ->
    add_topic(MP, Topic, Node),
    Acc.

add_topic(MP, Topic, Node) ->
    MPTopic = {MP, Topic},
    case ets:lookup(vmq_trie_topic, MPTopic) of
        [] ->
            ets:insert(vmq_trie_topic, {MPTopic, 1, maps:put(Node, 1, maps:new()), [Node]});
        [{_, TotalCnt, NodeMap, _}] ->
            NewNodeMap =
            case maps:find(Node, NodeMap) of
                error ->
                    maps:put(Node, 1, NodeMap);
                {ok, Cnt} ->
                    maps:put(Node, Cnt + 1, NodeMap)
            end,
            ets:insert(vmq_trie_topic, {MPTopic, TotalCnt + 1, NewNodeMap,
                                        [N || {N, _} <- maps:to_list(NewNodeMap)]})
    end,

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
      end, 'trie_match_#'(NodeId, ResAcc), [W, <<"+">>]).

'trie_match_#'({MP, _} = NodeId, ResAcc) ->
    case ets:lookup(vmq_trie, #trie_edge{node_id=NodeId, word= <<"#">>}) of
        [#trie{node_id=ChildId}] ->
            ets:lookup(vmq_trie_node, {MP, ChildId}) ++ ResAcc;
        [] ->
            ResAcc
    end.

del_topic(MP, Topic, Node) ->
    MPTopic = {MP, Topic},
    case ets:lookup(vmq_trie_topic, MPTopic) of
        [{_, TotalCnt, NodeMap, _}] ->
            {NewNodeMap, NewTotalCnt} =
            case maps:find(Node, NodeMap) of
                error ->
                    {NodeMap, TotalCnt};
                {ok, 1} ->
                    {maps:remove(Node, NodeMap), TotalCnt -1};
                {ok, Cnt} ->
                    {maps:put(Node, Cnt - 1, NodeMap), TotalCnt -1}
            end,
            case NewTotalCnt > 0 of
                true ->
                    ets:insert(vmq_trie_topic, {MPTopic, NewTotalCnt, NewNodeMap,
                                                [N || {N, _} <- maps:to_list(NewNodeMap)]}),
                    ignore;
               false ->
                    ets:delete(vmq_trie_topic, MPTopic),
                    trie_delete(MP, Topic)
            end;
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
            lager:debug("NodeId ~p not found", [NodeId]),
            ignore
    end.

add_subscriber(MP, Topic, SubscriberId, QoS) ->
    ets:insert(vmq_trie_subs, {{MP, Topic}, {SubscriberId, QoS}}).

del_subscriber(MP, Topic, SubscriberId, QoS) ->
    ets:delete_object(vmq_trie_subs, {{MP, Topic}, {SubscriberId, QoS}}).
