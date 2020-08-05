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

-module(vmq_reg_trie).

-include("vmq_server.hrl").

-dialyzer(no_undefined_callbacks).

-behaviour(gen_server2).
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
    gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec fold(subscriber_id(), topic(), fun(), any()) -> any().
fold({MP, _} = SubscriberId, Topic, FoldFun, Acc) when is_list(Topic) ->
    fold_(SubscriberId, FoldFun, Acc,
          [{Topic, node()}  %% local subscriptions without wildcard
           |lists:append(
              match(MP, Topic), %% local & remote subscriptions with wildcard
              get_remote_subscribers(MP, Topic)) %% remote subscriptions without wildcards
          ], []).

fold_({MP, _} = SubscriberId, FoldFun, Acc, [{Topic, {Node, Group}}|MatchedTopics], Remotes) ->
    fold_(SubscriberId, FoldFun,
          fold__(FoldFun, SubscriberId, Acc,
                 lookup_subs({MP, Group, Node, Topic})),
          MatchedTopics, Remotes);
fold_({MP, _} = SubscriberId, FoldFun, Acc, [{Topic, Node}|MatchedTopics], Remotes) when Node == node() ->
    fold_(SubscriberId, FoldFun,
          fold__(FoldFun, SubscriberId, Acc,
                 lookup_subs({MP, Topic})),
          MatchedTopics, Remotes);
fold_(SubscriberId, FoldFun, Acc, [{_Topic, Node}|MatchedTopics], Remotes) ->
    case lists:member(Node, Remotes) of
        true ->
            fold_(SubscriberId, FoldFun, Acc, MatchedTopics, Remotes);
        false ->
            fold_(SubscriberId, FoldFun, FoldFun(Node, SubscriberId, Acc), MatchedTopics, [Node|Remotes])
    end;
fold_(_, _, Acc, [], _) -> Acc.

lookup_subs(Key) ->
    case ets:lookup(vmq_trie_subs, Key) of
        [{_, fanout}] ->
            MS = [{{{Key,'$1'}},[],[{{{Key},'$1'}}]}],
            ets:select(vmq_trie_subs_fanout, MS);
         Res ->
            Res
    end.

fold__(FoldFun, SubscriberId, Acc, [{_, SubsIdQoS}|Rest]) ->
    fold__(FoldFun, SubscriberId, FoldFun(SubsIdQoS, SubscriberId, Acc), Rest);
fold__(_, _, Acc, []) -> Acc.


stats() ->
    NrOfSubs = info(vmq_trie_subs, size),
    NrOfRemoteSubs = info(vmq_trie_remote_subs, size),
    Mem1 = info(vmq_trie_subs, memory),
    Mem2 = info(vmq_trie_topic, memory),
    Mem3 = info(vmq_trie, memory),
    Mem4 = info(vmq_trie_node, memory),
    Mem5 = info(vmq_trie_remote_subs, memory),
    Mem6 = info(vmq_trie_subs_fanout, memory),
    Memory = Mem1 + Mem2 + Mem3 + Mem4 + Mem5 + Mem6,
    WordSize = erlang:system_info(wordsize),
    {NrOfSubs + NrOfRemoteSubs, Memory*WordSize}.

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
    _ = ets:new(vmq_trie_subs_fanout, [ordered_set|DefaultETSOpts]),
    _ = ets:new(vmq_trie_remote_subs, [{keypos, 1}|DefaultETSOpts]),
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
handle_call({event, Event}, _From, #state{event_handler=Handler} = State) ->
    %% used only for testing/microbenchmarking
    handle_event(Handler, Event),
    {reply, ok, State};
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
    persistent_term:put(subscribe_trie_ready, 1),
    lager:info("loaded ~p subscriptions into ~p", [NrOfSubscribers, ?MODULE]),
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
        {delete, SubscriberId, Subscriptions} ->
            Removed = vmq_subscriber:get_changes(Subscriptions),
            vmq_subscriber:fold(fun handle_delete_event/2, SubscriberId, Removed);
        {update, SubscriberId, OldValue, NewValue} ->
            {ToRemove,ToAdd} = vmq_subscriber:get_changes(OldValue, NewValue),
            vmq_subscriber:fold(fun handle_delete_event/2, SubscriberId, ToRemove),
            vmq_subscriber:fold(fun handle_add_event/2, SubscriberId, ToAdd);
        ignore ->
            ok
    end.

handle_add_event({[<<"$share">>, Group|Topic], SubInfo, Node}, {MP, _} = SubscriberId) ->
    add_complex_topic(MP, Topic, {Node, Group}, true),
    add_subscriber_group(MP, Node, Group, Topic, SubscriberId, SubInfo),
    SubscriberId;
handle_add_event({Topic, SubInfo, Node}, {MP, _} = SubscriberId) when Node == node() ->
    add_complex_topic(MP, Topic, Node, vmq_topic:contains_wildcard(Topic)),
    add_subscriber(MP, Topic, SubscriberId, SubInfo),
    SubscriberId;
handle_add_event({Topic, _, Node}, {MP, _} = SubscriberId) ->
    add_complex_topic(MP, Topic, Node, vmq_topic:contains_wildcard(Topic)),
    add_remote_subscriber(MP, Topic, Node),
    SubscriberId.

handle_delete_event({[<<"$share">>, Group|Topic], SubInfo, Node}, {MP, _} = SubscriberId) ->
    del_complex_topic(MP, Topic, {Node, Group}, true),
    del_subscriber_group(MP, Node, Group, Topic, SubscriberId, SubInfo),
    SubscriberId;
handle_delete_event({Topic, SubInfo, Node}, {MP, _} = SubscriberId) when Node == node() ->
    del_complex_topic(MP, Topic, Node, vmq_topic:contains_wildcard(Topic)),
    del_subscriber(MP, Topic, SubscriberId, SubInfo),
    SubscriberId;
handle_delete_event({Topic, _, Node}, {MP, _} = SubscriberId) ->
    del_complex_topic(MP, Topic, Node, vmq_topic:contains_wildcard(Topic)),
    del_remote_subscriber(MP, Topic, Node),
    SubscriberId.

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
        [{_, _, Nodes}] ->
            match(MP, Topic, Rest, match_(Name, Nodes, Acc))
    end;
match(MP, Topic, [_|Rest], Acc) ->
    match(MP, Topic, Rest, Acc);
match(_, _, [], Acc) -> Acc.

match_(Topic, [{NodeOrGroup,_}|Rest], Acc) ->
    match_(Topic, Rest, [{Topic, NodeOrGroup}|Acc]);
match_(_, [], Acc) -> Acc.

initialize_trie({MP, [<<"$share">>,Group|Topic], {SubscriberId, SubInfo, Node}}, Acc) ->
    add_complex_topic(MP, Topic, {Node, Group}, true),
    add_subscriber_group(MP, Node, Group, Topic, SubscriberId, SubInfo),
    Acc;
initialize_trie({MP, Topic, {SubscriberId, SubInfo, Node}}, Acc) when Node =:= node() ->
    add_complex_topic(MP, Topic, Node, vmq_topic:contains_wildcard(Topic)),
    add_subscriber(MP, Topic, SubscriberId, SubInfo),
    Acc;
initialize_trie({MP, Topic, {_SubscriberId, _SubInfo, Node}}, Acc)  ->
    add_complex_topic(MP, Topic, Node, vmq_topic:contains_wildcard(Topic)),
    add_remote_subscriber(MP, Topic, Node),
    Acc.

add_complex_topic(_, _, _, false) -> ignore;
add_complex_topic(MP, Topic, Node, true) ->
    MPTopic = {MP, Topic},
    case ets:lookup(vmq_trie_topic, MPTopic) of
        [] ->
            ets:insert(vmq_trie_topic, {MPTopic, 1, [{Node, 1}]});
        [{_, TotalCnt, Nodes}] ->
            NewNodes = add_and_inc(Node, Nodes),
            ets:insert(vmq_trie_topic, {MPTopic, TotalCnt + 1, NewNodes})
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

del_complex_topic(_, _, _, false) -> ignore;
del_complex_topic(MP, Topic, NodeOrGroup, true) ->
    MPTopic = {MP, Topic},
    case ets:lookup(vmq_trie_topic, MPTopic) of
        [{_, TotalCnt, Nodes}] when TotalCnt > 1 ->
            NewNodes = rem_and_dec(NodeOrGroup, Nodes),
            ets:insert(vmq_trie_topic, {MPTopic, TotalCnt - 1, NewNodes});
        [{_, 1, _}] ->
            ets:delete(vmq_trie_topic, MPTopic),
            trie_delete(MP, Topic);
        _ ->
            ignore
    end.

rem_and_dec(Node, Nodes) ->
    case lists:keysearch(Node, 1, Nodes) of
        {value, {_, 1}} ->
            lists:keydelete(Node, 1, Nodes);
        {value, {N, C}} ->
            lists:keyreplace(Node, 1, Nodes, {N, C - 1});
        false ->
            Nodes
    end.

add_and_inc(Node, Nodes) ->
    case lists:keysearch(Node, 1, Nodes) of
        {value, {N, C}} ->
            lists:keyreplace(Node, 1, Nodes, {N, C + 1});
        false ->
            [{Node, 1}|Nodes]
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
            ignore
    end.

add_subscriber_group(MP, Node, Group, Topic, SubscriberId, QoS) ->
    Key = {MP, Group, Node, Topic},
    Val = {Node, Group, SubscriberId, QoS},
    insert_trie_subs(Key, Val).

insert_trie_subs(Key, Val) ->
    E = {Key, Val},
    case ets:lookup(vmq_trie_subs, Key) of
        [] ->
            ets:insert(vmq_trie_subs, E);
        [E] ->
            %% duplicate - do nothing;
            true;
        [{Key, fanout}] ->
            ets:insert(vmq_trie_subs_fanout, {E});
        [E1] ->
            %% fanout - move to fanout table
            ets:delete(vmq_trie_subs, Key),
            ets:insert(vmq_trie_subs, {Key, fanout}),
            ets:insert(vmq_trie_subs_fanout, {E}),
            ets:insert(vmq_trie_subs_fanout, {E1})
    end.


del_subscriber_group(MP, Node, Group, Topic, SubscriberId, QoS) ->
    Key = {MP, Group, Node, Topic},
    Val = {Node, Group, SubscriberId, QoS},
    del_trie_subs(Key, Val).

del_trie_subs(Key, Val) ->
    case ets:lookup(vmq_trie_subs, Key) of
        [] ->
             %% do nothing
            true;
        [{Key, fanout}] ->
            %% we optimistically delete the entry from the fanout table
            ets:delete(vmq_trie_subs_fanout, {Key,Val}),

            %% select to retrieve max 2 results to determine if we
            %% need to move back to the normal table.
            MS = [{{{Key,'$1'}},[],[{{{Key},'$1'}}]}],
            case ets:select(vmq_trie_subs_fanout, MS, 2) of
                {[E], _Continuation} ->
                    %% last element in the fanout, move to normal table
                    ets:delete(vmq_trie_subs_fanout, E),
                    ets:delete_object(vmq_trie_subs, {Key,fanout}),
                    ets:insert(vmq_trie_subs, E);
                {[_,_], _Continuation} ->
                    %% not last element, do nothing
                    true
            end;
        [{Key,_}] ->
            ets:delete(vmq_trie_subs, Key)
    end.

add_subscriber(MP, Topic, SubscriberId, QoS) ->
    Key = {MP, Topic},
    Val = {SubscriberId, QoS},
    insert_trie_subs(Key, Val).

add_remote_subscriber(MP, Topic, Node) ->
    Key = {MP, Topic},
    NewRemotes =
    case ets:lookup(vmq_trie_remote_subs, Key) of
        [] ->
            [{Node, 1}];
        [{_, Remotes}] ->
            add_and_inc(Node, Remotes)
    end,
    ets:insert(vmq_trie_remote_subs, {Key, NewRemotes}).

get_remote_subscribers(MP, Topic) ->
    Key = {MP, Topic},
    case ets:lookup(vmq_trie_remote_subs, Key) of
        [] -> [];
        [{_, Remotes}] ->
            [{Topic, Node} || {Node, _} <- Remotes]
    end.

del_subscriber(MP, Topic, SubscriberId, QoS) ->
    Key = {MP, Topic},
    Val = {SubscriberId, QoS},
    del_trie_subs(Key, Val).

del_remote_subscriber(MP, Topic, Node) ->
    Key = {MP, Topic},
    case ets:lookup(vmq_trie_remote_subs, Key) of
        [] ->
            ignore;
        [{_, Remotes}] ->
            case rem_and_dec(Node, Remotes) of
                [] ->
                    ets:delete(vmq_trie_remote_subs, Key);
                NewRemotes ->
                    ets:insert(vmq_trie_remote_subs, {Key, NewRemotes})
            end
    end.
