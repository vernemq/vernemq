%% Copyright 2018 Erlio GmbH Basel Switzerland (http://erl.io)
%% Copyright 2018-2024 Octavo Labs/VerneMQ (https://vernemq.com/)
%% and Individual Contributors.
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
-include_lib("kernel/include/logger.hrl").

-dialyzer(no_undefined_callbacks).

-behaviour(gen_server).
-behaviour(vmq_reg_view).

%% API
-export([
    start_link/0,
    fold/4,
    fold_fanout_shard/5,
    update_subscriber/3,
    update_subscriber_changes/3,
    stats/0,
    init_subscriptions/0
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
    status = init,
    event_handler,
    event_queue = queue:new()
}).

-record(trie, {edge, node_id}).
-record(trie_node, {node_id, edge_count = 0, topic}).
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

init_subscriptions() ->
    gen_server:call(?MODULE, init_subs, 60000).

-spec fold(subscriber_id(), topic(), fun(), any()) -> any().
fold({MP, _} = SubscriberId, Topic, FoldFun, Acc) when is_list(Topic) ->
    fold_(
        SubscriberId,
        FoldFun,
        Acc,
        %% local subscriptions without wildcard
        [
            {Topic, node()}
            | lists:append(
                %% local & remote subscriptions with wildcard
                match(MP, Topic),
                %% remote subscriptions without wildcards
                get_remote_subscribers(MP, Topic)
            )
        ],
        []
    ).

update_subscriber(SubscriberId, OldSubs, NewSubs) ->
    gen_server:call(?MODULE, {update_subscriber, SubscriberId, OldSubs, NewSubs}, 60000).

update_subscriber_changes(SubscriberId, ToRemove, ToAdd) ->
    gen_server:call(?MODULE, {update_subscriber_changes, SubscriberId, ToRemove, ToAdd}, 60000).

fold_({MP, _} = SubscriberId, FoldFun, Acc, [{Topic, {Node, Group}} | MatchedTopics], Remotes) ->
    fold_(
        SubscriberId,
        FoldFun,
        fold__(
            FoldFun,
            SubscriberId,
            Acc,
            lookup_subs({MP, Group, Node, Topic})
        ),
        MatchedTopics,
        Remotes
    );
fold_({MP, _} = SubscriberId, FoldFun, Acc, [{Topic, Node} | MatchedTopics], Remotes) when
    Node == node()
->
    Key = {MP, Topic},
    fold_(
        SubscriberId,
        FoldFun,
        fold_local(Key, SubscriberId, FoldFun, Acc),
        MatchedTopics,
        Remotes
    );
fold_(SubscriberId, FoldFun, Acc, [{_Topic, Node} | MatchedTopics], Remotes) ->
    case lists:member(Node, Remotes) of
        true ->
            fold_(SubscriberId, FoldFun, Acc, MatchedTopics, Remotes);
        false ->
            fold_(SubscriberId, FoldFun, FoldFun(Node, SubscriberId, Acc), MatchedTopics, [
                Node | Remotes
            ])
    end;
fold_(_, _, Acc, [], _) ->
    Acc.

lookup_subs(Key) ->
    case ets:lookup(vmq_trie_subs, Key) of
        [{_, fanout}] ->
            fanout_entries(Key);
        Res ->
            Res
    end.

fold_local(Key, SubscriberId, FoldFun, Acc) ->
    case ets:lookup(vmq_trie_subs, Key) of
        [{_, fanout}] ->
            case fanout_shard_count() of
                ShardCount when ShardCount > 1 ->
                    case ShardCount =:= vmq_fanout_shard_sup:worker_count() of
                        true ->
                            case fanout_async_handoff() of
                                true ->
                                    ok = vmq_fanout_shard_sup:async_fold(
                                        Key, SubscriberId, FoldFun, Acc, ShardCount
                                    ),
                                    Acc;
                                false ->
                                    vmq_fanout_shard_sup:fold(
                                        Key, SubscriberId, FoldFun, Acc, ShardCount
                                    )
                            end;
                        false ->
                            fold__(FoldFun, SubscriberId, Acc, fanout_entries(Key))
                    end;
                _ ->
                    fold__(FoldFun, SubscriberId, Acc, fanout_entries(Key))
            end;
        Res ->
            fold__(FoldFun, SubscriberId, Acc, Res)
    end.

fold_fanout_shard(Shard, Key, SubscriberId, FoldFun, Acc) ->
    fold__(FoldFun, SubscriberId, Acc, ets:lookup(vmq_trie_subs_fanout, {Shard, Key})).

fold__(FoldFun, SubscriberId, Acc, [{_, SubsIdQoS} | Rest]) ->
    fold__(FoldFun, SubscriberId, FoldFun(SubsIdQoS, SubscriberId, Acc), Rest);
fold__(_, _, Acc, []) ->
    Acc.

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
    {NrOfSubs + NrOfRemoteSubs, Memory * WordSize}.

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
    create_tables(),
    Self = self(),
    spawn_link(
        fun() ->
            ok = vmq_reg:fold_subscriptions(fun initialize_trie/2, ok),
            Self ! subscribers_loaded
        end
    ),
    EventHandler = vmq_reg:subscribe_subscriber_changes([{skip_local_feedback, true}]),
    {ok, #state{event_handler = EventHandler}}.

create_tables() ->
    persistent_term:put({?MODULE, fanout_shard_count}, configured_fanout_shard_count()),
    DefaultETSOpts = [
        public,
        named_table,
        {read_concurrency, true}
    ],
    _ = ets:new(vmq_trie, [{keypos, 2} | DefaultETSOpts]),
    _ = ets:new(vmq_trie_node, [{keypos, 2} | DefaultETSOpts]),
    _ = ets:new(vmq_trie_topic, [{keypos, 1} | DefaultETSOpts]),
    _ = ets:new(vmq_trie_subs, [bag | DefaultETSOpts]),
    _ = ets:new(vmq_trie_subs_fanout, [bag | DefaultETSOpts]),
    _ = ets:new(vmq_trie_remote_subs, [{keypos, 1} | DefaultETSOpts]).

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
handle_call({event, Event}, _From, #state{event_handler = Handler} = State) ->
    %% used only for testing/microbenchmarking
    handle_event(Handler, Event),
    {reply, ok, State};
handle_call(
    {update_subscriber, _, _, _} = Update, _From, #state{status = init, event_queue = Q} = State
) ->
    {reply, ok, State#state{event_queue = queue:in(Update, Q)}};
handle_call(
    {update_subscriber_changes, _, _, _} = Update,
    _From,
    #state{status = init, event_queue = Q} = State
) ->
    {reply, ok, State#state{event_queue = queue:in(Update, Q)}};
handle_call({update_subscriber, SubscriberId, OldSubs, NewSubs}, _From, State) ->
    update_subscriber_(SubscriberId, OldSubs, NewSubs),
    {reply, ok, State};
handle_call({update_subscriber_changes, SubscriberId, ToRemove, ToAdd}, _From, State) ->
    update_subscriber_changes_(SubscriberId, ToRemove, ToAdd),
    {reply, ok, State};
handle_call(init_subs, _From, State) ->
    spawn_link(
        fun() ->
            ok = vmq_reg:fold_subscriptions(fun initialize_trie/2, ok),
            self() ! subscribers_loaded
        end
    ),
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
handle_info(
    subscribers_loaded,
    #state{
        event_handler = Handler,
        event_queue = Q
    } = State
) ->
    lists:foreach(
        fun(QueuedEvent) ->
            handle_queued_event(Handler, QueuedEvent)
        end,
        queue:to_list(Q)
    ),
    NrOfSubscribers = ets:info(vmq_trie_subs, size),
    NrOfRemoteSubscribers = ets:info(vmq_trie_remote_subs, size),
    persistent_term:put({subscribe_trie_ready, ?MODULE}, 1),
    ?LOG_INFO("loaded ~p local subscriptions and ~p remote subscriptions into ~p", [
        NrOfSubscribers, NrOfRemoteSubscribers, ?MODULE
    ]),
    {noreply, State#state{status = ready, event_queue = undefined}};
handle_info(Event, #state{status = init, event_queue = Q} = State) ->
    {noreply, State#state{event_queue = queue:in(Event, Q)}};
handle_info(Event, #state{event_handler = Handler} = State) ->
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
            {ToRemove, ToAdd} = vmq_subscriber:get_changes(OldValue, NewValue),
            vmq_subscriber:fold(fun handle_delete_event/2, SubscriberId, ToRemove),
            vmq_subscriber:fold(fun handle_add_event/2, SubscriberId, ToAdd);
        ignore ->
            ok
    end.

handle_queued_event(_Handler, {update_subscriber, SubscriberId, OldSubs, NewSubs}) ->
    update_subscriber_(SubscriberId, OldSubs, NewSubs);
handle_queued_event(_Handler, {update_subscriber_changes, SubscriberId, ToRemove, ToAdd}) ->
    update_subscriber_changes_(SubscriberId, ToRemove, ToAdd);
handle_queued_event(Handler, Event) ->
    handle_event(Handler, Event).

update_subscriber_(SubscriberId, OldSubs, NewSubs) ->
    {ToRemove, ToAdd} = vmq_subscriber:get_changes(OldSubs, NewSubs),
    update_subscriber_changes_(SubscriberId, ToRemove, ToAdd).

update_subscriber_changes_(SubscriberId, ToRemove, ToAdd) ->
    vmq_subscriber:fold(fun handle_delete_event/2, SubscriberId, ToRemove),
    vmq_subscriber:fold(fun handle_add_event/2, SubscriberId, ToAdd).

handle_add_event({[<<"$share">>, Group | Topic], SubInfo, Node}, {MP, _} = SubscriberId) ->
    case add_subscriber_group(MP, Node, Group, Topic, SubscriberId, SubInfo) of
        true -> add_complex_topic(MP, Topic, {Node, Group}, true);
        false -> ok
    end,
    SubscriberId;
handle_add_event({Topic, SubInfo, Node}, {MP, _} = SubscriberId) when Node == node() ->
    case add_subscriber(MP, Topic, SubscriberId, SubInfo) of
        true -> add_complex_topic(MP, Topic, Node, vmq_topic:contains_wildcard(Topic));
        false -> ok
    end,
    SubscriberId;
handle_add_event({Topic, _, Node}, {MP, _} = SubscriberId) ->
    add_complex_topic(MP, Topic, Node, vmq_topic:contains_wildcard(Topic)),
    add_remote_subscriber(MP, Topic, Node),
    SubscriberId.

handle_delete_event({[<<"$share">>, Group | Topic], SubInfo, Node}, {MP, _} = SubscriberId) ->
    case del_subscriber_group(MP, Node, Group, Topic, SubscriberId, SubInfo) of
        true -> del_complex_topic(MP, Topic, {Node, Group}, true);
        false -> ok
    end,
    SubscriberId;
handle_delete_event({Topic, SubInfo, Node}, {MP, _} = SubscriberId) when Node == node() ->
    case del_subscriber(MP, Topic, SubscriberId, SubInfo) of
        true -> del_complex_topic(MP, Topic, Node, vmq_topic:contains_wildcard(Topic));
        false -> ok
    end,
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
match(MP, [<<"$", _/binary>> | _] = Topic, [#trie_node{topic = [<<"#">>]} | Rest], Acc) ->
    match(MP, Topic, Rest, Acc);
match(MP, [<<"$", _/binary>> | _] = Topic, [#trie_node{topic = [<<"+">> | _]} | Rest], Acc) ->
    match(MP, Topic, Rest, Acc);
match(MP, Topic, [#trie_node{topic = Name} | Rest], Acc) when Name =/= undefined ->
    case ets:lookup(vmq_trie_topic, {MP, Name}) of
        [] ->
            match(MP, Topic, Rest, Acc);
        [{_, _, Nodes}] ->
            match(MP, Topic, Rest, match_(Name, Nodes, Acc))
    end;
match(MP, Topic, [_ | Rest], Acc) ->
    match(MP, Topic, Rest, Acc);
match(_, _, [], Acc) ->
    Acc.

match_(Topic, [{NodeOrGroup, _} | Rest], Acc) ->
    match_(Topic, Rest, [{Topic, NodeOrGroup} | Acc]);
match_(_, [], Acc) ->
    Acc.

initialize_trie(
    {MP, [<<"$share">>, Group | Topic], {SubscriberId, SubInfo, Node, _CleanSession}}, Acc
) ->
    add_complex_topic(MP, Topic, {Node, Group}, true),
    add_subscriber_group(MP, Node, Group, Topic, SubscriberId, SubInfo),
    Acc;
initialize_trie({_, _, {_, _, Node, CleanSession}}, Acc) when
    Node =:= node(), CleanSession == true
->
    Acc;
initialize_trie({MP, Topic, {SubscriberId, SubInfo, Node, _CleanSession}}, Acc) when
    Node =:= node()
->
    add_complex_topic(MP, Topic, Node, vmq_topic:contains_wildcard(Topic)),
    add_subscriber(MP, Topic, SubscriberId, SubInfo),
    Acc;
initialize_trie({MP, Topic, {_SubscriberId, _SubInfo, Node, _CleanSession}}, Acc) ->
    add_complex_topic(MP, Topic, Node, vmq_topic:contains_wildcard(Topic)),
    add_remote_subscriber(MP, Topic, Node),
    Acc.

add_complex_topic(_, _, _, false) ->
    ignore;
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
        [#trie_node{topic = Topic}] ->
            ignore;
        _ ->
            %% add trie path
            _ = [trie_add_path(MP, Triple) || Triple <- vmq_topic:triples(Topic)],
            %% add last node
            ets:insert(vmq_trie_node, #trie_node{node_id = MPTopic, topic = Topic})
    end.

trie_add_path(MP, {Node, Word, Child}) ->
    NodeId = {MP, Node},
    Edge = #trie_edge{node_id = NodeId, word = Word},
    case ets:lookup(vmq_trie_node, NodeId) of
        [TrieNode = #trie_node{edge_count = Count}] ->
            case ets:lookup(vmq_trie, Edge) of
                [] ->
                    ets:insert(
                        vmq_trie_node,
                        TrieNode#trie_node{edge_count = Count + 1}
                    ),
                    ets:insert(vmq_trie, #trie{edge = Edge, node_id = Child});
                [_] ->
                    ok
            end;
        [] ->
            ets:insert(vmq_trie_node, #trie_node{node_id = NodeId, edge_count = 1}),
            ets:insert(vmq_trie, #trie{edge = Edge, node_id = Child})
    end.

trie_match(MP, Words) ->
    trie_match(MP, root, Words, []).

trie_match(MP, Node, [], ResAcc) ->
    NodeId = {MP, Node},
    ets:lookup(vmq_trie_node, NodeId) ++ 'trie_match_#'(NodeId, ResAcc);
trie_match(MP, Node, [W | Words], ResAcc) ->
    NodeId = {MP, Node},
    lists:foldl(
        fun(WArg, Acc) ->
            case
                ets:lookup(
                    vmq_trie,
                    #trie_edge{node_id = NodeId, word = WArg}
                )
            of
                [#trie{node_id = ChildId}] ->
                    trie_match(MP, ChildId, Words, Acc);
                [] ->
                    Acc
            end
        end,
        'trie_match_#'(NodeId, ResAcc),
        [W, <<"+">>]
    ).

'trie_match_#'({MP, _} = NodeId, ResAcc) ->
    case ets:lookup(vmq_trie, #trie_edge{node_id = NodeId, word = <<"#">>}) of
        [#trie{node_id = ChildId}] ->
            ets:lookup(vmq_trie_node, {MP, ChildId}) ++ ResAcc;
        [] ->
            ResAcc
    end.

del_complex_topic(_, _, _, false) ->
    ignore;
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
            [{Node, 1} | Nodes]
    end.

trie_delete(MP, Topic) ->
    NodeId = {MP, Topic},
    case ets:lookup(vmq_trie_node, NodeId) of
        [#trie_node{edge_count = 0}] ->
            ets:delete(vmq_trie_node, NodeId),
            trie_delete_path(MP, lists:reverse(vmq_topic:triples(Topic)));
        _ ->
            ignore
    end.

trie_delete_path(_, []) ->
    ok;
trie_delete_path(MP, [{Node, Word, _} | RestPath]) ->
    NodeId = {MP, Node},
    Edge = #trie_edge{node_id = NodeId, word = Word},
    ets:delete(vmq_trie, Edge),
    case ets:lookup(vmq_trie_node, NodeId) of
        [#trie_node{edge_count = 1, topic = undefined}] ->
            ets:delete(vmq_trie_node, NodeId),
            trie_delete_path(MP, RestPath);
        [#trie_node{edge_count = Count} = TrieNode] ->
            ets:insert(vmq_trie_node, TrieNode#trie_node{edge_count = Count - 1});
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
            ets:insert(vmq_trie_subs, E),
            true;
        [E] ->
            %% duplicate - do nothing;
            false;
        [{Key, fanout}] ->
            FanoutEntry = fanout_entry(Key, Val),
            case
                lists:member(
                    FanoutEntry,
                    ets:lookup(vmq_trie_subs_fanout, element(1, FanoutEntry))
                )
            of
                true ->
                    false;
                false ->
                    ets:insert(vmq_trie_subs_fanout, FanoutEntry),
                    true
            end;
        [E1] ->
            %% fanout - move to fanout table
            ets:delete(vmq_trie_subs, Key),
            ets:insert(vmq_trie_subs, {Key, fanout}),
            ets:insert(vmq_trie_subs_fanout, fanout_entry(Key, Val)),
            ets:insert(vmq_trie_subs_fanout, fanout_entry(E1)),
            true
    end.

del_subscriber_group(MP, Node, Group, Topic, SubscriberId, QoS) ->
    Key = {MP, Group, Node, Topic},
    Val = {Node, Group, SubscriberId, QoS},
    del_trie_subs(Key, Val).

del_trie_subs(Key, Val) ->
    case ets:lookup(vmq_trie_subs, Key) of
        [] ->
            %% do nothing
            false;
        [{Key, fanout}] ->
            FanoutEntry = fanout_entry(Key, Val),
            case
                lists:member(
                    FanoutEntry,
                    ets:lookup(vmq_trie_subs_fanout, element(1, FanoutEntry))
                )
            of
                false ->
                    false;
                true ->
                    ets:delete_object(vmq_trie_subs_fanout, FanoutEntry),
                    maybe_compact_fanout(Key),
                    true
            end;
        [{Key, Val}] ->
            ets:delete(vmq_trie_subs, Key),
            true;
        [{Key, _}] ->
            false
    end.

add_subscriber(MP, Topic, SubscriberId, QoS) ->
    Key = {MP, Topic},
    Val = {SubscriberId, QoS, local_queue_pid(SubscriberId)},
    insert_trie_subs(Key, Val).

local_queue_pid(SubscriberId) ->
    case vmq_queue_sup_sup:get_queue_pid(SubscriberId) of
        not_found -> undefined;
        QPid -> QPid
    end.

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
        [{_, Remotes}] -> [{Topic, Node} || {Node, _} <- Remotes]
    end.

del_subscriber(MP, Topic, SubscriberId, QoS) ->
    Key = {MP, Topic},
    del_local_trie_sub(Key, SubscriberId, QoS).

del_local_trie_sub(Key, SubscriberId, QoS) ->
    case ets:lookup(vmq_trie_subs, Key) of
        [] ->
            false;
        [{Key, fanout}] ->
            case
                [
                    E
                 || {_FanoutKey, Val} = E <- fanout_entries(Key),
                    same_local_sub(Val, SubscriberId, QoS)
                ]
            of
                [] ->
                    false;
                [FanoutEntry | _] ->
                    ets:delete_object(vmq_trie_subs_fanout, FanoutEntry),
                    maybe_compact_fanout(Key),
                    true
            end;
        [{Key, Val}] ->
            case same_local_sub(Val, SubscriberId, QoS) of
                true ->
                    ets:delete(vmq_trie_subs, Key),
                    true;
                false ->
                    false
            end;
        Entries ->
            Deleted = lists:foldl(
                fun({_, Val} = E, Acc) ->
                    case same_local_sub(Val, SubscriberId, QoS) of
                        true ->
                            ets:delete_object(vmq_trie_subs, E),
                            true;
                        false ->
                            Acc
                    end
                end,
                false,
                Entries
            ),
            case Deleted of
                true ->
                    maybe_compact_plain_fanout(Key),
                    true;
                false ->
                    false
            end
    end.

maybe_compact_plain_fanout(Key) ->
    case ets:lookup(vmq_trie_subs, Key) of
        [] ->
            true;
        [{Key, _Val}] ->
            true;
        Entries ->
            ets:delete(vmq_trie_subs, Key),
            ets:insert(vmq_trie_subs, {Key, fanout}),
            lists:foreach(fun fanout_plain_entry/1, Entries),
            true
    end.

fanout_plain_entry({Key, Val}) ->
    ets:insert(vmq_trie_subs_fanout, fanout_entry(Key, Val)).

same_local_sub({SubscriberId, SubInfo, _QPid}, SubscriberId, SubInfo) ->
    true;
same_local_sub({SubscriberId, SubInfo}, SubscriberId, SubInfo) ->
    true;
same_local_sub({SubscriberId, SubInfo, _QPid}, SubscriberId, QoS) ->
    subinfo_qos(SubInfo) =:= subinfo_qos(QoS);
same_local_sub({SubscriberId, SubInfo}, SubscriberId, QoS) ->
    subinfo_qos(SubInfo) =:= subinfo_qos(QoS);
same_local_sub(_, _, _) ->
    false.

subinfo_qos({QoS, _Opts}) when is_integer(QoS) ->
    QoS;
subinfo_qos(QoS) when is_integer(QoS) ->
    QoS;
subinfo_qos(Other) ->
    Other.

maybe_compact_fanout(Key) ->
    case fanout_entries(Key) of
        [E] ->
            ets:delete_object(vmq_trie_subs_fanout, fanout_entry(E)),
            ets:delete_object(vmq_trie_subs, {Key, fanout}),
            ets:insert(vmq_trie_subs, plain_entry(Key, E));
        [_, _ | _] ->
            true;
        [] ->
            ets:delete_object(vmq_trie_subs, {Key, fanout})
    end.

configured_fanout_shard_count() ->
    case application:get_env(vmq_server, fanout_shard_count, 1) of
        Count when is_integer(Count), Count > 0 -> Count;
        _ -> 1
    end.

fanout_shard_count() ->
    persistent_term:get({?MODULE, fanout_shard_count}, 1).

fanout_async_handoff() ->
    application:get_env(vmq_server, fanout_async_handoff, false).

fanout_shard(SubscriberId) ->
    erlang:phash2(SubscriberId, fanout_shard_count()).

fanout_key(Key, SubscriberId) ->
    {fanout_shard(SubscriberId), Key}.

fanout_entry({{Shard, Key}, Val}) when is_integer(Shard) ->
    {{Shard, Key}, Val};
fanout_entry({Key, Val}) ->
    fanout_entry(Key, Val).

fanout_entry(Key, {SubscriberId, _QoS} = Val) ->
    {fanout_key(Key, SubscriberId), Val};
fanout_entry(Key, {SubscriberId, _QoS, _QPid} = Val) ->
    {fanout_key(Key, SubscriberId), Val};
fanout_entry(Key, {_Node, _Group, SubscriberId, _QoS} = Val) ->
    {fanout_key(Key, SubscriberId), Val}.

plain_entry(Key, {_FanoutKey, Val}) ->
    {Key, Val}.

fanout_entries(Key) ->
    fanout_entries(Key, 0, fanout_shard_count(), []).

fanout_entries(_Key, Shard, ShardCount, Acc) when Shard >= ShardCount ->
    Acc;
fanout_entries(Key, Shard, ShardCount, Acc) ->
    fanout_entries(
        Key, Shard + 1, ShardCount, ets:lookup(vmq_trie_subs_fanout, {Shard, Key}) ++ Acc
    ).

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
