%%%-------------------------------------------------------------------
%%% @author dhruvjain
%%% @copyright (C) 2022, Gojek
%%% @doc
%%%
%%% @end
%%% Created : 21. Apr 2022 12:44 PM
%%%-------------------------------------------------------------------
-module(vmq_reg_redis_trie).

-include("vmq_server.hrl").

-behaviour(vmq_reg_view).
-behaviour(gen_server2).

%% API
-export([
    start_link/0,
    fold/4,
    add_complex_topic/2,
    delete_complex_topic/2,
    get_complex_topics/0
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

-record(state, {status = init, file, interval, timer}).
-record(trie, {edge, node_id}).
-record(trie_edge, {node_id, word}).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(setup(F), {setup, fun setup/0, fun teardown/1, F}).
-endif.

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
    MatchedTopics = [Topic | match(MP, Topic)],
    case fold_matched_topics(MP, MatchedTopics, []) of
        [] ->
            case fetchSubscribers(MatchedTopics, MP) of
                {error, _} = Err -> Err;
                SubscribersList -> fold_subscriber_info(SubscriberId, SubscribersList, FoldFun, Acc)
            end;
        LocalSharedSubsList ->
            fold_local_shared_subscriber_info(
                SubscriberId, lists:flatten(LocalSharedSubsList), FoldFun, Acc
            )
    end.

fold_matched_topics(_MP, [], Acc) ->
    Acc;
fold_matched_topics(MP, [Topic | Rest], Acc) ->
    Key = {MP, Topic},
    Res = ets:select(?SHARED_SUBS_ETS_TABLE, [{{{Key, '$1'}}, [], ['$$']}]),
    case Res of
        [] ->
            vmq_metrics:incr_cache_miss(?LOCAL_SHARED_SUBS),
            fold_matched_topics(MP, Rest, Acc);
        SharedSubsWithInfo ->
            vmq_metrics:incr_cache_hit(?LOCAL_SHARED_SUBS),
            fold_matched_topics(MP, Rest, [lists:flatten(SharedSubsWithInfo) | Acc])
    end.

fetchSubscribers(Topics, MP) ->
    UnwordedTopics = [vmq_topic:unword(T) || T <- Topics],
    case
        vmq_redis:query(
            vmq_redis_client,
            [
                ?FCALL,
                ?FETCH_MATCHED_TOPIC_SUBSCRIBERS,
                0,
                MP,
                length(UnwordedTopics)
                | UnwordedTopics
            ],
            ?FCALL,
            ?FETCH_MATCHED_TOPIC_SUBSCRIBERS
        )
    of
        {ok, SubscribersList} -> SubscribersList;
        Err -> Err
    end.

fold_subscriber_info(_, [], _, Acc) ->
    Acc;
fold_subscriber_info({MP, _} = SubscriberId, [SubscriberInfoList | Rest], FoldFun, Acc) ->
    case SubscriberInfoList of
        [NodeBinary, ClientId, QoSBinary] ->
            SubscriberInfo = {
                binary_to_atom(NodeBinary), {MP, ClientId}, binary_to_term(QoSBinary)
            },
            fold_subscriber_info(
                SubscriberId, Rest, FoldFun, FoldFun(SubscriberInfo, SubscriberId, Acc)
            );
        [NodeBinary, Group, ClientId, QoSBinary] ->
            SubscriberInfo = {
                binary_to_atom(NodeBinary), Group, {MP, ClientId}, binary_to_term(QoSBinary)
            },
            fold_subscriber_info(
                SubscriberId, Rest, FoldFun, FoldFun(SubscriberInfo, SubscriberId, Acc)
            );
        _ ->
            fold_subscriber_info(SubscriberId, Rest, FoldFun, Acc)
    end.

fold_local_shared_subscriber_info(_, [], _, Acc) ->
    Acc;
fold_local_shared_subscriber_info(
    {MP, _} = SubscriberId, [{ClientId, QoS} | SubscribersList], FoldFun, Acc
) ->
    SubscriberInfo = {node(), 'constant_group', {MP, ClientId}, QoS},
    fold_local_shared_subscriber_info(
        SubscriberId, SubscribersList, FoldFun, FoldFun(SubscriberInfo, SubscriberId, Acc)
    ).

set_complex_trie_version_metrics(File) ->
    case vmq_util:extract_version(File) of
        Version when is_list(Version) ->
            vmq_metrics:update_config_version_metric(complex_trie_version, Version);
        nomatch ->
            vmq_metrics:update_config_version_metric(complex_trie_version, "N/A");
        {error, Reason} ->
            lager:error("can't load complex topics trie acl file ~p due to ~p", [File, Reason]),
            ok
    end.

load_from_file(File) ->
    case file:read_file(File) of
        {ok, BinaryData} ->
            FileContent = binary_to_list(BinaryData),
            Entries = string:tokens(FileContent, "\n"),
            TopicList = parse_topic_list(Entries),
            load_complex_topics(TopicList);
        {error, Reason} ->
            lager:error("can't load complex topics trie acl file ~p due to ~p", [File, Reason]),
            ok
    end.

load_complex_topics(Topics) ->
    update_complex_topic_trie(Topics),
    ets:delete_all_objects(complex_topics),
    lists:foreach(
        fun(Topic) -> ets:insert(complex_topics, {topic, Topic}) end,
        Topics
    ).

update_complex_topic_trie(Topics) ->
    Set = sets:from_list(Topics),
    lists:foreach(
        fun(Topic) ->
            case ets:lookup(complex_topics, Topic) of
                [_] -> ok;
                _ -> add_complex_topic("", Topic)
            end
        end,
        Topics
    ),
    iterate(complex_topics, fun(Topic) ->
        case sets:is_element(Topic, Set) of
            true -> ok;
            false -> delete_complex_topic("", Topic)
        end
    end).

add_complex_topic(MP, Topic) ->
    MPTopic = {MP, Topic},
    case ets:lookup(vmq_redis_trie_node, MPTopic) of
        [TrieNode = #trie_node{topic = undefined}] ->
            ets:insert(vmq_redis_trie_node, TrieNode#trie_node{topic = Topic});
        [#trie_node{topic = _Topic}] ->
            ignore;
        _ ->
            %% add trie path
            _ = [trie_add_path(MP, Triple) || Triple <- vmq_topic:triples(Topic)],
            %% add last node
            ets:insert(vmq_redis_trie_node, #trie_node{node_id = MPTopic, topic = Topic})
    end.

delete_complex_topic(MP, Topic) ->
    NodeId = {MP, Topic},
    case ets:lookup(vmq_redis_trie_node, NodeId) of
        [#trie_node{topic = undefined}] ->
            ok;
        [TrieNode = #trie_node{edge_count = EdgeCount}] when EdgeCount > 0 ->
            ets:insert(vmq_redis_trie_node, TrieNode#trie_node{topic = undefined});
        [_] ->
            ets:delete(vmq_redis_trie_node, NodeId),
            trie_delete_path(MP, lists:reverse(vmq_topic:triples(Topic)));
        _ ->
            ignore
    end.

get_complex_topics() ->
    [
        vmq_topic:unword(T)
     || T <- ets:select(vmq_redis_trie_node, [
            {
                #trie_node{
                    node_id = {"", '$1'}, topic = '$1', edge_count = '_'
                },
                [{'=/=', '$1', undefined}],
                ['$1']
            }
        ])
    ].

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
    DefaultETSOpts = [
        public,
        named_table,
        {read_concurrency, true}
    ],
    _ = ets:new(?SHARED_SUBS_ETS_TABLE, DefaultETSOpts),
    _ = ets:new(vmq_redis_trie, [{keypos, 2} | DefaultETSOpts]),
    _ = ets:new(vmq_redis_trie_node, [{keypos, 2} | DefaultETSOpts]),
    _ = ets:new(complex_topics, [{keypos, 2} | DefaultETSOpts]),

    {ok, init_state(#state{})}.

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
handle_info(
    reload, #state{file = File, interval = Interval} = State
) ->
    ok = load_from_file(File),
    set_complex_trie_version_metrics(File),
    erlang:send_after(Interval, self(), reload),
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
init_state(State) ->
    {ok, File} = application:get_env(vmq_server, complex_trie_file),
    {ok, Interval} = application:get_env(vmq_server, complex_trie_reload_interval),
    {NewI, NewTRef} = vmq_util:set_interval(Interval, self()),
    ok = load_from_file(File),
    set_complex_trie_version_metrics(File),
    State#state{status = ready, interval = NewI, timer = NewTRef, file = File}.

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
    match(MP, Topic, Rest, [Name | Acc]);
match(MP, Topic, [_ | Rest], Acc) ->
    match(MP, Topic, Rest, Acc);
match(_, _, [], Acc) ->
    Acc.

trie_add_path(MP, {Node, Word, Child}) ->
    NodeId = {MP, Node},
    Edge = #trie_edge{node_id = NodeId, word = Word},
    case ets:lookup(vmq_redis_trie_node, NodeId) of
        [TrieNode = #trie_node{edge_count = Count}] ->
            case ets:lookup(vmq_redis_trie, Edge) of
                [] ->
                    ets:insert(
                        vmq_redis_trie_node,
                        TrieNode#trie_node{edge_count = Count + 1}
                    ),
                    ets:insert(vmq_redis_trie, #trie{edge = Edge, node_id = Child});
                [_] ->
                    ok
            end;
        [] ->
            ets:insert(vmq_redis_trie_node, #trie_node{node_id = NodeId, edge_count = 1}),
            ets:insert(vmq_redis_trie, #trie{edge = Edge, node_id = Child})
    end.

trie_match(MP, Words) ->
    trie_match(MP, root, Words, []).

trie_match(MP, Node, [], ResAcc) ->
    NodeId = {MP, Node},
    ets:lookup(vmq_redis_trie_node, NodeId) ++ 'trie_match_#'(NodeId, ResAcc);
trie_match(MP, Node, [W | Words], ResAcc) ->
    NodeId = {MP, Node},
    lists:foldl(
        fun(WArg, Acc) ->
            case
                ets:lookup(
                    vmq_redis_trie,
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
    case ets:lookup(vmq_redis_trie, #trie_edge{node_id = NodeId, word = <<"#">>}) of
        [#trie{node_id = ChildId}] ->
            ets:lookup(vmq_redis_trie_node, {MP, ChildId}) ++ ResAcc;
        [] ->
            ResAcc
    end.

trie_delete_path(_, []) ->
    ok;
trie_delete_path(MP, [{Node, Word, _} | RestPath]) ->
    NodeId = {MP, Node},
    Edge = #trie_edge{node_id = NodeId, word = Word},
    ets:delete(vmq_redis_trie, Edge),
    case ets:lookup(vmq_redis_trie_node, NodeId) of
        [TrieNode = #trie_node{edge_count = EdgeCount, topic = undefined}] when EdgeCount > 1 ->
            ets:insert(vmq_redis_trie_node, TrieNode#trie_node{edge_count = EdgeCount - 1});
        [#trie_node{topic = undefined}] ->
            ets:delete(vmq_redis_trie_node, NodeId),
            trie_delete_path(MP, RestPath);
        [TrieNode = #trie_node{edge_count = EdgeCount}] ->
            ets:insert(vmq_redis_trie_node, TrieNode#trie_node{edge_count = EdgeCount - 1});
        [] ->
            ignore
    end.

-spec parse_topic_list(Topics :: [string()]) -> [[binary()]].
parse_topic_list(["# " ++ _ | Topics]) ->
    parse_topic_list(Topics);
parse_topic_list(Topics) ->
    lists:foldl(
        fun(T, Acc) ->
            Topic = list_to_binary(string:strip(T)),
            case vmq_topic:validate_topic(subscribe, Topic) of
                {ok, ParsedTopic} ->
                    case vmq_topic:contains_wildcard(ParsedTopic) of
                        true ->
                            [ParsedTopic | Acc];
                        false ->
                            lager:error("couldn't parse topic ~p", [Topic]),
                            Acc
                    end;
                _ ->
                    lager:error("couldn't parse topic ~p", [Topic]),
                    Acc
            end
        end,
        [],
        Topics
    ).

iterate(T, Fun) ->
    iterate(T, Fun, ets:first(T)).
iterate(_, _, '$end_of_table') ->
    ok;
iterate(T, Fun, K) ->
    Fun(K),
    iterate(T, Fun, ets:next(T, K)).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
%%%%%%%%%%%%%
%%% Tests
%%%%%%%%%%%%%

complex_topic_test_() ->
    [
        {"Add complex topic", ?setup(fun add_complex_topic_test/1)},
        {"Add existing topic", ?setup(fun add_existing_topic_test/1)},
        {"Sub-topic whitelisting", ?setup(fun add_sub_topic_test/1)},
        {"Delete complex topics", ?setup(fun delete_complex_topic_test/1)},
        {"Delete all complex topics", ?setup(fun delete_all_complex_topic_test/1)},
        {"Delete whitelisted sub-topics", ?setup(fun delete_whitelisted_subtopic_test/1)},
        {"Delete whitelisted parent-topics", ?setup(fun delete_whitelisted_parent_topic_test/1)},
        {"Delete non whitelisted sub-topics", ?setup(fun delete_non_whitelisted_subtopic_test/1)},
        {"Delete non existing topics", ?setup(fun delete_non_existing_topic_test/1)},
        {"Show individual whitelisted topics", ?setup(fun show_complex_topics_test/1)},
        {"Match individual whitelisted topics", ?setup(fun match_complex_topics_test/1)},
        {"Match after deleting topics", ?setup(fun match_complex_topics_after_delete_test/1)},
        {"Match complex topic with hash", ?setup(fun match_complex_topic_with_hash_test/1)}
    ].

setup() ->
    TABLE_OPTS = [public, named_table, {read_concurrency, true}],
    ets:new(vmq_redis_trie_node, [{keypos, 2} | TABLE_OPTS]),
    ets:new(vmq_redis_trie, [{keypos, 2} | TABLE_OPTS]),
    vmq_reg_redis_trie.

teardown(RegView) ->
    case RegView of
        vmq_reg_redis_trie ->
            ets:delete(vmq_redis_trie),
            ets:delete(vmq_redis_trie_node);
        _ ->
            ok
    end.

add_complex_topic_test(_) ->
    Topic1 = [<<"abc">>, <<"xyz">>, <<"+">>, <<"1">>],
    Topic2 = [<<"abc">>, <<"xyz">>, <<"+">>, <<"2">>],
    Topic3 = [<<"abc">>, <<"+">>, <<"+">>],
    Topic4 = [<<"pqr">>, <<"+">>, <<"1">>],
    add_complex_topic("", Topic1),
    add_complex_topic("", Topic2),
    add_complex_topic("", Topic3),
    add_complex_topic("", Topic4),
    [
        ?_assertEqual(
            [
                {trie_node, {[], root}, 2, undefined},
                {trie_node, {[], [<<"abc">>]}, 2, undefined},
                {trie_node, {[], [<<"abc">>, <<"+">>]}, 1, undefined},
                {trie_node, {[], [<<"abc">>, <<"+">>, <<"+">>]}, 0, [<<"abc">>, <<"+">>, <<"+">>]},
                {trie_node, {[], [<<"abc">>, <<"xyz">>]}, 1, undefined},
                {trie_node, {[], [<<"abc">>, <<"xyz">>, <<"+">>]}, 2, undefined},
                {trie_node, {[], [<<"abc">>, <<"xyz">>, <<"+">>, <<"1">>]}, 0, [
                    <<"abc">>, <<"xyz">>, <<"+">>, <<"1">>
                ]},
                {trie_node, {[], [<<"abc">>, <<"xyz">>, <<"+">>, <<"2">>]}, 0, [
                    <<"abc">>, <<"xyz">>, <<"+">>, <<"2">>
                ]},
                {trie_node, {[], [<<"pqr">>]}, 1, undefined},
                {trie_node, {[], [<<"pqr">>, <<"+">>]}, 1, undefined},
                {trie_node, {[], [<<"pqr">>, <<"+">>, <<"1">>]}, 0, [<<"pqr">>, <<"+">>, <<"1">>]}
            ],
            lists:usort(ets:tab2list(vmq_redis_trie_node))
        ),
        ?_assertEqual(
            [
                {trie, {trie_edge, {[], root}, <<"abc">>}, [<<"abc">>]},
                {trie, {trie_edge, {[], root}, <<"pqr">>}, [<<"pqr">>]},
                {trie, {trie_edge, {[], [<<"abc">>]}, <<"+">>}, [<<"abc">>, <<"+">>]},
                {trie, {trie_edge, {[], [<<"abc">>]}, <<"xyz">>}, [<<"abc">>, <<"xyz">>]},
                {trie, {trie_edge, {[], [<<"abc">>, <<"+">>]}, <<"+">>}, [
                    <<"abc">>, <<"+">>, <<"+">>
                ]},
                {trie, {trie_edge, {[], [<<"abc">>, <<"xyz">>]}, <<"+">>}, [
                    <<"abc">>, <<"xyz">>, <<"+">>
                ]},
                {trie, {trie_edge, {[], [<<"abc">>, <<"xyz">>, <<"+">>]}, <<"1">>}, [
                    <<"abc">>, <<"xyz">>, <<"+">>, <<"1">>
                ]},
                {trie, {trie_edge, {[], [<<"abc">>, <<"xyz">>, <<"+">>]}, <<"2">>}, [
                    <<"abc">>, <<"xyz">>, <<"+">>, <<"2">>
                ]},
                {trie, {trie_edge, {[], [<<"pqr">>]}, <<"+">>}, [<<"pqr">>, <<"+">>]},
                {trie, {trie_edge, {[], [<<"pqr">>, <<"+">>]}, <<"1">>}, [
                    <<"pqr">>, <<"+">>, <<"1">>
                ]}
            ],
            lists:usort(ets:tab2list(vmq_redis_trie))
        )
    ].
add_existing_topic_test(_) ->
    Topic1 = [<<"abc">>, <<"xyz">>, <<"+">>, <<"1">>],
    add_complex_topic("", Topic1),
    add_complex_topic("", Topic1),
    [
        ?_assertEqual(
            [
                {trie_node, {[], root}, 1, undefined},
                {trie_node, {[], [<<"abc">>]}, 1, undefined},
                {trie_node, {[], [<<"abc">>, <<"xyz">>]}, 1, undefined},
                {trie_node, {[], [<<"abc">>, <<"xyz">>, <<"+">>]}, 1, undefined},
                {trie_node, {[], [<<"abc">>, <<"xyz">>, <<"+">>, <<"1">>]}, 0, [
                    <<"abc">>, <<"xyz">>, <<"+">>, <<"1">>
                ]}
            ],
            lists:usort(ets:tab2list(vmq_redis_trie_node))
        ),
        ?_assertEqual(
            [
                {trie, {trie_edge, {[], root}, <<"abc">>}, [<<"abc">>]},
                {trie, {trie_edge, {[], [<<"abc">>]}, <<"xyz">>}, [<<"abc">>, <<"xyz">>]},
                {trie, {trie_edge, {[], [<<"abc">>, <<"xyz">>]}, <<"+">>}, [
                    <<"abc">>, <<"xyz">>, <<"+">>
                ]},
                {trie, {trie_edge, {[], [<<"abc">>, <<"xyz">>, <<"+">>]}, <<"1">>}, [
                    <<"abc">>, <<"xyz">>, <<"+">>, <<"1">>
                ]}
            ],
            lists:usort(ets:tab2list(vmq_redis_trie))
        )
    ].
add_sub_topic_test(_) ->
    Topic1 = [<<"abc">>, <<"xyz">>, <<"+">>, <<"1">>],
    Topic2 = [<<"abc">>, <<"xyz">>, <<"+">>],
    add_complex_topic("", Topic1),
    add_complex_topic("", Topic2),
    [
        ?_assertEqual(
            [
                {trie_node, {[], root}, 1, undefined},
                {trie_node, {[], [<<"abc">>]}, 1, undefined},
                {trie_node, {[], [<<"abc">>, <<"xyz">>]}, 1, undefined},
                {trie_node, {[], [<<"abc">>, <<"xyz">>, <<"+">>]}, 1, [
                    <<"abc">>, <<"xyz">>, <<"+">>
                ]},
                {trie_node, {[], [<<"abc">>, <<"xyz">>, <<"+">>, <<"1">>]}, 0, [
                    <<"abc">>, <<"xyz">>, <<"+">>, <<"1">>
                ]}
            ],
            lists:usort(ets:tab2list(vmq_redis_trie_node))
        ),
        ?_assertEqual(
            [
                {trie, {trie_edge, {[], root}, <<"abc">>}, [<<"abc">>]},
                {trie, {trie_edge, {[], [<<"abc">>]}, <<"xyz">>}, [<<"abc">>, <<"xyz">>]},
                {trie, {trie_edge, {[], [<<"abc">>, <<"xyz">>]}, <<"+">>}, [
                    <<"abc">>, <<"xyz">>, <<"+">>
                ]},
                {trie, {trie_edge, {[], [<<"abc">>, <<"xyz">>, <<"+">>]}, <<"1">>}, [
                    <<"abc">>, <<"xyz">>, <<"+">>, <<"1">>
                ]}
            ],
            lists:usort(ets:tab2list(vmq_redis_trie))
        )
    ].
delete_complex_topic_test(_) ->
    Topic1 = [<<"abc">>, <<"xyz">>, <<"+">>, <<"1">>],
    Topic2 = [<<"abc">>, <<"xyz">>, <<"+">>, <<"2">>],
    Topic3 = [<<"abc">>, <<"pqr">>, <<"+">>],
    add_complex_topic("", Topic1),
    add_complex_topic("", Topic2),
    add_complex_topic("", Topic3),
    delete_complex_topic("", Topic2),
    delete_complex_topic("", Topic3),
    [
        ?_assertEqual(
            [
                {trie_node, {[], root}, 1, undefined},
                {trie_node, {[], [<<"abc">>]}, 1, undefined},
                {trie_node, {[], [<<"abc">>, <<"xyz">>]}, 1, undefined},
                {trie_node, {[], [<<"abc">>, <<"xyz">>, <<"+">>]}, 1, undefined},
                {trie_node, {[], [<<"abc">>, <<"xyz">>, <<"+">>, <<"1">>]}, 0, [
                    <<"abc">>, <<"xyz">>, <<"+">>, <<"1">>
                ]}
            ],
            lists:usort(ets:tab2list(vmq_redis_trie_node))
        ),
        ?_assertEqual(
            [
                {trie, {trie_edge, {[], root}, <<"abc">>}, [<<"abc">>]},
                {trie, {trie_edge, {[], [<<"abc">>]}, <<"xyz">>}, [<<"abc">>, <<"xyz">>]},
                {trie, {trie_edge, {[], [<<"abc">>, <<"xyz">>]}, <<"+">>}, [
                    <<"abc">>, <<"xyz">>, <<"+">>
                ]},
                {trie, {trie_edge, {[], [<<"abc">>, <<"xyz">>, <<"+">>]}, <<"1">>}, [
                    <<"abc">>, <<"xyz">>, <<"+">>, <<"1">>
                ]}
            ],
            lists:usort(ets:tab2list(vmq_redis_trie))
        )
    ].
delete_all_complex_topic_test(_) ->
    Topic1 = [<<"abc">>, <<"xyz">>, <<"+">>, <<"1">>],
    Topic2 = [<<"abc">>, <<"xyz">>, <<"+">>, <<"2">>],
    Topic3 = [<<"abc">>, <<"pqr">>, <<"+">>],
    Topic4 = [<<"#">>],
    add_complex_topic("", Topic1),
    add_complex_topic("", Topic3),
    add_complex_topic("", Topic2),
    add_complex_topic("", Topic4),
    delete_complex_topic("", Topic1),
    delete_complex_topic("", Topic4),
    delete_complex_topic("", Topic3),
    delete_complex_topic("", Topic2),
    [
        ?_assertEqual(
            [],
            ets:tab2list(vmq_redis_trie_node)
        ),
        ?_assertEqual(
            [],
            ets:tab2list(vmq_redis_trie)
        )
    ].
delete_whitelisted_subtopic_test(_) ->
    Topic1 = [<<"abc">>, <<"xyz">>, <<"+">>, <<"1">>, <<"+">>],
    Topic2 = [<<"abc">>, <<"xyz">>, <<"+">>, <<"1">>],
    Topic3 = [<<"abc">>, <<"+">>],
    add_complex_topic("", Topic1),
    add_complex_topic("", Topic2),
    add_complex_topic("", Topic3),
    delete_complex_topic("", Topic2),
    [
        ?_assertEqual(
            [
                {trie_node, {[], root}, 1, undefined},
                {trie_node, {[], [<<"abc">>]}, 2, undefined},
                {trie_node, {[], [<<"abc">>, <<"+">>]}, 0, [<<"abc">>, <<"+">>]},
                {trie_node, {[], [<<"abc">>, <<"xyz">>]}, 1, undefined},
                {trie_node, {[], [<<"abc">>, <<"xyz">>, <<"+">>]}, 1, undefined},
                {trie_node, {[], [<<"abc">>, <<"xyz">>, <<"+">>, <<"1">>]}, 1, undefined},
                {trie_node, {[], [<<"abc">>, <<"xyz">>, <<"+">>, <<"1">>, <<"+">>]}, 0, [
                    <<"abc">>, <<"xyz">>, <<"+">>, <<"1">>, <<"+">>
                ]}
            ],
            lists:usort(ets:tab2list(vmq_redis_trie_node))
        ),
        ?_assertEqual(
            [
                {trie, {trie_edge, {[], root}, <<"abc">>}, [<<"abc">>]},
                {trie, {trie_edge, {[], [<<"abc">>]}, <<"+">>}, [<<"abc">>, <<"+">>]},
                {trie, {trie_edge, {[], [<<"abc">>]}, <<"xyz">>}, [<<"abc">>, <<"xyz">>]},
                {trie, {trie_edge, {[], [<<"abc">>, <<"xyz">>]}, <<"+">>}, [
                    <<"abc">>, <<"xyz">>, <<"+">>
                ]},
                {trie, {trie_edge, {[], [<<"abc">>, <<"xyz">>, <<"+">>]}, <<"1">>}, [
                    <<"abc">>, <<"xyz">>, <<"+">>, <<"1">>
                ]},
                {trie, {trie_edge, {[], [<<"abc">>, <<"xyz">>, <<"+">>, <<"1">>]}, <<"+">>}, [
                    <<"abc">>, <<"xyz">>, <<"+">>, <<"1">>, <<"+">>
                ]}
            ],
            lists:usort(ets:tab2list(vmq_redis_trie))
        )
    ].
delete_whitelisted_parent_topic_test(_) ->
    Topic1 = [<<"abc">>, <<"xyz">>, <<"+">>, <<"1">>],
    Topic2 = [<<"abc">>, <<"xyz">>, <<"+">>],
    add_complex_topic("", Topic1),
    add_complex_topic("", Topic2),
    delete_complex_topic("", Topic1),
    [
        ?_assertEqual(
            [
                {trie_node, {[], root}, 1, undefined},
                {trie_node, {[], [<<"abc">>]}, 1, undefined},
                {trie_node, {[], [<<"abc">>, <<"xyz">>]}, 1, undefined},
                {trie_node, {[], [<<"abc">>, <<"xyz">>, <<"+">>]}, 0, [
                    <<"abc">>, <<"xyz">>, <<"+">>
                ]}
            ],
            lists:usort(ets:tab2list(vmq_redis_trie_node))
        ),
        ?_assertEqual(
            [
                {trie, {trie_edge, {[], root}, <<"abc">>}, [<<"abc">>]},
                {trie, {trie_edge, {[], [<<"abc">>]}, <<"xyz">>}, [<<"abc">>, <<"xyz">>]},
                {trie, {trie_edge, {[], [<<"abc">>, <<"xyz">>]}, <<"+">>}, [
                    <<"abc">>, <<"xyz">>, <<"+">>
                ]}
            ],
            lists:usort(ets:tab2list(vmq_redis_trie))
        )
    ].
delete_non_whitelisted_subtopic_test(_) ->
    Topic1 = [<<"abc">>, <<"xyz">>, <<"+">>, <<"1">>],
    Topic2 = [<<"abc">>, <<"xyz">>, <<"+">>],
    Topic3 = [<<"abc">>],
    add_complex_topic("", Topic1),
    delete_complex_topic("", Topic2),
    delete_complex_topic("", Topic3),
    [
        ?_assertEqual(
            [
                {trie_node, {[], root}, 1, undefined},
                {trie_node, {[], [<<"abc">>]}, 1, undefined},
                {trie_node, {[], [<<"abc">>, <<"xyz">>]}, 1, undefined},
                {trie_node, {[], [<<"abc">>, <<"xyz">>, <<"+">>]}, 1, undefined},
                {trie_node, {[], [<<"abc">>, <<"xyz">>, <<"+">>, <<"1">>]}, 0, [
                    <<"abc">>, <<"xyz">>, <<"+">>, <<"1">>
                ]}
            ],
            lists:usort(ets:tab2list(vmq_redis_trie_node))
        ),
        ?_assertEqual(
            [
                {trie, {trie_edge, {[], root}, <<"abc">>}, [<<"abc">>]},
                {trie, {trie_edge, {[], [<<"abc">>]}, <<"xyz">>}, [<<"abc">>, <<"xyz">>]},
                {trie, {trie_edge, {[], [<<"abc">>, <<"xyz">>]}, <<"+">>}, [
                    <<"abc">>, <<"xyz">>, <<"+">>
                ]},
                {trie, {trie_edge, {[], [<<"abc">>, <<"xyz">>, <<"+">>]}, <<"1">>}, [
                    <<"abc">>, <<"xyz">>, <<"+">>, <<"1">>
                ]}
            ],
            lists:usort(ets:tab2list(vmq_redis_trie))
        )
    ].
delete_non_existing_topic_test(_) ->
    Topic1 = [<<"abc">>, <<"xyz">>, <<"+">>, <<"1">>],
    Topic2 = [<<"pqr">>, <<"xyz">>, <<"+">>],
    Topic3 = [<<"abcd">>, <<"+">>],
    add_complex_topic("", Topic1),
    delete_complex_topic("", Topic2),
    delete_complex_topic("", Topic3),
    [
        ?_assertEqual(
            [
                {trie_node, {[], root}, 1, undefined},
                {trie_node, {[], [<<"abc">>]}, 1, undefined},
                {trie_node, {[], [<<"abc">>, <<"xyz">>]}, 1, undefined},
                {trie_node, {[], [<<"abc">>, <<"xyz">>, <<"+">>]}, 1, undefined},
                {trie_node, {[], [<<"abc">>, <<"xyz">>, <<"+">>, <<"1">>]}, 0, [
                    <<"abc">>, <<"xyz">>, <<"+">>, <<"1">>
                ]}
            ],
            lists:usort(ets:tab2list(vmq_redis_trie_node))
        ),
        ?_assertEqual(
            [
                {trie, {trie_edge, {[], root}, <<"abc">>}, [<<"abc">>]},
                {trie, {trie_edge, {[], [<<"abc">>]}, <<"xyz">>}, [<<"abc">>, <<"xyz">>]},
                {trie, {trie_edge, {[], [<<"abc">>, <<"xyz">>]}, <<"+">>}, [
                    <<"abc">>, <<"xyz">>, <<"+">>
                ]},
                {trie, {trie_edge, {[], [<<"abc">>, <<"xyz">>, <<"+">>]}, <<"1">>}, [
                    <<"abc">>, <<"xyz">>, <<"+">>, <<"1">>
                ]}
            ],
            lists:usort(ets:tab2list(vmq_redis_trie))
        )
    ].
show_complex_topics_test(_) ->
    Topic1 = [<<"abc">>, <<"xyz">>, <<"+">>, <<"1">>, <<"+">>],
    Topic2 = [<<"abc">>, <<"xyz">>, <<"+">>, <<"2">>],
    Topic3 = [<<"abc">>, <<"+">>, <<"1">>, <<"2">>],
    Topic4 = [<<"#">>],
    SubTopic1 = [<<"abc">>, <<"xyz">>, <<"+">>, <<"1">>],
    SubTopic2 = [<<"abc">>, <<"xyz">>, <<"+">>],
    add_complex_topic("", Topic1),
    add_complex_topic("", Topic2),
    add_complex_topic("", SubTopic2),
    add_complex_topic("", Topic3),
    add_complex_topic("", Topic4),
    add_complex_topic("", SubTopic1),
    delete_complex_topic("", SubTopic2),
    [
        ?_assertEqual(
            [
                [<<"#">>],
                [<<"abc/+/1/2">>],
                [<<"abc/xyz/+/1">>],
                [<<"abc/xyz/+/1/+">>],
                [<<"abc/xyz/+/2">>]
            ],
            lists:usort(
                [
                    [iolist_to_binary((Topic))]
                 || Topic <- get_complex_topics()
                ]
            )
        )
    ].
match_complex_topics_test(_) ->
    Topic1 = [<<"abc">>, <<"xyz">>, <<"+">>, <<"1">>, <<"+">>],
    Topic2 = [<<"abc">>, <<"xyz">>, <<"+">>, <<"2">>],
    Topic3 = [<<"pqr">>, <<"efg">>, <<"#">>],
    SubTopic1 = [<<"abc">>, <<"xyz">>, <<"+">>],
    add_complex_topic("", Topic1),
    add_complex_topic("", Topic2),
    add_complex_topic("", Topic3),
    add_complex_topic("", SubTopic1),
    [
        % exact topic matching
        ?_assertEqual(
            [[<<"abc">>, <<"xyz">>, <<"+">>, <<"2">>]],
            match([], [<<"abc">>, <<"xyz">>, <<"two">>, <<"2">>])
        ),
        % wildcard mathing (+)
        ?_assertEqual(
            [[<<"abc">>, <<"xyz">>, <<"+">>, <<"1">>, <<"+">>]],
            match([], [<<"abc">>, <<"xyz">>, <<"one">>, <<"1">>, <<"two">>])
        ),
        ?_assertEqual(
            [[<<"abc">>, <<"xyz">>, <<"+">>, <<"1">>, <<"+">>]],
            match([], [<<"abc">>, <<"xyz">>, <<"three">>, <<"1">>, <<"four">>])
        ),
        % wildcard mathing (#)
        ?_assertEqual(
            [[<<"pqr">>, <<"efg">>, <<"#">>]],
            match([], [<<"pqr">>, <<"efg">>, <<"1">>, <<"2">>])
        ),
        ?_assertEqual(
            [[<<"pqr">>, <<"efg">>, <<"#">>]],
            match([], [<<"pqr">>, <<"efg">>, <<"3">>, <<"4">>])
        ),
        % sub-topic matching
        ?_assertEqual(
            [[<<"abc">>, <<"xyz">>, <<"+">>]],
            match([], [<<"abc">>, <<"xyz">>, <<"one">>])
        ),
        % non whitelisted sub-topic matching
        ?_assertEqual(
            [],
            match([], [<<"abc">>, <<"xyz">>, <<"one">>, <<"1">>])
        )
    ].
match_complex_topics_after_delete_test(_) ->
    Topic1 = [<<"abc">>, <<"xyz">>, <<"+">>, <<"1">>, <<"+">>],
    Topic2 = [<<"abc">>, <<"xyz">>, <<"+">>, <<"2">>],
    Topic3 = [<<"pqr">>, <<"#">>],
    SubTopic1 = [<<"abc">>, <<"xyz">>, <<"+">>, <<"1">>],
    SubTopic2 = [<<"abc">>, <<"xyz">>, <<"+">>],
    add_complex_topic("", Topic1),
    add_complex_topic("", Topic2),
    add_complex_topic("", Topic3),
    add_complex_topic("", SubTopic1),
    add_complex_topic("", SubTopic2),
    % delete parent path of sub-topic1
    delete_complex_topic("", Topic1),
    % delete topic same prefix of sub-topic1
    delete_complex_topic("", Topic2),
    % delete sub-topic
    delete_complex_topic("", SubTopic2),
    [
        ?_assertEqual(
            [],
            match([], [<<"abc">>, <<"xyz">>, <<"one">>, <<"1">>, <<"two">>])
        ),
        ?_assertEqual(
            [],
            match([], [<<"abc">>, <<"xyz">>, <<"one">>])
        ),
        ?_assertEqual(
            [],
            match([], [<<"abc">>, <<"xyz">>, <<"two">>, <<"2">>])
        ),
        ?_assertEqual(
            [[<<"abc">>, <<"xyz">>, <<"+">>, <<"1">>]],
            match([], [<<"abc">>, <<"xyz">>, <<"one">>, <<"1">>])
        ),
        ?_assertEqual(
            [[<<"pqr">>, <<"#">>]],
            match([], [<<"pqr">>, <<"efg">>, <<"1">>, <<"2">>])
        )
    ].
match_complex_topic_with_hash_test(_) ->
    Topic1 = [<<"abc">>, <<"xyz">>, <<"+">>, <<"1">>],
    Topic2 = [<<"#">>],
    add_complex_topic("", Topic1),
    add_complex_topic("", Topic2),
    [
        ?_assertEqual(
            [[<<"#">>], [<<"abc">>, <<"xyz">>, <<"+">>, <<"1">>]],
            match([], [<<"abc">>, <<"xyz">>, <<"one">>, <<"1">>])
        )
    ].
-endif.
