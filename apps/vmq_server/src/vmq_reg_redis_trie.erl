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

-dialyzer([no_undefined_callbacks]).

-behaviour(vmq_reg_view).
-behaviour(gen_server2).

%% API
-export([
    start_link/0,
    fold/4,
    add_complex_topics/1,
    add_complex_topic/2,
    delete_complex_topics/1,
    delete_complex_topic/2,
    get_complex_topics/0,
    safe_rpc/4
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

-record(state, {status = init}).
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
    gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec fold(subscriber_id(), topic(), fun(), any()) -> any().
fold({MP, _} = SubscriberId, Topic, FoldFun, Acc) when is_list(Topic) ->
    MatchedTopics = [Topic | match(MP, Topic)],
    case fold_matched_topics(MP, MatchedTopics, []) of
        [] ->
            SubscribersList = fetchSubscribers(MatchedTopics, MP),
            fold_subscriber_info(SubscriberId, SubscribersList, FoldFun, Acc);
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
    {ok, SubscribersList} = vmq_redis:query(
        redis_client,
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
    ),
    SubscribersList.

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

add_complex_topics(Topics) ->
    Nodes = vmq_cluster:nodes(),
    Query = lists:foldl(
        fun(T, Acc) ->
            lists:foreach(
                fun(Node) -> safe_rpc(Node, ?MODULE, add_complex_topic, ["", T]) end, Nodes
            ),
            [[?SADD, "wildcard_topics", term_to_binary(T)] | Acc]
        end,
        [],
        Topics
    ),
    vmq_redis:pipelined_query(redis_client, Query, ?ADD_COMPLEX_TOPICS_OPERATION),
    ok.

add_complex_topic(MP, Topic) ->
    MPTopic = {MP, Topic},
    case ets:lookup(vmq_redis_trie_node, MPTopic) of
        [#trie_node{topic = Topic}] ->
            ignore;
        _ ->
            %% add trie path
            _ = [trie_add_path(MP, Triple) || Triple <- vmq_topic:triples(Topic)],
            %% add last node
            ets:insert(vmq_redis_trie_node, #trie_node{node_id = MPTopic, topic = Topic})
    end.

delete_complex_topics(Topics) ->
    Nodes = vmq_cluster:nodes(),
    Query = lists:foldl(
        fun(T, Acc) ->
            lists:foreach(
                fun(Node) -> safe_rpc(Node, ?MODULE, delete_complex_topic, ["", T]) end, Nodes
            ),
            Acc ++ [[?SREM, "wildcard_topics", term_to_binary(T)]]
        end,
        [],
        Topics
    ),
    vmq_redis:pipelined_query(redis_client, Query, ?DELETE_COMPLEX_TOPICS_OPERATION),
    ok.

delete_complex_topic(MP, Topic) ->
    NodeId = {MP, Topic},
    case ets:lookup(vmq_redis_trie_node, NodeId) of
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
                #trie_node{node_id = {"", '$1'}, topic = '$1', edge_count = 0},
                [{'=/=', '$1', undefined}],
                ['$1']
            }
        ])
    ].

-spec safe_rpc(Node :: node(), Mod :: module(), Fun :: atom(), [any()]) -> any().
safe_rpc(Node, Module, Fun, Args) ->
    try rpc:call(Node, Module, Fun, Args) of
        Result ->
            Result
    catch
        exit:{noproc, _NoProcDetails} ->
            {badrpc, rpc_process_down};
        Type:Reason ->
            {Type, Reason}
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
    DefaultETSOpts = [
        public,
        named_table,
        {read_concurrency, true}
    ],
    _ = ets:new(?SHARED_SUBS_ETS_TABLE, DefaultETSOpts),
    _ = ets:new(vmq_redis_trie, [{keypos, 2} | DefaultETSOpts]),
    _ = ets:new(vmq_redis_trie_node, [{keypos, 2} | DefaultETSOpts]),
    _ = ets:new(vmq_redis_lua_scripts, DefaultETSOpts),
    SentinelEndpoints = vmq_schema_util:parse_list(
        application:get_env(vmq_server, redis_sentinel_endpoints, "[{\"127.0.0.1\", 26379}]")
    ),
    RedisDB = application:get_env(vmq_server, redis_database, 0),
    {ok, _Pid} = eredis:start_link([
        {sentinel, [{endpoints, SentinelEndpoints}]},
        {database, RedisDB},
        {name, {local, redis_client}}
    ]),
    load_redis_functions(),
    initialize_trie(),
    {ok, #state{status = ready}}.

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
handle_info(_, State) ->
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
    ok = eredis:stop(whereis(redis_client)).

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

load_redis_functions() ->
    LuaDir = application:get_env(vmq_server, redis_lua_dir, "./etc/lua"),
    {ok, RemapSubscriberScript} = file:read_file(LuaDir ++ "/remap_subscriber.lua"),
    {ok, SubscribeScript} = file:read_file(LuaDir ++ "/subscribe.lua"),
    {ok, UnsubscribeScript} = file:read_file(LuaDir ++ "/unsubscribe.lua"),
    {ok, DeleteSubscriberScript} = file:read_file(LuaDir ++ "/delete_subscriber.lua"),
    {ok, FetchMatchedTopicSubscribersScript} = file:read_file(
        LuaDir ++ "/fetch_matched_topic_subscribers.lua"
    ),
    {ok, FetchSubscriberScript} = file:read_file(LuaDir ++ "/fetch_subscriber.lua"),

    {ok, <<"remap_subscriber">>} = vmq_redis:query(
        redis_client,
        [?FUNCTION, "LOAD", "REPLACE", RemapSubscriberScript],
        ?FUNCTION_LOAD,
        ?REMAP_SUBSCRIBER
    ),
    {ok, <<"subscribe">>} = vmq_redis:query(
        redis_client, [?FUNCTION, "LOAD", "REPLACE", SubscribeScript], ?FUNCTION_LOAD, ?SUBSCRIBE
    ),
    {ok, <<"unsubscribe">>} = vmq_redis:query(
        redis_client,
        [?FUNCTION, "LOAD", "REPLACE", UnsubscribeScript],
        ?FUNCTION_LOAD,
        ?UNSUBSCRIBE
    ),
    {ok, <<"delete_subscriber">>} = vmq_redis:query(
        redis_client,
        [?FUNCTION, "LOAD", "REPLACE", DeleteSubscriberScript],
        ?FUNCTION_LOAD,
        ?DELETE_SUBSCRIBER
    ),
    {ok, <<"fetch_matched_topic_subscribers">>} = vmq_redis:query(
        redis_client,
        [?FUNCTION, "LOAD", "REPLACE", FetchMatchedTopicSubscribersScript],
        ?FUNCTION_LOAD,
        ?FETCH_MATCHED_TOPIC_SUBSCRIBERS
    ),
    {ok, <<"fetch_subscriber">>} = vmq_redis:query(
        redis_client,
        [?FUNCTION, "LOAD", "REPLACE", FetchSubscriberScript],
        ?FUNCTION_LOAD,
        ?FETCH_SUBSCRIBER
    ).

initialize_trie() ->
    {ok, TopicList} = vmq_redis:query(
        redis_client, [?SMEMBERS, "wildcard_topics"], ?SMEMBERS, ?INITIALIZE_TRIE_OPERATION
    ),
    lists:foreach(
        fun(T) ->
            Topic = binary_to_term(T),
            add_complex_topic("", Topic)
        end,
        TopicList
    ),
    ok.

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
        [_] ->
            ets:delete(vmq_redis_trie_node, NodeId),
            trie_delete_path(MP, RestPath);
        [] ->
            ignore
    end.
