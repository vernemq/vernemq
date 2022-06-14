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
-export([start_link/0,
    fold/4,
    put/3,
    get/1,
    delete/1,
    add_complex_topics/1,
    add_complex_topic/2,
    delete_complex_topics/1,
    delete_complex_topic/2,
    get_complex_topics/0,
    safe_rpc/4]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(state, {status=init}).
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
    MatchedTopics = [Topic | match(MP, Topic)],
    SubscribersList = fetchSubscribers(MatchedTopics, MP),
    fold_(SubscriberId, SubscribersList, FoldFun, Acc).

put(SubscriberId, fetch_old_subs, Subs) ->
    OldSubs = vmq_reg_redis_trie:get(SubscriberId),
    lists:foreach(fun({OldNode, _, _}) -> rpc:cast(OldNode, vmq_reg_mgr, handle_new_sub_event, [SubscriberId, Subs]) end, OldSubs),
    SetQuery = [["SET", term_to_binary(SubscriberId), term_to_binary(Subs)], ["EXEC"]],
    FinalQuery = case vmq_subscriber:get_changes(OldSubs, Subs) of
                     {[{OldNode, NSubs}], [{NewNode, NSubs}]} ->
                         update_topics(NSubs, OldNode, NewNode, SubscriberId, SetQuery);
                     {ToRemove,ToAdd} ->
                         {_, Query1} = vmq_subscriber:fold(fun handle_delete_event/2, {SubscriberId, SetQuery}, ToRemove),
                         {_, Query2} = vmq_subscriber:fold(fun handle_add_event/2, {SubscriberId, Query1}, ToAdd),
                         Query2
                 end,
    [{ok, _} | _] = pipelined_query([["MULTI"] | FinalQuery]),
    ok;
put(SubscriberId, OldSubs, Subs) ->
    lists:foreach(fun({OldNode, _, _}) -> rpc:cast(OldNode, vmq_reg_mgr, handle_new_sub_event, [SubscriberId, Subs]) end, OldSubs),
    SetQuery = [["SET", term_to_binary(SubscriberId), term_to_binary(Subs)], ["EXEC"]],
    FinalQuery = case vmq_subscriber:get_changes(OldSubs, Subs) of
                     {[{OldNode, NSubs}], [{NewNode, NSubs}]} ->
                         update_topics(NSubs, OldNode, NewNode, SubscriberId, SetQuery);
                     {ToRemove,ToAdd} ->
                         {_, Query1} = vmq_subscriber:fold(fun handle_delete_event/2, {SubscriberId, SetQuery}, ToRemove),
                         {_, Query2} = vmq_subscriber:fold(fun handle_add_event/2, {SubscriberId, Query1}, ToAdd),
                         Query2
                 end,
    [{ok, _} | _] = pipelined_query([["MULTI"] | FinalQuery]),
    ok.

get(SubscriberId) ->
    case query(["GET", term_to_binary(SubscriberId)]) of
        {ok, undefined} -> [];
        {ok, Result} -> binary_to_term(Result)
    end.

delete(SubscriberId) ->
    Subscriptions = vmq_reg_redis_trie:get(SubscriberId),
    Removed = vmq_subscriber:get_changes(Subscriptions),
    {_, Query} = vmq_subscriber:fold(fun handle_delete_event/2, {SubscriberId, [["EXEC"]]}, Removed),
    FinalQuery = [["DEL", term_to_binary(SubscriberId)] | Query],
    [{ok, _} | _] = pipelined_query([["MULTI"] | FinalQuery]).

add_complex_topics(Topics) ->
    Nodes = vmq_cluster:nodes(),
    Query = lists:foldl(fun(T, Acc) ->
        lists:foreach(fun(Node) -> safe_rpc(Node, ?MODULE, add_complex_topic, ["", T]) end, Nodes),
        [["SADD", "wildcard_topic_list", term_to_binary(T)] | Acc]
                        end, [], Topics),
    pipelined_query(Query),
    ok.

add_complex_topic(MP, Topic) ->
    MPTopic = {MP, Topic},
    case ets:lookup(vmq_redis_trie_node, MPTopic) of
        [#trie_node{topic=Topic}] ->
            ignore;
        _ ->
            %% add trie path
            _ = [trie_add_path(MP, Triple) || Triple <- vmq_topic:triples(Topic)],
            %% add last node
            ets:insert(vmq_redis_trie_node, #trie_node{node_id=MPTopic, topic=Topic})
    end.

delete_complex_topics(Topics) ->
    Nodes = vmq_cluster:nodes(),
    Query = lists:foldl(fun(T, Acc) ->
        lists:foreach(fun(Node) -> safe_rpc(Node, ?MODULE, delete_complex_topic, ["", T]) end, Nodes),
        Acc ++ [["SREM", "wildcard_topic_list", term_to_binary(T)]]
                        end, [], Topics),
    pipelined_query(Query),
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
    [convert_topic_to_string(T) || T <-  ets:select(vmq_redis_trie_node, [{#trie_node{node_id={"", '$1'}, topic='$1', edge_count=0}, [{'=/=', '$1', undefined}], ['$1']}])].

-spec safe_rpc(Node::node(), Mod::module(), Fun::atom(), [any()]) -> any().
safe_rpc(Node, Module, Fun, Args) ->
    try rpc:call(Node, Module, Fun, Args) of
        Result ->
            Result
    catch
        exit:{noproc, _NoProcDetails} ->
            {badrpc, rpc_process_down};
        Type:Reason -> {Type, Reason}
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
    _ = ets:new(vmq_redis_trie, [{keypos, 2}|DefaultETSOpts]),
    _ = ets:new(vmq_redis_trie_node, [{keypos, 2}|DefaultETSOpts]),
    RedisHost = application:get_env(vmq_server, redis_host, "127.0.0.1"),
    RedisPort = application:get_env(vmq_server, redis_port, 6379),
    RedisDB = application:get_env(vmq_server, redis_database, 0),
    {ok, _Pid} = eredis:start_link([{host, RedisHost}, {port, RedisPort}, {database, RedisDB}, {name, {local, redis_client}}]),
    initialize_trie(),
    {ok, #state{status=ready}}.

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
fetchSubscribers(Topics, MP) ->
    Query = lists:foldl(fun(Topic, Acc) -> [["SMEMBERS", term_to_binary({vmq_trie_subs, MP, Topic})] | Acc] end, [], Topics),
    lists:foldl(fun ({ok, MembersList}, Acc) -> lists:append(Acc, MembersList);
                    (_, Acc) -> Acc end,
        [], pipelined_query(Query)).

fold_(_, [], _, Acc) -> Acc;
fold_(SubscriberId, [ SubscriberInfoBinary | SubscribersList], FoldFun, Acc) ->
    case binary_to_term(SubscriberInfoBinary) of
        {_Node, _SId, _QoS} = SubscriberInfo -> fold_(SubscriberId, SubscribersList, FoldFun, FoldFun(SubscriberInfo, SubscriberId, Acc));
        {_Node, _Group, _SId, _QoS} = SubscriberInfo -> fold_(SubscriberId, SubscribersList, FoldFun, FoldFun(SubscriberInfo, SubscriberId, Acc));
        _ -> fold_(SubscriberId, SubscribersList, FoldFun, Acc)
    end.

initialize_trie() ->
    {ok, TopicList} = query(["SMEMBERS", "wildcard_topic_list"]),
    lists:foreach(fun(T) ->
        Topic = binary_to_term(T),
        add_complex_topic("", Topic) end, TopicList),
    ok.

handle_add_event({[<<"$share">>, Group|Topic], QoS, Node}, {{MP, _} = SubscriberId, Query}) ->
    Key = {vmq_trie_subs, MP, Topic},
    Val = {Node, Group, SubscriberId, QoS},
    {SubscriberId, [["SADD", term_to_binary(Key), Val] | Query]};
handle_add_event({Topic, QoS, Node}, {{MP, _} = SubscriberId, Query}) ->
    Key = {vmq_trie_subs, MP, Topic},
    Val = {Node, SubscriberId, QoS},
    {SubscriberId, [["SADD", term_to_binary(Key), Val] | Query]}.

handle_delete_event({[<<"$share">>, Group|Topic], QoS, Node}, {{MP, _} = SubscriberId, Query}) ->
    Key = {vmq_trie_subs, MP, Topic},
    Val = {Node, Group, SubscriberId, QoS},
    {SubscriberId, [["SREM", term_to_binary(Key), Val] | Query]};
handle_delete_event({Topic, QoS, Node}, {{MP, _} = SubscriberId, Query}) ->
    Key = {vmq_trie_subs, MP, Topic},
    Val = {Node, SubscriberId, QoS},
    {SubscriberId, [["SREM", term_to_binary(Key), Val] | Query]}.

update_topics([], _, _, _, Query) -> Query;
update_topics([{[<<"$share">>, Group|Topic], QoS} | Subs], OldNode, NewNode, {MP, _} = SubscriberId, Query) ->
    Key = {vmq_trie_subs, MP, Topic},
    OldVal = {OldNode, Group, SubscriberId, QoS},
    NewVal = {NewNode, Group, SubscriberId, QoS},
    NewQuery1 = [["SADD", term_to_binary(Key), NewVal] | Query],
    NewQuery2 = [["SREM", term_to_binary(Key), OldVal] | NewQuery1],
    update_topics(Subs, OldNode, NewNode, SubscriberId, NewQuery2);
update_topics([{Topic, QoS} | Subs], OldNode, NewNode, {MP, _} = SubscriberId, Query) ->
    Key = {vmq_trie_subs, MP, Topic},
    OldVal = {OldNode, SubscriberId, QoS},
    NewVal = {NewNode, SubscriberId, QoS},
    NewQuery1 = [["SADD", term_to_binary(Key), NewVal] | Query],
    NewQuery2 = [["SREM", term_to_binary(Key), OldVal] | NewQuery1],
    update_topics(Subs, OldNode, NewNode, SubscriberId, NewQuery2).

query([Cmd | _] = QueryCmd) ->
    vmq_metrics:incr_redis_cmd(list_to_atom(string:lowercase(Cmd))),
    V1 = vmq_util:ts(),
    Result = case eredis:q(whereis(redis_client), QueryCmd) of
                 {error, Reason} ->
                     vmq_metrics:incr_redis_cmd_err(list_to_atom(string:lowercase(Cmd))),
                     lager:error("Cannot ~p due to ~p", [Cmd, Reason]),
                     {error, Reason};
                 {ok, undefined} ->
                     vmq_metrics:incr_redis_cmd_miss(list_to_atom(string:lowercase(Cmd))),
                     {ok, undefined};
                 {ok, []} ->
                     vmq_metrics:incr_redis_cmd_miss(list_to_atom(string:lowercase(Cmd))),
                     {ok, []};
                 Res -> Res
             end,
    vmq_metrics:pretimed_measurement({redis_cmd, run, [{cmd, Cmd}]}, vmq_util:ts() - V1),
    Result.

pipelined_query(QueryList) ->
    [_ | PipelinedCmd] = lists:foldl(fun([Cmd | _], Acc) -> "|" ++ Cmd ++ Acc end, "", QueryList),
    vmq_metrics:incr_redis_cmd(pipeline),
    V1 = vmq_util:ts(),
    Result = case eredis:qp(whereis(redis_client), QueryList) of
                 {error, no_connection} ->
                     lager:error("No connection with Redis"),
                     {error, no_connection};
                 Res -> Res
             end,
    IsErrPresent = lists:foldl(fun ({ok, _}, Acc) -> Acc;
                    ({error, Reason}, _Acc) ->
                        lager:error("Cannot ~p due to ~p", [PipelinedCmd, Reason]),
                        true
             end, false, Result),
    if IsErrPresent -> vmq_metrics:incr_redis_cmd_err(pipeline);
        true -> ok
    end,
    vmq_metrics:pretimed_measurement({redis_cmd, run, [{cmd, PipelinedCmd}]}, vmq_util:ts() - V1),
    Result.

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
    match(MP, Topic, Rest, [Name | Acc]);
match(MP, Topic, [_|Rest], Acc) ->
    match(MP, Topic, Rest, Acc);
match(_, _, [], Acc) -> Acc.

trie_add_path(MP, {Node, Word, Child}) ->
    NodeId = {MP, Node},
    Edge = #trie_edge{node_id=NodeId, word=Word},
    case ets:lookup(vmq_redis_trie_node, NodeId) of
        [TrieNode = #trie_node{edge_count=Count}] ->
            case ets:lookup(vmq_redis_trie, Edge) of
                [] ->
                    ets:insert(vmq_redis_trie_node,
                        TrieNode#trie_node{edge_count=Count + 1}),
                    ets:insert(vmq_redis_trie, #trie{edge=Edge, node_id=Child});
                [_] ->
                    ok
            end;
        [] ->
            ets:insert(vmq_redis_trie_node, #trie_node{node_id=NodeId, edge_count=1}),
            ets:insert(vmq_redis_trie, #trie{edge=Edge, node_id=Child})
    end.

trie_match(MP, Words) ->
    trie_match(MP, root, Words, []).

trie_match(MP, Node, [], ResAcc) ->
    NodeId = {MP, Node},
    ets:lookup(vmq_redis_trie_node, NodeId) ++ 'trie_match_#'(NodeId, ResAcc);
trie_match(MP, Node, [W|Words], ResAcc) ->
    NodeId = {MP, Node},
    lists:foldl(
        fun(WArg, Acc) ->
            case ets:lookup(vmq_redis_trie,
                #trie_edge{node_id=NodeId, word=WArg}) of
                [#trie{node_id=ChildId}] ->
                    trie_match(MP, ChildId, Words, Acc);
                [] ->
                    Acc
            end
        end, 'trie_match_#'(NodeId, ResAcc), [W, <<"+">>]).

'trie_match_#'({MP, _} = NodeId, ResAcc) ->
    case ets:lookup(vmq_redis_trie, #trie_edge{node_id=NodeId, word= <<"#">>}) of
        [#trie{node_id=ChildId}] ->
            ets:lookup(vmq_redis_trie_node, {MP, ChildId}) ++ ResAcc;
        [] ->
            ResAcc
    end.

trie_delete_path(_, []) ->
    ok;
trie_delete_path(MP, [{Node, Word, _}|RestPath]) ->
    NodeId = {MP, Node},
    Edge = #trie_edge{node_id=NodeId, word=Word},
    ets:delete(vmq_redis_trie, Edge),
    case ets:lookup(vmq_redis_trie_node, NodeId) of
        [_] ->
            ets:delete(vmq_redis_trie_node, NodeId),
            trie_delete_path(MP, RestPath);
        [] ->
            ignore
    end.

convert_topic_to_string(Topic) ->
    lists:foldl(fun(Word, Acc) ->
        WordString = binary_to_list(Word),
        case Acc of
            "" -> WordString;
            _ -> Acc ++ "/" ++ WordString
        end end,"",Topic).
