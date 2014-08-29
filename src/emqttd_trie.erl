-module(emqttd_trie).
-include_lib("emqtt_commons/include/emqtt_internal.hrl").

-behaviour(gen_server).

-export([start_link/0,
         subscribe/2,
         unsubscribe/2,
         subscriptions/1,
         publish/4,
         route/4,
         register_client/2,
         disconnect_client/1,
         cleanup_client/1,
         match/1]).

-export([init/1,
	 handle_call/3,
	 handle_cast/2,
	 handle_info/2,
	 terminate/2,
	 code_change/3]).

-export([emqttd_table_defs/0,
         on_node_up/1,
         on_node_down/1]).

-record(st, {}).
-record(client, {id, node, pid}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

subscribe(ClientId, Topics) ->
    call({subscribe, self(), ClientId, Topics}).

unsubscribe(ClientId, Topics) ->
    call({unsubscribe, ClientId, Topics}).

subscriptions(RoutingKey) ->
    lists:foldl(
      fun(#topic{name=Topic, node=Node}, Acc) ->
              case Node == node() of
                  true ->
                      lists:foldl(fun
                                      (#subscriber{client=ClientId, qos=Qos}, Acc1) when Qos > 0 ->
                                          [{ClientId, Qos}|Acc1];
                                      (_, Acc1) ->
                                          Acc1
                                  end, Acc, mnesia:dirty_read(emqttd_subscriber, Topic));
                  false ->
                      Acc
              end
      end, [], match(RoutingKey)).


register_client(ClientId, CleanSession) ->
    case is_ready() of
        true ->
            Nodes = [Node || [{Node, true}]
                             <- ets:match(emqttd_status, '$1'), Node /= ready],
            Req = {register_client, node(), CleanSession, ClientId, self()},
            case gen_server:multi_call(Nodes, ?MODULE, Req) of
                {_, []} ->
                    ok;
                _ ->
                    {error, not_ready}
            end;
        false ->
            {error, not_ready}
    end.

%publish to cluster node.
publish(MsgId, RoutingKey, Payload, IsRetain) when is_list(RoutingKey) and is_binary(Payload) ->
    publish_(MsgId, RoutingKey, Payload, IsRetain).

publish_(MsgId, RoutingKey, Payload, true) ->
    case is_ready() of
        true ->
            case emqttd_msg_store:retain_action(MsgId, RoutingKey, Payload) of
                ok ->
                    lists:foreach(
                      fun(#topic{name=Name, node=Node}) ->
                              case Node == node() of
                                  true ->
                                      route(MsgId, Name, RoutingKey, Payload);
                                  false ->
                                      rpc:call(Node, ?MODULE, route, [MsgId, Name, RoutingKey, Payload])
                              end
                      end, match(RoutingKey));
                Error ->
                    Error
            end;
        false ->
            {error, not_ready}
    end;
publish_(MsgId, RoutingKey, Payload, false) ->
    MatchedTopics = match(RoutingKey),
    case check_single_node(node(), MatchedTopics, length(MatchedTopics)) of
        true ->
            %% in case we have only subscriptions on one single node
            %% we can deliver the messages even in case of network partitions
            lists:foreach(fun(#topic{name=Name}) ->
                                  route(MsgId, Name, RoutingKey, Payload)
                          end, MatchedTopics);
        false ->
            case is_ready() of
                true ->
                    lists:foreach(
                      fun(#topic{name=Name, node=Node}) ->
                              case Node == node() of
                                  true ->
                                      route(MsgId, Name, RoutingKey, Payload);
                                  false ->
                                      rpc:call(Node, ?MODULE, route, [MsgId, Name, RoutingKey, Payload])
                              end
                      end, MatchedTopics);
                false ->
                    {error, not_ready}
            end
    end.

check_single_node(Node, [#topic{node=Node}|Rest], Acc) ->
    check_single_node(Node, Rest, Acc -1);
check_single_node(Node, [_|Rest], Acc) ->
    check_single_node(Node, Rest, Acc);
check_single_node(_, [], 0) -> true;
check_single_node(_, [], _) -> false.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% RPC Callbacks / Maintenance
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
cleanup_client(ClientId) ->
    gen_server:call(?MODULE, {cleanup_client, ClientId}).

disconnect_client(ClientPid) when is_pid(ClientPid) ->
    emqttd_handler_fsm:disconnect(ClientPid);

disconnect_client(ClientId) ->
    case get_client_pid(ClientId) of
        {ok, ClientPid} -> disconnect_client(ClientPid);
        E -> E
    end.
%route locally, should only be called by publish
route(MsgId, Topic, RoutingKey, Payload) ->
    lists:foreach(fun
                    (#subscriber{qos=Qos, client=ClientId}) when Qos > 0 ->
                          MaybeNewMsgId = emqttd_msg_store:store(MsgId, RoutingKey, Payload),
                          deliver(ClientId, RoutingKey, Payload, Qos, MaybeNewMsgId);
                    (#subscriber{qos=0, client=ClientId}) ->
                          deliver(ClientId, RoutingKey, Payload, 0, undefined)
                end, mnesia:dirty_read(emqttd_subscriber, Topic)).

deliver(ClientId, RoutingKey, Payload, Qos, Ref) ->
    case get_client_pid(ClientId) of
        {ok, ClientPid} ->
            emqttd_handler_fsm:deliver(ClientPid, RoutingKey, Payload, Qos, false, Ref);
        _ when Qos > 0 ->
            emqttd_msg_store:defer_deliver(ClientId, Qos, Ref),
            ok;
        _ -> ok
    end.

match(Topic) when is_list(Topic) ->
    TrieNodes = mnesia:async_dirty(fun trie_match/1, [emqtt_topic:words(Topic)]),
    Names = [Name || #trie_node{topic=Name} <- TrieNodes, Name=/= undefined],
    lists:flatten([mnesia:dirty_read(emqttd_trie_topic, Name) || Name <- Names]).

emqttd_table_defs() ->
    [
     {emqttd_trie,[
       {record_name, trie},
       {attributes, record_info(fields, trie)},
       {disc_copies, [node()]},
       {match, #trie{_='_'}}]},
     {emqttd_trie_node,[
       {record_name, trie_node},
       {attributes, record_info(fields, trie_node)},
       {disc_copies, [node()]},
       {match, #trie_node{_='_'}}]},
     {emqttd_trie_topic,[
       {record_name, topic},
       {type, bag},
       {attributes, record_info(fields, topic)},
       {disc_copies, [node()]},
       {match, #topic{_='_'}}]},
     {emqttd_subscriber,[
        {record_name, subscriber},
        {type, bag},
        {attributes, record_info(fields, subscriber)},
        {disc_copies, [node()]},
        {match, #subscriber{_='_'}}]}
].

on_node_up(Node) ->
    wait_for_table(fun() ->
                           Nodes = mnesia_cluster_utils:cluster_nodes(all),
                           ets:insert(emqttd_status, {Node, true}),
                           update_ready(Nodes)
                   end).

on_node_down(Node) ->
    wait_for_table(fun() ->
                           Nodes = mnesia_cluster_utils:cluster_nodes(all),
                           ets:delete(emqttd_status, Node),
                           update_ready(Nodes)
                   end).

update_ready(Nodes) ->
    SortedNodes = lists:sort(Nodes),
    IsReady = lists:sort([Node || [{Node, true}]
                                    <- ets:match(emqttd_status, '$1'),
                                    Node /= ready]) == SortedNodes,
    ets:insert(emqttd_status, {ready, IsReady}).

is_ready() ->
    ets:lookup(emqttd_status, ready) == [{ready, true}].

wait_for_table(Fun) ->
    case lists:member(emqttd_status, ets:all()) of
        true -> Fun();
        false -> timer:sleep(100)
    end.


call(Msg) ->
    case is_ready() of
        true ->
            gen_server:call(?MODULE, Msg);
        false ->
            {error, not_ready}
    end.

init([]) ->
    io:fwrite("init([])~n"),
    ets:new(emqttd_client, [named_table, public, {read_concurrency, true}, {keypos, 2}]),
    ets:new(emqttd_status, [{read_concurrency, true}, public, named_table]),

    {ok, #st{}}.

handle_call({subscribe, ClientPid, ClientId, Topics}, _From, State) ->
    Errors =
    lists:foldl(fun({Topic, Qos}, Errors) ->
                        case mnesia:transaction(fun add_subscriber/3, [Topic, Qos, ClientId]) of
                            {atomic, _} ->
                                emqttd_msg_store:deliver_retained(ClientPid, Topic, Qos),
                                Errors;
                            {aborted, Reason} ->
                                [Reason|Errors]
                        end
                end, [], Topics),
    case Errors of
        [] ->
            {reply, ok, State};
        Errors ->
            {reply, {error, Errors}, State}
    end;

handle_call({unsubscribe, ClientId, Topics}, _From, State) ->
    lists:foreach(fun(Topic) ->
                          {atomic, _} = mnesia:transaction(fun del_subscriber/2, [Topic, ClientId])
                  end, Topics),
    {reply, ok, State};

handle_call({register_client, Node, CleanSession, ClientId, Pid}, _From, State) ->
    disconnect_client(ClientId), %% disconnect in case we already have such a client id
    case Node == node() of
        true ->
            ets:insert(emqttd_client, #client{id=ClientId, node=Node, pid=Pid}),
            monitor(process, Pid);
        false ->
            ok
    end,
    case CleanSession of
        false ->
            emqttd_msg_store:deliver_from_store(ClientId, Pid);
        true ->
            %% this will also cleanup the message store
            cleanup_client_(ClientId)
    end,
    {reply, ok, State};

handle_call({cleanup, ClientId}, _From, State) ->
    %% this will also cleanup the message store
    cleanup_client_(ClientId),
    {reply, ok, State}.

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info({'DOWN', _, process, ClientPid, _}, State) ->
    case ets:match_object(emqttd_client, #client{pid=ClientPid, _='_'}) of
        [] -> ignore;
        [#client{id=ClientId}] ->
            ets:delete(emqttd_client, ClientId)
    end,
    {noreply, State};

handle_info(_Msg, S) ->
    {noreply, S}.

code_change(_FromVsn, S, _Extra) ->
    {ok, S}.

terminate(_Reason, _S) ->
    ok.


add_subscriber(Topic, Qos, ClientId) ->
    mnesia:write(emqttd_subscriber, #subscriber{topic=Topic, qos=Qos, client=ClientId}, write),
    mnesia:write(emqttd_trie_topic, emqtt_topic:new(Topic), write),
    case mnesia:read(emqttd_trie_node, Topic) of
        [TrieNode=#trie_node{topic=undefined}] ->
            mnesia:write(emqttd_trie_node, TrieNode#trie_node{topic=Topic}, write);
        [#trie_node{topic=Topic}] ->
            ignore;
        [] ->
            %add trie path
            [trie_add_path(Triple) || Triple <- emqtt_topic:triples(Topic)],
            %add last node
            mnesia:write(emqttd_trie_node, #trie_node{node_id=Topic, topic=Topic}, write)
    end.


del_subscriber(Topic, ClientId) ->
    Objs = mnesia:match_object(emqttd_subscriber, #subscriber{topic=Topic, client=ClientId, _='_'}, read),
    lists:foreach(fun(Obj) ->
                          mnesia:delete_object(emqttd_subscriber, Obj, write)
                  end, Objs),
    del_topic(Topic).

del_topic('_') -> ok;
del_topic(Topic) ->
    case mnesia:read(emqttd_subscriber, Topic) of
        [] ->
            TopicRec = emqtt_topic:new(Topic),
            mnesia:delete_object(emqttd_trie_topic, TopicRec, write),
            case mnesia:read(emqttd_trie_topic, Topic) of
                [] -> trie_delete(Topic);
                _ -> ignore
            end;
        _ ->
            ok
    end.

trie_delete(Topic) ->
    case mnesia:read(emqttd_trie_node, Topic) of
        [#trie_node{edge_count=0}] ->
            mnesia:delete({emqttd_trie_node, Topic}),
            trie_delete_path(lists:reverse(emqtt_topic:triples(Topic)));
        [TrieNode] ->
            mnesia:write(emqttd_trie_node, TrieNode#trie_node{topic=Topic}, write);
        [] ->
            ignore
    end.

trie_match(Words) ->
    trie_match(root, Words, []).

trie_match(NodeId, [], ResAcc) ->
    mnesia:read(emqttd_trie_node, NodeId) ++ 'trie_match_#'(NodeId, ResAcc);

trie_match(NodeId, [W|Words], ResAcc) ->
    lists:foldl(
      fun(WArg, Acc) ->
              case mnesia:read(emqttd_trie, #trie_edge{node_id=NodeId, word=WArg}) of
                  [#trie{node_id=ChildId}] -> trie_match(ChildId, Words, Acc);
                  [] -> Acc
              end
      end, 'trie_match_#'(NodeId, ResAcc), [W, "+"]).

'trie_match_#'(NodeId, ResAcc) ->
    case mnesia:read(emqttd_trie, #trie_edge{node_id=NodeId, word="#"}) of
        [#trie{node_id=ChildId}] ->
            mnesia:read(emqttd_trie_node, ChildId) ++ ResAcc;
        [] ->
            ResAcc
    end.

trie_add_path({Node, Word, Child}) ->
    Edge = #trie_edge{node_id=Node, word=Word},
    case mnesia:read(emqttd_trie_node, Node) of
        [TrieNode = #trie_node{edge_count=Count}] ->
            case mnesia:read(emqttd_trie, Edge) of
                [] ->
                    mnesia:write(emqttd_trie_node, TrieNode#trie_node{edge_count=Count+1}, write),
                    mnesia:write(emqttd_trie, #trie{edge=Edge, node_id=Child}, write);
                [_] ->
                    ok
            end;
        [] ->
            mnesia:write(emqttd_trie_node, #trie_node{node_id=Node, edge_count=1}, write),
            mnesia:write(emqttd_trie, #trie{edge=Edge, node_id=Child}, write)
    end.

trie_delete_path([]) ->
    ok;
trie_delete_path([{NodeId, Word, _}|RestPath]) ->
    Edge = #trie_edge{node_id=NodeId, word=Word},
    mnesia:delete({emqttd_trie, Edge}),
    case mnesia:read(emqttd_trie_node, NodeId) of
        [#trie_node{edge_count=1, topic=undefined}] ->
            mnesia:delete({emqttd_trie_node, NodeId}),
            trie_delete_path(RestPath);
        [TrieNode=#trie_node{edge_count=1, topic=_}] ->
            mnesia:write(emqttd_trie_node, TrieNode#trie_node{edge_count=0}, write);
        [TrieNode=#trie_node{edge_count=Count}] ->
            mnesia:write(emqttd_trie_node, TrieNode#trie_node{edge_count=Count-1}, write);
        [] ->
            throw({notfound, NodeId})
    end.


get_client_pid(ClientId) ->
    case ets:lookup(emqttd_client, ClientId) of
        [#client{node=Node, pid=ClientPid}] when Node == node() ->
            {ok, ClientPid};
        _ ->
            {error, not_found}
    end.


cleanup_client_(ClientId) ->
    emqttd_msg_store:clean_session(ClientId),
    {atomic, _} = mnesia:transaction(fun del_subscriber/2, ['_', ClientId]).
