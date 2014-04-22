%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% Developer of the eMQTT Code is <ery.lee@gmail.com>
%% Copyright (c) 2012 Ery Lee.  All rights reserved.
%%
-module(emqttd_connection_reg).

-include("emqtt_internal.hrl").

-export([start_link/0]).

-export([topics/0,
         subscribe/3,
         unsubscribe/2,
         publish/3,
         route/3,
         match/1,
         register_client/2,
         disconnect_client/1,
         cleanup_client/1]).

-behaviour(gen_server).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).
-record(client, {id, node, pid}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

topics() ->
    mnesia:dirty_all_keys(topic).

subscribe(ClientId, Topic, Qos) ->
    case emqttd_node_watcher:ready() of
        true ->
            gen_server:call(?MODULE, {subscribe, {Topic, Qos}, ClientId, self()});
        false ->
            {error, maybe_cluster_split}
    end.

unsubscribe(ClientId, Topic) when is_list(Topic) ->
    case emqttd_node_watcher:ready() of
        true ->
            gen_server:call(?MODULE, {unsubscribe, Topic, ClientId});
        false ->
            {error, maybe_cluster_split}
    end.

register_client(ClientId, CleanSession) ->
    %% only allow to register new clients if we are not
    %% in a split brain situation
    %%
    %% TODO: check if we can stil register a new client
    %%       that has CleanSession == FALSE, within a
    %%       split-brain situation, if it used to be
    %%       registered on Node == node()
    case emqttd_node_watcher:ready() of
        true ->
            case mnesia:dirty_read(client, ClientId) of
                [#client{node=Node, pid=ClientPid}] when Node == node() ->
                    disconnect_client(ClientPid);
                [#client{node=Node, pid=ClientPid}] ->
                    rpc:call(Node, ?MODULE, disconnect_client, [ClientPid]);
                [] ->
                    ok
            end,
            gen_server:call(?MODULE, {register_client, CleanSession, ClientId, self()});
        false ->
            {error, maybe_cluster_split}
    end.

%publish to cluster node.
publish(RoutingKey, Payload, true) ->
    case emqttd_node_watcher:ready() of
        true ->
            publish_(RoutingKey, Payload, true);
        false ->
            %% we cannot store the retained Message, because
            %% we cannot tell the other cluster nodes to reset
            %% a possibly already retained message.
            {error, maybe_cluster_split}
    end;
publish(RoutingKey, Payload, false) ->
    publish_(RoutingKey, Payload, false).

publish_(RoutingKey, Payload, IsRetain) when is_list(RoutingKey) and is_binary(Payload) ->
    case IsRetain of
        true ->
            emqttd_msg_store:persist_retain_msg(RoutingKey, Payload);
        _ ->
            ignore
    end,
    ClusterNodes = emqttd_node_watcher:which_nodes(),
    lists:foreach(fun(#topic{name=Name, node=Node}) ->
                          case Node == node() of
                              true -> route(Name, RoutingKey, Payload);
                              false ->
                                  case lists:keyfind(Node, 1, ClusterNodes) of
                                      false ->
                                          io:format("!!! ROUTE to non configured Cluster Node ~p: RoutingKey: ~p Topic: ~p~n", [Node, RoutingKey, Name]);
                                      {Node, true} ->
                                          rpc:call(Node, ?MODULE, route, [Name, RoutingKey, Payload]);
                                      {Node, false} ->
                                          io:format("!!! ROUTE to unavailable Cluster Node ~p: RoutingKey: ~p Topic: ~p~n", [Node, RoutingKey, Name])
                                  end
                          end
                  end, match(RoutingKey)).

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
route(Topic, RoutingKey, Payload) ->
    UnroutableClients =
    lists:foldl(fun(#subscriber{qos=Qos, client=ClientId}, Acc) ->
                        case get_client_pid(ClientId) of
                            {ok, ClientPid} ->
                                emqttd_handler_fsm:deliver(ClientPid, RoutingKey, Payload, Qos, false),
                                Acc;
                            {error, not_found} when Qos > 0 ->
                                [{ClientId, Qos}|Acc];
                            {error, not_found} ->
                                Acc
                        end
                end, [], ets:lookup(subscriber, Topic)),
    io:format("unroutable client ~p~n", [UnroutableClients]),
    emqttd_msg_store:persist_for_later(UnroutableClients, RoutingKey, Payload).

match(Topic) when is_list(Topic) ->
    TrieNodes = mnesia:async_dirty(fun trie_match/1, [emqtt_topic:words(Topic)]),
    Names = [Name || #trie_node{topic=Name} <- TrieNodes, Name=/= undefined],
    lists:flatten([mnesia:dirty_read(topic, Name) || Name <- Names]).

init([]) ->
    mnesia:create_table(client, [
                                 {ram_copies, [node()]},
                                 {attributes, record_info(fields, client)}]),
    mnesia:add_table_copy(client, node(), ram_copies),

    mnesia:create_table(trie, [
                               {ram_copies, [node()]},
                               {attributes, record_info(fields, trie)}]),
    mnesia:add_table_copy(trie, node(), ram_copies),

    mnesia:create_table(trie_node, [
                                    {ram_copies, [node()]},
                                    {attributes, record_info(fields, trie_node)}]),
    mnesia:add_table_copy(trie_node, node(), ram_copies),

    mnesia:create_table(topic, [
                                {type, bag},
                                {record_name, topic},
                                {ram_copies, [node()]},
                                {attributes, record_info(fields, topic)}]),
    mnesia:add_table_copy(topic, node(), ram_copies),
    ets:new(subscriber, [{read_concurrency, true}, public, bag, named_table, {keypos, 2}]),
    {ok, #state{}}.

handle_call({register_client, CleanSession, ClientId, Pid}, _From, State) ->
    case mnesia:transaction(
           fun() ->
                   mnesia:write(#client{id=ClientId, node=node(), pid=Pid})
           end) of
        {atomic, _} ->
            monitor(process, Pid),
            %% this will also cleanup the message store
            cleanup_client(CleanSession, ClientId),
            case CleanSession of
                false ->
                    emqttd_msg_store:deliver_from_store(ClientId, Pid);
                true ->
                    ok
            end;
        {aborted, Reason} ->
            io:format("can't write to client table: ~p~n", [Reason])
    end,
    {reply, ok, State};
handle_call({subscribe, {Topic, Qos}, ClientId, ClientPid}, _From, State) ->
    case mnesia:transaction(fun subscriber_add/1, [Topic]) of
        {atomic, _} ->
            ets:insert(subscriber, #subscriber{topic=Topic, qos=Qos, client=ClientId}),
            ok = emqttd_msg_store:deliver_retained(ClientPid, Topic, Qos),
            {reply, ok, State};
        {aborted, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call({cleanup_client, ClientId}, _From, State) ->
    {reply, cleanup_client(true, ClientId), State};

handle_call({unsubscribe, Topic, ClientId}, _From, State) ->
    ets:match_delete(subscriber, #subscriber{topic=Topic, client=ClientId, _='_'}),
    try_remove_subscriber(Topic),
    {reply, ok, State};

handle_call(Req, _From, State) ->
    {stop, {badreq, Req}, State}.


handle_cast(Msg, State) ->
    {stop, {badmsg, Msg}, State}.

handle_info({'DOWN', _, process, ClientPid, _}, State) ->
    mnesia:transaction(
      fun() ->
              case mnesia:match_object(#client{pid=ClientPid, _='_'}) of
                  [] -> ignore;
                  [#client{id=ClientId}] ->
                      mnesia:delete({client, ClientId})
              end
      end),
    {noreply, State};

handle_info(Info, State) ->
    {stop, {badinfo, Info}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------------
%% internal functions
%% ------------------------------------------------------------------------
get_client_pid(ClientId) ->
    case mnesia:dirty_read(client, ClientId) of
        [#client{node=Node, pid=ClientPid}] when Node == node() ->
            {ok, ClientPid};
        _ ->
            {error, not_found}
    end.

cleanup_client(false, _) -> ok;
cleanup_client(true, ClientId) ->
    emqttd_msg_store:clean_session(ClientId),
    case ets:match_object(subscriber, #subscriber{client=ClientId, _='_'}) of
        [] -> ignore;
        Subs ->
            [try_remove_subscriber(Topic) || #subscriber{topic=Topic} <- Subs]
    end.

try_remove_subscriber(Name) ->
    case ets:member(subscriber, Name) of
        false ->
            Topic = emqtt_topic:new(Name),
            Fun = fun() ->
                          mnesia:delete_object(topic, Topic),
                          case mnesia:read(topic, Topic) of
                              [] -> trie_delete(Name);
                              _ -> ignore
                          end
                  end,
            mnesia:transaction(Fun);
        true ->
            ok
    end.


subscriber_add(Topic) ->
    mnesia:write(emqtt_topic:new(Topic)),
    case mnesia:read(trie_node, Topic) of
        [TrieNode=#trie_node{topic=undefined}] ->
            mnesia:write(TrieNode#trie_node{topic=Topic});
        [#trie_node{topic=Topic}] ->
            ignore;
        [] ->
            %add trie path
            [trie_add_path(Triple) || Triple <- emqtt_topic:triples(Topic)],
            %add last node
            mnesia:write(#trie_node{node_id=Topic, topic=Topic})
    end.

trie_delete(Topic) ->
    case mnesia:read(trie_node, Topic) of
        [#trie_node{edge_count=0}] ->
            mnesia:delete({trie_node, Topic}),
            trie_delete_path(lists:reverse(emqtt_topic:triples(Topic)));
        [TrieNode] ->
            mnesia:write(TrieNode#trie_node{topic=Topic});
        [] ->
            ignore
    end.

trie_match(Words) ->
    trie_match(root, Words, []).

trie_match(NodeId, [], ResAcc) ->
    mnesia:read(trie_node, NodeId) ++ 'trie_match_#'(NodeId, ResAcc);

trie_match(NodeId, [W|Words], ResAcc) ->
    lists:foldl(fun(WArg, Acc) ->
                        case mnesia:read(trie, #trie_edge{node_id=NodeId, word=WArg}) of
                            [#trie{node_id=ChildId}] -> trie_match(ChildId, Words, Acc);
                            [] -> Acc
                        end
                end, 'trie_match_#'(NodeId, ResAcc), [W, "+"]).

'trie_match_#'(NodeId, ResAcc) ->
 case mnesia:read(trie, #trie_edge{node_id=NodeId, word="#"}) of
     [#trie{node_id=ChildId}] ->
         mnesia:read(trie_node, ChildId) ++ ResAcc;
     [] ->
         ResAcc
 end.

trie_add_path({Node, Word, Child}) ->
    Edge = #trie_edge{node_id=Node, word=Word},
    case mnesia:read(trie_node, Node) of
        [TrieNode = #trie_node{edge_count=Count}] ->
            case mnesia:read(trie, Edge) of
                [] ->
                    mnesia:write(TrieNode#trie_node{edge_count=Count+1}),
                    mnesia:write(#trie{edge=Edge, node_id=Child});
                [_] ->
                    ok
            end;
        [] ->
            mnesia:write(#trie_node{node_id=Node, edge_count=1}),
            mnesia:write(#trie{edge=Edge, node_id=Child})
    end.

trie_delete_path([]) ->
    ok;
trie_delete_path([{NodeId, Word, _} | RestPath]) ->
    Edge = #trie_edge{node_id=NodeId, word=Word},
    mnesia:delete({trie, Edge}),
    case mnesia:read(trie_node, NodeId) of
        [#trie_node{edge_count=1, topic=undefined}] ->
            mnesia:delete({trie_node, NodeId}),
            trie_delete_path(RestPath);
        [TrieNode=#trie_node{edge_count=1, topic=_}] ->
            mnesia:write(TrieNode#trie_node{edge_count=0});
        [TrieNode=#trie_node{edge_count=C}] ->
            mnesia:write(TrieNode#trie_node{edge_count=C-1});
        [] ->
            throw({notfound, NodeId})
    end.
