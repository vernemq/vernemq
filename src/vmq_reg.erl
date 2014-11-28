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

-module(vmq_reg).
-include("vmq_server.hrl").

%% API
-export([start_link/0,

         %% used in vmq_session fsm handling
         subscribe/4,
         unsubscribe/3,
         register_client/3,
         %% used in vmq_session fsm handling AND vmq_systree
         publish/1,

         %% used in vmq_session:get_info/2 and vmq_session:list_sessions/1
         subscriptions_for_client/1,
         get_client_pid/1,

         %% used in vmq_msg_store:init/1
         subscriptions/1,
         match/1,

         %% used in vmq_systree
         total_clients/0,
         retained/0,

         %% used in vmq_session_expirer
         remove_expired_clients/1
        ]).

%% gen_server callback
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% used by RPC calls
-export([teardown_session/2, route/2,
         register_client_/4]).

%% used by mnesia_cluster for table setup
-export([vmq_table_defs/0]).
%% used from vmq-admin script (deployed with the VerneMQ release)
-export([reset_all_tables/1]).
%% used from plugins
-export([direct_plugin_exports/1]).

-record(state, {}).
-record(session, {client_id, pid, queue_pid, monitor, last_seen, clean}).

-record(topic, {name, node}).
-record(trie, {edge, node_id, vclock=unsplit_vclock:fresh()}).
-record(trie_node, {node_id, edge_count=0, topic,
                    vclock=unsplit_vclock:fresh()}).
-record(trie_edge, {node_id, word}).
-record(subscriber, {topic, qos, client, node}).
-record(retain, {words, routing_key, payload, vclock=unsplit_vclock:fresh()}).

-type state() :: #state{}.
-type subscriber() :: #subscriber{}.

-spec start_link() -> {ok, pid()} | ignore | {error, atom()}.
start_link() ->
    case ets:info(vmq_session, name) of
        undefined ->
            SessionFile = vmq_config:get_env(session_dump_file, "session.dump"),
            case filelib:is_file(SessionFile) of
                true ->
                    {ok, _Tab} = ets:file2tab(SessionFile);
                false ->
                    ets:new(vmq_session, [public,
                                          named_table,
                                          {keypos, 2},
                                          {read_concurrency, true},
                                          {write_concurrency, true}])
            end;
        _ ->
            ok
    end,
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec subscribe(username() | plugin_id(), client_id(), pid(),
                [{topic(), qos()}]) -> ok | {error, not_allowed | [any(), ...]}.
subscribe(User, ClientId, QPid, Topics) ->
    vmq_cluster:if_ready(fun subscribe_/4, [User, ClientId, QPid, Topics]).

-spec subscribe_(username() | plugin_id(), client_id(), pid(),
                 [{topic(), qos()}]) -> 'ok' | {'error','not_allowed'}.
subscribe_(User, ClientId, QPid, Topics) ->
    case vmq_plugin:all_till_ok(auth_on_subscribe, [User, ClientId, Topics]) of
        ok ->
            vmq_plugin:all(on_subscribe, [User, ClientId, Topics]),
            subscribe_tx(ClientId, QPid, Topics);
        {error, _} ->
            {error, not_allowed}
    end.

-spec subscribe_tx(client_id(), pid(), [{topic(), qos()}]) -> 'ok'.
subscribe_tx(_, _, []) -> ok;
subscribe_tx(ClientId, QPid, Topics) ->
    transaction(fun() ->
                        _ = [add_subscriber_tx(T, QoS, ClientId)
                             || {T, QoS} <- Topics]
                end,
                fun(_) ->
                        _ = [begin
                                 vmq_systree:incr_subscription_count(),
                                 deliver_retained(ClientId, QPid, T, QoS)
                             end || {T, QoS} <- Topics],
                        ok
                end).

-spec unsubscribe(username() | plugin_id(), client_id(), [topic()]) -> any().
unsubscribe(User, ClientId, Topics) ->
    vmq_cluster:if_ready(fun unsubscribe_/3, [User, ClientId, Topics]).

-spec unsubscribe_(username() | plugin_id(), client_id(), [topic()]) -> 'ok'.
unsubscribe_(User, ClientId, Topics) ->
    lists:foreach(fun(Topic) ->
                          del_subscriber(Topic, ClientId),
                          vmq_systree:decr_subscription_count()
                  end, Topics),
    vmq_plugin:all(on_unsubscribe, [User, ClientId, Topics]),
    ok.

-spec subscriptions(routing_key()) -> [{client_id(), qos()}].
subscriptions(RoutingKey) ->
    subscriptions(match(RoutingKey), []).

-spec subscriptions([topic()], _) -> [{client_id(), qos()}].
subscriptions([], Acc) -> Acc;
subscriptions([#topic{name=Topic, node=Node}|Rest], Acc) when Node == node() ->
    subscriptions(Rest, lists:foldl(
                          fun
                              (#subscriber{client=ClientId,
                                     qos=Qos}, Acc1) when Qos > 0 ->
                                  [{ClientId, Qos}|Acc1];
                        (_, Acc1) ->
                                  Acc1
                          end, Acc, mnesia:dirty_read(vmq_subscriber, Topic)));
subscriptions([_|Rest], Acc) ->
    subscriptions(Rest, Acc).

subscriptions_for_client(ClientId) ->
    Res = mnesia:dirty_match_object(vmq_subscriber,
                              #subscriber{client=ClientId, _='_'}),
    [{T, Q} || #subscriber{topic=T, qos=Q} <- Res].

-spec register_client(client_id(), pid(), flag()) -> ok | {error, _}.
register_client(ClientId, QPid, CleanSession) ->
    case vmq_reg_leader:register_client(self(), QPid, ClientId, CleanSession) of
        ok when not CleanSession ->
            vmq_session_proxy_sup:start_delivery(QPid, ClientId),
            ok;
        R ->
            R
    end.

teardown_session(ClientId, CleanSession) ->
    %% we first have to disconnect a local or remote session for
    %% this client id if one exists, disconnect_client(ClientID)
    %% blocks until the session is cleaned up
    NodeWithSession =
    case disconnect_client(ClientId) of
        ok ->
            [node()];
        {error, not_found} ->
            []
    end,
    case CleanSession of
        true -> vmq_msg_store:clean_session(ClientId);
        false ->
            ignore
    end,
    NodeWithSession.

-spec register_client_(pid(), pid(), client_id(), flag()) -> 'ok'
                                                      | {error, overloaded}.
register_client_(SessionPid, QPid, ClientId, CleanSession) ->
    %% cleanup session for this client id if needed
    case CleanSession of
        true ->
            transaction(fun() -> del_subscriber_tx('_', ClientId) end,
                        fun({error, Reason}) -> {error, Reason};
                           (_) -> register_client__(SessionPid, QPid,
                                                    ClientId, true)
                        end);
        false ->
            transaction(fun() -> remap_subscriptions_tx(ClientId) end,
                        fun({error, Reason}) -> {error, Reason};
                           (_) -> register_client__(SessionPid, QPid,
                                                    ClientId, false)
                        end)
    end.

-spec register_client__(pid(), pid(), client_id(), flag()) -> 'ok'.
register_client__(SessionPid, QPid, ClientId, CleanSession) ->
    NodesWithSession =
    lists:foldl(
      fun(Node, Acc) ->
              Res =
              case Node == node() of
                  true ->
                      teardown_session(ClientId, CleanSession);
                  false ->
                      rpc:call(Node, ?MODULE,
                               teardown_session, [ClientId,
                                                  CleanSession])
              end,
              [Res|Acc]
      end, [], vmq_cluster:nodes()),
    %% We SHOULD always have 0 or 1 Node(s) that had a sesssion
    NNodesWithSession = lists:flatten(NodesWithSession),
    case length(NNodesWithSession) of
        L when L =< 1 ->
            ok;
        _ -> lager:warning("client ~p was active on multiple nodes ~p",
                           [ClientId, NNodesWithSession])
    end,
    ok = gen_server:call(?MODULE, {monitor, SessionPid, QPid,
                                   ClientId, CleanSession}, infinity).

-spec publish(msg()) -> 'ok' | {'error', _}.
publish(#vmq_msg{routing_key=RoutingKey, retain=IsRetain} = Msg) ->
    MatchedTopics = match(RoutingKey),
    Ret =
    case IsRetain of
        true ->
            vmq_cluster:if_ready(fun publish_/2, [Msg, MatchedTopics]);
        false ->
            case check_single_node(node(), MatchedTopics,
                                   length(MatchedTopics)) of
                true ->
                    %% in case we have only subscriptions on one single node
                    %% we can deliver the messages even in case
                    %% of network partitions. TODO: this is partly true,
                    %% in a split brain situation we might not know if
                    %% another partition got matching subscriptions in the
                    %% meantime.
                    route_single_node(Msg, MatchedTopics);
                false ->
                    publish_if_ready(Msg, MatchedTopics)
            end
    end,
    Ret.


publish_if_ready(Msg, MatchedTopics) ->
    vmq_cluster:if_ready(fun publish__/2, [Msg, MatchedTopics]).

route_single_node(Msg, MatchedTopics) ->
    lists:foreach(fun(#topic{name=Name}) ->
                          route(Name, Msg)
                  end, MatchedTopics).

-spec publish_(msg(), [topic()]) -> 'ok' | {error, overloaded}.
publish_(#vmq_msg{retain=true} = Msg, MatchedTopics) ->
    case retain_action(Msg) of
        {error, overloaded} ->
            {error, overloaded};
        ok ->
            publish__(Msg#vmq_msg{retain=false}, MatchedTopics)
    end.

-spec publish__(msg(), [topic()]) -> 'ok'.
publish__(Msg, MatchedTopics) ->
    lists:foreach(
      fun(#topic{name=Name, node=Node}) ->
              case Node == node() of
                  true ->
                      route(Name, Msg);
                  false ->
                      rpc:call(Node, ?MODULE, route, [Name, Msg])
              end
      end, MatchedTopics).


-spec check_single_node(atom(), [topic()], integer()) -> boolean().
check_single_node(Node, [#topic{node=Node}|Rest], Acc) ->
    check_single_node(Node, Rest, Acc -1);
check_single_node(Node, [_|Rest], Acc) ->
    check_single_node(Node, Rest, Acc);
check_single_node(_, [], 0) -> true;
check_single_node(_, [], _) -> false.

-spec retain_action(msg()) -> 'ok' | {error, overloaded}.
retain_action(#vmq_msg{routing_key=RoutingKey, payload= <<>>}) ->
    %% retain-delete action
    Words = emqtt_topic:words(RoutingKey),
    transaction(fun() -> mnesia:delete({vmq_retain, Words}) end);
retain_action(#vmq_msg{routing_key=RoutingKey, payload= Payload}) ->
    %% retain-insert action
    Words = emqtt_topic:words(RoutingKey),
    transaction(
      fun() ->
              case mnesia:wread({vmq_retain, Words}) of
                  [] ->
                      mnesia:write(vmq_retain, #retain{words=Words,
                                                       routing_key=RoutingKey,
                                                       payload=Payload}, write);
                  [#retain{payload=Payload}] ->
                      %% same retain message, no update needed
                      ok;
                  [#retain{vclock=VClock} = Old] ->
                      mnesia:write(vmq_retain,
                                   Old#retain{payload=Payload,
                                              vclock=
                                                  unsplit_vclock:increment(
                                                    node(), VClock)
                                             }, write)
              end
      end).

-spec deliver_retained(client_id(), pid(), topic(), qos()) -> 'ok'.
deliver_retained(Client, QPid, Topic, QoS) ->
    Words = [case W of
                 "+" -> '_';
                 _ -> W
             end || W <- emqtt_topic:words(Topic)],
    NewWords =
    case lists:reverse(Words) of
        ["#"|Tail] -> lists:reverse(Tail) ++ '_' ;
        _ -> Words
    end,
    RetainedMsgs = mnesia:dirty_match_object(vmq_retain,
                                             #retain{words=NewWords,
                                                     routing_key='_',
                                                     payload='_', vclock='_'}),
    lists:foreach(
      fun(#retain{routing_key=RoutingKey, payload=Payload}) ->
              Msg = #vmq_msg{routing_key=RoutingKey,
                             payload=Payload,
                             retain=true,
                             dup=false},
              MaybeChangedMsg =
              case QoS of
                  0 -> Msg;
                  _ -> vmq_msg_store:store(Client, Msg)
              end,
              vmq_queue:enqueue(QPid, {deliver, QoS, MaybeChangedMsg})
      end, RetainedMsgs).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% RPC Callbacks / Maintenance
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec disconnect_client(client_id() | pid()) -> 'ok' | {'error','not_found'}.
disconnect_client(ClientPid) when is_pid(ClientPid) ->
    vmq_session:disconnect(ClientPid),
    ok;
disconnect_client(ClientId) ->
    wait_until_unregistered(ClientId).

-spec wait_until_unregistered(client_id()) -> {'error','not_found'}.
wait_until_unregistered(ClientId) ->
    case get_client_pid(ClientId) of
        {ok, ClientPid} ->
            disconnect_client(ClientPid),
            wait_until_stopped(ClientPid);
        E -> E
    end.

-spec wait_until_stopped(pid()) -> ok.
wait_until_stopped(ClientPid) ->
    case is_process_alive(ClientPid) of
        true ->
            timer:sleep(100),
            wait_until_stopped(ClientPid);
        false ->
            ok
    end.


%route locally, should only be called by publish
-spec route(topic(), msg()) -> 'ok'.
route(Topic, #vmq_msg{} = Msg) ->
    Subscribers = mnesia:dirty_read(vmq_subscriber, Topic),
    lists:foldl(fun
                    (#subscriber{qos=0} = Subs, AccMsg) ->
                        %% in case of a qos-upgrade we'll
                        %% increment the ref count inside the vmq_session
                        deliver(AccMsg, Subs),
                        AccMsg;
                    (#subscriber{client=Client} = Subs, AccMsg) ->
                        MaybeChangedMsg = vmq_msg_store:store(Client, AccMsg),
                        deliver(MaybeChangedMsg, Subs),
                        MaybeChangedMsg
                end, Msg, Subscribers),
    ok.

-spec deliver(msg(), subscriber()) -> 'ok' | {'error', 'not_found'}.
deliver(#vmq_msg{payload= <<>>, retain=true, msg_ref=Ref}, _) ->
    %% <<>> --> retain-delete action, we don't deliver the empty frame
    vmq_msg_store:deref(Ref),
    ok;
deliver(Msg, #subscriber{client=Client, qos=QoS}) ->
    case get_queue_pid(Client) of
        {ok, QPid} ->
            vmq_queue:enqueue(QPid, {deliver, QoS, Msg});
        _ when QoS > 0 ->
            vmq_msg_store:defer_deliver(Client, QoS, Msg#vmq_msg.msg_ref),
            ok;
        _ ->
            ok
    end.

-spec match(routing_key()) -> [topic()].
match(Topic) when is_list(Topic) ->
    TrieNodes = mnesia:async_dirty(fun trie_match/1,
                                   [emqtt_topic:words(Topic)]),
    Names = [Name || #trie_node{topic=Name} <- TrieNodes, Name=/= undefined],
    lists:flatten([mnesia:dirty_read(vmq_trie_topic, Name) || Name <- Names]).

-spec vmq_table_defs() -> [{atom(), [{atom(), any()}]}].
vmq_table_defs() ->
    [
     {vmq_trie, [
       {record_name, trie},
       {attributes, record_info(fields, trie)},
       {disc_copies, [node()]},
       {match, #trie{_='_'}},
       unsplit_vclock_props(#trie.vclock)]},
     {vmq_trie_node, [
       {record_name, trie_node},
       {attributes, record_info(fields, trie_node)},
       {disc_copies, [node()]},
       {match, #trie_node{_='_'}},
       unsplit_vclock_props(#trie_node.vclock)]},
     {vmq_trie_topic, [
       {record_name, topic},
       {type, bag},
       {attributes, record_info(fields, topic)},
       {disc_copies, [node()]},
       {match, #topic{_='_'}},
       unsplit_bag_props()]},
     {vmq_subscriber, [
        {record_name, subscriber},
        {type, bag},
        {attributes, record_info(fields, subscriber)},
        {disc_copies, [node()]},
        {match, #subscriber{_='_'}},
        unsplit_bag_props()]},
     {vmq_retain, [
        {record_name, retain},
        {attributes, record_info(fields, retain)},
        {disc_copies, [node()]},
        {match, #retain{_='_'}},
        unsplit_vclock_props(#retain.vclock)]}
].

unsplit_bag_props() ->
    {user_properties,
     [{unsplit_method, {unsplit_lib, bag, []}}]}.

unsplit_vclock_props(Pos) ->
    {user_properties,
     [{unsplit_method, {unsplit_lib, vclock, [Pos]}}]}.

-spec reset_all_tables([]) -> ok.
reset_all_tables([]) ->
    %% called using vmq-admin, mainly for test purposes
    %% you don't want to call this during production
    [reset_table(T) || {T, _}<- vmq_table_defs()],
    ets:delete_all_objects(vmq_session),
    ok.

-spec reset_table(atom()) -> ok | {error, overloaded}.
reset_table(Tab) ->
    Keys = mnesia:dirty_all_keys(Tab),
    transaction(
      fun() ->
              lists:foreach(fun(Key) ->
                                    mnesia:delete({Tab, Key})
                            end, Keys)
      end).

-spec wait_til_ready() -> 'ok'.
wait_til_ready() ->
    case vmq_cluster:if_ready(fun() -> true end, []) of
        true ->
            ok;
        {error, not_ready} ->
            timer:sleep(100),
            wait_til_ready()
    end.

-spec direct_plugin_exports(module()) -> {function(), function(), function()}.
direct_plugin_exports(Mod) ->
    %% This Function exports a generic Register, Publish, and Subscribe
    %% Fun, that a plugin can use if needed. Currently all functions
    %% block until the cluster is ready.
    ClientId = fun(T) ->
                       base64:encode_to_string(
                         integer_to_binary(
                           erlang:phash2(T)
                          )
                        )
               end,
    QueueSize = vmq_config:get_env(max_queued_messages, 1000),
    {ok, QPid} = vmq_queue:start_link(self(), QueueSize),

    RegisterFun =
    fun() ->
            wait_til_ready(),
            CallingPid = self(),
            register_client_(CallingPid, QPid, ClientId(CallingPid), true)
    end,

    PublishFun =
    fun(Topic, Payload) ->
            wait_til_ready(),
            Msg = #vmq_msg{routing_key=Topic,
                           payload=Payload,
                           dup=false,
                           retain=false},
            ok = publish(Msg),
            ok
    end,

    SubscribeFun =
    fun(Topic) ->
            wait_til_ready(),
            CallingPid = self(),
            User = {plugin, Mod, CallingPid},
            ok = subscribe(User, ClientId(CallingPid), QPid, [{Topic, 0}]),
            ok
    end,
    {RegisterFun, PublishFun, SubscribeFun}.

-spec add_subscriber_tx(topic(), qos(), client_id()) -> ok | ignore | abort.
add_subscriber_tx(Topic, Qos, ClientId) ->
    mnesia:write(vmq_subscriber,
                 #subscriber{topic=Topic, qos=Qos, client=ClientId,
                                             node=node()}, write),
    add_topic(Topic, node()).

-spec add_topic(topic(), atom()) -> ok | ignore | abort.
add_topic(Topic, Node) ->
    mnesia:write(vmq_trie_topic, #topic{name=Topic, node=Node}, write),
    case mnesia:wread({vmq_trie_node, Topic}) of
        [TrieNode=#trie_node{topic=undefined, vclock=VClock}] ->
            mnesia:write(vmq_trie_node,
                         TrieNode#trie_node{
                           topic=Topic,
                           vclock=unsplit_vclock:increment(node(),
                                                           VClock)}, write);
        [#trie_node{topic=Topic}] ->
            ignore;
        [] ->
            %add trie path
            [trie_add_path(Triple) || Triple <- emqtt_topic:triples(Topic)],
            %add last node
            mnesia:write(vmq_trie_node, #trie_node{node_id=Topic,
                                                   topic=Topic}, write)
    end.

-spec del_subscriber(topic() | '_' ,client_id()) -> ok.
del_subscriber(Topic, ClientId) ->
    transaction(fun() -> del_subscriber_tx(Topic, ClientId) end).

-spec del_subscriber_tx(topic() | '_' ,client_id()) -> ok.
del_subscriber_tx(Topic, ClientId) ->
    Objs = mnesia:match_object(vmq_subscriber,
                               #subscriber{topic=Topic,
                                           client=ClientId, _='_'}, write),
    lists:foreach(fun(#subscriber{topic=T} = Obj) ->
                          mnesia:delete_object(vmq_subscriber, Obj, write),
                          del_topic(T, node())
                  end, Objs).

-spec remap_subscriptions_tx(client_id()) -> ok.
remap_subscriptions_tx(ClientId) ->
    Objs = mnesia:match_object(vmq_subscriber,
                               #subscriber{client=ClientId, _='_'}, write),
    Node = node(),
    lists:foreach(
      fun(#subscriber{topic=T, qos=Qos, node=N} = Obj) ->
              case N of
                  Node -> ignore;
                  _ ->
                      %% TODO: we add the subscriber before deleting the old
                      %% one,if we get a route request in between for this topic
                      %% we might deliver the message to the old and new nodes
                      %% resulting in duplicate messages delivered from the
                      %% message stores residing on both nodes. We prefer to
                      %% deliver duplicates instead of losing messages. The
                      %% session fsm has currently no way to detect that it is
                      %% a duplicate message, this should be improved!
                      add_subscriber_tx(T, Qos, ClientId),
                      mnesia:delete_object(vmq_subscriber, Obj, write),
                      del_topic(T, N, [Obj#subscriber{node=Node}])
              end
      end, Objs).

-spec del_topic(topic(), atom()) -> any().
del_topic(Topic, Node) ->
    del_topic(Topic, Node, []).
del_topic(Topic, Node, Excl) ->
    case mnesia:wread({vmq_subscriber, Topic}) of
        Excl ->
            TopicRec = #topic{name=Topic, node=Node},
            mnesia:delete_object(vmq_trie_topic, TopicRec, write),
            case mnesia:wread({vmq_trie_topic, Topic}) of
                [] -> trie_delete(Topic);
                _ -> ignore
            end;
        _ ->
            ok
    end.

-spec trie_delete(topic()) -> any().
trie_delete(Topic) ->
    case mnesia:wread({vmq_trie_node, Topic}) of
        [#trie_node{edge_count=0}] ->
            mnesia:delete({vmq_trie_node, Topic}),
            trie_delete_path(lists:reverse(emqtt_topic:triples(Topic)));
        [#trie_node{vclock=VClock} = TrieNode] ->
            mnesia:write(vmq_trie_node,
                         TrieNode#trie_node{
                           topic=Topic,
                           vclock=unsplit_vclock:increment(node(), VClock)},
                         write);
        [] ->
            ignore
    end.

-spec trie_match(maybe_improper_list()) -> any().
trie_match(Words) ->
    trie_match(root, Words, []).

-spec trie_match(_, maybe_improper_list(), _) -> any().
trie_match(NodeId, [], ResAcc) ->
    mnesia:read(vmq_trie_node, NodeId) ++ 'trie_match_#'(NodeId, ResAcc);

trie_match(NodeId, [W|Words], ResAcc) ->
    lists:foldl(
      fun(WArg, Acc) ->
              case mnesia:read(vmq_trie,
                               #trie_edge{node_id=NodeId, word=WArg}) of
                  [#trie{node_id=ChildId}] -> trie_match(ChildId, Words, Acc);
                  [] -> Acc
              end
      end, 'trie_match_#'(NodeId, ResAcc), [W, "+"]).

-spec 'trie_match_#'(_, _) -> any().
'trie_match_#'(NodeId, ResAcc) ->
    case mnesia:read(vmq_trie, #trie_edge{node_id=NodeId, word="#"}) of
        [#trie{node_id=ChildId}] ->
            mnesia:read(vmq_trie_node, ChildId) ++ ResAcc;
        [] ->
            ResAcc
    end.

-spec trie_add_path({'root' | [any()], [any()], [any()]}) -> any().
trie_add_path({Node, Word, Child}) ->
    Edge = #trie_edge{node_id=Node, word=Word},
    case mnesia:read(vmq_trie_node, Node) of
        [TrieNode = #trie_node{edge_count=Count, vclock=VClock}] ->
            case mnesia:read(vmq_trie, Edge) of
                [] ->
                    mnesia:write(vmq_trie_node,
                                 TrieNode#trie_node{
                                   edge_count=Count+1,
                                   vclock=unsplit_vclock:increment(node(),
                                                                   VClock)
                                  }, write),
                    mnesia:write(vmq_trie, #trie{edge=Edge,
                                                 node_id=Child}, write);
                [_] ->
                    ok
            end;
        [] ->
            mnesia:write(vmq_trie_node,
                         #trie_node{node_id=Node, edge_count=1}, write),
            mnesia:write(vmq_trie, #trie{edge=Edge, node_id=Child}, write)
    end.

-spec trie_delete_path([{'root' | [any()], [any()], [any()]}]) -> any().
trie_delete_path([]) ->
    ok;
trie_delete_path([{NodeId, Word, _}|RestPath]) ->
    Edge = #trie_edge{node_id=NodeId, word=Word},
    mnesia:delete({vmq_trie, Edge}),
    case mnesia:wread({vmq_trie_node, NodeId}) of
        [#trie_node{edge_count=1, topic=undefined}] ->
            mnesia:delete({vmq_trie_node, NodeId}),
            trie_delete_path(RestPath);
        [TrieNode=#trie_node{edge_count=1, topic=_, vclock=VClock}] ->
            mnesia:write(vmq_trie_node,
                         TrieNode#trie_node{
                           edge_count=0,
                           vclock=unsplit_vclock:increment(node(), VClock)
                          }, write);
        [TrieNode=#trie_node{
                     edge_count=Count, vclock=VClock}] ->
            mnesia:write(vmq_trie_node,
                         TrieNode#trie_node{
                           edge_count=Count-1,
                           vclock=unsplit_vclock:increment(node(), VClock)
                          }, write);
        [] ->
            throw({not_found, NodeId})
    end.


-spec get_client_pid(_) -> {'error','not_found'} | {'ok',_}.
get_client_pid(ClientId) ->
    case ets:lookup(vmq_session, ClientId) of
        [#session{pid=Pid}] when is_pid(Pid) ->
            {ok, Pid};
        _ ->
            {error, not_found}
    end.

-spec get_queue_pid(_) -> {error, not_found} | {ok, pid()}.
get_queue_pid(ClientId) ->
    case ets:lookup(vmq_session, ClientId) of
        [#session{queue_pid=Pid}] when is_pid(Pid) ->
            {ok, Pid};
        _ ->
            {error, not_found}
    end.

-spec total_clients() -> non_neg_integer().
total_clients() ->
    ets:info(vmq_session, size).

-spec retained() -> non_neg_integer().
retained() ->
    mnesia:table_info(vmq_retain, size).


message_queue_monitor() ->
    {messages, Messages} = erlang:process_info(whereis(vmq_reg), messages),
    case length(Messages) of
        L when L > 100 ->
            lager:warning("vmq_reg overloaded ~p messages waiting", [L]);
        _ ->
            ok
    end,
    timer:sleep(1000),
    message_queue_monitor().

-spec remove_expired_clients(pos_integer()) -> ok.
remove_expired_clients(ExpiredSinceSeconds) ->
    ExpiredSince = epoch() - ExpiredSinceSeconds,
    remove_expired_clients_(ets:select(vmq_session,
                                       [{#session{last_seen='$1',
                                                  client_id='$2',
                                                  pid='$3', _='_'},
                                             [{'<',
                                               '$1',
                                               ExpiredSince}],
                                         [['$2', '$3']]}], 100)).

remove_expired_clients_({[[ClientId, Pid]|Rest], Cont}) ->
    case is_process_alive(Pid) of
        false ->
            transaction(fun() -> del_subscriber_tx('_', ClientId) end,
                        fun(_) ->
                                vmq_msg_store:clean_session(ClientId),
                                vmq_systree:incr_expired_clients(),
                                vmq_systree:decr_inactive_clients()
                        end);
        true ->
            ok
    end,
    remove_expired_clients_({Rest, Cont});
remove_expired_clients_({[], Cont}) ->
    remove_expired_clients_(ets:select(Cont));
remove_expired_clients_('$end_of_table') ->
    ok.

set_monitors() ->
    set_monitors(ets:select(vmq_session,
                            [{#session{pid='$1', client_id='$2', _='_'}, [],
                              [['$1', '$2']]}], 100)).

set_monitors('$end_of_table') ->
    ok;
set_monitors({PidsAndIds, Cont}) ->
    _ = [begin
             MRef = erlang:monitor(process, Pid),
             ets:insert(vmq_session_mons, {MRef, ClientId})
         end || [Pid, ClientId] <- PidsAndIds],
    set_monitors(ets:select(Cont)).


fix_session_table() ->
    fix_session_table(ets:select(vmq_session,
                                 [{#session{client_id='$1', pid='$2', _='_'},
                                   [], [['$1', '$2']]}], 100), []).
fix_session_table({[[ClientId, Pid]|Rest], Cont}, Acc) ->
    NewAcc =
    case get_client_pid(ClientId) of
        Pid -> Acc;
        _ -> [ClientId|Acc]
    end,
    fix_session_table({Rest, Cont}, NewAcc);
fix_session_table({[], Cont}, Acc) ->
    fix_session_table(ets:select(Cont), Acc);
fix_session_table('$end_of_table', Acc) ->
    _ = [begin
             case ets:lookup(vmq_session, ClientId) of
                 [#session{clean=true}] ->
                     transaction(fun() -> del_subscriber_tx('_', ClientId) end,
                                 fun(_) ->
                                         ets:delete(vmq_session, ClientId),
                                         vmq_msg_store:clean_session(ClientId)
                                 end);
                 [Session] ->
                     ets:insert(vmq_session,
                                Session#session{pid=undefined,
                                                queue_pid=undefined,
                                                monitor=undefined})
             end
         end || ClientId <- Acc],
    ok.




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% GEN_SERVER CALLBACKS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec init([]) -> {ok, state()}.
init([]) ->
    ets:new(vmq_session_mons, [named_table]),
    spawn_link(fun() -> message_queue_monitor() end),
    fix_session_table(),
    process_flag(trap_exit, true),
    process_flag(priority, high),
    set_monitors(),
    {ok, #state{}}.

-spec handle_call(_, _, []) -> {reply, ok, []}.
handle_call({monitor, SessionPid, QPid, ClientId, CleanSession},
            _From, State) ->
    case ets:lookup(vmq_session, ClientId) of
        [] -> ok;
        [#session{pid=undefined, monitor=undefined, clean=true}] ->
            ok;
        [#session{pid=undefined, monitor=undefined, clean=false}] ->
            vmq_systree:decr_inactive_clients();
        %% ------------------------
        %% only in a race condition
        %% vvvvvvvvvvvvvvvvvvvvvvvv
        [#session{pid=OldSessionPid, monitor=MRef, clean=OldClean}] ->
            demonitor(MRef, [flush]),
            ets:delete(vmq_session_mons, MRef),
            case OldClean of
                true ->
                    del_subscriber_fun(ClientId),
                    vmq_msg_store:clean_session(ClientId);
                false ->
                    vmq_systree:incr_inactive_clients()
            end,
            disconnect_client(OldSessionPid)
    end,

    Monitor = monitor(process, SessionPid),
    Session = #session{client_id=ClientId, pid=SessionPid, queue_pid=QPid,
                       monitor=Monitor, last_seen=epoch(), clean=CleanSession},
    ets:insert(vmq_session_mons, {Monitor, ClientId}),
    ets:insert(vmq_session, Session),
    {reply, ok, State}.

-spec handle_cast(_, []) -> {noreply, []}.
handle_cast(_Req, State) ->
    {noreply, State}.

-spec handle_info(_, []) -> {noreply, []}.
handle_info({'DOWN', MRef, process, _Pid, _Reason}, State) ->
    case ets:lookup(vmq_session_mons, MRef) of
        [] ->
            ignore;
        [{_, ClientId}] ->
            ets:delete(vmq_session_mons, MRef),
            unregister_client(ClientId)
    end,
    {noreply, State}.

-spec terminate(_, []) -> ok.
terminate(_Reason, _State) ->
    SessionFile = vmq_config:get_env(session_dump_file, "session.dump"),
    case ets:tab2file(vmq_session,
                      SessionFile,
                      [{extended_info, [md5sum, object_count]}]) of
        ok ->
            ok;
        {error, Reason} ->
            lager:error("can't dump session file due to ~p", [Reason])
    end.

-spec code_change(_, _, _) -> {ok, _}.
code_change(_OldVSN, State, _Extra) ->
    {ok, State}.

epoch() ->
    {Mega, Sec, _} = os:timestamp(),
    (Mega * 1000000 + Sec).


unregister_client(ClientId) ->
    case ets:lookup(vmq_session, ClientId) of
        [#session{client_id=ClientId, clean=true}] ->
            del_subscriber_fun(ClientId),
            ets:delete(vmq_session, ClientId),
            vmq_msg_store:clean_session(ClientId);
        [#session{clean=false} = Obj] ->
            ets:insert(vmq_session,
                       Obj#session{pid=undefined,
                                   queue_pid=undefined,
                                   monitor=undefined,
                                   last_seen=epoch()}),
            vmq_systree:incr_inactive_clients();
        [] ->
            {error, not_found}
    end.

del_subscriber_fun(ClientId) ->
    transaction(fun() -> del_subscriber_tx('_', ClientId) end).

transaction(TxFun) ->
    transaction(TxFun, fun(Ret) -> Ret end).

transaction(TxFun, SuccessFun) ->
    %% Making this a sync_transaction allows us to use dirty_read
    %% elsewhere and get a consistent result even when that read
    %% executes on a different node.
    case jobs:ask(mnesia_tx_queue) of
        {ok, JobId} ->
            try
                case sync_transaction_(TxFun) of
                    {sync, {atomic, Result}} ->
                        %% Rabbit enforces that data is synced to disk by
                        %% waiting for disk_log:sync(latest_log),
                        SuccessFun(Result);
                    {sync, {aborted, Reason}} -> throw({error, Reason});
                    {atomic, Result} -> SuccessFun(Result);
                    {aborted, Reason} -> throw({error, Reason});
                    {error, overloaded} -> {error, overloaded}
                end
            after
                jobs:done(JobId)
            end;
        {error, rejected} ->
            {error, overloaded}
    end.

sync_transaction_(TxFun) ->
    case mnesia:is_transaction() of
        false ->
            DiskLogBefore = mnesia_dumper:get_log_writes(),
            Res = mnesia:sync_transaction(TxFun),
            DiskLogAfter  = mnesia_dumper:get_log_writes(),
            case DiskLogAfter == DiskLogBefore of
                true  -> Res;
                false -> {sync, Res}
            end;
        true  ->
            mnesia:sync_transaction(TxFun)
    end.
