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

-export([start_link/0,
         subscribe/3,
         unsubscribe/3,
         subscriptions/1,
         publish/6,
         register_client/2,
         unregister_client/2,
         disconnect_client/1,
         match/1,
         remove_expired_clients/1,
         total_clients/0]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([register_client__/3,
         route/7]).

-export([vmq_table_defs/0,
         reset_all_tables/1]).

-export([direct_plugin_exports/1]).

-hook({auth_on_subscribe, only, 3}).
-hook({on_subscribe, all, 3}).
-hook({on_unsubscribe, all, 3}).
-hook({filter_subscribers, every, 5}).

-record(session, {client_id, node, pid, monitor, last_seen, clean}).

-spec start_link() -> {ok, pid()} | ignore | {error, atom()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec subscribe(username() | plugin_id(),client_id(),[{topic(), qos()}]) ->
    ok | {error, not_allowed | [any(),...]}.
subscribe(User, ClientId, Topics) ->
    vmq_cluster:if_ready(fun subscribe_/3, [User, ClientId, Topics]).

-spec subscribe_(username() | plugin_id(),client_id(),[{topic(), qos()}]) ->
    'ok' | {'error','not_allowed' | [any(),...]}.
subscribe_(User, ClientId, Topics) ->
    case vmq_hook:only(auth_on_subscribe, [User, ClientId, Topics]) of
        ok ->
            vmq_hook:all(on_subscribe, [User, ClientId, Topics]),
            case subscribe_tx(ClientId, Topics, []) of
                [] ->
                    ok;
                Errors ->
                    {error, Errors}
            end;
        not_found ->
            {error, not_allowed}
    end.

-spec subscribe_tx(client_id(),[{topic(),qos()}],[any()]) -> [any()].
subscribe_tx(_, [], Errors) -> Errors;
subscribe_tx(ClientId, [{Topic, Qos}|Rest], Errors) ->
    case mnesia:transaction(fun add_subscriber/3, [Topic, Qos, ClientId]) of
        {atomic, _} ->
            vmq_systree:incr_subscription_count(),
            vmq_msg_store:deliver_retained(self(), Topic, Qos),
            subscribe_tx(ClientId, Rest, Errors);
        {aborted, Reason} ->
            subscribe_tx(ClientId, Rest, [Reason|Errors])
    end.

-spec unsubscribe(username() | plugin_id(),client_id(),[topic()]) -> any().
unsubscribe(User, ClientId, Topics) ->
    vmq_cluster:if_ready(fun unsubscribe_/3, [User, ClientId, Topics]).

-spec unsubscribe_(username() | plugin_id(),client_id(),[topic()]) -> 'ok'.
unsubscribe_(User, ClientId, Topics) ->
    lists:foreach(fun(Topic) ->
                          {atomic, _} = del_subscriber(Topic, ClientId),
                          vmq_systree:decr_subscription_count()
                  end, Topics),
    vmq_hook:all(on_unsubscribe, [User, ClientId, Topics]),
    ok.

-spec subscriptions(routing_key()) -> [{client_id(), qos()}].
subscriptions(RoutingKey) ->
    subscriptions(match(RoutingKey), []).

-spec subscriptions([#topic{}],_) -> [{client_id(), qos()}].
subscriptions([], Acc) -> Acc;
subscriptions([#topic{name=Topic, node=Node}|Rest], Acc) when Node == node() ->
    subscriptions(Rest,
                  lists:foldl(
                    fun
                        (#subscriber{client=ClientId, qos=Qos}, Acc1) when Qos > 0 ->
                            [{ClientId, Qos}|Acc1];
                              (_, Acc1) ->
                            Acc1
                    end, Acc, mnesia:dirty_read(vmq_subscriber, Topic)));
subscriptions([_|Rest], Acc) ->
    subscriptions(Rest, Acc).

-spec register_client(client_id(),flag()) -> ok | {error, _}.
register_client(ClientId, CleanSession) ->
    vmq_cluster:if_ready(fun register_client_/2, [ClientId, CleanSession]).

-spec register_client_(client_id(),flag()) -> 'ok'.
register_client_(ClientId, CleanSession) ->
    Nodes = vmq_cluster:nodes(),
    lists:foreach(fun(Node) when Node == node() ->
                          ok = register_client__(self(), ClientId, CleanSession);
                     (Node) ->
                          rpc:call(Node, ?MODULE, register_client__, [self(), ClientId, CleanSession])
                  end, Nodes).

-spec register_client__(pid(),client_id(),flag()) -> ok | {error, timeout}.
register_client__(ClientPid, ClientId, CleanSession) ->
    {Ret, MaybeMonitor} =
    case mnesia:dirty_read(vmq_session, ClientId) of
        [] ->
            {new_client, undefined};
        [#session{pid=OldPid, monitor=OldRef, last_seen=LastSeen}]
          when is_pid(OldPid) and is_reference(OldRef) ->
            case epoch() of
                LastSeen ->
                    %% multiple connects in same second
                    timer:sleep(1000);
                _ ->
                    ok
            end,
            disconnect_client(OldPid),
            {known_client, OldRef};
        [_] ->
            %% persisted client reconnected
            {known_client, undefined}
    end,
    case is_process_alive(ClientPid) of
        true ->
            MRef = gen_server:call(?MODULE, {monitor, ClientPid, MaybeMonitor}, infinity),
            {atomic, _} =
            mnesia:transaction(
              fun() ->
                      Session = #session{client_id=ClientId, node=node(), pid=ClientPid,
                                         monitor=MRef, last_seen=epoch(), clean=CleanSession},
                      mnesia:write(vmq_session, Session, write),
                      case CleanSession of
                          true ->
                              del_subscriber_tx('_', ClientId);
                          _ ->
                              ok
                      end
              end),
            case CleanSession of
                true ->
                    vmq_msg_store:clean_session(ClientId);
                false ->
                    vmq_msg_store:deliver_from_store(ClientId, ClientPid)
            end,
            case Ret of
                known_client ->
                    vmq_systree:decr_inactive_clients();
                _ ->
                    ok
            end,
            ok;
        false ->
            lager:warning("Client ~p died during registration~n", [ClientPid]),
            ok
    end.

-spec unregister_client(client_id(),flag()) -> ok | {error, _}.
unregister_client(ClientId, CleanSession) ->
    vmq_cluster:if_ready(fun unregister_client_/2, [ClientId, CleanSession]).

-spec unregister_client_(client_id(),flag()) -> 'ok'.
unregister_client_(ClientId, CleanSession) ->
    case mnesia:dirty_read(vmq_session, ClientId) of
        [] ->
            {error, not_found};
        [#session{monitor=Ref} = Obj] ->
            ok = gen_server:call(?MODULE, {demonitor, Ref}, infinity),
            {atomic, _} = mnesia:transaction(fun() -> unregister_client_tx(Obj) end),
            case CleanSession of
                true ->
                    vmq_msg_store:clean_session(ClientId);
                false ->
                    vmq_systree:incr_inactive_clients()
            end,
            ok
    end.

unregister_client_tx(#session{client_id=ClientId, clean=CleanSession} = Obj) ->
    case CleanSession of
        true ->
            mnesia:delete({vmq_session, ClientId}),
            del_subscriber_tx('_', ClientId);
        false ->
            mnesia:write(vmq_session, Obj#session{pid=undefined,
                                                  monitor=undefined,
                                                  last_seen=epoch()}, write)
    end.

-spec publish(username() | plugin_id(),client_id(), undefined | msg_ref(),
              routing_key(),binary(),flag()) -> 'ok' | {'error',_}.
publish(User, ClientId, MsgId, RoutingKey, Payload, IsRetain)
  when is_list(RoutingKey) and is_binary(Payload) ->
    make_request(fun publish/7, [User, ClientId, MsgId, RoutingKey, Payload, IsRetain]).

make_request(Fun, Args) when is_function(Fun), is_list(Args) ->
    Ref = make_ref(),
    Caller = {self(), Ref},
    ReqF = fun() ->
                   exit({Ref, apply(Fun, [Caller|Args])})
           end,
    try spawn_monitor(ReqF) of
        {_, MRef} ->
            receive
                Ref ->
                    erlang:demonitor(MRef, [flush]),
                    ok;
                {'DOWN', MRef, process, Reason} ->
                    {error, Reason}
            end
    catch
        error: system_limit = E ->
            {error, E}
    end.

%publish to cluster node.
-spec publish({pid(), reference()}, username() | plugin_id(),client_id(),msg_id(),
              routing_key(),payload(),flag()) -> ok | {error, _}.
publish(Caller, User, ClientId, MsgId, RoutingKey, Payload, IsRetain) ->
    MatchedTopics = match(RoutingKey),
    case IsRetain of
        true ->
            vmq_cluster:if_ready(fun publish_/8, [User, ClientId, MsgId, RoutingKey, Payload, IsRetain, MatchedTopics, Caller]);
        false ->
            case check_single_node(node(), MatchedTopics, length(MatchedTopics)) of
                true ->
                    %% in case we have only subscriptions on one single node
                    %% we can deliver the messages even in case of network partitions
                    lists:foreach(fun(#topic{name=Name}) ->
                                          route(User, ClientId, MsgId, Name, RoutingKey, Payload, IsRetain)
                                  end, MatchedTopics),
                    {CallerPid, CallerRef} = Caller,
                    CallerPid ! CallerRef,
                    ok;
                false ->
                    vmq_cluster:if_ready(fun publish__/8, [User, ClientId, MsgId, RoutingKey, Payload, IsRetain, MatchedTopics, Caller])
            end
    end.

-spec publish_(username() | plugin_id(),client_id(),msg_id(),
               routing_key(),payload(),'true',[#topic{}],{pid(), reference()}) -> 'ok'.
publish_(User, ClientId, MsgId, RoutingKey, Payload, IsRetain = true, MatchedTopics, Caller) ->
    ok = vmq_msg_store:retain_action(User, ClientId, RoutingKey, Payload),
    publish__(User, ClientId, MsgId, RoutingKey, Payload, IsRetain, MatchedTopics, Caller).

-spec publish__(username() | plugin_id(),client_id(),msg_id(),
                routing_key(),payload(),flag(),[#topic{}],{pid(), reference()}) -> 'ok'.
publish__(User, ClientId, MsgId, RoutingKey, Payload, IsRetain, MatchedTopics, Caller) ->
    {CallerPid, CallerRef} = Caller,
    CallerPid ! CallerRef,
    lists:foreach(
      fun(#topic{name=Name, node=Node}) ->
              case Node == node() of
                  true ->
                      route(User, ClientId, MsgId, Name, RoutingKey, Payload, IsRetain);
                  false ->
                      rpc:call(Node, ?MODULE, route, [User, ClientId, MsgId, Name, RoutingKey, Payload, IsRetain])
              end
      end, MatchedTopics).


-spec check_single_node(atom(),[#topic{}],integer()) -> boolean().
check_single_node(Node, [#topic{node=Node}|Rest], Acc) ->
    check_single_node(Node, Rest, Acc -1);
check_single_node(Node, [_|Rest], Acc) ->
    check_single_node(Node, Rest, Acc);
check_single_node(_, [], 0) -> true;
check_single_node(_, [], _) -> false.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% RPC Callbacks / Maintenance
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec disconnect_client(client_id() | pid()) -> 'ok' | {'error','not_found'}.
disconnect_client(ClientPid) when is_pid(ClientPid) ->
    vmq_session:disconnect(ClientPid),
    ok;
disconnect_client(ClientId) ->
    wait_until_unregistered(ClientId, false).

-spec wait_until_unregistered(client_id(),boolean()) -> {'error','not_found'}.
wait_until_unregistered(ClientId, DisconnectRequested) ->
    case get_client_pid(ClientId) of
        {ok, ClientPid} ->
            case is_process_alive(ClientPid) of
                true when not DisconnectRequested->
                    disconnect_client(ClientPid),
                    wait_until_unregistered(ClientId, true);
                _ ->
                    timer:sleep(100),
                    wait_until_unregistered(ClientId, DisconnectRequested)
            end;
        E -> E
    end.


%route locally, should only be called by publish
-spec route(username() | plugin_id(),client_id(),msg_id(),topic(),
            routing_key(),payload(),flag()) -> 'ok'.
route(SendingUser, SendingClientId, MsgId, Topic, RoutingKey, Payload, IsRetain) ->
    Subscribers = mnesia:dirty_read(vmq_subscriber, Topic),
    FilteredSubscribers = vmq_hook:every(filter_subscribers, Subscribers,
                                         [SendingUser, SendingClientId,
                                          MsgId, RoutingKey, Payload]),
    lists:foreach(fun
                    (#subscriber{qos=Qos, client=ClientId}) when Qos > 0 ->
                          MaybeNewMsgId = vmq_msg_store:store(SendingUser, SendingClientId, MsgId, RoutingKey, Payload),
                          deliver(ClientId, RoutingKey, Payload, IsRetain, Qos, MaybeNewMsgId);
                    (#subscriber{qos=0, client=ClientId}) ->
                          deliver(ClientId, RoutingKey, Payload, IsRetain, 0, undefined)
                end, FilteredSubscribers).

-spec deliver(client_id(),routing_key(),payload(),flag(),
              qos(),msg_ref()) -> 'ok' | {'error','not_found'}.
deliver(_, _, <<>>, true, _, Ref) ->
    %% <<>> --> retain-delete action, we don't deliver the empty frame
    vmq_msg_store:deref(Ref),
    ok;
deliver(ClientId, RoutingKey, Payload, _, Qos, Ref) ->
    case get_client_pid(ClientId) of
        {ok, ClientPid} ->
            vmq_session:deliver(ClientPid, RoutingKey, Payload, Qos, false, false, Ref);
        _ when Qos > 0 ->
            vmq_msg_store:defer_deliver(ClientId, Qos, Ref),
            ok;
        _ ->
            ok
    end.

-spec match(routing_key()) -> [#topic{}].
match(Topic) when is_list(Topic) ->
    TrieNodes = mnesia:async_dirty(fun trie_match/1, [emqtt_topic:words(Topic)]),
    Names = [Name || #trie_node{topic=Name} <- TrieNodes, Name=/= undefined],
    lists:flatten([mnesia:dirty_read(vmq_trie_topic, Name) || Name <- Names]).

-spec vmq_table_defs() -> [{atom(), [{atom(), any()}]}].
vmq_table_defs() ->
    [
     {vmq_trie,[
       {record_name, trie},
       {attributes, record_info(fields, trie)},
       {disc_copies, [node()]},
       {match, #trie{_='_'}}]},
     {vmq_trie_node,[
       {record_name, trie_node},
       {attributes, record_info(fields, trie_node)},
       {disc_copies, [node()]},
       {match, #trie_node{_='_'}}]},
     {vmq_trie_topic,[
       {record_name, topic},
       {type, bag},
       {attributes, record_info(fields, topic)},
       {disc_copies, [node()]},
       {match, #topic{_='_'}}]},
     {vmq_subscriber,[
        {record_name, subscriber},
        {type, bag},
        {attributes, record_info(fields, subscriber)},
        {disc_copies, [node()]},
        {match, #subscriber{_='_'}}]},
     {vmq_session, [
        {record_name, session},
        {attributes, record_info(fields, session)},
        {disc_copies, [node()]},
        {match, #session{_='_'}}]}
].


-spec reset_all_tables([]) -> ok.
reset_all_tables([]) ->
    %% called using vmq-admin, mainly for test purposes
    %% you don't want to call this during production
    [reset_table(T) || {T,_}<- vmq_table_defs()],
    ok.

-spec reset_table(atom()) -> ok.
reset_table(Tab) ->
    Keys = mnesia:dirty_all_keys(Tab),
    {atomic, ok} = mnesia:transaction(
                     fun() ->
                             lists:foreach(fun(Key) ->
                                                   mnesia:dirty_delete(Tab, Key)
                                           end, Keys)
                     end),
    ok.



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

    RegisterFun =
    fun() ->
            wait_til_ready(),
            CallingPid = self(),
            register_client__(CallingPid, ClientId(CallingPid), true)
    end,

    PublishFun =
    fun(Topic, Payload) ->
            wait_til_ready(),
            CallingPid = self(),
            User = {plugin, Mod, CallingPid},
            ok = publish(User, ClientId(CallingPid), undefined, Topic, Payload, false),
            ok
    end,

    SubscribeFun =
    fun(Topic) ->
            wait_til_ready(),
            CallingPid = self(),
            User = {plugin, Mod, CallingPid},
            ok = subscribe(User, ClientId(CallingPid), [{Topic, 0}]),
            ok
    end,
    {RegisterFun, PublishFun, SubscribeFun}.

-spec add_subscriber(topic(),qos(),client_id()) -> ok | ignore | abort.
add_subscriber(Topic, Qos, ClientId) ->
    mnesia:write(vmq_subscriber, #subscriber{topic=Topic, qos=Qos, client=ClientId}, write),
    mnesia:write(vmq_trie_topic, emqtt_topic:new(Topic), write),
    case mnesia:read(vmq_trie_node, Topic) of
        [TrieNode=#trie_node{topic=undefined}] ->
            mnesia:write(vmq_trie_node, TrieNode#trie_node{topic=Topic}, write);
        [#trie_node{topic=Topic}] ->
            ignore;
        [] ->
            %add trie path
            [trie_add_path(Triple) || Triple <- emqtt_topic:triples(Topic)],
            %add last node
            mnesia:write(vmq_trie_node, #trie_node{node_id=Topic, topic=Topic}, write)
    end.


-spec del_subscriber(topic() | '_' ,client_id()) -> {'aborted',_} | {'atomic',ok}.
del_subscriber(Topic, ClientId) ->
    mnesia:transaction(fun del_subscriber_tx/2, [Topic, ClientId]).

-spec del_subscriber_tx(topic() | '_' ,client_id()) -> ok.
del_subscriber_tx(Topic, ClientId) ->
    Objs = mnesia:match_object(vmq_subscriber, #subscriber{topic=Topic, client=ClientId, _='_'}, read),
    lists:foreach(fun(#subscriber{topic=T} = Obj) ->
                          mnesia:delete_object(vmq_subscriber, Obj, write),
                          del_topic(T)
                  end, Objs).

-spec del_topic(topic()) -> any().
del_topic(Topic) ->
    case mnesia:read(vmq_subscriber, Topic) of
        [] ->
            TopicRec = emqtt_topic:new(Topic),
            mnesia:delete_object(vmq_trie_topic, TopicRec, write),
            case mnesia:read(vmq_trie_topic, Topic) of
                [] -> trie_delete(Topic);
                _ -> ignore
            end;
        _ ->
            ok
    end.

-spec trie_delete(maybe_improper_list()) -> any().
trie_delete(Topic) ->
    case mnesia:read(vmq_trie_node, Topic) of
        [#trie_node{edge_count=0}] ->
            mnesia:delete({vmq_trie_node, Topic}),
            trie_delete_path(lists:reverse(emqtt_topic:triples(Topic)));
        [TrieNode] ->
            mnesia:write(vmq_trie_node, TrieNode#trie_node{topic=Topic}, write);
        [] ->
            ignore
    end.

-spec trie_match(maybe_improper_list()) -> any().
trie_match(Words) ->
    trie_match(root, Words, []).

-spec trie_match(_,maybe_improper_list(),_) -> any().
trie_match(NodeId, [], ResAcc) ->
    mnesia:read(vmq_trie_node, NodeId) ++ 'trie_match_#'(NodeId, ResAcc);

trie_match(NodeId, [W|Words], ResAcc) ->
    lists:foldl(
      fun(WArg, Acc) ->
              case mnesia:read(vmq_trie, #trie_edge{node_id=NodeId, word=WArg}) of
                  [#trie{node_id=ChildId}] -> trie_match(ChildId, Words, Acc);
                  [] -> Acc
              end
      end, 'trie_match_#'(NodeId, ResAcc), [W, "+"]).

-spec 'trie_match_#'(_,_) -> any().
'trie_match_#'(NodeId, ResAcc) ->
    case mnesia:read(vmq_trie, #trie_edge{node_id=NodeId, word="#"}) of
        [#trie{node_id=ChildId}] ->
            mnesia:read(vmq_trie_node, ChildId) ++ ResAcc;
        [] ->
            ResAcc
    end.

-spec trie_add_path({'root' | [any()],[any()],[any()]}) -> any().
trie_add_path({Node, Word, Child}) ->
    Edge = #trie_edge{node_id=Node, word=Word},
    case mnesia:read(vmq_trie_node, Node) of
        [TrieNode = #trie_node{edge_count=Count}] ->
            case mnesia:read(vmq_trie, Edge) of
                [] ->
                    mnesia:write(vmq_trie_node, TrieNode#trie_node{edge_count=Count+1}, write),
                    mnesia:write(vmq_trie, #trie{edge=Edge, node_id=Child}, write);
                [_] ->
                    ok
            end;
        [] ->
            mnesia:write(vmq_trie_node, #trie_node{node_id=Node, edge_count=1}, write),
            mnesia:write(vmq_trie, #trie{edge=Edge, node_id=Child}, write)
    end.

-spec trie_delete_path([{'root' | [any()],[any()],[any()]}]) -> any().
trie_delete_path([]) ->
    ok;
trie_delete_path([{NodeId, Word, _}|RestPath]) ->
    Edge = #trie_edge{node_id=NodeId, word=Word},
    mnesia:delete({vmq_trie, Edge}),
    case mnesia:read(vmq_trie_node, NodeId) of
        [#trie_node{edge_count=1, topic=undefined}] ->
            mnesia:delete({vmq_trie_node, NodeId}),
            trie_delete_path(RestPath);
        [TrieNode=#trie_node{edge_count=1, topic=_}] ->
            mnesia:write(vmq_trie_node, TrieNode#trie_node{edge_count=0}, write);
        [TrieNode=#trie_node{edge_count=Count}] ->
            mnesia:write(vmq_trie_node, TrieNode#trie_node{edge_count=Count-1}, write);
        [] ->
            throw({notfound, NodeId})
    end.


-spec get_client_pid(_) -> {'error','not_found'} | {'ok',_}.
get_client_pid(ClientId) ->
    case mnesia:dirty_read(vmq_session, ClientId) of
        [#session{pid=Pid}] when is_pid(Pid) ->
            {ok, Pid};
        _ ->
            {error, not_found}
    end.

-spec cleanup_client_(client_id()) -> {'atomic','ok'}.
cleanup_client_(ClientId) ->
    vmq_msg_store:clean_session(ClientId),
    {atomic, ok} = del_subscriber('_', ClientId).

-spec remove_expired_clients(pos_integer()) -> ok.
remove_expired_clients(ExpiredSinceSeconds) ->
    ExpiredSince = epoch() - ExpiredSinceSeconds,
    Node = node(),
    MaybeCleanups= mnesia:dirty_select(vmq_session, [{#session{last_seen='$1',node=Node,
                                                               client_id='$2', pid='$3', _='_'},
                                                      [{'<', '$1', ExpiredSince}], [['$2', '$3']]}]),
    Cleanups = [ClientId || [ClientId, Pid] <- MaybeCleanups,
                            (Pid == undefined) orelse (is_process_alive(Pid) == false)],
    lists:foreach(fun(ClientId) ->
                          {atomic, ok} = cleanup_client_(ClientId),
                          {atomic, ok} = mnesia:transaction(
                                           fun() ->
                                                   mnesia:delete({vmq_session, ClientId})
                                           end),
                          vmq_systree:incr_expired_clients(),
                          vmq_systree:decr_inactive_clients()
                  end, Cleanups).

-spec total_clients() -> non_neg_integer().
total_clients() ->
    mnesia:table_info(vmq_session, size).


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



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% GEN_SERVER CALLBACKS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec init([any()]) -> {ok, []}.
init([]) ->
    Node = node(),
    spawn_link(fun() -> message_queue_monitor() end),

    {atomic, ok} =
    mnesia:transaction(
      fun() ->
              MatchHead = #session{
                             node=Node, pid='$1',
                             _='_'},
              Guard = {'/=', '$1', undefined},
              Result = '$_',
              Sessions = mnesia:select(vmq_session, [{MatchHead, [Guard], [Result]}]),
              lists:foreach(fun(#session{pid=Pid}=Obj) ->
                                    case is_process_alive(Pid) of
                                        true ->
                                            ok;
                                        false ->
                                            mnesia:write(vmq_session, Obj#session{pid=undefined, monitor=undefined}, write)
                                    end
                            end, Sessions)
      end),
    {ok, []}.

-spec handle_call(_, _, []) -> {reply, ok, []}.
handle_call({monitor, ClientPid, MaybeMonitor}, _From, State) ->
    case MaybeMonitor of
        undefined ->
            ok;
        _ ->
            demonitor(MaybeMonitor, [flush])
    end,
    MRef = monitor(process, ClientPid),
    {reply, MRef, State};
handle_call({demonitor, MaybeMonitor}, _From, State) ->
    case MaybeMonitor of
        undefined ->
            ok;
        _ ->
            demonitor(MaybeMonitor, [flush])
    end,
    {reply, ok, State}.

-spec handle_cast(_, []) -> {noreply, []}.
handle_cast(_Req, State) ->
    {noreply, State}.

-spec handle_info(_, []) -> {noreply, []}.
handle_info({'DOWN', MRef, process, _Pid, _Reason}, State) ->
    %% should only happen if a session does not propperly unregister
    {atomic, Ret} = mnesia:transaction(
                     fun() ->
                             case mnesia:match_object(vmq_session, #session{monitor=MRef, _='_'}, read) of
                                 [#session{client_id=ClientId, clean=CleanSession} = Obj] ->
                                     unregister_client_tx(Obj),
                                     {ok, {CleanSession, ClientId}};
                                 [] ->
                                     {error, not_found}
                             end
                     end),
    case Ret of
        {ok, {true, ClientId}} ->
            vmq_msg_store:clean_session(ClientId);
        {ok, {false, _ClientId}} ->
            vmq_systree:incr_inactive_clients();
        {error, not_found} ->
            io:format(user, "got DOWN event from ~p ~p ~p, but no matching session found", [_Pid, MRef, _Reason])
    end,
    {noreply, State}.

-spec terminate(_, []) -> ok.
terminate(_Reason, _State) ->
    ok.

-spec code_change(_, _, _) -> {ok, _}.
code_change(_OldVSN, State, _Extra) ->
    {ok, State}.

epoch() ->
    {Mega, Sec, _} = os:timestamp(),
    (Mega * 1000000 + Sec).
