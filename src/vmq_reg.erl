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
         subscribe/5,
         unsubscribe/4,
         register_client/4,
         register_session/2,
         %% used in vmq_session fsm handling
         publish/1,

         %% used in vmq_session:get_info/2 and vmq_session:list_sessions/1
         subscriptions_for_client/1,
         get_client_pid/1,

         %% used in vmq_msg_store:init/1
         subscriptions/1,

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
-export([teardown_session/2, publish_/2,
         register_client_/4]).

%% used by mnesia_cluster for table setup
-export([table_defs/0]).
%% used from vmq-admin script (deployed with the VerneMQ release)
-export([reset_all_tables/1]).
%% used from plugins
-export([direct_plugin_exports/1]).
%% used by vmq_reg_cache
-export([subscribe_subscriber_changes/0,
         fold_subscribers/2]).

-record(state, {}).
-record(session, {client_id, pid, queue_pid, monitor, last_seen, clean}).
-record(vmq_subscriber, {topic, qos, client, node}).
-record(retain, {words, routing_key, payload, vclock=unsplit_vclock:fresh()}).

-type state() :: #state{}.

-spec start_link() -> {ok, pid()} | ignore | {error, atom()}.
start_link() ->
    ets:new(vmq_session, [public,
                          bag,
                          named_table,
                          {keypos, 2},
                          {read_concurrency, true},
                          {write_concurrency, true}]),
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec subscribe(flag(), username() | plugin_id(), client_id(), pid(),
                [{topic(), qos()}]) -> ok | {error, not_allowed | [any(), ...]}.

subscribe(false, User, ClientId, QPid, Topics) ->
    %% trade availability for consistency
    vmq_cluster:if_ready(fun subscribe_/4, [User, ClientId, QPid, Topics]);
subscribe(true, User, ClientId, QPid, Topics) ->
    %% trade consistency for availability
    subscribe_(User, ClientId, QPid, Topics).

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
                                 vmq_plugin:all(incr_subscription_count, []),
                                 deliver_retained(ClientId, QPid, T, QoS)
                             end || {T, QoS} <- Topics],
                        ok
                end).

-spec unsubscribe(flag(), username() | plugin_id(),
                  client_id(), [topic()]) -> any().
unsubscribe(false, User, ClientId, Topics) ->
    %% trade availability for consistency
    vmq_cluster:if_ready(fun unsubscribe_/3, [User, ClientId, Topics]);
unsubscribe(true, User, ClientId, Topics) ->
    %% trade consistency for availability
    unsubscribe_(User, ClientId, Topics).

-spec unsubscribe_(username() | plugin_id(), client_id(), [topic()]) -> 'ok'.
unsubscribe_(User, ClientId, Topics) ->
    lists:foreach(fun(Topic) ->
                          del_subscriber(Topic, ClientId),
                          vmq_plugin:all(decr_subscription_count, [])
                  end, Topics),
    vmq_plugin:all(on_unsubscribe, [User, ClientId, Topics]),
    ok.

-spec subscriptions(routing_key()) -> [{client_id(), qos()}].
subscriptions(RoutingKey) ->
    subscriptions(fun(Obj, Acc) -> [Obj|Acc] end, [], RoutingKey).

-spec subscriptions(function(), any(), routing_key()) -> any().
subscriptions(FoldFun, Acc, RoutingKey) ->
    RegViews = vmq_config:get_env(reg_views, []),
    lists:flatten([begin
                       {NewAcc, _} = RV:fold(RoutingKey,
                                             fun subscriptions/2,
                                             {Acc, FoldFun}),
                       NewAcc
                   end || RV <- RegViews]).

subscriptions({Topic, Node}, {Acc, FoldFun}) when Node == node() ->
    NewAcc =
    lists:foldl(
      fun
          (#vmq_subscriber{client=ClientId, qos=Qos}, Acc1) ->
              FoldFun({ClientId, Qos}, Acc1);
          (_, Acc1) ->
              Acc1
      end, Acc, mnesia:dirty_read(vmq_subscriber, Topic)),
    {NewAcc, FoldFun};
subscriptions({_Topic, Node, ClientId, QoS, _Pid}, {Acc, FoldFun})
  when Node == node() ->
    {FoldFun({ClientId, QoS}, Acc), FoldFun};
subscriptions(_, Acc) ->
    Acc.

subscriptions_for_client(ClientId) ->
    Res = mnesia:dirty_match_object(vmq_subscriber,
                              #vmq_subscriber{client=ClientId, _='_'}),
    [{T, Q} || #vmq_subscriber{topic=T, qos=Q} <- Res].

-spec register_client(flag(), client_id(), pid(), flag()) -> ok | {error, _}.
register_client(false, ClientId, QPid, CleanSession) ->
    %% we don't allow multiple sessions using same client id
    register_client(ClientId, QPid, CleanSession);
register_client(true, ClientId, QPid, _CleanSession) ->
    %% we allow multiple sessions using same client id
    register_session(ClientId, QPid).

-spec register_client(client_id(), pid(), flag()) -> ok | {error, _}.
register_client(ClientId, QPid, CleanSession) ->
    case vmq_reg_leader:register_client(self(), QPid, ClientId, CleanSession) of
        ok when not CleanSession ->
            vmq_session_proxy_sup:start_delivery(QPid, ClientId),
            ok;
        R ->
            R
    end.

-spec register_session(client_id(), pid()) -> ok | {error, _}.
register_session(ClientId, QPid) ->
    %% register_session allows to have multiple clients connected
    %% with the same session_id (as oposed to register_client)
    SessionPid = self(),
    transaction(fun() -> remap_session_tx(ClientId) end,
                fun({error, Reason}) -> {error, Reason};
                   (_) ->
                        ok = gen_server:call(?MODULE,
                                             {monitor, SessionPid, QPid,
                                              ClientId, false, false},
                                             infinity),
                        vmq_session_proxy_sup:start_delivery(QPid, ClientId),
                        ok
                end).

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
                                   ClientId, CleanSession, true}, infinity).
-spec publish(msg()) -> 'ok' | {'error', _}.
publish(#vmq_msg{trade_consistency=true,
                 reg_view=RegModule,
                 routing_key=RoutingKey,
                 payload=Paylaod,
                 retain=IsRetain} = Msg) ->
    %% trade consistency for availability
    %% if the cluster is not consistent at the moment, it is possible
    %% that subscribers connected to other nodes won't get this message
    case IsRetain of
        true when Paylaod == <<>> ->
            %% retain delete action
            Words = emqtt_topic:words(RoutingKey),
            transaction(fun() -> mnesia:delete({vmq_retain, Words}) end);
        true ->
            %% retain set action
            case retain_msg(Msg) of
                {ok, RetainedMsg} ->
                    RegModule:fold(RoutingKey, fun publish_/2, RetainedMsg),
                    ok;
                {error, overloaded} ->
                    {error, overloaded}
            end;
        false ->
            RegModule:fold(RoutingKey, fun publish_/2, Msg)
    end;
publish(#vmq_msg{trade_consistency=false,
                 reg_view=RegModule,
                 routing_key=RoutingKey,
                 payload=Payload,
                 retain=IsRetain} = Msg) ->
    %% don't trade consistency for availability
    case vmq_cluster:is_ready() of
        true when IsRetain and (Payload == <<>>) ->
            %% retain delete action
            Words = emqtt_topic:words(RoutingKey),
            transaction(fun() -> mnesia:delete({vmq_retain, Words}) end);
        true when IsRetain ->
            case retain_msg(Msg) of
                {ok, RetainedMsg} ->
                    RegModule:fold(RoutingKey, fun publish_/2, RetainedMsg),
                    ok;
                {error, overloaded} ->
                    {error, overloaded}
            end;
        true ->
            RegModule:fold(RoutingKey, fun publish_/2, Msg),
            ok;
        false ->
            {error, not_ready}
    end.

publish_({Topic, Node}, Msg) when Node == node() ->
    Subscribers = mnesia:dirty_read(vmq_subscriber, Topic),
    lists:foldl(fun(#vmq_subscriber{qos=QoS, client=ClientId, node=SubNode}, AccMsg)
                          when SubNode == Node ->
                        case get_queue_pid(ClientId) of
                            {ok, QPids} ->
                                RefedMsg =
                                case QoS of
                                    0 ->
                                        AccMsg;
                                    _ ->
                                        vmq_msg_store:store(ClientId, AccMsg)
                                end,
                                lists:foldl(
                                  fun(QPid, AccAccMsg) ->
                                          publish_({Topic, Node, ClientId,
                                                    QoS, QPid}, AccAccMsg)
                                  end, RefedMsg, QPids);
                            _ when QoS > 0 ->
                                RefedMsg = vmq_msg_store:store(ClientId, AccMsg),
                                vmq_msg_store:defer_deliver(ClientId, QoS,
                                                            RefedMsg#vmq_msg.msg_ref),
                                RefedMsg;
                            _ ->
                                AccMsg
                        end;
                   (_, AccMsg) ->
                        AccMsg
                end, Msg, Subscribers);
publish_({Topic, Node}, Msg) ->
    rpc:call(Node, ?MODULE, publish_, [{Topic, Node}, Msg]),
    Msg;
publish_({_Topic, Node, _ClientId, 0, QPid}, Msg) when Node == node() ->
    case QPid of
        undefined -> Msg;
        _ ->
            %% in case of a qos-upgrade we'll
            %% increment the ref count inside the vmq_session
            vmq_queue:enqueue(QPid, {deliver, 0, Msg}),
            Msg
    end;
publish_({_Topic, Node, ClientId, QoS, QPid}, Msg) when Node == node() ->
    case QPid of
        undefined ->
            vmq_msg_store:defer_deliver(ClientId, QoS, Msg#vmq_msg.msg_ref),
            Msg;
        _ ->
            case vmq_queue:enqueue(QPid, {deliver, QoS, Msg}) of
                ok ->
                    Msg;
                {error, _} ->
                    vmq_msg_store:defer_deliver(ClientId, QoS, Msg#vmq_msg.msg_ref),
                    Msg
            end
    end.

retain_msg(Msg = #vmq_msg{routing_key=RoutingKey, payload=Payload}) ->
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
              end,
              {ok, Msg#vmq_msg{retain=false}}

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
        {ok, ClientPids} ->
            lists:foreach(fun(ClientPid) ->
                                  disconnect_client(ClientPid),
                                  wait_until_stopped(ClientPid)
                          end, ClientPids);
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

-spec table_defs() -> [{atom(), [{atom(), any()}]}].
table_defs() ->
    [
     {vmq_subscriber, [
        {record_name, vmq_subscriber},
        {type, bag},
        {attributes, record_info(fields, vmq_subscriber)},
        {disc_copies, [node()]},
        {match, #vmq_subscriber{_='_'}},
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
    [reset_table(T) || {T, _}<- table_defs()],
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
    case catch vmq_cluster:if_ready(fun() -> true end, []) of
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
    PluginPid = self(),
    {ok, QPid} = vmq_queue:start_link(
                   spawn_link(
                     fun() ->
                             plugin_queue_loop(PluginPid)
                     end)
                   , QueueSize),

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
            ok = subscribe(false, User, ClientId(CallingPid),
                           QPid, [{Topic, 0}]),
            ok
    end,
    UnsubscribeFun =
    fun(Topic) ->
            wait_til_ready(),
            CallingPid = self(),
            User = {plugin, Mod, CallingPid},
            ok = unsubscribe(false, User, ClientId(CallingPid), [{Topic, 0}]),
            ok
    end,
    {RegisterFun, PublishFun, {SubscribeFun, UnsubscribeFun}}.

plugin_queue_loop(PluginPid) ->
    receive
        {mail, QPid, new_data} ->
            vmq_queue:active(QPid),
            plugin_queue_loop(PluginPid);
        {mail, QPid, Msgs, _, _} ->
            [PluginPid ! {deliver, RoutingKey,
                              Payload,
                              QoS,
                              IsRetain,
                              IsDup}
             || {deliver, QoS, #vmq_msg{
                                  routing_key=RoutingKey,
                                  payload=Payload,
                                  retain=IsRetain,
                                  dup=IsDup}} <- Msgs],
            vmq_queue:notify(QPid),
            plugin_queue_loop(PluginPid);
        Other ->
            exit({unknown_msg_in_plugin_loop, Other})
    end.


subscribe_subscriber_changes() ->
    mnesia:subscribe({table, vmq_subscriber, simple}),
    Node = node(),
    {mnesia_table_event,
     fun
        ({delete_object, #vmq_subscriber{topic=Topic, qos=QoS,
                                     client=ClientId, node=N}, _})
          when N == Node ->
             {unsubscribe, Topic, {ClientId, QoS}};
        ({delete_object, #vmq_subscriber{topic=Topic, node=N}, _}) ->
             {unsubscribe, Topic, N};
        ({write, #vmq_subscriber{topic=Topic, qos=QoS,
                             client=ClientId, node=N}, _})
          when N == Node ->
             {subscribe, Topic, {ClientId, QoS, get_queue_pid(ClientId)}};
        ({write, #vmq_subscriber{topic=Topic, node=N}, _}) ->
             {subscribe, Topic, N};
        (_) ->
             ignore
     end}.

fold_subscribers(FoldFun, Acc) ->
    fold_subscribers(false, FoldFun, Acc).

fold_subscribers(ResolveQPids, FoldFun, Acc) ->
    Node = node(),
    mnesia:async_dirty(
      fun() ->
              mnesia:foldl(
                fun(#vmq_subscriber{topic=Topic, qos=QoS,
                                    client=ClientId, node=N}, AccAcc) ->
                        case Node == N of
                            true when ResolveQPids ->
                                FoldFun({Topic, {ClientId, QoS, get_queue_pid(ClientId)}}, AccAcc);
                            true ->
                                FoldFun({Topic, {ClientId, QoS, undefined}}, AccAcc);
                            false ->
                                FoldFun({Topic, N})
                        end
                end, Acc, vmq_subscriber)
      end).

-spec add_subscriber_tx(topic(), qos(), client_id()) -> ok | ignore | abort.
add_subscriber_tx(Topic, Qos, ClientId) ->
    mnesia:write(vmq_subscriber,
                 #vmq_subscriber{topic=Topic, qos=Qos, client=ClientId,
                                             node=node()}, write).

-spec del_subscriber(topic() | '_' ,client_id()) -> ok.
del_subscriber(Topic, ClientId) ->
    transaction(fun() -> del_subscriber_tx(Topic, ClientId) end).

-spec del_subscriber_tx(topic() | '_' ,client_id()) -> ok.
del_subscriber_tx(Topic, ClientId) ->
    Objs = mnesia:match_object(vmq_subscriber,
                               #vmq_subscriber{topic=Topic,
                                           client=ClientId, _='_'}, write),
    lists:foreach(fun(Obj) ->
                          mnesia:delete_object(vmq_subscriber, Obj, write)
                  end, Objs).

-spec remap_subscriptions_tx(client_id()) -> ok.
remap_subscriptions_tx(ClientId) ->
    Objs = mnesia:match_object(vmq_subscriber,
                               #vmq_subscriber{client=ClientId, _='_'}, write),
    Node = node(),
    lists:foreach(
      fun(#vmq_subscriber{topic=T, qos=Qos, node=N} = Obj) ->
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
                      mnesia:delete_object(vmq_subscriber, Obj, write)
              end
      end, Objs).

-spec remap_session_tx(client_id()) -> ok.
remap_session_tx(ClientId) ->
    Objs = mnesia:match_object(vmq_subscriber,
                               #vmq_subscriber{client=ClientId, _='_'}, write),
    Node = node(),
    lists:foreach(
      fun(#vmq_subscriber{topic=T, qos=Qos, node=N}) ->
              case N of
                  Node -> ignore;
                  _ ->
                      add_subscriber_tx(T, Qos, ClientId)
              end
      end, Objs).


-spec get_client_pid(_) -> {'error','not_found'} | {'ok', [pid()]}.
get_client_pid(ClientId) ->
    case [Pid || #session{pid=Pid}
                 <- ets:lookup(vmq_session, ClientId),
                 is_pid(Pid)] of
        [] ->
            {error, not_found};
        Pids ->
            {ok, Pids}
    end.

-spec get_queue_pid(_) -> {error, not_found} | {ok, [pid()]}.
get_queue_pid(ClientId) ->
    case [Pid || #session{queue_pid=Pid}
                 <- ets:lookup(vmq_session, ClientId),
                 is_pid(Pid)] of
        [] ->
            {error, not_found};
        Pids ->
            {ok, Pids}
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
                                vmq_plugin:all(incr_expired_clients, []),
                                vmq_plugin:all(decr_inactive_clients, [])
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


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% GEN_SERVER CALLBACKS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec init([]) -> {ok, state()}.
init([]) ->
    ets:new(vmq_session_mons, [named_table]),
    spawn_link(fun() -> message_queue_monitor() end),
    process_flag(trap_exit, true),
    process_flag(priority, high),
    set_monitors(),
    {ok, #state{}}.

-spec handle_call(_, _, []) -> {reply, ok, []}.
handle_call({monitor, SessionPid, QPid, ClientId, CleanSession, RemoveOldSess},
            _From, State) ->
    Sessions = ets:lookup(vmq_session, ClientId),
    lists:foreach(
      fun(#session{pid=undefined, monitor=undefined, clean=true} = Obj) ->
              %% should not happen, we clean this up
              ets:delete_object(vmq_session, Obj);
         (#session{pid=undefined, monitor=undefined, clean=false}) ->
              vmq_plugin:all(decr_inactive_clients, []);
         (#session{pid=OldSessionPid, monitor=MRef, clean=OldClean})
            when RemoveOldSess ->
              demonitor(MRef, [flush]),
              ets:delete(vmq_session_mons, MRef),
              case OldClean of
                  true ->
                      del_subscriber_fun(ClientId),
                      vmq_msg_store:clean_session(ClientId);
                  false ->
                      vmq_plugin:all(incr_inactive_clients, [])
              end,
              disconnect_client(OldSessionPid);
        (_) ->
              ok
      end, Sessions),

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
handle_info({'DOWN', MRef, process, Pid, _Reason}, State) ->
    case ets:lookup(vmq_session_mons, MRef) of
        [] ->
            ignore;
        [{_, ClientId}] ->
            ets:delete(vmq_session_mons, MRef),
            unregister_client(ClientId, Pid)
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


unregister_client(ClientId, ClientPid) ->
    case ets:lookup(vmq_session, ClientId) of
        [] ->
            {error, not_found};
        [#session{clean=true} = Obj] ->
            del_subscriber_fun(ClientId),
            ets:delete_object(vmq_session, Obj),
            vmq_msg_store:clean_session(ClientId);
        [#session{clean=false} = Obj] ->
            ets:delete_object(vmq_session, Obj),
            ets:insert(vmq_session,
                       Obj#session{pid=undefined,
                                   queue_pid=undefined,
                                   monitor=undefined,
                                   last_seen=epoch()}),
            vmq_plugin:all(incr_inactive_clients, []);
        Sessions ->
            %% at this moment we have multiple sessions for the same client id
            _ = [ets:delete_object(vmq_session, Obj)
                 || #session{pid=Pid} = Obj <- Sessions, Pid == ClientPid]
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
