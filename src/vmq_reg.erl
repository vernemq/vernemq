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
         register_subscriber/4,
         register_session/2,
         %% used in vmq_session fsm handling
         publish/1,

         %% used in vmq_session:get_info/2
         subscriptions_for_subscriber/1,
         get_subscriber_pids/1,

         %% used in vmq_msg_store:init/1
         subscriptions/2,

         %% used in vmq_server_utils
         total_subscribers/0,
         retained/0,

         %% used in vmq_session_expirer
         remove_expired_subscribers/1
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
         register_subscriber_/4]).

%% used by mnesia_cluster for table setup
-export([table_defs/0]).
%% used from vmq-admin script (deployed with the VerneMQ release)
-export([reset_all_tables/1]).
%% used from plugins
-export([direct_plugin_exports/1]).
%% used by reg views
-export([subscribe_subscriber_changes/0,
         fold_subscribers/2,
         fold_subscribers/3]).

-record(state, {}).
-record(session, {subscriber_id, pid, queue_pid, monitor, last_seen, clean}).
-record(vmq_subscriber, {topic, qos, id, node}).
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

-spec subscribe(flag(), username() | plugin_id(), subscriber_id(), pid(),
                [{topic(), qos()}]) -> ok | {error, not_allowed | [any(), ...]}.

subscribe(false, User, SubscriberId, QPid, Topics) ->
    %% trade availability for consistency
    vmq_cluster:if_ready(fun subscribe_/4, [User, SubscriberId, QPid, Topics]);
subscribe(true, User, SubscriberId, QPid, Topics) ->
    %% trade consistency for availability
    subscribe_(User, SubscriberId, QPid, Topics).

-spec subscribe_(username() | plugin_id(), subscriber_id(), pid(),
                 [{topic(), qos()}]) -> 'ok' | {'error','not_allowed'}.
subscribe_(User, SubscriberId, QPid, Topics) ->
    case vmq_plugin:all_till_ok(auth_on_subscribe,
                                [User, SubscriberId, Topics]) of
        ok ->
            vmq_plugin:all(on_subscribe, [User, SubscriberId, Topics]),
            subscribe_tx(SubscriberId, QPid, Topics);
        {error, _} ->
            {error, not_allowed}
    end.

-spec subscribe_tx(subscriber_id(), pid(), [{topic(), qos()}]) -> 'ok'.
subscribe_tx(_, _, []) -> ok;
subscribe_tx(SubscriberId, QPid, Topics) ->
    transaction(fun() ->
                        _ = [add_subscriber_tx(T, QoS, SubscriberId)
                             || {T, QoS} <- Topics]
                end,
                fun(_) ->
                        _ = [begin
                                 vmq_plugin:all(incr_subscription_count, []),
                                 deliver_retained(SubscriberId, QPid, T, QoS)
                             end || {T, QoS} <- Topics],
                        ok
                end).

-spec unsubscribe(flag(), username() | plugin_id(),
                  subscriber_id(), [topic()]) -> any().
unsubscribe(false, User, SubscriberId, Topics) ->
    %% trade availability for consistency
    vmq_cluster:if_ready(fun unsubscribe_/3, [User, SubscriberId, Topics]);
unsubscribe(true, User, SubscriberId, Topics) ->
    %% trade consistency for availability
    unsubscribe_(User, SubscriberId, Topics).

-spec unsubscribe_(username() | plugin_id(), subscriber_id(), [topic()]) -> 'ok'.
unsubscribe_(User, SubscriberId, Topics) ->
    lists:foreach(fun(Topic) ->
                          del_subscriber(Topic, SubscriberId),
                          vmq_plugin:all(decr_subscription_count, [])
                  end, Topics),
    vmq_plugin:all(on_unsubscribe, [User, SubscriberId, Topics]),
    ok.

-spec subscriptions(mountpoint(), routing_key()) -> [{subscriber_id(), qos()}].
subscriptions(MP, RoutingKey) ->
    subscriptions(MP, fun(Obj, Acc) -> [Obj|Acc] end, [], RoutingKey).

-spec subscriptions(mountpoint(), function(), any(), routing_key()) -> any().
subscriptions(MP, FoldFun, Acc, RoutingKey) ->
    RegViews = vmq_config:get_env(reg_views, []),
    lists:flatten([begin
                       {_, NewAcc, _} = RV:fold(MP, RoutingKey,
                                             fun subscriptions_/2,
                                             {MP, Acc, FoldFun}),
                       NewAcc
                   end || RV <- RegViews]).

subscriptions_({Topic, Node}, {MountPoint, Acc, FoldFun}) when Node == node() ->
    NewAcc =
    lists:foldl(
      fun
          (#vmq_subscriber{id={MP, _} = SubscriberId, qos=Qos}, Acc1)
            when MP == MountPoint ->
              FoldFun({SubscriberId, Qos}, Acc1);
          (_, Acc1) ->
              Acc1
      end, Acc, mnesia:dirty_read(vmq_subscriber, Topic)),
    {MountPoint, NewAcc, FoldFun};
subscriptions_({_Topic, Node, {MP, _} = SubscriberId, QoS, _Pid}, {MP, Acc, FoldFun})
  when Node == node() ->
    {MP, FoldFun({SubscriberId, QoS}, Acc), FoldFun};
subscriptions_(_, Acc) ->
    Acc.

subscriptions_for_subscriber(SubscriberId) ->
    Res = mnesia:dirty_match_object(vmq_subscriber,
                              #vmq_subscriber{id=SubscriberId, _='_'}),
    [{T, Q} || #vmq_subscriber{topic=T, qos=Q} <- Res].

-spec register_subscriber(flag(), subscriber_id(), pid(), flag()) ->
    ok | {error, _}.
register_subscriber(false, SubscriberId, QPid, CleanSession) ->
    %% we don't allow multiple sessions using same subscriber id
    register_subscriber(SubscriberId, QPid, CleanSession);
register_subscriber(true, SubscriberId, QPid, _CleanSession) ->
    %% we allow multiple sessions using same subscriber id
    %%
    %% !!! CleanSession is disabled if multiple sessions are in use
    %%
    register_session(SubscriberId, QPid).

-spec register_subscriber(subscriber_id(), pid(), flag()) -> ok | {error, _}.
register_subscriber(SubscriberId, QPid, CleanSession) ->
    case vmq_reg_leader:register_subscriber(self(), QPid, SubscriberId,
                                            CleanSession) of
        ok when not CleanSession ->
            vmq_session_proxy_sup:start_delivery(QPid, SubscriberId),
            ok;
        R ->
            R
    end.

-spec register_session(subscriber_id(), pid()) -> ok | {error, _}.
register_session(SubscriberId, QPid) ->
    %% register_session allows to have multiple subscribers connected
    %% with the same session_id (as oposed to register_subscriber)
    SessionPid = self(),
    transaction(fun() -> remap_session_tx(SubscriberId) end,
                fun({error, Reason}) -> {error, Reason};
                   (_) ->
                        ok = gen_server:call(?MODULE,
                                             {monitor, SessionPid, QPid,
                                              SubscriberId, false, false},
                                             infinity),
                        vmq_session_proxy_sup:start_delivery(QPid, SubscriberId),
                        ok
                end).

teardown_session(SubscriberId, CleanSession) ->
    %% we first have to disconnect a local or remote session for
    %% this client id if one exists, disconnect_subscriber(SubscriberId)
    %% blocks until the session is cleaned up
    NodeWithSession =
    case disconnect_subscriber(SubscriberId) of
        ok ->
            [node()];
        {error, not_found} ->
            []
    end,
    case CleanSession of
        true -> vmq_msg_store:clean_session(SubscriberId);
        false ->
            ignore
    end,
    NodeWithSession.

-spec register_subscriber_(pid(), pid(),
                           subscriber_id(), flag()) -> 'ok' | {error, overloaded}.
register_subscriber_(SessionPid, QPid, SubscriberId, CleanSession) ->
    %% cleanup session for this client id if needed
    case CleanSession of
        true ->
            transaction(fun() -> del_subscriber_tx('_', SubscriberId) end,
                        fun({error, Reason}) -> {error, Reason};
                           (_) -> register_subscriber__(SessionPid, QPid,
                                                        SubscriberId, true)
                        end);
        false ->
            transaction(fun() -> remap_subscriptions_tx(SubscriberId) end,
                        fun({error, Reason}) -> {error, Reason};
                           (_) -> register_subscriber__(SessionPid, QPid,
                                                        SubscriberId, false)
                        end)
    end.

-spec register_subscriber__(pid(), pid(), subscriber_id(), flag()) -> 'ok'.
register_subscriber__(SessionPid, QPid, SubscriberId, CleanSession) ->
    NodesWithSession =
    lists:foldl(
      fun(Node, Acc) ->
              Res =
              case Node == node() of
                  true ->
                      teardown_session(SubscriberId, CleanSession);
                  false ->
                      rpc:call(Node, ?MODULE,
                               teardown_session, [SubscriberId,
                                                  CleanSession])
              end,
              [Res|Acc]
      end, [], vmq_cluster:nodes()),
    %% We SHOULD always have 0 or 1 Node(s) that had a sesssion
    NNodesWithSession = lists:flatten(NodesWithSession),
    case length(NNodesWithSession) of
        L when L =< 1 ->
            ok;
        _ -> lager:warning("subscriber ~p was active on multiple nodes ~p",
                           [SubscriberId, NNodesWithSession])
    end,
    ok = gen_server:call(?MODULE, {monitor, SessionPid, QPid,
                                   SubscriberId, CleanSession, true}, infinity).

-spec publish(msg()) -> 'ok' | {'error', _}.
publish(#vmq_msg{trade_consistency=true,
                 reg_view=RegView,
                 mountpoint=MP,
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
                    RegView:fold(MP, RoutingKey, fun publish_/2, RetainedMsg),
                    ok;
                {error, overloaded} ->
                    {error, overloaded}
            end;
        false ->
            RegView:fold(MP, RoutingKey, fun publish_/2, Msg),
            ok
    end;
publish(#vmq_msg{trade_consistency=false,
                 reg_view=RegView,
                 mountpoint=MP,
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
                    RegView:fold(MP, RoutingKey, fun publish_/2, RetainedMsg),
                    ok;
                {error, overloaded} ->
                    {error, overloaded}
            end;
        true ->
            RegView:fold(MP, RoutingKey, fun publish_/2, Msg),
            ok;
        false ->
            {error, not_ready}
    end.

%% vmq_reg_trie reg view delivers this format
publish_({Topic, Node}, Msg) when Node == node() ->
    Subscribers = mnesia:dirty_read(vmq_subscriber, Topic),
    publish__(Node, Msg, Subscribers);

%% vmq_reg_pets reg view delivers this format
publish_({_Topic, Node, _SubscriberId, 0, undefined}, Msg)
  when Node == node() ->
    Msg;
publish_({_Topic, Node, _SubscriberId, 0, QPid}, Msg)
  when Node == node() ->
    vmq_queue:enqueue(QPid, {deliver, 0, Msg}),
    Msg;
publish_({_Topic, Node, SubscriberId, QoS, undefined}, Msg)
  when Node == node() ->
    RefedMsg = vmq_msg_store:store(SubscriberId, Msg),
    vmq_msg_store:defer_deliver(SubscriberId, QoS, RefedMsg#vmq_msg.msg_ref),
    RefedMsg;
publish_({_Topic, Node, SubscriberId, QoS, QPid}, Msg)
  when Node == node() ->
    RefedMsg = vmq_msg_store:store(SubscriberId, Msg),
    case vmq_queue:enqueue(QPid, {deliver, QoS, RefedMsg}) of
        ok ->
            RefedMsg;
        {error, _} ->
            vmq_msg_store:defer_deliver(SubscriberId, QoS, RefedMsg#vmq_msg.msg_ref),
            RefedMsg
    end;
%% we route the message to the proper node
publish_({Topic, Node}, Msg) ->
    vmq_cluster:publish(Node, {Topic, Msg}),
    Msg.

publish__(Node, Msg, [#vmq_subscriber{qos=QoS, id=Id, node=Node}|Rest]) ->
    {_, QPids} = get_queue_pids(Id),
    publish__(Node, publish___(Id, Msg, QoS, QPids), Rest);
publish__(Node, Msg, [_|Rest]) ->
    %% subscriber on other node
    publish__(Node, Msg, Rest);
publish__(_, Msg, []) -> Msg.

publish___(_, Msg, 0, not_found) -> Msg;
publish___(SubscriberId, Msg, QoS, not_found) ->
    RefedMsg = vmq_msg_store:store(SubscriberId, Msg),
    vmq_msg_store:defer_deliver(SubscriberId, QoS, RefedMsg#vmq_msg.msg_ref),
    RefedMsg;
publish___(SubscriberId, Msg, 0, [QPid|Rest]) ->
    vmq_queue:enqueue(QPid, {deliver, 0, Msg}),
    publish___(SubscriberId, Msg, 0, Rest);
publish___(SubscriberId, Msg, QoS, [QPid|Rest]) ->
    RefedMsg = vmq_msg_store:store(SubscriberId, Msg),
    vmq_queue:enqueue(QPid, {deliver, QoS, RefedMsg}),
    publish___(SubscriberId, RefedMsg, QoS, Rest);
publish___(_, Msg, _, []) -> Msg.

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

-spec deliver_retained(subscriber_id(), pid(), topic(), qos()) -> 'ok'.
deliver_retained(SubscriberId, QPid, Topic, QoS) ->
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
                  _ -> vmq_msg_store:store(SubscriberId, Msg)
              end,
              vmq_queue:enqueue(QPid, {deliver, QoS, MaybeChangedMsg})
      end, RetainedMsgs).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% RPC Callbacks / Maintenance
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec disconnect_subscriber(subscriber_id() | pid()) ->
    'ok' | {'error','not_found'}.
disconnect_subscriber(SubscriberPid) when is_pid(SubscriberPid) ->
    vmq_session:disconnect(SubscriberPid),
    ok;
disconnect_subscriber(SubscriberId) ->
    wait_until_unregistered(SubscriberId).

-spec wait_until_unregistered(subscriber_id()) -> {'error','not_found'}.
wait_until_unregistered(SubscriberId) ->
    case get_subscriber_pids(SubscriberId) of
        {ok, SubscriberPids} ->
            lists:foreach(fun(SubscriberPid) ->
                                  disconnect_subscriber(SubscriberPid),
                                  wait_until_stopped(SubscriberPid)
                          end, SubscriberPids);
        E -> E
    end.

-spec wait_until_stopped(pid()) -> ok.
wait_until_stopped(SubscriberPid) ->
    case is_process_alive(SubscriberPid) of
        true ->
            timer:sleep(100),
            wait_until_stopped(SubscriberPid);
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
        unsplit_vclock_props(vclock)]}
].

unsplit_bag_props() ->
    {user_properties,
     [{unsplit_method, {unsplit_lib, bag, []}}]}.

unsplit_vclock_props(Attr) ->
    {user_properties,
     [{unsplit_method, {unsplit_lib, vclock, [Attr]}}]}.

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
    TradeConsistency = vmq_config:get_env(trade_consistency),
    QueueSize = vmq_config:get_env(max_queued_messages, 1000),
    DefaultRegView = vmq_config:get_env(default_reg_view),
    MountPoint = "",
    ClientId = fun(T) ->
                       base64:encode_to_string(
                         integer_to_binary(
                           erlang:phash2(T)
                          )
                        )
               end,
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
            register_subscriber_(CallingPid, QPid,
                                 {MountPoint, ClientId(CallingPid)}, true)
    end,

    PublishFun =
    fun(Topic, Payload) ->
            wait_til_ready(),
            Msg = #vmq_msg{routing_key=Topic,
                           payload=Payload,
                           dup=false,
                           retain=false,
                           trade_consistency=TradeConsistency,
                           reg_view=DefaultRegView
                           },
            ok = publish(Msg),
            ok
    end,

    SubscribeFun =
    fun(Topic) ->
            wait_til_ready(),
            CallingPid = self(),
            User = {plugin, Mod, CallingPid},
            ok = subscribe(TradeConsistency, User,
                           {MountPoint, ClientId(CallingPid)},
                           QPid, [{Topic, 0}]),
            ok
    end,
    UnsubscribeFun =
    fun(Topic) ->
            wait_til_ready(),
            CallingPid = self(),
            User = {plugin, Mod, CallingPid},
            ok = unsubscribe(TradeConsistency, User,
                             {MountPoint, ClientId(CallingPid)},
                             [{Topic, 0}]),
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
                                         id={MP, _} = SubscriberId, node=N}, _})
          when N == Node ->
             {unsubscribe, MP, Topic, {SubscriberId, QoS}};
        ({delete_object, #vmq_subscriber{topic=Topic, id={MP, _}, node=N}, _}) ->
             {unsubscribe, MP, Topic, N};
        ({write, #vmq_subscriber{topic=Topic, qos=QoS,
                                 id={MP, _} = SubscriberId, node=N}, _})
          when N == Node ->
             QPids =
             case get_queue_pids(SubscriberId) of
                 {ok, Pids} -> Pids;
                 {error, not_found} -> {error, not_found}
             end,
             {subscribe, MP, Topic, {SubscriberId, QoS, QPids}};
        ({write, #vmq_subscriber{topic=Topic, id={MP, _}, node=N}, _}) ->
             {subscribe, MP, Topic, N};
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
                                    id={MP, _} = SubscriberId, node=N}, AccAcc) ->
                        case Node == N of
                            true when ResolveQPids ->
                                QPids =
                                case get_queue_pids(SubscriberId) of
                                    {ok, Pids} -> Pids;
                                    {error, not_found} -> {error, not_found}
                                end,
                                FoldFun({MP, Topic, {SubscriberId, QoS, QPids}},
                                        AccAcc);
                            true ->
                                FoldFun({MP, Topic, {SubscriberId, QoS, undefined}},
                                        AccAcc);
                            false ->
                                FoldFun({MP, Topic, N}, AccAcc)
                        end
                end, Acc, vmq_subscriber)
      end).

-spec add_subscriber_tx(topic(), qos(), subscriber_id()) -> ok | ignore | abort.
add_subscriber_tx(Topic, Qos, SubscriberId) ->
    mnesia:write(vmq_subscriber,
                 #vmq_subscriber{topic=Topic, qos=Qos, id=SubscriberId,
                                 node=node()}, write).

-spec del_subscriber(topic() | '_' , subscriber_id()) -> ok.
del_subscriber(Topic, SubscriberId) ->
    transaction(fun() -> del_subscriber_tx(Topic, SubscriberId) end).

-spec del_subscriber_tx(topic() | '_' , subscriber_id()) -> ok.
del_subscriber_tx(Topic, SubscriberId) ->
    Objs = mnesia:match_object(vmq_subscriber,
                               #vmq_subscriber{topic=Topic,
                                               id=SubscriberId, _='_'}, write),
    lists:foreach(fun(Obj) ->
                          mnesia:delete_object(vmq_subscriber, Obj, write)
                  end, Objs).

-spec remap_subscriptions_tx(subscriber_id()) -> ok.
remap_subscriptions_tx(SubscriberId) ->
    Objs = mnesia:match_object(vmq_subscriber,
                               #vmq_subscriber{id=SubscriberId, _='_'}, write),
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
                      add_subscriber_tx(T, Qos, SubscriberId),
                      mnesia:delete_object(vmq_subscriber, Obj, write)
              end
      end, Objs).

-spec remap_session_tx(subscriber_id()) -> ok.
remap_session_tx(SubscriberId) ->
    Objs = mnesia:match_object(vmq_subscriber,
                               #vmq_subscriber{id=SubscriberId, _='_'}, write),
    Node = node(),
    lists:foreach(
      fun(#vmq_subscriber{topic=T, qos=Qos, node=N}) ->
              case N of
                  Node -> ignore;
                  _ ->
                      add_subscriber_tx(T, Qos, SubscriberId)
              end
      end, Objs).


-spec get_subscriber_pids(subscriber_id()) ->
    {'error','not_found'} | {'ok', [pid()]}.
get_subscriber_pids(SubscriberId) ->
    case [Pid || #session{pid=Pid}
                 <- ets:lookup(vmq_session, SubscriberId),
                 is_pid(Pid)] of
        [] ->
            {error, not_found};
        Pids ->
            {ok, Pids}
    end.

-spec get_queue_pids(subscriber_id()) -> {error, not_found} | {ok, [pid()]}.
get_queue_pids(SubscriberId) ->
    case [Pid || #session{queue_pid=Pid}
                 <- ets:lookup(vmq_session, SubscriberId),
                 is_pid(Pid)] of
        [] ->
            {error, not_found};
        Pids ->
            {ok, Pids}
    end.

-spec total_subscribers() -> non_neg_integer().
total_subscribers() ->
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

-spec remove_expired_subscribers(pos_integer()) -> ok.
remove_expired_subscribers(ExpiredSinceSeconds) ->
    ExpiredSince = epoch() - ExpiredSinceSeconds,
    remove_expired_subscribers_(ets:select(vmq_session,
                                           [{#session{last_seen='$1',
                                                      subscriber_id='$2',
                                                      pid='$3', _='_'},
                                             [{'<',
                                               '$1',
                                               ExpiredSince}],
                                             [['$2', '$3']]}], 100)).

remove_expired_subscribers_({[[SubscriberId, Pid]|Rest], Cont}) ->
    case is_process_alive(Pid) of
        false ->
            transaction(fun() -> del_subscriber_tx('_', SubscriberId) end,
                        fun(_) ->
                                vmq_msg_store:clean_session(SubscriberId),
                                vmq_plugin:all(incr_expired_clients, []),
                                vmq_plugin:all(decr_inactive_clients, [])
                        end);
        true ->
            ok
    end,
    remove_expired_subscribers_({Rest, Cont});
remove_expired_subscribers_({[], Cont}) ->
    remove_expired_subscribers_(ets:select(Cont));
remove_expired_subscribers_('$end_of_table') ->
    ok.

set_monitors() ->
    set_monitors(ets:select(vmq_session,
                            [{#session{pid='$1', subscriber_id='$2', _='_'}, [],
                              [['$1', '$2']]}], 100)).

set_monitors('$end_of_table') ->
    ok;
set_monitors({PidsAndIds, Cont}) ->
    _ = [begin
             MRef = erlang:monitor(process, Pid),
             ets:insert(vmq_session_mons, {MRef, SubscriberId})
         end || [Pid, SubscriberId] <- PidsAndIds],
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
handle_call({monitor, SessionPid, QPid, SubscriberId,
             CleanSession, RemoveOldSess}, _From, State) ->
    Sessions = ets:lookup(vmq_session, SubscriberId),
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
                      del_subscriber_fun(SubscriberId),
                      vmq_msg_store:clean_session(SubscriberId);
                  false ->
                      vmq_plugin:all(incr_inactive_clients, [])
              end,
              disconnect_subscriber(OldSessionPid);
        (_) ->
              ok
      end, Sessions),

    Monitor = monitor(process, SessionPid),
    Session = #session{subscriber_id=SubscriberId,
                       pid=SessionPid, queue_pid=QPid,
                       monitor=Monitor, last_seen=epoch(),
                       clean=CleanSession},
    ets:insert(vmq_session_mons, {Monitor, SubscriberId}),
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
        [{_, SubscriberId}] ->
            ets:delete(vmq_session_mons, MRef),
            unregister_subscriber(SubscriberId, Pid)
    end,
    {noreply, State}.

-spec terminate(_, _) -> ok.
terminate(_Reason, _State) ->
    ok.

-spec code_change(_, _, _) -> {ok, _}.
code_change(_OldVSN, State, _Extra) ->
    {ok, State}.

epoch() ->
    {Mega, Sec, _} = os:timestamp(),
    (Mega * 1000000 + Sec).


unregister_subscriber(SubscriberId, SubscriberPid) ->
    case ets:lookup(vmq_session, SubscriberId) of
        [] ->
            {error, not_found};
        [#session{clean=true} = Obj] ->
            del_subscriber_fun(SubscriberId),
            ets:delete_object(vmq_session, Obj),
            vmq_msg_store:clean_session(SubscriberId);
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
                 || #session{pid=Pid} = Obj <- Sessions, Pid == SubscriberPid]
    end.

del_subscriber_fun(SubscriberId) ->
    transaction(fun() -> del_subscriber_tx('_', SubscriberId) end).


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
