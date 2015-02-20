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
         client_stats/0,
         total_sessions/0,
         total_inactive_sessions/0,
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
%% used by vmq_session:list_sessions
-export([fold_sessions/2]).

-record(state, {}).
-record(session, {subscriber_id, pid, queue_pid, monitor, last_seen, clean}).
-record(vmq_subscriber, {topic, qos, id, node}).
-record(retain, {words, routing_key, payload, vclock=unsplit_vclock:fresh()}).

-type state() :: #state{}.

-spec start_link() -> {ok, pid()} | ignore | {error, atom()}.
start_link() ->
    _ = ets:new(vmq_session, [public,
                              bag,
                              named_table,
                              {keypos, 2},
                              {read_concurrency, true},
                              {write_concurrency, true}]),
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec subscribe(flag(), username() | plugin_id(), subscriber_id(), pid(),
                [{topic(), qos()}]) -> ok | {error, not_allowed
                                             | overloaded
                                             | not_ready}.

subscribe(false, User, SubscriberId, QPid, Topics) ->
    %% trade availability for consistency
    vmq_cluster:if_ready(fun subscribe_/4, [User, SubscriberId, QPid, Topics]);
subscribe(true, User, SubscriberId, QPid, Topics) ->
    %% trade consistency for availability
    subscribe_(User, SubscriberId, QPid, Topics).

subscribe_(User, SubscriberId, QPid, Topics) ->
    case vmq_plugin:all_till_ok(auth_on_subscribe,
                                [User, SubscriberId, Topics]) of
        ok ->
            subscribe_tx(User, SubscriberId, QPid, Topics);
        {error, _} ->
            {error, not_allowed}
    end.

subscribe_tx(User, SubscriberId, QPid, Topics) ->
    transaction(fun() ->
                        _ = [add_subscriber_tx(T, QoS, SubscriberId)
                             || {T, QoS} <- Topics],
                        ok
                end,
                fun(_) ->
                        _ = [begin
                                 _ = vmq_exo:incr_subscription_count(),
                                 deliver_retained(SubscriberId, QPid, T, QoS)
                             end || {T, QoS} <- Topics],
                        vmq_plugin:all(on_subscribe, [User, SubscriberId, Topics]),
                        ok
                end).

-spec unsubscribe(flag(), username() | plugin_id(),
                  subscriber_id(), [topic()]) -> ok | {error, overloaded
                                                       | not_ready}.
unsubscribe(false, User, SubscriberId, Topics) ->
    %% trade availability for consistency
    vmq_cluster:if_ready(fun unsubscribe_/3, [User, SubscriberId, Topics]);
unsubscribe(true, User, SubscriberId, Topics) ->
    %% trade consistency for availability
    unsubscribe_(User, SubscriberId, Topics).

unsubscribe_(User, SubscriberId, Topics) ->
    unsubscribe_tx(User, SubscriberId, Topics).

unsubscribe_tx(User, SubscriberId, Topics) ->
    transaction(fun() ->
                    _ = [del_subscriber_tx(T, SubscriberId)
                         || T <- Topics],
                    ok
                end,
                fun(_) ->
                        _ = [vmq_exo:decr_subscription_count()
                             || _ <- Topics],
                        _ = vmq_plugin:all(on_unsubscribe, [User, SubscriberId, Topics]),
                        ok
                end).

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
                        case gen_server:call(?MODULE,
                                             {monitor, SessionPid, QPid,
                                              SubscriberId, false, false},
                                             infinity) of
                            ok ->
                                vmq_session_proxy_sup:start_delivery(QPid, SubscriberId);
                            {error, Reason} ->
                                {error, Reason}
                        end
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

-spec register_subscriber__(pid(), pid(), subscriber_id(), flag()) -> 'ok' | {error, _}.
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
    gen_server:call(?MODULE, {monitor, SessionPid, QPid,
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
            retain_msg(Msg);
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
            %% retain set action
            retain_msg(Msg);
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
    case vmq_queue:enqueue(QPid, {deliver, 0, Msg}) of
        ok ->
            Msg;
        {error, _} ->
            % ignore
            Msg
    end;
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
    case vmq_cluster:publish(Node, {Topic, Msg}) of
        ok ->
            Msg;
        {error, Reason} ->
            lager:warning("cant' publish to remote node ~p due to '~p'", [Node, Reason]),
            Msg
    end.

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
    _ = vmq_queue:enqueue(QPid, {deliver, 0, Msg}),
    publish___(SubscriberId, Msg, 0, Rest);
publish___(SubscriberId, Msg, QoS, [QPid|Rest]) ->
    RefedMsg = vmq_msg_store:store(SubscriberId, Msg),
    case vmq_queue:enqueue(QPid, {deliver, QoS, RefedMsg}) of
        ok ->
            publish___(SubscriberId, RefedMsg, QoS, Rest);
        {error, _} ->
            vmq_msg_store:defer_deliver(SubscriberId, QoS, RefedMsg#vmq_msg.msg_ref),
            publish___(SubscriberId, RefedMsg, QoS, Rest)
    end;
publish___(_, Msg, _, []) -> Msg.

retain_msg(Msg = #vmq_msg{mountpoint=MP, reg_view=RegView,
                          routing_key=RoutingKey, payload=Payload}) ->
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
              Msg#vmq_msg{retain=false}
      end,
     fun(RetainedMsg) ->
             _ = RegView:fold(MP, RoutingKey, fun publish_/2, RetainedMsg),
             ok
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
                                  _ = disconnect_subscriber(SubscriberPid),
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
    _ = [reset_table(T) || {T, _}<- table_defs()],
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
        _ ->
            timer:sleep(100),
            wait_til_ready()
    end.

-spec direct_plugin_exports(module()) -> {function(), function(), {function(), function()}} | {error, invalid_config}.
direct_plugin_exports(Mod) when is_atom(Mod) ->
    %% This Function exports a generic Register, Publish, and Subscribe
    %% Fun, that a plugin can use if needed. Currently all functions
    %% block until the cluster is ready.
    case {vmq_config:get_env(trade_consistency, false),
          vmq_config:get_env(max_queued_messages, 1000),
          vmq_config:get_env(default_reg_view, vmq_reg_trie)} of
        {TradeConsistency, QueueSize, DefaultRegView}
              when is_boolean(TradeConsistency)
                   and (QueueSize >= 0)
                   and is_atom(DefaultRegView) ->
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
                                     plugin_queue_loop(PluginPid, Mod)
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
                                   mountpoint=MountPoint,
                                   payload=Payload,
                                   dup=false,
                                   retain=false,
                                   trade_consistency=TradeConsistency,
                                   reg_view=DefaultRegView
                                  },
                    publish(Msg)
            end,

            SubscribeFun =
            fun(Topic) when is_list(Topic) ->
                    wait_til_ready(),
                    CallingPid = self(),
                    User = {plugin, Mod, CallingPid},
                    subscribe(TradeConsistency, User,
                              {MountPoint, ClientId(CallingPid)},
                              QPid, [{Topic, 0}]);
               (_) ->
                    {error, invalid_topic}
            end,
            UnsubscribeFun =
            fun(Topic) when is_list(Topic) ->
                    wait_til_ready(),
                    CallingPid = self(),
                    User = {plugin, Mod, CallingPid},
                    unsubscribe(TradeConsistency, User,
                                {MountPoint, ClientId(CallingPid)}, [Topic]);
               (_) ->
                    {error, invalid_topic}
            end,
            {RegisterFun, PublishFun, {SubscribeFun, UnsubscribeFun}};
        _ ->
            {error, invalid_config}
    end.


plugin_queue_loop(PluginPid, PluginMod) ->
    receive
        {mail, QPid, new_data} ->
            vmq_queue:active(QPid),
            plugin_queue_loop(PluginPid, PluginMod);
        {mail, QPid, Msgs, _, _} ->
            lists:foreach(fun({deliver, QoS, #vmq_msg{
                                                routing_key=RoutingKey,
                                                payload=Payload,
                                                retain=IsRetain,
                                                dup=IsDup}}) ->
                                  PluginPid ! {deliver, RoutingKey,
                                               Payload,
                                               QoS,
                                               IsRetain,
                                               IsDup};
                             (Msg) ->
                                  lager:warning("drop message ~p for plugin ~p", [Msg, PluginMod]),
                                  ok
                          end, Msgs),
            vmq_queue:notify(QPid),
            plugin_queue_loop(PluginPid, PluginMod);
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

fold_sessions(FoldFun, Acc) ->
    ets:foldl(fun(#session{subscriber_id=SubscriberId,
                           pid=SessionPid}, AccAcc) ->
                      FoldFun(SubscriberId, SessionPid, AccAcc)
              end, Acc, vmq_session).


-spec add_subscriber_tx(topic(), qos(), subscriber_id()) -> ok | ignore | abort.
add_subscriber_tx(Topic, Qos, SubscriberId) ->
    mnesia:write(vmq_subscriber,
                 #vmq_subscriber{topic=Topic, qos=Qos, id=SubscriberId,
                                 node=node()}, write).

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

client_stats() ->
    TotalSessions = total_sessions(),
    TotalInactiveSessions = total_inactive_sessions(),
    [{active, TotalSessions},
     {inactive, TotalInactiveSessions}].

-spec total_sessions() -> non_neg_integer().
total_sessions() ->
    ets:info(vmq_session, size).

-spec total_inactive_sessions() -> non_neg_integer().
total_inactive_sessions() ->
    Pattern = #session{pid=undefined,
                       queue_pid=undefined,
                       monitor=undefined,
                       _='_'},
    ets:select_count(vmq_session, [{Pattern, [], [true]}]).

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

-spec remove_expired_subscribers(pos_integer()) -> ok | {error, overloaded}.
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
            transaction(
              fun() -> del_subscriber_tx('_', SubscriberId) end,
              fun(_) ->
                      vmq_msg_store:clean_session(SubscriberId),
                      _ = vmq_exo:incr_expired_clients(),
                      remove_expired_subscribers_({Rest, Cont})
              end);
        true ->
            remove_expired_subscribers_({Rest, Cont})
    end;
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
    _ = ets:new(vmq_session_mons, [named_table]),
    spawn_link(fun() -> message_queue_monitor() end),
    process_flag(trap_exit, true),
    process_flag(priority, high),
    set_monitors(),
    {ok, #state{}}.


-spec handle_call(_, _, []) -> {reply, ok | {error, _}, []}.
handle_call({monitor, SessionPid, QPid, SubscriberId,
             CleanSession, RemoveOldSess}, _From, State) ->
    Reply =
    case cleanup_sessions(SubscriberId, RemoveOldSess) of
        ok ->
            Monitor = monitor(process, SessionPid),
            Session = #session{subscriber_id=SubscriberId,
                               pid=SessionPid, queue_pid=QPid,
                               monitor=Monitor, last_seen=epoch(),
                               clean=CleanSession},
            ets:insert(vmq_session_mons, {Monitor, SubscriberId}),
            ets:insert(vmq_session, Session),
            ok;
        {error, Reason} ->
            {error, Reason}
    end,
    {reply, Reply, State}.

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
            ok;
        [#session{clean=true} = Obj] ->
            _ = del_subscriber_fun(SubscriberId),
            ets:delete_object(vmq_session, Obj),
            vmq_msg_store:clean_session(SubscriberId);
        [#session{clean=false} = Obj] ->
            ets:delete_object(vmq_session, Obj),
            ets:insert(vmq_session,
                       Obj#session{pid=undefined,
                                   queue_pid=undefined,
                                   monitor=undefined,
                                   last_seen=epoch()}),
            ok;
        Sessions ->
            %% at this moment we have multiple sessions for the same client id
            _ = [ets:delete_object(vmq_session, Obj)
                 || #session{pid=Pid} = Obj <- Sessions, Pid == SubscriberPid],
            ok
    end.

del_subscriber_fun(SubscriberId) ->
    transaction(fun() -> del_subscriber_tx('_', SubscriberId) end).


-spec transaction(fun(() -> any())) -> any().
transaction(TxFun) ->
    transaction(TxFun, fun(Ret) -> Ret end).

-spec transaction(fun(() -> any()),
                  fun((any()) -> any())) -> any().
transaction(TxFun, SuccessFun) ->
    %% Making this a sync_transaction allows us to use dirty_read
    %% elsewhere and get a consistent result even when that read
    %% executes on a different node.
    case jobs:ask(mnesia_tx_queue) of
        {ok, JobId} ->
            try
                case sync_transaction_(TxFun) of
                    {sync, {atomic, Result}} ->
                        %% we enforces that data is synced to disk by
                        %% waiting for disk_log:sync(latest_log),
                        SuccessFun(Result);
                    {sync, {aborted, Reason}} -> throw({error, Reason});
                    {atomic, Result} -> SuccessFun(Result);
                    {aborted, Reason} -> throw({error, Reason})
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

cleanup_sessions(SubscriberId, RemoveOldSess) ->
    Sessions = ets:lookup(vmq_session, SubscriberId),
    cleanup_sessions(Sessions, SubscriberId, RemoveOldSess).
cleanup_sessions([#session{pid=undefined,
                           monitor=undefined,
                           clean=true} = Obj|Rest], SubscriberId, RemoveOldSess) ->
    %% should not happen, we clean this up
    ets:delete_object(vmq_session, Obj),
    cleanup_sessions(Rest, SubscriberId, RemoveOldSess);
cleanup_sessions([#session{pid=undefined,
                           monitor=undefined,
                           clean=false} = Obj|Rest], SubscriberId, RemoveOldSess) ->
    %% after a reconnect
    ets:delete_object(vmq_session, Obj),
    cleanup_sessions(Rest, SubscriberId, RemoveOldSess);
cleanup_sessions([#session{pid=SessionPid,
                           monitor=MRef,
                           clean=true}|Rest], SubscriberId, true) ->
    demonitor(MRef, [flush]),
    ets:delete(vmq_session_mons, MRef),
    _ = disconnect_subscriber(SessionPid),
    case del_subscriber_fun(SubscriberId) of
        ok ->
            ok = vmq_msg_store:clean_session(SubscriberId),
            cleanup_sessions(Rest, SubscriberId, true);
        {error, overloaded} ->
            %% it's safe to return an error, the CONNECT req will be nacked,
            %% and the client will retry...
            {error, overloaded}
    end;
cleanup_sessions([#session{pid=SessionPid,
                           monitor=MRef,
                           clean=false}|Rest], SubscriberId, true) ->
    demonitor(MRef, [flush]),
    ets:delete(vmq_session_mons, MRef),
    _ = disconnect_subscriber(SessionPid),
    cleanup_sessions(Rest, SubscriberId, true);
cleanup_sessions([_|Rest], SubscriberId, RemoveOldSess) ->
    % in case we allow multiple sessions, we land here
    cleanup_sessions(Rest, SubscriberId, RemoveOldSess);
cleanup_sessions([], _, _) -> ok.
