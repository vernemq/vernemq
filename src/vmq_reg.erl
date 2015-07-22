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
         register_subscriber/5,
         %% used in vmq_session fsm handling
         publish/1,

         %% used in vmq_session:get_info/2
         get_subscriber_pids/1,

         %% used in vmq_server_utils
         client_stats/0,
         total_sessions/0,
         total_inactive_sessions/0,
         total_subscriptions/0,
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

%% used by/through remote calls
-export([teardown_session/2,
         publish/2,
         register_subscriber_/4]).

%% used from plugins
-export([direct_plugin_exports/1]).
%% used by reg views
-export([subscribe_subscriber_changes/0,
         fold_subscribers/2,
         fold_subscribers/3]).
%% used by vmq_session:list_sessions
-export([fold_sessions/2]).

%% called when remote message delivery successfully finished for node
-export([remap_subscription/2]).

%% exported because currently used by netsplit tests
-export([subscriptions_for_subscriber_id/1]).

-record(state, {}).
-record(session, {subscriber_id,
                  pid,
                  queue_pid,
                  monitor,
                  last_seen,
                  balance,
                  clean}).

-type state() :: #state{}.

-define(RETAIN_DB, {vmq, retain}).
-define(SUBSCRIBER_DB, {vmq, subscriber}).
-define(TOMBSTONE, '$deleted').

-spec start_link() -> {ok, pid()} | ignore | {error, atom()}.
start_link() ->
    case ets:info(vmq_session) of
        undefined ->
            _ = ets:new(vmq_session, [public,
                                      bag,
                                      named_table,
                                      {keypos, 2},
                                      {read_concurrency, true}]);
        _ ->
            %% ets table already exists, we'll remap the monitors
            %% in the init callback.
            ignore
    end,
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
            subscribe_op(User, SubscriberId, QPid, Topics);
        {error, _} ->
            {error, not_allowed}
    end.

subscribe_op(User, SubscriberId, QPid, Topics) ->
    rate_limited_op(
      fun() ->
              _ = [add_subscriber(T, QoS, SubscriberId)
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
    vmq_cluster:if_ready(fun unsubscribe_op/3, [User, SubscriberId, Topics]);
unsubscribe(true, User, SubscriberId, Topics) ->
    %% trade consistency for availability
    unsubscribe_op(User, SubscriberId, Topics).

unsubscribe_op(User, SubscriberId, Topics) ->
    rate_limited_op(
      fun() ->
              _ = [del_subscriber(T, SubscriberId) || T <- Topics],
              ok
      end,
      fun(_) ->
              _ = [vmq_exo:decr_subscription_count() || _ <- Topics],
              _ = vmq_plugin:all(on_unsubscribe, [User, SubscriberId, Topics]),
              ok
      end).

-spec register_subscriber(flag(), flag(), subscriber_id(), pid(), flag()) ->
    ok | {error, _}.
register_subscriber(false, _, SubscriberId, QPid, CleanSession) ->
    %% we don't allow multiple sessions using same subscriber id
    %% allow_multiple_sessions is needed for session balancing
    register_subscriber(SubscriberId, QPid, CleanSession);
register_subscriber(true, BalanceSessions, SubscriberId, QPid, _CleanSession) ->
    %% we allow multiple sessions using same subscriber id
    %%
    %% !!! CleanSession is disabled if multiple sessions are in use
    %%
    register_session(SubscriberId, BalanceSessions, QPid).

-spec register_subscriber(subscriber_id(), pid(), flag()) -> ok | {error, _}.
register_subscriber(SubscriberId, QPid, CleanSession) ->
    case vmq_reg_leader:register_subscriber(self(), QPid, SubscriberId,
                                            CleanSession) of
        ok when CleanSession == ?true ->
            %% no need to remap
            ok;
        ok ->
            Subs = subscriptions_for_subscriber_id(SubscriberId),
            RemapedNodes = lists:usort([N || {_,_,N} <- Subs]),
            vmq_session_proxy_sup:start_delivery(RemapedNodes, QPid, SubscriberId);
        R ->
            R
    end.

-spec register_session(subscriber_id(), flag(), pid()) -> ok | {error, _}.
register_session(SubscriberId, BalanceSessions, QPid) ->
    %% register_session allows to have multiple subscribers connected
    %% with the same session_id (as oposed to register_subscriber)
    SessionPid = self(),
    Subs = subscriptions_for_subscriber_id(SubscriberId),
    case gen_server:call(?MODULE,
                         {monitor, SessionPid, QPid, SubscriberId,
                          BalanceSessions, false, false},
                         infinity) of
        ok ->
            RemapedNodes = lists:usort([N || {_,_,N} <- Subs]),
            vmq_session_proxy_sup:start_delivery(RemapedNodes, QPid,
                                                 SubscriberId);
        {error, Reason} ->
            {error, Reason}
    end.

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

-spec register_subscriber_(pid(), pid(), subscriber_id(), flag()) ->
    'ok' | {error, overloaded}.
register_subscriber_(SessionPid, QPid, SubscriberId, CleanSession) ->
    %% cleanup session for this client id if needed
    case CleanSession of
        ?true ->
            rate_limited_op(
              fun() ->
                      del_subscriber(SubscriberId)
              end,
              fun(ok) ->
                      register_subscriber__(SessionPid, QPid, SubscriberId, true);
                 (E) -> E
              end);
        ?false ->
            register_subscriber__(SessionPid, QPid, SubscriberId, false)
    end.

-spec register_subscriber__(pid(), pid(), subscriber_id(), flag()) -> 'ok' | {error, _}.
register_subscriber__(SessionPid, QPid, SubscriberId, CleanSession) ->
    %% TODO: make this more efficient, currently we have to rpc every
    %% node in the cluster
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
                              SubscriberId, false, CleanSession, true}, infinity).

-spec publish(msg()) -> 'ok' | {'error', _}.
publish(#vmq_msg{trade_consistency=true,
                 reg_view=RegView,
                 mountpoint=MP,
                 routing_key=Topic,
                 payload=Paylaod,
                 retain=IsRetain} = Msg) ->
    %% trade consistency for availability
    %% if the cluster is not consistent at the moment, it is possible
    %% that subscribers connected to other nodes won't get this message
    case IsRetain of
        ?true when Paylaod == <<>> ->
            %% retain delete action
            rate_limited_op(fun() -> plumtree_metadata:delete(?RETAIN_DB,
                                                              {Topic}) end);
        ?true ->
            %% retain set action
            retain_msg(Msg);
        ?false ->
            RegView:fold(MP, Topic, fun publish/2, Msg),
            ok
    end;
publish(#vmq_msg{trade_consistency=false,
                 reg_view=RegView,
                 mountpoint=MP,
                 routing_key=Topic,
                 payload=Payload,
                 retain=IsRetain} = Msg) ->
    %% don't trade consistency for availability
    case vmq_cluster:is_ready() of
        true when (IsRetain == ?true) and (Payload == <<>>) ->
            %% retain delete action
            rate_limited_op(fun() -> plumtree_metadata:delete(?RETAIN_DB,
                                                              {Topic}) end);
        true when (IsRetain == ?true) ->
            %% retain set action
            retain_msg(Msg);
        true ->
            RegView:fold(MP, Topic, fun publish/2, Msg),
            ok;
        false ->
            {error, not_ready}
    end.

%% publish/2 is used as the fold function in RegView:fold/4
publish({SubscriberId, QoS}, Msg) ->
    QPids =
    case get_queue_pids(SubscriberId) of
        [] -> not_found;
        Pids ->
            Pids
    end,
    publish(SubscriberId, Msg, QoS, QPids);
publish(Node, Msg) ->
    case vmq_cluster:publish(Node, Msg) of
        ok ->
            Msg;
        {error, Reason} ->
            lager:warning("can't publish to remote node ~p due to '~p'", [Node, Reason]),
            Msg
    end.

publish(_, Msg, 0, not_found) -> Msg;
publish(SubscriberId, Msg, QoS, not_found) ->
    RefedMsg = vmq_msg_store:store(SubscriberId, Msg),
    vmq_msg_store:defer_deliver(SubscriberId, QoS, RefedMsg#vmq_msg.msg_ref),
    RefedMsg;
publish(SubscriberId, Msg, 0, [QPid|Rest]) ->
    _ = vmq_queue:enqueue_nb(QPid, {deliver, 0, Msg}),
    publish(SubscriberId, Msg, 0, Rest);
publish(SubscriberId, Msg, QoS, [QPid|Rest]) ->
    RefedMsg = vmq_msg_store:store(SubscriberId, Msg),
    case vmq_queue:enqueue(QPid, {deliver, QoS, RefedMsg}) of
        ok ->
            publish(SubscriberId, RefedMsg, QoS, Rest);
        {error, _} ->
            vmq_msg_store:defer_deliver(SubscriberId, QoS, RefedMsg#vmq_msg.msg_ref),
            publish(SubscriberId, RefedMsg, QoS, Rest)
    end;
publish(_, Msg, _, []) -> Msg.

retain_msg(Msg = #vmq_msg{mountpoint=MP, reg_view=RegView,
                          routing_key=Topic, payload=Payload}) ->
    rate_limited_op(
      fun() ->
              plumtree_metadata:put(?RETAIN_DB, {Topic}, Payload),
              Msg#vmq_msg{retain=false}
      end,
      fun(RetainedMsg) ->
              _ = RegView:fold(MP, Topic, fun publish/2, RetainedMsg),
              ok
      end).

-spec deliver_retained(subscriber_id(), pid(), topic(), qos()) -> 'ok'.
deliver_retained(SubscriberId, QPid, Topic, QoS) ->
    Words = [case W of
                 "+" -> '_';
                 _ -> W
             end || W <- vmq_topic:words(Topic)],
    NewWords =
    case lists:reverse(Words) of
        ["#"|Tail] -> lists:reverse(Tail) ++ '_' ;
        _ -> Words
    end,
    plumtree_metadata:fold(
      fun ({_, ?TOMBSTONE}, Acc) -> Acc;
          ({{T}, Payload}, _) ->
              Msg = #vmq_msg{routing_key=T,
                             payload=Payload,
                             retain=true,
                             qos=QoS,
                             dup=false},
              MaybeChangedMsg =
              case QoS of
                  0 -> Msg;
                  _ -> vmq_msg_store:store(SubscriberId, Msg)
              end,
              ok = vmq_queue:enqueue(QPid, {deliver, QoS, MaybeChangedMsg}),
              ok
      end, ok, ?RETAIN_DB,
      %% iterator opts
      [{match, {NewWords}},
       {resolver, lww}]).

subscriptions_for_subscriber_id(SubscriberId) ->
    plumtree_metadata:get(?SUBSCRIBER_DB, SubscriberId, [{default, []}]).

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
                                  ok = disconnect_subscriber(SubscriberPid)
                          end, SubscriberPids);
        E -> E
    end.

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
            CallingPid = self(),
            SubscriberId = {MountPoint, ClientId(CallingPid)},
            User = {plugin, Mod, CallingPid},

            RegisterFun =
            fun() ->
                    PluginPid = self(),
                    wait_til_ready(),
                    {ok, QPid} = vmq_queue:start_link(
                                   SubscriberId,
                                   spawn_link(
                                     fun() ->
                                             plugin_queue_loop(PluginPid, Mod)
                                     end)
                                   , QueueSize),
                    put(vmq_queue_pid, QPid),
                    register_subscriber_(PluginPid, QPid, SubscriberId, ?true)
            end,

            PublishFun =
            fun(Topic, Payload) ->
                    wait_til_ready(),
                    Msg = #vmq_msg{routing_key=vmq_topic:words(Topic),
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
                              get(vmq_queue_pid), [{Topic, 0}]);
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
                                  PluginPid ! {deliver, lists:flatten(vmq_topic:unword(RoutingKey)),
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
    plumtree_metadata_manager:subscribe(?SUBSCRIBER_DB),
    fun
        ({deleted, ?SUBSCRIBER_DB, _, Val})
          when (Val == ?TOMBSTONE) or (Val == undefined) ->
            ignore;
        ({deleted, ?SUBSCRIBER_DB, SubscriberId, Subscriptions}) ->
            {delete, SubscriberId, Subscriptions};
        ({updated, ?SUBSCRIBER_DB, SubscriberId, OldVal, NewSubs})
          when (OldVal == ?TOMBSTONE) or (OldVal == undefined) ->
            {update, SubscriberId, [], NewSubs};
        ({updated, ?SUBSCRIBER_DB, SubscriberId, OldSubs, NewSubs}) ->
            {update, SubscriberId, OldSubs -- NewSubs, NewSubs -- OldSubs};
        (_) ->
            ignore
    end.

fold_subscribers(FoldFun, Acc) ->
    fold_subscribers(false, FoldFun, Acc).

fold_subscribers(ResolveQPids, FoldFun, Acc) ->
    Node = node(),
    plumtree_metadata:fold(
      fun ({_, ?TOMBSTONE}, AccAcc) -> AccAcc;
          ({{MP, _} = SubscriberId, Subs}, AccAcc) ->
              lists:foldl(
                fun({Topic, QoS, N}, AccAccAcc) when Node == N ->
                        case ResolveQPids of
                            true ->
                                QPids = get_queue_pids(SubscriberId),
                                FoldFun({MP, Topic, {SubscriberId, QoS, QPids}},
                                        AccAccAcc);
                            false ->
                                FoldFun({MP, Topic, {SubscriberId, QoS, undefined}},
                                        AccAccAcc)
                        end;
                   ({Topic, _, N}, AccAccAcc) ->
                        FoldFun({MP, Topic, N}, AccAccAcc)
                end, AccAcc, Subs)
      end, Acc, ?SUBSCRIBER_DB,
      [{resolver, lww}]).

fold_sessions(FoldFun, Acc) ->
    ets:foldl(fun(#session{subscriber_id=SubscriberId,
                           pid=SessionPid}, AccAcc) ->
                      FoldFun(SubscriberId, SessionPid, AccAcc)
              end, Acc, vmq_session).


-spec add_subscriber(topic(), qos(), subscriber_id()) -> ok.
add_subscriber(Topic, QoS, SubscriberId) ->
    NewSubs =
    case plumtree_metadata:get(?SUBSCRIBER_DB, SubscriberId) of
        undefined ->
            [{Topic, QoS, node()}];
        Subs ->
            lists:usort([{Topic, QoS, node()}|Subs])
    end,
    plumtree_metadata:put(?SUBSCRIBER_DB, SubscriberId, NewSubs).


-spec del_subscriber(subscriber_id()) -> ok.
del_subscriber(SubscriberId) ->
    plumtree_metadata:delete(?SUBSCRIBER_DB, SubscriberId).

-spec del_subscriber(topic() | '_' , subscriber_id()) -> ok.
del_subscriber(Topic, SubscriberId) ->
    Subs = plumtree_metadata:get(?SUBSCRIBER_DB, SubscriberId, [{default, []}]),
    NewSubs = [Sub || {T, _, N} = Sub <- Subs, (T /= Topic) and (N /= node())],
    plumtree_metadata:put(?SUBSCRIBER_DB, SubscriberId, NewSubs).

-spec remap_subscription(subscriber_id(), atom()) -> ok | {error, overloaded}.
remap_subscription(SubscriberId, Node) ->
    %% Called as the last step of the remote message delivery. when we reach
    %% this step, the session has received and acked all messages that were
    %% persisted on `Node'.
    %%
    %% As the last action we remap the subscription, telling the cluster that
    %% the subscription has finally moved to this node. There's not much that
    %% can go wrong at this point, but we still have to load protect the last
    %% update to the ?SUBSCRIBER_DB. vmq_session_proxy process keeps retrying
    %% if rate_limited_op/1 returns {error, overloaded}.
    %%
    %% In case the session process goes down (for good and bad reasons) in the
    %% meantime, the subscription is not moved and retried upon the next connect.
    rate_limited_op(
      fun() ->
              plumtree_metadata:get(?SUBSCRIBER_DB, SubscriberId, [{default, []}])
      end,
      fun ({error, Reason}) -> {error, Reason};
          (Subs) ->
              Self = node(),
              NewSubs =
              lists:foldl(fun({Topic, QoS, N}, Acc) when N == Node ->
                                  [{Topic, QoS, Self}|Acc];
                             (Sub, Acc) ->
                                  [Sub|Acc]
                          end, [], Subs),
              plumtree_metadata:put(?SUBSCRIBER_DB, SubscriberId, NewSubs),
              ok
      end
     ).

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

-spec get_queue_pids(subscriber_id()) -> [pid()].
get_queue_pids(SubscriberId) ->
    case ets:lookup(vmq_session, SubscriberId) of
        [] ->
            [];
        [#session{queue_pid=Pid}] when is_pid(Pid) ->
            %% optimization
            [Pid];
        [#session{balance=false}|_] = Sessions ->
            [P || #session{queue_pid=P} <- Sessions, is_pid(P)];
        [#session{balance=true}|_] = Sessions ->
            case [P || #session{queue_pid=P} <- Sessions, is_pid(P)] of
                [] -> [];
                VSessions ->
                    [lists:nth(random:uniform(length(VSessions)), VSessions)]
            end;
        _ -> []
    end.


client_stats() ->
    TotalSessions = total_sessions(),
    TotalInactiveSessions = total_inactive_sessions(),
    [{total, TotalSessions},
     {active, TotalSessions - TotalInactiveSessions},
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

total_subscriptions() ->
    [{total, plumtree_metadata_manager:size(?SUBSCRIBER_DB)}].

-spec retained() -> non_neg_integer().
retained() ->
    plumtree_metadata_manager:size(?RETAIN_DB).

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
                                                      pid=undefined, _='_'},
                                             [{'<',
                                               '$1',
                                               ExpiredSince}],
                                             [['$2']]}], 100)).

remove_expired_subscribers_({[[SubscriberId]|Rest], Cont}) ->
    rate_limited_op(
      fun() -> del_subscriber(SubscriberId) end,
      fun(_) ->
              vmq_msg_store:clean_session(SubscriberId),
              _ = vmq_exo:incr_expired_clients(),
              remove_expired_subscribers_({Rest, Cont})
      end);
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
    process_flag(priority, high),
    set_monitors(),
    {ok, #state{}}.


-spec handle_call(_, _, []) -> {reply, ok, []}.
handle_call({monitor, SessionPid, QPid, SubscriberId, Balance,
             CleanSession, RemoveOldSess}, _From, State) ->
    cleanup_sessions(SubscriberId, RemoveOldSess),
    Monitor = monitor(process, SessionPid),
    Session = #session{subscriber_id=SubscriberId,
                       pid=SessionPid, queue_pid=QPid,
                       monitor=Monitor, last_seen=epoch(),
                       balance=Balance,
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
            ok;
        [#session{clean=true} = Obj] ->
            del_subscriber(SubscriberId),
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

-spec rate_limited_op(fun(() -> any())) -> any() | {error, overloaded}.
rate_limited_op(OpFun) ->
    rate_limited_op(OpFun, fun(Ret) -> Ret end).

-spec rate_limited_op(fun(() -> any()),
                      fun((any()) -> any())) -> any() | {error, overloaded}.
rate_limited_op(OpFun, SuccessFun) ->
    case jobs:ask(plumtree_queue) of
        {ok, JobId} ->
            try
                SuccessFun(OpFun())
            after
                jobs:done(JobId)
            end;
        {error, rejected} ->
            {error, overloaded}
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
                           clean=true} = Obj|Rest], SubscriberId, true) ->
    demonitor(MRef, [flush]),
    ets:delete(vmq_session_mons, MRef),
    ets:delete_object(vmq_session, Obj),
    _ = disconnect_subscriber(SessionPid),
    del_subscriber(SubscriberId),
    ok = vmq_msg_store:clean_session(SubscriberId),
    cleanup_sessions(Rest, SubscriberId, true);
cleanup_sessions([#session{pid=SessionPid,
                           monitor=MRef,
                           clean=false} = Obj|Rest], SubscriberId, true) ->
    demonitor(MRef, [flush]),
    ets:delete(vmq_session_mons, MRef),
    ets:delete_object(vmq_session, Obj),
    _ = disconnect_subscriber(SessionPid),
    cleanup_sessions(Rest, SubscriberId, true);
cleanup_sessions([_|Rest], SubscriberId, RemoveOldSess) ->
    % in case we allow multiple sessions, we land here
    cleanup_sessions(Rest, SubscriberId, RemoveOldSess);
cleanup_sessions([], _, _) -> ok.
