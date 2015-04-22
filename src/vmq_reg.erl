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
         register_session/3,
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

%% used by RPC calls
-export([teardown_session/2, publish_/2,
         register_subscriber_/4]).

%% used from plugins
-export([direct_plugin_exports/1]).
%% used by reg views
-export([subscribe_subscriber_changes/0,
         fold_subscribers/2,
         fold_subscribers/3]).
%% used by vmq_session:list_sessions
-export([fold_sessions/2]).

%% currently used by netsplit tests
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
        ok when not CleanSession ->
            vmq_session_proxy_sup:start_delivery(QPid, SubscriberId),
            ok;
        R ->
            R
    end.

-spec register_session(subscriber_id(), flag(), pid()) -> ok | {error, _}.
register_session(SubscriberId, BalanceSessions, QPid) ->
    %% register_session allows to have multiple subscribers connected
    %% with the same session_id (as oposed to register_subscriber)
    SessionPid = self(),
    rate_limited_op(
      fun() -> remap_session(SubscriberId) end,
      fun({error, Reason}) -> {error, Reason};
         (_) ->
              case gen_server:call(?MODULE,
                                   {monitor, SessionPid, QPid, SubscriberId,
                                    BalanceSessions, false, false},
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
            rate_limited_op(fun() -> del_subscriber(SubscriberId) end,
                            fun({error, Reason}) -> {error, Reason};
                               (_) -> register_subscriber__(SessionPid, QPid,
                                                            SubscriberId, true)
                            end);
        false ->
            rate_limited_op(fun() -> remap_subscriptions(SubscriberId) end,
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
                              SubscriberId, false, CleanSession, true}, infinity).

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
            rate_limited_op(fun() -> plumtree_metadata:delete(?RETAIN_DB,
                                                              {Words}) end);
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
            rate_limited_op(fun() -> plumtree_metadata:delete(?RETAIN_DB,
                                                              {Words}) end);
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
    plumtree_metadata:fold(
      fun({{SubscriberId, {_, QoS, _}}, N}, AccMsg) ->
              case Node of
                  N ->
                      QPids =
                      case get_queue_pids(SubscriberId) of
                          [] -> not_found;
                          Pids ->
                              Pids
                      end,
                      publish___(SubscriberId, AccMsg, QoS, QPids);
                  _ ->
                      AccMsg
              end
      end, Msg, ?SUBSCRIBER_DB,
     [{match, {'_', {Topic, '_', '_'}}},
      {resolver, lww}]);

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
publish_({_, '$deleted'}, Msg) ->
    Msg;
publish_({Topic, Node}, Msg) ->
    case vmq_cluster:publish(Node, {Topic, Msg}) of
        ok ->
            Msg;
        {error, Reason} ->
            lager:warning("can't publish to remote node ~p due to '~p'", [Node, Reason]),
            Msg
    end.

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
    rate_limited_op(
      fun() ->
              plumtree_metadata:put(?RETAIN_DB, {Words}, {RoutingKey, Payload}),
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
    plumtree_metadata:fold(
      fun({_Key, {RoutingKey, Payload}}, _) ->
              Msg = #vmq_msg{routing_key=RoutingKey,
                             payload=Payload,
                             retain=true,
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
    plumtree_metadata:fold(
      fun({{_, {Topic, QoS, _}}, _}, Acc) ->
              [{Topic, QoS}|Acc]
      end, [], ?SUBSCRIBER_DB,
      [{match, {SubscriberId, '_'}},
       {resolver, lww}]).

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
                    register_subscriber_(PluginPid, QPid, SubscriberId, true)
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
    plumtree_metadata_manager:subscribe(?SUBSCRIBER_DB),
    Node = node(),
    fun
        ({delete, ?SUBSCRIBER_DB, _, []}) ->
            %% no existing subscription was deleted
            ignore;
        ({delete, ?SUBSCRIBER_DB, {{MP, _} = SubscriberId, {Topic, QoS, _}}, [Metadata]}) ->
            case plumtree_metadata_object:values(Metadata) of
                [Node|_] ->
                    {unsubscribe, MP, Topic, {SubscriberId, QoS}};
                [OtherNode|_] ->
                    {unsubscribe, MP, Topic, OtherNode}
            end;
        ({write, ?SUBSCRIBER_DB, {{MP, _} = SubscriberId, {Topic, QoS, _}}, Metadata}) ->
            case plumtree_metadata_object:values(Metadata) of
                [Node|_] ->
                    QPids =
                    case get_queue_pids(SubscriberId) of
                        [] -> {error, not_found};
                        Pids ->  Pids
                    end,
                    {subscribe, MP, Topic, {SubscriberId, QoS, QPids}};
                [OtherNode|_] ->
                    {subscribe, MP, Topic, OtherNode}
            end;
        (_) ->
            ignore
    end.

fold_subscribers(FoldFun, Acc) ->
    fold_subscribers(false, FoldFun, Acc).

fold_subscribers(ResolveQPids, FoldFun, Acc) ->
    Node = node(),
    plumtree_metadata:fold(
      fun({{{MP, _} = SubscriberId, {Topic, QoS, _}}, N}, AccAcc) ->
              case Node == N of
                  true when ResolveQPids ->
                      QPids = get_queue_pids(SubscriberId),
                      FoldFun({MP, Topic, {SubscriberId, QoS, QPids}},
                              AccAcc);
                  true ->
                      FoldFun({MP, Topic, {SubscriberId, QoS, undefined}},
                              AccAcc);
                  false ->
                      FoldFun({MP, Topic, N}, AccAcc)
              end
      end, Acc, ?SUBSCRIBER_DB,
      [{resolver, lww}]
     ).

fold_sessions(FoldFun, Acc) ->
    ets:foldl(fun(#session{subscriber_id=SubscriberId,
                           pid=SessionPid}, AccAcc) ->
                      FoldFun(SubscriberId, SessionPid, AccAcc)
              end, Acc, vmq_session).


-spec add_subscriber(topic(), qos(), subscriber_id()) -> ok.
add_subscriber(Topic, QoS, SubscriberId) ->
    add_subscriber(Topic, QoS, SubscriberId, os:timestamp()).
add_subscriber(Topic, QoS, SubscriberId, Timestamp) ->
    Key = {SubscriberId, {Topic, QoS, Timestamp}},
    plumtree_metadata:put(?SUBSCRIBER_DB, Key, node()).


-spec del_subscriber(subscriber_id()) -> ok.
del_subscriber(SubscriberId) ->
    del_subscriber('_', SubscriberId).

-spec del_subscriber(topic() | '_' , subscriber_id()) -> ok.
del_subscriber(Topic, SubscriberId) ->
    plumtree_metadata:fold(
      fun({Key, _}, _) ->
              plumtree_metadata:delete(?SUBSCRIBER_DB, Key)
      end, ok, ?SUBSCRIBER_DB,
      [{match, {SubscriberId, {Topic, '_', '_'}}}]).

-spec remap_subscriptions(subscriber_id()) -> ok.
remap_subscriptions(SubscriberId) ->
    Node = node(),
    plumtree_metadata:fold(
      fun({{_, {Topic, QoS, TS}}, N}, _) ->
              case N of
                  Node -> ok;
                  _ ->
                      add_subscriber(Topic, QoS, SubscriberId, TS)
              end
      end, ok, ?SUBSCRIBER_DB,
      [{match, {SubscriberId, '_'}},
       {resolver, lww}]).

-spec remap_session(subscriber_id()) -> ok.
remap_session(SubscriberId) ->
    Node = node(),
    plumtree_metadata:fold(
      fun({{_, {Topic, QoS, _}}, N}, _) ->
              case N of
                  Node -> ok;
                  _ ->
                      add_subscriber(Topic, QoS, SubscriberId)
              end
      end, ok, ?SUBSCRIBER_DB,
      [{match, {SubscriberId, '_'}},
       {resolver, lww}]).


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
    process_flag(trap_exit, true),
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

rate_limited_op(OpFun) ->
    rate_limited_op(OpFun, fun(Ret) -> Ret end).
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
                           clean=true}|Rest], SubscriberId, true) ->
    demonitor(MRef, [flush]),
    ets:delete(vmq_session_mons, MRef),
    _ = disconnect_subscriber(SessionPid),
    del_subscriber(SubscriberId),
    ok = vmq_msg_store:clean_session(SubscriberId),
    cleanup_sessions(Rest, SubscriberId, true);
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
