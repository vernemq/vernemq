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
-behaviour(gen_server).

%% API
-export([
         %% used in mqtt fsm handling
         subscribe/4,
         unsubscribe/4,
         register_subscriber/2,
         delete_subscriptions/1,
         %% used in mqtt fsm handling
         publish/1,

         %% used in :get_info/2
         get_session_pids/1,
         get_queue_pid/1,

         %% used in vmq_server_utils
         total_subscriptions/0,

         stored/1,
         status/1,

         migrate_offline_queues/1,
         fix_dead_queues/2

        ]).

%% gen_server
-export([start_link/0,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% called by vmq_cluster_com
-export([publish/2]).

%% used from plugins
-export([direct_plugin_exports/1]).
%% used by reg views
-export([subscribe_subscriber_changes/0,
         fold_subscriptions/2,
         fold_subscribers/2,
         fold_subscribers/3]).
%% used by vmq_mqtt_fsm list_sessions
-export([fold_sessions/2]).

%% exported because currently used by netsplit tests
-export([subscriptions_for_subscriber_id/1]).

-define(SUBSCRIBER_DB, {vmq, subscriber}).
-define(TOMBSTONE, '$deleted').
-define(NR_OF_REG_RETRIES, 10).

-spec subscribe(flag(), username() | plugin_id(), subscriber_id(),
                [{topic(), qos()}]) -> {ok, [qos() | not_allowed]}
                                       | {error, not_allowed
                                       | overloaded
                                       | not_ready}.

subscribe(false, User, SubscriberId, Topics) ->
    %% trade availability for consistency
    vmq_cluster:if_ready(fun subscribe_/3, [User, SubscriberId, Topics]);
subscribe(true, User, SubscriberId, Topics) ->
    %% trade consistency for availability
    subscribe_(User, SubscriberId, Topics).

subscribe_(User, SubscriberId, Topics) ->
    case vmq_plugin:all_till_ok(auth_on_subscribe,
                                [User, SubscriberId, Topics]) of
        ok ->
            subscribe_op(User, SubscriberId, Topics);
        {ok, NewTopics} when is_list(NewTopics) ->
            subscribe_op(User, SubscriberId, NewTopics);
        {error, _} ->
            {error, not_allowed}
    end.

subscribe_op(User, SubscriberId, Topics) ->
    rate_limited_op(
      fun() ->
              add_subscriber(Topics, SubscriberId)
      end,
      fun(_) ->
              QoSTable =
              lists:foldl(fun ({_, not_allowed}, AccQoSTable) ->
                                  [not_allowed|AccQoSTable];
                              ({T, QoS}, AccQoSTable) when is_integer(QoS) ->
                                  deliver_retained(SubscriberId, T, QoS),
                                  [QoS|AccQoSTable]
                          end, [], Topics),
              vmq_plugin:all(on_subscribe, [User, SubscriberId, Topics]),
              {ok, lists:reverse(QoSTable)}
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
    TTopics =
    case vmq_plugin:all_till_ok(on_unsubscribe, [User, SubscriberId, Topics]) of
        ok ->
            Topics;
        {ok, [[W|_]|_] = NewTopics} when is_binary(W) ->
            NewTopics;
        {error, _} ->
            Topics
    end,
    rate_limited_op(
      fun() ->
              del_subscriptions(TTopics, SubscriberId)
      end,
      fun(_) ->
              ok
      end).

delete_subscriptions(SubscriberId) ->
    del_subscriber(SubscriberId).

-spec register_subscriber(subscriber_id(), map()) ->
    {ok, boolean(), pid()} | {error, _}.
register_subscriber(SubscriberId, #{allow_multiple_sessions := false} = QueueOpts) ->
    %% we don't allow multiple sessions using same subscriber id
    %% allow_multiple_sessions is needed for session balancing
    case jobs:ask(plumtree_queue) of
        {ok, JobId} ->
            try
                vmq_reg_leader:register_subscriber(self(), SubscriberId, QueueOpts)
            after
                jobs:done(JobId)
            end;
        {error, rejected} ->
            {error, overloaded}
    end;
register_subscriber(SubscriberId, #{allow_multiple_sessions := true} = QueueOpts) ->
    %% we allow multiple sessions using same subscriber id
    %%
    %% !!! CleanSession is disabled if multiple sessions are in use
    %%
    case jobs:ask(plumtree_queue) of
        {ok, JobId} ->
            try
                register_session(SubscriberId, QueueOpts)
            after
                jobs:done(JobId)
            end;
        {error, rejected} ->
            {error, overloaded}
    end.

-spec register_subscriber(pid() | undefined, subscriber_id(), map(), non_neg_integer()) ->
    {'ok', boolean(), pid()} | {error, any()}.
register_subscriber(_, _, _, 0) ->
    {error, register_subscriber_retry_exhausted};
register_subscriber(SessionPid, SubscriberId,
                    #{clean_session := CleanSession} = QueueOpts, N) ->
    % wont create new queue in case it already exists
    {ok, QueuePresent, QPid} = vmq_queue_sup:start_queue(SubscriberId),
    % remap subscriber... enabling that new messages will eventually
    % reach the new queue.
    % Remapping triggers remote nodes to initiate queue migration
    {SubscriptionsPresent, _ChangedNodes} = maybe_remap_subscriber(SubscriberId, QueueOpts),
    SessionPresent1 = SubscriptionsPresent or QueuePresent,
    SessionPresent2 =
    case CleanSession of
        true ->
            false; %% SessionPresent is always false in case CleanSession=true
        false when QueuePresent ->
            %% no migration expected to happen, as queue is already local.
            SessionPresent1;
        false ->
            %wait_for_changed_nodes(SubscriberId, ChangedNodes),
            %% wait_quorum(SubscriberId),
            vmq_queue:block_until_migrated(QPid),
            SessionPresent1
    end,
    case catch vmq_queue:add_session(QPid, SessionPid, QueueOpts) of
        {'EXIT', {normal, _}} ->
            %% queue went down in the meantime, retry
            register_subscriber(SessionPid, SubscriberId, QueueOpts, N -1);
        {'EXIT', {noproc, _}} ->
            timer:sleep(100),
            %% queue was stopped in the meantime, retry
            register_subscriber(SessionPid, SubscriberId, QueueOpts, N -1);
        {'EXIT', Reason} ->
            {error, Reason};
        {error, draining} ->
            %% queue is still draining it's offline queue to a different
            %% remote queue. This can happen if a client hops around
            %% different nodes very frequently... adjust load balancing!!
            timer:sleep(100),
            register_subscriber(SessionPid, SubscriberId, QueueOpts, N -1);
        ok ->
            {ok, SessionPresent2, QPid}
    end.

-spec register_session(subscriber_id(), map()) -> {ok, pid()}.
register_session(SubscriberId, QueueOpts) ->
    %% register_session allows to have multiple subscribers connected
    %% with the same session_id (as oposed to register_subscriber)
    SessionPid = self(),
    {ok, QueuePresent, QPid} = vmq_queue_sup:start_queue(SubscriberId), % wont create new queue in case it already exists
    ok = vmq_queue:add_session(QPid, SessionPid, QueueOpts),
    %% TODO: How to handle SessionPresent flag for allow_multiple_sessions=true
    SessionPresent = QueuePresent,
    {ok, SessionPresent, QPid}.

-spec publish(msg()) -> 'ok' | {'error', _}.
publish(#vmq_msg{trade_consistency=true,
                 reg_view=RegView,
                 mountpoint=MP,
                 routing_key=Topic,
                 payload=Payload,
                 retain=IsRetain} = Msg) ->
    %% trade consistency for availability
    %% if the cluster is not consistent at the moment, it is possible
    %% that subscribers connected to other nodes won't get this message
    case IsRetain of
        true when Payload == <<>> ->
            %% retain delete action
            vmq_retain_srv:delete(MP, Topic),
            vmq_reg_view:fold(RegView, MP, Topic, fun publish/2, Msg#vmq_msg{retain=false}),
            ok;
        true ->
            %% retain set action
            vmq_retain_srv:insert(MP, Topic, Payload),
            vmq_reg_view:fold(RegView, MP, Topic, fun publish/2, Msg#vmq_msg{retain=false}),
            ok;
        false ->
            vmq_reg_view:fold(RegView, MP, Topic, fun publish/2, Msg),
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
        true when (IsRetain == true) and (Payload == <<>>) ->
            %% retain delete action
            vmq_retain_srv:delete(MP, Topic),
            vmq_reg_view:fold(RegView, MP, Topic, fun publish/2, Msg#vmq_msg{retain=false}),
            ok;
        true when (IsRetain == true) ->
            %% retain set action
            vmq_retain_srv:insert(MP, Topic, Payload),
            vmq_reg_view:fold(RegView, MP, Topic, fun publish/2, Msg#vmq_msg{retain=false}),
            ok;
        true ->
            vmq_reg_view:fold(RegView, MP, Topic, fun publish/2, Msg),
            ok;
        false ->
            {error, not_ready}
    end.

%% publish/2 is used as the fold function in RegView:fold/4
publish({SubscriberId, QoS}, Msg) ->
    publish(Msg, QoS, get_queue_pid(SubscriberId));
publish(Node, Msg) ->
    case vmq_cluster:publish(Node, Msg) of
        ok ->
            Msg;
        {error, Reason} ->
            lager:warning("can't publish to remote node ~p due to '~p'", [Node, Reason]),
            Msg
    end.

publish(Msg, _, not_found) -> Msg;
publish(Msg, QoS, QPid) ->
    ok = vmq_queue:enqueue(QPid, {deliver, QoS, Msg}),
    Msg.

-spec deliver_retained(subscriber_id(), topic(), qos()) -> 'ok'.
deliver_retained({MP, _} = SubscriberId, Topic, QoS) ->
    QPid = get_queue_pid(SubscriberId),
    vmq_retain_srv:match_fold(
      fun ({T, Payload}, _) ->
              Msg = #vmq_msg{routing_key=T,
                             payload=Payload,
                             retain=true,
                             qos=QoS,
                             dup=false,
                             mountpoint=MP,
                             msg_ref=vmq_mqtt_fsm:msg_ref()},
              vmq_queue:enqueue(QPid, {deliver, QoS, Msg})
      end, ok, MP, Topic).

subscriptions_for_subscriber_id(SubscriberId) ->
    plumtree_metadata:get(?SUBSCRIBER_DB, SubscriberId, [{default, []}]).

migrate_offline_queues([]) -> exit(no_target_available);
migrate_offline_queues(Targets) ->
    {_, NrOfQueues, TotalMsgs} = vmq_queue_sup:fold_queues(fun migrate_offline_queue/3, {Targets, 0, 0}),
    lager:info("MIGRATION SUMMARY: ~p queues migrated, ~p messages", [NrOfQueues, TotalMsgs]),
    ok.

migrate_offline_queue(SubscriberId, QPid, {[Target|Targets], AccQs, AccMsgs} = Acc) ->
    try vmq_queue:status(QPid) of
        {_, _, _, _, true} ->
            %% this is a queue belonging to a plugin.. ignore it.
            Acc;
        {offline, _, TotalStoredMsgs, _, _} ->
            OldNode = node(),
            %% Remap Subscriptions, taking into account subscriptions
            %% on other nodes by only remapping subscriptions on 'OldNode'
            case plumtree_metadata:get(?SUBSCRIBER_DB, SubscriberId) of
                undefined ->
                    ignore;
                [] ->
                    ignore;
                Subs ->
                    NewSubs =
                    lists:foldl(
                      fun({Topic, QoS, Node}, SubsAcc) when Node == OldNode ->
                              [{Topic, QoS, Target}|SubsAcc];
                         (Sub, SubsAcc) ->
                              [Sub|SubsAcc]
                      end, [], Subs),
                    %% writing the changed subscriptions will trigger
                    %% vmq_reg_mgr to initiate queue migration
                    plumtree_metadata:put(?SUBSCRIBER_DB, SubscriberId,
                                          lists:usort(NewSubs)),

                    %% block until 'Target' has changes
                    has_remote_subscriptions_changed(SubscriberId, Target),
                    case rpc:call(Target, vmq_queue_sup, get_queue_pid, [SubscriberId])
                    of
                        RemoteQPid when is_pid(RemoteQPid) ->
                            vmq_queue:block_until_migrated(RemoteQPid);
                        _ ->
                            ignore
                    end
            end,
            {Targets ++ [Target], AccQs + 1, AccMsgs + TotalStoredMsgs};
        _ ->
            Acc
    catch
        _:_ ->
            %% queue stopped in the meantime, that's ok.
            Acc
    end.

has_remote_subscriptions_changed(SubscriberId, Node) ->
    has_remote_subscriptions_changed(SubscriberId,
                                     plumtree_metadata:get(?SUBSCRIBER_DB,
                                                           SubscriberId), Node).

has_remote_subscriptions_changed(_, undefined, _) -> ignore;
has_remote_subscriptions_changed(_, [], _) -> ignore;
has_remote_subscriptions_changed(SubscriberId, Subs, Node) ->
    case rpc:call(Node, plumtree_metadata, get, [?SUBSCRIBER_DB, SubscriberId])
    of
        Subs ->
            %% we're on the same page.
            ok;
        OtherSubs ->
            case [Sub || {_, _, N} = Sub <- OtherSubs, N == node()] of
                [] ->
                    %% someone else interfered with our change,
                    %% ignore, as migration will take place anyways
                    ignore;
                _ ->
                    %% no change has reached 'Node' yet
                    timer:sleep(100),
                    has_remote_subscriptions_changed(SubscriberId, Subs, Node)
            end
    end.

fix_dead_queues(_, []) -> exit(no_target_available);
fix_dead_queues(DeadNodes, AccTargets) ->
    %% DeadNodes must be a list of offline VerneMQ nodes
    %% Targets must be a list of online VerneMQ nodes
    {_, _, N} = fold_subscribers(fun fix_dead_queue/3, {DeadNodes, AccTargets, 0}, false),
    lager:info("FIX DEAD QUEUES SUMMARY: ~p queues fixed", [N]).

fix_dead_queue(SubscriberId, Subs, {DeadNodes, [Target|Targets], N}) ->
    %%% Why not use maybe_remap_subscriber/2:
    %%%  it is possible that the original subscriber has used
    %%%  allow_multiple_sessions=true
    %%%
    %%%  we only remap the subscriptions on dead nodes
    %%%  and ensure that a queue exist for such subscriptions.
    %%%  In case allow_multiple_sessions=false (default) all
    %%%  subscriptions will be remapped
    {NewSubs, HasChanged} =
    lists:foldl(
      fun({Topic, QoS, Node} = S, {AccSubs, Changed}) ->
              case lists:member(Node, DeadNodes) of
                  true ->
                      {[{Topic, QoS, Target}|AccSubs], true};
                  false ->
                      {[S|AccSubs], Changed}
              end
      end, {[], false}, Subs),
    case HasChanged of
        true ->
            %% writing the changed subscriptions will trigger the
            %% vmq_reg_mgr to initiate queue migration
            plumtree_metadata:put(?SUBSCRIBER_DB, SubscriberId, lists:usort(NewSubs)),
            %% block until 'Target' has changes
            has_remote_subscriptions_changed(SubscriberId, Target),
            case rpc:call(Target, vmq_queue_sup, get_queue_pid, [SubscriberId])
            of
                RemoteQPid when is_pid(RemoteQPid) ->
                    vmq_queue:block_until_migrated(RemoteQPid);
                _ ->
                    ignore
            end,
            {DeadNodes, Targets ++ [Target], N + 1};
        false ->
            {DeadNodes, Targets, N}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% GEN_SERVER,
%%%
%%%  this gen_server is mainly used to allow remote control over local
%%%  registry entries.. alternatively the rpc module could have been
%%%  used, however this allows us more control over how such remote
%%%  calls are handled. (in fact version prior to 0.12.0 used rpc directly.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, maps:new()}.

handle_call({finish_register_subscriber_by_leader, SessionPid, SubscriberId, QueueOpts}, From, Waiting) ->
    %% called by vmq_reg_leader process
    {Pid, MRef} = async_op(
                    fun() ->
                            register_subscriber(SessionPid, SubscriberId,
                                                QueueOpts, ?NR_OF_REG_RETRIES)
                    end, From),
    {noreply, maps:put(MRef, {Pid, From}, Waiting)}.

handle_cast(_Req, State) ->
    {noreply, State}.

handle_info({'DOWN', MRef, process, _Pid, _}, Waiting) ->
    {noreply, maps:remove(MRef, Waiting)}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

async_op(Fun, From) ->
    spawn_monitor(
      fun() ->
              case catch Fun() of
                  {'EXIT', Reason} ->
                      gen_server:reply(From, {error, Reason}),
                      exit(Reason);
                  Ret ->
                      gen_server:reply(From, Ret)
              end
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
          vmq_config:get_env(default_reg_view, vmq_reg_trie)} of
        {TradeConsistency, DefaultRegView}
              when is_boolean(TradeConsistency)
                   and is_atom(DefaultRegView) ->
            MountPoint = "",
            ClientId = fun(T) ->
                               list_to_binary(
                                 base64:encode_to_string(
                                   integer_to_binary(
                                     erlang:phash2(T)
                                    )
                                  ))
                       end,
            CallingPid = self(),
            SubscriberId = {MountPoint, ClientId(CallingPid)},
            User = {plugin, Mod, CallingPid},

            RegisterFun =
            fun() ->
                    PluginPid = self(),
                    wait_til_ready(),
                    PluginSessionPid = spawn_link(
                                         fun() ->
                                                 monitor(process, PluginPid),
                                                 plugin_queue_loop(PluginPid, Mod)
                                         end),
                    QueueOpts = maps:merge(vmq_queue:default_opts(),
                                           #{clean_session => true,
                                             is_plugin => true}),
                    {ok, _, _} = register_subscriber(PluginSessionPid, SubscriberId,
                                                     QueueOpts, ?NR_OF_REG_RETRIES),
                    ok
            end,

            PublishFun =
            fun([W|_] = Topic, Payload, Opts) when is_binary(W)
                                                    and is_binary(Payload)
                                                    and is_map(Opts) ->
                    wait_til_ready(),
                    %% allow a plugin developer to override
                    %% - mountpoint
                    %% - dup flag
                    %% - retain flag
                    %% - trade-consistency flag
                    %% - reg_view
                    Msg = #vmq_msg{
                             routing_key=Topic,
                             mountpoint=maps:get(mountpoint, Opts, MountPoint),
                             payload=Payload,
                             msg_ref=vmq_mqtt_fsm:msg_ref(),
                             qos = maps:get(qos, Opts, 0),
                             dup=maps:get(dup, Opts, false),
                             retain=maps:get(retain, Opts, false),
                             trade_consistency=maps:get(trade_consistency, Opts,
                                                        TradeConsistency),
                             reg_view=maps:get(reg_view, Opts, DefaultRegView)
                            },
                    publish(Msg)
            end,

            SubscribeFun =
            fun([W|_] = Topic) when is_binary(W) ->
                    wait_til_ready(),
                    CallingPid = self(),
                    User = {plugin, Mod, CallingPid},
                    subscribe(TradeConsistency, User,
                              {MountPoint, ClientId(CallingPid)}, [{Topic, 0}]);
               (_) ->
                    {error, invalid_topic}
            end,

            UnsubscribeFun =
            fun([W|_] = Topic) when is_binary(W) ->
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
        {vmq_mqtt_fsm, {mail, QPid, new_data}} ->
            vmq_queue:active(QPid),
            plugin_queue_loop(PluginPid, PluginMod);
        {vmq_mqtt_fsm, {mail, QPid, Msgs, _, _}} ->
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
        {info_req, {Ref, CallerPid}, _} ->
            CallerPid ! {Ref, {error, i_am_a_plugin}},
            plugin_queue_loop(PluginPid, PluginMod);
        disconnect ->
            ok;
        {'DOWN', _MRef, process, PluginPid, Reason} ->
            case (Reason == normal) or (Reason == shutdown) of
                true ->
                    ok;
                false ->
                    lager:warning("Plugin Queue Loop for ~p stopped due to ~p", [PluginMod, Reason])
            end;
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


fold_subscriptions(FoldFun, Acc) ->
    Node = node(),
    fold_subscribers(
      fun ({MP, _} = SubscriberId, Subs, AccAcc) ->
              lists:foldl(
                fun({Topic, QoS, N}, AccAccAcc) when Node == N ->
                        FoldFun({MP, Topic, {SubscriberId, QoS, undefined}},
                                        AccAccAcc);
                   ({Topic, _, N}, AccAccAcc) ->
                        FoldFun({MP, Topic, N}, AccAccAcc)
                end, AccAcc, Subs)
      end, Acc, false).

fold_subscribers(FoldFun, Acc) ->
    fold_subscribers(FoldFun, Acc, true).

fold_subscribers(FoldFun, Acc, CompactResult) ->
    plumtree_metadata:fold(
      fun ({_, ?TOMBSTONE}, AccAcc) -> AccAcc;
          ({SubscriberId, Subs}, AccAcc) when CompactResult ->
              FoldFun(SubscriberId, subscriber_nodes(Subs), AccAcc);
          ({SubscriberId, Subs}, AccAcc) ->
              FoldFun(SubscriberId, Subs, AccAcc)
      end, Acc, ?SUBSCRIBER_DB,
      [{resolver, lww}]).

%% returns the nodes a subscriber was active
subscriber_nodes(Subs) ->
    subscriber_nodes(Subs, []).
subscriber_nodes([], Nodes) -> Nodes;
subscriber_nodes([{_, _, Node}|Rest], Nodes) ->
    case lists:member(Node, Nodes) of
        true ->
            subscriber_nodes(Rest, Nodes);
        false ->
            subscriber_nodes(Rest, [Node|Nodes])
    end.

fold_sessions(FoldFun, Acc) ->
    vmq_queue_sup:fold_queues(
      fun(SubscriberId, QPid, AccAcc) ->
              lists:foldl(
                fun(SessionPid, AccAccAcc) ->
                        FoldFun(SubscriberId, SessionPid, AccAccAcc)
                end, AccAcc, vmq_queue:get_sessions(QPid))
      end, Acc).

-spec add_subscriber([{topic(), qos() | not_allowed}], subscriber_id()) -> ok.
add_subscriber(Topics, SubscriberId) ->
    Node = node(),
    NewSubs =
    case plumtree_metadata:get(?SUBSCRIBER_DB, SubscriberId) of
        undefined ->
            %% not_allowed topics are filtered out here
            [{Topic, QoS, Node} || {Topic, QoS} <- Topics, is_integer(QoS)];
        Subs ->
            lists:foldl(fun ({_Topic, not_allowed}, NewSubsAcc) ->
                                NewSubsAcc;
                            ({Topic, QoS}, NewSubsAcc) ->
                                NewSub = {Topic, QoS, Node},
                                case lists:keyfind(Topic, 1, NewSubsAcc) of
                                    NewSub ->
                                        %% exactly the same subscription
                                        %% ignore it
                                        NewSubsAcc;
                                    {Topic, _, Node} ->
                                        %% same topic filter, but different qos
                                        %% replace subscription: [MQTT-3.8.4-3]
                                        lists:keyreplace(Topic, 1, NewSubsAcc,
                                                         NewSub);
                                    false ->
                                        %% new subscription
                                        [NewSub|NewSubsAcc]
                                end
                        end, Subs, Topics)
    end,
    plumtree_metadata:put(?SUBSCRIBER_DB, SubscriberId, NewSubs).


-spec del_subscriber(subscriber_id()) -> ok.
del_subscriber(SubscriberId) ->
    plumtree_metadata:delete(?SUBSCRIBER_DB, SubscriberId).

-spec del_subscriptions([topic()], subscriber_id()) -> ok.
del_subscriptions(Topics, SubscriberId) ->
    Subs = plumtree_metadata:get(?SUBSCRIBER_DB, SubscriberId, [{default, []}]),
    NewSubs =
    lists:foldl(fun({Topic, _, Node} = Sub, NewSubsAcc) ->
                        case Node == node() of
                            true ->
                                case lists:member(Topic, Topics) of
                                    true ->
                                        NewSubsAcc;
                                    false ->
                                        [Sub|NewSubsAcc]
                                end;
                            false ->
                                [Sub|NewSubsAcc]
                        end
                end, [], Subs),
    plumtree_metadata:put(?SUBSCRIBER_DB, SubscriberId, NewSubs).

%% the return value is used to inform the caller
%% if a session was already present for the given
%% subscriber id.
-spec maybe_remap_subscriber(subscriber_id(), map()) -> boolean().
maybe_remap_subscriber(SubscriberId, #{clean_session := true}) ->
    %% no need to remap, we can delete this subscriber
    del_subscriber(SubscriberId),
    {false, []};
maybe_remap_subscriber(SubscriberId, _) ->
    case plumtree_metadata:get(?SUBSCRIBER_DB, SubscriberId) of
        undefined ->
            {false, []};
        Subs ->
            Node = node(),
            {NewSubs, HasChanged, ChangedNodes} =
            lists:foldl(fun({Topic, QoS, N}, {Acc, _, ChNodes}) when N =/= Node ->
                                {[{Topic, QoS, Node}|Acc], true, [N|ChNodes]};
                           (Sub, {Acc, Changed, ChNodes}) ->
                                {[Sub|Acc], Changed, ChNodes}
                        end, {[], false, []}, Subs),
            case HasChanged of
                true ->
                    plumtree_metadata:put(?SUBSCRIBER_DB, SubscriberId, lists:usort(NewSubs));
                false ->
                    ignore
            end,
            {true, lists:usort(ChangedNodes)}
    end.

-spec get_session_pids(subscriber_id()) ->
    {'error','not_found'} | {'ok', pid(), [pid()]}.
get_session_pids(SubscriberId) ->
    case get_queue_pid(SubscriberId) of
        not_found ->
            {error, not_found};
        QPid ->
            Pids = vmq_queue:get_sessions(QPid),
            {ok, QPid, Pids}
    end.

-spec get_queue_pid(subscriber_id()) -> pid() | not_found.
get_queue_pid(SubscriberId) ->
    vmq_queue_sup:get_queue_pid(SubscriberId).

total_subscriptions() ->
    Total = plumtree_metadata:fold(
              fun ({_, ?TOMBSTONE}, Acc) -> Acc;
                  ({_, Subs}, Acc) ->
                      Acc + length(Subs)
              end, 0, ?SUBSCRIBER_DB,
              [{resolver, lww}]),
    [{total, Total}].

stored(SubscriberId) ->
    case get_queue_pid(SubscriberId) of
        not_found -> 0;
        QPid ->
            {_, _, Queued, _, _} = vmq_queue:status(QPid),
            Queued
    end.

status(SubscriberId) ->
    case get_queue_pid(SubscriberId) of
        not_found -> {error, not_found};
        QPid ->
            {ok, vmq_queue:status(QPid)}
    end.

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
