%% Copyright 2018 Erlio GmbH Basel Switzerland (http://erl.io)
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
-export([
         %% used in mqtt fsm handling
         subscribe/4,
         unsubscribe/4,
         register_subscriber/3,
         register_subscriber/4, %% used during testing
         delete_subscriptions/1,
         %% used in mqtt fsm handling
         publish/3,

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

%% called by vmq_cluster_com
-export([publish/2]).

%% used from plugins
-export([direct_plugin_exports/1]).
%% used by reg views
-export([subscribe_subscriber_changes/0,
         fold_subscriptions/2,
         fold_subscribers/2]).

%% exported because currently used by netsplit tests
-export([subscriptions_for_subscriber_id/1]).

-define(NR_OF_REG_RETRIES, 10).

-spec subscribe(flag(), username() | plugin_id(), subscriber_id(),
                [{topic(), qos()}]) -> {ok, [qos() | not_allowed]}
                                       | {error, not_allowed | not_ready}.

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
    add_subscriber(lists:usort(Topics), SubscriberId),
    QoSTable =
    lists:foldl(fun ({_, not_allowed}, AccQoSTable) ->
                        [not_allowed|AccQoSTable];
                    ({T, QoS}, AccQoSTable) when is_integer(QoS) ->
                        deliver_retained(SubscriberId, T, QoS),
                        [QoS|AccQoSTable]
                end, [], Topics),
    vmq_plugin:all(on_subscribe, [User, SubscriberId, Topics]),
    {ok, lists:reverse(QoSTable)}.

-spec unsubscribe(flag(), username() | plugin_id(),
                  subscriber_id(), [topic()]) -> ok | {error, not_ready}.
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
    del_subscriptions(TTopics, SubscriberId).

delete_subscriptions(SubscriberId) ->
    del_subscriber(SubscriberId).

-spec register_subscriber(flag(), subscriber_id(), map()) ->
    {ok, boolean(), pid()} | {error, _}.
register_subscriber(CAPAllowRegister, SubscriberId, #{allow_multiple_sessions := false} = QueueOpts) ->
    %% we don't allow multiple sessions using same subscriber id
    %% allow_multiple_sessions is needed for session balancing
    SessionPid = self(),
    case vmq_cluster:is_ready() of
        true ->
            vmq_reg_sync:sync(SubscriberId,
                              fun() ->
                                      register_subscriber(SessionPid, SubscriberId,
                                                          QueueOpts, ?NR_OF_REG_RETRIES)
                              end, 60000);
        false when CAPAllowRegister ->
            %% synchronize action on this node
            vmq_reg_sync:sync(SubscriberId,
                              fun() ->
                                      register_subscriber(SessionPid, SubscriberId,
                                                          QueueOpts, ?NR_OF_REG_RETRIES)
                              end, node(), 60000);
        false ->
            {error, not_ready}
    end;
register_subscriber(CAPAllowRegister, SubscriberId, #{allow_multiple_sessions := true} = QueueOpts) ->
    %% we allow multiple sessions using same subscriber id
    %%
    %% !!! CleanSession is disabled if multiple sessions are in use
    %%
    case vmq_cluster:is_ready() or CAPAllowRegister of
        true ->
            register_session(SubscriberId, QueueOpts);
        false ->
            {error, not_ready}
    end.

-spec register_subscriber(pid() | undefined, subscriber_id(), map(), non_neg_integer()) ->
    {'ok', boolean(), pid()} | {error, any()}.
register_subscriber(_, _, _, 0) ->
    {error, register_subscriber_retry_exhausted};
register_subscriber(SessionPid, SubscriberId,
                    #{clean_session := CleanSession} = QueueOpts, N) ->
    % wont create new queue in case it already exists
    {ok, QueuePresent, QPid} =
    case vmq_queue_sup_sup:start_queue(SubscriberId) of
        {ok, true, OldQPid} when CleanSession ->
            %% cleanup queue
            vmq_queue:cleanup(OldQPid),
            vmq_queue_sup_sup:start_queue(SubscriberId);
        Ret -> Ret
    end,
    % remap subscriber... enabling that new messages will eventually
    % reach the new queue.
    % Remapping triggers remote nodes to initiate queue migration
    {SubscriptionsPresent, UpdatedSubs, ChangedNodes}
    = maybe_remap_subscriber(SubscriberId, QueueOpts),
    SessionPresent1 = SubscriptionsPresent or QueuePresent,
    SessionPresent2 =
    case CleanSession of
        true ->
            false; %% SessionPresent is always false in case CleanSession=true
        false when QueuePresent ->
            %% no migration expected to happen, as queue is already local.
            SessionPresent1;
        false ->
            Fun = fun(Sid, OldNode) ->
                          case rpc:call(OldNode, ?MODULE, get_queue_pid, [Sid]) of
                              not_found ->
                                  case get_queue_pid(Sid) of
                                      not_found ->
                                          block;
                                      LocalPid when is_pid(LocalPid) ->
                                          done
                                  end;
                              OldPid when is_pid(OldPid) ->
                                  block
                          end
                  end,
            block_until_migrated(SubscriberId, UpdatedSubs, ChangedNodes, Fun),
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
        {error, cleanup} ->
            %% queue is still cleaning up.
            timer:sleep(100),
            register_subscriber(SessionPid, SubscriberId, QueueOpts, N -1);
        ok ->
            {ok, SessionPresent2, QPid}
    end.


%% block_until_migrated/4 has three cases to consider, the logic for
%% these cases are handled by the BlockCond function
%%
%% migrate queue to local node (register_subscriber):
%%
%%    we wait until there is no queue on the original node and until we have one locally
%%
%% migrate local queue to remote node (cluster leave):
%%
%%    we wait until there is no local queue and until there is one on the remote node.
%%
%% migrate dead queue (node down) to another node in cluster (including this one):
%%
%%    we have no local queue, but wait until the (offline) queue exists on target node.
block_until_migrated(_, _, [], _) -> ok;
block_until_migrated(SubscriberId, UpdatedSubs, [Node|Rest] = ChangedNodes, BlockCond) ->
    %% the call to subscriptions_for_subscriber_id will resolve any remaining
    %% conflicts to this entry by broadcasting the resolved value to the
    %% other nodes
    case subscriptions_for_subscriber_id(SubscriberId) of
        UpdatedSubs -> ok;
        _ ->
            %% in case the subscriptions were resolved elsewhere in the meantime
            %% we'll write 'our' version of the remapped subscriptions
            vmq_subscriber_db:store(SubscriberId, UpdatedSubs)
    end,

    case BlockCond(SubscriberId, Node) of
        block ->
            timer:sleep(100),
            block_until_migrated(SubscriberId, UpdatedSubs, ChangedNodes, BlockCond);
        done ->
            block_until_migrated(SubscriberId, UpdatedSubs, Rest, BlockCond)
    end.

-spec register_session(subscriber_id(), map()) -> {ok, boolean(), pid()}.
register_session(SubscriberId, QueueOpts) ->
    %% register_session allows to have multiple subscribers connected
    %% with the same session_id (as oposed to register_subscriber)
    SessionPid = self(),
    {ok, QueuePresent, QPid} = vmq_queue_sup_sup:start_queue(SubscriberId), % wont create new queue in case it already exists
    ok = vmq_queue:add_session(QPid, SessionPid, QueueOpts),
    %% TODO: How to handle SessionPresent flag for allow_multiple_sessions=true
    SessionPresent = QueuePresent,
    {ok, SessionPresent, QPid}.

publish(RegView, MP, Topic, FoldFun, #vmq_msg{sg_policy = SGPolicy} = Msg) ->
    Acc = publish_fold_acc(Msg),
    {NewMsg, SubscriberGroups} = vmq_reg_view:fold(RegView, MP, Topic, FoldFun, Acc),
    vmq_shared_subscriptions:publish(NewMsg, SGPolicy, SubscriberGroups).

publish_fold_acc(Msg) -> {Msg, undefined}.

-spec publish(flag(), module(), msg()) -> 'ok' | {'error', _}.
publish(true, RegView, #vmq_msg{mountpoint=MP,
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

            publish(RegView, MP, Topic, fun publish/2, Msg#vmq_msg{retain=false}),
            ok;
        true ->
            %% retain set action
            vmq_retain_srv:insert(MP, Topic, Payload),
            publish(RegView, MP, Topic, fun publish/2, Msg#vmq_msg{retain=false}),
            ok;
        false ->
            publish(RegView, MP, Topic, fun publish/2, Msg),
            ok
    end;
publish(false, RegView, #vmq_msg{mountpoint=MP,
                                 routing_key=Topic,
                                 payload=Payload,
                                 retain=IsRetain} = Msg) ->
    %% don't trade consistency for availability
    case vmq_cluster:is_ready() of
        true when (IsRetain == true) and (Payload == <<>>) ->
            %% retain delete action
            vmq_retain_srv:delete(MP, Topic),
            publish(RegView, MP, Topic, fun publish/2, Msg#vmq_msg{retain=false}),
            ok;
        true when (IsRetain == true) ->
            %% retain set action
            vmq_retain_srv:insert(MP, Topic, Payload),
            publish(RegView, MP, Topic, fun publish/2, Msg#vmq_msg{retain=false}),
            ok;
        true ->
            publish(RegView, MP, Topic, fun publish/2, Msg),
            ok;
        false ->
            {error, not_ready}
    end.

%% publish/2 is used as the fold function in RegView:fold/4
publish({{_,_} = SubscriberId, QoS}, {Msg, _} = Acc) ->
    case get_queue_pid(SubscriberId) of
        not_found -> Acc;
        QPid ->
            ok = vmq_queue:enqueue(QPid, {deliver, QoS, Msg}),
            Acc
    end;
publish({_Node, _Group, _SubscriberId, _QoS} = Sub, {Msg, SubscriberGroups}) ->
    %% collect subscriber group members for later processing
    {Msg, add_to_subscriber_group(Sub, SubscriberGroups)};
publish(Node, {Msg, _} = Acc) ->
    case vmq_cluster:publish(Node, Msg) of
        ok ->
            Acc;
        {error, Reason} ->
            lager:warning("can't publish to remote node ~p due to '~p'", [Node, Reason]),
            Acc
    end.

add_to_subscriber_group(Sub, undefined) ->
    add_to_subscriber_group(Sub, #{});
add_to_subscriber_group({Node, Group, SubscriberId, QoS}, SubscriberGroups) ->
    SubscriberGroup = maps:get(Group, SubscriberGroups, []),
    maps:put(Group, [{Node, SubscriberId, QoS}|SubscriberGroup],
             SubscriberGroups).

-spec deliver_retained(subscriber_id(), topic(), qos()) -> 'ok'.
deliver_retained(_SubscriberId, [<<"$share">>|_], _QoS) ->
    %% Never deliver retained messages to subscriber groups.
    ok;
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
    Default = [],
    vmq_subscriber_db:read(SubscriberId, Default).

migrate_offline_queues([]) -> exit(no_target_available);
migrate_offline_queues(Targets) ->
    {_, NrOfQueues, TotalMsgs} = vmq_queue_sup_sup:fold_queues(fun migrate_offline_queue/3, {Targets, 0, 0}),
    lager:info("migration summary: ~p queues migrated, ~p messages", [NrOfQueues, TotalMsgs]),
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
            Subs = subscriptions_for_subscriber_id(SubscriberId),
            NewSubs = vmq_subscriber:change_node(Subs, OldNode, Target, false),

            %% writing the changed subscriptions will trigger
            %% vmq_reg_mgr to initiate queue migration
            Fun =
                fun(Sid, TargetNode) ->
                        case get_queue_pid(Sid) of
                            not_found ->
                                case rpc:call(TargetNode, ?MODULE, get_queue_pid, [Sid]) of
                                    not_found ->
                                        lager:error("couldn't migrate queue for ~p to target node ~p",
                                                    [Sid, TargetNode]),
                                        done;
                                    LocalPid when is_pid(LocalPid) ->
                                        done
                                end;
                            Pid when is_pid(Pid) ->
                                block
                        end
                end,
            block_until_migrated(SubscriberId, NewSubs, [Target], Fun),
            {Targets ++ [Target], AccQs + 1, AccMsgs + TotalStoredMsgs};
        _ ->
            Acc
    catch
        _:_ ->
            %% queue stopped in the meantime, that's ok.
            Acc
    end.

fix_dead_queues(_, []) -> exit(no_target_available);
fix_dead_queues(DeadNodes, AccTargets) ->
    %% DeadNodes must be a list of offline VerneMQ nodes
    %% Targets must be a list of online VerneMQ nodes
    {_, _, N} = fold_subscribers(fun fix_dead_queue/3, {DeadNodes, AccTargets, 0}),
    lager:info("dead queues summary: ~p queues fixed", [N]).

fix_dead_queue(SubscriberId, Subs, {DeadNodes, [Target|Targets], N}) ->
    %%% Why not use maybe_remap_subscriber/2:
    %%%  it is possible that the original subscriber has used
    %%%  allow_multiple_sessions=true
    %%%
    %%%  we only remap the subscriptions on dead nodes
    %%%  and ensure that a queue exist for such subscriptions.
    %%%  In case allow_multiple_sessions=false (default) all
    %%%  subscriptions will be remapped
    NewSubs =
    lists:foldl(
      fun(DeadNode, AccSubs) ->
              %% Remapping the supbscriptions from DeadNode to
              %% TargetNode. In case a 'new' session is already
              %% present at TargetNode, we're merging the subscriptions
              %% IF it is using clean_session=false. IF it is using
              %% clean_session=true, the subscriptions are replaced.
              vmq_subscriber:change_node(AccSubs, DeadNode, Target, false)
      end, Subs, DeadNodes),
    case Subs of
        NewSubs ->
            %% no change
            {DeadNodes, [Target|Targets], N};
        _ ->
            Fun = fun(Sid, Tgt) ->
                          case rpc:call(Tgt, ?MODULE, get_queue_pid, [Sid]) of
                              not_found ->
                                  block;
                              Pid when is_pid(Pid) ->
                                  done
                          end
                  end,
            block_until_migrated(SubscriberId, NewSubs, [Target], Fun),
            {DeadNodes, Targets ++ [Target], N + 1}
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
    CAPPublish = vmq_config:get_env(allow_publish_during_netsplit, false),
    CAPSubscribe = vmq_config:get_env(allow_subscribe_during_netsplit, false),
    CAPUnsubscribe = vmq_config:get_env(allow_unsubscribe_during_netsplit, false),
    SGPolicyConfig = vmq_config:get_env(shared_subscription_policy, prefer_local),
    RegView = vmq_config:get_env(default_reg_view, vmq_reg_trie),
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
            %% - shared subscription policy
            Msg = #vmq_msg{
                     routing_key=Topic,
                     mountpoint=maps:get(mountpoint, Opts, MountPoint),
                     payload=Payload,
                     msg_ref=vmq_mqtt_fsm:msg_ref(),
                     qos = maps:get(qos, Opts, 0),
                     dup=maps:get(dup, Opts, false),
                     retain=maps:get(retain, Opts, false),
                     sg_policy=maps:get(shared_subscription_policy, Opts, SGPolicyConfig)
                    },
            publish(CAPPublish, RegView, Msg)
    end,

    SubscribeFun =
    fun([W|_] = Topic) when is_binary(W) ->
            wait_til_ready(),
            CallingPid = self(),
            User = {plugin, Mod, CallingPid},
            subscribe(CAPSubscribe, User,
                      {MountPoint, ClientId(CallingPid)}, [{Topic, 0}]);
       (_) ->
            {error, invalid_topic}
    end,

    UnsubscribeFun =
    fun([W|_] = Topic) when is_binary(W) ->
            wait_til_ready(),
            CallingPid = self(),
            User = {plugin, Mod, CallingPid},
            unsubscribe(CAPUnsubscribe, User,
                        {MountPoint, ClientId(CallingPid)}, [Topic]);
       (_) ->
            {error, invalid_topic}
    end,
    {RegisterFun, PublishFun, {SubscribeFun, UnsubscribeFun}}.

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
                                  lager:warning("dropped message ~p for plugin ~p", [Msg, PluginMod]),
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
                    lager:warning("plugin queue loop for ~p stopped due to ~p", [PluginMod, Reason])
            end;
        Other ->
            exit({unknown_msg_in_plugin_loop, Other})
    end.

subscribe_subscriber_changes() ->
    vmq_subscriber_db:subscribe_db_events().

fold_subscriptions(FoldFun, Acc) ->
    Node = node(),
    fold_subscribers(
      fun ({MP, _} = SubscriberId, Subs, AAcc) ->
              vmq_subscriber:fold(
                fun({Topic, QoS, N}, AAAcc) when Node == N ->
                        FoldFun({MP, Topic, {SubscriberId, QoS, undefined}}, AAAcc);
                   ({Topic, _QoS, N}, AAAcc) ->
                        FoldFun({MP, Topic, N}, AAAcc)
                end, AAcc, Subs)
      end, Acc).

fold_subscribers(FoldFun, Acc) ->
    vmq_subscriber_db:fold(
      fun ({SubscriberId, Subs}, AccAcc) ->
              FoldFun(SubscriberId, Subs, AccAcc)
      end, Acc).

-spec add_subscriber([{topic(), qos() | not_allowed}], subscriber_id()) -> ok.
add_subscriber(Topics, SubscriberId) ->
    OldSubs = subscriptions_for_subscriber_id(SubscriberId),
    case vmq_subscriber:add(OldSubs, [{T, QoS}||{T, QoS} <- Topics, is_integer(QoS)]) of
        {NewSubs, true} ->
            vmq_subscriber_db:store(SubscriberId, NewSubs);
        _ ->
            ok
    end.

-spec del_subscriber(subscriber_id()) -> ok.
del_subscriber(SubscriberId) ->
    vmq_subscriber_db:delete(SubscriberId).

-spec del_subscriptions([topic()], subscriber_id()) -> ok.
del_subscriptions(Topics, SubscriberId) ->
    OldSubs = subscriptions_for_subscriber_id(SubscriberId),
    case vmq_subscriber:remove(OldSubs, Topics) of
        {NewSubs, true} ->
            vmq_subscriber_db:store(SubscriberId, NewSubs);
        _ ->
            ok
    end.

%% the return value is used to inform the caller
%% if a session was already present for the given
%% subscriber id.
-spec maybe_remap_subscriber(subscriber_id(), map()) ->
    {boolean(), undefined | vmq_subscriber:subs(), [node()]}.
maybe_remap_subscriber(SubscriberId, #{clean_session := true}) ->
    %% no need to remap, we can delete this subscriber
    %% we overwrite any other value
    Subs = vmq_subscriber:new(true),
    vmq_subscriber_db:store(SubscriberId, Subs),
    {false, Subs, []};
maybe_remap_subscriber(SubscriberId, #{clean_session := CleanSession}) ->
    case vmq_subscriber_db:read(SubscriberId) of
        undefined ->
            %% Store empty Subscriber Data
            Subs = vmq_subscriber:new(CleanSession),
            vmq_subscriber_db:store(SubscriberId, Subs),
            {false, Subs, []};
        Subs ->
            case vmq_subscriber:change_node_all(Subs, node(), CleanSession) of
                {NewSubs, ChangedNodes} when length(ChangedNodes) > 0 ->
                    vmq_subscriber_db:store(SubscriberId, NewSubs),
                    {true, NewSubs, ChangedNodes};
                _ ->
                    {true, Subs, []}
            end
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
    vmq_queue_sup_sup:get_queue_pid(SubscriberId).

total_subscriptions() ->
    Total =
    vmq_subscriber_db:fold(
      fun({_, Subs}, Acc) ->
              vmq_subscriber:fold(fun(_, AccAcc) ->
                                          AccAcc + 1
                                  end, Acc, Subs)
      end, 0),
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

