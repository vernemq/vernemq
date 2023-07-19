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
-include_lib("vmq_commons/include/vmq_types.hrl").
-include("vmq_server.hrl").

-import(vmq_subscriber, [check_format/1]).

%% TODO: Add undefined in eredis:q spec return_value's list
-dialyzer({no_match, [subscribe_op/2, maybe_remap_subscriber/2]}).

%% API
-export([
    %% used in mqtt fsm handling
    subscribe/2,
    unsubscribe/2,
    register_subscriber/4,
    %% used during testing
    register_subscriber_/5,
    delete_subscriptions/1,
    %% used in mqtt fsm handling
    publish/3,

    %% used in :get_info/2
    get_session_pids/1,
    get_queue_pid/1,

    %% used in vmq_server_utils
    total_subscriptions/0,

    stored/1,

    enqueue_msg/2,

    migrate_offline_queue/2
]).

%% used from plugins
-export([
    direct_plugin_exports/1,
    direct_plugin_exports/2
]).
%% used by reg views
-export([
    subscribe_subscriber_changes/0,
    fold_subscriptions/2,
    fold_subscribers/2
]).

%% exported because currently used by queue & netsplit tests
-export([subscriptions_for_subscriber_id/1]).

%% exported for testing purposes
-export([retain_pre/1]).

-record(publish_fold_acc, {
    msg :: msg(),
    subscriber_groups = undefined :: undefined | map(),
    local_matches = 0 :: non_neg_integer(),
    remote_matches = 0 :: non_neg_integer()
}).

-define(NR_OF_REG_RETRIES, 10).
-define(DefaultRegView, application:get_env(vmq_server, default_reg_view, vmq_reg_trie)).

-spec subscribe(
    subscriber_id(),
    [subscription()]
) ->
    {ok, [qos() | not_allowed]}
    | {error, _}.
subscribe(SubscriberId, Topics) ->
    %% trade consistency for availability
    if_ready(fun subscribe_op/2, [SubscriberId, Topics]).

subscribe_op({MP, ClientId} = SubscriberId, Topics) ->
    update_qos1_metrics(Topics),
    {NumOfTopics, UnwordedTopicsWithBinaryQoS} =
        lists:foldr(
            fun
                ({T, QoS}, {Num, Acc}) when is_integer(QoS) ->
                    {Num + 1, [vmq_topic:unword(T), term_to_binary(QoS) | Acc]};
                ({T, {QoS, _Opts} = QoSWithOpts}, {Num, Acc}) when is_integer(QoS) ->
                    {Num + 1, [vmq_topic:unword(T), term_to_binary(QoSWithOpts) | Acc]};
                (_, Acc) ->
                    %% Filters not_allowed topics
                    Acc
            end,
            {0, []},
            lists:ukeysort(1, Topics)
        ),
    OldSubs =
        case
            vmq_redis:query(
                vmq_redis_client,
                [
                    ?FCALL,
                    ?SUBSCRIBE,
                    0,
                    MP,
                    ClientId,
                    node(),
                    os:system_time(nanosecond),
                    NumOfTopics
                    | UnwordedTopicsWithBinaryQoS
                ],
                ?FCALL,
                ?SUBSCRIBE
            )
        of
            {ok, [_, CS, NTWQ]} ->
                CleanSessionBool =
                    case CS of
                        <<"1">> -> true;
                        undefined -> false
                    end,
                NewTopicsWithQoS = [
                    {vmq_topic:word(Topic), binary_to_term(QoS)}
                 || [Topic, QoS] <- NTWQ
                ],
                [{node(), CleanSessionBool, NewTopicsWithQoS}];
            {ok, []} ->
                [];
            {ok, _} ->
                {error, unwanted_redis_response};
            Err ->
                Err
        end,
    case OldSubs of
        {error, _} = ErrRes ->
            ErrRes;
        _ ->
            lists:foreach(
                fun
                    ({[<<"$share">>, _Group | Topic], QoS}) ->
                        Key = {MP, Topic},
                        Value = {ClientId, QoS},
                        ets:insert(?SHARED_SUBS_ETS_TABLE, {{Key, Value}}),
                        vmq_metrics:incr_cache_insert(?LOCAL_SHARED_SUBS);
                    (_) ->
                        ok
                end,
                Topics
            ),
            Existing = subscriptions_exist(OldSubs, Topics),
            QoSTable =
                lists:foldl(
                    fun
                        %% MQTTv4 clauses
                        ({_, {_, not_allowed}}, AccQoSTable) ->
                            [not_allowed | AccQoSTable];
                        ({Exists, {T, QoS}}, AccQoSTable) when is_integer(QoS) ->
                            deliver_retained(SubscriberId, T, QoS, #{}, Exists),
                            [QoS | AccQoSTable];
                        %% MQTTv5 clauses and new MQTTv4 clause
                        ({_, {_, {not_allowed, _}}}, AccQoSTable) ->
                            [not_allowed | AccQoSTable];
                        ({Exists, {T, {QoS, SubOpts}}}, AccQoSTable) when
                            is_integer(QoS), is_map(SubOpts)
                        ->
                            deliver_retained(SubscriberId, T, QoS, SubOpts, Exists),
                            [QoS | AccQoSTable]
                    end,
                    [],
                    lists:zip(Existing, Topics)
                ),
            {ok, lists:reverse(QoSTable)}
    end.

-spec unsubscribe(subscriber_id(), [topic()]) -> ok | {error, _}.
unsubscribe(SubscriberId, Topics) ->
    del_subscriptions(Topics, SubscriberId).

delete_subscriptions(SubscriberId) ->
    del_subscriber(?DefaultRegView, SubscriberId).

-spec register_subscriber(flag(), subscriber_id(), boolean(), map()) ->
    {ok, #{
        initial_msg_id := msg_id(),
        session_present := flag(),
        queue_pid := pid()
    }}
    | {error, _}.
register_subscriber(
    CoordinateRegs,
    SubscriberId,
    StartClean,
    #{allow_multiple_sessions := false} = QueueOpts
) ->
    %% we don't allow multiple sessions using same subscriber id
    %% allow_multiple_sessions is needed for session balancing
    SessionPid = self(),
    case CoordinateRegs of
        true ->
            %% no netsplit, but coordinated registrations required.
            vmq_reg_sync:sync(
                SubscriberId,
                fun() ->
                    register_subscriber_(
                        SessionPid,
                        SubscriberId,
                        StartClean,
                        QueueOpts,
                        ?NR_OF_REG_RETRIES
                    )
                end,
                60000
            );
        _ ->
            %% all other cases we allow registrations but unsynced.
            register_subscriber_(
                SessionPid,
                SubscriberId,
                StartClean,
                QueueOpts,
                ?NR_OF_REG_RETRIES
            )
    end;
register_subscriber(_CoordinateRegs, _SubscriberId, _StartClean, #{allow_multiple_sessions := true}) ->
    %% we do not allow multiple sessions using same subscriber id
    {error, multiple_sessions_true}.

register_subscriber_(SessionPid, SubscriberId, StartClean, QueueOpts, N) ->
    register_subscriber_(
        SessionPid, SubscriberId, StartClean, QueueOpts, N, register_subscriber_retry_exhausted
    ).

-spec register_subscriber_(
    pid() | undefined, subscriber_id(), boolean(), map(), non_neg_integer(), any()
) ->
    {'ok', map()} | {error, any()}.
register_subscriber_(_, _, _, _, 0, Reason) ->
    {error, Reason};
register_subscriber_(SessionPid, SubscriberId, StartClean, QueueOpts, N, Reason) ->
    % wont create new queue in case it already exists
    {ok, QueuePresent, QPid} =
        case vmq_queue_sup_sup:start_queue(SubscriberId) of
            {ok, true, OldQPid} when StartClean ->
                %% cleanup queue
                vmq_queue:cleanup(OldQPid, ?SESSION_TAKEN_OVER),
                vmq_queue_sup_sup:start_queue(SubscriberId);
            Ret ->
                Ret
        end,
    case vmq_queue:info(QPid) of
        #{statename := drain} ->
            %% queue is draining it's offline queue to a different
            %% remote queue. This can happen if a client hops around
            %% different nodes very frequently... adjust load balancing!!
            timer:sleep(100),
            %% retry and see if it is finished soon before trying to
            %% remap the subscriber.
            register_subscriber_(
                SessionPid,
                SubscriberId,
                StartClean,
                QueueOpts,
                N - 1,
                {register_subscriber_retry_exhausted, draining}
            );
        _ ->
            %% remap subscriber... enabling that new messages will
            %% eventually reach the new queue.  Remapping triggers
            %% remote nodes to initiate queue migration
            case maybe_remap_subscriber(SubscriberId, StartClean) of
                {error, _} = Err ->
                    Err;
                {SubscriptionsPresent, UpdatedSubs, ChangedNodes} ->
                    case {SubscriptionsPresent, QueuePresent, ChangedNodes} of
                        {true, false, []} ->
                            vmq_queue:init_offline_queue(QPid);
                        {true, _, [OldNode]} ->
                            rpc:call(OldNode, vmq_reg_mgr, handle_new_sub_event, [
                                SubscriberId, UpdatedSubs
                            ]),
                            vmq_queue:init_offline_queue(QPid);
                        _ ->
                            ok
                    end,
                    SessionPresent1 = SubscriptionsPresent or QueuePresent,
                    SessionPresent2 =
                        case StartClean of
                            true ->
                                %% SessionPresent is always false in case CleanupSession=true
                                false;
                            false ->
                                SessionPresent1
                        end,
                    case catch vmq_queue:add_session(QPid, SessionPid, QueueOpts) of
                        {'EXIT', {normal, _}} ->
                            %% queue went down in the meantime, retry
                            register_subscriber_(
                                SessionPid,
                                SubscriberId,
                                StartClean,
                                QueueOpts,
                                N - 1,
                                register_subscriber_retry_exhausted
                            );
                        {'EXIT', {noproc, _}} ->
                            timer:sleep(100),
                            %% queue was stopped in the meantime, retry
                            register_subscriber_(
                                SessionPid,
                                SubscriberId,
                                StartClean,
                                QueueOpts,
                                N - 1,
                                register_subscriber_retry_exhausted
                            );
                        {'EXIT', Reason} ->
                            {error, Reason};
                        {error, draining} ->
                            %% queue is still draining it's offline queue to a different
                            %% remote queue. This can happen if a client hops around
                            %% different nodes very frequently... adjust load balancing!!
                            timer:sleep(100),
                            register_subscriber_(
                                SessionPid,
                                SubscriberId,
                                StartClean,
                                QueueOpts,
                                N - 1,
                                {register_subscriber_retry_exhausted, draining}
                            );
                        {error, {cleanup, _Reason}} ->
                            %% queue is still cleaning up.
                            timer:sleep(100),
                            register_subscriber_(
                                SessionPid,
                                SubscriberId,
                                StartClean,
                                QueueOpts,
                                N - 1,
                                register_subscriber_retry_exhausted
                            );
                        {ok, Opts} ->
                            {ok, Opts#{
                                session_present => SessionPresent2,
                                queue_pid => QPid
                            }};
                        _ ->
                            timer:sleep(100),
                            register_subscriber_(
                                SessionPid,
                                SubscriberId,
                                StartClean,
                                QueueOpts,
                                N - 1,
                                register_subscriber_retry_exhausted
                            )
                    end
            end
    end.

%% publish/4
-spec publish(module(), client_id() | ?INTERNAL_CLIENT_ID, msg()) ->
    {ok, {integer(), integer()}} | {'error', _}.
publish(
    RegView,
    ClientId,
    #vmq_msg{
        mountpoint = MP,
        routing_key = Topic,
        payload = Payload,
        retain = IsRetain,
        properties = Properties
    } = Msg
) ->
    %% trade consistency for availability
    %% if the cluster is not consistent at the moment, it is possible
    %% that subscribers connected to other nodes won't get this message
    case IsRetain of
        true when Payload == <<>> ->
            %% retain delete action
            vmq_retain_srv:delete(MP, Topic),
            publish_fold_wrapper(RegView, ClientId, Topic, Msg);
        true ->
            %% retain set action
            vmq_retain_srv:insert(MP, Topic, #retain_msg{
                payload = Payload,
                properties = Properties,
                expiry_ts = maybe_set_expiry_ts(Properties)
            }),
            publish_fold_wrapper(RegView, ClientId, Topic, Msg);
        false ->
            publish_fold_wrapper(RegView, ClientId, Topic, Msg)
    end.

-spec publish_fold_wrapper(module(), client_id() | ?INTERNAL_CLIENT_ID, topic(), msg()) ->
    {ok, {integer(), integer()}} | {error, _}.
publish_fold_wrapper(
    RegView,
    ClientId,
    Topic,
    #vmq_msg{
        sg_policy = SGPolicy,
        mountpoint = MP
    } = Msg
) ->
    Acc = #publish_fold_acc{msg = Msg},
    case vmq_reg_view:fold(RegView, {MP, ClientId}, Topic, fun publish_fold_fun/3, Acc) of
        #publish_fold_acc{
            msg = NewMsg,
            subscriber_groups = SubscriberGroups,
            local_matches = LocalMatches0,
            remote_matches = RemoteMatches0
        } ->
            {LocalMatches1, RemoteMatches1} = vmq_shared_subscriptions:publish(
                NewMsg, SGPolicy, SubscriberGroups, LocalMatches0, RemoteMatches0
            ),
            vmq_metrics:incr_router_matches_local(LocalMatches1),
            vmq_metrics:incr_router_matches_remote(RemoteMatches1),
            {ok, {LocalMatches1, RemoteMatches1}};
        {error, _} = Err ->
            Err
    end.

%% publish_fold_fun/3 is used as the fold function in RegView:fold/4
publish_fold_fun({_, SubscriberId, {_, #{no_local := true}}}, SubscriberId, Acc) ->
    %% Publisher is the same as subscriber, discard.
    Acc;
publish_fold_fun(
    {Node, SubscriberId, SubInfo},
    _FromClientId,
    #publish_fold_acc{
        local_matches = N,
        msg = Msg
    } = Acc
) when
    Node == node()
->
    enqueue_msg({SubscriberId, SubInfo}, Msg),
    Acc#publish_fold_acc{local_matches = N + 1};
publish_fold_fun(
    {Node, SubscriberId, SubInfo},
    FromClientId,
    #publish_fold_acc{
        remote_matches = RN,
        msg = Msg
    } = Acc
) ->
    case vmq_cluster_mon:is_node_alive(Node) of
        true ->
            vmq_redis_queue:enqueue(
                Node, term_to_binary(SubscriberId), term_to_binary({SubInfo, Msg})
            ),
            Acc#publish_fold_acc{remote_matches = RN + 1};
        _ ->
            %% Transfer the client on local node if the remote node is not alive.
            %% It could happen that the client has reconnected to the cluster before we
            %% can transfer it here. In that case, we should enqueue the msg there without local Xfer.
            %% In case the client has not yet reconnected, then remap(Xfer) it here and enqueue.
            %%
            %% The question is how do we know whether the client reconnected or not in this case?
            %% The source of truth is redis and this operation must be atomic on redis. When we
            %% attempt to Xfer the client here, checking on redis for old node comparsion is a must
            %% before redis remap operations are performed. In case the node value is unaffected node
            %% then continue with remap otherwise return and initiate the enqueue op on the new
            %% node's mainQueue.
            %%
            %% What is the time period in which this specific case is true?
            %% It is when node is down, publish happened, subscriber info was fetched from redis
            %% and before we Xfer it here, the client reconnects/Xfered elsewhere.
            %% Probability of such msgs will be --
            %% Until the complete Xfer happens - msg rate * vulnerable clients / total clients
            %% And the above figure is upper bound
            %%
            %%
            %%
            %% Now to migrate the client from remote node, the following operations need to be performed.
            %% 1. Start the queue with clean session false
            %% 2. Migrate client on Redis - If the client had connected with clean session true, then
            %% delete its entry and return nil. Otherwise change its node name and return true.
            %% 3. If the result was undefined then terminate the queue otherwise initialize offline queue
            %% and then enqueue.
            case migrate_offline_queue(SubscriberId, Node) of
                {error, _} ->
                    Acc;
                NewNode ->
                    publish_fold_fun({NewNode, SubscriberId, SubInfo}, FromClientId, Acc)
            end
    end;
publish_fold_fun({_Node, _Group, SubscriberId, {_, #{no_local := true}}}, SubscriberId, Acc) ->
    %% Publisher is the same as subscriber, discard.
    Acc;
publish_fold_fun(
    {_Node, _Group, _SubscriberId, _SubInfo} = Sub,
    _FromClientId,
    #publish_fold_acc{
        msg = #vmq_msg{sg_policy = SGPolicy},
        subscriber_groups = SubscriberGroups
    } = Acc
) ->
    %% collect subscriber group members for later processing
    Acc#publish_fold_acc{
        subscriber_groups = add_to_subscriber_group(Sub, SubscriberGroups, SGPolicy)
    }.

-spec enqueue_msg({subscriber_id(), subinfo()}, msg()) -> ok.
enqueue_msg({{_, _} = SubscriberId, SubInfo}, Msg0) ->
    case get_queue_pid(SubscriberId) of
        not_found ->
            vmq_metrics:incr_msg_enqueue_subscriber_not_found();
        QPid ->
            Msg1 = handle_rap_flag(SubInfo, Msg0),
            Msg2 = maybe_add_sub_id(SubInfo, Msg1),
            Msg3 = handle_non_persistence(SubInfo, Msg2),
            Msg4 = handle_retry_flag(SubInfo, Msg3),
            QoS = qos(SubInfo),
            ok = vmq_queue:enqueue(QPid, {deliver, QoS, Msg4})
    end.

maybe_set_expiry_ts(#{p_message_expiry_interval := ExpireAfter}) ->
    {vmq_time:timestamp(second) + ExpireAfter, ExpireAfter};
maybe_set_expiry_ts(_) ->
    undefined.

-spec handle_rap_flag(subinfo(), msg()) -> msg().
handle_rap_flag({_QoS, #{rap := true}}, Msg) ->
    Msg;
handle_rap_flag(_SubInfo, Msg) ->
    %% Default is to set the retain flag to false to be compatible with MQTTv3
    Msg#vmq_msg{retain = false}.

-spec handle_retry_flag(subinfo(), msg()) -> msg().
handle_retry_flag({_QoS, #{non_retry := NonRetry}}, Msg) ->
    Msg#vmq_msg{non_retry = NonRetry};
handle_retry_flag(_SubInfo, Msg) ->
    %% Default is to set the non_retry flag to false so that default qos 1 behaviour is preserved
    Msg#vmq_msg{non_retry = false}.

-spec handle_non_persistence(subinfo(), msg()) -> msg().
handle_non_persistence({_QoS, #{non_persistence := NonPersistence}}, Msg) ->
    Msg#vmq_msg{non_persistence = NonPersistence};
handle_non_persistence(_SubInfo, Msg) ->
    %% Default is to set the non_persistence flag to false to be compatible with MQTTv3 behaviour
    Msg#vmq_msg{non_persistence = false}.

maybe_add_sub_id({_, #{sub_id := SubId}}, #vmq_msg{properties = Props} = Msg) ->
    Msg#vmq_msg{properties = Props#{p_subscription_id => [SubId]}};
maybe_add_sub_id(_, Msg) ->
    Msg.

-spec qos(subinfo()) -> qos().
qos({QoS, _}) when is_integer(QoS) ->
    QoS;
qos(QoS) when is_integer(QoS) ->
    QoS.

add_to_subscriber_group(Sub, undefined, SGPolicy) ->
    add_to_subscriber_group(Sub, #{}, SGPolicy);
add_to_subscriber_group({Node, _Group, _SubscriberId, _SubInfo}, SubscriberGroups, local_only) when
    Node =/= node()
->
    SubscriberGroups;
add_to_subscriber_group({Node, Group, SubscriberId, SubInfo}, SubscriberGroups, _SGPolicy) ->
    SubscriberGroup = maps:get(Group, SubscriberGroups, []),
    maps:put(
        Group,
        [{Node, SubscriberId, SubInfo} | SubscriberGroup],
        SubscriberGroups
    ).

-spec deliver_retained(subscriber_id(), topic(), qos(), subopts(), boolean()) -> 'ok'.
deliver_retained(_, _, _, #{retain_handling := dont_send}, _) ->
    %% don't send, skip
    ok;
deliver_retained(_, _, _, #{retain_handling := send_if_new_sub}, true) ->
    %% subscription already existed, skip.
    ok;
deliver_retained(_SubscriberId, [<<"$share">> | _], _QoS, _SubOpts, _) ->
    %% Never deliver retained messages to subscriber groups.
    ok;
deliver_retained({MP, _} = SubscriberId, Topic, QoS, SubOpts, _) ->
    QPid = get_queue_pid(SubscriberId),
    vmq_retain_srv:match_fold(
        fun
            (
                {T, #retain_msg{
                    payload = Payload,
                    properties = Properties,
                    expiry_ts = ExpiryTs
                }},
                _
            ) ->
                Msg = #vmq_msg{
                    routing_key = T,
                    payload = retain_pre(Payload),
                    retain = true,
                    qos = QoS,
                    dup = false,
                    mountpoint = MP,
                    msg_ref = vmq_mqtt_fsm_util:msg_ref(),
                    expiry_ts = ExpiryTs,
                    properties = Properties
                },
                Msg1 = maybe_add_sub_id({QoS, SubOpts}, Msg),
                maybe_delete_expired(ExpiryTs, MP, Topic),
                vmq_queue:enqueue(QPid, {deliver, QoS, Msg1});
            ({T, Payload}, _) when is_binary(Payload) ->
                %% compatibility with old style retained messages.
                Msg = #vmq_msg{
                    routing_key = T,
                    payload = retain_pre(Payload),
                    retain = true,
                    qos = QoS,
                    dup = false,
                    mountpoint = MP,
                    msg_ref = vmq_mqtt_fsm_util:msg_ref(),
                    properties = #{}
                },
                vmq_queue:enqueue(QPid, {deliver, QoS, Msg})
        end,
        ok,
        MP,
        Topic
    ).

maybe_delete_expired(undefined, _, _) ->
    ok;
maybe_delete_expired({Ts, _}, MP, Topic) ->
    case vmq_time:is_past(Ts) of
        true ->
            vmq_retain_srv:delete(MP, Topic);
        _ ->
            ok
    end.

subscriptions_for_subscriber_id(SubscriberId) ->
    Default = [],
    vmq_subscriber_db:read(SubscriberId, Default).

-spec direct_plugin_exports(module()) ->
    {function(), function(), {function(), function()}} | {error, invalid_config}.
direct_plugin_exports(Mod) when is_atom(Mod) ->
    %% This Function exports a generic Register, Publish, and Subscribe
    %% Fun, that a plugin can use if needed. Currently all functions
    %% block until the cluster is ready.
    SGPolicyConfig = vmq_config:get_env(shared_subscription_policy, prefer_local),
    RegView = vmq_config:get_env(default_reg_view, vmq_reg_trie),
    {ok, #{
        register_fun := RegFun,
        publish_fun := PubFun,
        subscribe_fun := SubFun,
        unsubscribe_fun := UnsubFun
    }} =
        direct_plugin_exports(Mod, #{
            mountpoint => "",
            sg_policy => SGPolicyConfig,
            reg_view => RegView
        }),
    {RegFun, PubFun, {SubFun, UnsubFun}}.

-spec direct_plugin_exports(module(), map()) ->
    {ok, #{
        client_id := client_id(),
        register_fun := function(),
        publish_fun := function(),
        subscribe_fun := function(),
        unsubscribe_fun := function()
    }}.
direct_plugin_exports(LogName, Opts) ->
    SGPolicy = maps:get(sg_policy, Opts, prefer_local),
    Mountpoint = maps:get(mountpoint, Opts, ""),
    RegView = maps:get(reg_view, Opts, vmq_reg_trie),

    CallingPid = self(),
    ClientIdDef = list_to_binary(
        base64:encode_to_string(
            integer_to_binary(
                erlang:phash2(CallingPid)
            )
        )
    ),
    ClientId = maps:get(client_id, Opts, ClientIdDef),
    SubscriberId = {Mountpoint, ClientId},
    RegisterFun =
        fun() ->
            PluginPid = self(),
            PluginSessionPid = spawn_link(
                fun() ->
                    monitor(process, PluginPid),
                    vmq_mqtt_fsm_util:plugin_receive_loop(PluginPid, LogName)
                end
            ),
            QueueOpts = maps:merge(
                vmq_queue:default_opts(),
                #{
                    cleanup_on_disconnect => true,
                    is_plugin => true
                }
            ),
            {ok, _} = register_subscriber_(
                PluginSessionPid,
                SubscriberId,
                true,
                QueueOpts,
                ?NR_OF_REG_RETRIES
            ),
            ok
        end,

    PublishFun =
        fun([W | _] = Topic, Payload, Opts_) when
            is_binary(W) and
                is_binary(Payload) and
                is_map(Opts_)
        ->
            %% allow a plugin developer to override
            %% - mountpoint
            %% - dup flag
            %% - retain flag
            %% - trade-consistency flag
            %% - reg_view
            %% - shared subscription policy
            Msg = #vmq_msg{
                routing_key = Topic,
                mountpoint = maps:get(mountpoint, Opts_, Mountpoint),
                payload = Payload,
                msg_ref = vmq_mqtt_fsm_util:msg_ref(),
                qos = maps:get(qos, Opts_, 0),
                dup = maps:get(dup, Opts_, false),
                retain = maps:get(retain, Opts_, false),
                sg_policy = maps:get(shared_subscription_policy, Opts_, SGPolicy)
            },
            publish(RegView, ClientId, Msg)
        end,

    SubscribeFun =
        fun
            ([W | _] = Topic) when is_binary(W) ->
                CallingPid = self(),
                subscribe({Mountpoint, ClientId}, [{Topic, 0}]);
            (_) ->
                {error, invalid_topic}
        end,

    UnsubscribeFun =
        fun
            ([W | _] = Topic) when is_binary(W) ->
                CallingPid = self(),
                unsubscribe({Mountpoint, ClientId}, [Topic]);
            (_) ->
                {error, invalid_topic}
        end,
    {ok, #{
        register_fun => RegisterFun,
        publish_fun => PublishFun,
        subscribe_fun => SubscribeFun,
        unsubscribe_fun => UnsubscribeFun,
        client_id => ClientId
    }}.

subscribe_subscriber_changes() ->
    vmq_subscriber_db:subscribe_db_events().

fold_subscriptions(FoldFun, Acc) ->
    fold_subscribers(
        fun({MP, _} = SubscriberId, Subs, AAcc) ->
            vmq_subscriber:fold(
                fun({Topic, QoS, Node}, AAAcc) ->
                    FoldFun({MP, Topic, {SubscriberId, QoS, Node}}, AAAcc)
                end,
                AAcc,
                Subs
            )
        end,
        Acc
    ).

fold_subscribers(FoldFun, Acc) ->
    vmq_subscriber_db:fold(
        fun({SubscriberId, Subs}, AccAcc) ->
            FoldFun(SubscriberId, Subs, AccAcc)
        end,
        Acc
    ).

-spec subscriptions_exist(vmq_subscriber:subs(), [topic()]) -> [boolean()].
subscriptions_exist(OldSubs, Topics) ->
    [vmq_subscriber:exists(Topic, OldSubs) || {Topic, _} <- Topics].

-spec del_subscriber(atom(), subscriber_id()) -> ok.
del_subscriber(vmq_reg_redis_trie, {MP, ClientId} = _SubscriberId) ->
    Key = {MP, '$1'},
    Value = {ClientId, '$2'},
    ets:select_delete(?SHARED_SUBS_ETS_TABLE, [{{{Key, Value}}, [], [true]}]),
    vmq_metrics:incr_cache_delete(?LOCAL_SHARED_SUBS),
    vmq_redis:query(
        vmq_redis_client,
        [
            ?FCALL,
            ?DELETE_SUBSCRIBER,
            0,
            MP,
            ClientId,
            node(),
            os:system_time(nanosecond)
        ],
        ?FCALL,
        ?DELETE_SUBSCRIBER
    ),
    ok;
del_subscriber(_, SubscriberId) ->
    vmq_subscriber_db:delete(SubscriberId).

-spec del_subscriptions([topic()], subscriber_id()) -> ok | {error, _}.
del_subscriptions(Topics, {MP, ClientId} = _SubscriberId) ->
    lists:foreach(
        fun
            ([<<"$share">>, _Group | Topic]) ->
                Key = {MP, Topic},
                Value = {ClientId, '$1'},
                ets:select_delete(?SHARED_SUBS_ETS_TABLE, [{{{Key, Value}}, [], [true]}]),
                vmq_metrics:incr_cache_delete(?LOCAL_SHARED_SUBS);
            (_) ->
                ok
        end,
        Topics
    ),
    SortedUnwordedTopics = [vmq_topic:unword(T) || T <- lists:usort(Topics)],
    case
        vmq_redis:query(
            vmq_redis_client,
            [
                ?FCALL,
                ?UNSUBSCRIBE,
                0,
                MP,
                ClientId,
                node(),
                os:system_time(nanosecond),
                length(SortedUnwordedTopics)
                | SortedUnwordedTopics
            ],
            ?FCALL,
            ?UNSUBSCRIBE
        )
    of
        {ok, <<"1">>} -> ok;
        {ok, _} -> {error, unwanted_redis_response};
        Err -> Err
    end.

%% the return value is used to inform the caller
%% if a session was already present for the given
%% subscriber id.
-spec maybe_remap_subscriber(subscriber_id(), boolean()) ->
    {boolean(), undefined | vmq_subscriber:subs(), [node()]} | {error, binary | atom()}.
maybe_remap_subscriber({MP, ClientId}, _StartClean = true) ->
    Subs = vmq_subscriber:new(true),
    case
        vmq_redis:query(
            vmq_redis_client,
            [
                ?FCALL,
                ?REMAP_SUBSCRIBER,
                0,
                MP,
                ClientId,
                node(),
                true,
                os:system_time(nanosecond)
            ],
            ?FCALL,
            ?REMAP_SUBSCRIBER
        )
    of
        {ok, [undefined, [_, <<"1">>, []]]} -> {false, Subs, []};
        {ok, [<<"1">>, [_, <<"1">>, []]]} -> {true, Subs, []};
        {ok, [<<"1">>, [_, <<"1">>, []], OldNode]} -> {true, Subs, [binary_to_atom(OldNode)]};
        {ok, _} -> {error, unwanted_redis_response};
        Err -> Err
    end;
maybe_remap_subscriber({MP, ClientId}, _StartClean = false) ->
    case
        vmq_redis:query(
            vmq_redis_client,
            [
                ?FCALL,
                ?REMAP_SUBSCRIBER,
                0,
                MP,
                ClientId,
                node(),
                false,
                os:system_time(nanosecond)
            ],
            ?FCALL,
            ?REMAP_SUBSCRIBER
        )
    of
        {ok, [undefined, [NewNode, undefined, []]]} ->
            {false, [{binary_to_atom(NewNode), false, []}], []};
        {ok, [<<"1">>, [NewNode, undefined, TopicsWithQoS]]} ->
            NewTopicsWithQoS = [
                {vmq_topic:word(Topic), binary_to_term(QoS)}
             || [Topic, QoS] <- TopicsWithQoS
            ],
            {true, [{binary_to_atom(NewNode), false, NewTopicsWithQoS}], []};
        {ok, [<<"1">>, [NewNode, undefined, TopicsWithQoS], OldNode]} ->
            NewTopicsWithQoS = [
                {vmq_topic:word(Topic), binary_to_term(QoS)}
             || [Topic, QoS] <- TopicsWithQoS
            ],
            NewSubs = [{binary_to_atom(NewNode), false, NewTopicsWithQoS}],
            {true, NewSubs, [binary_to_atom(OldNode)]};
        {ok, _} ->
            {error, unwanted_redis_response};
        Err ->
            Err
    end.

-spec get_session_pids(subscriber_id()) ->
    {'error', 'not_found'} | {'ok', pid(), [pid()]}.
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
                vmq_subscriber:fold(
                    fun(_, AccAcc) ->
                        AccAcc + 1
                    end,
                    Acc,
                    Subs
                )
            end,
            0
        ),
    [{total, Total}].

stored(SubscriberId) ->
    case get_queue_pid(SubscriberId) of
        not_found ->
            0;
        QPid ->
            {_, _, Queued, _, _} = vmq_queue:status(QPid),
            Queued
    end.

%% retain, pre-versioning
%% {MPTopic, MsgOrDeleted}
%% future version:
%% {retain_msg, version, payload, properties, expiry_ts, ...}

-define(RETAIN_PRE_V, 0).

-spec retain_pre(binary() | tuple()) -> binary().
retain_pre(Msg) when is_binary(Msg) ->
    Msg;
retain_pre(FutureRetain) when
    is_tuple(FutureRetain),
    element(1, FutureRetain) =:= retain_msg,
    is_integer(element(2, FutureRetain)),
    element(2, FutureRetain) > ?RETAIN_PRE_V
->
    element(3, FutureRetain).

-spec if_ready(_, _) -> any().
if_ready(Fun, Args) ->
    case persistent_term:get(subscribe_trie_ready, 0) of
        1 ->
            apply(Fun, Args);
        0 ->
            {error, not_ready}
    end.

update_qos1_metrics(Topics) ->
    lists:foreach(
        fun
            ({_, 1}) ->
                _ = vmq_metrics:incr_qos1_opts({false, false});
            ({_, {1, #{non_retry := NonRetry, non_persistence := NonPersistence}}}) ->
                _ = vmq_metrics:incr_qos1_opts({NonRetry, NonPersistence});
            (_) ->
                ok
        end,
        Topics
    ).

-spec migrate_offline_queue(subscriber_id(), node()) -> node() | {error, _}.
migrate_offline_queue({MP, ClientId} = SubscriberId, OldNode) ->
    {ok, _QueuePresent, QPid} = vmq_queue_sup_sup:start_queue(SubscriberId),
    case
        vmq_redis:query(
            vmq_redis_client,
            [
                ?FCALL,
                ?MIGRATE_OFFLINE_QUEUE,
                0,
                MP,
                ClientId,
                OldNode,
                node(),
                os:system_time(nanosecond)
            ],
            ?FCALL,
            ?MIGRATE_OFFLINE_QUEUE
        )
    of
        {ok, undefined} ->
            vmq_queue:terminate(QPid, normal),
            {error, client_does_not_exist};
        {ok, NodeBin} when is_binary(NodeBin) ->
            case binary_to_atom(NodeBin) of
                LocalNode when LocalNode == node() ->
                    vmq_queue:init_offline_queue(QPid),
                    LocalNode;
                RemoteNode ->
                    vmq_queue:terminate(QPid, normal),
                    RemoteNode
            end;
        Res ->
            lager:warning("~p", [Res]),
            vmq_queue:terminate(QPid, normal),
            {error, unwanted_response}
    end.
