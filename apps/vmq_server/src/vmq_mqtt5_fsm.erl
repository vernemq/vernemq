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

-module(vmq_mqtt5_fsm).
-include("vmq_server.hrl").
-include_lib("vmq_commons/include/vmq_types.hrl").
-include_lib("vmq_commons/include/vmq_types_mqtt5.hrl").

-export([init/3,
         data_in/2,
         msg_in/2,
         info/2]).

-export([msg_ref/0]).

-define(CLOSE_AFTER, 5000).
-define(ALLOWED_MQTT_VERSIONS, [5]).

-type timestamp() :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}.

-record(cap_settings, {
         allow_register = false    :: boolean(),
         allow_publish = false     :: boolean(),
         allow_subscribe = false   :: boolean(),
         allow_unsubscribe = false   :: boolean()
         }).
-type cap_settings() :: #cap_settings{}.

-type topic_aliases_in() :: map().

-record(auth_data, {
          method :: binary(),
          type   :: connect | re_auth,
          data   :: any()
         }).

-type auth_data() :: auth_data().

-record(state, {
          %% mqtt layer requirements
          next_msg_id=1                     :: msg_id(),
          subscriber_id                     :: undefined | subscriber_id() | {mountpoint(), undefined},
          will_msg                          :: undefined | msg(),
          waiting_acks=maps:new()           :: map(),
          waiting_msgs=[]                   :: list(),
          %% auth backend requirement
          peer                              :: peer(),
          username                          :: undefined | username() |
                                               {preauth, string() | undefined},
          keep_alive                        :: undefined | non_neg_integer(),
          keep_alive_tref                   :: undefined | reference(),
          retry_queue=queue:new()           :: queue:queue(),
          clean_session=false               :: flag(),
          proto_ver                         :: undefined | pos_integer(),
          queue_pid                         :: pid() | undefined,

          last_time_active=os:timestamp()   :: timestamp(),
          last_trigger=os:timestamp()       :: timestamp(),

          %% Data used for enhanced authentication and
          %% re-authentication. TODOv5: move this to pdict?
          enhanced_auth                     :: undefined | auth_data(),

          %% config
          allow_anonymous=false             :: boolean(),
          max_client_id_size=100            :: non_neg_integer(),

          %% changeable by auth_on_register
          shared_subscription_policy=prefer_local :: sg_policy(),
          max_inflight_messages=20          :: non_neg_integer(), %% 0 means unlimited
          max_message_rate=0                :: non_neg_integer(), %% 0 means unlimited
          retry_interval=20000              :: pos_integer(),
          upgrade_qos=false                 :: boolean(),
          reg_view=vmq_reg_trie             :: atom(),
          cap_settings=#cap_settings{}      :: cap_settings(),
          topic_alias_max                   :: non_neg_integer(), %% 0 means no topic aliases allowed.
          topic_aliases_in=#{}              :: topic_aliases_in(), %% topic aliases used from client to broker.

          trace_fun                        :: undefined | any() %% TODO
         }).

-type state() :: #state{}.
-define(state_val(Key, Args, State), prop_val(Key, Args, State#state.Key)).
-define(cap_val(Key, Args, State), prop_val(Key, Args, CAPSettings#cap_settings.Key)).

init(Peer, Opts, #mqtt5_connect{keep_alive=KeepAlive} = ConnectFrame) ->
    rand:seed(exsplus, os:timestamp()),
    MountPoint = proplists:get_value(mountpoint, Opts, ""),
    SubscriberId = {string:strip(MountPoint, right, $/), undefined},
    PreAuthUser =
    case lists:keyfind(preauth, 1, Opts) of
        false -> undefined;
        {_, undefined} -> undefined;
        {_, PreAuth} -> {preauth, PreAuth}
    end,
    AllowAnonymous = vmq_config:get_env(allow_anonymous, false),
    SharedSubPolicy = vmq_config:get_env(shared_subscription_policy, prefer_local),
    MaxClientIdSize = vmq_config:get_env(max_client_id_size, 23),
    RetryInterval = vmq_config:get_env(retry_interval, 20),
    MaxInflightMsgs = vmq_config:get_env(max_inflight_messages, 20),
    MaxMessageSize = vmq_config:get_env(max_message_size, 0),
    MaxMessageRate = vmq_config:get_env(max_message_rate, 0),
    UpgradeQoS = vmq_config:get_env(upgrade_outgoing_qos, false),
    RegView = vmq_config:get_env(default_reg_view, vmq_reg_trie),
    CAPRegister = vmq_config:get_env(allow_register_during_netsplit, false),
    CAPPublish = vmq_config:get_env(allow_publish_during_netsplit, false),
    CAPSubscribe= vmq_config:get_env(allow_subscribe_during_netsplit, false),
    CAPUnsubscribe= vmq_config:get_env(allow_unsubscribe_during_netsplit, false),
    CAPSettings = #cap_settings{
                     allow_register=CAPRegister,
                     allow_publish=CAPPublish,
                     allow_subscribe=CAPSubscribe,
                     allow_unsubscribe=CAPUnsubscribe
                    },
    TopicAliasMax = vmq_config:get_env(topic_alias_max, 0),
    TraceFun = vmq_config:get_env(trace_fun, undefined),
    maybe_initiate_trace(ConnectFrame, TraceFun),
    set_max_msg_size(MaxMessageSize),

    _ = vmq_metrics:incr_mqtt_connect_received(),
    %% the client is allowed "grace" of a half a time period
    set_keepalive_check_timer(KeepAlive),

    State = #state{peer=Peer,
                   upgrade_qos=UpgradeQoS,
                   subscriber_id=SubscriberId,
                   allow_anonymous=AllowAnonymous,
                   shared_subscription_policy=SharedSubPolicy,
                   max_inflight_messages=MaxInflightMsgs,
                   max_message_rate=MaxMessageRate,
                   username=PreAuthUser,
                   max_client_id_size=MaxClientIdSize,
                   keep_alive=KeepAlive,
                   keep_alive_tref=undefined,
                   retry_interval=1000 * RetryInterval,
                   cap_settings=CAPSettings,
                   topic_alias_max=TopicAliasMax,
                   reg_view=RegView,
                   trace_fun=TraceFun,
                   last_time_active=os:timestamp()},

    case check_enhanced_auth(ConnectFrame, State) of
        {stop, _, _} = R -> R;
        {pre_connect_auth, NewState, Out} ->
            {{pre_connect_auth, NewState}, Out};
        {NewState, Out} ->
            {{connected, set_last_time_active(true, NewState)}, Out}
    end.

data_in(Data, SessionState) when is_binary(Data) ->
    data_in(Data, SessionState, []).

data_in(Data, SessionState, OutAcc) ->
    case vmq_parser_mqtt5:parse(Data, max_msg_size()) of
        more ->
            {ok, SessionState, Data, serialise(OutAcc)};
        {error, packet_exceeds_max_size} = E ->
            _ = vmq_metrics:incr_mqtt_error_invalid_msg_size(),
            E;
        {error, Reason} ->
            {error, Reason, serialise(OutAcc)};
        {Frame, Rest} ->
            case in(Frame, SessionState, true) of
                {stop, Reason, Out} ->
                    {stop, Reason, serialise([Out|OutAcc])};
                {NewSessionState, {throttle, Out}} ->
                    {throttle, NewSessionState, Rest, serialise([Out|OutAcc])};
                {NewSessionState, Out} when byte_size(Rest) == 0 ->
                    %% optimization
                    {ok, NewSessionState, Rest, serialise([Out|OutAcc])};
                {NewSessionState, Out} ->
                    data_in(Rest, NewSessionState, [Out|OutAcc])
            end
    end.

msg_in(Msg, SessionState) ->
   case in(Msg, SessionState, false) of
       {stop, Reason, Out} ->
           {stop, Reason, serialise([Out])};
       {NewSessionState, {throttle, Out}} ->
           %% we ignore throttling for the internal message flow
           {ok, NewSessionState, serialise([Out])};
       {NewSessionState, Out} ->
           {ok, NewSessionState, serialise([Out])}
   end.

%%% init --> pre_connect_auth | connected | --> terminate
in(Msg, {connected, State}, IsData) ->
    case connected(Msg, State) of
        {stop, _, _} = R -> R;
        {NewState, Out} ->
            {{connected, set_last_time_active(IsData, NewState)}, Out}
    end;
in(Msg, {pre_connect_auth, State}, IsData) ->
    case pre_connect_auth(Msg, State) of
        {stop, _, _} = R -> R;
        {pre_connect_auth, NewState, Out} ->
            {{pre_connect_auth, NewState}, Out};
        {NewState, Out} ->
            {{connected, set_last_time_active(IsData, NewState)}, Out}
    end.

serialise(Frames) ->
    serialise(Frames, []).

serialise([], Acc) -> Acc;
serialise([[]|Frames], Acc) ->
    serialise(Frames, Acc);
serialise([[B|T]|Frames], Acc) when is_binary(B) ->
    serialise([T|Frames], [B|Acc]);
serialise([[F|T]|Frames], Acc) ->
    serialise([T|Frames], [vmq_parser_mqtt5:serialise(F)|Acc]).

maybe_initiate_trace(_Frame, undefined) ->
    ok;
maybe_initiate_trace(Frame, TraceFun) ->
    TraceFun(self(), Frame).

-spec pre_connect_auth(mqtt5_frame(), state()) ->
    {state(), [mqtt5_frame() | binary()]} |
    {state(), {throttle, [mqtt5_frame() | binary()]}} |
    {stop, any(), [mqtt5_frame() | binary()]}.
pre_connect_auth(#mqtt5_auth{properties = #{p_authentication_method := AuthMethod,
                                            p_authentication_data := _} = Props},
                 #state{enhanced_auth = #auth_data{method = AuthMethod,
                                                   data = ConnectFrame}} = State) ->
    case vmq_plugin:all_till_ok(on_auth_v1, [Props]) of
        {ok, Modifiers} ->
            %% we're don with enhanced auth on connect.
            NewAuthData = #auth_data{method = AuthMethod},
            OutProps = proplists:get_value(properties, Modifiers, #{}),
            check_connect(ConnectFrame, OutProps,
                          State#state{enhanced_auth = NewAuthData});
        {continue_auth, Modifiers} ->
            %% TODOv5: filter / rename properties?
            OutProps = proplists:get_value(properties, Modifiers, #{}),
            Frame = #mqtt5_auth{reason_code = ?M5_CONTINUE_AUTHENTICATION,
                                properties = OutProps},
            {pre_connect_auth, State, [Frame]};
        {error, Reason} ->
            %% TODOv5
            throw({not_implemented, Reason})
    end;
pre_connect_auth(#mqtt5_disconnect{}, State) ->
    %% TODOv5 add metric?
    terminate(mqtt_client_disconnect, State);
pre_connect_auth(_, State) ->
    terminate(?PROTOCOL_ERROR, State).


-spec connected(mqtt5_frame(), state()) ->
    {state(), [mqtt5_frame() | binary()]} |
    {state(), {throttle, [mqtt5_frame() | binary()]}} |
    {stop, any(), [mqtt5_frame() | binary()]}.
connected(#mqtt5_publish{message_id=MessageId, topic=Topic,
                         qos=QoS, retain=IsRetain,
                         payload=Payload,
                         properties=Properties},
          #state{subscriber_id={MountPoint,_},
                 shared_subscription_policy=SGPolicy} = State) ->
    DoThrottle = do_throttle(State),
    %% we disallow Publishes on Topics prefixed with '$'
    %% this allows us to use such prefixes for e.g. '$SYS' Tree
    _ = vmq_metrics:incr_mqtt_publish_received(),
    Ret =
    case Topic of
        [<<"$", _binary>> |_] ->
            %% $SYS
            [];
        _ ->
            Msg = #vmq_msg{routing_key=Topic,
                           payload=Payload,
                           retain=unflag(IsRetain),
                           qos=QoS,
                           mountpoint=MountPoint,
                           msg_ref=msg_ref(),
                           sg_policy=SGPolicy,
                           properties=Properties,
                           expiry_ts=msg_expiration(Properties)},
            dispatch_publish(QoS, MessageId, Msg, State)
    end,
    case Ret of
        {error, not_allowed} ->
            terminate(publish_not_authorized_3_1_1, State);
        Out when is_list(Out) and not DoThrottle ->
            {State, Out};
        Out when is_list(Out) ->
            {State, {throttle, Out}};
        {NewState, Out} when is_list(Out) and not DoThrottle ->
            {NewState, Out};
        {NewState, Out} ->
            {NewState, {throttle, Out}}
    end;
connected({mail, QPid, new_data}, #state{queue_pid=QPid} = State) ->
    vmq_queue:active(QPid),
    {State, []};
connected({mail, QPid, Msgs, _, Dropped},
          #state{subscriber_id=SubscriberId, queue_pid=QPid,
                 waiting_msgs=Waiting} = State) ->
    NewState =
    case Dropped > 0 of
        true ->
            lager:warning("subscriber ~p dropped ~p messages",
                          [SubscriberId, Dropped]),
            State;
        false ->
            State
    end,
    {NewState2, Out} =
    case handle_messages(Msgs, [], 0, NewState, Waiting) of
        {NewState1, HandledMsgs, []} ->
            vmq_queue:notify(QPid),
            {NewState1, HandledMsgs};
        {NewState1, HandledMsgs, NewWaiting} ->
            %% messages aren't delivered (yet) but are queued in this process
            %% we tell the queue to get rid of them
            vmq_queue:notify_recv(QPid),
            %% we call vmq_queue:notify as soon as
            %% the check_in_flight returns true again
            %% SEE: Comment in handle_waiting_msgs function.
            {NewState1#state{waiting_msgs=NewWaiting}, HandledMsgs}
    end,
    {NewState2, Out};
connected(#mqtt5_puback{message_id=MessageId}, #state{waiting_acks=WAcks} = State) ->
    %% qos1 flow
    _ = vmq_metrics:incr_mqtt_puback_received(),
    case maps:get(MessageId, WAcks, not_found) of
        #vmq_msg{} ->
            handle_waiting_msgs(State#state{waiting_acks=maps:remove(MessageId, WAcks)});
        not_found ->
            _ = vmq_metrics:incr_mqtt_error_invalid_puback(),
            {State, []}
    end;
connected(#mqtt5_pubrec{message_id=MessageId}, State) ->
    #state{waiting_acks=WAcks, retry_interval=RetryInterval,
           retry_queue=RetryQueue} = State,
    %% qos2 flow
    _ = vmq_metrics:incr_mqtt_pubrec_received(),
    case maps:get(MessageId, WAcks, not_found) of
        #vmq_msg{} ->
            PubRelFrame = #mqtt5_pubrel{message_id=MessageId, reason_code=?M5_SUCCESS, properties=#{}},
            _ = vmq_metrics:incr_mqtt_pubrel_sent(),
            {State#state{
               retry_queue=set_retry(MessageId, RetryInterval, RetryQueue),
               waiting_acks=maps:update(MessageId, PubRelFrame, WAcks)},
            [PubRelFrame]};
        not_found ->
            lager:debug("stopped connected session, due to qos2 puback missing ~p", [MessageId]),
            _ = vmq_metrics:incr_mqtt_error_invalid_pubrec(),
            terminate(normal, State)
    end;
connected(#mqtt5_pubrel{message_id=MessageId}, State) ->
    #state{waiting_acks=WAcks, username=User, reg_view=RegView,
           subscriber_id=SubscriberId, cap_settings=CAPSettings} = State,
    %% qos2 flow
    _ = vmq_metrics:incr_mqtt_pubrel_received(),
    case maps:get({qos2, MessageId} , WAcks, not_found) of
        {#mqtt5_pubrec{}, #vmq_msg{routing_key=Topic, qos=QoS, payload=Payload,
                                  retain=IsRetain} = Msg} ->
            HookArgs = [User, SubscriberId, QoS, Topic, Payload, unflag(IsRetain)],
            case on_publish_hook(vmq_reg:publish(CAPSettings#cap_settings.allow_publish, RegView, Msg), HookArgs) of
                ok ->
                    {NewState, Msgs} =
                    handle_waiting_msgs(
                      State#state{
                        waiting_acks=maps:remove({qos2, MessageId}, WAcks)}),
                    _ = vmq_metrics:incr_mqtt_pubcomp_sent(),
                    {NewState, [#mqtt5_pubcomp{message_id=MessageId,
                                               reason_code=?M5_SUCCESS}|Msgs]};
                {error, _Reason} ->
                    %% cant publish due to overload or netsplit,
                    %% client will retry
                    _ = vmq_metrics:incr_mqtt_error_publish(),
                    {State, []}
            end;
        not_found ->
            %% already delivered OR we pretended that we delivered the message
            %% as required by 3.1 . Client expects a PUBCOMP
            {State, [#mqtt5_pubcomp{message_id=MessageId,
                                    reason_code=?M5_SUCCESS}]}
    end;
connected(#mqtt5_pubcomp{message_id=MessageId}, State) ->
    #state{waiting_acks=WAcks} = State,
    %% qos2 flow
    _ = vmq_metrics:incr_mqtt_pubcomp_received(),
    case maps:get(MessageId, WAcks, not_found) of
        #mqtt5_pubrel{} ->
            handle_waiting_msgs(State#state{
                                  waiting_acks=maps:remove(MessageId, WAcks)});
        not_found -> % error or wrong waiting_ack, definitely not well behaving client
            lager:debug("stopped connected session, due to qos2 pubrel missing ~p", [MessageId]),
            _ = vmq_metrics:incr_mqtt_error_invalid_pubcomp(),
            terminate(normal, State)
    end;
connected(#mqtt5_subscribe{message_id=MessageId, topics=Topics, properties=_Properties}, State) ->
    #state{subscriber_id=SubscriberId, username=User,
           cap_settings=CAPSettings} = State,
    _ = vmq_metrics:incr_mqtt_subscribe_received(),
    OnAuthSuccess =
        fun(_User, _SubscriberId, MaybeChangedTopics) ->
                case vmq_reg:subscribe(CAPSettings#cap_settings.allow_subscribe, SubscriberId, convert_to_v4(MaybeChangedTopics)) of
                    {ok, _} = Res ->
                        vmq_plugin:all(on_subscribe_v1, [User, SubscriberId, MaybeChangedTopics]),
                        Res;
                    Res -> Res
                end
        end,
    case auth_on_subscribe(User, SubscriberId, Topics, OnAuthSuccess) of
        {ok, QoSs} ->
            Frame = #mqtt5_suback{message_id=MessageId, reason_codes=QoSs, properties=#{}},
            _ = vmq_metrics:incr_mqtt_suback_sent(),
            {State, [Frame]};
        {error, not_allowed} ->
            %% allow the parser to add the 0x80 Failure return code
            QoSs = [not_allowed || _ <- Topics],
            Frame = #mqtt5_suback{message_id=MessageId, reason_codes=QoSs, properties=#{}},
            _ = vmq_metrics:incr_mqtt_error_auth_subscribe(),
            {State, [Frame]};
        {error, _Reason} ->
            %% cant subscribe due to overload or netsplit,
            %% Subscribe uses QoS 1 so the client will retry
            _ = vmq_metrics:incr_mqtt_error_subscribe(),
            {State, []}
    end;
connected(#mqtt5_unsubscribe{message_id=MessageId, topics=Topics, properties =_Properties}, State) ->
    #state{subscriber_id=SubscriberId, username=User,
           cap_settings=CAPSettings} = State,
    _ = vmq_metrics:incr_mqtt_unsubscribe_received(),
    OnSuccess =
        fun(_SubscriberId, MaybeChangedTopics) ->
                vmq_reg:unsubscribe(CAPSettings#cap_settings.allow_unsubscribe, SubscriberId, MaybeChangedTopics)
        end,
    case unsubscribe(User, SubscriberId, Topics, OnSuccess) of
        ok ->
            ReasonCodes = [?M5_SUCCESS || _ <- Topics],
            Frame = #mqtt5_unsuback{message_id=MessageId, reason_codes=ReasonCodes, properties=#{}},
            _ = vmq_metrics:incr_mqtt_unsuback_sent(),
            {State, [Frame]};
        {error, _Reason} ->
            %% cant unsubscribe due to overload or netsplit,
            %% Unsubscribe uses QoS 1 so the client will retry
            _ = vmq_metrics:incr_mqtt_error_unsubscribe(),
            {State, []}
    end;
connected(#mqtt5_auth{properties=#{p_authentication_method := AuthMethod} = Props},
          #state{enhanced_auth = #auth_data{method = AuthMethod}} = State) ->
    case vmq_plugin:all_till_ok(on_auth_v1, [Props]) of
        {ok, Modifiers} ->
            OutProps = proplists:get_value(properties, Modifiers, #{}),
            Frame = #mqtt5_auth{reason_code = ?M5_SUCCESS,
                                properties = OutProps},
            NewState =
                State#state{
                  username = proplists:get_value(username, Modifiers, State#state.username)
                 },
            {NewState, [Frame]};
        {continue_auth, Modifiers} ->
            OutProps = proplists:get_value(properties, Modifiers, #{}),
            %% TODOv5: filter / rename properties?
            Frame = #mqtt5_auth{reason_code = ?M5_CONTINUE_AUTHENTICATION,
                                properties = OutProps},
            {State, [Frame]};
        {error, Reason} ->
            %% TODOv5
            throw({not_implemented, Reason})
    end;
connected(#mqtt5_pingreq{}, State) ->
    _ = vmq_metrics:incr_mqtt_pingreq_received(),
    Frame = #mqtt5_pingresp{},
    _ = vmq_metrics:incr_mqtt_pingresp_sent(),
    {State, [Frame]};
connected(#mqtt5_disconnect{}, State) ->
    _ = vmq_metrics:incr_mqtt_disconnect_received(),
    terminate(mqtt_client_disconnect, State);
connected(retry,
    #state{waiting_acks=WAcks, retry_interval=RetryInterval,
           retry_queue=RetryQueue} = State) ->
    {RetryFrames, NewRetryQueue} = handle_retry(RetryInterval, RetryQueue, WAcks),
    {State#state{retry_queue=NewRetryQueue}, RetryFrames};
connected({disconnect, Reason}, State) ->
    lager:debug("stop due to disconnect", []),
    terminate(Reason, State);
connected(check_keepalive, #state{last_time_active=Last, keep_alive=KeepAlive,
                                  subscriber_id=SubscriberId, username=UserName} = State) ->
    Now = os:timestamp(),
    case timer:now_diff(Now, Last) > (1500000 * KeepAlive) of
        true ->
            lager:warning("client ~p with username ~p stopped due to keepalive expired", [SubscriberId, UserName]),
            terminate(?DISCONNECT_KEEP_ALIVE, State);
        false ->
            set_keepalive_check_timer(KeepAlive),
            {State, []}
    end;
connected({'DOWN', _MRef, process, QPid, Reason}, #state{queue_pid=QPid} = State) ->
    queue_down_terminate(Reason, State);
connected({info_req, {Ref, CallerPid}, InfoItems}, State) ->
    CallerPid ! {Ref, {ok, get_info_items(InfoItems, State)}},
    {State, []};
connected({Ref, ok}, State) when is_reference(Ref) ->
    %% Late arrival of ack after enqueueing to a remote
    %% queue.
    %%
    %% TODO: this should be cleaned up for 2.0 as changing this is
    %% likely backwards incompatible.
    {State, []};
connected({Ref, {error, cant_remote_enqueue}}, State) when is_reference(Ref) ->
    %% Late arrival of negative ack after enqueueing to a remote
    %% queue.
    %%
    %% TODO: this should be cleaned up for 2.0 as changing this is
    %% likely backwards incompatible.
    {State, []};
connected(Unexpected, State) ->
    lager:debug("stopped connected session, due to unexpected frame type ~p", [Unexpected]),
    terminate({error, unexpected_message, Unexpected}, State).

connack_terminate(RC, State) ->
    connack_terminate(RC, [], State).

connack_terminate(RC, Properties, _State) ->
    %% TODO: we need to be able to handle the MQTT 5 reason codes in
    %% the metrics.
    %% _ = vmq_metrics:incr_mqtt_connack_sent(RC),
    {stop, normal, [#mqtt5_connack{session_present=false,
                                   reason_code=RC,
                                   properties=Properties}]}.

queue_down_terminate(shutdown, State) ->
    terminate(normal, State);
queue_down_terminate(Reason, #state{queue_pid=QPid} = State) ->
    terminate({error, {queue_down, QPid, Reason}}, State).

terminate(mqtt_client_disconnect, #state{clean_session=CleanSession} = State) ->
    _ = case CleanSession of
            true -> ok;
            false ->
                handle_waiting_acks_and_msgs(State)
        end,
    {stop, normal, []};
terminate(Reason, #state{clean_session=CleanSession} = State) ->
    _ = case CleanSession of
            true -> ok;
            false ->
                handle_waiting_acks_and_msgs(State)
        end,
    %% TODO: the counter update is missing the last will message
    maybe_publish_last_will(State),
    Disconnect = gen_disconnect(Reason),
    {stop, Reason, [Disconnect]}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

check_enhanced_auth(#mqtt5_connect{properties=#{p_authentication_method := AuthMethod}=Props} = F, State) ->
    case vmq_plugin:all_till_ok(on_auth_v1, [Props]) of
        {ok, Modifiers} ->
            %% TODOv5: what about the properties returned from
            %% `on_auth`?
            OutProps = proplists:get_value(properties, Modifiers, #{}),
            EnhancedAuth = #auth_data{method = AuthMethod},
            check_connect(F, OutProps, State#state{enhanced_auth = EnhancedAuth});
        {continue_auth, Modifiers} ->
            EnhancedAuth = #auth_data{method = AuthMethod,
                                      type = connect,
                                      data = F},
            OutProps = proplists:get_value(properties, Modifiers, #{}),
            Frame = #mqtt5_auth{reason_code = ?M5_CONTINUE_AUTHENTICATION,
                                properties = OutProps},
            {pre_connect_auth, State#state{enhanced_auth = EnhancedAuth}, [Frame]};
        {error, Reason} ->
            terminate(Reason, State)
    end;
check_enhanced_auth(F, State) ->
    %% No enhanced authentication
    check_connect(F, #{}, State).

check_connect(#mqtt5_connect{proto_ver=Ver, clean_start=CleanStart} = F, OutProps, State) ->
    CCleanStart = unflag(CleanStart),
    check_client_id(F, OutProps, State#state{clean_session=CCleanStart, proto_ver=Ver}).

check_client_id(#mqtt5_connect{} = Frame,
                OutProps,
                #state{username={preauth, UserNameFromCert}} = State) ->
    check_client_id(Frame#mqtt5_connect{username=UserNameFromCert},
                    OutProps,
                    State#state{username=UserNameFromCert});

check_client_id(#mqtt5_connect{client_id= <<>>, proto_ver=5} = F,
                OutProps,
                State) ->
    RandomClientId = random_client_id(),
    {MountPoint, _} = State#state.subscriber_id,
    SubscriberId = {MountPoint, RandomClientId},
    check_user(F#mqtt5_connect{client_id=RandomClientId},
               OutProps#{p_assigned_client_id => RandomClientId},
               State#state{subscriber_id=SubscriberId});
check_client_id(#mqtt5_connect{client_id=ClientId, proto_ver=V} = F,
                OutProps,
                #state{max_client_id_size=S} = State)
  when byte_size(ClientId) =< S ->
    {MountPoint, _} = State#state.subscriber_id,
    SubscriberId = {MountPoint, ClientId},
    case lists:member(V, ?ALLOWED_MQTT_VERSIONS) of
        true ->
            check_user(F, OutProps, State#state{subscriber_id=SubscriberId});
        false ->
            lager:warning("invalid protocol version for ~p ~p",
                          [SubscriberId, V]),
            connack_terminate(?M5_UNSUPPORTED_PROTOCOL_VERSION, State)
    end;
check_client_id(#mqtt5_connect{client_id=Id}, _OutProps, State) ->
    lager:warning("invalid client id ~p", [Id]),
    connack_terminate(?M5_CLIENT_IDENTIFIER_NOT_VALID, State).

check_user(#mqtt5_connect{username=User, password=Password, properties=Properties} = F,
           OutProps,
           State) ->
    case State#state.allow_anonymous of
        false ->
            case auth_on_register(User, Password, Properties, State) of
                {ok, QueueOpts, NewState} ->
                    register_subscriber(F, OutProps, QueueOpts, NewState);
                {error, no_matching_hook_found} ->
                    lager:error("can't authenticate client ~p due to
                                no_matching_hook_found", [State#state.subscriber_id]),
                    connack_terminate(?M5_BAD_USERNAME_OR_PASSWORD, State);
                {error, invalid_credentials} ->
                    lager:warning(
                      "can't authenticate client ~p due to
                              invalid_credentials", [State#state.subscriber_id]),
                    connack_terminate(?M5_BAD_USERNAME_OR_PASSWORD, State);
                {error, Error} ->
                    %% can't authenticate due to other reason
                    lager:warning(
                      "can't authenticate client ~p due to ~p",
                      [State#state.subscriber_id, Error]),
                    connack_terminate(?M5_BAD_USERNAME_OR_PASSWORD, State)
            end;
        true ->
            register_subscriber(F, OutProps, queue_opts(State, []), State)
    end.

register_subscriber(#mqtt5_connect{username=User}=F, OutProps,
                    QueueOpts, #state{peer=Peer, subscriber_id=SubscriberId,
                                      cap_settings=CAPSettings} = State) ->
    case vmq_reg:register_subscriber(CAPSettings#cap_settings.allow_register, SubscriberId, QueueOpts) of
        {ok, SessionPresent, QPid} ->
            monitor(process, QPid),
            _ = vmq_plugin:all(on_register_v1, [Peer, SubscriberId,
                                             User]),
            check_will(F, SessionPresent, OutProps, State#state{queue_pid=QPid,username=User});
        {error, Reason} ->
            lager:warning("can't register client ~p with username ~p due to ~p",
                          [SubscriberId, User, Reason]),
            connack_terminate(?M5_SERVER_UNAVAILABLE, State)
    end.


check_will(#mqtt5_connect{lwt=undefined}, SessionPresent, OutProps, State) ->
    OutProps0 = maybe_add_topic_alias_max(OutProps, State),
    {State, [#mqtt5_connack{session_present=SessionPresent,
                            reason_code=?M5_CONNACK_ACCEPT,
                            properties=OutProps0}]};
check_will(#mqtt5_connect{
              lwt=#mqtt5_lwt{will_topic=Topic, will_msg=Payload, will_qos=Qos,
                             will_retain=IsRetain,will_properties=_Properties}},
           SessionPresent,
           OutProps,
           State) ->
    #state{username=User, subscriber_id={MountPoint, _}=SubscriberId} = State,
    case maybe_apply_topic_alias(User, SubscriberId,
                                 #vmq_msg{routing_key=Topic,
                                          payload=Payload,
                                          msg_ref=msg_ref(),
                                          qos=Qos,
                                          retain=unflag(IsRetain),
                                          mountpoint=MountPoint
                                 },
                                 fun(Msg, _) -> {ok, Msg} end,
                                 State) of
        {ok, Msg, NewState} ->
            OutProps0 = maybe_add_topic_alias_max(OutProps, State),
            {NewState#state{will_msg=Msg},
             [#mqtt5_connack{session_present=SessionPresent,
                             reason_code=?M5_CONNACK_ACCEPT,
                             properties=OutProps0}]};
        {error, Reason} ->
            lager:warning("can't authenticate last will
                          for client ~p due to ~p", [SubscriberId, Reason]),
            connack_terminate(?M5_NOT_AUTHORIZED, State)
    end.

-spec maybe_apply_topic_alias(username(), password(), msg(), fun(), state()) ->
                                     {ok, msg(), state()} |
                                     {error, any()}.
maybe_apply_topic_alias(User, SubscriberId,
                        #vmq_msg{routing_key = [],
                                 properties = #{p_topic_alias := AliasId}} = Msg,
                        Fun, #state{topic_aliases_in = TA} = State) ->
    case maps:find(AliasId, TA) of
        {ok, AliasedTopic} ->
            case auth_on_publish(User, SubscriberId,
                                 remove_property(p_topic_alias, Msg#vmq_msg{routing_key = AliasedTopic}), Fun)
            of
                {ok, NewMsg} ->
                    {ok, NewMsg, State};
                {error, _} = E -> E
            end;
        error ->
            %% TODOv5 check the error code and add test case
            %% for it
            {error, ?M5_TOPIC_ALIAS_INVALID}
    end;
maybe_apply_topic_alias(User, SubscriberId,
                        #vmq_msg{properties = #{p_topic_alias := AliasId}} = Msg,
                        Fun, #state{topic_aliases_in = TA} = State) ->
    case auth_on_publish(User, SubscriberId, remove_property(p_topic_alias, Msg), Fun) of
        {ok, #vmq_msg{routing_key=MaybeChangedTopic}=NewMsg} ->
            %% TODOv5: Should we check here that the topic isn't empty?
            {ok, NewMsg, State#state{topic_aliases_in = TA#{AliasId => MaybeChangedTopic}}};
        {error, E} = E -> E
    end;
maybe_apply_topic_alias(_User, _SubscriberId, #vmq_msg{routing_key = []},
                        _Fun, _State) ->
    %% Empty topic but no topic alias property (handled above).
    %% TODOv5 this is an error check it is handled correctly
    %% and add test-case
    {error, ?M5_TOPIC_FILTER_INVALID};
maybe_apply_topic_alias(User, SubscriberId, Msg,
                        Fun, State) ->
    %% normal publish
    case auth_on_publish(User, SubscriberId, remove_property(p_topic_alias, Msg), Fun) of
        {ok, NewMsg} ->
            {ok, NewMsg, State};
        {error, _} = E -> E
    end.

remove_property(p_topic_alias, #vmq_msg{properties = Properties} = Msg) ->
    Msg#vmq_msg{properties = maps:remove(p_topic_alias, Properties)}.

auth_on_register(User, Password, Properties, State) ->
    #state{clean_session=Clean, peer=Peer, cap_settings=CAPSettings,
           subscriber_id=SubscriberId} = State,
    HookArgs = [Peer, SubscriberId, User, Password, Clean, Properties],
    case vmq_plugin:all_till_ok(auth_on_register_v1, HookArgs) of
        ok ->
            {ok, queue_opts(State, []), State};
        {ok, Args} ->
            set_sock_opts(prop_val(tcp_opts, Args, [])),
            ChangedCAPSettings
            = CAPSettings#cap_settings{
                allow_register=?cap_val(allow_register, Args, CAPSettings),
                allow_publish=?cap_val(allow_publish, Args, CAPSettings),
                allow_subscribe=?cap_val(allow_subscribe, Args, CAPSettings),
                allow_unsubscribe=?cap_val(allow_unsubscribe, Args, CAPSettings)
               },

            %% for efficiency reason the max_message_size isn't kept in the state
            set_max_msg_size(prop_val(max_message_size, Args, max_msg_size())),

            ChangedState = State#state{
                             subscriber_id=?state_val(subscriber_id, Args, State),
                             clean_session=?state_val(clean_session, Args, State),
                             reg_view=?state_val(reg_view, Args, State),
                             max_message_rate=?state_val(max_message_rate, Args, State),
                             max_inflight_messages=?state_val(max_inflight_messages, Args, State),
                             shared_subscription_policy=?state_val(shared_subscription_policy, Args, State),
                             retry_interval=?state_val(retry_interval, Args, State),
                             upgrade_qos=?state_val(upgrade_qos, Args, State),
                             topic_alias_max=?state_val(topic_alias_max, Args, State),
                             topic_aliases_in=?state_val(topic_aliases_in, Args, State),
                             cap_settings=ChangedCAPSettings
                            },
            {ok, queue_opts(ChangedState, Args), ChangedState};
        {error, Reason} ->
            {error, Reason}
    end.

set_sock_opts(Opts) ->
    self() ! {set_sock_opts, Opts}.

-spec auth_on_subscribe(username(), subscriber_id(), [{topic(), qos()}],
                        fun((username(), subscriber_id(), [{topic(), qos()}]) ->
                                   {ok, [qos() | not_allowed]} | {error, atom()})
                       ) -> {ok, [qos() | not_allowed]} | {error, atom()}.
auth_on_subscribe(User, SubscriberId, Topics, AuthSuccess) ->
    case vmq_plugin:all_till_ok(auth_on_subscribe_v1,
                                [User, SubscriberId, convert_to_v4(Topics)]) of
        ok ->
            AuthSuccess(User, SubscriberId, Topics);
        {ok, NewTopics} when is_list(NewTopics) ->
            AuthSuccess(User, SubscriberId, NewTopics);
        {error, _} ->
            {error, not_allowed}
    end.

-spec unsubscribe(username(), subscriber_id(), [{topic(), qos()}],
                  fun((subscriber_id(), [{topic(), qos()}]) ->
                             ok | {error, not_ready})
                 ) -> ok | {error, not_ready}.
unsubscribe(User, SubscriberId, Topics, UnsubFun) ->
    TTopics =
        case vmq_plugin:all_till_ok(on_unsubscribe_v1, [User, SubscriberId, Topics]) of
            ok ->
                Topics;
            {ok, [[W|_]|_] = NewTopics} when is_binary(W) ->
                NewTopics;
            {error, _} ->
                Topics
        end,
    UnsubFun(SubscriberId, TTopics).

-spec auth_on_publish(username(), subscriber_id(), msg(),
                      fun((msg(), list()) -> {ok, msg()} | {error, atom()})
                        ) -> {ok, msg()} | {error, atom()}.
auth_on_publish(User, SubscriberId, #vmq_msg{routing_key=Topic,
                                             payload=Payload,
                                             qos=QoS,
                                             retain=IsRetain} = Msg,
                AuthSuccess) ->
    
    HookArgs = [User, SubscriberId, QoS, Topic, Payload, unflag(IsRetain)],
    case vmq_plugin:all_till_ok(auth_on_publish_v1, HookArgs) of
        ok ->
            AuthSuccess(Msg, HookArgs);
        {ok, ChangedPayload} when is_binary(ChangedPayload) ->
            HookArgs1 = [User, SubscriberId, QoS, Topic, ChangedPayload, unflag(IsRetain)],
            AuthSuccess(Msg#vmq_msg{payload=ChangedPayload}, HookArgs1);
        {ok, Args} when is_list(Args) ->
            #vmq_msg{mountpoint=MP} = Msg,
            ChangedTopic = proplists:get_value(topic, Args, Topic),
            ChangedPayload = proplists:get_value(payload, Args, Payload),
            ChangedQoS = proplists:get_value(qos, Args, QoS),
            ChangedIsRetain = proplists:get_value(retain, Args, IsRetain),
            ChangedMountpoint = proplists:get_value(mountpoint, Args, MP),
            HookArgs1 = [User, SubscriberId, ChangedQoS,
                         ChangedTopic, ChangedPayload, ChangedIsRetain],
            AuthSuccess(Msg#vmq_msg{routing_key=ChangedTopic,
                                    payload=ChangedPayload,
                                    qos=ChangedQoS,
                                    retain=ChangedIsRetain,
                                    mountpoint=ChangedMountpoint},
                        HookArgs1);
        {error, Re} ->
            lager:error("can't auth publish ~p due to ~p", [HookArgs, Re]),
            {error, not_allowed}
    end.

-spec publish(cap_settings(), module(), username(), subscriber_id(), msg(), state()) ->
                     {ok, msg(), state()} | {error, atom()}.
publish(CAPSettings, RegView, User, SubscriberId, Msg, State) ->
    maybe_apply_topic_alias(User, SubscriberId, Msg,
                            fun(MaybeChangedMsg, HookArgs) ->
                                    case on_publish_hook(vmq_reg:publish(CAPSettings#cap_settings.allow_publish, RegView, MaybeChangedMsg),
                                                         HookArgs) of
                                        ok -> {ok, MaybeChangedMsg};
                                        E -> E
                                    end
                            end,
                            State).

-spec on_publish_hook(ok | {error, _}, list()) -> ok | {error, _}.
on_publish_hook(ok, HookParams) ->
    _ = vmq_plugin:all(on_publish_v1, HookParams),
    ok;
on_publish_hook(Other, _) -> Other.

-spec dispatch_publish(qos(), msg_id(), msg(), state()) ->
    list() | {state(), list()} | {error, not_allowed}.
dispatch_publish(Qos, MessageId, Msg, State) ->
    dispatch_publish_(Qos, MessageId, Msg, State).

-spec dispatch_publish_(qos(), msg_id(), msg(), state()) ->
    list() | {error, not_allowed}.
dispatch_publish_(0, MessageId, Msg, State) ->
    dispatch_publish_qos0(MessageId, Msg, State);
dispatch_publish_(1, MessageId, Msg, State) ->
    dispatch_publish_qos1(MessageId, Msg, State);
dispatch_publish_(2, MessageId, Msg, State) ->
    dispatch_publish_qos2(MessageId, Msg, State).

-spec dispatch_publish_qos0(msg_id(), msg(), state()) ->
    list()
    | {state(), list()}
    | {error, not_allowed}.
dispatch_publish_qos0(_MessageId, Msg, State) ->
    #state{username=User, subscriber_id=SubscriberId, proto_ver=Proto,
           cap_settings=CAPSettings, reg_view=RegView} = State,
    case publish(CAPSettings, RegView, User, SubscriberId, Msg, State) of
        {ok, _, NewState} ->
            {NewState, []};
        {error, not_allowed} when Proto == 4 ->
            %% we have to close connection for 3.1.1
            _ = vmq_metrics:incr_mqtt_error_auth_publish(),
            {error, not_allowed};
        {error, _Reason} ->
            %% can't publish due to overload or netsplit
            _ = vmq_metrics:incr_mqtt_error_publish(),
            []
    end.

-spec dispatch_publish_qos1(msg_id(), msg(), state()) ->
    list() | {error, not_allowed}.
dispatch_publish_qos1(MessageId, Msg, State) ->
        #state{username=User, subscriber_id=SubscriberId, proto_ver=Proto,
               cap_settings=CAPSettings, reg_view=RegView} = State,
    case publish(CAPSettings, RegView, User, SubscriberId, Msg, State) of
        {ok, _, NewState} ->
            _ = vmq_metrics:incr_mqtt_puback_sent(),
                {NewState, [#mqtt5_puback{message_id=MessageId,
                                          reason_code=?M5_SUCCESS,
                                          properties=#{}}]};
        {error, not_allowed} when Proto == 4 ->
            %% we have to close connection for 3.1.1
            _ = vmq_metrics:incr_mqtt_error_auth_publish(),
            {error, not_allowed};
        {error, not_allowed} ->
            %% we pretend as everything is ok for 3.1 and Bridge
            _ = vmq_metrics:incr_mqtt_error_auth_publish(),
            _ = vmq_metrics:incr_mqtt_puback_sent(),
            [#mqtt5_puback{message_id=MessageId}];
        {error, _Reason} ->
            %% can't publish due to overload or netsplit
            _ = vmq_metrics:incr_mqtt_error_publish(),
            []
    end.

-spec dispatch_publish_qos2(msg_id(), msg(), state()) -> list() |
                                                         {state(), list()} |
                                                         {error, not_allowed}.
dispatch_publish_qos2(MessageId, Msg, State) ->
    #state{username=User, subscriber_id=SubscriberId, proto_ver=Proto,
           waiting_acks=WAcks, retry_interval=RetryInterval,
           retry_queue=RetryQueue} = State,

    case maybe_apply_topic_alias(User, SubscriberId, Msg,
                                 fun(MaybeChangedMsg, _) -> {ok, MaybeChangedMsg} end,
                                 State) of
        {ok, NewMsg, State0} ->
            Frame = #mqtt5_pubrec{message_id=MessageId, reason_code=?M5_SUCCESS, properties=#{}},
            _ = vmq_metrics:incr_mqtt_pubrec_sent(),
            {State0#state{
               retry_queue=set_retry({qos2, MessageId}, RetryInterval, RetryQueue),
               waiting_acks=maps:put({qos2, MessageId}, {Frame, NewMsg}, WAcks)},
             [Frame]};
        {error, not_allowed} when Proto == 4 ->
            %% we have to close connection for 3.1.1
            _ = vmq_metrics:incr_mqtt_error_auth_publish(),
            {error, not_allowed};
        {error, not_allowed} ->
            %% we pretend as everything is ok for 3.1 and Bridge
            _ = vmq_metrics:incr_mqtt_error_auth_publish(),
            _ = vmq_metrics:incr_mqtt_pubrec_sent(),
            Frame = #mqtt5_pubrec{message_id=MessageId},
            [Frame];
        {error, _Reason} ->
            %% can't publish due to overload or netsplit
            _ = vmq_metrics:incr_mqtt_error_publish(),
            []
    end.

-spec handle_waiting_acks_and_msgs(state()) -> ok.
handle_waiting_acks_and_msgs(State) ->
    #state{waiting_acks=WAcks, waiting_msgs=WMsgs, queue_pid=QPid} = State,
    MsgsToBeDeliveredNextTime =
    lists:foldl(fun ({{qos2, _}, _}, Acc) ->
                      Acc;
                  ({MsgId, #mqtt5_pubrel{} = Frame}, Acc) ->
                      %% unacked PUBREL Frame
                      Bin = vmq_parser_mqtt5:serialise(Frame),
                      [{deliver_bin, {MsgId, Bin}}|Acc];
                  ({MsgId, Bin}, Acc) when is_binary(Bin) ->
                      %% unacked PUBREL Frame
                      [{deliver_bin, {MsgId, Bin}}|Acc];
                  ({_MsgId, #vmq_msg{qos=QoS} = Msg}, Acc) ->
                      [{deliver, QoS, Msg#vmq_msg{dup=true}}|Acc]
              end, WMsgs,
                %% 3. the sorted list has the oldest waiting-ack at the head.
                %% the way we add it to the accumulator 'WMsgs' we have to
                %% reverse the list to make sure the oldest stays at the head.
                lists:reverse(
                  %% 2. we have to sort this list given the message id
                  lists:keysort(1,
                                %% 1. maps:to_list gives us an
                                %% arbitrary ordered list, (it looks
                                %% like it is ordered with the newest
                                %% item at the to)
                                maps:to_list(WAcks)
                               )
                 )),
    catch vmq_queue:set_last_waiting_acks(QPid, MsgsToBeDeliveredNextTime).

handle_waiting_msgs(#state{waiting_msgs=[]} = State) ->
    {State, []};
handle_waiting_msgs(#state{waiting_msgs=Msgs, queue_pid=QPid} = State) ->
    case handle_messages(lists:reverse(Msgs), [], 0, State, []) of
        {NewState, HandledMsgs, []} ->
            %% we're ready to take more
            vmq_queue:notify(QPid),
            {NewState#state{waiting_msgs=[]}, HandledMsgs};
        {NewState, HandledMsgs, Waiting} ->
            %% TODO: since we don't notfiy the queue it is now possible
            %% that ALSO QoS0 messages are getting queued up, and need
            %% to wait until check_in_flight(_) returns true again.
            %% That's unfortunate, but would need a different implementation
            %% of the vmq_queue FSM which differentiates between QoS.
            {NewState#state{waiting_msgs=Waiting}, HandledMsgs}
    end.

handle_messages([{deliver, 0, Msg}|Rest], Frames, BinCnt, State, Waiting) ->
    {Frame, NewState} = prepare_frame(0, Msg, State),
    handle_messages(Rest, [Frame|Frames], BinCnt, NewState, Waiting);
handle_messages([{deliver, QoS, Msg} = Obj|Rest], Frames, BinCnt, State, Waiting) ->
    case check_in_flight(State) of
        true ->
            {Frame, NewState} = prepare_frame(QoS, Msg, State),
            handle_messages(Rest, [Frame|Frames], BinCnt, NewState, Waiting);
        false ->
            % only qos 1&2 are constrained by max_in_flight
            handle_messages(Rest, Frames, BinCnt, State, [Obj|Waiting])
    end;
handle_messages([{deliver_bin, {_, Bin} = Term}|Rest], Frames, BinCnt, State, Waiting) ->
    handle_messages(Rest, [Bin|Frames], BinCnt, handle_bin_message(Term, State), Waiting);
handle_messages([], [], _, State, Waiting) ->
    {State, [], Waiting};
handle_messages([], Frames, BinCnt, State, Waiting) ->
    NrOfFrames = length(Frames) - BinCnt, %% subtract pubrel frames
    _ = vmq_metrics:incr_mqtt_publishes_sent(NrOfFrames),
    {State, Frames, Waiting}.

handle_bin_message({MsgId, Bin}, State) ->
    %% this is called when a pubrel is retried after a client reconnects
    #state{waiting_acks=WAcks, retry_interval=RetryInterval, retry_queue=RetryQueue} = State,
    _ = vmq_metrics:incr_mqtt_pubrel_sent(),
    State#state{retry_queue=set_retry(MsgId, RetryInterval, RetryQueue),
                waiting_acks=maps:put(MsgId, Bin, WAcks)}.

prepare_frame(QoS, Msg, State) ->
    #state{username=User, subscriber_id=SubscriberId, waiting_acks=WAcks,
           retry_queue=RetryQueue, retry_interval=RetryInterval} = State,
    #vmq_msg{routing_key=Topic,
             payload=Payload,
             retain=IsRetained,
             dup=IsDup,
             qos=MsgQoS,
             properties=Properties,
             expiry_ts=ExpiryTS} = Msg,
    NewQoS = maybe_upgrade_qos(QoS, MsgQoS, State),
    HookArgs = [User, SubscriberId, Topic, Payload],
    {NewTopic, NewPayload} =
    case vmq_plugin:all_till_ok(on_deliver_v1, HookArgs) of
        {error, _} ->
            %% no on_deliver hook specified... that's ok
            {Topic, Payload};
        ok ->
            {Topic, Payload};
        {ok, ChangedPayload} when is_binary(ChangedPayload) ->
            {Topic, ChangedPayload};
        {ok, Args} when is_list(Args) ->
            ChangedTopic = proplists:get_value(topic, Args, Topic),
            ChangedPayload = proplists:get_value(payload, Args, Payload),
            {ChangedTopic, ChangedPayload}
    end,
    {OutgoingMsgId, State1} = get_msg_id(NewQoS, State),
    Frame = #mqtt5_publish{message_id=OutgoingMsgId,
                           topic=NewTopic,
                           qos=NewQoS,
                           retain=IsRetained,
                           dup=IsDup,
                           payload=NewPayload,
                           properties=update_expiry_interval(Properties, ExpiryTS)},
    case NewQoS of
        0 ->
            {Frame, State1};
        _ ->
            {Frame, State1#state{
                      retry_queue=set_retry(OutgoingMsgId, RetryInterval, RetryQueue),
                      waiting_acks=maps:put(OutgoingMsgId,
                                            Msg#vmq_msg{qos=NewQoS}, WAcks)}}
    end.

-spec maybe_publish_last_will(state()) -> ok.
maybe_publish_last_will(#state{will_msg=undefined}) -> ok;
maybe_publish_last_will(#state{subscriber_id=SubscriberId, username=User,
                               will_msg=Msg, reg_view=RegView, cap_settings=CAPSettings}) ->
    #vmq_msg{qos=QoS, routing_key=Topic, payload=Payload, retain=IsRetain} = Msg,
    HookArgs = [User, SubscriberId, QoS, Topic, Payload, IsRetain],
    _ = on_publish_hook(vmq_reg:publish(CAPSettings#cap_settings.allow_publish,
                                        RegView, Msg), HookArgs),
    ok.

-spec check_in_flight(state()) -> boolean().
check_in_flight(#state{waiting_acks=WAcks, max_inflight_messages=Max}) ->
    case Max of
        0 -> true;
        V ->
            maps:size(WAcks) < V
    end.

%% The MQTT specification requires that the QoS of a message delivered to a
%% subscriber is never upgraded to match the QoS of the subscription. If
%% upgrade_outgoing_qos is set true, messages sent to a subscriber will always
%% match the QoS of its subscription. This is a non-standard option not provided
%% for by the spec.
maybe_upgrade_qos(SubQoS, PubQoS, _) when SubQoS =< PubQoS ->
    %% already ref counted in vmq_reg
    SubQoS;
maybe_upgrade_qos(SubQoS, PubQoS, #state{upgrade_qos=true}) when SubQoS > PubQoS ->
    %% already ref counted in vmq_reg
    SubQoS;
maybe_upgrade_qos(_, PubQoS, _) ->
    %% matches when PubQoS is smaller than SubQoS and upgrade_qos=false
    %% SubQoS = 0, PubQoS cannot be smaller than 0, --> matched in first case
    %% SubQoS = 1|2, PubQoS = 0 ---> this case
    PubQoS.

-spec get_msg_id(qos(), state()) -> {msg_id(), state()}.
get_msg_id(0, State) ->
    {undefined, State};
get_msg_id(_, #state{next_msg_id=65535} = State) ->
    {1, State#state{next_msg_id=2}};
get_msg_id(_, #state{next_msg_id=MsgId} = State) ->
    {MsgId, State#state{next_msg_id=MsgId + 1}}.

-spec random_client_id() -> binary().
random_client_id() ->
    list_to_binary(["anon-", base64:encode_to_string(crypto:strong_rand_bytes(20))]).


set_keepalive_check_timer(0) -> ok;
set_keepalive_check_timer(KeepAlive) ->
    %% This allows us to heavily reduce start and cancel timers,
    %% however we're losing precision. But that's ok for the keepalive timer.
    _ = send_after(KeepAlive * 750, check_keepalive),
    ok.

-spec send_after(non_neg_integer(), any()) -> reference().
send_after(Time, Msg) ->
    vmq_mqtt_fsm_util:send_after(Time, Msg).

do_throttle(#state{max_message_rate=0}) -> false;
do_throttle(#state{max_message_rate=Rate}) ->
    not vmq_metrics:check_rate(msg_in_rate, Rate).

set_last_time_active(true, State) ->
    Now = os:timestamp(),
    State#state{last_time_active=Now};
set_last_time_active(false, State) ->
    State.

%% Retry Mechanism:
%%
%% Erlang's timer stick to a millisecond resolution, this can be problematic
%% if we have to retry many frames at the same time. This can happen during
%% heavy load, but also if a reconnect triggers a buch of offline messages
%% delivered to the client. If the client doesn't ack those messages we'll
%% retry many messages within the same millisecond. The firing of the timers
%% within the same millisecond aren't properly ordered. This can be simply
%% reproduced via the shell:
%%
%%  [erlang:send_after(1000, self(), I) || I <- lists:seq(1, 50)].
%%  ... after 1 second
%%  flush().
%%
%%  the result shows the unordered output of the fired timers.
%%
%%  To overcome this problem we stick to an own timer queue instead of setting
%%  a new timer for every frame to be retried. This enables to keep at most
%%  one retry timer per session at any time.
%%
%%  Timer Queue:
%%  When the retry timer fires `handle_retry/3` takes the oldest item from the
%%  queue and checks if a retry is required (no ack received on time). If it is
%%  required it adds the frame to the outgoing accumulator. If the frame has
%%  already been acked it ignores it. This process is repeated until the queue
%%  is empty or the time difference between timestamp a popped queue item  is
%%  smaller than the retry interval. In this case we set a new retry timer
%%  wrt. to the time difference.
%%
set_retry(MsgId, Interval, {[],[]} = RetryQueue) ->
    %% no waiting ack
    send_after(Interval, retry),
    Now = os:timestamp(),
    queue:in({Now, MsgId}, RetryQueue);
set_retry(MsgId, _, RetryQueue) ->
    Now = os:timestamp(),
    queue:in({Now, MsgId}, RetryQueue).

handle_retry(Interval, RetryQueue, WAcks) ->
    %% the fired timer was set for the oldest element in the queue!
    Now = os:timestamp(),
    handle_retry(Now, Interval, queue:out(RetryQueue), WAcks, []).

handle_retry(Now, Interval, {{value, {Ts, MsgId} = Val}, RetryQueue}, WAcks, Acc) ->
    NowDiff = timer:now_diff(Now, Ts) div 1000,
    case NowDiff < Interval of
        true ->
            send_after(Interval - NowDiff, retry),
            {Acc, queue:in_r(Val, RetryQueue)};
        false ->
            case get_retry_frame(MsgId, maps:get(MsgId, WAcks, not_found), Acc) of
                already_acked ->
                    handle_retry(Now, Interval, queue:out(RetryQueue), WAcks, Acc);
                NewAcc ->
                    NewRetryQueue = queue:in({Now, MsgId}, RetryQueue),
                    handle_retry(Now, Interval, queue:out(NewRetryQueue), WAcks, NewAcc)
            end
    end;
handle_retry(_, Interval, {empty, Queue}, _, Acc) when length(Acc) > 0 ->
    send_after(Interval, retry),
    {Acc, Queue};
handle_retry(_, _, {empty, Queue}, _, Acc) ->
    {Acc, Queue}.

get_retry_frame(MsgId, #vmq_msg{routing_key=Topic, qos=QoS, retain=Retain,
                         payload=Payload}, Acc) ->
    _ = vmq_metrics:incr_mqtt_publish_sent(),
    Frame = #mqtt5_publish{message_id=MsgId,
                          topic=Topic,
                          qos=QoS,
                          retain=Retain,
                          dup=true,
                          payload=Payload},
    [Frame|Acc];
get_retry_frame(_, #mqtt5_pubrel{} = Frame, Acc) ->
    _ = vmq_metrics:incr_mqtt_pubrel_sent(),
    [Frame|Acc];
get_retry_frame(_, #mqtt5_pubrec{} = Frame, Acc) ->
    _ = vmq_metrics:incr_mqtt_pubrec_sent(),
    [Frame|Acc];
get_retry_frame(_, {#mqtt5_pubrec{} = Frame, _}, Acc) ->
    _ = vmq_metrics:incr_mqtt_pubrec_sent(),
    [Frame|Acc];
get_retry_frame(_, not_found, _) ->
    %% already acked
    already_acked.

prop_val(Key, Args, Default) when is_tuple(Default) ->
    prop_val(Key, Args, Default, fun erlang:is_tuple/1);
prop_val(Key, Args, Default) when is_list(Default) ->
    prop_val(Key, Args, Default, fun erlang:is_list/1);
prop_val(Key, Args, Default) when is_integer(Default) ->
    prop_val(Key, Args, Default, fun erlang:is_integer/1);
prop_val(Key, Args, Default) when is_boolean(Default) ->
    prop_val(Key, Args, Default, fun erlang:is_boolean/1);
prop_val(Key, Args, Default) when is_atom(Default) ->
    prop_val(Key, Args, Default, fun erlang:is_atom/1);
prop_val(Key, Args, Default) when is_map(Default) ->
    prop_val(Key, Args, Default, fun erlang:is_map/1).

prop_val(Key, Args, Default, Validator) ->
    case proplists:get_value(Key, Args) of
        undefined -> Default;
        Val -> case Validator(Val) of
                   true -> Val;
                   false -> Default
               end
    end.

queue_opts(#state{clean_session=CleanSession}, Args) ->
    Opts = maps:from_list([{clean_session, CleanSession} | Args]),
    maps:merge(vmq_queue:default_opts(), Opts).

unflag(true) -> true;
unflag(false) -> false;
unflag(?true) -> true;
unflag(?false) -> false.

msg_ref() ->
    GUID =
    case get(guid) of
        undefined ->
            {{node(), self(), erlang:timestamp()}, 0};
        {S, I} ->
            {S, I + 1}
    end,
    put(guid, GUID),
    erlang:md5(term_to_binary(GUID)).

max_msg_size() ->
    case get(max_msg_size) of
        undefined ->
            %% no limit
            0;
        V -> V
    end.

set_max_msg_size(MaxMsgSize) when MaxMsgSize >= 0 ->
    put(max_msg_size, MaxMsgSize),
    MaxMsgSize.

info(Pid, Items) ->
    Ref = make_ref(),
    CallerRef = {Ref, self()},
    MRef = monitor(process, Pid),
    Pid ! {info_req, CallerRef, Items},
    receive
        {Ref, Ret} -> Ret;
        {'DOWN', MRef, process, Pid, Reason} ->
            {error, Reason}
    end.

get_info_items([], State) ->
    DefaultItems = [pid, client_id, user, peer_host, peer_port],
    get_info_items(DefaultItems, State);
get_info_items(Items, State) ->
    get_info_items(Items, State, []).

get_info_items([pid|Rest], State, Acc) ->
    get_info_items(Rest, State, [{pid, self()}|Acc]);
get_info_items([client_id|Rest], State, Acc) ->
    #state{subscriber_id={_, ClientId}} = State,
    get_info_items(Rest, State, [{client_id, ClientId}|Acc]);
get_info_items([mountpoint|Rest], State, Acc) ->
    #state{subscriber_id={MountPoint, _}} = State,
    get_info_items(Rest, State, [{mountpoint, MountPoint}|Acc]);
get_info_items([user|Rest], State, Acc) ->
    User =
    case State#state.username of
        {preauth, UserName} -> UserName;
        UserName -> UserName
    end,
    get_info_items(Rest, State, [{user, User}|Acc]);
get_info_items([node|Rest], State, Acc) ->
    get_info_items(Rest, State, [{node, node()}|Acc]);
get_info_items([peer_port|Rest], State, Acc) ->
    {_PeerIp, PeerPort} = State#state.peer,
    get_info_items(Rest, State, [{peer_port, PeerPort}|Acc]);
get_info_items([peer_host|Rest], State, Acc) ->
    {PeerIp, _} = State#state.peer,
    Host = list_to_binary(inet:ntoa(PeerIp)),
    get_info_items(Rest, State, [{peer_host, Host}|Acc]);
get_info_items([protocol|Rest], State, Acc) ->
    get_info_items(Rest, State, [{protocol, State#state.proto_ver}|Acc]);
get_info_items([timeout|Rest], State, Acc) ->
    get_info_items(Rest, State,
                   [{timeout, State#state.keep_alive}|Acc]);
get_info_items([retry_timeout|Rest], State, Acc) ->
    get_info_items(Rest, State,
                   [{timeout, State#state.retry_interval}|Acc]);
get_info_items([waiting_acks|Rest], State, Acc) ->
    Size = maps:size(State#state.waiting_acks),
    get_info_items(Rest, State,
                   [{waiting_acks, Size}|Acc]);
get_info_items([_|Rest], State, Acc) ->
    get_info_items(Rest, State, Acc);
get_info_items([], _, Acc) -> Acc.

gen_disconnect(?NORMAL_DISCONNECT) -> gen_disconnect_(?M5_NORMAL_DISCONNECT);
gen_disconnect(?SESSION_TAKEN_OVER) -> gen_disconnect_(?M5_SESSION_TAKEN_OVER);
gen_disconnect(?ADMINISTRATIVE_ACTION) -> gen_disconnect_(?M5_ADMINISTRATIVE_ACTION);
gen_disconnect(?DISCONNECT_KEEP_ALIVE) -> gen_disconnect_(?M5_KEEP_ALIVE_TIMEOUT);
gen_disconnect(?BAD_AUTH_METHOD) -> gen_disconnect_(?M5_BAD_AUTHENTICATION_METHOD);
gen_disconnect(?PROTOCOL_ERROR) -> gen_disconnect_(?M5_PROTOCOL_ERROR).

gen_disconnect_(RC) ->
    #mqtt5_disconnect{reason_code = RC, properties = []}.

convert_to_v4(Topics) ->
    %% TODO: this compatibility function is only here temporarily to
    %% not break existing tests during v5 development. Must be removed
    %% and tests fixed.
    lists:map(fun(#mqtt5_subscribe_topic{topic = T, qos = QoS}) -> {T, QoS};
                 (V4) -> V4
              end,
              Topics).

msg_expiration(#{p_message_expiry_interval := ExpireAfter}) ->
    {expire_after, ExpireAfter};
msg_expiration(_) ->
    undefined.

update_expiry_interval(Properties, undefined) -> Properties;
update_expiry_interval(Properties, {_, Remaining}) ->
    Properties#{p_message_expiry_interval => Remaining}.

maybe_add_topic_alias_max(Props, #state{topic_alias_max=0}) ->
    Props;
maybe_add_topic_alias_max(Props, #state{topic_alias_max=Max}) ->
    Props#{p_topic_alias_max => Max}.
