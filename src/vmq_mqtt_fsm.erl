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

-module(vmq_mqtt_fsm).
-include("vmq_server.hrl").
-export([init/2,
         data_in/2,
         msg_in/2,
         send/2,
         info_items/0,
         list_sessions/3,
         info/2]).

-export([msg_ref/0]).

-define(CLOSE_AFTER, 5000).
-define(HIBERNATE_AFTER, 5000).
-define(ALLOWED_MQTT_VERSIONS, [3, 4, 131]).
-define(MAX_SAMPLES, 10).

-type timestamp() :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}.
-type msg_id() :: undefined | 1..65535.

-record(state, {
          %% mqtt layer requirements
          next_msg_id=1                     :: msg_id(),
          subscriber_id                     :: undefined | subscriber_id(),
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
          queue_pid                         :: pid(),

          last_time_active=os:timestamp()   :: timestamp(),
          last_trigger=os:timestamp()       :: timestamp(),

          %% config
          allow_anonymous=false             :: boolean(),
          mountpoint=""                     :: mountpoint(),
          max_client_id_size=100            :: non_neg_integer(),

          %% changeable by auth_on_register
          max_inflight_messages=20          :: non_neg_integer(), %% 0 means unlimited
          max_message_size=0                :: non_neg_integer(), %% 0 means unlimited
          max_message_rate=0                :: non_neg_integer(), %% 0 means unlimited
          retry_interval=20000              :: pos_integer(),
          upgrade_qos=false                 :: boolean(),
          trade_consistency=false           :: boolean(),
          reg_view=vmq_reg_trie             :: atom()
         }).

-type state() :: #state{}.
-define(state_val(Key, Args, State), prop_val(Key, Args, State#state.Key)).

init(Peer, Opts) ->
    {A, B, C} = os:timestamp(),
    random:seed(A, B, C),
    MountPoint = proplists:get_value(mountpoint, Opts, ""),
    PreAuthUser =
    case lists:keyfind(preauth, 1, Opts) of
        false -> undefined;
        {_, undefined} -> undefined;
        {_, PreAuth} -> {preauth, PreAuth}
    end,
    AllowAnonymous = vmq_config:get_env(allow_anonymous, false),
    TradeConsistency = vmq_config:get_env(trade_consistency, false),
    RetryInterval = vmq_config:get_env(retry_interval, 20),
    MaxClientIdSize = vmq_config:get_env(max_client_id_size, 23),
    MaxInflightMsgs = vmq_config:get_env(max_inflight_messages, 20),
    MaxMessageSize = vmq_config:get_env(message_size_limit, 0),
    MaxMessageRate = vmq_config:get_env(max_message_rate, 0),
    UpgradeQoS = vmq_config:get_env(upgrade_outgoing_qos, false),
    RegView = vmq_config:get_env(default_reg_view, vmq_reg_trie),
    TRef = send_after(?CLOSE_AFTER, close_timeout),
    {wait_for_connect, #state{peer=Peer,
                                     upgrade_qos=UpgradeQoS,
                                     mountpoint=string:strip(MountPoint, right, $/),
                                     allow_anonymous=AllowAnonymous,
                                     max_inflight_messages=MaxInflightMsgs,
                                     max_message_size=MaxMessageSize,
                                     max_message_rate=MaxMessageRate,
                                     username=PreAuthUser,
                                     max_client_id_size=MaxClientIdSize,
                                     keep_alive_tref=TRef,
                                     retry_interval=1000 * RetryInterval,
                                     trade_consistency=TradeConsistency,
                                     reg_view=RegView}}.

data_in(Data, SessionState) when is_binary(Data) ->
    data_in(Data, SessionState, []).

data_in(Data, SessionState, OutAcc) ->
    case vmq_parser:parse(Data) of
        more ->
            {ok, SessionState, Data, serialise(OutAcc)};
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

%%% init  --> | wait_for_connect | --> | connected | --> terminate
in(Msg, {connected, State}, IsData) ->
    case connected(Msg, State) of
        {stop, _, _} = R -> R;
        {NewState, Out} ->
            {{connected, set_last_time_active(IsData, NewState)}, Out}
    end;
in(Msg, {wait_for_connect, State}, IsData) ->
    case wait_for_connect(Msg, State) of
        {stop, _, _} = R -> R;
        {NewState, Out} ->
            %% state transition to | connected |
            {{connected, set_last_time_active(IsData, NewState)}, Out}
    end.


send(SessionPid, Msg) ->
    SessionPid ! {?MODULE, Msg},
    ok.

serialise(Frames) ->
    serialise(Frames, []).

serialise([], Acc) -> Acc;
serialise([[]|Frames], Acc) ->
    serialise(Frames, Acc);
serialise([[B|T]|Frames], Acc) when is_binary(B) ->
    serialise([T|Frames], [B|Acc]);
serialise([[F|T]|Frames], Acc) ->
    serialise([T|Frames], [vmq_parser:serialise(F)|Acc]).

-spec wait_for_connect(mqtt_frame(), state()) ->
    {state(), [mqtt_frame() | binary()]} | {stop, any(), [mqtt_frame() | binary()]}.
wait_for_connect(#mqtt_connect{keep_alive=KeepAlive} = Frame,
                 #state{keep_alive_tref=TRef} = State) ->
    cancel_timer(TRef),
    _ = vmq_metrics:incr_mqtt_connect_received(),
    %% the client is allowed "grace" of a half a time period
    set_keepalive_timer(KeepAlive),
    check_connect(Frame, State#state{last_time_active=os:timestamp(),
                                       keep_alive=KeepAlive,
                                       keep_alive_tref=undefined});
wait_for_connect(close_timeout, State) ->
    lager:debug("[~p] stop due to timeout~n", [self()]),
    terminate(normal, State);
wait_for_connect({'DOWN', _MRef, process, QPid, Reason}, #state{queue_pid=QPid} = State) ->
    queue_down_terminate(Reason, State);
wait_for_connect(_, State) ->
    %% invalid handshake
    terminate(normal, State).

-spec connected(mqtt_frame(), state()) ->
    {state(), [mqtt_frame() | binary()]} |
    {state(), {throttle, [mqtt_frame() | binary()]}} |
    {stop, any(), [mqtt_frame() | binary()]}.
connected(#mqtt_publish{message_id=MessageId, topic=Topic,
                        qos=QoS, retain=IsRetain,
                        payload=Payload}, State) ->
    DoThrottle = do_throttle(State),
    #state{mountpoint=MountPoint,
           max_message_size=MaxMessageSize, reg_view=RegView,
           trade_consistency=Consistency} = State,
    %% we disallow Publishes on Topics prefixed with '$'
    %% this allows us to use such prefixes for e.g. '$SYS' Tree
    _ = vmq_metrics:incr_mqtt_publish_received(),
    Ret =
    case {Topic, valid_msg_size(Payload, MaxMessageSize)} of
        {[<<"$", _binary>> |_], _} ->
            %% $SYS
            [];
        {_, true} ->
            Msg = #vmq_msg{routing_key=Topic,
                           payload=Payload,
                           retain=unflag(IsRetain),
                           qos=QoS,
                           trade_consistency=Consistency,
                           reg_view=RegView,
                           mountpoint=MountPoint,
                           msg_ref=msg_ref()},
            dispatch_publish(QoS, MessageId, Msg, State);
        {_, false} ->
            _ = vmq_metrics:incr_mqtt_error_invalid_msg_size(),
            []
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
            lager:warning("subscriber ~p dropped ~p messages~n",
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
connected(#mqtt_puback{message_id=MessageId}, #state{waiting_acks=WAcks} = State) ->
    %% qos1 flow
    _ = vmq_metrics:incr_mqtt_puback_received(),
    case maps:get(MessageId, WAcks, not_found) of
        #vmq_msg{} ->
            handle_waiting_msgs(State#state{waiting_acks=maps:remove(MessageId, WAcks)});
        not_found ->
            _ = vmq_metrics:incr_mqtt_error_invalid_puback(),
            {State, []}
    end;
connected(#mqtt_pubrec{message_id=MessageId}, State) ->
    #state{waiting_acks=WAcks, retry_interval=RetryInterval,
           retry_queue=RetryQueue} = State,
    %% qos2 flow
    _ = vmq_metrics:incr_mqtt_pubrec_received(),
    case maps:get(MessageId, WAcks, not_found) of
        #vmq_msg{} ->
            PubRelFrame = #mqtt_pubrel{message_id=MessageId},
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
connected(#mqtt_pubrel{message_id=MessageId}, State) ->
    #state{waiting_acks=WAcks, username=User, subscriber_id=SubscriberId} = State,
    %% qos2 flow
    _ = vmq_metrics:incr_mqtt_pubrel_received(),
    case maps:get({qos2, MessageId} , WAcks, not_found) of
        {#mqtt_pubrec{}, #vmq_msg{routing_key=Topic, qos=QoS, payload=Payload,
                                  retain=IsRetain} = Msg} ->
            HookArgs = [User, SubscriberId, QoS, Topic, Payload, unflag(IsRetain)],
            case on_publish_hook(vmq_reg:publish(Msg), HookArgs) of
                ok ->
                    {NewState, Msgs} =
                    handle_waiting_msgs(
                      State#state{
                        waiting_acks=maps:remove({qos2, MessageId}, WAcks)}),
                    _ = vmq_metrics:incr_mqtt_pubcomp_sent(),
                    {NewState, [#mqtt_pubcomp{message_id=MessageId}|Msgs]};
                {error, _Reason} ->
                    %% cant publish due to overload or netsplit,
                    %% client will retry
                    _ = vmq_metrics:incr_mqtt_error_publish(),
                    {State, []}
            end;
        not_found ->
            %% already delivered OR we pretended that we delivered the message
            %% as required by 3.1 . Client expects a PUBCOMP
            {State, [#mqtt_pubcomp{message_id=MessageId}]}
    end;
connected(#mqtt_pubcomp{message_id=MessageId}, State) ->
    #state{waiting_acks=WAcks} = State,
    %% qos2 flow
    _ = vmq_metrics:incr_mqtt_pubcomp_received(),
    case maps:get(MessageId, WAcks, not_found) of
        #mqtt_pubrel{} ->
            handle_waiting_msgs(State#state{
                                  waiting_acks=maps:remove(MessageId, WAcks)});
        not_found -> % error or wrong waiting_ack, definitely not well behaving client
            lager:debug("stopped connected session, due to qos2 pubrel missing ~p", [MessageId]),
            _ = vmq_metrics:incr_mqtt_error_invalid_pubcomp(),
            terminate(normal, State)
    end;
connected(#mqtt_subscribe{message_id=MessageId, topics=Topics}, State) ->
    #state{subscriber_id=SubscriberId, username=User,
           trade_consistency=Consistency} = State,
    _ = vmq_metrics:incr_mqtt_subscribe_received(),
    case vmq_reg:subscribe(Consistency, User, SubscriberId, Topics) of
        {ok, QoSs} ->
            Frame = #mqtt_suback{message_id=MessageId, qos_table=QoSs},
            _ = vmq_metrics:incr_mqtt_suback_sent(),
            {State, [Frame]};
        {error, not_allowed} ->
            %% allow the parser to add the 0x80 Failure return code
            QoSs = [not_allowed || _ <- Topics],
            Frame = #mqtt_suback{message_id=MessageId, qos_table=QoSs},
            _ = vmq_metrics:incr_mqtt_error_auth_subscribe(),
            {State, [Frame]};
        {error, _Reason} ->
            %% cant subscribe due to overload or netsplit,
            %% Subscribe uses QoS 1 so the client will retry
            _ = vmq_metrics:incr_mqtt_error_subscribe(),
            {State, []}
    end;
connected(#mqtt_unsubscribe{message_id=MessageId, topics=Topics}, State) ->
    #state{subscriber_id=SubscriberId, username=User,
           trade_consistency=Consistency} = State,
    _ = vmq_metrics:incr_mqtt_unsubscribe_received(),
    case vmq_reg:unsubscribe(Consistency, User, SubscriberId, Topics) of
        ok ->
            Frame = #mqtt_unsuback{message_id=MessageId},
            _ = vmq_metrics:incr_mqtt_unsuback_sent(),
            {State, [Frame]};
        {error, _Reason} ->
            %% cant unsubscribe due to overload or netsplit,
            %% Unsubscribe uses QoS 1 so the client will retry
            _ = vmq_metrics:incr_mqtt_error_unsubscribe(),
            {State, []}
    end;
connected(#mqtt_pingreq{}, State) ->
    _ = vmq_metrics:incr_mqtt_pingreq_received(),
    Frame = #mqtt_pingresp{},
    _ = vmq_metrics:incr_mqtt_pingresp_sent(),
    {State, [Frame]};
connected(#mqtt_disconnect{}, State) ->
    _ = vmq_metrics:incr_mqtt_disconnect_received(),
    terminate(mqtt_client_disconnect, State);
connected(retry,
    #state{waiting_acks=WAcks, retry_interval=RetryInterval,
           retry_queue=RetryQueue} = State) ->
    {RetryFrames, NewRetryQueue} = handle_retry(RetryInterval, RetryQueue, WAcks),
    {State#state{retry_queue=NewRetryQueue}, RetryFrames};
connected(disconnect, State) ->
    lager:debug("[~p] stop due to disconnect", [self()]),
    terminate(normal, State);
connected(check_keepalive, #state{last_time_active=Last, keep_alive=KeepAlive} = State) ->
    Now = os:timestamp(),
    case (timer:now_diff(Now, Last) div 1000000) > KeepAlive of
        true ->
            lager:warning("[~p] stop due to keepalive expired", [self()]),
            terminate(normal, State);
        false ->
            set_keepalive_timer(KeepAlive),
            {State, []}
    end;
connected({'DOWN', _MRef, process, QPid, Reason}, #state{queue_pid=QPid} = State) ->
    queue_down_terminate(Reason, State);
connected({info_req, {Ref, CallerPid}, InfoItems}, State) ->
    CallerPid ! {Ref, {ok, get_info_items(InfoItems, State)}},
    {State, []};
connected(Unexpected, State) ->
    lager:debug("stopped connected session, due to unexpected frame type ~p", [Unexpected]),
    terminate({error, unexpected_message, Unexpected}, State).



connack_terminate(RC, _State) ->
    {stop, normal, [#mqtt_connack{session_present=false, return_code=RC}]}.

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
    {stop, Reason, []}.





%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

check_connect(#mqtt_connect{proto_ver=Ver, clean_session=CleanSession} = F, State) ->
    CCleanSession = unflag(CleanSession),
    check_client_id(F, State#state{clean_session=CCleanSession, proto_ver=Ver}).

check_client_id(#mqtt_connect{} = Frame,
                #state{username={preauth, UserNameFromCert}} = State) ->
    check_client_id(Frame#mqtt_connect{username=UserNameFromCert},
                    State#state{username=UserNameFromCert});

check_client_id(#mqtt_connect{client_id= <<>>, proto_ver=4} = F, State) ->
    %% [MQTT-3.1.3-8]
    %% If the Client supplies a zero-byte ClientId with CleanSession set to 0,
    %% the Server MUST respond to the >CONNECT Packet with a CONNACK return
    %% code 0x02 (Identifier rejected) and then close the Network
    case State#state.clean_session of
        false ->
            connack_terminate(?CONNACK_INVALID_ID, State);
        true ->
            RandomClientId = random_client_id(),
            SubscriberId = {State#state.mountpoint, RandomClientId},
            check_user(F#mqtt_connect{client_id=RandomClientId},
                       State#state{subscriber_id=SubscriberId})
    end;
check_client_id(#mqtt_connect{client_id= <<>>, proto_ver=3}, State) ->
    lager:warning("empty client id not allowed in mqttv3 ~p",
                [State#state.subscriber_id]),
    connack_terminate(?CONNACK_INVALID_ID, State);
check_client_id(#mqtt_connect{client_id=ClientId, proto_ver=V} = F,
                #state{max_client_id_size=S} = State)
  when byte_size(ClientId) =< S ->
    SubscriberId = {State#state.mountpoint, ClientId},
    case lists:member(V, ?ALLOWED_MQTT_VERSIONS) of
        true ->
            check_user(F, State#state{subscriber_id=SubscriberId});
        false ->
            lager:warning("invalid protocol version for ~p ~p",
                          [SubscriberId, V]),
            connack_terminate(?CONNACK_PROTO_VER, State)
    end;
check_client_id(#mqtt_connect{client_id=Id}, State) ->
    lager:warning("invalid client id ~p", [Id]),
    connack_terminate(?CONNACK_INVALID_ID, State).

check_user(#mqtt_connect{username=User, password=Password} = F, State) ->
    case State#state.allow_anonymous of
        false ->
            case auth_on_register(User, Password, State) of
                {ok, QueueOpts, #state{peer=Peer, subscriber_id=SubscriberId} = NewState} ->
                    case vmq_reg:register_subscriber(SubscriberId, QueueOpts) of
                        {ok, SessionPresent, QPid} ->
                            monitor(process, QPid),
                            _ = vmq_plugin:all(on_register, [Peer, SubscriberId,
                                                             User]),
                            check_will(F, SessionPresent, NewState#state{username=User, queue_pid=QPid});
                        {error, Reason} ->
                            lager:warning("can't register client ~p with username ~p due to ~p",
                                          [SubscriberId, User, Reason]),
                            _ = vmq_metrics:incr_mqtt_error_connect(),
                            connack_terminate(?CONNACK_SERVER, State)
                    end;
                {error, no_matching_hook_found} ->
                    lager:error("can't authenticate client ~p due to
                                no_matching_hook_found", [State#state.subscriber_id]),
                    _ = vmq_metrics:incr_mqtt_error_auth_connect(),
                    connack_terminate(?CONNACK_AUTH, State);
                {error, Errors} ->
                    case lists:keyfind(invalid_credentials, 2, Errors) of
                        {error, invalid_credentials} ->
                            lager:warning(
                              "can't authenticate client ~p due to
                              invalid_credentials", [State#state.subscriber_id]),
                            _ = vmq_metrics:incr_mqtt_error_auth_connect(),
                            connack_terminate(?CONNACK_CREDENTIALS, State);
                        false ->
                            %% can't authenticate due to other reasons
                            lager:warning(
                              "can't authenticate client ~p due to ~p",
                              [State#state.subscriber_id, Errors]),
                            _ = vmq_metrics:incr_mqtt_error_auth_connect(),
                            connack_terminate(?CONNACK_AUTH, State)
                    end
            end;
        true ->
            #state{peer=Peer, subscriber_id=SubscriberId} = State,
            case vmq_reg:register_subscriber(SubscriberId, queue_opts(State, [])) of
                {ok, SessionPresent, QPid} ->
                    monitor(process, QPid),
                    _ = vmq_plugin:all(on_register, [Peer, SubscriberId, User]),
                    check_will(F, SessionPresent, State#state{queue_pid=QPid, username=User});
                {error, Reason} ->
                    lager:warning("can't register client ~p due to reason ~p",
                                [SubscriberId, Reason]),
                    _ = vmq_metrics:incr_mqtt_error_connect(),
                    connack_terminate(?CONNACK_SERVER, State)
            end
    end.

check_will(#mqtt_connect{will_topic=undefined, will_msg=undefined}, SessionPresent, State) ->
    {State, [#mqtt_connack{session_present=SessionPresent, return_code=?CONNACK_ACCEPT}]};
check_will(#mqtt_connect{will_topic=Topic, will_msg=Payload, will_qos=Qos, will_retain=IsRetain},
           SessionPresent, State) ->
    #state{mountpoint=MountPoint, username=User, subscriber_id=SubscriberId,
           max_message_size=MaxMessageSize, trade_consistency=Consistency,
           reg_view=RegView} = State,
    case auth_on_publish(User, SubscriberId, #vmq_msg{routing_key=Topic,
                                                      payload=Payload,
                                                      msg_ref=msg_ref(),
                                                      qos=Qos,
                                                      retain=unflag(IsRetain),
                                                      trade_consistency=Consistency,
                                                      reg_view=RegView,
                                                      mountpoint=MountPoint
                                                      },
                         fun(Msg, _) -> {ok, Msg} end) of
        {ok, #vmq_msg{payload=MaybeNewPayload} = Msg} ->
            case valid_msg_size(MaybeNewPayload, MaxMessageSize) of
                true ->
                    {State#state{will_msg=Msg},
                     [#mqtt_connack{session_present=SessionPresent,
                                    return_code=?CONNACK_ACCEPT}]};
                false ->
                    lager:warning(
                      "last will message has invalid size for subscriber ~p",
                      [SubscriberId]),
                    connack_terminate(?CONNACK_SERVER, State)
            end;
        {error, Reason} ->
            lager:warning("can't authenticate last will
                          for client ~p due to ~p", [SubscriberId, Reason]),
            connack_terminate(?CONNACK_AUTH, State)
    end.

auth_on_register(User, Password, State) ->
    #state{clean_session=Clean, peer=Peer, subscriber_id={_, ClientId} = SubscriberId} = State,
    HookArgs = [Peer, SubscriberId, User, Password, Clean],
    case vmq_plugin:all_till_ok(auth_on_register, HookArgs) of
        ok ->
            {ok, queue_opts(State, []), State};
        {ok, Args} ->
            ChangedState = State#state{
                             subscriber_id={?state_val(mountpoint, Args, State), ClientId},
                             mountpoint=?state_val(mountpoint, Args, State),
                             clean_session=?state_val(clean_session, Args, State),
                             reg_view=?state_val(reg_view, Args, State),
                             max_message_size=?state_val(max_message_size, Args, State),
                             max_message_rate=?state_val(max_message_rate, Args, State),
                             max_inflight_messages=?state_val(max_inflight_messages, Args, State),
                             retry_interval=?state_val(retry_interval, Args, State),
                             upgrade_qos=?state_val(upgrade_qos, Args, State),
                             trade_consistency=?state_val(trade_consistency, Args, State)
                            },
            {ok, queue_opts(ChangedState, Args), ChangedState};
        {error, Reason} ->
            {error, Reason}
    end.

-spec auth_on_publish(username(), subscriber_id(), msg(),
                      fun((msg(), list()) -> {ok, msg()} | {error, atom()})
                        ) -> {ok, msg()} | {error, atom()}.
auth_on_publish(User, SubscriberId, #vmq_msg{routing_key=Topic,
                                             payload=Payload,
                                             qos=QoS,
                                             retain=IsRetain} = Msg,
               AuthSuccess) ->
    HookArgs = [User, SubscriberId, QoS, Topic, Payload, unflag(IsRetain)],
    case vmq_plugin:all_till_ok(auth_on_publish, HookArgs) of
        ok ->
            AuthSuccess(Msg, HookArgs);
        {ok, ChangedPayload} when is_binary(ChangedPayload) ->
            HookArgs1 = [User, SubscriberId, QoS, Topic, ChangedPayload, unflag(IsRetain)],
            AuthSuccess(Msg#vmq_msg{payload=ChangedPayload}, HookArgs1);
        {ok, Args} when is_list(Args) ->
            #vmq_msg{reg_view=RegView, mountpoint=MP} = Msg,
            ChangedTopic = proplists:get_value(topic, Args, Topic),
            ChangedPayload = proplists:get_value(payload, Args, Payload),
            ChangedRegView = proplists:get_value(reg_view, Args, RegView),
            ChangedQoS = proplists:get_value(qos, Args, QoS),
            ChangedIsRetain = proplists:get_value(retain, Args, IsRetain),
            ChangedMountpoint = proplists:get_value(mountpoint, Args, MP),
            HookArgs1 = [User, SubscriberId, ChangedQoS,
                         ChangedTopic, ChangedPayload, ChangedIsRetain],
            AuthSuccess(Msg#vmq_msg{routing_key=ChangedTopic,
                                    payload=ChangedPayload,
                                    reg_view=ChangedRegView,
                                    qos=ChangedQoS,
                                    retain=ChangedIsRetain,
                                    mountpoint=ChangedMountpoint},
                        HookArgs1);
        {error, Re} ->
            lager:error("can't auth publish ~p due to ~p", [HookArgs, Re]),
            {error, not_allowed}
    end.

-spec publish(username(), subscriber_id(), msg()) ->  {ok, msg()} | {error, atom()}.
publish(User, SubscriberId, Msg) ->
    auth_on_publish(User, SubscriberId, Msg,
                    fun(MaybeChangedMsg, HookArgs) ->
                            case on_publish_hook(vmq_reg:publish(MaybeChangedMsg),
                                            HookArgs) of
                                ok -> {ok, MaybeChangedMsg};
                                E -> E
                            end
                    end).

-spec on_publish_hook(ok | {error, _}, list()) -> ok | {error, _}.
on_publish_hook(ok, HookParams) ->
    _ = vmq_plugin:all(on_publish, HookParams),
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
    list() | {error, not_allowed}.
dispatch_publish_qos0(_MessageId, Msg, State) ->
    #state{username=User, subscriber_id=SubscriberId, proto_ver=Proto} = State,
    case publish(User, SubscriberId, Msg) of
        {ok, _} ->
            [];
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
    #state{username=User, subscriber_id=SubscriberId, proto_ver=Proto} = State,
    case publish(User, SubscriberId, Msg) of
        {ok, _} ->
            _ = vmq_metrics:incr_mqtt_puback_sent(),
            [#mqtt_puback{message_id=MessageId}];
        {error, not_allowed} when Proto == 4 ->
            %% we have to close connection for 3.1.1
            _ = vmq_metrics:incr_mqtt_error_auth_publish(),
            {error, not_allowed};
        {error, not_allowed} ->
            %% we pretend as everything is ok for 3.1 and Bridge
            _ = vmq_metrics:incr_mqtt_error_auth_publish(),
            _ = vmq_metrics:incr_mqtt_puback_sent(),
            [#mqtt_puback{message_id=MessageId}];
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

    case auth_on_publish(User, SubscriberId, Msg,
                        fun(MaybeChangedMsg, _) -> {ok, MaybeChangedMsg} end) of
        {ok, NewMsg} ->
            Frame = #mqtt_pubrec{message_id=MessageId},
            _ = vmq_metrics:incr_mqtt_pubrec_sent(),
            {State#state{
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
            Frame = #mqtt_pubrec{message_id=MessageId},
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
                  ({MsgId, #mqtt_pubrel{} = Frame}, Acc) ->
                      %% unacked PUBREL Frame
                      Bin = vmq_parser:serialise(Frame),
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
             qos=MsgQoS} = Msg,
    NewQoS = maybe_upgrade_qos(QoS, MsgQoS, State),
    HookArgs = [User, SubscriberId, Topic, Payload],
    {NewTopic, NewPayload} =
    case vmq_plugin:all_till_ok(on_deliver, HookArgs) of
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
    Frame = #mqtt_publish{message_id=OutgoingMsgId,
                          topic=NewTopic,
                          qos=NewQoS,
                          retain=IsRetained,
                          dup=IsDup,
                          payload=NewPayload},
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
maybe_publish_last_will(#state{subscriber_id=SubscriberId, username=User, will_msg=Msg}) ->
    #vmq_msg{qos=QoS, routing_key=Topic, payload=Payload, retain=IsRetain} = Msg,
    HookArgs = [User, SubscriberId, QoS, Topic, Payload, IsRetain],
    _ = on_publish_hook(vmq_reg:publish(Msg), HookArgs),
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
    list_to_binary(["anon-", base64:encode_to_string(crypto:rand_bytes(20))]).


set_keepalive_timer(0) -> ok;
set_keepalive_timer(KeepAlive) ->
    %% This allows us to heavily reduce start and cancel timers,
    %% however we're losing precision. But that's ok for the keepalive timer.
    _ = send_after(KeepAlive * 750, check_keepalive),
    ok.

-spec send_after(non_neg_integer(), any()) -> reference().
send_after(Time, Msg) ->
    erlang:send_after(Time, self(), {?MODULE, Msg}).

-spec cancel_timer('undefined' | reference()) -> 'ok'.
cancel_timer(undefined) -> ok;
cancel_timer(TRef) -> _ = erlang:cancel_timer(TRef), ok.

-spec valid_msg_size(binary(), non_neg_integer()) -> boolean().
valid_msg_size(_, 0) -> true;
valid_msg_size(Payload, Max) when byte_size(Payload) =< Max -> true;
valid_msg_size(_, _) -> false.

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
    Frame = #mqtt_publish{message_id=MsgId,
                          topic=Topic,
                          qos=QoS,
                          retain=Retain,
                          dup=true,
                          payload=Payload},
    [Frame|Acc];
get_retry_frame(_, #mqtt_pubrel{} = Frame, Acc) ->
    _ = vmq_metrics:incr_mqtt_pubrel_sent(),
    [Frame|Acc];
get_retry_frame(_, #mqtt_pubrec{} = Frame, Acc) ->
    _ = vmq_metrics:incr_mqtt_pubrec_sent(),
    [Frame|Acc];
get_retry_frame(_, {#mqtt_pubrec{} = Frame, _}, Acc) ->
    _ = vmq_metrics:incr_mqtt_pubrec_sent(),
    [Frame|Acc];
get_retry_frame(_, not_found, _) ->
    %% already acked
    already_acked.

prop_val(Key, Args, Default) when is_list(Default) ->
    prop_val(Key, Args, Default, fun erlang:is_list/1);
prop_val(Key, Args, Default) when is_integer(Default) ->
    prop_val(Key, Args, Default, fun erlang:is_integer/1);
prop_val(Key, Args, Default) when is_boolean(Default) ->
    prop_val(Key, Args, Default, fun erlang:is_boolean/1);
prop_val(Key, Args, Default) when is_atom(Default) ->
    prop_val(Key, Args, Default, fun erlang:is_atom/1).

prop_val(Key, Args, Default, Validator) ->
    case proplists:get_value(Key, Args) of
        undefined -> Default;
        Val -> case Validator(Val) of
                   true -> Val;
                   false -> Default
               end
    end.

queue_opts(#state{trade_consistency=TradeConsistency,
                  clean_session=CleanSession}, Args) ->
    Opts = maps:from_list([{trade_consistency, TradeConsistency},
                           {clean_session, CleanSession} | Args]),
    maps:merge(vmq_queue:default_opts(), Opts).

unflag(true) -> true;
unflag(false) -> false;
unflag(?true) -> true;
unflag(?false) -> false.

msg_ref() ->
    GUID =
    case get(guid) of
        undefined ->
            {{node(), self(), time_compat:timestamp()}, 0};
        {S, I} ->
            {S, I + 1}
    end,
    put(guid, GUID),
    erlang:md5(term_to_binary(GUID)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% info items
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

info_items() ->
    [pid, client_id, user, peer_host, peer_port,
     mountpoint, node, protocol, timeout, retry_timeout,
     waiting_acks].

-spec list_sessions([atom()|string()], function(), any()) -> any().
list_sessions(InfoItems, Fun, Acc) ->
    list_sessions(vmq_cluster:nodes(), InfoItems, Fun, Acc).
list_sessions([Node|Nodes], InfoItems, Fun, Acc) when Node == node() ->
    NewAcc = list_sessions_(InfoItems, Fun, Acc),
    list_sessions(Nodes, InfoItems, Fun, NewAcc);
list_sessions([Node|Nodes], InfoItems, Fun, Acc) ->
    Self = self(),
    F = fun(SubscriberId, Infos, NrOfSessions) ->
                Self ! {session_info, SubscriberId, Infos},
                NrOfSessions + 1
        end,
    Key = rpc:async_call(Node, ?MODULE, list_sessions_, [InfoItems, F, 0]),
    NewAcc = list_session_recv_loop(Key, Fun, Acc),
    list_sessions(Nodes, InfoItems, Fun, NewAcc);
list_sessions([], _, _, Acc) -> Acc.

list_sessions_(InfoItems, Fun, Acc) ->
    vmq_reg:fold_sessions(
      fun(SubscriberId, Pid, AccAcc) when is_pid(Pid) ->
              case info(Pid, InfoItems) of
                  {ok, Res} ->
                      Fun(SubscriberId, Res, AccAcc);
                  {error, _Reason} ->
                      %% session went down in the meantime
                      AccAcc
              end;
         (_, _, AccAcc) ->
              AccAcc
      end, Acc).

list_session_recv_loop(RPCKey, Fun, Acc) ->
    case rpc:nb_yield(RPCKey) of
        {value, _NrOfSessions} ->
            Acc;
        timeout ->
            receive
                {session_info, SubscriberId, Infos} ->
                    list_session_recv_loop(RPCKey, Fun,
                                           Fun(SubscriberId, Infos, Acc));
                _ ->
                    Acc
            end
    end.


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
    Host =
    case inet:gethostbyaddr(PeerIp) of
        {ok, {hostent, HostName, _, inet, _, _}} ->  HostName;
        _ -> PeerIp
    end,
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
