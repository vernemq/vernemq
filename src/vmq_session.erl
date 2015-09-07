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

-module(vmq_session).
-behaviour(gen_fsm).
-include("vmq_server.hrl").

-export([start_link/3,
         in/2,
         disconnect/1,
         disconnect/2,
         get_info/2,
         info_items/0,
         list_sessions/3,
         list_sessions_/3]).

-export([init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-export([wait_for_connect/2,
         connected/2]).

%% used for testing
-export([msg_ref/0]).

-define(CLOSE_AFTER, 5000).
-define(HIBERNATE_AFTER, 5000).
-define(ALLOWED_MQTT_VERSIONS, [3, 4, 131]).
-define(MAX_SAMPLES, 10).

-type proplist() :: [{atom(), any()}].
-type statename() :: atom().

-type timestamp() :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}.
-type counter() :: {non_neg_integer(), non_neg_integer(), [non_neg_integer()]}.
-type msg_id() :: undefined | 1..65535.

-record(state, {
                %% networking requirements
          send_fun                          :: function(),
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
          clean_session=false               :: flag(),
          proto_ver                         :: undefined | pos_integer(),
          queue_pid                         :: pid(),

          last_time_active=os:timestamp()   :: timestamp(),

          %% stats
          recv_cnt=init_counter()           :: counter(),
          send_cnt=init_counter()           :: counter(),
          pub_recv_cnt=init_counter()       :: counter(),
          pub_dropped_cnt=init_counter()    :: counter(),
          pub_send_cnt=init_counter()       :: counter(),

          %% config
          allow_anonymous=false             :: boolean(),
          mountpoint=""                     :: mountpoint(),
          max_client_id_size=23             :: non_neg_integer(),

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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% API FUNCTIONS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec start_link(peer(), function(), proplist()) -> {ok, pid()}.
start_link(Peer, SendFun, Opts) ->
    gen_fsm:start_link(?MODULE, [Peer, SendFun, Opts], []).

-spec disconnect(pid()) -> ok.
disconnect(FsmPid) ->
    disconnect(FsmPid, false).

-spec disconnect(pid(), boolean()) -> ok.
disconnect(FsmPid, true) ->
    gen_fsm:send_all_state_event(FsmPid, disconnect),
    wait_until_disconnected(FsmPid);
disconnect(FsmPid, false) ->
    gen_fsm:send_all_state_event(FsmPid, disconnect).

wait_until_disconnected(FsmPid) ->
    case is_process_alive(FsmPid) of
        true ->
            timer:sleep(100),
            wait_until_disconnected(FsmPid);
        false ->
            ok
    end.

-spec in(pid(), mqtt_frame()) ->  ok | throttle.
in(FsmPid, Event) ->
    save_sync_send_all_state_event(FsmPid, {input, Event}, ok, infinity).

-spec get_info(subscriber_id() | pid(), [atom()]) -> proplist().
get_info(SubscriberId, InfoItems) when is_tuple(SubscriberId) ->
    case vmq_reg:get_session_pids(SubscriberId) of
        {error, not_found} -> [];
        {ok, _QPid, Pids} ->
            [get_info(Pid, InfoItems)|| Pid <- Pids]
    end;
get_info(FsmPid, InfoItems) when is_pid(FsmPid) ->
    AInfoItems =
    [case I of
         _ when is_list(I) ->
             try list_to_existing_atom(I)
             catch
                 _:_ -> undefined
             end;
         _ when is_atom(I) -> I
     end || I <- InfoItems],
    save_sync_send_all_state_event(FsmPid, {get_info, AInfoItems}, [], 1000).

save_sync_send_all_state_event(FsmPid, Event, DefaultRet, Timeout) ->
    case catch gen_fsm:sync_send_all_state_event(FsmPid, Event, Timeout) of
        {'EXIT', {normal, _}} ->
            %% Session Pid died while sync_send_all_state_event
            %% was waiting to be handled
            DefaultRet;
        {'EXIT', {noproc, _}} ->
            %% Session Pid is dead, we will be dead pretty soon too.
            DefaultRet;
        {'EXIT', {timeout, _}} ->
            %% if the session is overloaded
            DefaultRet;
        {'EXIT', Reason} -> exit(Reason);
        Ret -> Ret
    end.

info_items() ->
    [pid, client_id, user, peer_host, peer_port, state,
     mountpoint, node, protocol, timeout, retry_timeout,
     recv_cnt, send_cnt, waiting_acks].

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
              Fun(SubscriberId, get_info(Pid, InfoItems), AccAcc);
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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% FSM FUNCTIONS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec wait_for_connect(timeout, state()) -> {stop, normal, state()}.
wait_for_connect(timeout, State) ->
    lager:debug("[~p] stop due to timeout~n", [self()]),
    {stop, normal, State}.

-spec connected(timeout
                | {retry, msg_id()}
                | {deliver_bin, {msg_id(), qos(), payload()}}
                | {deliver, topic(), payload(), qos(),
                   flag(), flag(), binary()}, state()) ->
    {next_state, connected, state()} | {stop, normal, state()}.
connected(keepalive_expired, State) ->
    lager:warning("[~p] stop due to ~p ~p~n", [self(), keepalive_expired, State#state.keep_alive]),
    {stop, normal, State};
connected({retry, MessageId}, State) ->
    #state{send_fun=SendFun, waiting_acks=WAcks,
           retry_interval=RetryInterval, send_cnt=SendCnt} = State,
    NewNewState =
    case maps:get(MessageId, WAcks, not_found) of
        not_found ->
            State;
        {_TRef, #vmq_msg{routing_key=Topic, qos=QoS, retain=Retain, payload=Payload} = Msg} ->
            Frame = #mqtt_publish{message_id=MessageId,
                                  topic=iolist_to_binary(vmq_topic:unword(Topic)),
                                  qos=QoS,
                                  retain=Retain,
                                  dup=true,
                                  payload=Payload},
            SendFun(Frame),
            Ref = send_after(RetryInterval, {retry, MessageId}),
            State#state{send_cnt=incr_msg_sent_cnt(SendCnt),
                        waiting_acks=maps:update(MessageId, {Ref, Msg}, WAcks)};
        {_TRef, #mqtt_pubrel{} = Frame} ->
            SendFun(Frame),
            Ref = send_after(RetryInterval, {retry, MessageId}),
            State#state{waiting_acks=maps:update(MessageId, {Ref, Frame}, WAcks)};
        {_TRef, #mqtt_pubrec{} = Frame, Msg} ->
            SendFun(Frame),
            Ref = send_after(RetryInterval, {retry, MessageId}),
            State#state{waiting_acks=maps:update(MessageId, {Ref, Frame, Msg}, WAcks)}
    end,
    {next_state, connected, NewNewState}.

-spec init(_) -> {ok, wait_for_connect, state(), ?CLOSE_AFTER}.
init([Peer, SendFun, Opts]) ->
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
    {ok, wait_for_connect, #state{peer=Peer, send_fun=SendFun,
                                  upgrade_qos=UpgradeQoS,
                                  mountpoint=string:strip(MountPoint, right, $/),
                                  allow_anonymous=AllowAnonymous,
                                  max_inflight_messages=MaxInflightMsgs,
                                  max_message_size=MaxMessageSize,
                                  max_message_rate=MaxMessageRate,
                                  username=PreAuthUser,
                                  max_client_id_size=MaxClientIdSize,
                                  retry_interval=1000 * RetryInterval,
                                  trade_consistency=TradeConsistency,
                                  reg_view=RegView
                                 }, ?CLOSE_AFTER}.

-spec handle_event(disconnect, _, state()) -> {stop, normal, state()}.
handle_event(disconnect, StateName, State) ->
    lager:debug("[~p] stop in state ~p due to ~p~n",
                  [self(), StateName, disconnect]),
    {stop, normal, State}.

-spec handle_sync_event(_, _, _, state()) -> {reply, _, statename(), state()} |
                                             {stop, {error, {unknown_req, _}},
                                              state()}.
handle_sync_event({input, #mqtt_disconnect{}}, _From, _, State) ->
    {stop, normal, ok, State};
handle_sync_event({input, Frame}, _From, StateName, State) ->
    #state{recv_cnt=RecvCnt, keep_alive_tref=TRef} = State,
    cancel_timer(TRef),
    case handle_frame(StateName, Frame,
                      State#state{recv_cnt=incr_msg_recv_cnt(RecvCnt)}) of
        {connected, #state{keep_alive=0} = NewState} ->
            {reply, ok, connected, maybe_trigger_counter_update(NewState)};
        {connected, #state{keep_alive=KeepAlive} = NewState} ->
            NewTRef = gen_fsm:send_event_after(KeepAlive, keepalive_expired),
            {reply, ok, connected, maybe_trigger_counter_update(
                                     NewState#state{keep_alive_tref=NewTRef})};
        {NextStateName, NewState} ->
            {reply, ok, NextStateName,
             maybe_trigger_counter_update(NewState), ?CLOSE_AFTER};
        Ret -> Ret
    end;
handle_sync_event({get_info, Items}, _From, StateName, State) ->
    Reply = get_info_items(Items, StateName, State),
    {reply, Reply, StateName, State};
handle_sync_event(Req, _From, _StateName, State) ->
    {stop, {error, {unknown_req, Req}}, State}.

-spec handle_info(any(), statename(), state()) ->
    {next_state, statename(), state()} | {stop, {error, any()}, state()}.
handle_info({mail, QPid, new_data}, StateName,
            #state{queue_pid=QPid} = State) ->
    vmq_queue:active(QPid),
    {next_state, StateName, State};
handle_info({mail, QPid, Msgs, _, Dropped}, StateName,
            #state{subscriber_id=SubscriberId, queue_pid=QPid} = State) ->
    NewState =
    case Dropped > 0 of
        true ->
            lager:warning("subscriber ~p dropped ~p messages~n",
                          [SubscriberId, Dropped]),
            drop(Dropped, State);
        false ->
            State
    end,
    NewState2 =
    case handle_messages(Msgs, [], NewState, []) of
        {NewState1, []} ->
            vmq_queue:notify(QPid),
            NewState1;
        {NewState1, Waiting} ->
            %% messages aren't delivered (yet) but are queued in this process
            %% we tell the queue to get rid of them
            vmq_queue:notify_recv(QPid),
            %% we call vmq_queue:notify as soon as
            %% the check_in_flight returns true again
            %% SEE: Comment in handle_waiting_msgs function.
            NewState1#state{waiting_msgs=Waiting}
    end,
    {next_state, StateName, maybe_trigger_counter_update(NewState2)};
handle_info({'DOWN', _MRef, process, QPid, Reason}, _StateName,
            #state{queue_pid=QPid} = State) ->
    case Reason of
        shutdown ->
            {stop, normal, State};
        _ ->
            {stop, {error, {queue_down, QPid, Reason}}, State}
    end;
handle_info(Info, _StateName, State) ->
    {stop, {error, {unknown_info, Info}}, State} .

-spec terminate(any(), statename(), state()) -> ok.
terminate(_Reason, connected, State) ->
    #state{clean_session=CleanSession} = State,
    _ = case CleanSession of
            true ->
                ok;
            false ->
                handle_waiting_acks_and_msgs(State)
        end,
    trigger_counter_update(State),
    %% TODO: the counter update is missing the last will message
    maybe_publish_last_will(State);
terminate(_Reason, _, _) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% INTERNALS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
handle_messages([{deliver, 0, Msg}|Rest], Frames, State, Waiting) ->
    {Frame, NewState} = prepare_frame(0, Msg, State),
    handle_messages(Rest, [Frame|Frames], NewState, Waiting);
handle_messages([{deliver, QoS, Msg} = Obj|Rest], Frames, State, Waiting) ->
    case check_in_flight(State) of
        true ->
            {Frame, NewState} = prepare_frame(QoS, Msg, State),
            handle_messages(Rest, [Frame|Frames], NewState, Waiting);
        false ->
            % only qos 1&2 are constrained by max_in_flight
            handle_messages(Rest, Frames, State, [Obj|Waiting])
    end;
handle_messages([{deliver_bin, Term}|Rest], Frames, State, Waiting) ->
    handle_messages(Rest, Frames, handle_bin_message(Term, State), Waiting);
handle_messages([], [], State, Waiting) ->
    {State, lists:reverse(Waiting)};
handle_messages([], Frames, State, Waiting) ->
    {send_publish_frames(Frames, State), lists:reverse(Waiting)}.

prepare_frame(QoS, Msg, State) ->
    #state{waiting_acks=WAcks, retry_interval=RetryInterval} = State,
    #vmq_msg{routing_key=Topic,
             payload=Payload,
             retain=IsRetained,
             dup=IsDup,
             qos=MsgQoS} = Msg,
    NewQoS = maybe_upgrade_qos(QoS, MsgQoS, State),
    {OutgoingMsgId, State1} = get_msg_id(NewQoS, State),
    Frame = #mqtt_publish{message_id=OutgoingMsgId,
                          topic=iolist_to_binary(vmq_topic:unword(Topic)),
                          qos=NewQoS,
                          retain=IsRetained,
                          dup=IsDup,
                          payload=Payload},
    case NewQoS of
        0 ->
            {Frame, State1};
        _ ->
            Ref = send_after(RetryInterval, {retry, OutgoingMsgId}),
            {Frame, State1#state{
                      waiting_acks=maps:put(OutgoingMsgId,
                                            {Ref, Msg#vmq_msg{qos=NewQoS}}, WAcks)}}
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

handle_bin_message({MsgId, Bin}, State) ->
    %% this is called when a pubrel is retried after a client reconnects
    #state{send_fun=SendFun, waiting_acks=WAcks,
           retry_interval=RetryInterval, send_cnt=SendCnt} = State,
    SendFun(Bin),
    Ref = send_after(RetryInterval, {retry, MsgId}),
    State#state{send_cnt=incr_msg_sent_cnt(SendCnt),
                waiting_acks=maps:put(MsgId, {Ref, Bin}, WAcks)}.

-spec handle_frame(statename(), mqtt_frame(), state()) ->
                   {statename(), state()} | {stop, atom(), state()}.

handle_frame(wait_for_connect, #mqtt_connect{keep_alive=KeepAlive} = Frame, State) ->
    _ = vmq_exo:incr_connect_received(),
    {A, B, C} = now(),
    random:seed(A, B, C),
    %% the client is allowed "grace" of a half a time period
    KKeepAlive = (KeepAlive + (KeepAlive div 2)) * 1000,
    check_connect(Frame, State#state{keep_alive=KKeepAlive});
handle_frame(wait_for_connect, _, #state{retry_interval=RetryInterval} = State) ->
    %% drop frame
    {wait_for_connect, State#state{keep_alive=RetryInterval}};

handle_frame(connected, #mqtt_puback{message_id=MessageId}, State) ->
    #state{waiting_acks=WAcks} = State,
    %% qos1 flow
    case maps:get(MessageId, WAcks, not_found) of
        {TRef, _} ->
            cancel_timer(TRef),
            {connected, handle_waiting_msgs(State#state{
                                              waiting_acks=maps:remove(MessageId, WAcks)
                                             })};
        not_found ->
            {connected, State}
    end;

handle_frame(connected, #mqtt_pubrec{message_id=MessageId}, State) ->
    #state{waiting_acks=WAcks, send_fun=SendFun,
           retry_interval=RetryInterval, send_cnt=SendCnt} = State,
    %% qos2 flow
    case maps:get(MessageId, WAcks, not_found) of
        {TRef, _} ->
            cancel_timer(TRef), % cancel republish timer
            PubRelFrame = #mqtt_pubrel{message_id=MessageId},
            SendFun(PubRelFrame),
            NewRef = send_after(RetryInterval, {retry, MessageId}),
            {connected, State#state{
                          send_cnt=incr_msg_sent_cnt(SendCnt),
                          waiting_acks=maps:update(MessageId, {NewRef, PubRelFrame}, WAcks)}};
        not_found ->
            lager:debug("stopped connected session, due to qos2 puback missing ~p", [MessageId]),
            {stop, normal, State}
    end;

handle_frame(connected, #mqtt_pubrel{message_id=MessageId}, State) ->
    #state{waiting_acks=WAcks, username=User, subscriber_id=SubscriberId} = State,
    %% qos2 flow
    NewState =
    case maps:get({qos2, MessageId} , WAcks, not_found) of
        {TRef, _, Msg} ->
            cancel_timer(TRef),
            case publish(User, SubscriberId, Msg) of
                {ok, _} ->
                    handle_waiting_msgs(
                      State#state{
                        waiting_acks=maps:remove({qos2, MessageId}, WAcks)});
                {error, _Reason} ->
                    %% cant publish due to overload or netsplit,
                    %% client will retry
                    State
            end;
        not_found ->
            %% already delivered, Client expects a PUBCOMP
            State
    end,
    {connected, send_frame(#mqtt_pubcomp{message_id=MessageId}, NewState)};

handle_frame(connected, #mqtt_pubcomp{message_id=MessageId}, State) ->
    #state{waiting_acks=WAcks} = State,
    %% qos2 flow
    case maps:get(MessageId, WAcks, not_found) of
        {TRef, _} ->
            cancel_timer(TRef), % cancel rpubrel timer
            {connected,
             handle_waiting_msgs(State#state{
                                   waiting_acks=maps:remove(MessageId, WAcks)})};
        not_found -> % error or wrong waiting_ack, definitely not well behaving client
            lager:debug("stopped connected session, due to qos2 pubrel missing ~p", [MessageId]),
            {stop, normal, State}
    end;

handle_frame(connected, #mqtt_publish{message_id=MessageId, topic=Topic,
                                      qos=QoS, retain=IsRetain,
                                      payload=Payload}, State) ->
    DoThrottle = do_throttle(State),
    #state{mountpoint=MountPoint, pub_recv_cnt=PubRecvCnt,
           max_message_size=MaxMessageSize, reg_view=RegView,
           trade_consistency=Consistency} = State,
    %% we disallow Publishes on Topics prefixed with '$'
    %% this allows us to use such prefixes for e.g. '$SYS' Tree
    NewState =
    case {Topic, valid_msg_size(Payload, MaxMessageSize)} of
        {[$$|_], _} ->
            %% $SYS
            State;
        {_, true} ->
            Msg = #vmq_msg{routing_key=vmq_topic:words(Topic),
                           payload=Payload,
                           retain=unflag(IsRetain),
                           qos=QoS,
                           trade_consistency=Consistency,
                           reg_view=RegView,
                           mountpoint=MountPoint,
                           msg_ref=msg_ref()},
            dispatch_publish(QoS, MessageId, Msg,
                             State#state{
                               pub_recv_cnt=incr_pub_recv_cnt(
                                              PubRecvCnt)
                              });
        {_, false} ->
            State
    end,
    case DoThrottle of
        false ->
            {connected, NewState};
        true ->
            {reply, throttle, connected, maybe_trigger_counter_update(NewState)}
    end;

handle_frame(connected, #mqtt_subscribe{message_id=MessageId, topics=Topics},
             State) ->
    #state{subscriber_id=SubscriberId, username=User,
           trade_consistency=Consistency} = State,
    case vmq_reg:subscribe(Consistency, User, SubscriberId, Topics) of
        ok ->
            {_, QoSs} = lists:unzip(Topics),
            NewState = send_frame(#mqtt_suback{message_id=MessageId,
                                               qos_table=QoSs}, State),
            {connected, NewState};
        {error, _Reason} ->
            %% cant subscribe due to overload or netsplit,
            %% Subscribe uses QoS 1 so the client will retry
            {connected, State}
    end;

handle_frame(connected, #mqtt_unsubscribe{message_id=MessageId, topics=Topics}, State) ->
    #state{subscriber_id=SubscriberId, username=User,
           trade_consistency=Consistency} = State,
    case vmq_reg:unsubscribe(Consistency, User, SubscriberId, Topics) of
        ok ->
            NewState = send_frame(#mqtt_unsuback{message_id=MessageId}, State),
            {connected, NewState};
        {error, _Reason} ->
            %% cant unsubscribe due to overload or netsplit,
            %% Unsubscribe uses QoS 1 so the client will retry
            {connected, State}
    end;

handle_frame(connected, #mqtt_pingreq{}, State) ->
    NewState = send_frame(#mqtt_pingresp{}, State),
    {connected, NewState};
handle_frame(connected, Unexpected, State) ->
    lager:debug("stopped connected session, due to unexpected frame type ~p", [Unexpected]),
    {stop, normal, State}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% INTERNAL
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
check_connect(#mqtt_connect{proto_ver=Ver, clean_session=CleanSession} = F, State) ->
    CCleanSession = unflag(CleanSession),
    check_client_id(F, State#state{clean_session=CCleanSession, proto_ver=Ver}).

check_client_id(#mqtt_connect{} = Frame,
                #state{username={preauth, UserNameFromCert}} = State) ->
    check_client_id(Frame#mqtt_connect{username=UserNameFromCert},
                    State#state{username=UserNameFromCert});

check_client_id(#mqtt_connect{client_id=undefined, proto_ver=4} = F, State) ->
    %% [MQTT-3.1.3-8]
    %% If the Client supplies a zero-byte ClientId with CleanSession set to 0,
    %% the Server MUST respond to the >CONNECT Packet with a CONNACK return
    %% code 0x02 (Identifier rejected) and then close the Network
    case State#state.clean_session of
        false ->
            {stop, normal, send_connack(?CONNACK_INVALID_ID, State)};
        true ->
            RandomClientId = random_client_id(),
            SubscriberId = {State#state.mountpoint, RandomClientId},
            check_user(F#mqtt_connect{client_id=RandomClientId},
                       State#state{subscriber_id=SubscriberId})
    end;
check_client_id(#mqtt_connect{client_id=undefined, proto_ver=3}, State) ->
    lager:warning("empty client id not allowed in mqttv3 ~p",
                [State#state.subscriber_id]),
    {stop, normal,
     send_connack(?CONNACK_INVALID_ID, State)};
check_client_id(#mqtt_connect{client_id=ClientId, proto_ver=V} = F,
                #state{max_client_id_size=S} = State)
  when length(ClientId) =< S ->
    SubscriberId = {State#state.mountpoint, ClientId},
    case lists:member(V, ?ALLOWED_MQTT_VERSIONS) of
        true ->
            check_user(F, State#state{subscriber_id=SubscriberId});
        false ->
            lager:warning("invalid protocol version for ~p ~p",
                          [SubscriberId, V]),
            {wait_for_connect,
             send_connack(?CONNACK_PROTO_VER, State)}
    end;
check_client_id(#mqtt_connect{client_id=Id}, State) ->
    lager:warning("invalid client id ~p", [Id]),
    {stop, normal,
     send_connack(?CONNACK_INVALID_ID, State)}.

do_throttle(#state{max_message_rate=0}) -> false;
do_throttle(#state{max_message_rate=Rate, pub_recv_cnt={AvgRecvCnt,_,_}}) ->
    AvgRecvCnt > Rate.

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

check_user(#mqtt_connect{username=User, password=Password} = F, State) ->
    case State#state.allow_anonymous of
        false ->
            case auth_on_register(User, Password, State) of
                {ok, QueueOpts, #state{peer=Peer, subscriber_id=SubscriberId} = NewState} ->
                    case vmq_reg:register_subscriber(SubscriberId, QueueOpts) of
                        {ok, QPid} ->
                            monitor(process, QPid),
                            _ = vmq_plugin:all(on_register, [Peer, SubscriberId,
                                                             User]),
                            check_will(F, NewState#state{username=User, queue_pid=QPid});
                        {error, Reason} ->
                            lager:warning("can't register client ~p with username ~p due to ~p",
                                          [SubscriberId, User, Reason]),
                            {wait_for_connect,
                             send_connack(?CONNACK_SERVER, State)}
                    end;
                {error, no_matching_hook_found} ->
                    lager:error("can't authenticate client ~p due to
                                no_matching_hook_found", [State#state.subscriber_id]),
                    {wait_for_connect,
                     send_connack(?CONNACK_AUTH, State)};
                {error, Errors} ->
                    case lists:keyfind(invalid_credentials, 2, Errors) of
                        {error, invalid_credentials} ->
                            lager:warning(
                              "can't authenticate client ~p due to
                              invalid_credentials", [State#state.subscriber_id]),
                            {wait_for_connect,
                             send_connack(?CONNACK_CREDENTIALS, State)};
                        false ->
                            %% can't authenticate due to other reasons
                            lager:warning(
                              "can't authenticate client ~p due to ~p",
                              [State#state.subscriber_id, Errors]),
                            {wait_for_connect,
                             send_connack(?CONNACK_AUTH, State)}
                    end
            end;
        true ->
            #state{peer=Peer, subscriber_id=SubscriberId} = State,
            case vmq_reg:register_subscriber(SubscriberId, queue_opts(State, [])) of
                {ok, QPid} ->
                    monitor(process, QPid),
                    _ = vmq_plugin:all(on_register, [Peer, SubscriberId, User]),
                    check_will(F, State#state{queue_pid=QPid, username=User});
                {error, Reason} ->
                    lager:warning("can't register client ~p due to reason ~p",
                                [SubscriberId, Reason]),
                    {wait_for_connect,
                     send_connack(?CONNACK_SERVER, State)}
            end
    end.

check_will(#mqtt_connect{will_topic=undefined, will_msg=undefined}, State) ->
    {connected, send_connack(?CONNACK_ACCEPT, State)};
check_will(#mqtt_connect{will_topic=Topic, will_msg=Payload, will_qos=Qos, will_retain=IsRetain},
           State) ->
    #state{mountpoint=MountPoint, username=User, subscriber_id=SubscriberId,
           max_message_size=MaxMessageSize, trade_consistency=Consistency,
           reg_view=RegView} = State,
    case auth_on_publish(User, SubscriberId, #vmq_msg{routing_key=vmq_topic:words(Topic),
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
                    {connected, send_connack(?CONNACK_ACCEPT,
                                             State#state{will_msg=Msg})};
                false ->
                    lager:warning(
                      "last will message has invalid size for subscriber ~p",
                      [SubscriberId]),
                    {wait_for_connect,
                     send_connack(?CONNACK_SERVER, State)}
            end;
        {error, Reason} ->
            lager:warning("can't authenticate last will
                          for client ~p due to ~p", [SubscriberId, Reason]),
            {wait_for_connect, send_connack(?CONNACK_AUTH, State)}
    end.

-spec send_connack(non_neg_integer(), state()) -> state().
send_connack(ReturnCode, State) ->
    send_frame(#mqtt_connack{return_code=ReturnCode}, State).

-spec send_frame(mqtt_frame(), state()) -> state().
send_frame(Frame, #state{send_fun=SendFun, send_cnt=SendCnt} = State) ->
    SendFun(Frame),
    State#state{send_cnt=incr_msg_sent_cnt(SendCnt)}.


-spec maybe_publish_last_will(state()) -> ok.
maybe_publish_last_will(#state{will_msg=undefined}) -> ok;
maybe_publish_last_will(#state{subscriber_id=SubscriberId, username=User, will_msg=Msg}) ->
    #vmq_msg{qos=QoS, routing_key=Topic, payload=Payload, retain=IsRetain} = Msg,
    HookArgs = [User, SubscriberId, QoS, Topic, Payload, IsRetain],
    _ = on_publish_hook(vmq_reg:publish(Msg), HookArgs),
    ok.


-spec dispatch_publish(qos(), msg_id(), msg(), state()) -> state().
dispatch_publish(Qos, MessageId, Msg, State) ->
    dispatch_publish_(Qos, MessageId, Msg, State).

-spec dispatch_publish_(qos(), msg_id(), msg(), state()) -> state().
dispatch_publish_(0, MessageId, Msg, State) ->
    dispatch_publish_qos0(MessageId, Msg, State);
dispatch_publish_(1, MessageId, Msg, State) ->
    dispatch_publish_qos1(MessageId, Msg, State);
dispatch_publish_(2, MessageId, Msg, State) ->
    dispatch_publish_qos2(MessageId, Msg, State).

-spec dispatch_publish_qos0(msg_id(), msg(), state()) -> state().
dispatch_publish_qos0(_MessageId, Msg, State) ->
    #state{username=User, subscriber_id=SubscriberId} = State,
    case publish(User, SubscriberId, Msg) of
        {ok, _} ->
            State;
        {error, _Reason} ->
            drop(State)
    end.

-spec dispatch_publish_qos1(msg_id(), msg(), state()) -> state().
dispatch_publish_qos1(MessageId, Msg, State) ->
    case check_in_flight(State) of
        true ->
            #state{username=User, subscriber_id=SubscriberId} = State,
            case publish(User, SubscriberId, Msg) of
                {ok, _} ->
                    NewState = send_frame(#mqtt_puback{message_id=MessageId}, State),
                    NewState;
                {error, _Reason} ->
                    %% can't publish due to overload or netsplit
                    drop(State)
            end;
        false ->
            drop(State)
    end.

-spec dispatch_publish_qos2(msg_id(), msg(), state()) -> state().
dispatch_publish_qos2(MessageId, Msg, State) ->
    case check_in_flight(State) of
        true ->
            #state{waiting_acks=WAcks, retry_interval=RetryInterval,
                   send_fun=SendFun, send_cnt=SendCnt} = State,
            Ref = send_after(RetryInterval, {retry, {qos2, MessageId}}),
            Frame = #mqtt_pubrec{message_id=MessageId},
            SendFun(Frame),
            State#state{
              send_cnt=incr_msg_sent_cnt(SendCnt),
              waiting_acks=maps:put(
                             {qos2, MessageId}, {Ref, Frame, Msg}, WAcks)};
        false ->
            drop(State)
    end.

-spec get_msg_id(qos(), state()) -> {msg_id(), state()}.
get_msg_id(0, State) ->
    {undefined, State};
get_msg_id(_, #state{next_msg_id=65535} = State) ->
    {1, State#state{next_msg_id=2}};
get_msg_id(_, #state{next_msg_id=MsgId} = State) ->
    {MsgId, State#state{next_msg_id=MsgId + 1}}.

drop(State) ->
    drop(1, State).

drop(I, #state{pub_dropped_cnt=PubDropped} = State) ->
    State#state{pub_dropped_cnt=incr_pub_dropped_cnt(I, PubDropped)}.

-spec send_publish_frames([mqtt_frame()], state()) -> state().
send_publish_frames(Frames, State) ->
    #state{send_fun=SendFun, send_cnt=SendCnt, pub_send_cnt=PubSendCnt} = State,
    ok = SendFun(Frames),
    NrOfFrames = length(Frames),
    State#state{
      send_cnt=incr_msg_sent_cnt(NrOfFrames, SendCnt),
      pub_send_cnt=incr_pub_sent_cnt(NrOfFrames, PubSendCnt)
     }.

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
            AuthSuccess(Msg#vmq_msg{payload=ChangedPayload}, HookArgs);
        {ok, Args} when is_list(Args) ->
            #vmq_msg{reg_view=RegView, mountpoint=MP} = Msg,
            ChangedTopic = proplists:get_value(topic, Args, Topic),
            ChangedPayload = proplists:get_value(payload, Args, Payload),
            ChangedRegView = proplists:get_value(reg_view, Args, RegView),
            ChangedQoS = proplists:get_value(qos, Args, QoS),
            ChangedIsRetain = proplists:get_value(retain, Args, IsRetain),
            ChangedMountpoint = proplists:get_value(mountpoint, Args, MP),
            AuthSuccess(Msg#vmq_msg{routing_key=ChangedTopic,
                                    payload=ChangedPayload,
                                    reg_view=ChangedRegView,
                                    qos=ChangedQoS,
                                    retain=ChangedIsRetain,
                                    mountpoint=ChangedMountpoint}, HookArgs);
        {error, _} ->
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

-spec random_client_id() -> string().
random_client_id() ->
    lists:flatten(["anon-", base64:encode_to_string(crypto:rand_bytes(20))]).

-spec handle_waiting_acks_and_msgs(state()) -> ok.
handle_waiting_acks_and_msgs(State) ->
    #state{waiting_acks=WAcks, waiting_msgs=WMsgs, queue_pid=QPid} = State,
    MsgsToBeDeliveredNextTime =
    maps:fold(fun ({qos2, _}, _, Acc) ->
                      Acc;
                  (MsgId, {TRef, #mqtt_pubrel{} = Frame}, Acc) ->
                      %% unacked PUBREL Frame
                      cancel_timer(TRef),
                      Bin = vmq_parser:serialise(Frame),
                      [{deliver_bin, {MsgId, Bin}}|Acc];
                  (MsgId, {TRef, Bin}, Acc) when is_binary(Bin) ->
                      %% unacked PUBREL Frame
                      cancel_timer(TRef),
                      [{deliver_bin, {MsgId, Bin}}|Acc];
                  (_MsgId, {TRef, #vmq_msg{qos=QoS} = Msg}, Acc) ->
                      cancel_timer(TRef),
                      [{deliver, QoS, Msg#vmq_msg{dup=true}}|Acc]
              end, WMsgs, WAcks),
    catch vmq_queue:set_last_waiting_acks(QPid, MsgsToBeDeliveredNextTime).


handle_waiting_msgs(#state{waiting_msgs=[]} = State) -> State;
handle_waiting_msgs(#state{waiting_msgs=Msgs, queue_pid=QPid} = State) ->
    case handle_messages(Msgs, [], State, []) of
        {NewState, []} ->
            %% we're ready to take more
            vmq_queue:notify(QPid),
            NewState#state{waiting_msgs=[]};
        {NewState, Waiting} ->
            %% TODO: since we don't notfiy the queue it is now possible
            %% that ALSO QoS0 messages are getting queued up, and need
            %% to wait until check_in_flight(_) returns true again.
            %% That's unfortunate, but would need a different implementation
            %% of the vmq_queue FSM which differentiates between QoS.
            NewState#state{waiting_msgs=Waiting}
    end.



-spec send_after(non_neg_integer(), any()) -> reference().
send_after(Time, Msg) ->
    gen_fsm:send_event_after(Time, Msg).

-spec cancel_timer('undefined' | reference()) -> 'ok'.
cancel_timer(undefined) -> ok;
cancel_timer(TRef) -> _ = gen_fsm:cancel_timer(TRef), ok.

-spec check_in_flight(state()) -> boolean().
check_in_flight(#state{waiting_acks=WAcks, max_inflight_messages=Max}) ->
    case Max of
        0 -> true;
        V ->
            maps:size(WAcks) < V
    end.

-spec valid_msg_size(binary(), non_neg_integer()) -> boolean().
valid_msg_size(_, 0) -> true;
valid_msg_size(Payload, Max) when byte_size(Payload) =< Max -> true;
valid_msg_size(_, _) -> false.

get_info_items([], StateName, State) ->
    DefaultItems = [pid, client_id, user, peer_host, peer_port, state],
    get_info_items(DefaultItems, StateName, State);
get_info_items(Items, StateName, State) ->
    get_info_items(Items, StateName, State, []).

get_info_items([pid|Rest], StateName, State, Acc) ->
    get_info_items(Rest, StateName, State, [{pid, self()}|Acc]);
get_info_items([client_id|Rest], StateName, State, Acc) ->
    #state{subscriber_id={_, ClientId}} = State,
    get_info_items(Rest, StateName, State, [{client_id, ClientId}|Acc]);
get_info_items([mountpoint|Rest], StateName, State, Acc) ->
    #state{subscriber_id={MountPoint, _}} = State,
    get_info_items(Rest, StateName, State, [{mountpoint, MountPoint}|Acc]);
get_info_items([user|Rest], StateName, State, Acc) ->
    User =
    case State#state.username of
        {preauth, UserName} -> UserName;
        UserName -> UserName
    end,
    get_info_items(Rest, StateName, State, [{user, User}|Acc]);
get_info_items([node|Rest], StateName, State, Acc) ->
    get_info_items(Rest, StateName, State, [{node, node()}|Acc]);
get_info_items([peer_port|Rest], StateName, State, Acc) ->
    {_PeerIp, PeerPort} = State#state.peer,
    get_info_items(Rest, StateName, State, [{peer_port, PeerPort}|Acc]);
get_info_items([peer_host|Rest], StateName, State, Acc) ->
    {PeerIp, _} = State#state.peer,
    Host =
    case inet:gethostbyaddr(PeerIp) of
        {ok, {hostent, HostName, _, inet, _, _}} ->  HostName;
        _ -> PeerIp
    end,
    get_info_items(Rest, StateName, State, [{peer_host, Host}|Acc]);
get_info_items([protocol|Rest], StateName, State, Acc) ->
    get_info_items(Rest, StateName, State,
                   [{protocol, State#state.proto_ver}|Acc]);
get_info_items([state|Rest], StateName, State, Acc) ->
    get_info_items(Rest, StateName, State,
                   [{state, StateName}|Acc]);
get_info_items([timeout|Rest], StateName, State, Acc) ->
    get_info_items(Rest, StateName, State,
                   [{timeout, State#state.keep_alive}|Acc]);
get_info_items([retry_timeout|Rest], StateName, State, Acc) ->
    get_info_items(Rest, StateName, State,
                   [{timeout, State#state.retry_interval}|Acc]);
get_info_items([recv_cnt|Rest], StateName, #state{recv_cnt={_,V,_}} = State, Acc) ->
    get_info_items(Rest, StateName, State,
                   [{recv_cnt, V}|Acc]);
get_info_items([send_cnt|Rest], StateName, #state{send_cnt={_,V,_}} = State, Acc) ->
    get_info_items(Rest, StateName, State,
                   [{send_cnt, V}|Acc]);
get_info_items([waiting_acks|Rest], StateName, State, Acc) ->
    Size = maps:size(State#state.waiting_acks),
    get_info_items(Rest, StateName, State,
                   [{waiting_acks, Size}|Acc]);
get_info_items([_|Rest], StateName, State, Acc) ->
    get_info_items(Rest, StateName, State, Acc);
get_info_items([], _, _, Acc) -> Acc.

init_counter() ->
    {0, 0, []}.

incr_msg_sent_cnt(SndCnt) -> incr_msg_sent_cnt(1, SndCnt).
incr_msg_sent_cnt(I, SndCnt) -> incr_cnt(I, SndCnt).
incr_msg_recv_cnt(RcvCnt) -> incr_cnt(1, RcvCnt).
incr_pub_recv_cnt(PubRecvCnt) -> incr_cnt(1, PubRecvCnt).
incr_pub_dropped_cnt(I, PubDroppedCnt) -> incr_cnt(I, PubDroppedCnt).
incr_pub_sent_cnt(I, PubSendCnt) -> incr_cnt(I, PubSendCnt).

incr_cnt(IncrV, {Avg, Total, []}) ->
    {Avg, Total + IncrV, [IncrV]};
incr_cnt(IncrV, {Avg, Total, [V|Vals]}) ->
    {Avg, Total + IncrV, [V + IncrV|Vals]}.


maybe_trigger_counter_update(#state{last_time_active={MSecs, Secs, _}} = State) ->
    NewTS = os:timestamp(),
    case NewTS of
        {MSecs, Secs, _} ->
            State;
        _ ->
            trigger_counter_update(State#state{last_time_active=NewTS})
    end.

trigger_counter_update(State) ->
    #state{recv_cnt=RecvCnt,
           send_cnt=SendCnt,
           pub_recv_cnt=PubRecvCnt,
           pub_send_cnt=PubSendCnt,
           pub_dropped_cnt=PubDroppedCnt} = State,
    State#state{
        recv_cnt=trigger_counter_update(incr_messages_received, RecvCnt),
        send_cnt=trigger_counter_update(incr_messages_sent, SendCnt),
        pub_recv_cnt=trigger_counter_update(incr_publishes_received,
                                            PubRecvCnt),
        pub_send_cnt=trigger_counter_update(incr_publishes_sent,
                                            PubSendCnt),
        pub_dropped_cnt=trigger_counter_update(incr_publishes_dropped,
                                               PubDroppedCnt)}.

trigger_counter_update(_, {_, _, []} = Counter) ->
    Counter;
trigger_counter_update(IncrFun, {_, Total, Vals}) ->
    L = length(Vals),
    Avg = lists:sum(Vals) div L,
    _ = apply(vmq_exo, IncrFun, [Avg]),
    case L < ?MAX_SAMPLES of
        true ->
            {Avg, Total, [0|Vals]};
        false ->
            % remove oldest sample
            {Avg, Total, [0|lists:reverse(tl(lists:reverse(Vals)))]}
    end.

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
            {{node(), now()}, 0};
        {S, I} ->
            {S, I + 1}
    end,
    put(guid, GUID),
    erlang:md5(term_to_binary(GUID)).
