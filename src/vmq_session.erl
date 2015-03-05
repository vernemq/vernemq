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
         get_info/2,
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


-define(CLOSE_AFTER, 5000).
-define(ALLOWED_MQTT_VERSIONS, [3, 4, 131]).

-type proplist() :: [{atom(), any()}].
-type statename() :: atom().
-type peer() :: {inet:ip_address(), inet:port_number()}.
-type mqtt_variable_ping() :: undefined.
-type mqtt_variable() :: #mqtt_frame_connect{}
                       | #mqtt_frame_connack{}
                       | #mqtt_frame_publish{}
                       | #mqtt_frame_subscribe{}
                       | #mqtt_frame_suback{}
                       | mqtt_variable_ping().
-type mqtt_frame() :: #mqtt_frame{}.
-type mqtt_frame_fixed() ::  #mqtt_frame_fixed{}.
-type timestamp() :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}.
-type counter() :: {timestamp(), non_neg_integer(), non_neg_integer()}.

-ifdef(namespaced_types).
-type ddict()   :: dict:dict().
-else.
-type ddict()   :: dict().
-endif.


-record(state, {
                %% networking requirements
          send_fun                          :: function(),
          %% mqtt layer requirements
          next_msg_id=1                     :: msg_id(),
          subscriber_id                     :: undefined | subscriber_id(),
          will_msg                          :: undefined | msg(),
          waiting_acks=dict:new()           :: ddict(),
          %% auth backend requirement
          peer                              :: peer(),
          username                          :: undefined | username() |
                                               {preauth, string() | undefined},
          keep_alive                        :: undefined | pos_integer(),
          keep_alive_timer                  :: undefined | reference(),
          clean_session=false               :: flag(),
          proto_ver                         :: undefined | pos_integer(),
          queue_pid                         :: pid(),

          %% stats
          recv_cnt=init_counter()           :: counter(),
          send_cnt=init_counter()           :: counter(),
          pub_recv_cnt=init_counter()       :: counter(),
          pub_dropped_cnt=init_counter()    :: counter(),
          pub_send_cnt=init_counter()       :: counter(),

          %% config
          allow_anonymous=false             :: flag(),
          max_inflight_messages=20          :: non_neg_integer(), %% 0 means unlimited
          max_message_size=0                :: non_neg_integer(), %% 0 means unlimited
          mountpoint=""                     :: mountpoint(),
          retry_interval=20000              :: pos_integer(),
          upgrade_qos=false                 :: flag(),
          max_client_id_size=23             :: non_neg_integer(),
          trade_consistency=false           :: flag(),
          reg_view=vmq_reg_trie             :: atom(),
          allow_multiple_sessions=false     :: flag()

         }).

-type state() :: #state{}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% API FUNCTIONS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec start_link(peer(), function(), proplist()) -> {ok, pid()}.
start_link(Peer, SendFun, Opts) ->
    gen_fsm:start_link(?MODULE, [Peer, SendFun, Opts], []).

-spec disconnect(pid()) -> ok.
disconnect(FsmPid) ->
    gen_fsm:send_all_state_event(FsmPid, disconnect).

-spec in(pid(), mqtt_frame()) ->  ok.
in(FsmPid, #mqtt_frame{fixed=#mqtt_frame_fixed{type=?PUBLISH}} = Event) ->
    Level = vmq_sysmon:cpu_load_level(),
    case Level of
        0 -> ok;
        _ -> timer:sleep(Level * 100) %% introduces sleep, min 100ms, max 300ms
    end,
    in_(FsmPid, Event);
in(FsmPid, Event) ->
    in_(FsmPid, Event).

in_(FsmPid, Event) ->
    save_sync_send_all_state_event(FsmPid, {input, Event}, ok, infinity).

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

-spec get_info(subscriber_id() | pid(), [atom()]) -> proplist().
get_info(SubscriberId, InfoItems) when is_tuple(SubscriberId) ->
    case vmq_reg:get_subscriber_pids(SubscriberId) of
        {ok, Pids} -> [get_info(Pid, InfoItems)|| Pid <- Pids];
        _ -> []
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
                | keepalive_expired
                | {retry, msg_id()}
                | {deliver_bin, {msg_id(), qos(), payload()}}
                | {deliver, topic(), payload(), qos(),
                   flag(), flag(), binary()}, state()) ->
    {next_state, connected, state()} | {stop, normal, state()}.
connected(timeout, State) ->
    % ignore
    {next_state, connected, State};

connected({retry, MessageId}, State) ->
    #state{send_fun=SendFun, waiting_acks=WAcks,
           retry_interval=RetryInterval, send_cnt=SendCnt} = State,
    NewState =
    case dict:find(MessageId, WAcks) of
        error ->
            State;
        {ok, {QoS, #mqtt_frame{fixed=Fixed} = Frame, _, MsgStoreRef}} ->
            FFrame =
            case Fixed of
                #mqtt_frame_fixed{type=?PUBREC} ->
                    Frame;
                _ ->
                    Frame#mqtt_frame{
                      fixed=Fixed#mqtt_frame_fixed{
                              dup=true}}
            end,
            SendFun(FFrame),
            Ref = send_after(RetryInterval, {retry, MessageId}),
            State#state{
              send_cnt=incr_msg_sent_cnt(SendCnt),
              waiting_acks=dict:store(MessageId,
                                      {QoS, Frame, Ref, MsgStoreRef},
                                      WAcks)}
    end,
    {next_state, connected, NewState};

connected(keepalive_expired, State) ->
    lager:debug("[~p] stop due to ~p~n", [self(), keepalive_expired]),
    {stop, normal, State}.

-spec init(_) -> {ok, wait_for_connect, state(), ?CLOSE_AFTER}.
init([Peer, SendFun, Opts]) ->
    MountPoint = proplists:get_value(mountpoint, Opts, ""),
    PreAuthUser =
    case lists:keyfind(preauth, 1, Opts) of
        false -> undefined;
        {_, undefined} -> undefined;
        {_, PreAuth} -> {preauth, PreAuth}
    end,
    MaxClientIdSize = vmq_config:get_env(max_client_id_size, 23),
    RetryInterval = vmq_config:get_env(retry_interval, 20),
    QueueSize = vmq_config:get_env(max_queued_messages, 1000),
    UpgradeQoS = vmq_config:get_env(upgrade_outgoing_qos, false),
    AllowAnonymous = vmq_config:get_env(allow_anonymous, false),
    MaxInflightMsgs = vmq_config:get_env(max_inflight_messages, 20),
    MaxMessageSize = vmq_config:get_env(message_size_limit, 0),
    TradeConsistency = vmq_config:get_env(trade_consistency, false),
    RegView = vmq_config:get_env(default_reg_view, vmq_reg_trie),
    AllowMultiple = vmq_config:get_env(allow_multiple_sessions, false),
    {ok, QPid} = vmq_queue:start_link(self(), QueueSize),
    {ok, wait_for_connect, #state{peer=Peer, send_fun=SendFun,
                                  upgrade_qos=UpgradeQoS,
                                  mountpoint=string:strip(MountPoint,
                                                          right, $/),
                                  allow_anonymous=AllowAnonymous,
                                  max_inflight_messages=MaxInflightMsgs,
                                  max_message_size=MaxMessageSize,
                                  username=PreAuthUser,
                                  max_client_id_size=MaxClientIdSize,
                                  retry_interval=1000 * RetryInterval,
                                  queue_pid=QPid,
                                  trade_consistency=TradeConsistency,
                                  reg_view=RegView,
                                  allow_multiple_sessions=AllowMultiple
                                 }, ?CLOSE_AFTER}.

-spec handle_event(disconnect, _, state()) -> {stop, normal, state()}.
handle_event(disconnect, StateName, State) ->
    lager:debug("[~p] stop in state ~p due to ~p~n",
                  [self(), StateName, disconnect]),
    {stop, normal, State}.

-spec handle_sync_event(_, _, _, state()) -> {reply, _, statename(), state()} |
                                             {stop, {error, {unknown_req, _}},
                                              state()}.
handle_sync_event({input, #mqtt_frame{
                             fixed=#mqtt_frame_fixed{type=?DISCONNECT}}},
             _From, _, State) ->
    {stop, normal, State};
handle_sync_event({input, Frame}, _From, StateName, State) ->
    #state{keep_alive_timer=KARef, recv_cnt=RecvCnt} = State,
    #mqtt_frame{fixed=Fixed,
                variable=Variable,
                payload=Payload} = Frame,
    cancel_timer(KARef),
    case handle_frame(StateName, Fixed, Variable, Payload,
                      State#state{recv_cnt=incr_msg_recv_cnt(RecvCnt)}) of
        {connected, #state{keep_alive=KeepAlive} = NewState} ->
            case StateName of
                connected ->
                    %% no state change
                    ignore;
                _ ->
                    process_flag(trap_exit, true)
            end,
            {reply, ok, connected,
             NewState#state{keep_alive_timer=gen_fsm:send_event_after(KeepAlive,
                                                           keepalive_expired)}};
        {NextStateName, NewState} ->
            {reply, ok, NextStateName, NewState, ?CLOSE_AFTER};
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
handle_info({mail, QPid, Msgs, _, Dropped}, connected,
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
    case handle_messages(Msgs, [], NewState) of
        {ok, NewState1} ->
            vmq_queue:notify(QPid),
            {next_state, connected, NewState1};
        Ret ->
            Ret
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
                handle_waiting_acks(State)
        end,
    maybe_publish_last_will(State);
terminate(_Reason, _, _) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% INTERNALS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
handle_messages([{deliver, QoS, Msg}|Rest], Frames, State) ->
    {ok, Frame, NewState} = prepare_frame(QoS, Msg, State),
    handle_messages(Rest, [Frame|Frames], NewState);
handle_messages([{deliver_bin, Term}|Rest], Frames, State) ->
    {ok, NewState} = handle_bin_message(Term, State),
    handle_messages(Rest, Frames, NewState);
handle_messages([], [], State) -> {ok, State};
handle_messages([], Frames, State) ->
    case send_publish_frames(Frames, State) of
        {ok, NewState} ->
            {ok, NewState};
        {error, Reason} ->
            lager:debug("[~p] stop due to ~p~n", [self(), Reason]),
            {stop, normal, State}
    end.

prepare_frame(QoS, Msg, State) ->
    #state{waiting_acks=WAcks, retry_interval=RetryInterval} = State,
    #vmq_msg{routing_key=RoutingKey,
             payload=Payload,
             retain=IsRetained,
             dup=IsDup,
             qos=MsgQoS} = Msg,
    {NewQoS, #vmq_msg{msg_ref=MsgStoreRef}} = maybe_upgrade_qos(QoS, MsgQoS,
                                                                Msg, State),
    {OutgoingMsgId, State1} = get_msg_id(NewQoS, State),
    Frame = #mqtt_frame{
               fixed=#mqtt_frame_fixed{
                        type=?PUBLISH,
                        qos=NewQoS,
                        retain=IsRetained,
                        dup=IsDup
                       },
               variable=#mqtt_frame_publish{
                           topic_name=RoutingKey,
                           message_id=OutgoingMsgId},
               payload=Payload
              },
    case NewQoS of
        0 ->
            {ok, Frame, State1};
        _ ->
            Ref = send_after(RetryInterval, {retry, OutgoingMsgId}),
            {ok, Frame, State1#state{
                          waiting_acks=dict:store(OutgoingMsgId,
                                                  {NewQoS, Frame,
                                                   Ref,
                                                   MsgStoreRef},
                                                  WAcks)}}
    end.

%% The MQTT specification requires that the QoS of a message delivered to a
%% subscriber is never upgraded to match the QoS of the subscription. If
%% upgrade_outgoing_qos is set true, messages sent to a subscriber will always
%% match the QoS of its subscription. This is a non-standard option not provided
%% for by the spec.
maybe_upgrade_qos(0, _, Msg, _) ->
    {0, Msg};
maybe_upgrade_qos(SubQoS, PubQoS, Msg,
                  #state{upgrade_qos=true, subscriber_id=SubscriberId})
  when SubQoS > PubQoS ->
    {SubQoS, vmq_msg_store:store(SubscriberId, Msg)};
maybe_upgrade_qos(_, PubQoS, Msg, _) ->
    {PubQoS, Msg}.

handle_bin_message({MsgId, QoS, Bin}, State) ->
    #state{send_fun=SendFun, waiting_acks=WAcks,
           retry_interval=RetryInterval, send_cnt=SendCnt} = State,
    SendFun(Bin),
    Ref = send_after(RetryInterval, {retry, MsgId}),
    {ok, State#state{send_cnt=incr_msg_sent_cnt(SendCnt),
                     waiting_acks=dict:store(MsgId,
                                             {QoS, Bin, Ref, undefined},
                                             WAcks)}}.

-spec handle_frame(statename(), mqtt_frame_fixed(),
                   mqtt_variable(), payload(), state()) ->
                   {statename(), state()} | {stop, atom(), state()}.

handle_frame(wait_for_connect, _,
             #mqtt_frame_connect{keep_alive=KeepAlive} = Var, _, State) ->
    _ = vmq_exo:incr_connect_received(),
    %% the client is allowed "grace" of a half a time period
    KKeepAlive = (KeepAlive + (KeepAlive div 2)) * 1000,
    check_connect(Var, State#state{keep_alive=KKeepAlive});
handle_frame(wait_for_connect, _, _, _,
             #state{retry_interval=RetryInterval} = State) ->
    %% drop frame
    {wait_for_connect, State#state{keep_alive=RetryInterval}};

handle_frame(connected, #mqtt_frame_fixed{type=?PUBACK}, Var, _, State) ->
    #state{waiting_acks=WAcks} = State,
    #mqtt_frame_publish{message_id=MessageId} = Var,
    %% qos1 flow
    case dict:find(MessageId, WAcks) of
        {ok, {_, _, Ref, MsgStoreRef}} ->
            cancel_timer(Ref),
            _ = vmq_msg_store:deref(MsgStoreRef),
            {connected, State#state{waiting_acks=dict:erase(MessageId, WAcks)}};
        error ->
            {connected, State}
    end;

handle_frame(connected, #mqtt_frame_fixed{type=?PUBREC}, Var, _, State) ->
    #state{waiting_acks=WAcks, send_fun=SendFun,
           retry_interval=RetryInterval, send_cnt=SendCnt} = State,
    #mqtt_frame_publish{message_id=MessageId} = Var,
    %% qos2 flow
    {_, _, Ref, MsgStoreRef} = dict:fetch(MessageId, WAcks),
    cancel_timer(Ref), % cancel republish timer
    PubRelFrame =#mqtt_frame{
                             fixed=#mqtt_frame_fixed{type=?PUBREL, qos=1},
                             variable=#mqtt_frame_publish{message_id=MessageId},
                             payload= <<>>},
    SendFun(PubRelFrame),
    NewRef = send_after(RetryInterval, {retry, MessageId}),
    _ = vmq_msg_store:deref(MsgStoreRef),
    {connected, State#state{
                  send_cnt=incr_msg_sent_cnt(SendCnt),
                  waiting_acks=dict:store(MessageId,
                                          {2, PubRelFrame,
                                           NewRef,
                                           undefined}, WAcks)}};

handle_frame(connected, #mqtt_frame_fixed{type=?PUBREL, dup=IsDup},
             Var, _, State) ->
    #state{waiting_acks=WAcks, username=User, reg_view=RegView, mountpoint=MP,
           subscriber_id=SubscriberId, trade_consistency=Consistency} = State,
    #mqtt_frame_publish{message_id=MessageId} = Var,
    %% qos2 flow
    NewState =
    case dict:find({qos2, MessageId} , WAcks) of
        {ok, {_, _, TRef, {MsgRef, IsRetain}}} ->
            cancel_timer(TRef),
            {ok, {RoutingKey, Payload}} = vmq_msg_store:retrieve(MsgRef),
            Msg = #vmq_msg{msg_ref=MsgRef,
                           routing_key=RoutingKey,
                           payload=Payload,
                           retain=IsRetain,
                           qos=2,
                           trade_consistency=Consistency,
                           reg_view=RegView,
                           mountpoint=MP},
            case publish(User, SubscriberId, Msg) of
                {ok, _} ->
                    _ = vmq_msg_store:deref(MsgRef),
                    State#state{
                      waiting_acks=dict:erase({qos2, MessageId}, WAcks)};
                {error, _Reason} ->
                    %% cant publish due to overload or netsplit,
                    %% client will retry
                    State
            end;
        error when IsDup ->
            %% already delivered, Client expects a PUBCOMP
            State
    end,
    {connected, send_frame(?PUBCOMP, #mqtt_frame_publish{
                                       message_id=MessageId
                                      }, <<>>, NewState)};

handle_frame(connected, #mqtt_frame_fixed{type=?PUBCOMP}, Var, _, State) ->
    #state{waiting_acks=WAcks} = State,
    #mqtt_frame_publish{message_id=MessageId} = Var,
    %% qos2 flow
    {_, _, Ref, undefined} = dict:fetch(MessageId, WAcks),
    cancel_timer(Ref), % cancel rpubrel timer
    {connected, State#state{waiting_acks=dict:erase(MessageId, WAcks)}};

handle_frame(connected, #mqtt_frame_fixed{type=?PUBLISH,
                                          qos=QoS,
                                          retain=IsRetain},
             Var, Payload, State) ->
    #state{mountpoint=MountPoint, pub_recv_cnt=PubRecvCnt,
           max_message_size=MaxMessageSize, reg_view=RegView,
           trade_consistency=Consistency} = State,
    #mqtt_frame_publish{topic_name=Topic, message_id=MessageId} = Var,
    %% we disallow Publishes on Topics prefixed with '$'
    %% this allows us to use such prefixes for e.g. '$SYS' Tree
    case {hd(Topic), valid_msg_size(Payload, MaxMessageSize)} of
        {$$, _} ->
            {connected, State};
        {_, true} ->
            Msg = #vmq_msg{routing_key=Topic,
                           payload=Payload,
                           retain=IsRetain,
                           qos=QoS,
                           trade_consistency=Consistency,
                           reg_view=RegView,
                           mountpoint=MountPoint},
            {connected, dispatch_publish(QoS, MessageId, Msg,
                                         State#state{
                                           pub_recv_cnt=incr_pub_recv_cnt(
                                                          PubRecvCnt)
                                          })};
        {_, false} ->
            {connected, State}
    end;

handle_frame(connected, #mqtt_frame_fixed{type=?SUBSCRIBE}, Var, _, State) ->
    #state{subscriber_id=SubscriberId, username=User,
           queue_pid=QPid, trade_consistency=Consistency} = State,
    #mqtt_frame_subscribe{topic_table=Topics, message_id=MessageId} = Var,
    TTopics = [{Name, QoS} || #mqtt_topic{name=Name, qos=QoS} <- Topics],
    case vmq_reg:subscribe(Consistency, User, SubscriberId, QPid, TTopics) of
        ok ->
            {_, QoSs} = lists:unzip(TTopics),
            NewState = send_frame(?SUBACK, #mqtt_frame_suback{
                                              message_id=MessageId,
                                              qos_table=QoSs}, <<>>, State),
            {connected, NewState};
        {error, _Reason} ->
            %% cant subscribe due to overload or netsplit,
            %% Subscribe uses QoS 1 so the client will retry
            {connected, State}
    end;

handle_frame(connected, #mqtt_frame_fixed{type=?UNSUBSCRIBE}, Var, _, State) ->
    #state{subscriber_id=SubscriberId, username=User,
           trade_consistency=Consistency} = State,
    #mqtt_frame_subscribe{topic_table=Topics, message_id=MessageId} = Var,
    TTopics = [Name || #mqtt_topic{name=Name} <- Topics],
    case vmq_reg:unsubscribe(Consistency, User, SubscriberId, TTopics) of
        ok ->
            NewState = send_frame(?UNSUBACK, #mqtt_frame_suback{
                                                message_id=MessageId
                                               }, <<>>, State),
            {connected, NewState};
        {error, _Reason} ->
            %% cant unsubscribe due to overload or netsplit,
            %% Unsubscribe uses QoS 1 so the client will retry
            {connected, State}
    end;

handle_frame(connected, #mqtt_frame_fixed{type=?PINGREQ}, _, _, State) ->
    NewState = send_frame(?PINGRESP, undefined, <<>>, State),
    {connected, NewState}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% INTERNAL
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
check_connect(#mqtt_frame_connect{proto_ver=Ver} = F, State) ->
    check_client_id(F, State#state{proto_ver=Ver}).

check_client_id(#mqtt_frame_connect{},
                #state{username={preauth, undefined}, peer=Peer} = State) ->
    %% No common name found in client certificate
    lager:warning("can't authenticate ssl client ~p due to
                  no_common_name_found", [Peer]),
    {wait_for_connect,
     send_connack(?CONNACK_CREDENTIALS, State)};
check_client_id(#mqtt_frame_connect{clean_sess=CleanSession} = F,
                #state{username={preauth, ClientId}, queue_pid=QPid, peer=Peer,
                       allow_multiple_sessions=AllowMultiple, mountpoint=MP} = State) ->
    SubscriberId = {MP, ClientId},
    %% User preauthenticated using e.g. SSL client certificate
    case vmq_reg:register_subscriber(AllowMultiple, SubscriberId,
                                     QPid, CleanSession) of
        ok ->
            vmq_plugin:all(on_register, [Peer, SubscriberId, undefined]),
            check_will(F, State#state{subscriber_id=SubscriberId,
                                      clean_session=CleanSession});
        {error, Reason} ->
            lager:warning("can't register client ~p due to ~p",
                        [ClientId, Reason]),
            {wait_for_connect,
             send_connack(?CONNACK_SERVER, State)}
    end;
check_client_id(#mqtt_frame_connect{client_id=missing}, State) ->
    {stop, normal, State};
check_client_id(#mqtt_frame_connect{client_id=empty, proto_ver=4} = F, State) ->
    RandomClientId = random_client_id(),
    SubscriberId = {State#state.mountpoint, RandomClientId},
    check_user(F#mqtt_frame_connect{client_id=RandomClientId},
               State#state{subscriber_id=SubscriberId});
check_client_id(#mqtt_frame_connect{client_id=empty, proto_ver=3}, State) ->
    lager:warning("empty protocol version not allowed in mqttv3 ~p",
                [State#state.subscriber_id]),
    {wait_for_connect,
     send_connack(?CONNACK_INVALID_ID, State)};
check_client_id(#mqtt_frame_connect{client_id=ClientId, proto_ver=V} = F,
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
check_client_id(#mqtt_frame_connect{client_id=Id}, State) ->
    lager:warning("invalid client id ~p", [Id]),
    {wait_for_connect,
     send_connack(?CONNACK_INVALID_ID, State)}.

auth_on_register(Peer, {DefaultMP, ClientId} = SubscriberId, User,
                 Password, DefaultRegView) ->
    HookArgs = [Peer, SubscriberId, User, Password],
    case vmq_plugin:all_till_ok(auth_on_register, HookArgs) of
        ok ->
            {ok, SubscriberId, DefaultRegView};
        {ok, Args} ->
            ChangedMP = proplists:get_value(mountpoint, Args, DefaultMP),
            ChangedRegView = proplists:get_value(regview, Args, DefaultRegView),
            {ok, {ChangedMP, ClientId}, ChangedRegView};
        {error, Reason} ->
            {error, Reason}
    end.

check_user(#mqtt_frame_connect{username=""} = F, State) ->
    check_user(F#mqtt_frame_connect{username=undefined,
                                    password=undefined}, State);
check_user(#mqtt_frame_connect{username=User, password=Password,
                               clean_sess=CleanSess} = F, State) ->
    #state{peer=Peer, queue_pid=QPid, allow_multiple_sessions=AllowMultiple,
           allow_anonymous=AllowAnonymous, reg_view=DefaultRegView,
           subscriber_id=SubscriberId} = State,
    case AllowAnonymous of
        false ->
            case auth_on_register(Peer, SubscriberId, User,
                                  Password, DefaultRegView) of
                {ok, NewSubscriberId, NewRegView} ->
                    case vmq_reg:register_subscriber(AllowMultiple,
                                                     NewSubscriberId,
                                                     QPid, CleanSess) of
                        ok ->
                            vmq_plugin:all(on_register, [Peer, NewSubscriberId,
                                                         User]),
                            check_will(F, State#state{
                                            subscriber_id=NewSubscriberId,
                                            username=User,
                                            reg_view=NewRegView});
                        {error, Reason} ->
                            lager:warning("can't register client ~p due to ~p",
                                          [SubscriberId, Reason]),
                            {wait_for_connect,
                             send_connack(?CONNACK_SERVER, State)}
                    end;
                {error, no_matching_hook_found} ->
                    lager:error("can't authenticate client ~p due to
                                no_matching_hook_found", [SubscriberId]),
                    {wait_for_connect,
                     send_connack(?CONNACK_AUTH, State)};
                {error, Errors} ->
                    case lists:keyfind(invalid_credentials, 2, Errors) of
                        {error, invalid_credentials} ->
                            lager:warning(
                              "can't authenticate client ~p due to
                              invalid_credentials", [SubscriberId]),
                            {wait_for_connect,
                             send_connack(?CONNACK_CREDENTIALS, State)};
                        false ->
                            %% can't authenticate due to other reasons
                            lager:warning(
                              "can't authenticate client ~p due to ~p",
                              [SubscriberId, Errors]),
                            {wait_for_connect,
                             send_connack(?CONNACK_AUTH, State)}
                    end
            end;
        true ->
            case vmq_reg:register_subscriber(AllowMultiple, SubscriberId,
                                             QPid, CleanSess) of
                ok ->
                    _ = vmq_plugin:all(on_register, [Peer, SubscriberId, User]),
                    check_will(F, State#state{username=User});
                {error, Reason} ->
                    lager:warning("can't register client ~p due to reason ~p",
                                [SubscriberId, Reason]),
                    {wait_for_connect,
                     send_connack(?CONNACK_SERVER, State)}
            end
    end.

check_will(#mqtt_frame_connect{will_topic=undefined}, State) ->
    {connected, send_connack(?CONNACK_ACCEPT, State)};
check_will(#mqtt_frame_connect{will_topic=""}, State) ->
    %% null topic.... Mosquitto sends a CONNACK_INVALID_ID...
    lager:warning("invalid last will topic for client ~p",
                [State#state.subscriber_id]),
    {wait_for_connect,
     send_connack(?CONNACK_INVALID_ID, State)};
check_will(#mqtt_frame_connect{will_topic=Topic, will_msg=Payload, will_qos=Qos},
           State) ->
    #state{mountpoint=MountPoint, username=User, subscriber_id=SubscriberId,
           max_message_size=MaxMessageSize, trade_consistency=Consistency,
           reg_view=RegView} = State,
    case auth_on_publish(User, SubscriberId, #vmq_msg{routing_key=Topic,
                                                      payload=Payload,
                                                      qos=Qos,
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
    send_frame(?CONNACK, #mqtt_frame_connack{return_code=ReturnCode},
               <<>>, State).

-spec send_frame(non_neg_integer(), mqtt_variable(),
                 payload(), state()) -> state().
send_frame(Type, Variable, Payload, State) ->
    send_frame(Type, false, Variable, Payload, State).

send_frame(Type, DUP, Variable, Payload,
           #state{send_fun=SendFun, send_cnt=SendCnt} = State) ->
    SendFun(#mqtt_frame{
               fixed=#mqtt_frame_fixed{type=Type, dup=DUP},
               variable=Variable,
               payload=Payload
              }),
    State#state{send_cnt=incr_msg_sent_cnt(SendCnt)}.


-spec maybe_publish_last_will(state()) -> ok.
maybe_publish_last_will(#state{will_msg=undefined}) -> ok;
maybe_publish_last_will(#state{will_msg=Msg} = State) ->
    {MsgId, NewState} = get_msg_id(Msg#vmq_msg.qos, State),
    _ = dispatch_publish(Msg#vmq_msg.qos, MsgId, Msg, NewState),
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
            #vmq_msg{msg_ref=MsgRef} = UpdatedMsg =
                vmq_msg_store:store(SubscriberId, Msg),
            case publish(User, SubscriberId, UpdatedMsg) of
                {ok, _} ->
                    NewState = send_frame(?PUBACK,
                                          #mqtt_frame_publish{
                                             message_id=MessageId
                                            }, <<>>, State),
                    _ = vmq_msg_store:deref(MsgRef),
                    NewState;
                {error, _Reason} ->
                    %% can't publish due to overload or netsplit
                    _ = vmq_msg_store:deref(MsgRef),
                    drop(State)
            end;
        false ->
            drop(State)
    end.

-spec dispatch_publish_qos2(msg_id(), msg(), state()) -> state().
dispatch_publish_qos2(MessageId, Msg, State) ->
    case check_in_flight(State) of
        true ->
            #state{subscriber_id=SubscriberId,
                   waiting_acks=WAcks, retry_interval=RetryInterval,
                   send_fun=SendFun, send_cnt=SendCnt} = State,
            #vmq_msg{msg_ref=MsgRef, retain=IsRetain} =
                vmq_msg_store:store(SubscriberId, Msg),
            Ref = send_after(RetryInterval, {retry, {qos2, MessageId}}),
            Frame = #mqtt_frame{
                       fixed=#mqtt_frame_fixed{type=?PUBREC},
                       variable=#mqtt_frame_publish{message_id=MessageId},
                       payload= <<>>
                      },
            SendFun(Frame),
            State#state{
              send_cnt=incr_msg_sent_cnt(SendCnt),
              waiting_acks=dict:store(
                             {qos2, MessageId},
                             {2, Frame, Ref, {MsgRef, IsRetain}}, WAcks)};
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

-spec send_publish_frames([mqtt_frame()], state()) -> state() | {error, atom()}.
send_publish_frames(Frames, State) ->
    #state{send_fun=SendFun, send_cnt=SendCnt, pub_send_cnt=PubSendCnt} = State,
    case SendFun(Frames) of
        ok ->
            NrOfFrames = length(Frames),
            NewState = State#state{
                         send_cnt=incr_msg_sent_cnt(NrOfFrames, SendCnt),
                         pub_send_cnt=incr_pub_sent_cnt(NrOfFrames, PubSendCnt)
                        },
            {ok, NewState};
        {error, Reason} ->
            {error, Reason}
    end.

-spec auth_on_publish(username(), subscriber_id(), msg(),
                      fun((msg(), list()) -> {ok, msg()} | {error, atom()})
                        ) -> {ok, msg()} | {error, atom()}.
auth_on_publish(User, SubscriberId, #vmq_msg{routing_key=Topic,
                                             payload=Payload,
                                             reg_view=RegView,
                                             qos=QoS,
                                             retain=IsRetain,
                                             dup=_IsDup,
                                             mountpoint=MP} = Msg,
               AuthSuccess) ->
    HookArgs = [User, SubscriberId, QoS, Topic, Payload, IsRetain],
    case vmq_plugin:all_till_ok(auth_on_publish, HookArgs) of
        ok ->
            AuthSuccess(Msg, HookArgs);
        {ok, ChangedPayload} when is_binary(Payload) ->
            AuthSuccess(Msg#vmq_msg{payload=ChangedPayload}, HookArgs);
        {ok, Args} when is_list(Args) ->
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

-spec handle_waiting_acks(state()) -> ddict().
handle_waiting_acks(State) ->
    #state{subscriber_id=SubscriberId, waiting_acks=WAcks} = State,
    dict:fold(fun ({qos2, _}, _, Acc) ->
                      Acc;
                  (MsgId, {QoS, #mqtt_frame{fixed=Fixed} = Frame,
                           TRef, undefined}, Acc) ->
                      %% unacked PUBREL Frame
                      cancel_timer(TRef),
                      Bin = emqtt_frame:serialise(
                              Frame#mqtt_frame{
                                fixed=Fixed#mqtt_frame_fixed{
                                        dup=true}}),
                      vmq_msg_store:defer_deliver_uncached(SubscriberId,
                                                           {MsgId, QoS, Bin}),
                      Acc;
                  (MsgId, {QoS, Bin, TRef, undefined}, Acc) ->
                      cancel_timer(TRef),
                      vmq_msg_store:defer_deliver_uncached(SubscriberId,
                                                           {MsgId, QoS, Bin}),
                      Acc;
                  (_MsgId, {QoS, _Frame, TRef, MsgStoreRef}, Acc) ->
                      cancel_timer(TRef),
                      vmq_msg_store:defer_deliver(SubscriberId, QoS,
                                                  MsgStoreRef, true),
                      Acc
              end, [], WAcks).

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
            dict:size(WAcks) < V
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
    Size = dict:size(State#state.waiting_acks),
    get_info_items(Rest, StateName, State,
                   [{waiting_acks, Size}|Acc]);
get_info_items([_|Rest], StateName, State, Acc) ->
    get_info_items(Rest, StateName, State, Acc);
get_info_items([], _, _, Acc) -> Acc.

init_counter() ->
    {os:timestamp(), 0, 0}.

incr_msg_sent_cnt(SndCnt) ->
    incr_msg_sent_cnt(1, SndCnt).
incr_msg_sent_cnt(I, SndCnt) ->
    incr_cnt(incr_messages_sent, I, SndCnt).

incr_msg_recv_cnt(RcvCnt) ->
    incr_cnt(incr_messages_received, 1, RcvCnt).
incr_pub_recv_cnt(PubRecvCnt) ->
    incr_cnt(incr_publishes_received, 1, PubRecvCnt).

incr_pub_dropped_cnt(I, PubDroppedCnt) ->
    incr_cnt(incr_publishes_dropped, I, PubDroppedCnt).
incr_pub_sent_cnt(I, PubSendCnt) ->
    incr_cnt(incr_publishes_sent, I, PubSendCnt).

incr_cnt(IncrFun, IncrV, {{M, S, _}, V, I}) ->
    NewV = V + IncrV,
    NewI = I + IncrV,
    case os:timestamp() of
        {M, S, _} = TS ->
            {TS, NewV, NewI};
        TS ->
            _ = apply(vmq_exo, IncrFun, [NewI]),
            {TS, NewV, 0}
    end.
