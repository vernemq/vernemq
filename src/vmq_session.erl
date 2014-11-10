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
         list_sessions/1,
         list_sessions_/1]).

-export([init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-export([wait_for_connect/2,
         connected/2]).


-define(CLOSE_AFTER, 5000).

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
-type ts() :: {os:timestamp(), non_neg_integer(), non_neg_integer()}.


-record(state, {
                %% networking requirements
          send_fun                          :: function(),
          %% mqtt layer requirements
          next_msg_id=1                     :: msg_id(),
          client_id                         :: undefined | client_id(),
          max_client_id_size=23             :: non_neg_integer(),
          will_topic                        :: undefined | topic(),
          will_msg                          :: undefined | payload(),
          will_qos                          :: undefined | qos(),
          waiting_acks=dict:new()           :: dict(),
          %% auth backend requirement
          peer                              :: peer(),
          username                          :: undefined | username() |
                                               {preauth, string() | undefined},
          msg_log_handler                   :: fun((client_id(), topic(),
                                                    payload()) -> any()),
          mountpoint=""                     :: string(),
          retry_interval=20000              :: pos_integer(),
          keep_alive                        :: undefined | pos_integer(),
          keep_alive_timer                  :: undefined | reference(),
          clean_session=false               :: flag(),
          proto_ver                         :: undefined | pos_integer(),
          recv_cnt=init_ts()                :: ts(),
          send_cnt=init_ts()                :: ts(),
          pub_recv_cnt=init_ts()            :: ts(),
          pub_dropped_cnt=init_ts()         :: ts(),
          pub_send_cnt=init_ts()            :: ts(),
          queue_pid                         :: pid()
         }).

-type state() :: #state{}.

-hook({auth_on_publish, only, 6}).
-hook({on_publish, all, 6}).
-hook({auth_on_register, only, 4}).
-hook({on_register, all, 4}).

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
in(FsmPid, Event) ->
    case catch gen_fsm:sync_send_all_state_event(FsmPid, {input, Event},
                                                 infinity) of
        ok -> ok;
        {'EXIT', {normal, _}} -> ok;
        {'EXIT', Reason} -> exit(Reason)
    end.

-spec get_info(string() | pid(), [atom()]) -> proplist().
get_info(ClientId, InfoItems) when is_list(ClientId) ->
    case vmq_reg:get_client_pid(ClientId) of
        {ok, Pid} -> get_info(Pid, InfoItems);
        E -> E
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
    gen_fsm:sync_send_all_state_event(FsmPid, {get_info, AInfoItems}).

-spec list_sessions([atom()|string()]) -> proplist().
list_sessions(InfoItems) ->
    Nodes = vmq_cluster:nodes(),
    lists:foldl(fun(Node, Acc) when Node == node() ->
                        [{Node, list_sessions_(InfoItems)}|Acc];
                   (Node, Acc) ->
                        Res = rpc:call(Node, ?MODULE,
                                       list_sessions_, [InfoItems]),
                        [{Node, Res}|Acc]
                end, [], Nodes).

list_sessions_(InfoItems) ->
    [get_info(Child, InfoItems)
     || {_, Child, _, _} <- supervisor:which_children(vmq_session_sup),
     Child /= restarting].

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
    {_, MountPoint} = lists:keyfind(mountpoint, 1, Opts),
    {_, MaxClientIdSize} = lists:keyfind(max_client_id_size, 1, Opts),
    {_, RetryInterval} = lists:keyfind(retry_interval, 1, Opts),
    PreAuthUser =
    case lists:keyfind(preauth, 1, Opts) of
        false -> undefined;
        {_, PreAuth} -> {preauth, PreAuth}
    end,
    MsgLogHandler =
    case lists:keyfind(msg_log_handler, 1, Opts) of
        {_, undefined} ->
            fun(_, _, _) -> ok end;
        {_, Mod} when is_atom(Mod) ->
            fun(ClientId, Topic, Msg) ->
                    Args = [self(), ClientId, Topic, Msg],
                    apply(Mod, handle, Args)
            end
    end,
    QueueSize = vmq_config:get_env(queue_size, 1000),
    {ok, QPid} = vmq_queue:start_link(self(), QueueSize, queue),
    {ok, wait_for_connect, #state{peer=Peer, send_fun=SendFun,
                                  msg_log_handler=MsgLogHandler,
                                  mountpoint=string:strip(MountPoint,
                                                          right, $/),
                                  username=PreAuthUser,
                                  max_client_id_size=MaxClientIdSize,
                                  retry_interval=1000 * RetryInterval,
                                  queue_pid=QPid
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
    vmq_queue:active(QPid, fun(Msg,St) -> {{ok, Msg}, St} end, []),
    {next_state, StateName, State};
handle_info({mail, QPid, Msgs, _, Dropped}, connected,
            #state{client_id=ClientId, queue_pid=QPid} = State) ->
    case Dropped > 0 of
        true ->
            lager:warning("client ~p dropped ~p messages~n",
                          [ClientId, Dropped]);
        false ->
            ok
    end,
    case handle_messages(Msgs, State) of
        {ok, NewState} ->
            vmq_queue:notify(QPid),
            {next_state, connected, NewState};
        Ret ->
            Ret
    end;
handle_info(Info, _StateName, State) ->
    {stop, {error, {unknown_info, Info}}, State} .

-spec terminate(any(), statename(), state()) -> ok.
terminate(_Reason, connected, State) ->
    #state{clean_session=CleanSession} = State,
    case CleanSession of
        true ->
            ok;
        false ->
            handle_waiting_acks(State)
    end,
    maybe_publish_last_will(State),
    ok;
terminate(_Reason, _, _) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% INTERNALS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
handle_messages([{deliver, QoS, Msg}|Rest], State) ->
    case handle_message(QoS, Msg, State) of
        {ok, NewState} ->
            handle_messages(Rest, NewState);
        {stop, Reason, NewState} ->
            {stop, Reason, NewState}
    end;
handle_messages([{deliver_bin, Term}|Rest], State) ->
    {ok, NewState} = handle_bin_message(Term, State),
    handle_messages(Rest, NewState);
handle_messages([], State) -> {ok, State}.

handle_message(QoS, Msg, State) ->
    #state{client_id=ClientId, waiting_acks=WAcks, mountpoint=MountPoint,
           retry_interval=RetryInterval} = State,
    #vmq_msg{routing_key=RoutingKey,
             payload=Payload,
             retain=IsRetained,
             dup=IsDup,
             msg_ref=MsgStoreRef} = Msg,
    {OutgoingMsgId, State1} = get_msg_id(QoS, State),
    Frame = #mqtt_frame{
               fixed=#mqtt_frame_fixed{
                        type=?PUBLISH,
                        qos=QoS,
                        retain=IsRetained,
                        dup=IsDup
                       },
               variable=#mqtt_frame_publish{
                           topic_name=clean_mp(MountPoint, RoutingKey),
                           message_id=OutgoingMsgId},
               payload=Payload
              },
    case send_publish_frame(Frame, State1) of
        {error, Reason} when QoS > 0 ->
            vmq_msg_store:defer_deliver(ClientId, QoS, MsgStoreRef),
            lager:debug("[~p] stop due to ~p, deliver when client reconnects~n",
                        [self(), Reason]),
            {stop, normal, State1};
        {error, Reason} ->
            lager:debug("[~p] stop due to ~p~n", [self(), Reason]),
            {stop, normal, State1};
        NewState when QoS == 0 ->
            {ok, NewState};
        NewState when QoS > 0 ->
            Ref = send_after(RetryInterval, {retry, OutgoingMsgId}),
            {ok, NewState#state{waiting_acks=dict:store(OutgoingMsgId,
                                                        {QoS, Frame,
                                                         Ref,
                                                         MsgStoreRef},
                                                        WAcks)}}
    end.

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
    vmq_systree:incr_connect_received(),
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
            vmq_msg_store:deref(MsgStoreRef),
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
    vmq_msg_store:deref(MsgStoreRef),
    {connected, State#state{
                  send_cnt=incr_msg_sent_cnt(SendCnt),
                  waiting_acks=dict:store(MessageId,
                                          {2, PubRelFrame,
                                           NewRef,
                                           undefined}, WAcks)}};

handle_frame(connected, #mqtt_frame_fixed{type=?PUBREL, dup=IsDup},
             Var, _, State) ->
    #state{waiting_acks=WAcks, username=User, client_id=ClientId} = State,
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
                           retain=IsRetain},
            case publish(User, ClientId, 2, Msg) of
                ok ->
                    vmq_msg_store:deref(MsgRef),
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
    #state{mountpoint=MountPoint, pub_recv_cnt=PubRecvCnt} = State,
    #mqtt_frame_publish{topic_name=Topic, message_id=MessageId} = Var,
    %% we disallow Publishes on Topics prefixed with '$'
    %% this allows us to use such prefixes for e.g. '$SYS' Tree
    case {hd(Topic), valid_msg_size(Payload)} of
        {$$, _} ->
            {connected, State};
        {_, true} ->
            Msg = #vmq_msg{routing_key=combine_mp(MountPoint, Topic),
                           payload=Payload,
                           retain=IsRetain},
            {connected, dispatch_publish(QoS, MessageId, Msg,
                                         State#state{
                                           pub_recv_cnt=incr_pub_recv_cnt(
                                                          PubRecvCnt)
                                          })};
        {_, false} ->
            {connected, State}
    end;

handle_frame(connected, #mqtt_frame_fixed{type=?SUBSCRIBE}, Var, _, State) ->
    #state{client_id=Id, username=User, mountpoint=MountPoint,
           queue_pid=QPid} = State,
    #mqtt_frame_subscribe{topic_table=Topics, message_id=MessageId} = Var,
    TTopics = [{combine_mp(MountPoint, Name), QoS} ||
                  #mqtt_topic{name=Name, qos=QoS} <- Topics],
    case vmq_reg:subscribe(User, Id, QPid, TTopics) of
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
    #state{client_id=Id, username=User, mountpoint=MountPoint} = State,
    #mqtt_frame_subscribe{topic_table=Topics, message_id=MessageId} = Var,
    TTopics = [combine_mp(MountPoint, Name) ||
                  #mqtt_topic{name=Name} <- Topics],
    case vmq_reg:unsubscribe(User, Id, TTopics) of
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
                #state{username={preauth, ClientId}, peer=Peer,
                       queue_pid=QPid} = State) ->
    %% User preauthenticated using e.g. SSL client certificate
    case vmq_reg:register_client(ClientId, QPid, CleanSession) of
        ok ->
            vmq_hook:all(on_register, [Peer, ClientId, undefined, undefined]),
            check_will(F, State#state{clean_session=CleanSession});
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
    check_user(F#mqtt_frame_connect{client_id=RandomClientId},
               State#state{client_id=RandomClientId});
check_client_id(#mqtt_frame_connect{client_id=empty, proto_ver=3}, State) ->
    lager:warning("empty protocol version not allowed in mqttv3 ~p",
                [State#state.client_id]),
    {wait_for_connect,
     send_connack(?CONNACK_INVALID_ID, State)};
check_client_id(#mqtt_frame_connect{client_id=Id, proto_ver=V} = F,
                #state{max_client_id_size=S} = State) when length(Id) =< S ->
    case lists:member(V, [3, 4, 131]) of
        true ->
            check_user(F, State#state{client_id=Id});
        false ->
            lager:warning("invalid protocol version for ~p ~p", [Id, V]),
            {wait_for_connect,
             send_connack(?CONNACK_PROTO_VER, State)}
    end;
check_client_id(#mqtt_frame_connect{client_id=Id}, State) ->
    lager:warning("invalid client id ~p", [Id]),
    {wait_for_connect,
     send_connack(?CONNACK_INVALID_ID, State)}.

check_user(#mqtt_frame_connect{username=""} = F, State) ->
    check_user(F#mqtt_frame_connect{username=undefined,
                                    password=undefined}, State);
check_user(#mqtt_frame_connect{username=User, password=Password,
                               client_id=ClientId,
                               clean_sess=CleanSession} = F, State) ->
    #state{peer=Peer, queue_pid=QPid} = State,
    case vmq_config:get_env(allow_anonymous, false) of
        false ->
            case vmq_hook:only(auth_on_register,
                               [Peer, ClientId, User, Password]) of
                ok ->
                    case vmq_reg:register_client(ClientId, QPid, CleanSession) of
                        ok ->
                            vmq_hook:all(on_register, [Peer, ClientId,
                                                       User, Password]),
                            check_will(F, State#state{username=User});
                        {error, Reason} ->
                            lager:warning("can't register client ~p due to ~p",
                                        [ClientId, Reason]),
                            {wait_for_connect,
                             send_connack(?CONNACK_SERVER, State)}
                    end;
                not_found ->
                                                % returned when no hook
                                                % on_register hook was
                                                % able to authenticate user
                    lager:warning(
                      "can't authenticate client ~p due to not_found",
                      [ClientId]),
                    {wait_for_connect,
                     send_connack(?CONNACK_AUTH, State)};
                {error, invalid_credentials} ->
                    lager:warning(
                      "can't authenticate client ~p due to invalid_credentials",
                      [ClientId]),
                    {wait_for_connect,
                     send_connack(?CONNACK_CREDENTIALS, State)};
                {error, not_authorized} ->
                    lager:warning(
                      "can't authenticate client ~p due to not_authorized",
                      [ClientId]),
                    {wait_for_connect,
                     send_connack(?CONNACK_AUTH, State)}
            end;
        true ->
            case vmq_reg:register_client(ClientId, QPid, CleanSession) of
                ok ->
                    vmq_hook:all(on_register, [Peer, ClientId, User, Password]),
                    check_will(F, State#state{username=User});
                {error, Reason} ->
                    lager:warning("can't register client ~p due to reason ~p",
                                [ClientId, Reason]),
                    {wait_for_connect,
                     send_connack(?CONNACK_SERVER, State)}
            end
    end.

check_will(#mqtt_frame_connect{will_topic=undefined}, State) ->
    {connected, send_connack(?CONNACK_ACCEPT, State)};
check_will(#mqtt_frame_connect{will_topic=""}, State) ->
    %% null topic.... Mosquitto sends a CONNACK_INVALID_ID...
    lager:warning("invalid last will topic for client ~p",
                [State#state.client_id]),
    {wait_for_connect,
     send_connack(?CONNACK_INVALID_ID, State)};
check_will(#mqtt_frame_connect{will_topic=Topic, will_msg=Msg, will_qos=Qos},
           State) ->
    #state{mountpoint=MountPoint, username=User, client_id=ClientId} = State,
    LWTopic = combine_mp(MountPoint, Topic),
    case vmq_hook:only(auth_on_publish,
                       [User, ClientId, last_will, LWTopic, Msg, false]) of
        ok ->
            case valid_msg_size(Msg) of
                true ->
                    {connected, send_connack(?CONNACK_ACCEPT,
                                             State#state{will_qos=Qos,
                                                         will_topic=LWTopic,
                                                         will_msg=Msg})};
                false ->
                    lager:warning(
                      "last will message has invalid size for client ~p",
                      [ClientId]),
                    {wait_for_connect,
                     send_connack(?CONNACK_SERVER, State)}
            end;
        _ ->
            lager:warning("can't authenticate last will for client ~p",
                        [ClientId]),
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


-spec maybe_publish_last_will(state()) -> state().
maybe_publish_last_will(#state{will_topic=undefined} = State) -> State;
maybe_publish_last_will(#state{will_qos=QoS, will_topic=Topic,
                               will_msg=Msg } = State) ->
    {MsgId, NewState} = get_msg_id(QoS, State),
    dispatch_publish(QoS, MsgId,
                     #vmq_msg{routing_key=Topic, payload=Msg}, NewState).


-spec dispatch_publish(qos(), msg_id(), msg(), state()) -> state().
dispatch_publish(Qos, MessageId, #vmq_msg{routing_key=Topic,
                                          payload=Payload} = Msg, State) ->
    #state{client_id=ClientId, msg_log_handler=MsgLogHandler} = State,
    MsgLogHandler(ClientId, Topic, Payload),
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
    #state{username=User, client_id=ClientId} = State,
    case publish(User, ClientId, 0, Msg) of
        ok ->
            State;
        {error, _Reason} ->
            drop(State)
    end.

-spec dispatch_publish_qos1(msg_id(), msg(), state()) -> state().
dispatch_publish_qos1(MessageId, Msg, State) ->
    case check_in_flight(State) of
        true ->
            #state{username=User, client_id=ClientId} = State,
            #vmq_msg{msg_ref=MsgRef} = UpdatedMsg =
                vmq_msg_store:store(State#state.client_id, Msg),
            case publish(User, ClientId, 1, UpdatedMsg) of
                ok ->
                    NewState = send_frame(?PUBACK,
                                          #mqtt_frame_publish{
                                             message_id=MessageId
                                            }, <<>>, State),
                    vmq_msg_store:deref(MsgRef),
                    NewState;
                {error, _Reason} ->
                    %% can't publish due to overload or netsplit
                    vmq_msg_store:deref(MsgRef),
                    drop(State)
            end;
        false ->
            drop(State)
    end.

-spec dispatch_publish_qos2(msg_id(), msg(), state()) -> state().
dispatch_publish_qos2(MessageId, Msg, State) ->
    case check_in_flight(State) of
        true ->
            #state{client_id=ClientId,
                   waiting_acks=WAcks, retry_interval=RetryInterval,
                   send_fun=SendFun, send_cnt=SendCnt} = State,
            #vmq_msg{msg_ref=MsgRef, retain=IsRetain} =
                vmq_msg_store:store(ClientId, Msg),
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

drop(#state{pub_dropped_cnt=PubDropped} = State) ->
    State#state{pub_dropped_cnt=incr_pub_dropped_cnt(PubDropped)}.

-spec send_publish_frame(mqtt_frame(), state()) -> state() | {error, atom()}.
send_publish_frame(Frame, State) ->
    #state{send_fun=SendFun, send_cnt=SendCnt, pub_send_cnt=PubSendCnt} = State,
    case SendFun(Frame) of
        ok ->
            State#state{send_cnt=incr_msg_sent_cnt(SendCnt),
                        pub_send_cnt=incr_pub_sent_cnt(PubSendCnt)};
        {error, Reason} ->
            {error, Reason}
    end.

-spec publish(username(), client_id(), qos(), msg()) ->  ok | {error,atom()}.
publish(User, ClientId, QoS, Msg) ->
    %% auth_on_publish hook must return either:
    %% next | ok
    #vmq_msg{routing_key=Topic,
             payload=Payload,
             retain=IsRetain, dup=_IsDup} = Msg,
    HookParams = [User, ClientId, QoS, Topic, Payload, IsRetain],
    case vmq_hook:only(auth_on_publish, HookParams) of
        not_found ->
            {error, not_allowed};
        ok when QoS > 0 ->
            MaybeUpdatedMsg = vmq_msg_store:store(ClientId, Msg),
            on_publish_hook(vmq_reg:publish(MaybeUpdatedMsg), HookParams);
        ok ->
            on_publish_hook(vmq_reg:publish(Msg), HookParams)
    end.

on_publish_hook(ok, HookParams) ->
    vmq_hook:all(on_publish, HookParams),
    ok;
on_publish_hook(Other, _) -> Other.

-spec combine_mp(_, 'undefined' | string()) -> 'undefined' | [any()].
combine_mp("", Topic) -> Topic;
combine_mp(MountPoint, Topic) ->
    lists:flatten([MountPoint, "/", string:strip(Topic, left, $/)]).

-spec clean_mp([any()], _) -> any().
clean_mp("", Topic) -> Topic;
clean_mp(MountPoint, MountedTopic) ->
    lists:sublist(MountedTopic, length(MountPoint) + 1, length(MountedTopic)).

-spec random_client_id() -> string().
random_client_id() ->
    lists:flatten(["anon-", base64:encode_to_string(crypto:rand_bytes(20))]).

-spec handle_waiting_acks(state()) -> dict().
handle_waiting_acks(State) ->
    #state{client_id=ClientId, waiting_acks=WAcks} = State,
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
                      vmq_msg_store:defer_deliver_uncached(ClientId,
                                                           {MsgId, QoS, Bin}),
                      Acc;
                  (MsgId, {QoS, Bin, TRef, undefined}, Acc) ->
                      cancel_timer(TRef),
                      vmq_msg_store:defer_deliver_uncached(ClientId,
                                                           {MsgId, QoS, Bin}),
                      Acc;
                  (_MsgId, {QoS, _Frame, TRef, MsgStoreRef}, Acc) ->
                      cancel_timer(TRef),
                      vmq_msg_store:defer_deliver(ClientId, QoS,
                                                  MsgStoreRef, true),
                      Acc
              end, [], WAcks).

-spec send_after(non_neg_integer(), any()) -> reference().
send_after(Time, Msg) ->
    gen_fsm:send_event_after(Time, Msg).

-spec cancel_timer('undefined' | reference()) -> 'ok'.
cancel_timer(undefined) -> ok;
cancel_timer(TRef) -> gen_fsm:cancel_timer(TRef), ok.

-spec check_in_flight(state()) -> boolean().
check_in_flight(#state{waiting_acks=WAcks}) ->
    case vmq_config:get_env(max_inflight_messages, 20) of
        0 -> true;
        V ->
            dict:size(WAcks) < V
    end.

-spec valid_msg_size(binary()) -> boolean().
valid_msg_size(Payload) ->
    case vmq_config:get_env(message_size_limit, 0) of
        0 -> true;
        S when byte_size(Payload) =< S -> true;
        _ -> false
    end.

get_info_items([], StateName, State) ->
    DefaultItems = [pid, client_id, user, peer_host, peer_port, state],
    get_info_items(DefaultItems, StateName, State);
get_info_items(Items, StateName, State) ->
    get_info_items(Items, StateName, State, []).

get_info_items([pid|Rest], StateName, State, Acc) ->
    get_info_items(Rest, StateName, State, [{pid, self()}|Acc]);
get_info_items([client_id|Rest], StateName, State, Acc) ->
    get_info_items(Rest, StateName, State, [{client_id,
                                             State#state.client_id}|Acc]);
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
get_info_items([mountpoint|Rest], StateName, State, Acc) ->
    get_info_items(Rest, StateName, State,
                   [{mountpoint, State#state.mountpoint}|Acc]);
get_info_items([timeout|Rest], StateName, State, Acc) ->
    get_info_items(Rest, StateName, State,
                   [{timeout, State#state.keep_alive}|Acc]);
get_info_items([retry_timeout|Rest], StateName, State, Acc) ->
    get_info_items(Rest, StateName, State,
                   [{timeout, State#state.retry_interval}|Acc]);
get_info_items([recv_cnt|Rest], StateName, State, Acc) ->
    get_info_items(Rest, StateName, State,
                   [{recv_cnt, State#state.recv_cnt}|Acc]);
get_info_items([send_cnt|Rest], StateName, State, Acc) ->
    get_info_items(Rest, StateName, State,
                   [{send_cnt, State#state.send_cnt}|Acc]);
get_info_items([waiting_acks|Rest], StateName, State, Acc) ->
    Size = dict:size(State#state.waiting_acks),
    get_info_items(Rest, StateName, State,
                   [{waiting_acks, Size}|Acc]);
get_info_items([subscriptions|Rest], StateName, State, Acc) ->
    Subscriptions = vmq_reg:subscriptions_for_client(State#state.client_id),
    get_info_items(Rest, StateName, State,
                   [{subscriptions, Subscriptions}|Acc]);
get_info_items([_|Rest], StateName, State, Acc) ->
    get_info_items(Rest, StateName, State, Acc);
get_info_items([], _, _, Acc) -> Acc.

init_ts() ->
    {os:timestamp(), 0, 0}.

incr_msg_sent_cnt(SndCnt) ->
    incr_cnt(incr_messages_sent, SndCnt).
incr_msg_recv_cnt(RcvCnt) ->
    incr_cnt(incr_messages_received, RcvCnt).
incr_pub_recv_cnt(PubRecvCnt) ->
    incr_cnt(incr_publishes_received, PubRecvCnt).
incr_pub_dropped_cnt(PubDroppedCnt) ->
    incr_cnt(incr_publishes_dropped, PubDroppedCnt).
incr_pub_sent_cnt(PubSendCnt) ->
    incr_cnt(incr_publishes_sent, PubSendCnt).

incr_cnt(IncrFun, {{M, S, _}, V, I}) ->
    NewV = V + 1,
    NewI = I + 1,
    case os:timestamp() of
        {M, S, _} = TS ->
            {TS, NewV, NewI};
        TS ->
            apply(vmq_systree, IncrFun, [NewI]),
            {TS, NewV, 0}
    end.
