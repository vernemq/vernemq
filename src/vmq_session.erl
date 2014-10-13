-module(vmq_session).
-behaviour(gen_fsm).
-include_lib("emqtt_commons/include/emqtt_frame.hrl").

-export([start/4,
         stop/1,
         start_link/4,
         deliver/7,
         in/2,
         deliver_bin/2,
         disconnect/1]).

-export([init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-export([wait_for_connect/2,
         connection_attempted/2,
         connected/2]).


-define(CLOSE_AFTER, 5000).

-record(state, {
                %% parser requirements
                buffer= <<>>                                :: binary(),
                parser_state=emqtt_frame:initial_state()    :: none | fun((_) -> any()),
                %% networking requirements
                send_fun                                    :: function(),
                %% mqtt layer requirements
                next_msg_id=1                               :: msg_id(),
                client_id                                   :: client_id(),
                max_client_id_size=23                       :: non_neg_integer(),
                will_topic                                  :: topic(),
                will_msg                                    :: payload(),
                will_qos                                    :: qos(),
                waiting_acks=dict:new()                     :: dict(),
                %% statemachine requirements
                connection_attempted=false                  :: boolean(),
                %% auth backend requirement
                peer                                        :: {inet:ip_address(), inet:port_number()},
                username                                    :: username() | {preauth, string() | undefined},
                msg_log_handler                             :: fun((client_id(), topic(), payload()) -> any()),
                mountpoint=""                               :: string(),
                retry_interval=20000                        :: pos_integer(),
                keep_alive                                  :: pos_integer(),
                keep_alive_timer                            :: undefined | reference(),
                clean_session=false                         :: flag()

         }).

-type statename() :: atom().

-type mqtt_variable_ping() :: undefined.
-type mqtt_variable() :: #mqtt_frame_connect{}
                       | #mqtt_frame_connack{}
                       | #mqtt_frame_publish{}
                       | #mqtt_frame_subscribe{}
                       | #mqtt_frame_suback{}
                       | mqtt_variable_ping().

-hook({auth_on_publish, only, 6}).
-hook({on_publish, all, 6}).
-hook({auth_on_register, only, 4}).
-hook({on_register, all, 4}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% API FUNCTIONS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start(ReaderPid, Peer, SendFun, Opts) ->
    supervisor:start_child(vmq_session_sup, [ReaderPid, Peer, SendFun, Opts]).

stop(FsmPid) ->
    supervisor:terminate_child(vmq_session_sup, FsmPid).

start_link(ReaderPid, Peer, SendFun, Opts) ->
    gen_fsm:start_link(?MODULE, [ReaderPid, Peer, SendFun, Opts], []).

-spec deliver(pid(),topic(),payload(),qos(),flag(), flag(), msg_ref()) -> ok.
deliver(FsmPid, Topic, Payload, QoS, IsRetained, IsDup, Ref) ->
    gen_fsm:send_event(FsmPid, {deliver, Topic, Payload, QoS, IsRetained, IsDup, Ref}).

-spec deliver_bin(pid(), {msg_id(), qos(), binary()}) -> ok.
deliver_bin(FsmPid, Term) ->
    gen_fsm:send_event(FsmPid, {deliver_bin, Term}).

-spec disconnect(pid()) -> ok.
disconnect(FsmPid) ->
    gen_fsm:send_event(FsmPid, disconnect).

in(FsmPid, Event) ->
    gen_fsm:send_all_state_event(FsmPid, {input, Event}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% FSM FUNCTIONS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

wait_for_connect(timeout, State) ->
    io:format("[~p] stop due to timeout~n", [self()]),
    {stop, normal, State}.

connection_attempted(timeout, State) ->
    {next_state, wait_for_connect, State, ?CLOSE_AFTER}.

connected(timeout, State) ->
    % ignore
    {next_state, connected, State};
connected({deliver, Topic, Payload, QoS, IsRetained, IsDup, MsgStoreRef}, State) ->
    case check_in_flight(State) of
        true ->
            #state{client_id=ClientId, waiting_acks=WAcks, mountpoint=MountPoint,
                   retry_interval=RetryInterval} = State,
            {OutgoingMsgId, State1} = get_msg_id(QoS, State),
            Frame = #mqtt_frame{
                       fixed=#mqtt_frame_fixed{
                                type=?PUBLISH,
                                qos=QoS,
                                retain=IsRetained,
                                dup=IsDup
                               },
                       variable=#mqtt_frame_publish{
                                   topic_name=clean_mp(MountPoint, Topic),
                                   message_id=OutgoingMsgId},
                       payload=Payload
                      },
            case send_publish_frame(Frame, State1) of
                {error, Reason} when QoS > 0 ->
                    vmq_msg_store:defer_deliver(ClientId, QoS, MsgStoreRef),
                    io:format("[~p] stop due to ~p, deliver when client reconnects~n", [self(), Reason]),
                    {stop, normal, State1};
                {error, Reason} ->
                    io:format("[~p] stop due to ~p~n", [self(), Reason]),
                    {stop, normal, State1};
                NewState when QoS == 0 ->
                    {next_state, connected, NewState};
                NewState when QoS > 0 ->
                    Ref = send_after(RetryInterval, {retry, OutgoingMsgId}),
                    {next_state, connected, NewState#state{
                                              waiting_acks=dict:store(OutgoingMsgId,
                                                                      {QoS, Frame,
                                                                       Ref,
                                                                       MsgStoreRef},
                                                                      WAcks)}}
            end;
        false->
            %% drop
            vmq_systree:incr_publishes_dropped(),
            case QoS > 0 of
                true ->
                    vmq_msg_store:deref(MsgStoreRef);
                false ->
                    ok
            end,
            {next_state, connected, State}
    end;
connected({deliver_bin, {MsgId, QoS, Bin}},State) ->
    #state{send_fun=SendFun, waiting_acks=WAcks, retry_interval=RetryInterval} = State,
    SendFun(Bin),
    vmq_systree:incr_messages_sent(),
    Ref = send_after(RetryInterval, {retry, MsgId}),
    {next_state, connected, State#state{
                  waiting_acks=dict:store(MsgId,
                                          {QoS, Bin, Ref, undefined},
                                          WAcks)}};

connected({retry, MessageId}, State) ->
    #state{send_fun=SendFun, waiting_acks=WAcks, retry_interval=RetryInterval} = State,
    NewState =
    case dict:find(MessageId, WAcks) of
        error ->
            State;
        {ok, {QoS, #mqtt_frame{fixed=Fixed} = Frame, _, MsgStoreRef}} ->
            SendFun(Frame#mqtt_frame{
                      fixed=Fixed#mqtt_frame_fixed{
                              dup=true
                             }}),
            vmq_systree:incr_messages_sent(),
            Ref = send_after(RetryInterval, {retry, MessageId}),
            State#state{waiting_acks=dict:store(MessageId,
                                                {QoS, Frame, Ref, MsgStoreRef},
                                                WAcks)}
    end,
    {next_state, connected, NewState};

connected(keepalive_expired, State) ->
    io:format("[~p] stop due to ~p~n", [self(), keepalive_expired]),
    {stop, normal, State};

connected(disconnect, State) ->
    io:format("[~p] stop due to ~p~n", [self(), disconnect]),
    {stop, normal, State}.

init([ReaderPid, Peer, SendFun, Opts]) ->
    {_,MountPoint} = lists:keyfind(mountpoint, 1, Opts),
    {_,MaxClientIdSize} = lists:keyfind(max_client_id_size, 1, Opts),
    {_,RetryInterval} = lists:keyfind(retry_interval, 1, Opts),
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
    process_flag(trap_exit, true),
    monitor(process, ReaderPid),
    {ok, wait_for_connect, #state{peer=Peer, send_fun=SendFun,
                                  msg_log_handler=MsgLogHandler,
                                  mountpoint=string:strip(MountPoint, right, $/),
                                  username=PreAuthUser,
                                  max_client_id_size=MaxClientIdSize,
                                  retry_interval=1000 * RetryInterval
                                  }, ?CLOSE_AFTER}.


handle_event({input, #mqtt_frame{fixed=#mqtt_frame_fixed{type=?DISCONNECT}}}, _, State) ->
    {stop, normal, State};
handle_event({input, Frame}, StateName, #state{keep_alive_timer=KARef} = State) ->
    #mqtt_frame{fixed=Fixed,
                variable=Variable,
                payload=Payload} = Frame,
    cancel_timer(KARef),
    vmq_systree:incr_messages_received(),
    case handle_frame(StateName, Fixed, Variable, Payload, State) of
        {NextStateName, #state{keep_alive=KeepAlive} = NewState} ->
            {next_state, NextStateName,
             NewState#state{keep_alive_timer=gen_fsm:send_event_after(KeepAlive, keepalive_expired)}};
        Ret -> Ret
    end.

handle_sync_event(Req, _From, _StateName, State) ->
    {stop, {error, {unknown_req, Req}}, State}.

handle_info({'DOWN', _, process, Pid, Reason}, _StateName, State) ->
    lager:info("Reader Proc ~p of ~p went down, die too", [Pid, self()]),
    {stop, Reason, State} .

terminate(_Reason, connected, State) ->
    #state{client_id=ClientId, clean_session=CleanSession} = State,
    case CleanSession of
        true ->
            ok;
        false ->
            handle_waiting_acks(State)
    end,
    maybe_publish_last_will(State),
    vmq_reg:unregister_client(ClientId, CleanSession),
    ok;
terminate(_Reason, _, _) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% INTERNALS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec handle_frame(statename(), #mqtt_frame_fixed{}, mqtt_variable(), payload(), #state{}) ->
    {statename(), #state{}} | {stop, atom(), #state{}}.

handle_frame(wait_for_connect, _, #mqtt_frame_connect{keep_alive=KeepAlive} = Var, _, State) ->
    vmq_systree:incr_connect_received(),
    %% the client is allowed "grace" of a half a time period
    KKeepAlive = (KeepAlive + (KeepAlive div 2)) * 1000,
    check_connect(Var, State#state{keep_alive=KKeepAlive});
handle_frame(wait_for_connect, _, _, _, #state{retry_interval=RetryInterval} = State) ->
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
            %io:format("got error~n"),
            {connected, State}
    end;

handle_frame(connected, #mqtt_frame_fixed{type=?PUBREC}, Var, _, State) ->
    #state{waiting_acks=WAcks, send_fun=SendFun, retry_interval=RetryInterval} = State,
    #mqtt_frame_publish{message_id=MessageId} = Var,
    %% qos2 flow
    {_, _, Ref, MsgStoreRef} = dict:fetch(MessageId, WAcks),
    cancel_timer(Ref), % cancel republish timer
    PubRelFrame =#mqtt_frame{
                             fixed=#mqtt_frame_fixed{type=?PUBREL, qos=1},
                             variable=#mqtt_frame_publish{message_id=MessageId},
                             payload= <<>>},
    SendFun(PubRelFrame),
    vmq_systree:incr_messages_sent(),
    NewRef = send_after(RetryInterval, {retry, MessageId}),
    vmq_msg_store:deref(MsgStoreRef),
    {connected, State#state{
                  waiting_acks=dict:store(MessageId,
                                          {2, PubRelFrame,
                                           NewRef,
                                           undefined}, WAcks)}};

handle_frame(connected, #mqtt_frame_fixed{type=?PUBREL, dup=IsDup}, Var, _, State) ->
    #state{username=User, client_id=ClientId, waiting_acks=WAcks} = State,
    #mqtt_frame_publish{message_id=MessageId} = Var,
    %% qos2 flow
    NewState =
    case dict:find({qos2, MessageId} , WAcks) of
        {ok, {_, _, TRef, {MsgRef, IsRetain}}} ->
            cancel_timer(TRef),
            {ok, {RoutingKey, Payload}} = vmq_msg_store:retrieve(MsgRef),
            publish(User, ClientId, MsgRef, RoutingKey, Payload, IsRetain),
            vmq_msg_store:deref(MsgRef),
            State#state{waiting_acks=dict:erase({qos2, MessageId}, WAcks)};
        error when IsDup ->
            %% already delivered, Client expects a PUBCOMP
            State
    end,
    {connected, send_frame(?PUBCOMP,#mqtt_frame_publish{
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
    #state{mountpoint=MountPoint} = State,
    #mqtt_frame_publish{topic_name=Topic, message_id=MessageId} = Var,
    %% we disallow Publishes on Topics prefixed with '$'
    %% this allows us to use such prefixes for e.g. '$SYS' Tree
    case {hd(Topic), valid_msg_size(Payload)} of
        {$$, _} ->
            {connected, State};
        {_, true} ->
            vmq_systree:incr_publishes_received(),
            {connected, dispatch_publish(QoS, MessageId, combine_mp(MountPoint, Topic), Payload,
                                         IsRetain, State)};
        {_, false} ->
            {connected, State}
    end;

handle_frame(connected, #mqtt_frame_fixed{type=?SUBSCRIBE}, Var, _, State) ->
    #state{client_id=Id, username=User, mountpoint=MountPoint} = State,
    #mqtt_frame_subscribe{topic_table=Topics, message_id=MessageId} = Var,
    TTopics = [{combine_mp(MountPoint, Name), QoS} || #mqtt_topic{name=Name, qos=QoS} <- Topics],
    case vmq_reg:subscribe(User, Id, TTopics) of
        ok ->
            {_, QoSs} = lists:unzip(TTopics),
            NewState = send_frame(?SUBACK, #mqtt_frame_suback{
                                              message_id=MessageId,
                                              qos_table=QoSs}, <<>>, State),
            {connected, NewState};
        {error, _Reason} ->
            %% cant subscribe due to netsplit,
            %% Subscribe uses QoS 1 so the client will retry
            {connected, State}
    end;

handle_frame(connected, #mqtt_frame_fixed{type=?UNSUBSCRIBE}, Var, _, State) ->
    #state{client_id=Id, username=User, mountpoint=MountPoint} = State,
    #mqtt_frame_subscribe{topic_table=Topics, message_id=MessageId} = Var,
    TTopics = [combine_mp(MountPoint, Name) || #mqtt_topic{name=Name} <- Topics],
    case vmq_reg:unsubscribe(User, Id, TTopics) of
        ok ->
            NewState = send_frame(?UNSUBACK, #mqtt_frame_suback{
                                                message_id=MessageId
                                               }, <<>>, State),
            {connected, NewState};
        {error, _Reason} ->
            %% cant unsubscribe due to netsplit,
            %% Unsubscribe uses QoS 1 so the client will retry
            {connected, State}
    end;

handle_frame(connected, #mqtt_frame_fixed{type=?PINGREQ}, _, _, State) ->
    NewState = send_frame(?PINGRESP, undefined, <<>>, State),
    {connected, NewState}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% INTERNAL
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
check_connect(F, State) ->
    check_client_id(F, State).

check_client_id(#mqtt_frame_connect{}, #state{username={preauth, undefined}, peer=Peer} = State) ->
    %% No common name found in client certificate
    lager:error("can't authenticate ssl client ~p due to no_common_name_found", [Peer]),
    {connection_attempted,
     send_connack(?CONNACK_CREDENTIALS, State)};
check_client_id(#mqtt_frame_connect{clean_sess=CleanSession} = F,
                #state{username={preauth, ClientId}, peer=Peer} = State) ->
    %% User preauthenticated using e.g. SSL client certificate
    case vmq_reg:register_client(ClientId, CleanSession) of
        ok ->
            vmq_hook:all(on_register, [Peer, ClientId, undefined, undefined]),
            check_will(F, State#state{clean_session=CleanSession});
        {error, Reason} ->
            lager:error("can't register client ~p due to ~p", [ClientId, Reason]),
            {connection_attempted,
             send_connack(?CONNACK_SERVER, State)}
    end;
check_client_id(#mqtt_frame_connect{client_id=missing}, State) ->
    {stop, normal, State};
check_client_id(#mqtt_frame_connect{client_id=empty, proto_ver=4} = F, State) ->
    RandomClientId = random_client_id(),
    check_user(F#mqtt_frame_connect{client_id=RandomClientId}, State#state{client_id=RandomClientId});
check_client_id(#mqtt_frame_connect{client_id=empty, proto_ver=3}, State) ->
    lager:error("empty protocol version not allowed in mqttv3 ~p", [State#state.client_id]),
    {connection_attempted,
     send_connack(?CONNACK_INVALID_ID, State)};
check_client_id(#mqtt_frame_connect{client_id=Id, proto_ver=V} = F,
                #state{max_client_id_size=S} = State) when length(Id) =< S ->
    case lists:member(V, [3,4,131]) of
        true ->
            check_user(F, State#state{client_id=Id});
        false ->
            lager:error("invalid protocol version for ~p ~p", [Id, V]),
            {connection_attempted,
             send_connack(?CONNACK_PROTO_VER, State)}
    end;
check_client_id(#mqtt_frame_connect{client_id=Id}, State) ->
    lager:error("invalid client id ~p", [Id]),
    {connection_attempted,
     send_connack(?CONNACK_INVALID_ID, State)}.

check_user(#mqtt_frame_connect{username=""} = F, State) ->
    check_user(F#mqtt_frame_connect{username=undefined, password=undefined}, State);
check_user(#mqtt_frame_connect{username=User, password=Password,
                               client_id=ClientId, clean_sess=CleanSession} = F, State) ->
    #state{peer=Peer} = State,
    case vmq_config:get_env(allow_anonymous, false) of
        false ->
            case vmq_hook:only(auth_on_register, [Peer, ClientId, User, Password]) of
                ok ->
                    case vmq_reg:register_client(ClientId, CleanSession) of
                        ok ->
                            vmq_hook:all(on_register, [Peer, ClientId, User, Password]),
                            check_will(F, State#state{username=User});
                        {error, Reason} ->
                            lager:error("can't register client ~p due to ", [ClientId, Reason]),
                            {connection_attempted,
                             send_connack(?CONNACK_SERVER, State)}
                    end;
                not_found ->
                    % returned when no hook on_register hook was
                    % able to authenticate user
                    lager:error("can't authenticate client ~p due to not_found", [ClientId]),
                    {connection_attempted,
                     send_connack(?CONNACK_AUTH, State)};
                {error, invalid_credentials} ->
                    lager:error("can't authenticate client ~p due to invalid_credentials", [ClientId]),
                    {connection_attempted,
                     send_connack(?CONNACK_CREDENTIALS, State)};
                {error, not_authorized} ->
                    lager:error("can't authenticate client ~p due to not_authorized", [ClientId]),
                    {connection_attempted,
                     send_connack(?CONNACK_AUTH, State)}
            end;
        true ->
            case vmq_reg:register_client(ClientId, CleanSession) of
                ok ->
                    vmq_hook:all(on_register, [Peer, ClientId, User, Password]),
                    check_will(F, State#state{username=User});
                {error, Reason} ->
                    lager:error("can't register client ~p due to reason", [ClientId, Reason]),
                    {connection_attempted,
                     send_connack(?CONNACK_SERVER, State)}
            end
    end.

check_will(#mqtt_frame_connect{will_topic=undefined}, State) ->
    {connected, send_connack(?CONNACK_ACCEPT, State)};
check_will(#mqtt_frame_connect{will_topic=""}, State) ->
    %% null topic.... Mosquitto sends a CONNACK_INVALID_ID...
    lager:error("invalid last will topic for client ~p", [State#state.client_id]),
    {connection_attempted,
     send_connack(?CONNACK_INVALID_ID, State)};
check_will(#mqtt_frame_connect{will_topic=Topic, will_msg=Msg, will_qos=Qos}, State) ->
    #state{mountpoint=MountPoint, username=User, client_id=ClientId} = State,
    LWTopic = combine_mp(MountPoint, Topic),
    case vmq_hook:only(auth_on_publish, [User, ClientId, last_will, LWTopic, Msg, false]) of
        ok ->
            case valid_msg_size(Msg) of
                true ->
                    {connected, send_connack(?CONNACK_ACCEPT,
                                             State#state{will_qos=Qos,
                                                         will_topic=LWTopic,
                                                         will_msg=Msg})};
                false ->
                    lager:error("last will message has invalid size for client ~p", [ClientId]),
                    {connection_attempted,
                     send_connack(?CONNACK_SERVER, State)}
            end;
        _ ->
            lager:error("can't authenticate last will for client ~p", [ClientId]),
            {connection_attempted, send_connack(?CONNACK_AUTH, State)}
    end.

-spec send_connack(non_neg_integer(), #state{}) -> #state{}.
send_connack(ReturnCode, State) ->
    send_frame(?CONNACK, #mqtt_frame_connack{return_code=ReturnCode},
               <<>>, State).

-spec send_frame(non_neg_integer(),mqtt_variable(),payload(),#state{}) -> #state{}.
send_frame(Type, Variable, Payload, State) ->
    send_frame(Type, false, Variable, Payload, State).

send_frame(Type, DUP, Variable, Payload, #state{send_fun=SendFun} = State) ->
    SendFun(#mqtt_frame{
               fixed=#mqtt_frame_fixed{type=Type, dup=DUP},
               variable=Variable,
               payload=Payload
              }),
    vmq_systree:incr_messages_sent(),
    State.


-spec maybe_publish_last_will(#state{}) -> #state{}.
maybe_publish_last_will(#state{will_topic=undefined} = State) -> State;
maybe_publish_last_will(#state{will_qos=QoS, will_topic=Topic,
                               will_msg=Msg } = State) ->
    {MsgId, NewState} = get_msg_id(QoS, State),
    dispatch_publish(QoS, MsgId, Topic, Msg, false, NewState).


-spec dispatch_publish(qos(), msg_id(), topic(), payload(), flag(), #state{}) -> #state{}.
dispatch_publish(Qos, MessageId, Topic, Payload, IsRetain, State) ->
    #state{client_id=ClientId, msg_log_handler=MsgLogHandler} = State,
    MsgLogHandler(ClientId, Topic, Payload),
    dispatch_publish_(Qos, MessageId, Topic, Payload, IsRetain, State).

-spec dispatch_publish_(qos(), msg_id(), topic(), payload(), flag(), #state{}) -> #state{}.
dispatch_publish_(0, MessageId, Topic, Payload, IsRetain, State) ->
    dispatch_publish_qos0(MessageId, Topic, Payload, IsRetain, State);
dispatch_publish_(1, MessageId, Topic, Payload, IsRetain, State) ->
    dispatch_publish_qos1(MessageId, Topic, Payload, IsRetain, State);
dispatch_publish_(2, MessageId, Topic, Payload, IsRetain, State) ->
    dispatch_publish_qos2(MessageId, Topic, Payload, IsRetain, State).

-spec dispatch_publish_qos0(msg_id(), topic(), payload(), flag(), #state{}) -> #state{}.
dispatch_publish_qos0(_MessageId, Topic, Payload, IsRetain, State) ->
    #state{username=User, client_id=ClientId} = State,
    publish(User, ClientId, undefined, Topic, Payload, IsRetain),
    State.

-spec dispatch_publish_qos1(msg_id(), topic(), payload(), flag(), #state{}) -> #state{}.
dispatch_publish_qos1(MessageId, Topic, Payload, IsRetain, State) ->
    case check_in_flight(State) of
        true ->
            #state{username=User, client_id=ClientId} = State,
            MsgRef = vmq_msg_store:store(User, ClientId, Topic, Payload),
            publish(User, ClientId, MsgRef, Topic, Payload, IsRetain),
            NewState = send_frame(?PUBACK, #mqtt_frame_publish{message_id=MessageId},
                                  <<>>, State),
            vmq_msg_store:deref(MsgRef),
            NewState;
        false ->
            %% drop
            vmq_systree:incr_publishes_dropped(),
            State
    end.

-spec dispatch_publish_qos2(msg_id(), topic(), payload(), flag(), #state{}) -> #state{}.
dispatch_publish_qos2(MessageId, Topic, Payload, IsRetain, State) ->
    case check_in_flight(State) of
        true ->
            #state{username=User, client_id=ClientId, waiting_acks=WAcks,
                   retry_interval=RetryInterval, send_fun=SendFun} = State,
            MsgRef = vmq_msg_store:store(User, ClientId, Topic, Payload),
            Ref = send_after(RetryInterval, {retry, {qos2, MessageId}}),
            Frame = #mqtt_frame{
                       fixed=#mqtt_frame_fixed{type=?PUBREC},
                       variable=#mqtt_frame_publish{message_id=MessageId},
                       payload= <<>>
                      },
            SendFun(Frame),
            vmq_systree:incr_messages_sent(),
            State#state{waiting_acks=dict:store(
                                       {qos2, MessageId},
                                       {2, Frame, Ref, {MsgRef, IsRetain}}, WAcks)};
        false ->
            %% drop
            vmq_systree:incr_publishes_dropped(),
            State
    end.

-spec get_msg_id(qos(),#state{}) -> {msg_id(), #state{}}.
get_msg_id(0, State) ->
    {undefined, State};
get_msg_id(_, #state{next_msg_id=65535} = State) ->
    {1, State#state{next_msg_id=2}};
get_msg_id(_, #state{next_msg_id=MsgId} = State) ->
    {MsgId, State#state{next_msg_id=MsgId + 1}}.

-spec send_publish_frame(#mqtt_frame{}, #state{}) -> #state{} | {error, atom()}.
send_publish_frame(Frame, State) ->
    #state{send_fun=SendFun} = State,
    case SendFun(Frame) of
        ok ->
            vmq_systree:incr_publishes_sent(),
            vmq_systree:incr_messages_sent(),
            State;
        {error, Reason} ->
            {error, Reason}
    end.

-spec publish(username(), client_id(), undefined | msg_ref(), topic(), payload(), flag()) ->
    ok | {error,atom()}.
publish(User, ClientId, MsgRef, Topic, Payload, IsRetain) ->
    %% auth_on_publish hook must return either:
    %% next | ok
    case vmq_hook:only(auth_on_publish, [User, ClientId, MsgRef, Topic,
                                            Payload, IsRetain]) of
        not_found ->
            {error, not_allowed};
        ok ->
            R = vmq_reg:publish(User, ClientId, MsgRef, Topic,
                                   Payload, IsRetain),
            vmq_hook:all(on_publish, [User, ClientId, MsgRef,
                                         Topic, Payload, IsRetain]),
            R
    end.

-spec combine_mp(_,'undefined' | string()) -> 'undefined' | [any()].
combine_mp("", Topic) -> Topic;
combine_mp(MountPoint, Topic) ->
    lists:flatten([MountPoint, "/", string:strip(Topic, left, $/)]).

-spec clean_mp([any()],_) -> any().
clean_mp("", Topic) -> Topic;
clean_mp(MountPoint, MountedTopic) ->
    lists:sublist(MountedTopic, length(MountPoint) + 1, length(MountedTopic)).

-spec random_client_id() -> string().
random_client_id() ->
    lists:flatten(["anon-", base64:encode_to_string(crypto:rand_bytes(20))]).

-spec handle_waiting_acks(#state{}) -> dict().
handle_waiting_acks(State) ->
    #state{client_id=ClientId, waiting_acks=WAcks} = State,
    dict:fold(fun ({qos2, _}, _, Acc) ->
                      Acc;
                  (MsgId, {QoS, #mqtt_frame{fixed=Fixed} = Frame, TRef, undefined}, Acc) ->
                      %% unacked PUBREL Frame
                      cancel_timer(TRef),
                      Bin = emqtt_frame:serialise(
                              Frame#mqtt_frame{
                                fixed=Fixed#mqtt_frame_fixed{
                                        dup=true}}),
                      vmq_msg_store:defer_deliver_uncached(ClientId, {MsgId, QoS, Bin}),
                      Acc;
                  (MsgId, {QoS, Bin, TRef, undefined}, Acc) ->
                      cancel_timer(TRef),
                      vmq_msg_store:defer_deliver_uncached(ClientId, {MsgId, QoS, Bin}),
                      Acc;
                  (_MsgId, {QoS, _Frame, TRef, MsgStoreRef}, Acc) ->
                      cancel_timer(TRef),
                      vmq_msg_store:defer_deliver(ClientId, QoS, MsgStoreRef, true),
                      Acc
              end, [], WAcks).

-spec send_after(non_neg_integer(), any()) -> reference().
send_after(Time, Msg) ->
    gen_fsm:send_event_after(Time, Msg).

-spec cancel_timer('undefined' | reference()) -> 'ok'.
cancel_timer(undefined) -> ok;
cancel_timer(TRef) -> gen_fsm:cancel_timer(TRef), ok.

-spec check_in_flight(#state{}) -> boolean().
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
