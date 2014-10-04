-module(vmq_fsm).
-include_lib("emqtt_commons/include/emqtt_frame.hrl").

-export([deliver/7,
         deliver_bin/2,
         disconnect/1]).

-export([init/3,
         handle_fsm_msg/2,
         handle_input/2,
         handle_close/1,
         handle_error/2
        ]).


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
-type retval() :: {statename(), #state{}} | stop.
-type retval2() :: {statename(), #state{}} | {stop, atom(), #state{}}.
-type fsm_msg() :: disconnect
                 | timeout
                 | {deliver, topic(), payload(), qos(), flag(), flag(), msg_ref()}
                 | {deliver_bin, {msg_id(), qos(), binary()}}
                 | {retry, msg_id()}
                 | {unhandled_transport_error, atom()}.

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
-spec deliver(pid(),topic(),payload(),qos(),flag(), flag(), msg_ref()) -> ok.
deliver(FsmPid, Topic, Payload, QoS, IsRetained, IsDup, Ref) ->
    FsmPid ! {deliver, Topic, Payload, QoS, IsRetained, IsDup, Ref},
    ok.

-spec deliver_bin(pid(), {msg_id(), qos(), binary()}) -> ok.
deliver_bin(FsmPid, Term) ->
    FsmPid ! {deliver_bin, Term},
    ok.

-spec disconnect(pid()) -> ok.
disconnect(FsmPid) ->
    FsmPid ! disconnect,
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% FSM FUNCTIONS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec init({inet:ip_address(), inet:port_number()},function(),[{atom(), any()}]) -> retval().
init(Peer, SendFun, Opts) ->
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
    send_after(?CLOSE_AFTER, timeout),
    ret({wait_for_connect, #state{peer=Peer, send_fun=SendFun,
                                  msg_log_handler=MsgLogHandler,
                                  mountpoint=string:strip(MountPoint, right, $/),
                                  username=PreAuthUser,
                                  max_client_id_size=MaxClientIdSize,
                                  retry_interval=1000 * RetryInterval
                                  }}).

-spec handle_input(binary(),{statename(), #state{}}) -> retval().
handle_input(Data, {StateName, State}) ->
    #state{buffer=Buffer} = State,
    ret(process_bytes(<<Buffer/binary, Data/binary>>, StateName, State)).

-spec handle_close({statename(),#state{}}) -> retval().
handle_close({_StateName, State}) ->
    io:format("[~p] stop due to ~p~n", [self(), tcp_closed]),
    ret({stop, normal, State}).

-spec handle_error(atom(),{statename(),#state{}}) -> retval().
handle_error(Reason, {_StateName, State}) ->
    io:format("[~p] stop due to ~p~n", [self(), Reason]),
    ret({stop, normal, State}).

-spec handle_fsm_msg(fsm_msg(), {statename(), #state{}}) -> retval().
handle_fsm_msg(timeout, {wait_for_connect, State}) ->
    io:format("[~p] stop due to timeout~n", [self()]),
    ret({stop, normal, State});
handle_fsm_msg(timeout, {connection_attempted, State}) ->
    send_after(?CLOSE_AFTER, timeout),
    ret({wait_for_connect, State});
handle_fsm_msg(timeout, {connected, State}) ->
    ret({connected, State});
handle_fsm_msg({deliver, Topic, Payload, QoS, IsRetained, IsDup,
                MsgStoreRef}, {connected, State}) ->
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
            ret({stop, normal, State1});
        {error, Reason} ->
            io:format("[~p] stop due to ~p~n", [self(), Reason]),
            ret({stop, normal, State1});
        NewState when QoS == 0 ->
            ret({connected, NewState});
        NewState when QoS > 0 ->
            Ref = send_after(RetryInterval, {retry, OutgoingMsgId}),
            ret({connected, NewState#state{
                              waiting_acks=dict:store(OutgoingMsgId,
                                                      {QoS, Frame,
                                                       Ref,
                                                       MsgStoreRef},
                                                      WAcks)}})
    end;
handle_fsm_msg({deliver_bin, {MsgId, QoS, Bin}}, {connected, State}) ->
    #state{send_fun=SendFun, waiting_acks=WAcks, retry_interval=RetryInterval} = State,
    SendFun(Bin),
    vmq_systree:incr_messages_sent(),
    Ref = send_after(RetryInterval, {retry, MsgId}),
    ret({connected, State#state{
                  waiting_acks=dict:store(MsgId,
                                          {QoS, Bin, Ref, undefined},
                                          WAcks)}});

handle_fsm_msg({retry, MessageId}, {connected, State}) ->
    #state{send_fun=SendFun, waiting_acks=WAcks, retry_interval=RetryInterval} = State,
    {QoS, Bin, _, MsgStoreRef} =
    case dict:fetch(MessageId, WAcks) of
        {_, #mqtt_frame{fixed=Fixed} = Frame, _, _} = Item ->
            NewBin = emqtt_frame:serialise(
                       Frame#mqtt_frame{
                         fixed=Fixed#mqtt_frame_fixed{
                                 dup=true
                                }}),
            setelement(2, Item, NewBin);
        Item -> Item
    end,
    SendFun(Bin),
    vmq_systree:incr_messages_sent(),
    Ref = send_after(RetryInterval, {retry, MessageId}),
    ret({connected, State#state{
                  waiting_acks=dict:store(MessageId,
                                          {QoS, Bin, Ref, MsgStoreRef},
                                          WAcks)}});
handle_fsm_msg(disconnect, {connected, State}) ->
    io:format("[~p] stop due to ~p~n", [self(), disconnect]),
    ret({stop, normal, State});

handle_fsm_msg({unhandled_transport_error, Reason}, {_, State}) ->
    io:format("[~p] stop due to ~p~n",
              [self(), {unhandled_transport_error, Reason}]),
    ret({stop, normal, State}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% INTERNALS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec process_bytes(bitstring(), statename(), #state{}) ->
    {statename(), #state{}} | {stop, normal, #state{}}.
process_bytes(Bytes, StateName, State) ->
    #state{parser_state=ParserState, keep_alive_timer=KARef} = State,
    case emqtt_frame:parse(Bytes, ParserState) of
        {more, NewParserState} ->
            {StateName, State#state{parser_state=NewParserState}};
        {ok, #mqtt_frame{fixed=Fixed, variable=Variable,
                         payload=Payload}, Rest} ->
            vmq_systree:incr_messages_received(),
            case handle_frame(StateName, Fixed, Variable,
                              Payload, State) of
                {NextStateName, #state{keep_alive=KeepAlive} = NewState} ->
                    cancel_timer(KARef),
                    PS = emqtt_frame:initial_state(),
                    process_bytes(Rest, NextStateName,
                                  NewState#state{
                                    parser_state=PS,
                                    keep_alive_timer=send_after(
                                                       KeepAlive,
                                                       disconnect)
                                   });
                Ret ->
                    Ret
            end;
        {error, Reason} ->
            io:format("parse error ~p~n", [Reason]),
            {StateName, State#state{
                          buffer= <<>>,
                          parser_state=emqtt_frame:initial_state()}}
    end.


-spec handle_frame(statename(), #mqtt_frame_fixed{}, mqtt_variable(), payload(), #state{}) ->
    {statename(), #state{}} | {stop, atom(), #state{}}.

handle_frame(wait_for_connect, _, #mqtt_frame_connect{keep_alive=KeepAlive} = Var, _, State) ->
    vmq_systree:incr_connect_received(),
    %% the client is allowed "grace" of a half a time period
    KKeepAlive = (KeepAlive + (KeepAlive div 2)) * 1000,
    check_connect(Var, State#state{keep_alive=KKeepAlive});

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
            io:format("got error~n"),
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
    Bin = emqtt_frame:serialise(PubRelFrame),
    SendFun(Bin),
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
    {connected, NewState};

handle_frame(connected, #mqtt_frame_fixed{type=?DISCONNECT}, _, _, State) ->
    {stop, normal, State}.


-spec ret({_,_} | {'stop','normal',#state{}}) -> 'stop' | {_,_}.
ret({stop, _Reason, State}) ->
    case State#state.clean_session of
        true ->
            ok;
        false ->
            handle_waiting_acks(State)
    end,
    maybe_publish_last_will(State),
    stop;
ret({_, _} = R) -> R.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% INTERNAL
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec check_connect(#mqtt_frame_connect{}, #state{}) -> retval2().
check_connect(F, State) ->
    check_client_id(F, State).

-spec check_client_id(#mqtt_frame_connect{}, #state{}) -> retval2().
check_client_id(#mqtt_frame_connect{}, #state{username={preauth, undefined}} = State) ->
    %% No common name found in client certificate
    {connection_attempted,
     send_connack(?CONNACK_CREDENTIALS, State)};
check_client_id(#mqtt_frame_connect{clean_sess=CleanSession} = F,
                #state{username={preauth, ClientId}, peer=Peer} = State) ->
    %% User preauthenticated using e.g. SSL client certificate
    case vmq_reg:register_client(ClientId, CleanSession) of
        ok ->
            vmq_hook:all(on_register, [Peer, ClientId, undefined, undefined]),
            check_will(F, State#state{clean_session=CleanSession});
        {error, _Reason} ->
            {connection_attempted,
             send_connack(?CONNACK_SERVER, State)}
    end;
check_client_id(#mqtt_frame_connect{client_id=missing}, State) ->
    {stop, normal, State};
check_client_id(#mqtt_frame_connect{client_id=empty, proto_ver=4} = F, State) ->
    RandomClientId = random_client_id(),
    check_user(F#mqtt_frame_connect{client_id=RandomClientId}, State#state{client_id=RandomClientId});
check_client_id(#mqtt_frame_connect{client_id=empty, proto_ver=3}, State) ->
    {connection_attempted,
     send_connack(?CONNACK_INVALID_ID, State)};
check_client_id(#mqtt_frame_connect{client_id=Id, proto_ver=V} = F,
                #state{max_client_id_size=S} = State) when length(Id) =< S ->
    case lists:member(V, [3,4,131]) of
        true ->
            check_user(F, State#state{client_id=Id});
        false ->
            {connection_attempted,
             send_connack(?CONNACK_PROTO_VER, State)}
    end;
check_client_id(_, State) ->
    {connection_attempted,
     send_connack(?CONNACK_INVALID_ID, State)}.

-spec check_user(#mqtt_frame_connect{}, #state{}) -> retval2().
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
                        {error, _Reason} ->
                            {connection_attempted,
                             send_connack(?CONNACK_SERVER, State)}
                    end;
                not_found ->
                    % returned when no hook on_register hook was
                    % able to authenticate user
                    {connection_attempted,
                     send_connack(?CONNACK_AUTH, State)};
                {error, invalid_credentials} ->
                    {connection_attempted,
                     send_connack(?CONNACK_CREDENTIALS, State)};
                {error, not_authorized} ->
                    {connection_attempted,
                     send_connack(?CONNACK_AUTH, State)}
            end;
        true ->
            case vmq_reg:register_client(ClientId, CleanSession) of
                ok ->
                    vmq_hook:all(on_register, [Peer, ClientId, User, Password]),
                    check_will(F, State#state{username=User});
                {error, _Reason} ->
                    {connection_attempted,
                     send_connack(?CONNACK_SERVER, State)}
            end
    end.

-spec check_will(#mqtt_frame_connect{}, #state{}) -> retval2().
check_will(#mqtt_frame_connect{will_topic=undefined}, State) ->
    {connected, send_connack(?CONNACK_ACCEPT, State)};
check_will(#mqtt_frame_connect{will_topic=""}, State) ->
    {stop, normal, State}; %% null topic
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
                    {connection_attempted,
                     send_connack(?CONNACK_SERVER, State)}
            end;
        _ ->
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
    Bin = emqtt_frame:serialise(#mqtt_frame{
                             fixed=#mqtt_frame_fixed{type=Type, dup=DUP},
                             variable=Variable,
                             payload=Payload
                            }),
    SendFun(Bin),
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
    case check_in_flight() of
        true ->
            #state{username=User, client_id=ClientId} = State,
            MsgRef = vmq_msg_store:store(User, ClientId, Topic, Payload),
            publish(User, ClientId, MsgRef, Topic, Payload, IsRetain),
            NewState = send_frame(?PUBACK, #mqtt_frame_publish{message_id=MessageId},
                                  <<>>, State),
            vmq_msg_store:deref(MsgRef),
            NewState;
        false ->
            State
    end.

-spec dispatch_publish_qos2(msg_id(), topic(), payload(), flag(), #state{}) -> #state{}.
dispatch_publish_qos2(MessageId, Topic, Payload, IsRetain, State) ->
    case check_in_flight() of
        true ->
            #state{username=User, client_id=ClientId, waiting_acks=WAcks,
                   retry_interval=RetryInterval, send_fun=SendFun} = State,
            MsgRef = vmq_msg_store:store(User, ClientId, Topic, Payload),
            Ref = send_after(RetryInterval, {retry, {qos2, MessageId}}),
            Bin = emqtt_frame:serialise(#mqtt_frame{
                                           fixed=#mqtt_frame_fixed{type=?PUBREC},
                                           variable=#mqtt_frame_publish{message_id=MessageId},
                                           payload= <<>>
                                          }),
            SendFun(Bin),
            vmq_systree:incr_messages_sent(),
            State#state{waiting_acks=dict:store(
                                       {qos2, MessageId},
                                       {2, Bin, Ref, {MsgRef, IsRetain}}, WAcks)};
        false ->
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
    Bin = emqtt_frame:serialise(Frame),
    case SendFun(Bin) of
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
    erlang:send_after(Time, self(), Msg).

-spec cancel_timer('undefined' | reference()) -> 'ok'.
cancel_timer(undefined) -> ok;
cancel_timer(TRef) -> erlang:cancel_timer(TRef), ok.

-spec check_in_flight() -> boolean().
check_in_flight() ->
    case vmq_config:get_env(max_inflight_messages, 20) of
        0 -> true;
        V ->
            vmq_msg_store:in_flight() < V
    end.

-spec valid_msg_size(binary()) -> boolean().
valid_msg_size(Payload) ->
    case vmq_config:get_env(message_size_limit, 0) of
        0 -> true;
        S when byte_size(Payload) =< S -> true;
        _ -> false
    end.
