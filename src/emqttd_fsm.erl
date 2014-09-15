-module(emqttd_fsm).
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
                buffer= <<>>,
                parser_state=emqtt_frame:initial_state(),
                %% networking requirements
                send_fun,
                %% mqtt layer requirements
                next_msg_id=1,
                client_id,
                max_client_id_size=23,
                will_topic,
                will_msg,
                will_qos,
                waiting_acks=dict:new(),
                %% statemachine requirements
                connection_attempted=false,
                %% auth backend requirement
                peer,
                username,
                msg_log_handler,
                mountpoint="",
                retry_interval=20000

         }).

-hook({auth_on_publish, only, 6}).
-hook({on_publish, all, 6}).
-hook({auth_on_register, only, 4}).
-hook({on_register, all, 4}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% API FUNCTIONS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
deliver(FsmPid, Topic, Payload, QoS, IsRetained, IsDup, Ref) ->
    FsmPid ! {deliver, Topic, Payload, QoS, IsRetained, IsDup, Ref}.

deliver_bin(FsmPid, Term) ->
    FsmPid ! {deliver_bin, Term}.

disconnect(FsmPid) ->
    FsmPid ! disconnect.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% FSM FUNCTIONS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init(Peer, SendFun, Opts) ->
    {_,MountPoint} = lists:keyfind(mountpoint, 1, Opts),
    {_,MaxClientIdSize} = lists:keyfind(max_client_id_size, 1, Opts),
    {_,RetryInterval} = lists:keyfind(retry_interval, 1, Opts),
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
    erlang:send_after(?CLOSE_AFTER, self(), timeout),
    ret({wait_for_connect, #state{peer=Peer, send_fun=SendFun,
                                  msg_log_handler=MsgLogHandler,
                                  mountpoint=MountPoint,
                                  max_client_id_size=MaxClientIdSize,
                                  retry_interval=1000 * RetryInterval
                                  }}).

handle_input(Data, {StateName, State}) ->
    #state{buffer=Buffer} = State,
    ret(process_bytes(<<Buffer/binary, Data/binary>>, StateName, State)).

handle_close({_StateName, State}) ->
    io:format("[~p] stop due to ~p~n", [self(), tcp_closed]),
    ret({stop, normal, State}).

handle_error(Reason, {_StateName, State}) ->
    io:format("[~p] stop due to ~p~n", [self(), Reason]),
    ret({stop, normal, State}).

handle_fsm_msg(timeout, {wait_for_connect, State}) ->
    io:format("[~p] stop due to timeout~n", [self()]),
    ret({stop, normal, State});
handle_fsm_msg(timeout, {connection_attempted, State}) ->
    erlang:send_after(?CLOSE_AFTER, self(), timeout),
    ret({wait_for_connect, State});
handle_fsm_msg(timeout, {connected, State}) ->
    ret({connected, State});
handle_fsm_msg({deliver, Topic, Payload, QoS, IsRetained, IsDup,
                MsgStoreRef}, {connected, State}) ->
    #state{waiting_acks=WAcks, mountpoint=MountPoint, retry_interval=RetryInterval} = State,
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
    case send_publish_frame(OutgoingMsgId, Frame, QoS, State1) of
        {error, Reason} ->
            io:format("[~p] stop due to ~p~n", [self(), Reason]),
            ret({stop, normal, State1});
        NewState when QoS == 0 ->
            ret({connected, NewState});
        NewState when QoS > 0 ->
            Ref = erlang:send_after(RetryInterval, self(), {retry, OutgoingMsgId}),
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
    Ref = erlang:send_after(RetryInterval, self(), {retry, MsgId}),
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
    Ref = erlang:send_after(RetryInterval, self(), {retry, MessageId}),
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
process_bytes(Bytes, StateName, State) ->
    #state{parser_state=ParserState} = State,
    case emqtt_frame:parse(Bytes, ParserState) of
        {more, NewParserState} ->
            {StateName, State#state{parser_state=NewParserState}};
        {ok, #mqtt_frame{fixed=Fixed, variable=Variable,
                         payload=Payload}, Rest} ->
            case handle_frame(StateName, Fixed, Variable,
                              Payload, State) of
                {NextStateName, NewState} ->
                    PS = emqtt_frame:initial_state(),
                    process_bytes(Rest, NextStateName,
                                  NewState#state{parser_state=PS});
                Ret ->
                    Ret
            end;
        {error, Reason} ->
            io:format("parse error ~p~n", [Reason]),
            {StateName, State#state{
                          buffer= <<>>,
                          parser_state=emqtt_frame:initial_state()}}
    end.

handle_frame(wait_for_connect, _, #mqtt_frame_connect{} = Var, _, State) ->
    #state{peer=Peer, mountpoint=MountPoint, max_client_id_size=MaxClientIdSize} = State,
    #mqtt_frame_connect{
       client_id=Id,
       username=User,
       password=Password,
       proto_ver=Version,
       clean_sess=CleanSession,
       will_qos=WillQoS,
       will_topic=WillTopic,
       will_msg=WillMsg} = Var,

    case check_version(MaxClientIdSize, Id, Version) of
        {ok, ClientId} ->
            %% auth_on_register hook must return either:
            %%  ok | next | {error, invalid_credentials | not_authorized}
            User1 = case User of "" -> undefined; _ -> User end,
            case emqttd_hook:only(auth_on_register,
                                  [Peer, ClientId, User1, Password]) of
                ok ->
                    case emqttd_reg:register_client(ClientId, CleanSession) of
                        ok ->
                            emqttd_hook:all(on_register,
                                            [Peer, ClientId, User, Password]),
                            {connected,
                             send_connack(?CONNACK_ACCEPT,
                                          State#state{
                                            client_id=ClientId,
                                            username=User1,
                                            will_qos=WillQoS,
                                            will_topic=combine_mp(MountPoint, WillTopic),
                                            will_msg=WillMsg})};
                        {error, _Reason} ->
                            {connection_attempted,
                             send_connack(?CONNACK_SERVER, State)}
                    end;
                not_found ->
                    % returned when no hook on_register hook was
                    % able to authenticate user
                    {connection_attempted,
                     send_connack(?CONNACK_INVALID_ID, State)};
                {error, invalid_credentials} ->
                    {connection_attempted,
                     send_connack(?CONNACK_CREDENTIALS, State)};
                {error, not_authorized} ->
                    {connection_attempted,
                     send_connack(?CONNACK_AUTH, State)}
            end;
        {error, dont_reply} ->
            %% TODO: maybe log
            {stop, normal, State};

        {error, ProtoErr} ->
            io:format("--- check version err ~p~n", [{Id,Version, ProtoErr}]),
            {connection_attempted,
             send_connack(ProtoErr, State)}
    end;

handle_frame(connected, #mqtt_frame_fixed{type=?PUBACK}, Var, _, State) ->
    #state{waiting_acks=WAcks} = State,
    #mqtt_frame_publish{message_id=MessageId} = Var,
    %% qos1 flow
    case dict:find(MessageId, WAcks) of
        {ok, {_, _, Ref, MsgStoreRef}} ->
            erlang:cancel_timer(Ref),
            emqttd_msg_store:deref(MsgStoreRef),
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
    erlang:cancel_timer(Ref), % cancel republish timer
    PubRelFrame =#mqtt_frame{
                             fixed=#mqtt_frame_fixed{type=?PUBREL, qos=1},
                             variable=#mqtt_frame_publish{message_id=MessageId},
                             payload= <<>>},
    Bin = emqtt_frame:serialise(PubRelFrame),
    SendFun(Bin),
    NewRef = erlang:send_after(RetryInterval, self(), {retry, MessageId}),
    emqttd_msg_store:deref(MsgStoreRef),
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
            erlang:cancel_timer(TRef),
            {ok, {RoutingKey, Payload}} = emqttd_msg_store:retrieve(MsgRef),
            publish(User, ClientId, MsgRef, RoutingKey, Payload, IsRetain),
            emqttd_msg_store:deref(MsgRef),
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
    erlang:cancel_timer(Ref), % cancel rpubrel timer
    {connected, State#state{waiting_acks=dict:erase(MessageId, WAcks)}};

handle_frame(connected, #mqtt_frame_fixed{type=?PUBLISH, retain=1},
             Var, <<>>, State) ->
    #state{username=User, client_id=ClientId, mountpoint=MountPoint} = State,
    #mqtt_frame_publish{topic_name=Topic, message_id=MessageId} = Var,
    %% delete retained msg,
    case emqttd_reg:publish(User, ClientId, undefined, combine_mp(MountPoint, Topic), <<>>, true) of
        ok ->
            NewState = send_frame(?PUBACK,
                                  #mqtt_frame_publish{message_id=MessageId},
                                  <<>>, State),
            {connected, NewState};
        {error, _Reason} ->
            %% we can't delete the retained message, due to a network split,
            %% if the client uses QoS 1 it will retry this message.
            {connected, State}
    end;

handle_frame(connected, #mqtt_frame_fixed{type=?PUBLISH,
                                          qos=QoS,
                                          retain=IsRetain},
             Var, Payload, State) ->
    #state{mountpoint=MountPoint} = State,
    #mqtt_frame_publish{topic_name=Topic, message_id=MessageId} = Var,
    {connected, dispatch_publish(QoS, MessageId, combine_mp(MountPoint, Topic), Payload,
                                 IsRetain, State)};

handle_frame(connected, #mqtt_frame_fixed{type=?SUBSCRIBE}, Var, _, State) ->
    #state{client_id=Id, username=User, mountpoint=MountPoint} = State,
    #mqtt_frame_subscribe{topic_table=Topics, message_id=MessageId} = Var,
    TTopics = [{combine_mp(MountPoint, Name), QoS} || #mqtt_topic{name=Name, qos=QoS} <- Topics],
    case emqttd_reg:subscribe(User, Id, TTopics) of
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
    case emqttd_reg:unsubscribe(User, Id, TTopics) of
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


ret({stop, _Reason, State}) ->
    handle_waiting_acks(State),
    maybe_publish_last_will(State),
    stop;
ret({_, _} = R) -> R.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% INTERNAL
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
check_version(_, missing, _) -> {error, dont_reply};
check_version(_, empty, 4) -> {ok, random_client_id()};
check_version(_, empty, 3) -> {error, ?CONNACK_INVALID_ID};
check_version(MaxClientIdSize, Id, Version) when length(Id) =< MaxClientIdSize ->
    case {Id, Version} of
        {_, 4} -> {ok, Id};
        {_, 3} -> {ok, Id};
        _ -> {error, ?CONNACK_PROTO_VER}
    end;
check_version(_,_,_) -> %% invalid size
    {error, ?CONNACK_INVALID_ID}.

send_connack(ReturnCode, State) ->
    send_frame(?CONNACK, #mqtt_frame_connack{return_code=ReturnCode},
               <<>>, State).

send_frame(Type, Variable, Payload, State) ->
    send_frame(Type, false, Variable, Payload, State).

send_frame(Type, DUP, Variable, Payload, #state{send_fun=SendFun} = State) ->
    Bin = emqtt_frame:serialise(#mqtt_frame{
                             fixed=#mqtt_frame_fixed{type=Type, dup=DUP},
                             variable=Variable,
                             payload=Payload
                            }),
    SendFun(Bin),
    State.


maybe_publish_last_will(#state{will_topic=undefined} = State) -> State;
maybe_publish_last_will(#state{will_qos=QoS, will_topic=Topic,
                               will_msg=Msg } = State) ->
    {MsgId, NewState} = get_msg_id(QoS, State),
    dispatch_publish(QoS, MsgId, Topic, Msg, false, NewState).


dispatch_publish(Qos, MessageId, Topic, Payload, IsRetain, State) ->
    #state{client_id=ClientId, msg_log_handler=MsgLogHandler} = State,
    MsgLogHandler(ClientId, Topic, Payload),
    dispatch_publish_(Qos, MessageId, Topic, Payload, IsRetain, State).

dispatch_publish_(0, MessageId, Topic, Payload, IsRetain, State) ->
    dispatch_publish_qos0(MessageId, Topic, Payload, IsRetain, State);
dispatch_publish_(1, MessageId, Topic, Payload, IsRetain, State) ->
    dispatch_publish_qos1(MessageId, Topic, Payload, IsRetain, State);
dispatch_publish_(2, MessageId, Topic, Payload, IsRetain, State) ->
    dispatch_publish_qos2(MessageId, Topic, Payload, IsRetain, State).

dispatch_publish_qos0(_MessageId, Topic, Payload, IsRetain, State) ->
    #state{username=User, client_id=ClientId} = State,
    publish(User, ClientId, undefined, Topic, Payload, IsRetain),
    State.

dispatch_publish_qos1(MessageId, Topic, Payload, IsRetain, State) ->
    #state{username=User, client_id=ClientId} = State,
    MsgRef = emqttd_msg_store:store(User, ClientId, Topic, Payload),
    publish(User, ClientId, MsgRef, Topic, Payload, IsRetain),
    NewState = send_frame(?PUBACK, #mqtt_frame_publish{message_id=MessageId},
                          <<>>, State),
    emqttd_msg_store:deref(MsgRef),
    NewState.

dispatch_publish_qos2(MessageId, Topic, Payload, IsRetain, State) ->
    #state{username=User, client_id=ClientId, waiting_acks=WAcks,
           retry_interval=RetryInterval, send_fun=SendFun} = State,
    MsgRef = emqttd_msg_store:store(User, ClientId, Topic, Payload),
    Ref = erlang:send_after(RetryInterval, self(), {retry, {qos2, MessageId}}),
    Bin = emqtt_frame:serialise(#mqtt_frame{
                             fixed=#mqtt_frame_fixed{type=?PUBREC},
                             variable=#mqtt_frame_publish{message_id=MessageId},
                             payload= <<>>
                            }),
    SendFun(Bin),
    State#state{waiting_acks=dict:store(
                               {qos2, MessageId},
                               {2, Bin, Ref, {MsgRef, IsRetain}}, WAcks)}.

get_msg_id(0, State) ->
    {undefined, State};
get_msg_id(_, #state{next_msg_id=65535} = State) ->
    {1, State#state{next_msg_id=2}};
get_msg_id(_, #state{next_msg_id=MsgId} = State) ->
    {MsgId, State#state{next_msg_id=MsgId + 1}}.

send_publish_frame(_OutgoingMsgId, Frame, QoS, State) ->
    #state{send_fun=SendFun} = State,
    Bin = emqtt_frame:serialise(Frame),
    case SendFun(Bin) of
        ok ->
            State;
        {error, Reason} when QoS > 0 ->
            %% we cant send, process will die, store msg so we can
            %% retry when client reconnects.
            #mqtt_frame{variable=#mqtt_frame_publish{topic_name=Topic},
                        payload=Payload} = Frame,
            %% save to call even in a split brain situation
            emqttd_msg_store:persist_for_later([{State#state.client_id, QoS}],
                                               Topic, Payload),
            {error, Reason};
        {error, Reason} ->
            {error, Reason}
    end.

publish(User, ClientId, MsgRef, Topic, Payload, IsRetain) ->
    %% auth_on_publish hook must return either:
    %% next | ok
    case emqttd_hook:only(auth_on_publish, [User, ClientId, MsgRef, Topic,
                                            Payload, IsRetain]) of
        not_found ->
            {error, not_allowed};
        ok ->
            R = emqttd_reg:publish(User, ClientId, MsgRef, Topic,
                                   Payload, IsRetain),
            emqttd_hook:all(on_publish, [User, ClientId, MsgRef,
                                         Topic, Payload, IsRetain]),
            R
    end.

combine_mp("", Topic) -> Topic;
combine_mp(MountPoint, Topic) ->
    lists:flatten([MountPoint, "/", string:strip(Topic, left, $/)]).

clean_mp("", Topic) -> Topic;
clean_mp(MountPoint, MountedTopic) ->
    lists:sublist(MountedTopic, length(MountPoint) + 1, length(MountedTopic)).

random_client_id() ->
    lists:flatten(["anon-", base64:encode_to_string(crypto:rand_bytes(20))]).

handle_waiting_acks(State) ->
    #state{client_id=ClientId, waiting_acks=WAcks} = State,
    dict:fold(fun ({qos2, _}, _, Acc) ->
                      Acc;
                  (MsgId, {QoS, #mqtt_frame{fixed=Fixed} = Frame, TRef, undefined}, Acc) ->
                      %% unacked PUBREL Frame
                      erlang:cancel_timer(TRef),
                      Bin = emqtt_frame:serialise(
                              Frame#mqtt_frame{
                                fixed=Fixed#mqtt_frame_fixed{
                                        dup=true}}),
                      emqttd_msg_store:defer_deliver_uncached(ClientId, {MsgId, QoS, Bin}),
                      Acc;
                  (MsgId, {QoS, Bin, TRef, undefined}, Acc) ->
                      erlang:cancel_timer(TRef),
                      emqttd_msg_store:defer_deliver_uncached(ClientId, {MsgId, QoS, Bin}),
                      Acc;
                  (_MsgId, {QoS, _Frame, TRef, MsgStoreRef}, Acc) ->
                      erlang:cancel_timer(TRef),
                      emqttd_msg_store:defer_deliver(ClientId, QoS, MsgStoreRef, true),
                      Acc
              end, [], WAcks).
