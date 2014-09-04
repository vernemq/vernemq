-module(emqttd_fsm).
-include_lib("emqtt_commons/include/emqtt_frame.hrl").

-export([deliver/6,
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
                will_topic,
                will_msg,
                will_qos,
                waiting_acks=dict:new(),
                %% statemachine requirements
                connection_attempted=false,
                %% auth backend requirement
                peer,
                username,
                auth_providers,
                msg_log_handler

         }).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% API FUNCTIONS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
deliver(FsmPid, Topic, Payload, QoS, IsRetained, Ref) ->
    FsmPid ! {deliver, Topic, Payload, QoS, IsRetained, Ref}.

disconnect(FsmPid) ->
    FsmPid ! disconnect.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% FSM FUNCTIONS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init(Peer, SendFun, Opts) ->
    {_, AuthProviders} = lists:keyfind(auth_providers, 1, Opts),
    MsgLogHandler =
    case lists:keyfind(msg_log_handler, 1, Opts) of
        {_, undefined} ->
            fun(_,_,_) -> ok end;
        {_, Mod} when is_atom(Mod) ->
            fun(ClientId, Topic, Msg) -> apply(Mod, handle, [self(), ClientId, Topic, Msg]) end
    end,
    erlang:send_after(?CLOSE_AFTER, self(), timeout),
    ret({wait_for_connect, #state{peer=Peer, send_fun=SendFun,
                              auth_providers=AuthProviders,
                              msg_log_handler=MsgLogHandler}}).

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
handle_fsm_msg({deliver, Topic, Payload, QoS, _IsRetained, MsgStoreRef}, {connected, State}) ->
    #state{waiting_acks=WAcks} = State,
    {OutgoingMsgId, State1} = get_msg_id(QoS, State),
    Frame = #mqtt_frame{
               fixed=#mqtt_frame_fixed{
                        type=?PUBLISH,
                        qos=QoS
                       },
               variable=#mqtt_frame_publish{
                           topic_name=Topic,
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
            Ref = erlang:send_after(10000, self(), {retry, OutgoingMsgId}),
            ret({connected, NewState#state{waiting_acks=dict:store(OutgoingMsgId, {Frame, Ref, MsgStoreRef}, WAcks)}})
    end;
handle_fsm_msg({retry, MessageId}, {connected, State}) ->
    #state{send_fun=SendFun, waiting_acks=WAcks} = State,
    {Bin, _, MsgStoreRef} =
    case dict:fetch(MessageId, WAcks) of
        {#mqtt_frame{fixed=Fixed} = Frame, _, _} = Item ->
            NewBin = emqtt_frame:serialise(Frame#mqtt_frame{
                                             fixed=Fixed#mqtt_frame_fixed{dup=true}}),
            setelement(1, Item, NewBin);
        Item -> Item
    end,
    SendFun(Bin),
    Ref = erlang:send_after(10000, self(), {retry, MessageId}),
    ret({connected, State#state{
                  waiting_acks=dict:store(MessageId, {Bin, Ref, MsgStoreRef}, WAcks)}});
handle_fsm_msg(disconnect, {connected, State}) ->
    io:format("[~p] stop due to ~p~n", [self(), disconnect]),
    ret({stop, normal, State});

handle_fsm_msg({unhandled_transport_error, Reason}, {_, State}) ->
    io:format("[~p] stop due to ~p~n", [self(), {unhandled_transport_error, Reason}]),
    ret({stop, normal, State}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% INTERNALS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
process_bytes(Bytes, StateName, #state{parser_state=ParserState} = State) ->
    case emqtt_frame:parse(Bytes, ParserState) of
        {more, NewParserState} ->
            {StateName, State#state{parser_state=NewParserState}};
        {ok, #mqtt_frame{fixed=Fixed, variable=Variable, payload=Payload}, Rest} ->
            case handle_frame(StateName, Fixed, Variable, Payload, State) of
                {NextStateName, NewState} ->
                    PS = emqtt_frame:initial_state(),
                    process_bytes(Rest, NextStateName, NewState#state{parser_state=PS});
                Ret ->
                    Ret
            end;
        {error, Reason} ->
            io:format("parse error ~p~n", [Reason]),
            {StateName, State#state{buffer= <<>>,
                                    parser_state=emqtt_frame:initial_state()}}
    end.

handle_frame(wait_for_connect, _, #mqtt_frame_connect{} = Var, _, State) ->
    #state{peer=Peer, auth_providers=AuthProviders} = State,
    #mqtt_frame_connect{
       client_id=Id,
       username=User,
       password=Password,
       proto_ver=Version,
       clean_sess=CleanSession,
       will_qos=WillQoS,
       will_topic=WillTopic,
       will_msg=WillMsg} = Var,

    case check_version(Version) of
        true ->
            case auth_user(Peer, Id, User, Password, AuthProviders) of
                ok ->
                    case emqttd_reg:register_client(Id, CleanSession) of
                        ok ->
                            {connected,
                             send_connack(?CONNACK_ACCEPT, State#state{
                                                             client_id=Id,
                                                             username=User,
                                                             will_qos=WillQoS,
                                                             will_topic=WillTopic,
                                                             will_msg=WillMsg})};
                        {error, _Reason} ->
                            {connection_attempted, send_connack(?CONNACK_SERVER, State)}
                    end;
                {error, unknown} ->
                    {connection_attempted, send_connack(?CONNACK_INVALID_ID, State)};
                {error, invalid_credentials} ->
                    {connection_attempted, send_connack(?CONNACK_CREDENTIALS, State)};
                {error, not_authorized} ->
                    {connection_attempted, send_connack(?CONNACK_AUTH, State)}
            end;
        false ->
            {connection_attempted, send_connack(?CONNACK_PROTO_VER, State)}
    end;

handle_frame(connected, #mqtt_frame_fixed{type=?PUBACK}, Var, _, State) ->
    #state{waiting_acks=WAcks} = State,
    #mqtt_frame_publish{message_id=MessageId} = Var,
    %% qos1 flow
    case dict:find(MessageId, WAcks) of
        {ok, {_, Ref, MsgStoreRef}} ->
            erlang:cancel_timer(Ref),
            emqttd_msg_store:deref(MsgStoreRef),
            {connected, State#state{waiting_acks=dict:erase(MessageId, WAcks)}};
        error ->
            io:format("got error~n"),
            {connected, State}
    end;

handle_frame(connected, #mqtt_frame_fixed{type=?PUBREC}, Var, _, State) ->
    #state{waiting_acks=WAcks, send_fun=SendFun} = State,
    #mqtt_frame_publish{message_id=MessageId} = Var,
    %% qos2 flow
    {_, Ref, MsgStoreRef} = dict:fetch(MessageId, WAcks),
    erlang:cancel_timer(Ref), % cancel republish timer
    PubRelFrame =#mqtt_frame{
                             fixed=#mqtt_frame_fixed{type=?PUBREL, qos=1},
                             variable=#mqtt_frame_publish{message_id=MessageId},
                             payload= <<>>},
    Bin = emqtt_frame:serialise(PubRelFrame),
    SendFun(Bin),
    NewRef = erlang:send_after(10000, self(), {retry, MessageId}),
    emqttd_msg_store:deref(MsgStoreRef),
    {connected, State#state{
                              waiting_acks=dict:store(MessageId,
                                                      {PubRelFrame, NewRef, undefined}, WAcks)}};

handle_frame(connected, #mqtt_frame_fixed{type=?PUBREL}, Var, _, State) ->
    #state{waiting_acks=WAcks} = State,
    #mqtt_frame_publish{message_id=MessageId} = Var,
    %% qos2 flow
    {MsgRef, IsRetain} = dict:fetch({qos2, MessageId}, WAcks),
    {ok, {RoutingKey, Payload}} = emqttd_msg_store:retrieve(MsgRef),
    emqttd_reg:publish(MsgRef, RoutingKey, Payload, IsRetain),
    NewState = send_frame(?PUBCOMP, #mqtt_frame_publish{message_id=MessageId}, <<>>, State),
    {connected, NewState};

handle_frame(connected, #mqtt_frame_fixed{type=?PUBCOMP}, Var, _, State) ->
    #state{waiting_acks=WAcks} = State,
    #mqtt_frame_publish{message_id=MessageId} = Var,
    %% qos2 flow
    {_, Ref, undefined} = dict:fetch(MessageId, WAcks),
    erlang:cancel_timer(Ref), % cancel rpubrel timer
    {connected, State#state{waiting_acks=dict:erase(MessageId, WAcks)}};

handle_frame(connected, #mqtt_frame_fixed{type=?PUBLISH, retain=1}, Var, <<>>, State) ->
    #mqtt_frame_publish{topic_name=Topic, message_id=MessageId} = Var,
    %% delete retained msg,
    case emqttd_reg:publish(Topic, <<>>, true) of
        ok ->
            NewState = send_frame(?PUBACK, #mqtt_frame_publish{message_id=MessageId}, <<>>, State),
            {connected, NewState};
        {error, _Reason} ->
            %% we can't delete the retained message, due to a network split,
            %% if the client uses QoS 1 it will retry this message.
            {connected, State}
    end;

handle_frame(connected, #mqtt_frame_fixed{type=?PUBLISH, qos=QoS, retain=IsRetain}, Var, Payload, State) ->
    #mqtt_frame_publish{topic_name=Topic, message_id=MessageId} = Var,
    {connected, dispatch_publish(QoS, MessageId, Topic, Payload, IsRetain, State)};

handle_frame(connected, #mqtt_frame_fixed{type=?SUBSCRIBE}, Var, _, State) ->
    #mqtt_frame_subscribe{topic_table=Topics, message_id=MessageId} = Var,
    TTopics = [{Name, QoS} || #mqtt_topic{name=Name, qos=QoS} <- Topics],
    case emqttd_reg:subscribe(State#state.client_id, TTopics) of
        ok ->
            {_, QoSs} = lists:unzip(TTopics),
            NewState = send_frame(?SUBACK, #mqtt_frame_suback{message_id=MessageId, qos_table=QoSs}, <<>>, State),
            {connected, NewState};
        {error, _Reason} ->
            %% cant subscribe due to netsplit, Subscribe uses QoS 1 so the client will retry
            {connected, State}
    end;

handle_frame(connected, #mqtt_frame_fixed{type=?UNSUBSCRIBE}, Var, _, State) ->
    #mqtt_frame_subscribe{topic_table=Topics, message_id=MessageId} = Var,
    TTopics = [Name || #mqtt_topic{name=Name} <- Topics],
    case emqttd_reg:unsubscribe(State#state.client_id, TTopics) of
        ok ->
            NewState = send_frame(?UNSUBACK, #mqtt_frame_suback{message_id=MessageId}, <<>>, State),
            {connected, NewState};
        {error, _Reason} ->
            %% cant unsubscribe due to netsplit, Unsubscribe uses QoS 1 so the client will retry
            {connected, State}
    end;

handle_frame(connected, #mqtt_frame_fixed{type=?PINGREQ}, _, _, State) ->
    NewState = send_frame(?PINGRESP, undefined, <<>>, State),
    {connected, NewState};

handle_frame(connected, #mqtt_frame_fixed{type=?DISCONNECT}, _, _, State) ->
    {stop, normal, State}.


ret({stop, _Reason, State}) ->
    maybe_publish_last_will(State),
    stop;
ret({_, _} = R) -> R.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% INTERNAL
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

check_version(?MQTT_PROTO_MAJOR) -> true;
check_version(_) -> false.

auth_user(_, _, _, _, []) -> {error, unknown};
auth_user(SrcIp, Id, User, Password, [AuthProvider|AuthProviders]) ->
    case apply(AuthProvider, authenticate, [SrcIp, Id, User, Password]) of
        ok ->
            ok;
        {error, unknown} ->
            % loop through Auth Providers
            auth_user(SrcIp, Id, User, Password, AuthProviders);
        {error, Reason} ->
            {error, Reason}
    end.

send_connack(ReturnCode, State) ->
    send_frame(?CONNACK, #mqtt_frame_connack{return_code=ReturnCode}, <<>>, State).

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
    emqttd_reg:publish(undefined, Topic, Payload, IsRetain),
    State.

dispatch_publish_qos1(MessageId, Topic, Payload, IsRetain, State) ->
    MsgRef = emqttd_msg_store:store(Topic, Payload),
    emqttd_reg:publish(MsgRef, Topic, Payload, IsRetain),
    NewState = send_frame(?PUBACK, #mqtt_frame_publish{message_id=MessageId}, <<>>, State),
    emqttd_msg_store:deref(MsgRef),
    NewState.

dispatch_publish_qos2(MessageId, Topic, Payload, IsRetain, #state{waiting_acks=WAcks} = State) ->
    MsgRef = emqttd_msg_store:store(Topic, Payload),
    NewState = State#state{waiting_acks=dict:store({qos2, MessageId}, {MsgRef, IsRetain}, WAcks)},
    send_frame(?PUBREC, #mqtt_frame_publish{message_id=MessageId}, <<>>, NewState).

get_msg_id(0, State) ->
    {undefined, State};
get_msg_id(_, #state{next_msg_id=65535} = State) ->
    {1, State#state{next_msg_id=2}};
get_msg_id(_, #state{next_msg_id=MsgId} = State) ->
    {MsgId, State#state{next_msg_id=MsgId + 1}}.

send_publish_frame(_OutgoingMsgId, Frame, QoS, #state{send_fun=SendFun} = State) ->
    Bin = emqtt_frame:serialise(Frame),
    case SendFun(Bin) of
        ok ->
            State;
        {error, Reason} when QoS > 0 ->
            %% we cant send, process will die, store msg so we can retry when client
            %% reconnects.
            #mqtt_frame{variable=#mqtt_frame_publish{topic_name=Topic},
                        payload=Payload} = Frame,
            %% save to call even in a split brain situation
            emqttd_msg_store:persist_for_later([{State#state.client_id, QoS}], Topic, Payload),
            {error, Reason};
        {error, Reason} ->
            {error, Reason}
    end.

