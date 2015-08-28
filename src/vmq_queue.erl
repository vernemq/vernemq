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

-module(vmq_queue).
-include("vmq_server.hrl").

-behaviour(gen_fsm).

%% API functions
-export([start_link/1,
         active/1,
         notify/1,
         enqueue/2,
         status/1,
         add_session/4,
         get_sessions/1,
         set_opts/2,
         set_last_waiting_acks/2,
         enqueue_many/2,
         migrate/2]).

-export([online/2, online/3,
         offline/2, offline/3,
         wait_for_offline/2, wait_for_offline/3,
         drain/2, drain/3]).

%% gen_server callbacks
-export([init/1,
         handle_sync_event/4,
         handle_event/3,
         handle_info/3,
         terminate/3,
         code_change/4]).


-record(queue, {
          queue = queue:new(),
          type = fifo,
          max,
          size = 0,
          drop = 0
         }).

-record(session, {
          pid,
          clean,
          status = notify,
          queue = #queue{}
         }).

-record(state, {
          id,
          offline = #queue{},
          deliver_mode = fanout,
          sessions = maps:new(),
          expiry_timer,
          drain_time,
          drain_over_timer,
          waiting_call
         }).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(SubscriberId) ->
    gen_fsm:start_link(?MODULE, [SubscriberId], []).


active(Queue) when is_pid(Queue) ->
    gen_fsm:send_event(Queue, {change_state, active, self()}).

notify(Queue) when is_pid(Queue) ->
    gen_fsm:send_event(Queue, {change_state, notify, self()}).

enqueue(Queue, Msg) when is_pid(Queue) ->
    gen_fsm:send_event(Queue, {enqueue, Msg}).

enqueue_many(Queue, Msgs) when is_pid(Queue) and is_list(Msgs) ->
    gen_fsm:send_event(Queue, {enqueue_many, Msgs}).

add_session(Queue, SessionPid, Clean, AllowMultiple) when is_pid(Queue) ->
    gen_fsm:sync_send_event(Queue, {add_session, SessionPid, Clean, AllowMultiple}, infinity).

get_sessions(Queue) when is_pid(Queue) ->
    gen_fsm:sync_send_all_state_event(Queue, get_sessions, infinity).

set_opts(Queue, Opts) when is_pid(Queue) ->
    gen_fsm:sync_send_event(Queue, {set_opts, self(), Opts}, infinity).

set_last_waiting_acks(Queue, WAcks) ->
    gen_fsm:sync_send_event(Queue, {set_last_waiting_acks, WAcks}, infinity).

migrate(Queue, OtherQueue) ->
    gen_fsm:sync_send_event(Queue, {migrate, OtherQueue}, infinity).

status(Queue) ->
    gen_fsm:sync_send_all_state_event(Queue, status, infinity).

%%%===================================================================
%%% gen_fsm state callbacks
%%%===================================================================
online({change_state, NewSessionState, SessionPid}, State) ->
    {next_state, online, change_session_state(NewSessionState, SessionPid, State)};
online({enqueue, Msg}, State) ->
    {next_state, online, insert(Msg, State)};
online({enqueue_many, Msgs}, State) ->
    {next_state, online, insert_many(Msgs, State)};

online(Event, State) ->
    lager:error("got unknown event in online state ~p", [Event]),
    {next_state, online, State}.

online({set_opts, SessionPid, Opts}, _From, #state{sessions=Sessions} = State) ->
    DeliverMode = proplists:get_value(deliver_mode, Opts, fanout),
    MaxQueuedMessages = proplists:get_value(max_queued_messages, Opts,
                                            vmq_config:get_env(max_queued_messages)),
    #session{queue=Queue} = Session = maps:get(SessionPid, Sessions),
    NewSessions = maps:update(SessionPid,
                              Session#session{
                                queue=Queue#queue{
                                        max=MaxQueuedMessages
                                       }
                               }, Sessions),
    {reply, ok, online, State#state{deliver_mode=DeliverMode, sessions=NewSessions}};
online({add_session, SessionPid, Clean, true}, _From, State) ->
    %% allow multiple sessions per queue
    {reply, ok, online, unset_expiry_timer(add_session(SessionPid, Clean, State))};
online({add_session, SessionPid, Clean, false}, From, State)
  when State#state.waiting_call == undefined ->
    %% forbid multiple sessions per queue,
    %% we've to disconnect currently attached sessions
    %% and wait with the reply until all the sessions
    %% have been disconnected
    disconnect_sessions(State),
    {next_state, state_change(add_session, online, wait_for_offline),
     State#state{waiting_call={add_session, SessionPid, Clean, From}}};
online({migrate, OtherQueue}, From, State)
  when State#state.waiting_call == undefined ->
    disconnect_sessions(State),
    {next_state, state_change(migrate, online, wait_for_offline),
     State#state{waiting_call={migrate, OtherQueue, From}}};
online({set_last_waiting_acks, WAcks}, _From, State) ->
    {reply, ok, online, handle_waiting_acks_and_msgs(WAcks, State)};
online(Event, _From, State) ->
    lager:error("got unknown sync event in online state ~p", [Event]),
    {reply, {error, online}, State}.

wait_for_offline({enqueue, Msg}, #state{offline=Offline, id=SId} = State) ->
    %% enqueue this message directly into the offline queue
    {next_state, wait_for_offline,
     State#state{offline=queue_insert(Msg, Offline, SId)}};
wait_for_offline(Event, State) ->
    lager:error("got unknown event in wait_for_offline state ~p", [Event]),
    {next_state, wait_for_offline, State}.

wait_for_offline({set_last_waiting_acks, WAcks}, _From, State) ->
    {reply, ok, wait_for_offline, handle_waiting_acks_and_msgs(WAcks, State)};
wait_for_offline(Event, _From, State) ->
    lager:error("got unknown sync event in wait_for_offline state ~p", [Event]),
    {reply, {error, wait_for_offline}, wait_for_offline, State}.


drain(drain_start, #state{offline=#queue{queue=Q} = Queue,
                          drain_time=DrainTimeout,
                          waiting_call={migrate, RemoteQueue, From}} = State) ->
    Msgs = queue:to_list(Q),
    %% remote_enqueue triggers an enqueue_many inside the remote queue
    %% but forces the traffic to go over the distinct communication link
    %% instead of the erlang distribution link.
    vmq_cluster:remote_enqueue(node(RemoteQueue), {enqueue, RemoteQueue, Msgs}),
    case status(RemoteQueue) of
        {RemoteState, _} when (RemoteState == offline)
                              or (RemoteState == online) ->
            %% the extra timeout gives the chance that pending messages
            %% in the erlang mailbox could still get enqueued and
            %% therefore eventually transmitted to the remote queue
            {next_state, drain,
             State#state{drain_over_timer=gen_fsm:send_event_after(DrainTimeout, drain_over),
                         offline=Queue#queue{size=0, drop=0,
                                             queue=queue:new()}}};
        {OtherRemoteState, _} ->
            %% this shouldn't happen, as the register_subsciber is synchronized
            %% using the vmq_reg_leader process. However this could theoretically
            %% happen in case of an inconsistent (but un-detected) cluster state.
            %% we don't drain in this case.
            lager:error("wrong remote state '~p' during drain for [~p][~p]",
                          [OtherRemoteState, self(), RemoteQueue]),
            gen_fsm:reply(From, ok),
            {stop, normal, State#state{waiting_call=undefined}}
    end;
drain({enqueue, Msg}, #state{drain_over_timer=TRef} =  State) ->
    %% even in drain state it is possible that an enqueue message
    %% reaches this process, so we've to queue this message otherwise
    %% it would be lost.
    gen_fsm:cancel_timer(TRef),
    gen_fsm:send_event(self(), drain_start),
    {next_state, drain, insert(Msg, State)};
drain(drain_over, #state{waiting_call={migrate, _, From}} =
      #state{offline=#queue{size=0}} = State) ->
    %% we're done with the migrate, offline queue is empty
    gen_fsm:reply(From, ok),
    {stop, normal, State};
drain(drain_over, State) ->
    %% we still have undrained messages in the offline queue
    gen_fsm:send_event(self(), drain_start),
    {next_state, drain, State};
drain(Event, State) ->
    lager:error("got unknown event in drain state ~p", [Event]),
    {next_state, drain, State}.

drain(Event, _From, State) ->
    lager:error("got unknown sync event in drain state ~p", [Event]),
    {reply, {error, draining}, drain, State}.


offline({enqueue, Msg}, State) ->
    %% storing the message in the offline queue
    {next_state, offline, insert(Msg, State)};
offline({enqueue_many, Msgs}, State) ->
    {next_state, offline, insert_many(Msgs, State)};
offline(expire_session, #state{id=SId, offline=#queue{queue=Q}} = State) ->
    %% session has expired cleanup and go down
    vmq_reg:delete_subscriptions(SId),
    cleanup_queue(SId, queue:out(Q)),
    {stop, normal, State};
offline(Event, State) ->
    lager:error("got unknown event in offline state ~p", [Event]),
    {next_state, offline, State}.

offline({add_session, SessionPid, Clean, _}, _From, State) ->
    {reply, ok, state_change(add_session, offline, online),
     unset_expiry_timer(add_session(SessionPid, Clean, State))};
offline({migrate, OtherQueue}, From, State) ->
    gen_fsm:send_event(self(), drain_start),
    {next_state, state_change(migrate, offline, drain),
     State#state{waiting_call={migrate, OtherQueue, From}}};
offline(Event, _From, State) ->
    lager:error("got unknown sync event in offline state ~p", [Event]),
    {reply, {error, offline}, offline, State}.


%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

init([SubscriberId]) ->
    MaxOfflineMsgs = vmq_config:get_env(max_offline_messages),
    DeliverMode = vmq_config:get_env(queue_deliver_mode, fanout),
    QueueType = vmq_config:get_env(queue_type, fifo),
    DrainTime = vmq_config:get_env(max_drain_time, 100),
    OfflineQueue = #queue{type=QueueType, max=MaxOfflineMsgs},
    {ok, offline,  #state{id=SubscriberId,
                          offline=OfflineQueue,
                          drain_time=DrainTime,
                          deliver_mode=DeliverMode}}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(status, _From, StateName,
                  #state{offline=#queue{size=OfflineSize}, sessions=Sessions} = State) ->
    TotalStoredMsgs =
    maps:fold(fun(_, #session{queue=#queue{size=Size}}, Acc) ->
                      Acc + Size
              end, OfflineSize, Sessions),
    {reply, {StateName, TotalStoredMsgs}, StateName, State};
handle_sync_event(get_sessions, _From, StateName, #state{sessions=Sessions} = State) ->
    {reply, maps:keys(Sessions), StateName, State};

handle_sync_event(Event, _From, _StateName, State) ->
    {stop, {error, {unknown_sync_event, Event}}, State}.

handle_info({'DOWN', _MRef, process, SessionPid, _}, StateName,
            #state{id=SId, waiting_call=WaitingCall} = State) ->
    {NewState, DeletedSession} = del_session(SessionPid, State),
    case {maps:size(NewState#state.sessions), StateName, WaitingCall} of
        {0, wait_for_offline, {add_session, NewSessionPid, Clean, From}} ->
            %% last session gone
            %% ... but we've a new session waiting
            %%     no need to go into offline state
            gen_fsm:reply(From, ok),
            {next_state, state_change({'DOWN', add_session}, wait_for_offline, online),
             add_session(NewSessionPid, Clean, NewState#state{waiting_call=undefined})};
        {0, wait_for_offline, {migrate, _, _}} ->
            %% last session gone
            %% ... but we've a migrate request waiting
            %%     go into drain state
            gen_fsm:send_event(self(), drain_start),
            {next_state, state_change({'DOWN', migrate}, wait_for_offline, drain), NewState};
        {0, _, _} when DeletedSession#session.clean ->
            %% last session gone
            %% ... we've to cleanup and go down
            %%
            %% it is assumed that all attached sessions use the same
            %% clean session flag
            vmq_reg:delete_subscriptions(SId),
            {stop, normal, NewState};
        {0, OldStateName, _} ->
            %% last session gone
            %% ... we've to stay around and store the messages
            %%     inside the offline queue
            {next_state, state_change('DOWN', OldStateName, offline),
             maybe_set_expiry_timer(NewState)};
        _ ->
            %% still one or more sessions online
            {next_state, StateName, NewState}
    end;
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
add_session(SessionPid, Clean, #state{offline=Offline, sessions=Sessions} = State) ->
    NewSessions =
    case maps:get(SessionPid, Sessions, not_found) of
        not_found ->
            monitor(process, SessionPid),
            MaxQueuedMessages = vmq_config:get_env(max_queued_messages),
            maps:put(SessionPid,
                     #session{pid=SessionPid, clean=Clean,
                              queue=#queue{max=MaxQueuedMessages}}, Sessions);
        _ ->
            Sessions
    end,
    insert_from_queue(
      queue:out(Offline#queue.queue), State#state{ offline=Offline#queue{
                                                             size=0, drop=0,
                                                             queue = queue:new()
                                                            },
                                                   sessions=NewSessions}).

del_session(SessionPid, #state{id=SId, sessions=Sessions} = State) ->
    NewSessions = maps:remove(SessionPid, Sessions),
    case maps:get(SessionPid, Sessions) of
        #session{clean=true} = Session ->
            cleanup_session(SId, Session),
            {State#state{sessions=NewSessions}, Session};
        Session ->
            %% give queue content of this session to other alive sessions
            %% or to offline queue
            {insert_from_session(Session, State#state{sessions=NewSessions}),
             Session}
    end.

handle_waiting_acks_and_msgs(WAcks, #state{id=SId, sessions=Sessions, offline=Offline} = State) ->
    %% we can only handle the last waiting acks and msgs if this is
    %% the last session active for this queue.
    case maps:size(Sessions) of
        1 ->
            %% this is the last active session
            NewOfflineQueue =
            lists:foldl(fun(Msg, AccOffline) ->
                                queue_insert(Msg, AccOffline, SId)
                        end, Offline, WAcks),
            lager:info("handle waiting acks ~p", [NewOfflineQueue#queue.size]),
            State#state{offline=NewOfflineQueue};
        N ->
            lager:error("handle waiting acks for multiple sessions (~p) not possible", [N]),
            %% it doesn't make sense to keep the waiting acks around
            %% however depending on the balancing strategy it would
            %% make sense to re-enqueue messages for other active
            %% sessions --> TODO
            State
    end.

disconnect_sessions(#state{sessions=Sessions}) ->
    maps:fold(fun(SessionPid, #session{}, _) ->
                      %% before the session is going to die it
                      %% will send out LWT messages and will give
                      %% us back the waiting acks and messages
                      %% calling set_last_waiting_acks/2
                      %% then the 'DOWN' message gets triggerd
                      %% finally deleting the session
                      vmq_session:disconnect(SessionPid)
              end, ok, Sessions).

change_session_state(NewState, SessionPid, #state{sessions=Sessions} = State) ->

    Session = maps:get(SessionPid, Sessions),
    UpdatedSession = change_session_state(NewState, Session),
    NewSessions = maps:update(SessionPid, UpdatedSession, Sessions),
    State#state{sessions=NewSessions}.

%% in active state
change_session_state(active, #session{status=active} = Session) ->
    Session;
change_session_state(notify, #session{status=active} = Session) ->
    Session#session{status=notify};

%% in passive state
change_session_state(notify, #session{status=passive, queue=#queue{size=0}} = Session) ->
    Session#session{status=notify};
change_session_state(notify, #session{status=passive} = Session) ->
    send_notification(Session);
change_session_state(active, #session{status=passive, queue=#queue{size=0}} = Session) ->
    Session#session{status=active};
change_session_state(active, #session{status=passive} = Session) ->
    send(Session);

%% in notify state
change_session_state(active, #session{status=notify, queue=#queue{size=0}} = Session) ->
    Session#session{status=active};
change_session_state(active, #session{status=notify} = Session) ->
    send(Session);
change_session_state(notify, #session{status=notify} = Session) ->
    Session.

insert_from_session(#session{queue=#queue{queue=Q}},
                    #state{deliver_mode=fanout, sessions=Sessions} = State)
  when Sessions == #{} ->
    %% all will go into offline queue
    insert_from_queue(queue:out(Q), State);
insert_from_session(_, #state{deliver_mode=fanout} = State) ->
    %% due to fanout other sessions have already received the messages
    State;
insert_from_session(#session{queue=#queue{queue=Q}},
                    #state{deliver_mode=balance} = State) ->
    %% allow other sessions to balance the messages of the dead queue
    insert_from_queue(queue:out(Q), State).

insert_from_queue({{value, Msg}, Q}, State) ->
    insert_from_queue(queue:out(Q), insert(Msg, State));
insert_from_queue({empty, _}, State) ->
    State.

insert_many(Msgs, State) ->
    lists:foldl(fun(Msg, AccState) ->
                        insert(Msg, AccState)
                end, State, Msgs).

%% Offline Queue
insert({deliver, 0, _}, #state{offline=#queue{drop=Drop}, sessions=Sessions} = State)
  when Sessions == #{} ->
    %% no session online, drop
    State#state{offline=Drop#queue{drop=Drop + 1}};
insert(Msg, #state{id=SId, offline=Offline, sessions=Sessions} = State)
  when Sessions == #{} ->
    %% no session online, insert in offline queue
    State#state{offline=queue_insert(Msg, Offline, SId)};

%% Online Queue
insert(Msg, #state{id=SId, deliver_mode=fanout, sessions=Sessions} = State) ->
    {NewSessions, _} = mapfold(fun session_insert/2, {Msg, SId}, Sessions),
    State#state{sessions=NewSessions};

insert(Msg, #state{id=SId, deliver_mode=balance, sessions=Sessions} = State) ->
    Keys = maps:keys(Sessions),
    RandomKey = lists:nth(random:uniform(length(Keys)), Keys),
    RandomSession = maps:get(RandomKey, Sessions),
    {UpdatedSession, _} = session_insert(RandomSession, {Msg, SId}),
    State#state{sessions=maps:update(RandomKey, UpdatedSession, Sessions)}.


session_insert(#session{status=active, queue=Q} = Session, {Msg, SId} = Acc) ->
    {send(Session#session{queue=queue_insert(Msg, Q, SId)}), Acc};
session_insert(#session{status=passive, queue=Q} = Session, {Msg, SId} = Acc) ->
    {Session#session{queue=queue_insert(Msg, Q, SId)}, Acc};
session_insert(#session{status=notify, queue=Q} = Session, {Msg, SId} = Acc) ->
    {send_notification(Session#session{queue=queue_insert(Msg, Q, SId)}), Acc}.

%% unlimited messages accepted
queue_insert(Msg, #queue{max=0, size=Size, queue=Queue} = Q, _) ->
    Q#queue{queue=queue:in(Msg, Queue), size=Size + 1};
%% tail drop in case of fifo
queue_insert(Msg, #queue{type=fifo, max=Max, size=Max, drop=Drop} = Q, SId) ->
    case Msg of
        {deliver, QoS, #vmq_msg{msg_ref=MsgRef}} when QoS > 0 ->
            _ = vmq_msg_store:deref(SId, MsgRef);
        _ ->
            %% {deliver, 0, _} messages no need to deref,
            %% {deliver_bin, _} messages no need to deref
            ignore
    end,
    Q#queue{drop=Drop + 1};
%% drop oldest in case of lifo
queue_insert(Msg, #queue{type=lifo, max=Max, size=Max, queue=Queue, drop=Drop} = Q, SId) ->
    NewNewQueue =
    case queue:out(Queue) of
        {{value, {deliver, QoS, #vmq_msg{msg_ref=MsgRef}}}, NewQueue} when QoS > 0 ->
            _ = vmq_msg_store:deref(SId, MsgRef),
            NewQueue;
        {{value, _}, NewQueue} ->
            %% {deliver, 0, _} messages no need to deref,
            %% {deliver_bin, _} messages no need to deref
            NewQueue
    end,
    Q#queue{queue=queue:in(Msg, NewNewQueue), drop=Drop + 1};
%% normal enqueue
queue_insert(Msg, #queue{queue=Queue, size=Size} = Q, _) ->
    Q#queue{queue=queue:in(Msg, Queue), size=Size + 1}.

send(#session{pid=Pid, queue=Q} = Session) ->
    Session#session{status=passive, queue=send(Pid, Q)}.

send(Pid, #queue{queue=Queue, size=Count, drop=Dropped} = Q) ->
    Msgs = queue:to_list(Queue),
    Pid ! {mail, self(), Msgs, Count, Dropped},
    Q#queue{queue=queue:new(), size=0, drop=0}.

send_notification(#session{pid=Pid} = Session) ->
    Pid ! {mail, self(), new_data},
    Session#session{status=passive}.

cleanup_session(SubscriberId, #session{queue=#queue{queue=Q}}) ->
    cleanup_queue(SubscriberId, queue:out(Q)).

cleanup_queue(SId, {{value, {deliver, 0, _}}, NewQueue}) ->
    % no need to deref
    cleanup_queue(SId, queue:out(NewQueue));
cleanup_queue(SId, {{value, {deliver, _, #vmq_msg{msg_ref=MsgRef}}}, NewQueue}) ->
    % need to deref
    _ = vmq_msg_store:deref(SId, MsgRef),
    cleanup_queue(SId, queue:out(NewQueue));
cleanup_queue(_, {empty, _}) -> ok.


mapfold(Fun, Acc, Map) ->
    mapfold(Fun, Acc, Map, maps:keys(Map)).

mapfold(Fun, Acc, Map, [K|Rest]) ->
    {NewV, NewAcc} = Fun(maps:get(K, Map), Acc),
    mapfold(Fun, NewAcc, maps:update(K, NewV, Map), Rest);
mapfold(_, Acc, Map, []) ->
    {Map, Acc}.

maybe_set_expiry_timer(#state{sessions=Sessions} = State) when Sessions == #{} ->
    Duration = vmq_config:get_env(persistent_client_expiration, 0),
    maybe_set_expiry_timer(Duration, State);
maybe_set_expiry_timer(State) -> State.

maybe_set_expiry_timer(0, State) ->
    %% never expire
    State;
maybe_set_expiry_timer(ExpireAfter, State) when ExpireAfter > 0 ->
    Ref = erlang:send_after(ExpireAfter * 1000, self(), expire_session),
    State#state{expiry_timer=Ref}.

unset_expiry_timer(#state{expiry_timer=undefined} = State) -> State;
unset_expiry_timer(#state{expiry_timer=Ref} = State) ->
    erlang:cancel_timer(Ref),
    State#state{expiry_timer=undefined}.


state_change(Msg, OldStateName, NewStateName) ->
    lager:debug("[~p] transition from ~p --> ~p because of ~p", [self(), OldStateName, NewStateName, Msg]),
   NewStateName.
