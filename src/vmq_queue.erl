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
-export([start_link/2,
         active/1,
         notify/1,
         notify_recv/1,
         enqueue/2,
         status/1,
         migration_status/1,
         block_until_migrated/1,
         add_session/3,
         get_sessions/1,
         set_opts/2,
         get_opts/1,
         default_opts/0,
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
          backup = queue:new(),
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
          max_msgs_per_drain_step,
          waiting_call,
          migrations = [],
          opts
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
start_link(SubscriberId, Clean) ->
    gen_fsm:start_link(?MODULE, [SubscriberId, Clean], []).


active(Queue) when is_pid(Queue) ->
    gen_fsm:send_event(Queue, {change_state, active, self()}).

notify(Queue) when is_pid(Queue) ->
    gen_fsm:send_event(Queue, {change_state, notify, self()}).

notify_recv(Queue) when is_pid(Queue) ->
    gen_fsm:send_event(Queue, {notify_recv, self()}).

enqueue(Queue, Msg) when is_pid(Queue) ->
    gen_fsm:send_event(Queue, {enqueue, Msg}).

enqueue_many(Queue, Msgs) when is_pid(Queue) and is_list(Msgs) ->
    gen_fsm:sync_send_event(Queue, {enqueue_many, Msgs}, infinity).

add_session(Queue, SessionPid, Opts) when is_pid(Queue) ->
    gen_fsm:sync_send_event(Queue, {add_session, SessionPid, Opts}, infinity).

get_sessions(Queue) when is_pid(Queue) ->
    gen_fsm:sync_send_all_state_event(Queue, get_sessions, infinity).

set_opts(Queue, Opts) when is_pid(Queue) ->
    gen_fsm:sync_send_event(Queue, {set_opts, self(), Opts}, infinity).

get_opts(Queue) when is_pid(Queue) ->
    gen_fsm:sync_send_all_state_event(Queue, get_opts, infinity).

set_last_waiting_acks(Queue, WAcks) ->
    gen_fsm:sync_send_event(Queue, {set_last_waiting_acks, WAcks}, infinity).

migrate(Queue, OtherQueue) ->
    %% Migrate Messages of 'Queue' to 'OtherQueue'
    %% -------------------------------------------

    %% inform 'OtherQueue' about the migration process,
    %% see 'block_until_migrated/1' how the status information is used.
    gen_fsm:send_all_state_event(OtherQueue, {migrate_info, Queue}),

    case catch gen_fsm:sync_send_event(Queue, {migrate, OtherQueue}, infinity) of
        {'EXIT', {normal, _}} ->
            ok;
        {'EXIT', {noproc, _}} ->
            ok;
        {'EXIT', Reason} ->
            exit(Reason);
        Ret ->
            Ret
    end.

status(Queue) ->
    gen_fsm:sync_send_all_state_event(Queue, status, infinity).

migration_status(Queue) ->
    gen_fsm:sync_send_all_state_event(Queue, migration_status, infinity).

block_until_migrated(Queue) ->
    timer:sleep(100),
    case migration_status(Queue) of
        [] ->
            ok;
        _ ->
            block_until_migrated(Queue)
    end.


default_opts() ->
    #{allow_multiple_sessions => vmq_config:get_env(allow_multiple_sessions),
      max_online_messages => vmq_config:get_env(max_online_messages),
      max_offline_messages => vmq_config:get_env(max_offline_messages),
      queue_deliver_mode => vmq_config:get_env(queue_deliver_mode),
      queue_type => vmq_config:get_env(queue_type),
      max_drain_time => vmq_config:get_env(max_drain_time),
      max_msgs_per_drain_step => vmq_config:get_env(max_msgs_per_drain_step),
      is_plugin => false}.

%%%===================================================================
%%% gen_fsm state callbacks
%%%===================================================================
online({change_state, NewSessionState, SessionPid}, State) ->
    {next_state, online, change_session_state(NewSessionState, SessionPid, State)};
online({notify_recv, SessionPid}, #state{id=SId, sessions=Sessions} = State) ->
    #session{queue=#queue{backup=Backup} = Queue} = Session = maps:get(SessionPid, Sessions),
    cleanup_queue(SId, Backup),
    NewSessions = maps:update(SessionPid,
                              Session#session{queue=Queue#queue{backup=queue:new()}},
                              Sessions),
    _ = vmq_metrics:incr_queue_out(queue:len(Backup)),
    {next_state, online, State#state{sessions=NewSessions}};
online({enqueue, Msg}, State) ->
    _ = vmq_metrics:incr_queue_in(),
    {next_state, online, insert(Msg, State)};
online(Event, State) ->
    lager:error("got unknown event in online state ~p", [Event]),
    {next_state, online, State}.

online({set_opts, SessionPid, Opts}, _From, #state{opts=OldOpts} = State) ->
    MergedOpts = maps:merge(OldOpts, Opts),
    NewState1 = set_general_opts(MergedOpts, #state{opts=MergedOpts} = State),
    NewState2 = set_session_opts(SessionPid, MergedOpts, NewState1),
    {reply, ok, online, NewState2};
online({add_session, SessionPid, #{allow_multiple_sessions := true} = Opts}, _From, State) ->
    %% allow multiple sessions per queue
    {reply, ok, online, unset_expiry_timer(add_session_(SessionPid, Opts, State))};
online({add_session, SessionPid, #{allow_multiple_sessions := false} = Opts}, From, State)
  when State#state.waiting_call == undefined ->
    %% forbid multiple sessions per queue,
    %% we've to disconnect currently attached sessions
    %% and wait with the reply until all the sessions
    %% have been disconnected
    disconnect_sessions(State),
    {next_state, state_change(add_session, online, wait_for_offline),
     State#state{waiting_call={add_session, SessionPid, Opts, From}}};
online({migrate, OtherQueue}, From, State)
  when State#state.waiting_call == undefined ->
    disconnect_sessions(State),
    {next_state, state_change(migrate, online, wait_for_offline),
     State#state{waiting_call={migrate, OtherQueue, From}}};
online({set_last_waiting_acks, WAcks}, _From, State) ->
    {reply, ok, online, handle_waiting_acks_and_msgs(WAcks, State)};
online({enqueue_many, Msgs}, _From, State) ->
    _ = vmq_metrics:incr_queue_in(length(Msgs)),
    {reply, ok, online, insert_many(Msgs, State)};
online(Event, _From, State) ->
    lager:error("got unknown sync event in online state ~p", [Event]),
    {reply, {error, online}, State}.

wait_for_offline({enqueue, Msg}, State) ->
    _ = vmq_metrics:incr_queue_in(),
    {next_state, wait_for_offline, insert(Msg, State)};
wait_for_offline(Event, State) ->
    lager:error("got unknown event in wait_for_offline state ~p", [Event]),
    {next_state, wait_for_offline, State}.

wait_for_offline({set_last_waiting_acks, WAcks}, _From, State) ->
    {reply, ok, wait_for_offline, handle_waiting_acks_and_msgs(WAcks, State)};
wait_for_offline(Event, _From, State) ->
    lager:error("got unknown sync event in wait_for_offline state ~p", [Event]),
    {reply, {error, wait_for_offline}, wait_for_offline, State}.

drain(drain_start, #state{id=SId, offline=#queue{queue=Q} = Queue,
                          drain_time=DrainTimeout,
                          max_msgs_per_drain_step=DrainStepSize,
                          waiting_call={migrate, RemoteQueue, From}} = State) ->
    {DrainQ, NewQ} = queue_split(DrainStepSize, Q),
    #queue{queue=DecompressedDrainQ} = decompress_queue(SId, #queue{queue=DrainQ}),

    Msgs = queue:to_list(DecompressedDrainQ),
    %% remote_enqueue triggers an enqueue_many inside the remote queue
    %% but forces the traffic to go over the distinct communication link
    %% instead of the erlang distribution link.
    case vmq_cluster:remote_enqueue(node(RemoteQueue), {enqueue, RemoteQueue, Msgs}) of
        ok ->
            cleanup_queue(SId, DrainQ),
            _ = vmq_metrics:incr_queue_out(queue:len(DrainQ)),
            case queue:len(NewQ) of
                L when L > 0 ->
                    gen_fsm:send_event(self(), drain_start),
                    {next_state, drain,
                     State#state{offline=Queue#queue{size=L, drop=0,
                                                     queue=NewQ}}};
                _ ->
                    %% the extra timeout gives the chance that pending messages
                    %% in the erlang mailbox could still get enqueued and
                    %% therefore eventually transmitted to the remote queue
                    {next_state, drain,
                     State#state{drain_over_timer=gen_fsm:send_event_after(DrainTimeout, drain_over),
                                 offline=Queue#queue{size=0, drop=0,
                                                     queue=queue:new()}}}
            end;
        {error, Reason} ->
            %% this shouldn't happen, as the register_subscriber is synchronized
            %% using the vmq_reg_leader process. However this could theoretically
            %% happen in case of an inconsistent (but un-detected) cluster state.
            %% we don't drain in this case.
            lager:error("can't drain queue '~p' for [~p][~p] due to ~p",
                          [SId, self(), RemoteQueue, Reason]),
            gen_fsm:reply(From, ok),
            %% transition to offline, and let a future session drain this queue
            {next_state, state_change(drain_error, drain, offline),
             State#state{waiting_call=undefined}}
    end;
drain({enqueue, Msg}, #state{drain_over_timer=TRef} =  State) ->
    %% even in drain state it is possible that an enqueue message
    %% reaches this process, so we've to queue this message otherwise
    %% it would be lost.
    gen_fsm:cancel_timer(TRef),
    gen_fsm:send_event(self(), drain_start),
    _ = vmq_metrics:incr_queue_in(),
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


offline(init_offline_queue, #state{id=SId} = State) ->
    case vmq_plugin:only(msg_store_find, [SId]) of
        {ok, MsgRefs} ->
            {next_state, offline,
             insert_many(MsgRefs, State)};
        {error, no_matching_hook_found} ->
            % that's ok
            {next_state, offline, State};
        {error, Reason} ->
            lager:error("can't initialize queue from offline storage due to ~p, retry in 1 sec", [Reason]),
            gen_fsm:send_event_after(1000, init_offline_queue),
            {next_state, offline, State}
    end;
offline({enqueue, Msg}, #state{id=SId} = State) ->
    _ = vmq_plugin:all(on_offline_message, [SId]),
    %% storing the message in the offline queue
    _ = vmq_metrics:incr_queue_in(),
    {next_state, offline, insert(Msg, State)};
offline(expire_session, #state{id=SId, offline=#queue{queue=Q}} = State) ->
    %% session has expired cleanup and go down
    vmq_reg:delete_subscriptions(SId),
    cleanup_queue(SId, Q),
    _ = vmq_metrics:incr_queue_unhandled(queue:len(Q)),
    {stop, normal, State};
offline(Event, State) ->
    lager:error("got unknown event in offline state ~p", [Event]),
    {next_state, offline, State}.

offline({add_session, SessionPid, Opts}, _From, State) ->
    {reply, ok, state_change(add_session, offline, online),
     unset_expiry_timer(add_session_(SessionPid, Opts, State))};
offline({migrate, OtherQueue}, From, State) ->
    gen_fsm:send_event(self(), drain_start),
    {next_state, state_change(migrate, offline, drain),
     State#state{waiting_call={migrate, OtherQueue, From}}};
offline({enqueue_many, Msgs}, _From, State) ->
    _ = vmq_metrics:incr_queue_in(length(Msgs)),
    {reply, ok, offline, insert_many(Msgs, State)};
offline(Event, _From, State) ->
    lager:error("got unknown sync event in offline state ~p", [Event]),
    {reply, {error, offline}, offline, State}.


%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

init([SubscriberId, Clean]) ->
    Defaults = default_opts(),
    #{max_offline_messages := MaxOfflineMsgs,
      queue_deliver_mode := DeliverMode,
      queue_type := QueueType,
      max_drain_time := DrainTime,
      max_msgs_per_drain_step := MaxMsgsPerDrainStep} = Defaults,
    OfflineQueue = #queue{type=QueueType, max=MaxOfflineMsgs},
    {A, B, C} = os:timestamp(),
    random:seed(A, B, C),
    case Clean of
        true ->
            ignore;
        false ->
            %% only init offline queue if this is a queue generated
            %% during broker startup or we were restarted by the
            %% vmq_queue_sup supervisor
            gen_fsm:send_event(self(), init_offline_queue)
    end,
    vmq_metrics:incr_queue_setup(),
    {ok, offline,  #state{id=SubscriberId,
                          offline=OfflineQueue,
                          drain_time=DrainTime,
                          deliver_mode=DeliverMode,
                          max_msgs_per_drain_step=MaxMsgsPerDrainStep,
                          opts=Defaults}}.

handle_event({migrate_info, RemoteQueue}, StateName,
             #state{migrations=Migrations} = State) ->
    NewMigrations = [RemoteQueue|Migrations],
    {next_state, StateName, State#state{migrations=NewMigrations}};
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(migration_status, _From, StateName,
                  #state{migrations=Migrations} = State) ->
    NewMigrations =
    [MQPid || MQPid <- Migrations, rpc:pinfo(MQPid, status) /= undefined],
    {reply, NewMigrations, StateName, State#state{migrations=NewMigrations}};
handle_sync_event(status, _From, StateName,
                  #state{deliver_mode=Mode, offline=#queue{size=OfflineSize},
                         sessions=Sessions, opts=#{is_plugin := IsPlugin}} = State) ->
    TotalStoredMsgs =
    maps:fold(fun(_, #session{queue=#queue{size=Size}}, Acc) ->
                      Acc + Size
              end, OfflineSize, Sessions),
    {reply, {StateName, Mode, TotalStoredMsgs, maps:size(Sessions), IsPlugin}, StateName, State};
handle_sync_event(get_sessions, _From, StateName, #state{sessions=Sessions} = State) ->
    {reply, maps:keys(Sessions), StateName, State};
handle_sync_event(get_opts, _From, StateName, #state{opts=Opts} = State) ->
    {reply, Opts, StateName, State};
handle_sync_event(Event, _From, _StateName, State) ->
    {stop, {error, {unknown_sync_event, Event}}, State}.

handle_info({'DOWN', _MRef, process, Pid, _}, StateName, State) ->
    handle_session_down(Pid, StateName, State);
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    vmq_metrics:incr_queue_teardown(),
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
add_session_(SessionPid, Opts, #state{id=SId, offline=Offline,
                                      sessions=Sessions, opts=OldOpts} = State) ->
    #{clean_session := Clean,
      max_online_messages := MaxOnlineMessages,
      max_offline_messages := MaxOfflineMsgs,
      queue_deliver_mode := DeliverMode,
      queue_type := QueueType} = Opts,
    NewSessions =
    case maps:get(SessionPid, Sessions, not_found) of
        not_found ->
            _ = vmq_plugin:all(on_client_wakeup, [SId]),
            monitor(process, SessionPid),
            maps:put(SessionPid,
                     #session{pid=SessionPid, clean=Clean,
                              queue=#queue{max=MaxOnlineMessages}}, Sessions);
        _ ->
            Sessions
    end,
    insert_from_queue(Offline#queue{type=QueueType},
                      State#state{
                        deliver_mode=DeliverMode,
                        offline=Offline#queue{
                                  max=MaxOfflineMsgs,
                                  type=QueueType,
                                  size=0, drop=0,
                                  queue = queue:new()
                                 },
                        opts=maps:merge(OldOpts, Opts),
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

handle_session_down(SessionPid, StateName,
                    #state{id=SId, waiting_call=WaitingCall} = State) ->
    {NewState, DeletedSession} = del_session(SessionPid, State),
    case {maps:size(NewState#state.sessions), StateName, WaitingCall} of
        {0, wait_for_offline, {add_session, NewSessionPid, Opts, From}} ->
            %% last session gone
            %% ... but we've a new session waiting
            %%     no need to go into offline state
            gen_fsm:reply(From, ok),
            case DeletedSession#session.clean of
                true ->
                    _ = vmq_plugin:all(on_client_gone, [SId]);
                false ->
                    _ = vmq_plugin:all(on_client_offline, [SId])
            end,
            {next_state, state_change({'DOWN', add_session}, wait_for_offline, online),
             add_session_(NewSessionPid, Opts, NewState#state{waiting_call=undefined})};
        {0, wait_for_offline, {migrate, _, From}} when DeletedSession#session.clean ->
            %% last session gone
            %% ... we dont need to migrate this one
            vmq_reg:delete_subscriptions(SId),
            _ = vmq_plugin:all(on_client_gone, [SId]),
            gen_fsm:reply(From, ok),
            {stop, normal, NewState};
        {0, wait_for_offline, {migrate, _, _}} ->
            %% last session gone
            %% ... but we've a migrate request waiting
            %%     go into drain state
            gen_fsm:send_event(self(), drain_start),
            _ = vmq_plugin:all(on_client_offline, [SId]),
            {next_state, state_change({'DOWN', migrate}, wait_for_offline, drain), NewState};
        {0, _, _} when DeletedSession#session.clean ->
            %% last session gone
            %% ... we've to cleanup and go down
            %%
            %% it is assumed that all attached sessions use the same
            %% clean session flag
            vmq_reg:delete_subscriptions(SId),
            _ = vmq_plugin:all(on_client_gone, [SId]),
            {stop, normal, NewState};
        {0, OldStateName, _} ->
            %% last session gone
            %% ... we've to stay around and store the messages
            %%     inside the offline queue
            _ = vmq_plugin:all(on_client_offline, [SId]),
            {next_state, state_change('DOWN', OldStateName, offline),
             maybe_set_expiry_timer(NewState#state{offline=compress_queue(SId, NewState#state.offline)})};
        _ ->
            %% still one or more sessions online
            {next_state, StateName, NewState}
    end.

handle_waiting_acks_and_msgs(WAcks, #state{id=SId, sessions=Sessions, offline=Offline} = State) ->
    %% we can only handle the last waiting acks and msgs if this is
    %% the last session active for this queue.
    case maps:size(Sessions) of
        1 ->
            %% this is the last active session
            NewOfflineQueue =
            lists:foldl(
              fun({deliver, QoS, #vmq_msg{persisted=true} = Msg}, AccOffline) ->
                      queue_insert(true, {deliver, QoS, Msg#vmq_msg{persisted=false}}, AccOffline, SId);
                 (Msg, AccOffline) ->
                      queue_insert(true, Msg, AccOffline, SId)
              end, Offline, WAcks),
            State#state{offline=NewOfflineQueue};
        N ->
            lager:debug("handle waiting acks for multiple sessions (~p) not possible", [N]),
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
                      SessionPid ! {vmq_mqtt_fsm, disconnect},
                      ok
              end, ok, Sessions).

change_session_state(NewState, SessionPid, #state{id=SId, sessions=Sessions} = State) ->
    #session{queue=#queue{backup=Backup} = Queue} = Session = maps:get(SessionPid, Sessions),
    cleanup_queue(SId, Backup),
    _ = vmq_metrics:incr_queue_out(queue:len(Backup)),
    UpdatedSession = change_session_state(NewState,
                                          Session#session{queue=Queue#queue{backup=queue:new()}}),
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

insert_from_session(#session{queue=Queue},
                    #state{deliver_mode=fanout, sessions=Sessions} = State)
  when Sessions == #{} ->
    %% all will go into offline queue
    insert_from_queue(Queue, State);
insert_from_session(_, #state{deliver_mode=fanout} = State) ->
    %% due to fanout other sessions have already received the messages
    State;
insert_from_session(#session{queue=Queue},
                    #state{deliver_mode=balance} = State) ->
    %% allow other sessions to balance the messages of the dead queue
    insert_from_queue(Queue, State).

insert_from_queue(#queue{type=fifo, queue=Q}, State) ->
    insert_from_queue(fun queue:out/1, queue:out(Q), State);
insert_from_queue(#queue{type=lifo, queue=Q}, State) ->
    insert_from_queue(fun queue:out_r/1, queue:out_r(Q), State).

insert_from_queue(F, {{value, Msg}, Q}, State) when is_tuple(Msg) ->
    insert_from_queue(F, F(Q), insert(Msg, State));
insert_from_queue(F, {{value, MsgRef}, Q}, #state{id=SId} = State) when is_binary(MsgRef) ->
    case vmq_plugin:only(msg_store_read, [SId, MsgRef]) of
        {ok, #vmq_msg{qos=QoS} = Msg} ->
            insert_from_queue(F, F(Q), insert({deliver, QoS, Msg}, State));
        {error, _} ->
            insert_from_queue(F, F(Q), State)
    end;
insert_from_queue(_F, {empty, _}, State) ->
    State.

insert_many(MsgsOrRefs, State) ->
    lists:foldl(fun(MsgOrRef, AccState) ->
                        insert(MsgOrRef, AccState)
                end, State, MsgsOrRefs).

%% Offline Queue
insert({deliver, 0, _}, #state{sessions=Sessions} = State)
  when Sessions == #{} ->
    %% no session online, skip message for QoS0 Subscription
    State;
insert({deliver, _, #vmq_msg{qos=0}}, #state{sessions=Sessions} = State)
  when Sessions == #{} ->
    %% no session online, skip QoS0 message for QoS1 or QoS2 Subscription
    State;
insert(MsgOrRef, #state{id=SId, offline=Offline, sessions=Sessions} = State)
  when Sessions == #{} ->
    %% no session online, insert in offline queue
    State#state{offline=queue_insert(true, MsgOrRef, Offline, SId)};

%% Online Queue
insert(MsgOrRef, #state{id=SId, deliver_mode=fanout, sessions=Sessions} = State) ->
    {NewSessions, _} = session_fold(SId, fun session_insert/3, MsgOrRef, Sessions),
    State#state{sessions=NewSessions};

insert(MsgOrRef, #state{id=SId, deliver_mode=balance, sessions=Sessions} = State) ->
    Pids = maps:keys(Sessions),
    RandomPid = lists:nth(random:uniform(length(Pids)), Pids),
    RandomSession = maps:get(RandomPid, Sessions),
    {UpdatedSession, _} = session_insert(SId, RandomSession, MsgOrRef),
    State#state{sessions=maps:update(RandomPid, UpdatedSession, Sessions)}.


session_insert(SId, #session{status=active, queue=Q} = Session, MsgOrRef) ->
    {send(Session#session{queue=queue_insert(false, MsgOrRef, Q, SId)}), MsgOrRef};
session_insert(SId, #session{status=passive, queue=Q} = Session, MsgOrRef) ->
    {Session#session{queue=queue_insert(false, MsgOrRef, Q, SId)}, MsgOrRef};
session_insert(SId, #session{status=notify, queue=Q} = Session, MsgOrRef) ->
    {send_notification(Session#session{queue=queue_insert(false, MsgOrRef, Q, SId)}), MsgOrRef}.

%% unlimited messages accepted
queue_insert(Offline, MsgOrRef, #queue{max=-1, size=Size, queue=Queue} = Q, SId) ->
    Q#queue{queue=queue:in(maybe_offline_store(Offline, SId, MsgOrRef), Queue), size=Size + 1};
%% tail drop in case of fifo
queue_insert(_Offline, MsgOrRef, #queue{type=fifo, max=Max, size=Size, drop=Drop} = Q, SId)
  when Size >= Max ->
    vmq_metrics:incr_queue_drop(),
    maybe_offline_delete(SId, MsgOrRef),
    Q#queue{drop=Drop + 1};
%% drop oldest in case of lifo
queue_insert(Offline, MsgOrRef, #queue{type=lifo, max=Max, size=Size, queue=Queue, drop=Drop} = Q, SId)
  when Size >= Max ->
    {{value, OldMsgOrRef}, NewQueue} = queue:out(Queue),
    vmq_metrics:incr_queue_drop(),
    maybe_offline_delete(SId, OldMsgOrRef),
    Q#queue{queue=queue:in(maybe_offline_store(Offline, SId, MsgOrRef), NewQueue), drop=Drop + 1};

%% normal enqueue
queue_insert(Offline, MsgOrRef, #queue{queue=Queue, size=Size} = Q, SId) ->
    Q#queue{queue=queue:in(maybe_offline_store(Offline, SId, MsgOrRef), Queue), size=Size + 1}.

send(#session{pid=Pid, queue=Q} = Session) ->
    Session#session{status=passive, queue=send(Pid, Q)}.

send(Pid, #queue{type=fifo, queue=Queue, size=Count, drop=Dropped} = Q) ->
    Msgs = queue:to_list(Queue),
    vmq_mqtt_fsm:send(Pid, {mail, self(), Msgs, Count, Dropped}),
    Q#queue{queue=queue:new(), backup=Queue, size=0, drop=0};
send(Pid, #queue{type=lifo, queue=Queue, size=Count, drop=Dropped} = Q) ->
    Msgs = lists:reverse(queue:to_list(Queue)),
    vmq_mqtt_fsm:send(Pid, {mail, self(), Msgs, Count, Dropped}),
    Q#queue{queue=queue:new(), backup=Queue, size=0, drop=0}.

send_notification(#session{pid=Pid} = Session) ->
    vmq_mqtt_fsm:send(Pid, {mail, self(), new_data}),
    Session#session{status=passive}.

cleanup_session(SubscriberId, #session{queue=#queue{queue=Q}}) ->
    _ = vmq_metrics:incr_queue_unhandled(queue:len(Q)),
    cleanup_queue(SubscriberId, Q).

cleanup_queue(_, {[],[]}) -> ok; %% optimization
cleanup_queue(SId, Queue) ->
    cleanup_queue_(SId, queue:out(Queue)).

cleanup_queue_(SId, {{value, {deliver, _, _} = Msg}, NewQueue}) ->
    maybe_offline_delete(SId, Msg),
    cleanup_queue_(SId, queue:out(NewQueue));
cleanup_queue_(SId, {{value, {deliver_bin, _}}, NewQueue}) ->
    % no need to deref
    cleanup_queue_(SId, queue:out(NewQueue));
cleanup_queue_(SId, {{value, MsgRef}, NewQueue}) when is_binary(MsgRef) ->
    maybe_offline_delete(SId, MsgRef),
    cleanup_queue_(SId, queue:out(NewQueue));
cleanup_queue_(_, {empty, _}) -> ok.


session_fold(SId, Fun, Acc, Map) ->
    session_fold(SId, Fun, Acc, Map, maps:keys(Map)).

session_fold(SId, Fun, Acc, Map, [K|Rest]) ->
    {NewV, NewAcc} = Fun(SId, maps:get(K, Map), Acc),
    session_fold(SId, Fun, NewAcc, maps:update(K, NewV, Map), Rest);
session_fold(_, _, Acc, Map, []) ->
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

maybe_offline_store(Offline, SubscriberId, {deliver, QoS, #vmq_msg{persisted=false} = Msg}) when QoS > 0 ->
    PMsg = Msg#vmq_msg{persisted=true},
    case vmq_plugin:only(msg_store_write, [SubscriberId, PMsg#vmq_msg{qos=QoS}]) of
        %% in case we have no online/serving session attached
        %% to this queue anymore we can save memory by only
        %% keeping the message ref in the queue
        ok when Offline ->
            PMsg#vmq_msg.msg_ref;
        {ok, NewMsgRef} when Offline ->
            NewMsgRef;
        %% in case we still have online sessions attached
        %% to this queue, we keep the full message structure around
        ok ->
            {deliver, QoS, PMsg};
        {ok, NewMsgRef} ->
            {deliver, QoS, PMsg#vmq_msg{msg_ref=NewMsgRef}};
        %% in case we cannot store the message we keep the
        %% full message structure around
        {error, _} ->
            {deliver, QoS, Msg}
    end;
maybe_offline_store(true, _, {deliver, _, #vmq_msg{persisted=true} = Msg}) ->
    Msg#vmq_msg.msg_ref;
maybe_offline_store(_, _, MsgOrRef) -> MsgOrRef.

maybe_offline_delete(SubscriberId, {deliver, _, #vmq_msg{persisted=true, msg_ref=MsgRef}}) ->
    _ = vmq_plugin:only(msg_store_delete, [SubscriberId, MsgRef]),
    ok;
maybe_offline_delete(SubscriberId, MsgRef) when is_binary(MsgRef) ->
    _ = vmq_plugin:only(msg_store_delete, [SubscriberId, MsgRef]),
    ok;
maybe_offline_delete(_, _) -> ok.

unset_expiry_timer(#state{expiry_timer=undefined} = State) -> State;
unset_expiry_timer(#state{expiry_timer=Ref} = State) ->
    erlang:cancel_timer(Ref),
    State#state{expiry_timer=undefined}.

state_change(Msg, OldStateName, NewStateName) ->
    lager:debug("[~p] transition from ~p --> ~p because of ~p", [self(), OldStateName, NewStateName, Msg]),
    NewStateName.

set_general_opts(#{queue_deliver_mode := DeliverMode,
                   max_offline_messages := MaxOfflineMsgs,
                   max_msgs_per_drain_step := MaxMsgsPerDrainStep},
                 #state{offline=Offline} = State) ->
    State#state{offline=Offline#queue{max=MaxOfflineMsgs},
                deliver_mode=DeliverMode,
                max_msgs_per_drain_step=MaxMsgsPerDrainStep}.

set_session_opts(SessionPid, #{max_online_messages := MaxOnlineMsgs,
                               queue_type := Type}, #state{sessions=Sessions} = State) ->
    #session{queue=Queue} = Session = maps:get(SessionPid, Sessions),
    NewSessions = maps:update(SessionPid,
                              Session#session{
                                queue=Queue#queue{
                                        max=MaxOnlineMsgs,
                                        type=Type
                                       }
                               }, Sessions),
    State#state{sessions=NewSessions}.

compress_queue(SId, #queue{queue=Q} = Queue) ->
    NewQueue = compress_queue(SId, queue:to_list(Q), []),
    Queue#queue{queue=NewQueue}.

compress_queue(_, [], Acc) ->
    queue:from_list(lists:reverse(Acc));
compress_queue(SId, [Msg|Rest], Acc) ->
    compress_queue(SId, Rest, [maybe_offline_store(true, SId, Msg)|Acc]).

decompress_queue(SId, #queue{queue=Q} = Queue) ->
    NewQueue = decompress_queue(SId, queue:to_list(Q), []),
    Queue#queue{queue=NewQueue}.

decompress_queue(_, [], Acc) ->
    queue:from_list(lists:reverse(Acc));
decompress_queue(SId, [MsgRef|Rest], Acc) when is_binary(MsgRef) ->
    case vmq_plugin:only(msg_store_read, [SId, MsgRef]) of
        {ok, #vmq_msg{qos=QoS} = Msg} ->
            decompress_queue(SId, Rest,
                             [{deliver, QoS, Msg#vmq_msg{persisted=false}}|Acc]);
        {error, Reason} ->
            lager:warning("can't decompress queue item with msg_ref ~p for subscriber ~p due to ~p",
                          [MsgRef, SId, Reason]),
            decompress_queue(SId, Rest, Acc)
    end;
decompress_queue(SId, [{deliver, QoS, Msg}|Rest], Acc) ->
    decompress_queue(SId, Rest, [{deliver, QoS, Msg#vmq_msg{persisted=false}}|Acc]);
decompress_queue(SId, [_|Rest], Acc) ->
    decompress_queue(SId, Rest, Acc).

queue_split(N, Queue) ->
    NN = case queue:len(Queue) of
             L when L > N -> N;
             L -> L
         end,
    queue:split(NN, Queue).
