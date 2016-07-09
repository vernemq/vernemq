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

-module(vmq_queue_sup).

%% API functions
-export([start_link/3,
         start_queue/1,
         get_queue_pid/1,
         fold_queues/2,
         summary/0,
         nr_of_queues/0]).

%% Supervisor callbacks
-export([init/4]).
-export([system_continue/3]).
-export([system_terminate/4]).
-export([system_code_change/4]).

-define(QUEUE_TAB, vmq_queue_tab).
-record(state, {parent, shutdown, r=0, max_r, max_t, reset_timer}).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Shutdown, MaxR, MaxT) ->
    case proc_lib:start_link(?MODULE, init, [self(), Shutdown, MaxR, MaxT * 1000]) of
        {ok, Pid} = Ret ->
            register(?MODULE, Pid),
            {InitPid, MRef} = spawn_monitor(vmq_reg, fold_subscribers,
                                            [fun fold_subscribers/3, ok]),
            receive
                {'DOWN', MRef, process, InitPid, normal} ->
                    Ret;
                {'DOWN', MRef, process, InitPid, Reason} ->
                    exit(Pid, kill),
                    {error, {init_error, Reason}}
            end;
        {error, Error} ->
            {error, Error}
    end.

fold_subscribers(SubscriberId, Nodes, Acc) ->
    case lists:member(node(), Nodes) of
        true ->
            start_queue(SubscriberId, false);
        false ->
            Acc
    end.

start_queue(SubscriberId) ->
    start_queue(SubscriberId, true).

start_queue(SubscriberId, Clean) ->
    %% The Clean flag is used to distinguish between Queues created during
    %% broker start and newly created ones.
    %% The main difference is that the ones created at broker start have
    %% to initialize themselves using the message store,
    %% whereas new ones don't require to access the message store at startup
    %%
    %% if a queue terminates abnormally and gets restarted by this supervisor
    %% we enforce the roundtrip to the message store.
    Ref = make_ref(),
    ?MODULE ! {?MODULE, {self(), Ref}, {start_queue, SubscriberId, Clean}},
    receive
        {Ref, Reply} -> Reply
    end.

get_queue_pid(SubscriberId) ->
    case ets:lookup(?QUEUE_TAB, SubscriberId) of
        [] ->
            not_found;
        [{_, Pid}] ->
            Pid
    end.

fold_queues(FoldFun, Acc) ->
    ets:foldl(fun({SubscriberId, QPid}, AccAcc) ->
                      FoldFun(SubscriberId, QPid, AccAcc)
              end, Acc, ?QUEUE_TAB).

summary() ->
    fold_queues(
      fun(_, QPid, {AccOnline, AccWait, AccDrain, AccOffline, AccStoredMsgs} = Acc) ->
              try vmq_queue:status(QPid) of
                  {_, _, _, _, true} ->
                      %% this is a queue belonging to a plugin... ignore it
                      Acc;
                  {online, _, TotalStoredMsgs, _, _} ->
                      {AccOnline + 1, AccWait, AccDrain, AccOffline, AccStoredMsgs + TotalStoredMsgs};
                  {wait_for_offline, _, TotalStoredMsgs, _, _} ->
                      {AccOnline, AccWait + 1, AccDrain, AccOffline, AccStoredMsgs + TotalStoredMsgs};
                  {drain, _, TotalStoredMsgs, _, _} ->
                      {AccOnline, AccWait, AccDrain + 1, AccOffline, AccStoredMsgs + TotalStoredMsgs};
                  {offline, _, TotalStoredMsgs, _, _} ->
                      {AccOnline, AccWait, AccDrain, AccOffline + 1, AccStoredMsgs + TotalStoredMsgs}
              catch
                  _:_ ->
                      %% queue stopped in the meantime, that's ok.
                      Acc
              end
      end, {0, 0, 0, 0, 0}).

nr_of_queues() ->
    case ets:info(?QUEUE_TAB, size) of
        undefined -> 0;
        V -> V
    end.

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init(Parent, Shutdown, MaxR, MaxT) ->
    process_flag(trap_exit, true),
    ok = proc_lib:init_ack(Parent, {ok, self()}),
    ets:new(?QUEUE_TAB, [public, {read_concurrency, true}, named_table]),
    loop(#state{parent=Parent, shutdown=Shutdown, max_r=MaxR, max_t=MaxT}, 0).

loop(State = #state{parent=Parent}, NrOfChildren) ->
    receive
        {?MODULE, Caller, {start_queue, SubscriberId, Clean}} ->
            case ets:lookup(?QUEUE_TAB, SubscriberId) of
                [] ->
                    loop(State, start_queue(Caller, SubscriberId, Clean, NrOfChildren));
                [{_, Pid}] ->
                    %% already started
                    reply(Caller, {ok, true, Pid}),
                    loop(State, NrOfChildren)
            end;
        {'EXIT', Parent, Reason} ->
            terminate(State, NrOfChildren, Reason);
        {'EXIT', Pid, Reason} ->
            erase(Pid),
            case Reason of
                shutdown ->
                    ets:match_delete(?QUEUE_TAB, {'_', Pid}),
                    loop(State, NrOfChildren - 1);
                normal ->
                    ets:match_delete(?QUEUE_TAB, {'_', Pid}),
                    loop(State, NrOfChildren - 1);
                Reason ->
                    #state{r=R, max_r=MaxR, max_t=MaxT, reset_timer=Reset} = State,
                    case R >= MaxR of
                        true ->
                            terminate(State#state{shutdown=brutal_kill},
                                      NrOfChildren, exhausted_restart_strategy);
                        false ->
                            [{SubscriberId, _}] = ets:match_object(?QUEUE_TAB, {'_', Pid}),
                            report_error(SubscriberId, Pid, Reason),
                            loop(State#state{r=R + 1,
                                             reset_timer=maybe_set_reset_timer(MaxT, Reset)},
                                 start_queue(undefined, SubscriberId, false, NrOfChildren - 1))
                    end
            end;
        {?MODULE, reset_timer} ->
            loop(State#state{r=0, reset_timer=undefined}, NrOfChildren);
        {system, From, Request} ->
			sys:handle_system_msg(Request, From, Parent, ?MODULE, [],
                                  {State, NrOfChildren});
        %% Calls from the supervisor module.
		{'$gen_call', {To, Tag}, which_children} ->
			Pids = get_keys(true),
			Children = [{vmq_queue, Pid, worker, [vmq_queue]}
                        || Pid <- Pids, is_pid(Pid)],
			To ! {Tag, Children},
			loop(State, NrOfChildren);
		{'$gen_call', {To, Tag}, count_children} ->
            Counts =
            [{specs, 1}, {active, NrOfChildren},
             {supervisors, 0}, {workers, NrOfChildren}],
            To ! {Tag, Counts},
			loop(State, NrOfChildren);
		{'$gen_call', {To, Tag}, _} ->
			To ! {Tag, {error, ?MODULE}},
			loop(State, NrOfChildren);
        Msg ->
            lager:error("vmq_queue_sup received unexpected message ~p", [Msg])
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% Kill all children and then exit. We unlink first to avoid
%% getting a message for each child getting killed.
terminate(#state{shutdown=brutal_kill}, _, Reason) ->
    ets:delete_all_objects(?QUEUE_TAB),
    _ = [begin
             unlink(P),
             exit(P, kill)
         end || P <- get_keys(true)],
    exit(Reason);
%% Attemsupervisor.htmlpt to gracefully shutdown all children.
terminate(#state{shutdown=Shutdown}, NrOfChildren, Reason) ->
    ets:delete_all_objects(?QUEUE_TAB),
    shutdown_children(),
    case Shutdown of
        infinity ->
            ok;
        _ ->
            erlang:send_after(Shutdown, self(), kill)
    end,
    wait_children(NrOfChildren),
    exit(Reason).

shutdown_children() ->
    _ = [begin
             monitor(process, P),
             unlink(P),
             exit(P, shutdown)
         end || P <- get_keys(true)],
    ok.

wait_children(0) -> ok;
wait_children(NrOfChildren) ->
    receive
        {'DOWN', _, process, Pid, _} ->
            _ = erase(Pid),
            wait_children(NrOfChildren - 1);
        kill ->
            _ = [exit(P, kill) || P <- get_keys(true)],
            ok
    end.

start_queue(Caller, SubscriberId, Clean, NrOfChildren) ->
    try vmq_queue:start_link(SubscriberId, Clean) of
        {ok, Pid} ->
            ets:insert(?QUEUE_TAB, {SubscriberId, Pid}),
            put(Pid, true),
            reply(Caller, {ok, false, Pid}),
            NrOfChildren + 1;
        Ret ->
            lager:error("vmq_queue_sup can't start vmq_queue for ~p due to ~p",
                        [SubscriberId, Ret]),
            reply(Caller, {error, cant_start_queue}),
            NrOfChildren
    catch
        Class:Reason ->
            lager:error("vmq_queue_sup can't start vmq_queue for ~p due crash ~p:~p",
                        [SubscriberId, Class, Reason]),
            reply(Caller, {error, Reason}),
            NrOfChildren
    end.

maybe_set_reset_timer(MaxT, undefined) ->
    erlang:send_after(MaxT, self(), {?MODULE, reset_timer});
maybe_set_reset_timer(_, TRef) -> TRef.

reply({CallerPid, CallerRef}, Reply) ->
    CallerPid ! {CallerRef, Reply};
reply(undefined, Reply) -> Reply.

report_error(SubscriberID, Pid, Reason) ->
    lager:error("vmq_queue_sup had vmq_queue process ~p for subscriber ~p exit with reason: ~p",
                [Pid, SubscriberID, Reason]).

system_continue(_, _, {State, NrOfChildren}) ->
	loop(State, NrOfChildren).

-spec system_terminate(any(), _, _, _) -> no_return().
system_terminate(Reason, _, _, {State, NrOfChildren}) ->
	terminate(State, NrOfChildren, Reason).

system_code_change(Misc, _, _, _) ->
	{ok, Misc}.
