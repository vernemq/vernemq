-module(vmq_queue).
-include("vmq_server.hrl").
-behaviour(gen_fsm).

-type max() :: non_neg_integer().
-type drop() :: non_neg_integer().
-type in() :: {'post', Msg::term()}.
-type note() :: {'mail', Self::pid(), new_data}.
-type mail() :: {'mail', Self::pid(), Msgs::list(),
                         Count::non_neg_integer(), Lost::drop()}.

-export_type([max/0, in/0, mail/0, note/0]).

-record(state, {queue = queue:new() :: queue(),
                max = undefined :: non_neg_integer(),
                size = 0 :: non_neg_integer(),
                drop = 0 :: drop(),
                owner :: pid()}).

-export([start_link/2, resize/2,
         active/1, notify/1, enqueue/2]).
-export([init/1,
         active/2, passive/2, notify/2,
         active/3, passive/3, notify/3,
         handle_event/3, handle_sync_event/4, handle_info/3,
         terminate/3, code_change/4]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% API Function Definitions %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Owner, QueueSize) when is_pid(Owner),
                                 is_integer(QueueSize) ->
    gen_fsm:start_link(?MODULE, [Owner, QueueSize], []).

%% @doc Allows to take a given queue, and make it larger or smaller.
%% A Queue can be made larger without overhead, but it may take
%% more work to make it smaller given there could be a
%% need to drop messages that would now be considered overflow.
%% if Size equals to zero, we wont drop messages.
-spec resize(pid(), max()) -> ok.
resize(Queue, NewSize) when NewSize >= 0 ->
    gen_fsm:sync_send_all_state_event(Queue, {resize, NewSize}).

%% @doc Forces the queue into an active state where it will
%% send the data it has accumulated.
-spec active(pid()) -> ok.
active(Queue) when is_pid(Queue) ->
    gen_fsm:send_event(Queue, active).

%% @doc Forces the queueinto its notify state, where it will send a single
%% message alerting the Owner of new messages before going back to the passive
%% state.
-spec notify(pid()) -> ok.
notify(Queue) ->
    gen_fsm:send_event(Queue, notify).

%% @doc Enqueues a message.
-spec enqueue(pid(), term()) -> ok.
enqueue(Queue, Msg) ->
    case catch gen_fsm:sync_send_event(Queue, {enqueue, Msg}, 100) of
        ok -> ok;
        {'EXIT', _Reason} ->
            % we are not allowed to crash, this would
            % teardown 'decoupled' publisher process
            ok
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% gen_fsm Function Definitions %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @private
init([Owner, QueueSize]) ->
    monitor(process, Owner),
    {ok, notify, #state{max=QueueSize, owner=Owner}}.

%% @private
active(active, S = #state{}) ->
    {next_state, active, S};
active(notify, S = #state{}) ->
    {next_state, notify, S};
active({enqueue, Msg}, S = #state{}) ->
    send(insert(Msg, S));
active(_Msg, S = #state{}) ->
    %% unexpected
    {next_state, active, S}.

active({enqueue, Msg}, From, S = #state{}) ->
    gen_fsm:reply(From, ok),
    send(insert(Msg, S)).

%% @private
passive(notify, #state{size=0} = S) ->
    {next_state, notify, S};
passive(notify, #state{size=Size} = S) when Size > 0 ->
    send_notification(S);
passive(active, S = #state{size=0} = S) ->
    {next_state, active, S};
passive(active, S = #state{size=Size} = S) when Size > 0 ->
    send(S);
passive({enqueue, Msg}, S) ->
    {next_state, passive, insert(Msg, S)};
passive(_Msg, S = #state{}) ->
    %% unexpected
    {next_state, passive, S}.

passive({enqueue, Msg}, From, S = #state{}) ->
    gen_fsm:reply(From, ok),
    {next_state, passive, insert(Msg, S)}.

%% @private
notify(active, S = #state{size=0}) ->
    {next_state, active, S};
notify(active, S = #state{size=Size}) when Size > 0 ->
    send(S);
notify(notify, S = #state{}) ->
    {next_state, notify, S};
notify({enqueue, Msg}, S) ->
    send_notification(insert(Msg, S));
notify(_Msg, S = #state{}) ->
    %% unexpected
    {next_state, notify, S}.

notify({enqueue, Msg}, From, S = #state{}) ->
    gen_fsm:reply(From, ok),
    send_notification(insert(Msg, S)).

%% @private
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%% @private
handle_sync_event({resize, NewSize}, _From, StateName, S) ->
    {reply, ok, StateName, resize_buf(NewSize, S)};
handle_sync_event(_Event, _From, StateName, State) ->
    %% die of starvation, caller!
    {next_state, StateName, State}.

%% @private
handle_info({'DOWN', _, process, _, Reason}, _, State) ->
    {stop, Reason, State};
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

%% @private
terminate(_Reason, _StateName, _State) ->
    ok.

%% @private
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Private Function Definitions %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

send(S=#state{queue = Queue, size=Count, drop=Dropped, owner=Pid}) ->
    Msgs = queue:to_list(Queue),
    Pid ! {mail, self(), Msgs, Count, Dropped},
    NewState = S#state{queue=queue:new(), size=0, drop=0},
    {next_state, passive, NewState}.

send_notification(S = #state{owner=Owner}) ->
    Owner ! {mail, self(), new_data},
    {next_state, passive, S}.

insert(Msg, #state{max=0, size=Size, queue=Queue} = State) ->
    State#state{queue=queue:in(Msg, Queue), size=Size + 1};
insert(Msg, #state{max=Size, size=Size, drop=Drop} = State) ->
    %% tail drop
    case Msg of
        {deliver, 0, _} -> ok;
        {deliver, _, #vmq_msg{msg_ref=MsgRef}} ->
            vmq_msg_store:deref(MsgRef);
        _ ->
            ok
    end,
    State#state{drop=Drop + 1};
insert(Msg, #state{size=Size, queue=Queue} = State) ->
    State#state{queue=queue:in(Msg, Queue), size=Size + 1}.

resize_buf(0, S) ->
    S#state{max=0};
resize_buf(NewMax, #state{max=Max} = S) when Max =< NewMax ->
    S#state{max=NewMax};
resize_buf(NewMax, #state{queue=Queue, size=Size, drop=Drop} = S) ->
    if Size > NewMax ->
           ToDrop = Size - NewMax,
           S#state{queue=drop(ToDrop, Size, Queue),
                   size=NewMax, max=NewMax, drop=Drop + ToDrop};
       Size =< NewMax ->
           S#state{max=NewMax}
    end.

drop(N, Size, Queue) ->
    if Size > N  ->
           {QDrop, NewQueue} = queue:split(N, Queue),
           deref(QDrop),
           NewQueue;
       Size =< N ->
           deref(Queue),
           queue:new()
    end.

deref({{value, {deliver, 0, _}}, Q}) ->
    deref(queue:out(Q));
deref({{value, {deliver, _, #vmq_msg{msg_ref=MsgRef}}}, Q}) ->
    vmq_msg_store:deref(MsgRef),
    deref(queue:out(Q));
deref({{value, _}, Q}) ->
    deref(queue:out(Q));
deref({empty, _}) ->
    ok;
deref(Q) ->
    deref(queue:out(Q)).
