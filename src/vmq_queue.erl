%%%-------------------------------------------------------------------
%% @copyright Fred Hebert, Geoff Cant
%% @author Fred Hebert <mononcqc@ferd.ca>
%% @author Geoff Cant <nem@erlang.geek.nz>
%% @doc Generic process that acts as an external mailbox and a
%% message buffer that will drop requests as required. For more
%% information, see README.txt
%% @end
%%%-------------------------------------------------------------------
-module(vmq_queue).
-behaviour(gen_fsm).
-compile({no_auto_import,[size/1]}).

-record(buf, {type = undefined :: 'stack' | 'queue' | 'keep_old',
              max = undefined :: max(),
              size = 0 :: non_neg_integer(),
              drop = 0 :: drop(),
              data = undefined :: queue() | list()}).

-type max() :: pos_integer().
-type drop() :: non_neg_integer().
-type buffer() :: #buf{}.
-type filter() :: fun( (Msg::term(), State::term()) ->
                        {{ok,NewMsg::term()} | drop , State::term()} | skip).

-type in() :: {'post', Msg::term()}.
-type note() :: {'mail', Self::pid(), new_data}.
-type mail() :: {'mail', Self::pid(), Msgs::list(),
                         Count::non_neg_integer(), Lost::drop()}.

-export_type([max/0, filter/0, in/0, mail/0, note/0]).

-record(state, {buf :: buffer(),
                owner :: pid(),
                filter :: filter(),
                filter_state :: term()}).

-export([start_link/3, start_link/4, start_link/5, resize/2,
         active/3, notify/1, post/2]).
-export([init/1,
         active/2, passive/2, notify/2,
         handle_event/3, handle_sync_event/4, handle_info/3,
         terminate/3, code_change/4]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% API Function Definitions %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts a new buffer process. The implementation can either
%% be a stack or a queue, depending on which messages will be dropped
%% (older ones or newer ones). Note that stack buffers do not guarantee
%% message ordering.
%% The initial state can be either passive or notify, depending on whether
%% the user wants to get notifications of new messages as soon as possible.
-spec start_link(pid() | atom(), max(), 'stack' | 'queue') -> {ok, pid()}.
start_link(Owner, Size, Type) ->
    start_link(Owner, Size, Type, notify).

%% This one is messy because we have two clauses with 4 values, so we look them
%% up based on guards.
-spec start_link(pid() | atom(), max(), 'stack' | 'queue', 'notify'|'passive') -> {ok, pid()}
      ;         (term(), pid(), max(), stack | queue) -> {ok, pid()}.
start_link(Owner, Size, Type, StateName) when is_pid(Owner);
                                              is_atom(Owner),
                                              is_integer(Size), Size > 0 ->
    gen_fsm:start_link(?MODULE, {Owner, Size, Type, StateName}, []);
start_link(Name, Owner, Size, Type) ->
    start_link(Name, Owner, Size, Type, notify).

-spec start_link(term(), pid(), max(), stack | queue,
                 'notify'|'passive') -> {ok, pid()}.
start_link(Name, Owner, Size, Type, StateName) when Size > 0,
                                                    Type =:= queue orelse
                                                    Type =:= stack orelse
                                                    Type =:= keep_old,
                                                    StateName =:= notify orelse
                                                    StateName =:= passive ->
    gen_fsm:start_link(Name, ?MODULE, {Owner, Size, Type, StateName}, []).

%% @doc Allows to take a given buffer, and make it larger or smaller.
%% A buffer can be made larger without overhead, but it may take
%% more work to make it smaller given there could be a
%% need to drop messages that would now be considered overflow.
-spec resize(pid(), max()) -> ok.
resize(Box, NewSize) when NewSize > 0 ->
    gen_fsm:sync_send_all_state_event(Box, {resize, NewSize}).

%% @doc Forces the buffer into an active state where it will
%% send the data it has accumulated. The fun passed needs to have
%% two arguments: A message, and a term for state. The function can return,
%% for each element, a tuple of the form {Res, NewState}, where `Res' can be:
%% - `{ok, Msg}' to receive the message in the block that gets shipped
%% - `drop' to ignore the message
%% - `skip' to stop removing elements from the stack, and keep them for later.
-spec active(pid(), filter(), State::term()) -> ok.
active(Box, Fun, FunState) when is_function(Fun,2) ->
    gen_fsm:send_event(Box, {active, Fun, FunState}).

%% @doc Forces the buffer into its notify state, where it will send a single
%% message alerting the Owner of new messages before going back to the passive
%% state.
-spec notify(pid()) -> ok.
notify(Box) ->
    gen_fsm:send_event(Box, notify).

%% @doc Sends a message to the PO Box, to be buffered.
-spec post(pid(), term()) -> ok.
post(Box, Msg) ->
    gen_fsm:send_event(Box, {post, Msg}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% gen_fsm Function Definitions %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @private
init({Owner, Size, Type, StateName}) ->
    monitor(process, Owner),
    {ok, StateName, #state{buf = buf_new(Type, Size), owner=Owner}}.

%% @private
active({active, Fun, FunState}, S = #state{}) ->
    {next_state, active, S#state{filter=Fun, filter_state=FunState}};
active(notify, S = #state{}) ->
    {next_state, notify, S#state{filter=undefined, filter_state=undefined}};
active({post, Msg}, S = #state{buf=Buf}) ->
    NewBuf = insert(Msg, Buf),
    send(S#state{buf=NewBuf});
active(_Msg, S = #state{}) ->
    %% unexpected
    {next_state, active, S}.

%% @private
passive(notify, State = #state{buf=Buf}) ->
    case size(Buf) of
        0 -> {next_state, notify, State};
        N when N > 0 -> send_notification(State)
    end;
passive({active, Fun, FunState}, S = #state{buf=Buf}) ->
    NewState = S#state{filter=Fun, filter_state=FunState},
    case size(Buf) of
        0 -> {next_state, active, NewState};
        N when N > 0 -> send(NewState)
    end;
passive({post, Msg}, S = #state{buf=Buf}) ->
    {next_state, passive, S#state{buf=insert(Msg, Buf)}};
passive(_Msg, S = #state{}) ->
    %% unexpected
    {next_state, passive, S}.

%% @private
notify({active, Fun, FunState}, S = #state{buf=Buf}) ->
    NewState = S#state{filter=Fun, filter_state=FunState},
    case size(Buf) of
        0 -> {next_state, active, NewState};
        N when N > 0 -> send(NewState)
    end;
notify(notify, S = #state{}) ->
    {next_state, notify, S};
notify({post, Msg}, S = #state{buf=Buf}) ->
    send_notification(S#state{buf=insert(Msg, Buf)});
notify(_Msg, S = #state{}) ->
    %% unexpected
    {next_state, notify, S}.

%% @private
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%% @private
handle_sync_event({resize, NewSize}, _From, StateName, S=#state{buf=Buf}) ->
    {reply, ok, StateName, S#state{buf=resize_buf(NewSize,Buf)}};
handle_sync_event(_Event, _From, StateName, State) ->
    %% die of starvation, caller!
    {next_state, StateName, State}.

%% @private
handle_info({post, Msg}, StateName, State) ->
    %% We allow anonymous posting and redirect it to the internal form.
    ?MODULE:StateName({post, Msg}, State);
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

send(S=#state{buf = Buf, owner=Pid, filter=Fun, filter_state=FilterState}) ->
    {Msgs, Count, Dropped, NewBuf} = buf_filter(Buf, Fun, FilterState),
    Pid ! {mail, self(), Msgs, Count, Dropped},
    NewState = S#state{buf=NewBuf, filter=undefined, filter_state=undefined},
    {next_state, passive, NewState}.

send_notification(S = #state{owner=Owner}) ->
    Owner ! {mail, self(), new_data},
    {next_state, passive, S}.

%%% Generic buffer ops
-spec buf_new('queue' | 'stack' | 'keep_old', max()) -> buffer().
buf_new(queue, Size) -> #buf{type=queue, max=Size, data=queue:new()};
buf_new(stack, Size) -> #buf{type=stack, max=Size, data=[]};
buf_new(keep_old, Size) -> #buf{type=keep_old, max=Size, data=queue:new()}.

insert(Msg, B=#buf{type=T, max=Size, size=Size, drop=Drop, data=Data}) ->
    B#buf{drop=Drop+1, data=push_drop(T, Msg, Size, Data)};
insert(Msg, B=#buf{type=T, size=Size, data=Data}) ->
    B#buf{size=Size+1, data=push(T, Msg, Data)}.

size(#buf{size=Size}) -> Size.

resize_buf(NewMax, B=#buf{max=Max}) when Max =< NewMax ->
    B#buf{max=NewMax};
resize_buf(NewMax, B=#buf{type=T, size=Size, drop=Drop, data=Data}) ->
    if Size > NewMax ->
        ToDrop = Size - NewMax,
        B#buf{size=NewMax, max=NewMax, drop=Drop+ToDrop,
              data=drop(T, ToDrop, Size, Data)};
       Size =< NewMax ->
        B#buf{max=NewMax}
    end.

buf_filter(Buf=#buf{type=T, drop=D, data=Data, size=C}, Fun, State) ->
    {Msgs, Count, Dropped, NewData} = filter(T, Data, Fun, State),
    {Msgs, Count, Dropped+D, Buf#buf{drop=0, size=C-(Count+Dropped), data=NewData}}.

filter(T, Data, Fun, State) ->
    filter(T, Data, Fun, State, [], 0, 0).

filter(T, Data, Fun, State, Msgs, Count, Drop) ->
    case pop(T, Data) of
        {empty, NewData} ->
            {lists:reverse(Msgs), Count, Drop, NewData};
        {{value,Msg}, NewData} ->
            case Fun(Msg, State) of
                {{ok, Term}, NewState} ->
                    filter(T, NewData, Fun, NewState, [Term|Msgs], Count+1, Drop);
                {drop, NewState} ->
                    filter(T, NewData, Fun, NewState, Msgs, Count, Drop+1);
                skip ->
                    {lists:reverse(Msgs), Count, Drop, Data}
            end
    end.

%% Specific buffer ops
push_drop(keep_old, _Msg, _Size, Data) -> Data;
push_drop(T, Msg, Size, Data) -> push(T, Msg, drop(T, Size, Data)).

drop(T, Size, Data) -> drop(T, 1, Size, Data).

drop(_, 0, _Size, Data) -> Data;
drop(queue, 1, _Size, Queue) -> queue:drop(Queue);
drop(stack, 1, _Size, [_|T]) -> T;
drop(keep_old, 1, _Size, Queue) -> queue:drop_r(Queue);
drop(queue, N, Size, Queue) ->
    if Size > N  -> element(2, queue:split(N, Queue));
       Size =< N -> queue:new()
    end;
drop(stack, N, Size, L) ->
    if Size > N  -> lists:nthtail(N, L);
       Size =< N -> []
    end;
drop(keep_old, N, Size, Queue) ->
    if Size > N  -> element(1, queue:split(N, Queue));
       Size =< N -> queue:new()
    end.

push(queue, Msg, Q) -> queue:in(Msg, Q);
push(stack, Msg, L) -> [Msg|L];
push(keep_old, Msg, Q) -> queue:in(Msg, Q).

pop(queue, Q) -> queue:out(Q);
pop(stack, []) -> {empty, []};
pop(stack, [H|T]) -> {{value,H}, T};
pop(keep_old, Q) -> queue:out(Q).

