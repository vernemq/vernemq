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
%%
-module(vmq_reg_pets).

-behaviour(gen_server).

%% API
-export([start_link/0,
         fold/4]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {status=init,
                event_handler,
                event_prefix,
                event_queue=queue:new()}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

fold(MP, Topic, FoldFun, Acc) when is_list(Topic) ->
    case find_cache_table({MP, Topic}) of
        {ok, T} ->
            {_, _, ResAcc} = ets:foldl(fun fold_/2, {Topic, FoldFun, Acc}, T),
            ResAcc;
        {error, not_found} ->
            Acc
    end.

fold_({{ClientId, QoS}, Pid}, {Topic, FoldFun, Acc}) ->
    NewAcc = FoldFun({Topic, node(), ClientId, QoS, Pid}, Acc),
    {Topic, FoldFun, NewAcc};
fold_({{node, Node}, _}, {Topic, FoldFun, Acc}) ->
    NewAcc = FoldFun({Topic, Node}, Acc),
    {Topic, FoldFun, NewAcc}.



%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    ets:new(?MODULE, [named_table, public, {read_concurrency, true}]),
    Self = self(),
    spawn_link(
      fun() ->
              ResolveQPids = true,
              vmq_reg:fold_subscribers(ResolveQPids,
                                       fun initialize_tables/2, Self),
              Self ! subscribers_loaded
      end),
    {EventPrefix, EventHandler} = vmq_reg:subscribe_subscriber_changes(),
    {ok, #state{event_prefix=EventPrefix, event_handler=EventHandler}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(subscribers_loaded, #state{event_handler=Handler,
                                       event_queue=Q} = State) ->
    lists:foreach(fun(Event) ->
                          handle_event(Handler, Event)
                  end, queue:to_list(Q)),
    lager:info("all subscribers loaded into ~p", [?MODULE]),
    {noreply, State#state{status=ready, event_queue=undefined}};
handle_info({'ETS-TRANSFER', _, _, _}, State) ->
    {noreply, State};
handle_info({Prefix, Event},
            #state{status=init, event_prefix=Prefix, event_queue=Q} = State) ->
    {noreply, State#state{event_queue=queue:in(Event, Q)}};
handle_info({Prefix, Event},
            #state{event_prefix=Prefix, event_handler=Handler} = State) ->
    handle_event(Handler, Event),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
handle_event(Handler, Event) ->
    case Handler(Event) of
        {EvtType, MountPoint, Topic, EventVal} ->
            case emqtt_topic:type(emqtt_topic:new(Topic)) of
                direct ->
                    T = ensure_table_exists(MountPoint, Topic),
                    handle_sub_event(EvtType, T, EventVal);
                wildcard ->
                    ignore
            end;
        ignore ->
            ok
    end.

initialize_tables({MP, Topic, Val}, Parent) ->
    T = ensure_table_exists(MP, Topic, Parent),
    handle_sub_event(subscribe, T, Val),
    Parent.

ensure_table_exists(MountPoint, Topic) ->
    ensure_table_exists(MountPoint, Topic, self()).

ensure_table_exists(MountPoint, Topic, Parent) ->
    case find_cache_table({MountPoint, Topic}) of
        {ok, T} -> T;
        {error, not_found} ->
            T = ets:new(?MODULE, [public, bag, {read_concurrency, true}]),
            ets:insert(?MODULE, {{MountPoint, Topic}, T}),
            case Parent == self() of
                true ->
                    T;
                false ->
                    ets:give_away(T, Parent, ?MODULE),
                    T
            end
    end.

find_cache_table(TableKey) ->
    case ets:lookup(?MODULE, TableKey) of
        [] -> {error, not_found};
        [{_, Tid}] ->
            {ok, Tid}
    end.


handle_sub_event(subscribe, T, {ClientId, QoS, {error, not_found}}) ->
    %% no local process found for client id
    ets:insert(T, {{ClientId, QoS}, undefined});
handle_sub_event(subscribe, T, {ClientId, QoS, QPids}) ->
    Objects = [{{ClientId, QoS}, Pid} || Pid <- QPids],
    ets:insert(T, Objects);
handle_sub_event(subscribe, T, Node) ->
    case ets:insert_new(T, {{node, Node}, 1}) of
        true ->
            ok;
        false ->
            ets:update_counter(T, {node, Node}, 1)
    end;
handle_sub_event(unsubscribe, T, {ClientId, QoS}) ->
    ets:delete(T, {ClientId, QoS}),
    case ets:info(T, size) of
        0 -> ets:delete(T);
        _ -> ok
    end;
handle_sub_event(unsubscribe, T, Node) ->
    case ets:update_counter(T, {node, Node}, -1) of
        R when R =< 0 ->
            ets:delete(T, {node, Node}),
            case ets:info(T, size) of
                0 -> ets:delete(T);
                _ -> ok
            end;
        _ ->
            ok
    end.

