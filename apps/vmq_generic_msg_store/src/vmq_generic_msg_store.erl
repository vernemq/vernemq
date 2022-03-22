%% Copyright 2018 Erlio GmbH Basel Switzerland (http://erl.io)
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

-module(vmq_generic_msg_store).
-include("vmq_generic_msg_store.hrl").
-behaviour(gen_server).

%% API
-export([start_link/1,
         msg_store_write/2,
         msg_store_read/2,
         msg_store_delete/2,
         msg_store_find/2,
         get_engine/1,
         refcount/1,
         get_state/1]).

-export([msg_store_init_queue_collector/4]).

-export([parse_p_idx_val_pre/1,
         parse_p_msg_val_pre/1,
         serialize_p_idx_val_pre/1,
         serialize_p_msg_val_pre/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
          engine,
          engine_module,
          refs = ets:new(?MODULE, [])
         }).


-define(P_IDX_PRE, 0).
-define(P_MSG_PRE, 0).


%% Subsequent formats should always extend by adding new elements to
%% the end of the record or tuple.
-type p_msg_val_pre() :: {routing_key(), payload() | tuple()}.
-record(p_idx_val, {
          ts  :: erlang:timestamp(),
          dup :: flag(),
          qos :: qos()
         }).
-type p_idx_val_pre() :: #p_idx_val{}.

%%%===================================================================
%%% API
%%%===================================================================
start_link(Id) ->
    gen_server:start_link(?MODULE, [Id], []).

msg_store_write(SubscriberId, #vmq_msg{msg_ref=MsgRef} = Msg) ->
    call(MsgRef, {write, SubscriberId, Msg}).

msg_store_delete(SubscriberId, MsgRef) ->
    call(MsgRef, {delete, SubscriberId, MsgRef}).

msg_store_read(SubscriberId, MsgRef) ->
    call(MsgRef, {read, SubscriberId, MsgRef}).


%% We differentiate between queue_init and other as queue_init must
%% not be called concurrently for the same subscriber.
msg_store_find(SubscriberId, Type) when Type =:= queue_init;
                                        Type =:= other ->
    Ref = make_ref(),
    {Pid, MRef} = spawn_monitor(?MODULE, msg_store_init_queue_collector,
                                [self(), SubscriberId, Ref, Type]),
    receive
        {'DOWN', MRef, process, Pid, Reason} ->
            {error, Reason};
        {Pid, Ref, MsgRefs} ->
            demonitor(MRef, [flush]),
            {ok, MsgRefs}
    end.

msg_store_init_queue_collector(ParentPid, SubscriberId, Ref, queue_init) ->
    MsgRefs =
    case msg_store_init_from_tbl(init, SubscriberId) of
        [] ->
            init_from_disk(SubscriberId);
        Res ->
            Res
    end,
    ParentPid ! {self(), Ref, MsgRefs};
msg_store_init_queue_collector(ParentPid, SubscriberId, Ref, other) ->
    MsgRefs = init_from_disk(SubscriberId),
    ParentPid ! {self(), Ref, MsgRefs}.

init_from_disk(SubscriberId) ->
    TblIdxRef = make_ref(),
    Pids = vmq_generic_msg_store_sup:get_bucket_pids(),
    ok = msg_store_collect(TblIdxRef, SubscriberId, Pids),
    msg_store_init_from_tbl(TblIdxRef, SubscriberId).

msg_store_init_from_tbl(Prefix, SubscriberId) ->
    MS = ets:fun2ms(
           fun({{P, S, _TS, MsgRef}}) when P =:= Prefix, S =:= SubscriberId ->
                   MsgRef
           end),
    MSDel = ets:fun2ms(
              fun({{P, S, _TS, _MsgRef}}) when P =:= Prefix, S =:= SubscriberId ->
                      true
              end),
    Table = select_table(SubscriberId),
    MsgRefs = ets:select(Table, MS),
    _Deleted = ets:select_delete(Table, MSDel),
    MsgRefs.

msg_store_collect(_Ref, _, []) -> ok;
msg_store_collect(Ref, SubscriberId, [Pid|Rest]) ->
    ok = try
             gen_server:call(Pid, {find_for_subscriber_id, Ref, SubscriberId}, infinity)
         catch
             {'EXIT', {noproc, _}} ->
                 ok
         end,
    msg_store_collect(Ref, SubscriberId, Rest).

get_engine(BucketPid) ->
    gen_server:call(BucketPid, get_engine).

refcount(MsgRef) ->
    call(MsgRef, {refcount, MsgRef}).

get_state(Pid) ->
    gen_server:call(Pid, get_state, infinity).

call(Key, Req) ->
    case vmq_generic_msg_store_sup:get_bucket_pid(Key) of
        {ok, BucketPid} ->
            gen_server:call(BucketPid, Req, infinity);
        {error, Reason} ->
            {error, Reason}
    end.

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
init([InstanceId]) ->
    %% Initialize random seed
    rand:seed(exsplus, os:timestamp()),

    {ok, EngineModule} = application:get_env(vmq_generic_msg_store, msg_store_engine),
    Opts = application:get_env(vmq_generic_msg_store, msg_store_opts, []),
    DataDir1 = proplists:get_value(store_dir, Opts, "data/msgstore"),
    DataDir2 = filename:join(DataDir1, integer_to_list(InstanceId)),

    process_flag(trap_exit, true),
    case apply(EngineModule, open, [DataDir2, Opts]) of
        {ok, EngineState} ->
            self() ! {initialize_from_storage, InstanceId},
            {ok, #state{engine=EngineState, engine_module=EngineModule}};
        {error, Reason} ->
            {stop, Reason}
    end.

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
handle_call(get_engine, _From, #state{engine=EngineState, engine_module=EngineModule} = State) ->
    {reply, {EngineModule, EngineState}, State};
handle_call({refcount, MsgRef}, _From, State) ->
    RefCount =
    case ets:lookup(State#state.refs, MsgRef) of
        [] -> 0;
        [{_, Cnt}] -> Cnt
    end,
    {reply, RefCount, State};
handle_call(get_state, _From, State) ->
    %% when called externally, the store is always initialized, so
    %% just return that here.
    {reply, initialized, State};
handle_call(Request, _From, State) ->
    {reply, handle_req(Request, State), State}.

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
handle_cast(_Request, State) ->
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
handle_info({initialize_from_storage, InstanceId}, State) ->
    case setup_index(State) of
        0 -> ok;
        N ->
            lager:info("indexed ~p offline messages in msg store instance ~p",
                       [N, InstanceId])
    end,
    %% Register Bucket Instance with the Bucket Registry
    vmq_generic_msg_store_sup:register_bucket_pid(InstanceId, self()),
    {noreply, State};
handle_info(_Info, State) ->
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
terminate(_Reason, #state{engine=EngineState, engine_module=EngineModule}) ->
    apply(EngineModule, close, [EngineState]),
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
handle_req({write, {MP, _} = SubscriberId,
            #vmq_msg{msg_ref=MsgRef, mountpoint=MP, dup=Dup, qos=QoS,
                     routing_key=RoutingKey, properties=Properties, payload=Payload}},
           #state{engine=EngineState, engine_module=EngineModule, refs=Refs}) ->
    MsgKey = sext:encode({msg, MsgRef, {MP, ''}}),
    IdxKey = sext:encode({idx, SubscriberId, MsgRef}),
    IdxVal = serialize_p_idx_val_pre(#p_idx_val{ts=os:timestamp(), dup=Dup, qos=QoS}),
    case incr_ref(Refs, MsgRef) of
        1 ->
            %% new message
	    %% serialize payload and properties as a tuple
            Val = serialize_p_msg_val_pre({RoutingKey, {Payload, Properties}}),
            apply(EngineModule, write, [EngineState, [{put, MsgKey, Val},
                                                      {put, IdxKey, IdxVal}]]);
        _ ->
            %% only write the idx
            apply(EngineModule, write, [EngineState, [{put, IdxKey, IdxVal}]])
    end;
handle_req({read, {MP, _} = SubscriberId, MsgRef},
           #state{engine=EngineState, engine_module=EngineModule}) ->
    MsgKey = sext:encode({msg, MsgRef, {MP, ''}}),
    IdxKey = sext:encode({idx, SubscriberId, MsgRef}),
    case apply(EngineModule, read, [EngineState, MsgKey]) of
        {ok, Val} ->
            {RoutingKey, Persisted} = parse_p_msg_val_pre(Val),
	    if is_binary(Persisted) -> %% legacy behaviour was to just persist the payload
		       {Payload, Properties} = {Persisted, {}};
		true ->
		       {Payload, Properties} = Persisted
	    end,

            case apply(EngineModule, read, [EngineState, IdxKey]) of
                {ok, IdxVal} ->
                    #p_idx_val{dup=Dup, qos=QoS} = parse_p_idx_val_pre(IdxVal),
                    Msg = #vmq_msg{msg_ref=MsgRef, mountpoint=MP, dup=Dup, qos=QoS,
                                   routing_key=RoutingKey, properties=Properties, payload=Payload, persisted=true},
                    {ok, Msg};
                not_found ->
                    {error, idx_val_not_found}
            end;
        not_found ->
            {error, not_found}
    end;
handle_req({delete, {MP, _} = SubscriberId, MsgRef},
           #state{refs=Refs, engine=EngineState, engine_module=EngineModule}) ->
    MsgKey = sext:encode({msg, MsgRef, {MP, ''}}),
    IdxKey = sext:encode({idx, SubscriberId, MsgRef}),
    case decr_ref(Refs, MsgRef) of
        not_found ->
            lager:warning("delete failed ~p due to not found", [MsgRef]);
        0 ->
            %% last one to be deleted
            apply(EngineModule, write, [EngineState, [{delete, IdxKey},
                                                      {delete, MsgKey}]]);
        _ ->
            %% we have to keep the message, but can delete the idx
            apply(EngineModule, write, [EngineState, [{delete, IdxKey}]])
    end;
handle_req({find_for_subscriber_id, TblIdxRef, SubscriberId},
           #state{engine=EngineState, engine_module=EngineModule}) ->
    FirstIdxKey = sext:encode({idx, SubscriberId, ''}),
    apply(EngineModule, fold,
          [EngineState,
           fun(IdxKey, IdxVal, _Acc) ->
                   case sext:decode(IdxKey) of
                       {idx, SubscriberId, MsgRef} ->
                           #p_idx_val{ts=TS} = parse_p_idx_val_pre(IdxVal),
                           Table = select_table(SubscriberId),
                           true = ets:insert(Table, {{TblIdxRef, SubscriberId, TS, MsgRef}});
                       _ ->
                           %% all message refs accumulated for this subscriber
                           throw(finished)
                   end
           end, ignore, FirstIdxKey]),
    ok.

setup_index(#state{engine=EngineState, engine_module=EngineModule, refs=Refs}) ->
    FirstIdxKey = sext:encode({idx, '', ''}),
    apply(EngineModule, fold,
          [EngineState,
           fun(Key, IdxVal, N) ->
                   case sext:decode(Key) of
                       {idx, SubscriberId, MsgRef} ->
                           #p_idx_val{ts=TS} = parse_p_idx_val_pre(IdxVal),
                           Table = select_table(SubscriberId),
                           true = ets:insert(Table, {{init, SubscriberId, TS, MsgRef}}),
                           incr_ref(Refs, MsgRef),
                           N + 1;
                       _ ->
                           throw(finished)
                   end
           end, 0, FirstIdxKey]).

incr_ref(Refs, MsgRef) ->
    case ets:insert_new(Refs, {MsgRef, 1}) of
        true -> 1;
        false ->
            ets:update_counter(Refs, MsgRef, 1)
    end.

decr_ref(Refs, MsgRef) ->
    try ets:update_counter(Refs, MsgRef, -1) of
        0 ->
            ets:delete(Refs, MsgRef),
            0;
        V ->
            V
    catch
        _:_ ->
            not_found
    end.

select_table(SubscriberId) ->
    persistent_term:get({?TBL_MSG_INIT, erlang:phash2(SubscriberId, ?NR_OF_BUCKETS) + 1}).

%% pre version idx:
%% {p_idx_val, ts, dup, qos}
%% future version:
%% {p_idx_val, vesion, ts, dup, qos, ...}

%% current version of the index value
-spec parse_p_idx_val_pre(binary()) -> p_idx_val_pre().
parse_p_idx_val_pre(BinTerm) ->
    parse_p_idx_val_pre_(binary_to_term(BinTerm)).

parse_p_idx_val_pre_({TS, Dup, QoS}) ->
    #p_idx_val{ts=TS, dup=Dup, qos=QoS};
%% newer versions of the store -> downgrade
parse_p_idx_val_pre_(T) when element(1,T) =:= p_idx_val,
                             is_integer(element(2, T)),
                             element(2,T) > ?P_IDX_PRE ->
    TS = element(3, T),
    Dup = element(4, T),
    QoS = element(5, T),
    #p_idx_val{ts=TS, dup=Dup, qos=QoS}.

%% current version of the index value
-spec serialize_p_idx_val_pre(p_idx_val_pre()) -> binary().
serialize_p_idx_val_pre(#p_idx_val{ts=TS, dup=Dup, qos=QoS}) ->
    term_to_binary({TS, Dup, QoS});
serialize_p_idx_val_pre(T) when element(1,T) =:= p_idx_val,
                                is_integer(element(2, T)),
                                element(2,T) > ?P_MSG_PRE ->
    term_to_binary(
      {element(3, T),
       element(4, T),
       element(5, T)}).

%% pre msg version:
%% {routing_key, payload}
%% future version:
%% {version, routing_key, payload, ...}

%% parse messages to message type from before versioning.
-spec parse_p_msg_val_pre(binary()) -> p_msg_val_pre().
parse_p_msg_val_pre(BinTerm) ->
    parse_p_msg_val_pre_(binary_to_term(BinTerm)).

parse_p_msg_val_pre_({RoutingKey, Persisted}) ->
    {RoutingKey, Persisted};
%% newer version of the msg value
parse_p_msg_val_pre_(T) when is_integer(element(1, T)),
                             element(1,T) > ?P_MSG_PRE ->
    {element(2, T),
     element(3, T)}.

%% current version of the msg value
-spec serialize_p_msg_val_pre(p_msg_val_pre()) -> binary().
serialize_p_msg_val_pre({_RoutingKey, _Persisted} = T) ->
    term_to_binary(T);
serialize_p_msg_val_pre(T) when is_integer(element(1, T)),
                                element(1,T) > ?P_MSG_PRE ->
    term_to_binary(
      {element(2, T),
       element(3, T)}).
