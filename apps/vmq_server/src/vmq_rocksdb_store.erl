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

-module(vmq_rocksdb_store).
-include("vmq_server.hrl").
-behaviour(gen_server).

%% API
-export([start_link/1,
         msg_store_write/2,
         msg_store_read/2,
         msg_store_delete/2,
         msg_store_find/1,
         get_ref/1,
         refcount/1]).

-export([msg_store_init_queue_collector/3]).

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

-record(state, {ref :: rocksdb:db_handle(),
                data_root :: string(),
                open_opts = [],
                config :: config(),
                read_opts = [],
                write_opts = [],
                fold_opts = [{fill_cache, false}],
                refs = ets:new(?MODULE, [])
               }).
-type config() :: [{atom(), term()}].


-define(P_IDX_PRE, 0).
-define(P_MSG_PRE, 0).


%% Subsequent formats should always extend by adding new elements to
%% the end of the record or tuple.
-type p_msg_val_pre() :: {routing_key(), payload()}.
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

msg_store_find(SubscriberId) ->
    Ref = make_ref(),
    {Pid, MRef} = spawn_monitor(?MODULE, msg_store_init_queue_collector,
                                [self(), SubscriberId, Ref]),
    receive
        {'DOWN', MRef, process, Pid, Reason} ->
            {error, Reason};
        {Pid, Ref, Result} ->
            demonitor(MRef, [flush]),
            {_, MsgRefs} = lists:unzip(Result),
            {ok, MsgRefs}
    end.

msg_store_init_queue_collector(ParentPid, SubscriberId, Ref) ->
    Pids = vmq_rocksdb_store_sup:get_bucket_pids(),
    Acc = ordsets:new(),
    ResAcc = msg_store_collect(SubscriberId, Pids, Acc),
    ParentPid ! {self(), Ref, ordsets:to_list(ResAcc)}.

msg_store_collect(_, [], Acc) -> Acc;
msg_store_collect(SubscriberId, [Pid|Rest], Acc) ->
    Res = gen_server:call(Pid, {find_for_subscriber_id, SubscriberId}, infinity),
    msg_store_collect(SubscriberId, Rest, ordsets:union(Res, Acc)).

get_ref(BucketPid) ->
    gen_server:call(BucketPid, get_ref).

refcount(MsgRef) ->
    call(MsgRef, {refcount, MsgRef}).

call(Key, Req) ->
    case vmq_rocksdb_store_sup:get_bucket_pid(Key) of
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

    Opts = vmq_config:get_env(msg_store_opts, []),
    DataDir1 = proplists:get_value(store_dir, Opts, "data/msgstore"),
    DataDir2 = filename:join(DataDir1, integer_to_list(InstanceId)),

    %% Initialize state
    S0 = init_state(DataDir2, Opts),
    process_flag(trap_exit, true),
    case open_db(Opts, S0) of
        {ok, State} ->
            case check_store(State) of
                0 -> ok; % no unreferenced images
                N ->
                    lager:info("found and deleted ~p unreferenced messages in msg store instance ~p",
                               [N, InstanceId])
            end,
            %% Register Bucket Instance with the Bucket Registry
            vmq_rocksdb_store_sup:register_bucket_pid(InstanceId, self()),
            {ok, State};
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
handle_call(get_ref, _From, #state{ref=Ref} = State) ->
    {reply, Ref, State};
handle_call({refcount, MsgRef}, _From, State) ->
    RefCount =
    case ets:lookup(State#state.refs, MsgRef) of
        [] -> 0;
        [{_, Cnt}] -> Cnt
    end,
    {reply, RefCount, State};
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
terminate(_Reason, #state{ref=Ref}) ->
    rocksdb:close(Ref),
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

%% @private
init_state(DataRoot, Config) ->
    %% Get the data root directory
    filelib:ensure_dir(filename:join(DataRoot, "msg_store_dummy")),

    OpenOpts = [{create_if_missing, true},
                {data_dir, DataRoot}],

    FoldOpts = [{fill_cache, false}],

    ReadOpts = [{fill_cache, false}],

    WriteOpts = [{sync, false}],

    %% Generate a debug message with the options we'll use for each operation
    lager:debug("datadir ~s options for RocksDB: ~p\n",
                [DataRoot, [{open, OpenOpts}, {read, ReadOpts}, {write, WriteOpts}, {fold, FoldOpts}]]),
    #state { data_root = DataRoot,
             open_opts = OpenOpts,
             read_opts = ReadOpts,
             write_opts = WriteOpts,
             fold_opts = FoldOpts,
             config = Config }.

open_db(Opts, State) ->
    RetriesLeft = proplists:get_value(open_retries, Opts, 30),
    open_db(Opts, State, max(1, RetriesLeft), undefined).

open_db(_Opts, _State0, 0, LastError) ->
    {error, LastError};
open_db(Opts, State0, RetriesLeft, _) ->
    case rocksdb:open(State0#state.data_root, State0#state.open_opts) of
        {ok, Ref} ->
            {ok, State0#state { ref = Ref }};
        %% Check specifically for lock error, this can be caused if
        %% a crashed instance takes some time to flush rocksdb information
        %% out to disk.  The process is gone, but the NIF resource cleanup
        %% may not have completed.
        {error, {db_open, OpenErr}=Reason} ->
            case lists:prefix("IO error: While lock ", OpenErr) of
                true ->
                    SleepFor = proplists:get_value(open_retry_delay, Opts, 2000),
                    lager:debug("VerneMQ RocksDB backend retrying ~p in ~p ms after error ~s\n",
                                [State0#state.data_root, SleepFor, OpenErr]),
                    timer:sleep(SleepFor),
                    open_db(Opts, State0, RetriesLeft - 1, Reason);
                false ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.


handle_req({write, {MP, _} = SubscriberId,
            #vmq_msg{msg_ref=MsgRef, mountpoint=MP, dup=Dup, qos=QoS,
                     routing_key=RoutingKey, payload=Payload}},
           #state{ref=Bucket, refs=Refs, write_opts=WriteOpts}) ->
    MsgKey = sext:encode({msg, MsgRef, {MP, ''}}),
    RefKey = sext:encode({msg, MsgRef, SubscriberId}),
    IdxKey = sext:encode({idx, SubscriberId, MsgRef}),
    IdxVal = serialize_p_idx_val_pre(#p_idx_val{ts=os:timestamp(), dup=Dup, qos=QoS}),
    case incr_ref(Refs, MsgRef) of
        1 ->
            %% new message
            Val = serialize_p_msg_val_pre({RoutingKey, Payload}),
            rocksdb:write(Bucket, [{put, MsgKey, Val},
                                   {put, RefKey, <<>>},
                                   {put, IdxKey, IdxVal}], WriteOpts);
        _ ->
            %% only write ref
            rocksdb:write(Bucket, [{put, RefKey, <<>>},
                                   {put, IdxKey, IdxVal}], WriteOpts)
    end;
handle_req({read, {MP, _} = SubscriberId, MsgRef},
           #state{ref=Bucket, read_opts=ReadOpts}) ->
    MsgKey = sext:encode({msg, MsgRef, {MP, ''}}),
    IdxKey = sext:encode({idx, SubscriberId, MsgRef}),
    case rocksdb:get(Bucket, MsgKey, ReadOpts) of
        {ok, Val} ->
            {RoutingKey, Payload} = parse_p_msg_val_pre(Val),
            case rocksdb:get(Bucket, IdxKey, ReadOpts) of
                {ok, IdxVal} ->
                    #p_idx_val{dup=Dup, qos=QoS} = parse_p_idx_val_pre(IdxVal),
                    Msg = #vmq_msg{msg_ref=MsgRef, mountpoint=MP, dup=Dup, qos=QoS,
                                   routing_key=RoutingKey, payload=Payload, persisted=true},
                    {ok, Msg};
                not_found ->
                    {error, idx_val_not_found}
            end;
        not_found ->
            {error, not_found}
    end;
handle_req({delete, {MP, _} = SubscriberId, MsgRef},
           #state{ref=Bucket, refs=Refs, write_opts=WriteOpts}) ->
    MsgKey = sext:encode({msg, MsgRef, {MP, ''}}),
    RefKey = sext:encode({msg, MsgRef, SubscriberId}),
    IdxKey = sext:encode({idx, SubscriberId, MsgRef}),
    case decr_ref(Refs, MsgRef) of
        not_found ->
            lager:warning("delete failed ~p due to not found", [MsgRef]);
        0 ->
            %% last one to be deleted
            rocksdb:write(Bucket, [{delete, RefKey},
                                   {delete, IdxKey},
                                   {delete, MsgKey}], WriteOpts);
        _ ->
            %% we have to keep the message, but can delete the ref and idx
            rocksdb:write(Bucket, [{delete, RefKey},
                                   {delete, IdxKey}], WriteOpts)
    end;
handle_req({find_for_subscriber_id, SubscriberId},
           #state{ref=Bucket, fold_opts=FoldOpts} = State) ->
    {ok, Itr} = rocksdb:iterator(Bucket, FoldOpts),
    FirstIdxKey = sext:encode({idx, SubscriberId, ''}),
    iterate_index_items(rocksdb:iterator_move(Itr, FirstIdxKey),
                        SubscriberId, ordsets:new(), Itr, State).

iterate_index_items({error, _}, _, Acc, _, _) ->
    %% no need to close the iterator
    Acc;
iterate_index_items({ok, IdxKey, IdxVal}, SubscriberId, Acc, Itr, State) ->
    case sext:decode(IdxKey) of
        {idx, SubscriberId, MsgRef} ->
            #p_idx_val{ts=TS} = parse_p_idx_val_pre(IdxVal),
            iterate_index_items(rocksdb:iterator_move(Itr, next), SubscriberId,
                                ordsets:add_element({TS, MsgRef}, Acc), Itr, State);
        _ ->
            %% all message refs accumulated for this subscriber
            rocksdb:iterator_close(Itr),
            Acc
    end.

check_store(#state{ref=Bucket, fold_opts=FoldOpts, write_opts=WriteOpts,
                   refs=Refs}) ->
    {{LastPivotMsgRef, LastPivotMP, LastHadRefs}, NewN} =
    rocksdb:fold_keys(
      Bucket,
      fun(Key, {{PivotMsgRef, PivotMP, HadRefs} = Pivot, N}) ->
              case sext:decode(Key) of
                  {msg, PivotMsgRef, {PivotMP, ClientId}} when ClientId =/= '' ->
                      %% Inside the 'same' Message Section. This means we have found refs
                      %% for this message -> no delete required.
                      {{PivotMsgRef, PivotMP, true}, N};
                  {msg, NewPivotMsgRef, {NewPivotMP, ''}} when HadRefs ->
                      %% New Message Section started, previous section included refs
                      %% -> no delete required
                      {{NewPivotMsgRef, NewPivotMP, false}, N}; %% challenge the new message
                  {msg, NewPivotMsgRef, {NewPivotMP, ''}} -> % HadRefs == false
                      %% New Message Section started, previous section didn't include refs
                      %% -> delete required
                      rocksdb:write(Bucket, [{delete, Key}], WriteOpts),
                      {{NewPivotMsgRef, NewPivotMP, false}, N + 1}; %% challenge the new message
                  {idx, _, MsgRef} ->
                      incr_ref(Refs, MsgRef),
                      {Pivot, N};
                  Entry ->
                      lager:warning("inconsistent message store entry detected: ~p", [Entry]),
                      {Pivot, N}
              end
      end, {{undefined, undefined, true}, 0}, FoldOpts),
    case LastHadRefs of
        false ->
            % delete last message
            Key = sext:encode({msg, LastPivotMsgRef, {LastPivotMP, ''}}),
            rocksdb:write(Bucket, [{delete, Key}], WriteOpts),
            NewN + 1;
        _ ->
            NewN
    end.

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

parse_p_msg_val_pre_({RoutingKey, Payload}) ->
    {RoutingKey, Payload};
%% newer version of the msg value
parse_p_msg_val_pre_(T) when is_integer(element(1, T)),
                             element(1,T) > ?P_MSG_PRE ->
    {element(2, T),
     element(3, T)}.

%% current version of the msg value
-spec serialize_p_msg_val_pre(p_msg_val_pre()) -> binary().
serialize_p_msg_val_pre({_RoutingKey, _Payload} = T) ->
    term_to_binary(T);
serialize_p_msg_val_pre(T) when is_integer(element(1, T)),
                                element(1,T) > ?P_MSG_PRE ->
    term_to_binary(
      {element(2, T),
       element(3, T)}).
