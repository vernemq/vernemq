%% Copyright 2018 Octavo Labs AG Zurich Switzerland (https://octavolabs.com)
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

-module(vmq_swc_store).
-include("vmq_swc.hrl").
-behaviour(gen_server).

-export([start_link/1,
         write/3,
         write/4,
         write_batch/2,
         read/2,
         fold_values/4,
         subscribe/3,
         dump/1,
         watermark/1,

         process_batch/2,

         lock/1,
         remote_lock/2,
         rpc_lock/2,

         unlock/1,
         remote_unlock/2,
         rpc_unlock/2,

         sync_missing/2,
         remote_sync_missing/3,
         rpc_sync_missing/3,

         sync_repair/4,
         remote_sync_repair/4,
         rpc_sync_repair/4,

         recover_objects/5,

         node_clock/1,
         remote_node_clock/2,
         rpc_node_clock/1,

         update_watermark/3,
         remote_update_watermark/3,
         rpc_update_watermark/3,

         do_gc/2,
         set_group_members/2,

         rpc_broadcast/2
        ]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% for testing only.
-export([set_broadcast/2]).

-record(state,
        {id,
         mode,
         config,
         idx_name,
         bvv, %% node clock
         kvv, %% watermark
         peers,
         sync_lock,
         auto_gc           :: boolean(),
         subscriptions=#{},
         broadcast_enabled :: boolean()
        }).

-define(SERVER, ?MODULE).
-define(DB_REFS, vmq_swc_db_refs).
%-define(DBG_OP(Str, Format), io:format(Str, Format)).
-define(DBG_OP(_Str, _Format), ok).

start_link(#swc_config{store=StoreName} = Config) ->
    gen_server:start_link({local, StoreName}, ?MODULE, [Config], []).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Store API

write(Config, Key, Value) ->
    write(Config, Key, Value, swc_vv:new()).

write(Config, Key, Value, Context) ->
    write_batch(Config, [{Key, Value, Context}]).

write_batch(Config, [{_Key, _Val, _Context}|_] = WriteOps) ->
    enqueue_op_sync(Config, {write, WriteOps}).

read(Config, Key) ->
    SKey = sext:encode(Key),
    case get_dcc_for_key(Config, SKey) of
        {Vs,C} = Empty when (map_size(Vs) == 0) and map_size(C) == 0 ->
            Empty;
        DCC0 ->
            BVV=get_bvv(Config),
            DCC1 = swc_kv:fill(DCC0, BVV),
            Values = swc_kv:values(DCC1),
            Context = swc_kv:context(DCC1),
            {Values, Context}
    end.

fold_values(Config, Fun, Acc, FullPrefix) ->
    SFirstKey = sext:encode({FullPrefix, '_'}),
    Iterator = vmq_swc_db:iterator(Config, vmq_swc_db:backend(), dcc, SFirstKey),
    BVV=get_bvv(Config),
    db_foldl(fun(SKey, BValue, AccAcc) ->
                     case sext:decode(SKey) of
                         {FullPrefix, _Key} = PKey ->
                             DCC0 = binary_to_term(BValue),
                             DCC1 = swc_kv:fill(DCC0, BVV),
                             Values = swc_kv:values(DCC1),
                             Context = swc_kv:context(DCC1),
                             Fun(PKey, {Values, Context}, AccAcc);
                         _ ->
                             stop
                     end
             end, Acc, Iterator).

subscribe(#swc_config{store=StoreName}, FullPrefix, ConvertFun) ->
    gen_server:call(StoreName, {subscribe, FullPrefix, ConvertFun, self()}, infinity).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Sync API, used by vmq_swc_exchange_fsm

lock(#swc_config{peer=OriginPeer} = Config) ->
    % local lock
    lock(Config, OriginPeer).

lock(#swc_config{store=StoreName}, OriginPeer) ->
    gen_server:call(StoreName, {lock, OriginPeer}, infinity).

remote_lock(#swc_config{transport=TMod, peer=Peer} = Config, RemotePeer) ->
    TMod:rpc(Config, RemotePeer, ?MODULE, rpc_lock, [Peer]).

rpc_lock(RemotePeer, #swc_config{} = Config) ->
    lock(Config, RemotePeer).

unlock(#swc_config{peer=OriginPeer} = Config) ->
    unlock(Config, OriginPeer).

unlock(#swc_config{store=StoreName}, Peer) ->
    gen_server:call(StoreName, {unlock, Peer}, infinity).

remote_unlock(#swc_config{transport=TMod, peer=Peer} = Config, RemotePeer) ->
    TMod:rpc(Config, RemotePeer, ?MODULE, rpc_unlock, [Peer]).

rpc_unlock(RemotePeer, #swc_config{} = Config) ->
    unlock(Config, RemotePeer).

sync_missing(#swc_config{peer=Peer} = Config, Dots) ->
    sync_missing(Config, Peer, Dots).

sync_missing(#swc_config{store=StoreName}, OriginPeer, Dots) ->
    gen_server:call(StoreName, {sync_missing, OriginPeer, Dots}, infinity).

remote_sync_missing(#swc_config{transport=TMod, peer=OriginPeer} = Config, RemotePeer, Dots) ->
    TMod:rpc(Config, RemotePeer, ?MODULE, rpc_sync_missing, [OriginPeer, Dots]).

rpc_sync_missing(OriginPeer, Dots, #swc_config{} = Config) ->
    sync_missing(Config, OriginPeer, Dots).


sync_repair(#swc_config{peer=OriginPeer} = Config, MissingObjects, RemotePeer, RemotePeerBVV) ->
    sync_repair(Config, OriginPeer, MissingObjects, RemotePeer, RemotePeerBVV).

sync_repair(#swc_config{store=StoreName}, OriginPeer, MissingObjects, RemotePeer, RemotePeerBVV) ->
    gen_server:call(StoreName, {sync_repair, OriginPeer, RemotePeer, RemotePeerBVV, MissingObjects}, infinity).

remote_sync_repair(#swc_config{transport=TMod, peer=Peer} = Config, MissingObjects, RemotePeer, PeerBVV) ->
   TMod:rpc(Config, RemotePeer, ?MODULE, rpc_sync_repair, [MissingObjects, Peer, PeerBVV]).

rpc_sync_repair(MissingObjects, RemotePeer, RemotePeerBVV, Config) ->
    sync_repair(Config, RemotePeer, MissingObjects, RemotePeer, RemotePeerBVV).

recover_objects(#swc_config{store=StoreName}, RemotePeer, RemoteClock, MissingObjects, RemoteWatermark) ->
    gen_server:call(StoreName, {recover_objects, RemotePeer, RemoteClock, MissingObjects, RemoteWatermark}, infinity).

node_clock(#swc_config{store=StoreName}) ->
    gen_server:call(StoreName, get_node_clock, infinity).

remote_node_clock(#swc_config{transport=TMod} = Config, RemotePeer) ->
    TMod:rpc(Config, RemotePeer, ?MODULE, rpc_node_clock, []).

rpc_node_clock(#swc_config{} = Config) ->
    node_clock(Config).


update_watermark(#swc_config{peer=Peer} = Config, RemotePeer, RemoteNodeClock) ->
    update_watermark(Config, Peer, RemotePeer, RemoteNodeClock).

update_watermark(#swc_config{store=StoreName}, OriginPeer, RemotePeer, RemoteNodeClock) ->
    gen_server:call(StoreName, {update_watermark, OriginPeer, RemotePeer, RemoteNodeClock}, infinity).

remote_update_watermark(#swc_config{transport=TMod, peer=Peer} = Config, RemotePeer, NodeClock) ->
    TMod:rpc(Config, RemotePeer, ?MODULE, rpc_update_watermark, [Peer, NodeClock]).

rpc_update_watermark(RemotePeer, RemoteNodeClock, #swc_config{} = Config) ->
    update_watermark(Config, RemotePeer, RemotePeer, RemoteNodeClock).






rpc_broadcast(Msg, #swc_config{store=StoreName} = _Config) ->
    gen_server:cast(StoreName, {swc_broadcast, Msg}).

watermark(Config) ->
    get_kvv(Config).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Misc API

do_gc(StoreName, Node) ->
    gen_server:call({StoreName, Node}, do_gc, infinity).

dump(StoreName) ->
    gen_server:call(StoreName, dump, infinity).


process_batch(StoreName, Batch) ->
    gen_server:call(StoreName, {batch, Batch}, infinity).

set_broadcast(#swc_config{store=StoreName}, IsBroadcastEnabled) when is_boolean(IsBroadcastEnabled) ->
    gen_server:call(StoreName, {set_broadcast, IsBroadcastEnabled}).

set_group_members(#swc_config{store=StoreName}, Members) ->
    gen_server:cast(StoreName, {set_group_members, Members}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%5
%%% GEN_SERVER Callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%5
init([#swc_config{peer=Peer, store=StoreName} = Config]) ->
    StartSync = application:get_env(vmq_swc, sync_interval, {5000, 2500}),
    StartSync =/= 0 andalso erlang:send_after(1000, self(), sync),

    BVV = get_bvv(Config),
    Peers = swc_node:ids(BVV) -- [Peer],

    KVV0 = get_kvv(Config),

    KVV1 = fix_watermark(KVV0, [Peer|Peers]),

    IdxName = list_to_atom(atom_to_list(StoreName) ++ "_index"),
    init_log_index(Config, IdxName),

    IsBroadcastEnabled = application:get_env(vmq_swc, enable_broadcast, true),

    {ok, #state{id=Peer,
                config=Config,
                idx_name=IdxName,
                bvv=get_bvv(Config),
                kvv=KVV1,
                peers = Peers,
                auto_gc = application:get_env(vmq_swc, auto_gc, true),
                broadcast_enabled = IsBroadcastEnabled
               }}.

handle_call({batch, Batch}, _From, #state{config=Config,
                                          peers=Peers,
                                          broadcast_enabled=IsBroadcastEnabled} = State0) ->
    {ReplicateObjects, DbOps, Events, #state{bvv=BVV} = State1} =
    lists:foldl(fun({{CallerPid, CallerRef}, {write, WriteOps}}, Acc0) ->
                        Acc1 =
                        lists:foldl(fun(WriteOp, AccAcc0) ->
                                            process_write_op(WriteOp, AccAcc0)
                                    end, Acc0, WriteOps),
                        CallerPid ! {CallerRef, ok},
                        Acc1
                end, {[], [], [], State0}, Batch),
    UpdateBVV_DBOp = update_bvv_db_op(BVV),
    db_write(Config, [UpdateBVV_DBOp | DbOps]),
    case IsBroadcastEnabled of
        true ->
            #swc_config{transport=TMod} = Config,
            lists:foreach(
              fun(Peer) ->
                      TMod:rpc_cast(Config, Peer, ?MODULE, rpc_broadcast, [ReplicateObjects])
              end, Peers);
        _ ->
            ok
    end,
    trigger_events(Events),
    {reply, ok, State1};

handle_call({subscribe, FullPrefix, ConvertFun, Pid}, _From, #state{subscriptions=Subs0} = State0) ->
    SubsForPrefix = maps:get(FullPrefix, Subs0, []),
    Subs1 = maps:put(FullPrefix, [{Pid, ConvertFun}|SubsForPrefix], Subs0),
    {reply, ok, State0#state{subscriptions=Subs1}};

handle_call({lock, Peer}, _From, #state{id=Id, peers=Peers, sync_lock=SyncLock} = State0) ->
    case lists:member(Peer, [Id|Peers]) of
        true when SyncLock == undefined ->
            NewSyncLock = init_sync_lock(Peer),
            {reply, ok, State0#state{sync_lock=NewSyncLock}};
        true ->
            {reply, {error, already_locked}, State0};
        false ->
            {reply, {error, not_cluster_member}, State0}
    end;

handle_call({unlock, Peer}, _From, #state{sync_lock={Peer, _} = SyncLock0} = State0) ->
    % early release of the lock, we don't need to wait until the timer fires
    SyncLock1 = release_sync_lock(SyncLock0),
    {reply, ok, State0#state{sync_lock=SyncLock1}};

handle_call({unlock, Peer}, _From, State) ->
    lager:warning("can't unlock sync lock for ~p, lock is ~p", [Peer, State#state.sync_lock]),
    {reply, {error, invalid_lock}, State};

handle_call({sync_missing, OriginPeer, Dots}, From, #state{config=Config, sync_lock={OriginPeer, _} = SyncLock0} = State0) ->
    SyncLock1 = refresh_sync_lock(SyncLock0),
    spawn_link(
      fun() ->
              Result =
              lists:map(
                fun(Dot) ->
                        DotKey = sext:encode(Dot),
                        case vmq_swc_db:get(Config, log, DotKey) of
                            not_found ->
                                %% this case should never occur as
                                %% this would mean we have a dot
                                %% without an entry in the
                                %% dot-key-map!
                                not_found;
                            {ok, SKey} ->
                                case vmq_swc_db:get(Config, dcc, SKey) of
                                    not_found ->
                                        %% the object was deleted in
                                        %% the past, replace it with
                                        %% an empty object which will
                                        %% become the delete-marker on
                                        %% the other node.
                                        {SKey, swc_kv:new()};
                                    {ok, BDCC} ->
                                        {SKey, binary_to_term(BDCC)}
                                end
                        end
                end, Dots),
              gen_server:reply(From, Result)
      end),
    {noreply, State0#state{sync_lock=SyncLock1}};

handle_call({sync_missing, _, _}, _From, State) ->
    {reply, {error, not_locked}, State};

handle_call(get_node_clock, _From, #state{bvv=BVV} = State) ->
    {reply, BVV, State};

handle_call({update_watermark, OriginPeer, Node, NodeClock}, _From, #state{config=Config, sync_lock={OriginPeer, _} = SyncLock0} = State0) ->
    SyncLock1 = refresh_sync_lock(SyncLock0),
    KVV1 = update_watermark_internal(Node, NodeClock, State0),
    UpdateKVV_DBop = update_kvv_db_op(KVV1),
    db_write(Config, [UpdateKVV_DBop]),
    {reply, ok, State0#state{kvv=KVV1, sync_lock=SyncLock1}};

handle_call({update_watermark, _, _, _}, _From, State) ->
    {reply, {error, not_locked}, State};

handle_call({sync_repair, OriginPeer, TargetNode, TargetNodeBVV, MissingObjects}, _From,
            #state{config=Config, idx_name=IdxName, peers=Peers, bvv=BVV0, sync_lock={OriginPeer, _} = SyncLock0} = State0) ->
    % sync repair step! TODO: what happens if exchange_fsm dies before receiving ALL missing objects.

    % replace the current entry in the node clock for the responding clock with
    % the current knowledge it's receiving
    TargetNodeBVV1 = maps:filter(fun(Id, _) -> Id == TargetNode end, TargetNodeBVV),
    BVV1 = swc_node:merge(BVV0, TargetNodeBVV1),
    % get the local objects corresponding to the received objects and fill the causal history for all of tem
    FilledObjects =
    [{SKey, swc_kv:fill(DCC, TargetNodeBVV), swc_kv:fill(get_dcc_for_key(Config, SKey), BVV0)}
     || {SKey, DCC} <- MissingObjects],
    % synchronize / merge the remote and local objects
    SyncedObjects =
    [{SKey, swc_kv:sync(Remote, Local), Local} || {SKey, Remote, Local} <- FilledObjects],
    % filter the objects that are not missing after all
    % -- we pass the Local (or old value) along so we can use it later for event handling
    RealMissingObjects =
    [{SKey, Synced, Local}
     || {SKey, {D1,_} = Synced, {D2, _} = Local} <- SyncedObjects,
        (D1 =/= D2) orelse
        (D1 == [] andalso D2 == [])],
    % add each new dot to our node clock
    BVV2 = lists:foldl(fun ({_K, O, _L}, Acc) -> swc_kv:add(Acc, O) end, BVV1, RealMissingObjects),
    % add new keys to the log (Dotkeymap)
    AccDBOps1 = add_objects_to_log(IdxName, Peers == [], RealMissingObjects, [], sync_resp),
    % save the synced objects and strip their causal history
    {FinalDBOps, Events} = strip_save_batch(RealMissingObjects, AccDBOps1, [], State0#state{bvv=BVV2}, sync_resp),
    SyncLock1 = refresh_sync_lock(SyncLock0),
    % finishing up
    State1 = State0#state{bvv=BVV2},
    UpdateBVV_DBop = update_bvv_db_op(BVV2),
    db_write(Config, lists:reverse([UpdateBVV_DBop | FinalDBOps])),
    State1#state.auto_gc andalso incremental_cleanup_db_ops(State1),
    trigger_events(Events),
    {reply, ok, State1#state{sync_lock=SyncLock1}};

handle_call({sync_repair, _, _, _, _}, _From, State) ->
    {reply, {error, not_locked}, State};


handle_call({recover_objects, _RemotePeer, RemoteClock, MissingObjects, _RemoteWatermark = undefined}, _From, #state{id=Id, sync_lock={Id, _} = SyncLock0, config=Config} = State0) ->
    {DBOps0, Events, #state{bvv=BVV} = State1} = recover_objects(MissingObjects, RemoteClock, State0#state.bvv, State0),
    SyncLock1 = refresh_sync_lock(SyncLock0),
    DBOps1 = [update_bvv_db_op(BVV)|DBOps0],
    db_write(Config, DBOps1),
    trigger_events(Events),
    {reply, ok, State1#state{sync_lock=SyncLock1}};

handle_call({recover_objects, RemotePeer, RemoteClock0, MissingObjects, RemoteWatermark}, _From, #state{id=Id, sync_lock={Id, _} = SyncLock0, config=Config} = State0) ->
    % LAST batch of recovered objects

    % sync node clocks:
    % replace the current entry in the node clock for the responding clock with
    % the current knowledge it's receiving
    NodeClock0 = swc_node:merge(State0#state.bvv, RemoteClock0),
    {DBOps0, Events, #state{bvv=BVV} = State1} = recover_objects(MissingObjects, RemoteClock0, State0#state.bvv, State0#state{bvv=NodeClock0}),

    % update my watermark
    KVV1 = update_watermark_internal(RemotePeer, RemoteClock0, State1),
    KVV2 = swc_watermark:left_join(KVV1, RemoteWatermark),

    SyncLock1 = refresh_sync_lock(SyncLock0),
    DBOps1 = [update_bvv_db_op(BVV), update_kvv_db_op(KVV2)|DBOps0],
    db_write(Config, DBOps1),
    trigger_events(Events),
    {reply, ok, State1#state{kvv=KVV2, sync_lock=SyncLock1}};

handle_call(bvv, _, #state{bvv=Bvv} = State) ->
    {reply, Bvv, State};

handle_call(do_gc, _, State) ->
    incremental_cleanup_db_ops(State),
    {reply, ok, State};

handle_call(dump, _, #state{config=Config} = State) ->
    {reply, dump_tables(Config), State};

handle_call({set_broadcast, IsBroadcastEnabled}, _From, State) ->
    {reply, ok, State#state{broadcast_enabled=IsBroadcastEnabled}}.

handle_cast({set_group_members, UpdatedPeerList},
            #state{config=Config,
                   peers=Peers0,
                   idx_name=IdxName,
                   sync_lock=undefined, bvv=BVV0, id=Id, kvv=KVV0} = State) ->


    AddedNodes = UpdatedPeerList  -- [Id|Peers0],
    LeftNodes = [Id|Peers0] -- UpdatedPeerList,

    {Peers1, BVV1, KVV1} =
    case {AddedNodes, LeftNodes} of
        {[], []} ->
            % all good
            {Peers0, BVV0, KVV0};
        _ ->
            %% add the new node(s) to the node clock
            TmpBVV = lists:foldl(fun(Peer, AccBVV) -> swc_node:add(AccBVV, {Peer, 0}) end, swc_node:new(), UpdatedPeerList),
            TmpPeers = swc_node:ids(TmpBVV) -- [Id],

            % maybe full sync
            case random_peer(AddedNodes) of
                {ok, SyncPeer} ->
                    % TODO: what if full sync doesn't complete with this peer?
                    % maybe it would be better to pass all AddedNodes to the start_full_sync
                    % state machine and ensure it keeps trying to contact every node
                    % until it has completed the full sync
                    vmq_swc_exchange_sup:start_full_exchange(Config, SyncPeer, application:get_env(vmq_swc, sync_timeout, 60000));
                _ ->
                    ignore
            end,

            % maybe remove left nodes
            lists:foreach(
              fun(LP) ->
                      % TODO: should we remove the leavingpeer from the NodeClock?
                      remove_logs_for_peer(Config, IdxName, LP)
              end, LeftNodes),

            TmpKVV = fix_watermark(KVV0, UpdatedPeerList),
            {TmpPeers, TmpBVV, TmpKVV}
    end,
    UpdateKVV_DBop = update_kvv_db_op(KVV1),
    UpdateBVV_DBop = update_bvv_db_op(BVV1),
    db_write(Config, [UpdateKVV_DBop, UpdateBVV_DBop]),
    lager:info("Peer membership changed ~p~n", [Peers1]),
    {noreply, State#state{bvv=BVV1, kvv=KVV1, peers=Peers1}};
handle_cast({set_group_members, NewPeers}, #state{config=Config} = State) ->
    lager:warning("Defer peer membership change due to locked store, retry in 1 second", []),
    % currently a sync going on.. wait until sync_lock is free
    timer:apply_after(1000, ?MODULE, set_group_members, [Config, NewPeers]),
    {noreply, State};

handle_cast({swc_broadcast, Objects}, #state{config=Config} = State0) ->
    {DbOps, Events, #state{bvv=BVV} = State1} =
    lists:foldl(fun(Object, Acc) ->
                        process_replicate_op(Object, Acc)
                end, {[], [], State0}, Objects),
    UpdateBVV_DBOp = update_bvv_db_op(BVV),
    db_write(Config, [UpdateBVV_DBOp | DbOps]),
    trigger_events(Events),
    {noreply, State1}.

handle_info({batch, Batch}, #state{config=Config,
                                   peers=Peers,
                                   broadcast_enabled=IsBroadcastEnabled} = State0) ->
    {ReplicateObjects, DbOps, Events, #state{bvv=BVV} = State1} =
    lists:foldl(fun({{CallerPid, CallerRef}, {write, WriteOps}}, Acc0) ->
                        Acc1 =
                        lists:foldl(fun(WriteOp, AccAcc0) ->
                                            process_write_op(WriteOp, AccAcc0)
                                    end, Acc0, WriteOps),
                        CallerPid ! {CallerRef, ok},
                        Acc1
                end, {[], [], [], State0}, Batch),
    UpdateBVV_DBOp = update_bvv_db_op(BVV),
    db_write(Config, [UpdateBVV_DBOp | DbOps]),
    case IsBroadcastEnabled of
        true ->
            #swc_config{transport=TMod} = Config,
            lists:foreach(
              fun(Peer) ->
                      TMod:rpc_cast(Config, Peer, ?MODULE, rpc_broadcast, [ReplicateObjects])
              end, Peers);
        _ ->
            ok
    end,
    trigger_events(Events),
    {noreply, State1};

handle_info({sync_with, Peer}, #state{config=Config} = State) ->
    vmq_swc_exchange_sup:start_exchange(Config, Peer, application:get_env(vmq_swc, sync_timeout, 60000)),
    {noreply, State};

handle_info(sync, #state{config=Config, sync_lock=undefined, peers=Peers} = State) ->
    case random_peer(Peers) of
        {ok, SyncNode} ->
            vmq_swc_exchange_sup:start_exchange(Config, SyncNode, application:get_env(vmq_swc, sync_timeout, 60000));
        {error, no_peer_available} ->
            undefined
    end,
    maybe_schedule_sync(),
    {noreply, State};

handle_info(sync, State) ->
    maybe_schedule_sync(),
    {noreply, State};

handle_info({sync_lock_expired, Peer}, #state{sync_lock=SyncLock} = State0) ->
    case SyncLock of
        undefined ->
            % late arrival
            {noreply, State0};
        {Peer, _} ->
            lager:warning("Synchronization lock expired for peer ~p", [Peer]),
            % expired
            {noreply, State0#state{sync_lock=undefined}};
        {_OtherPeer, _} ->
            % ignore
            {noreply, State0}
    end.

terminate(_Reason, _State) ->
    ok.

code_change(_, _, State) ->
    {ok, State}.

%% INTERNAL

init_sync_lock(Peer) ->
    lager:debug("locked for ~p", [Peer]),
    TRef = erlang:send_after(10000, self(), {sync_lock_expired, Peer}),
    {Peer, TRef}.

refresh_sync_lock({Peer, TRef}) ->
    lager:debug("refresh lock for ~p", [Peer]),
    erlang:cancel_timer(TRef),
    init_sync_lock(Peer).

release_sync_lock({Peer, TRef}) ->
    lager:debug("release lock for ~p", [Peer]),
    erlang:cancel_timer(TRef),
    undefined.

random_peer([]) -> {error, no_peer_available};
random_peer(Peers) ->
    {ok, lists:nth(rand:uniform(length(Peers)), Peers)}.

fix_watermark(KVV, Peers) ->
    lists:foldl(
      fun(Peer, KVVAcc0) ->
              % This will reset all counters to zero
              KVVAcc1 = swc_watermark:add_peer(KVVAcc0, Peer, Peers),
              % set the resetted counters to its old value,
              % new nodes will have a counter of '
              lists:foldl(
                fun(P, A) ->
                        swc_watermark:update_cell(A, Peer, P, swc_watermark:get(KVV, Peer, P))
                end, KVVAcc1, Peers)
      end, swc_watermark:new(), Peers).

update_watermark_internal(Node, NodeClock, #state{id=Id, kvv=KVV0, bvv=BVV}) ->
    % Store the knowledge the other node has about us
    % update my watermark with what I know, based on my node clock
    KVV1 = swc_watermark:update_peer(KVV0, Id, BVV),
    % update my watermark with what my peer knows, based on its node clock
    swc_watermark:update_peer(KVV1, Node, NodeClock).

recover_objects(Objects, RemoteClock, LocalClock, State) ->
    recover_objects(Objects, RemoteClock, LocalClock, [], [], State).

recover_objects([], _, _, DBOps, Events, State) ->
    {DBOps, Events, State};
recover_objects([{SKey, DCC}|Rest], RemoteClock, LocalClock, AccDBOps0, AccEvents0, #state{idx_name=IdxName, config=Config, peers=Peers} = State0) ->
    % fill the Object with the sending node clock
    FilledRemoteObject = swc_kv:fill(DCC, RemoteClock),
    % get and fill the causal history of the local key
    DiskObject = swc_kv:fill(get_dcc_for_key(Config, SKey), LocalClock),
    % synchronize both objects
    FinalObject = swc_kv:sync(FilledRemoteObject, DiskObject),
    % test if the FinalObject has newer information
    FinalValues = swc_kv:values(FinalObject),
    CurrentValues = swc_kv:values(DiskObject),
    case (FinalValues /= CurrentValues) orelse (FinalValues == [] andalso CurrentValues == []) of
        false ->
            recover_objects(Rest, RemoteClock, LocalClock, AccDBOps0, AccEvents0, State0);
        true ->
            % add each new dot to our node clock
            StateNodeClock = swc_kv:add(State0#state.bvv, FinalObject),
            % add new keys to the log (Dotkeymap)
            Object = {SKey, FinalObject, DiskObject},
            AccDBOps1 = add_objects_to_log(IdxName, Peers == [], [Object], AccDBOps0, recover_objects),
            % removed unnecessary causality from the object, based on the current node clock
            {DBOps, Events} = strip_save_batch([Object], AccDBOps1, AccEvents0, State0, recover_objects),
            recover_objects(Rest, RemoteClock, LocalClock, DBOps, Events, State0#state{bvv=StateNodeClock})
    end.


strip_save_batch([], DBOps, Events, _State, _DbgCategory) ->
    {DBOps, lists:reverse(Events)};
strip_save_batch([{SKey, DCC, OldDCC}|Rest], DBOps0, Events0, #state{bvv=BVV0} = State, DbgCategory) ->
    % remove unnecessary causality from the DCC, based on the current node clock
    {Values0, Context} = _StrippedDCC0 = swc_kv:strip(DCC, BVV0),
    Values1 = maps:filter(fun(_D, V) -> V =/= '$deleted' end, Values0),
    StrippedDCC1 = {Values1, Context},
    % The resulting DCC is one of the following:
    % 0 - it has no value but has causal history -> it's a delete, but still must be persisted
    % 1 - it has no value and no causal history -> can be deleted
    % 2 - has values, with causal history -> it's a normal write and must be persisted
    % 3 - has values, but no causal history -> it's the final form for this write
    {DBOps1, Events1} =
    case {map_size(Values1), map_size(Context)} of
        {0, C} when (C == 0) or (State#state.peers == []) -> % case 1
            {[delete_dcc_db_op(SKey, [DbgCategory, strip_save_batch])|DBOps0],
             event(deleted, SKey, undefined, OldDCC, Events0, State)};
        {0, _} -> % case 0
            {[update_dcc_db_op(SKey, StrippedDCC1, [DbgCategory, strip_save_batch])|DBOps0],
             event(deleted, SKey, undefined, OldDCC, Events0, State)};
        _ -> % case 2 & 3
            {[update_dcc_db_op(SKey, StrippedDCC1, [DbgCategory, strip_save_batch])|DBOps0],
             event(updated, SKey, StrippedDCC1, OldDCC, Events0, State)}
    end,
    strip_save_batch(Rest, DBOps1, Events1, State, DbgCategory).

event(Type, SKey, NewDCC, OldDCC, EventsAcc, #state{subscriptions=Subscriptions}) ->
    {FullPrefix, Key} = sext:decode(SKey),
    OldValues = swc_kv:values(OldDCC),
    SubsForPrefix = maps:get(FullPrefix, Subscriptions, []),
    lists:foldl(
      fun
          ({Pid, ConvertFun}, Acc) when Type == deleted ->
              [{Pid, ConvertFun({Type, FullPrefix, Key, OldValues})}|Acc];
          ({Pid, ConvertFun}, Acc) when Type == updated ->
              [{Pid, ConvertFun({Type, FullPrefix, Key, OldValues, swc_kv:values(NewDCC)})}|Acc]
      end, EventsAcc, SubsForPrefix).

trigger_events([{Pid, Event}|Rest]) ->
    Pid ! Event,
    trigger_events(Rest);
trigger_events([]) -> ok.

process_write_op({Key, Value, Context}, {AccReplicate0, AccDBOps0, AccEvents0, #state{config=Config, idx_name=IdxName, id=Id, bvv=BVV0} = State0}) ->
    % sext encode key
    SKey = sext:encode(Key),
    % get and fill the causal history of the local key
    DiskDCC = swc_kv:fill(get_dcc_for_key(Config, SKey), BVV0),
    % discard obsolete values wrt. the causal context
    DiscardDCC = swc_kv:discard(DiskDCC, Context),
    % generate a new dot for this write/delete and add it to the node clock
    {Dot, BVV1} = swc_node:event(BVV0, Id),
    % test if this is a delete; if not, add dot-value to the DCC
    NewDCC =
    case Value of
        ?DELETED ->
            swc_kv:add(DiscardDCC, {Id, Dot}, ?DELETED);
        _ ->
            swc_kv:add(DiscardDCC, {Id, Dot}, Value)
    end,
    % save the new k/v and remove unnecessary causal information
    {AccDBOps1, AccEvents1} = strip_save_batch([{SKey, NewDCC, DiskDCC}], AccDBOps0, AccEvents0, State0, write_op),
    % append the key to the tail of the Log (Dotkeymap)
    AccDBOps2 =
    case State0#state.peers of
        [] ->
            % we're a single node cluster, no need to write a log
            AccDBOps1;
        _ ->
            [append_log_db_op(IdxName, SKey, Id, Dot, write_op) | AccDBOps1]
    end,

    AccReplicate1 = [{SKey, NewDCC}|AccReplicate0],

    State1 = State0#state{bvv=BVV1},

    {AccReplicate1, AccDBOps2, AccEvents1, State1}.

process_replicate_op({SKey, DCC}, {AccDBOps0, AccEvents0, #state{config=Config, idx_name=IdxName, peers=Peers, bvv=BVV0} = State0}) ->
    BVV1 = swc_kv:add(BVV0, DCC),
    State1 = State0#state{bvv=BVV1},
    % get and fill the causal history of the local key
    DiskDCC = swc_kv:fill(get_dcc_for_key(Config, SKey), BVV0),
    % append the key to the tail of the Log (Dotkeymap)
    AccDBOps1 = add_objects_to_log(IdxName, Peers == [], [{SKey, DCC, DiskDCC}], AccDBOps0, replicate_op),
    % synchronize both objects
    FinalDCC = swc_kv:sync(DCC, DiskDCC),
    % test if the FinalObject has newer information
    case FinalDCC == DiskDCC of
        true ->
            % ignored, same object
            {AccDBOps1, AccEvents0, State1};
        false ->
            % save the new object, while stripping the unnecessary causality
            {AccDBOps2, AccEvents1} = strip_save_batch([{SKey, FinalDCC, DiskDCC}], AccDBOps1, AccEvents0, State0, replicate_op),
            {AccDBOps2, AccEvents1, State1}
    end.

add_objects_to_log(_, false = _IsEmpty, [], DBOps, _DbgCategory) -> DBOps;
add_objects_to_log(IdxName, false = _IsEmpty, [{SKey, DCC, _Local}|Rest], DBOps0, DbgCategory) ->
    {Dots, _} = DCC,
    DBOps1 =
    maps:fold(
      fun(_Dot={Id, Counter}, _, Acc) ->
              [append_log_db_op(IdxName, SKey, Id, Counter, DbgCategory)|Acc]
      end, DBOps0, Dots),
    add_objects_to_log(IdxName, false, Rest, DBOps1, DbgCategory);
add_objects_to_log(_, true = _IsEmpty, _, DBOps, _) -> DBOps.

enqueue_op_sync(Config, Op) ->
    Ref = make_ref(),
    CallerRef = {self(), Ref},
    vmq_swc_store_batcher:add_to_batch(Config, {CallerRef, Op}),
    %% we'll monitor the store, as this is where the batched operation
    %% is being processed
    MRef = monitor(process, Config#swc_config.store),
    receive
        {Ref, Reply} ->
            demonitor(MRef),
            Reply;
        {'DOWN', MRef, process, _, Reason} ->
            {error, Reason}
    end.

get_dcc_for_key(Config, SKey) ->
    case vmq_swc_db:get(Config, dcc, SKey) of
        {ok, BDCC} -> binary_to_term(BDCC);
        not_found -> swc_kv:new()
    end.

incremental_cleanup_db_ops(#state{kvv={A, B}}) when (map_size(A) == 0) and map_size(B) == 0 ->
    ok;
incremental_cleanup_db_ops(#state{config=Config, idx_name=IdxName, bvv=BVV, kvv=KVV}) ->
    % check if watermark is uptodate see dotted_db_vnode:is_watermark_up_to_date/1
    % and only cleanup

    % calculate minimums
    Mins =
    lists:foldl(fun(Id, Acc) ->
                        Min = swc_watermark:min(KVV, Id),
                        [{Id, Min, sext:encode({Id, Min})} | Acc]
                end, [], swc_watermark:peers(KVV)),
    DBOps =
    lists:foldl(
        fun({Id, Min, _MinLogKey}, DbOpsAcc) ->
                Candidates = vmq_swc_log_index:find_gc_candidates(IdxName, Id, Min),
                lists:foldl(
                  fun({Peer, SKey, Counters}, DbOpsAccAcc) ->
                          DeleteLog_DBOps = [delete_log_db_op(IdxName, Peer, SKey, C, incremental_gc) || C <- Counters],

                          DCC0 = get_dcc_for_key(Config, SKey),
                          case swc_kv:strip(DCC0, BVV) of
                              {Vs, _C} when map_size(Vs) == 0 ->
                                  DeleteDCC_DBOp = delete_dcc_db_op(SKey, incremental_gc),
                                  [DeleteDCC_DBOp | DeleteLog_DBOps] ++ DbOpsAccAcc;
                              _ ->
                                  DeleteLog_DBOps ++ DbOpsAccAcc
                          end
                  end, DbOpsAcc, Candidates)
        end, [], Mins),
    db_write(Config, DBOps).

remove_logs_for_peer(Config, IdxName, Peer) ->
    DBOps =
    lists:foldl(
      fun({_, SKey, Counters}, Acc) ->
              [delete_log_db_op(IdxName, Peer, SKey, C, retire_peer_gc) || C <- Counters] ++ Acc
      end, [], vmq_swc_log_index:find_all(IdxName, Peer)),
    db_write(Config, DBOps).

init_log_index(Config, IdxName) ->
    vmq_swc_log_index:init(IdxName),
    Itr = vmq_swc_db:iterator(Config, log),
    db_foldl(
      fun(LogKey, SKey, _) ->
              {Id, Counter} = sext:decode(LogKey),
              vmq_swc_log_index:insert(IdxName, Id, SKey, Counter)
      end, ok, Itr).

dump_tables(Config) ->
    #{dcc => dump_table(Config, dcc, fun(K) -> sext:decode(K) end, fun(V) -> binary_to_term(V) end),
      log => dump_table(Config, log, fun(K) -> sext:decode(K) end, fun(V) -> sext:decode(V) end),
      default => dump_table(Config, default, fun(K) -> K end, fun(V) -> binary_to_term(V) end)}.

dump_table(Config, Type, KeyDecoder, ValDecoder) ->
    Itr = vmq_swc_db:iterator(Config, Type),
    {NumItems, KeyBytes, DataBytes, Data} =
    db_foldl(
      fun(Key, Val, {N, KeySpaceSize, DataSize, Acc}) ->
              {N + 1, byte_size(Key) + KeySpaceSize, byte_size(Val) + DataSize, [{KeyDecoder(Key), ValDecoder(Val)}|Acc]}
      end, {0, 0, 0, []}, Itr),
    #{n => NumItems, key_memory => KeyBytes, data_memory => DataBytes, data => Data}.

update_dcc_db_op(SKey, {Vs, C}, Category) when (map_size(Vs) == 0) and map_size(C) == 0 ->
    delete_dcc_db_op(SKey, [Category, through_dcc_update]);
update_dcc_db_op(SKey, DCC, _Category) ->
    ?DBG_OP("PUT DCC DB[~p] ~p~n", [_Category, SKey]),
    {dcc, SKey, term_to_binary(DCC)}.

delete_dcc_db_op(SKey, _Category) ->
    ?DBG_OP("DEL DCC DB[~p] ~p~n", [_Category, SKey]),
    {dcc, SKey, ?DELETED}.

append_log_db_op(IdxName, SKey, Id, Counter, _Category) ->
    K = sext:encode({Id, Counter}),
    ?DBG_OP("PUT LOG DB[~p] ~p : ~p~n", [_Category, K, SKey]),
    vmq_swc_log_index:insert(IdxName, Id, SKey, Counter),
    {log, K, SKey}.

delete_log_db_op(IdxName, Id, SKey, Counter, _Category) ->
    K = sext:encode({Id, Counter}),
    ?DBG_OP("DEL LOG DB[~p] ~p~n", [_Category, K]),
    vmq_swc_log_index:remove(IdxName, Id, SKey, Counter),
    {log, K, ?DELETED}.

update_bvv_db_op(BVV) ->
    {default, <<"BVV">>, term_to_binary(BVV)}.

update_kvv_db_op(KVV) ->
    {default, <<"KVV">>, term_to_binary(KVV)}.

get_bvv(Config) ->
    case vmq_swc_db:get(Config, default, <<"BVV">>) of
        {ok, BBVV} -> binary_to_term(BBVV);
        not_found -> swc_node:new()
    end.

get_kvv(Config) ->
    case vmq_swc_db:get(Config, default, <<"KVV">>) of
        {ok, BKVV} -> binary_to_term(BKVV);
        not_found -> swc_watermark:new()
    end.

db_write(Config, DbOps) ->
    vmq_swc_db:put_many(Config, DbOps).

-type db_fold_fun() :: fun((key(), value(), any()) -> stop | {pause, any()} | any()).
-spec db_foldl(db_fold_fun(), any(), vmq_swc_db_iterator:iterator()) -> any().
db_foldl(Fun, Acc0, Itr0) ->
    case vmq_swc_db:iterator_next(Itr0) of
        {{Key, Val}, Itr1} ->
            case Fun(Key, Val, Acc0) of
                stop ->
                    vmq_swc_db:iterator_close(Itr1),
                    Acc0;
                {pause, Acc1} ->
                    {pause, Acc1, Itr1};
                Acc1 ->
                    db_foldl(Fun, Acc1, Itr1)
            end;
        '$end_of_table' ->
            Acc0
    end.

maybe_schedule_sync() ->
    case application:get_env(vmq_swc, sync_interval, {15000, 2500}) of
        0 -> ok;
        {FixedInt, RandInt} ->
            erlang:send_after(FixedInt + rand:uniform(RandInt), self(), sync)
    end.
