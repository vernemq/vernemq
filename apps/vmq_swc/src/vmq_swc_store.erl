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

         process_batch/2,

         lock/1,

         remote_sync_missing/3,
         rpc_sync_missing/3,

         sync_repair/5,

         node_clock/1,
         remote_node_clock/2,
         rpc_node_clock/1,

         watermark/1,
         remote_watermark/2,
         rpc_watermark/1,

         update_watermark/3,

         do_gc/2,
         set_group_members/2,
         node_clock_by_storename/1, % testing
         watermark_by_storename/1,   % testing
         rpc_broadcast/3,
         fix_watermark/2, % testing
         set_nodeclock/2 % testing
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
        {
         %id                :: peer(),
         id                :: swc_id(),
         old_id            :: swc_id(),
         mode,
         group,
         config            :: config(),
         dotkeymap         :: dotkeymap(),
         nodeclock         :: nodeclock(),
         watermark         :: watermark(),
         %peers             :: list(peer()),
         peers             :: list(swc_id()),
         sync_lock,
         auto_gc           :: boolean(),
         periodic_gc       :: boolean(),
         subscriptions=#{},
         broadcast_enabled :: boolean()
        }).

%-define(DBG_OP(Str, Format), io:format(Str, Format)).
-define(DBG_OP(_Str, _Format), ok).

start_link(#swc_config{store=StoreName} = Config) ->
    gen_server:start_link({local, StoreName}, ?MODULE, [Config], []).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Store API

-spec write(config(), key(), value()) -> ok.
write(Config, Key, Value) ->
    write(Config, Key, Value, swc_vv:new()).

-spec write(config(), key(), value(), context() | undefined) -> ok.
write(Config, Key, Value, Context) ->
    write_batch(Config, [{Key, Value, Context}]).

-spec write_batch(config(), [{key(), value(), context() | undefined}]) -> ok.
write_batch(Config, [{_Key, _Val, _Context}|_] = WriteOps) ->
    enqueue_op_sync(Config, {write, WriteOps}).

-spec read(config(), key()) -> {[value()], context()}.
read(Config, Key) ->
    SKey = sext:encode(Key),
    Obj0 = maybe_get_cached_object(Config, SKey),
    LocalClock=get_cached_node_clock(Config),
    Obj1 = swc_kv:fill(Obj0, LocalClock),
    Values = swc_kv:values(Obj1),
    Context = swc_kv:context(Obj1),
    {Values, Context}.

-spec fold_values(config(), fun((key(), {[value()], context()}, any()) -> any()), any(), key_prefix()) -> any().
fold_values(Config, Fun, Acc, FullPrefix) ->
    LocalClock=get_cached_node_clock(Config),
    SFirstKey = sext:encode({FullPrefix, 0}), %% apparently 0 < '_'
    vmq_swc_db:fold(Config, ?DB_OBJ,
                    fun(SKey, BValue, AccAcc) ->
                            case sext:decode(SKey) of
                                {FullPrefix, _Key} = PKey ->
                                    Obj0 = binary_to_term(BValue),
                                    Obj1 = swc_kv:fill(Obj0, LocalClock),
                                    Values = swc_kv:values(Obj1),
                                    Context = swc_kv:context(Obj1),
                                    Fun(PKey, {Values, Context}, AccAcc);
                                _ ->
                                    stop
                            end
                    end, Acc, SFirstKey).

-spec subscribe(config(), key_prefix(),
                fun(({updated, key_prefix(), key_suffix(), [value()], [value()]}
                    |{deleted, key_prefix(), key_suffix(), [value()]}
                    ) -> any())) -> ok.
subscribe(#swc_config{store=StoreName}, FullPrefix, ConvertFun) ->
    gen_server:call(StoreName, {subscribe, FullPrefix, ConvertFun, self()}, infinity).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Sync API, used by vmq_swc_exchange_fsm

-spec lock(config()) -> ok | {error, atom()}.
lock(#swc_config{store=StoreName}) ->
    gen_server:call(StoreName, {lock, self()}, infinity).

-spec remote_sync_missing(config(), peer(), [dot()]) -> [{db_key(), object()}].
remote_sync_missing(#swc_config{group=SwcGroup, transport=TMod, peer={OriginPeer, _Actor}}, RemotePeer, Dots) ->
    TMod:rpc(SwcGroup, RemotePeer, ?MODULE, rpc_sync_missing, [OriginPeer, Dots]).

-spec rpc_sync_missing(peer(), [dot()], config()) -> [{db_key(), object()}].
rpc_sync_missing(OriginPeer, Dots, #swc_config{store=StoreName}) ->
    gen_server:call(StoreName, {sync_missing, OriginPeer, Dots}, infinity).

-spec sync_repair(config(), [{db_key(), object()}], peer(), nodeclock(), false | {true, watermark()}) -> ok.
sync_repair(#swc_config{store=StoreName, peer=LocalNodeId}, MissingObjects, RemotePeer, RemoteClock, LastBatch) ->
    gen_server:call(StoreName, {sync_repair, LocalNodeId, RemotePeer, RemoteClock, MissingObjects, LastBatch}, infinity).

-spec node_clock(config()) -> nodeclock().
node_clock(#swc_config{store=StoreName}) ->
    gen_server:call(StoreName, get_node_clock, infinity).
node_clock_by_storename(StoreName) ->
    gen_server:call(StoreName, get_node_clock, infinity).
watermark_by_storename(StoreName) ->
    gen_server:call(StoreName, get_watermark, infinity).
set_nodeclock(StoreName, NodeClock) ->
    gen_server:call(StoreName, {set_node_clock, NodeClock}, infinity).

-spec remote_node_clock(config(), peer()) -> nodeclock().
remote_node_clock(#swc_config{group=SwcGroup, transport=TMod}, RemotePeer) ->
    TMod:rpc(SwcGroup, RemotePeer, ?MODULE, rpc_node_clock, []).

-spec rpc_node_clock(config()) -> nodeclock().
rpc_node_clock(#swc_config{} = Config) ->
    node_clock(Config).

-spec watermark(config()) -> watermark().
watermark(Config) ->
   Watermark = get_watermark(Config),
   Watermark.

-spec remote_watermark(config(), peer()) -> watermark().
remote_watermark(#swc_config{group=SwcGroup, transport=TMod}, RemotePeer) ->
    TMod:rpc(SwcGroup, RemotePeer, ?MODULE, rpc_watermark, []).

-spec rpc_watermark(config()) -> watermark().
rpc_watermark(#swc_config{} = Config) ->
    watermark(Config).

-spec update_watermark(config(), peer(), nodeclock()) -> ok.
update_watermark(#swc_config{store=StoreName, peer=Peer}, RemotePeer, RemoteNodeClock) ->
    gen_server:call(StoreName, {update_watermark, Peer, RemotePeer, RemoteNodeClock}, infinity).

-spec rpc_broadcast(peer(), any(), config()) -> ok.
rpc_broadcast(FromPeer, Msg, #swc_config{store=StoreName} = _Config) ->
    gen_server:cast(StoreName, {swc_broadcast, FromPeer, Msg}).

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
init([#swc_config{group=Group, peer=SWC_ID, store=StoreName, r_o_w_cache=CacheName} = Config]) ->
    process_flag(priority, high),
    StartSync = application:get_env(vmq_swc, sync_interval, {5000, 2500}),
    StartSync =/= 0 andalso erlang:send_after(1000, self(), sync),

    NodeClock = get_nodeclock(Config),
    Watermark = get_watermark(Config),

    DKM = init_dotkeymap(Config),
    case application:get_env(vmq_swc, periodic_gc, true) of
        true ->
            self() ! do_gc;
        false ->
            ignore
    end,

    IsBroadcastEnabled = application:get_env(vmq_swc, enable_broadcast, true),
    IsAutoGc = application:get_env(vmq_swc, auto_gc, true),
    IsPeriodicGc = application:get_env(vmq_swc, periodic_gc, true),

    ets:new(StoreName, [public, named_table, {read_concurrency, true}]),
    ets:new(CacheName, [public, named_table, {read_concurrency, true}]),
   % Members = vmq_swc_peer_service_manager:get_actors(),
   % {ok, LocalState} = vmq_swc_peer_service_manager:get_local_state(),

   % reset the entire watermark
   % NewWatermark = swc_watermark:reset_counters(NewWatermark0),
   % Members = vmq_swc_peer_service_manager:get_actors_and_peers(),

    vmq_swc_metrics:register_gauge({swc_object_count, Group},
                                   fun() -> {[{group, atom_to_list(Group)}],
                                             <<"The number of replicated objects by this SWC group.">>,
                                            vmq_swc_dkm:info(DKM, object_count)}
                                   end),
    vmq_swc_metrics:register_gauge({swc_tombstone_count, Group},
                                   fun() -> {[{group, atom_to_list(Group)}],
                                             <<"The number of replicated tombstones by this SWC group.">>,
                                            vmq_swc_dkm:info(DKM, tombstone_count)}
                                   end),
    vmq_swc_metrics:register_gauge({swc_dotkeymap_memory, Group},
                                   fun() -> {[{group, atom_to_list(Group)}],
                                             <<"The number of words allocated to the SWC group dotkeymap.">>,
                                            vmq_swc_dkm:info(DKM, memory)}
                                   end),
    % {ok, Actor} = vmq_swc_peer_service_manager:get_actor(),
    Members = vmq_swc_group_membership:get_members(Config),
    Members2 = vmq_swc_group_membership:swc_ids(Members),
    OLD_SWC_ID =
    case ets:lookup(swc_cluster_state, old_actor) of
        [] -> {node(), <<"initial">>};
        [{old_actor, OLD_SWC_Actor}] -> {node(), OLD_SWC_Actor}
    end,
    State = set_peers(Members2, #state{id=SWC_ID, 
                                      old_id = OLD_SWC_ID,
                                      peers=[],
                                      group=Group,
                                      config=Config,
                                      dotkeymap=DKM,
                                      nodeclock=NodeClock,
                                      watermark=Watermark,
                                      auto_gc = IsAutoGc,
                                      periodic_gc = IsPeriodicGc,
                                      broadcast_enabled = IsBroadcastEnabled
                                     }),
    {ok, State}.

handle_call({batch, Batch}, _From, #state{config=Config,
                                          peers=Peers,
                                          id=Id,
                                          broadcast_enabled=IsBroadcastEnabled} = State0) ->
    {Local, _} = Id,
    Peers1 = proplists:get_keys(Peers),
    {ReplicateObjects, DbOps, #state{nodeclock=NodeClock} = State1} =
    lists:foldl(fun({{CallerPid, CallerRef}, {write, WriteOps}}, Acc0) ->
                        Acc1 =
                        lists:foldl(fun(WriteOp, AccAcc0) ->
                                            process_write_op(WriteOp, AccAcc0)
                                    end, Acc0, WriteOps),
                        CallerPid ! {CallerRef, ok},
                        Acc1
                end, {[], [], State0}, Batch),
    UpdateNodeClock_DBOp = update_nodeclock_db_op(NodeClock),
    db_write(Config, [UpdateNodeClock_DBOp | lists:reverse(DbOps)]),
    r_o_w_cache_clear(Config),
 %   io:format("Batch, Replicate Objects ~p~n", [ReplicateObjects]),
 %   io:format("Batch, DBOpts ~p~n", [DbOps]),
 %   io:format("Batch, NodeClock ~p~n", [NodeClock]),

    case IsBroadcastEnabled of
        true ->
            #swc_config{group=SwcGroup, transport=TMod} = Config,
            lists:foreach(
              fun(Peer) ->
                      TMod:rpc_cast(SwcGroup, Peer, ?MODULE, rpc_broadcast, [Local, lists:reverse(ReplicateObjects)])
              end, Peers1);
        _ ->
            ok
    end,
    {reply, ok, cache_node_clock(State1)};

handle_call({subscribe, FullPrefix, ConvertFun, Pid}, _From, #state{subscriptions=Subs0} = State0) ->
    SubsForPrefix = maps:get(FullPrefix, Subs0, []),
    Subs1 = maps:put(FullPrefix, [{Pid, ConvertFun}|SubsForPrefix], Subs0),
    {reply, ok, State0#state{subscriptions=Subs1}};

handle_call({lock, OwnerPid}, _From, #state{id=Id, sync_lock=SyncLock} = State0) ->
    {Peer, _Actor} = Id,
 %   lager:info("Local ID in lock request: ~p~n", [Id]),
    case node(OwnerPid) == Peer of
        true when SyncLock == undefined ->
            MRef = monitor(process, OwnerPid),
            Lock = {OwnerPid, MRef},
            {reply, ok, State0#state{sync_lock=Lock}};
        true ->
            {reply, {error, already_locked}, State0};
        false ->
            {reply, {error, invalid_lock_request}, State0}
    end;

handle_call({sync_missing, _OriginPeer, Dots}, From, #state{config=Config} = State0) ->
    spawn_link(
      fun() ->
              Result =
              lists:filtermap(
                fun(Dot) ->
                        DotKey = sext:encode(Dot),
                        case vmq_swc_db:get(Config, ?DB_DKM, DotKey) of
                            not_found ->
                                false;
                            {ok, SKey} ->
                                case vmq_swc_db:get(Config, ?DB_OBJ, SKey) of
                                    not_found ->
                                        %% the object was deleted in
                                        %% the past, replace it with
                                        %% an empty object which will
                                        %% become the delete-marker on
                                        %% the other node.
                                        {true, {SKey, swc_kv:add(swc_kv:new(), Dot, '$deleted')}};
                                    {ok, BObj} ->
                                        {true, {SKey, binary_to_term(BObj)}}
                                end
                        end
                end, Dots),
              gen_server:reply(From, Result)
      end),
    {noreply, State0};

handle_call(get_node_clock, _From, #state{nodeclock=NodeClock} = State) ->
    {reply, NodeClock, State};

handle_call({set_node_clock, NodeClock}, _From, State) ->
    {reply, ok, State#state{nodeclock=NodeClock}};

handle_call(get_watermark, _From, #state{watermark=Watermark} = State) ->
    {reply, Watermark, State};

handle_call({update_watermark, _OriginPeer, RemotePeer, RemoteNodeClock}, _From, #state{config=Config} = State0) ->
    Watermark = update_watermark_internal(RemotePeer, RemoteNodeClock, State0),    %{Node, Actor}, NodeClock, State0),
    UpdateWatermark_DBop = update_watermark_db_op(Watermark),
    db_write(Config, [UpdateWatermark_DBop]),
    {reply, ok, State0#state{watermark=Watermark}};

handle_call({sync_repair, _OriginPeer, RemotePeer, RemoteNodeClock, MissingObjects, LastBatch}, _From,
            #state{config=Config} = State0) ->
    RemoteActor = vmq_swc_peer_service_manager:get_actor_for_peer(RemotePeer),
    {DbOps, State1} = fill_strip_save_batch(MissingObjects, RemoteNodeClock, State0),
    State2 =
    case LastBatch of
        false ->
            UpdateNodeClock_DBop = update_nodeclock_db_op(State1#state.nodeclock),
            db_write(Config, lists:reverse([UpdateNodeClock_DBop | DbOps])),
            State1;
        {true, RemoteWatermark} ->
            NodeClock0 = sync_clocks({RemotePeer, RemoteActor}, RemoteNodeClock, State1#state.nodeclock),
            Watermark = update_watermark_after_sync(State1#state.watermark, RemoteWatermark, State1#state.id, {RemotePeer, RemoteActor}, NodeClock0, RemoteNodeClock),
            UpdateNodeClock_DBop = update_nodeclock_db_op(NodeClock0),
            UpdateWatermark_DBop = update_watermark_db_op(Watermark),
            db_write(Config, lists:reverse([UpdateNodeClock_DBop, UpdateWatermark_DBop | DbOps])),
            incremental_gc(State1#state{watermark=Watermark, nodeclock=NodeClock0})
    end,
    {reply, ok, cache_node_clock(State2)};

handle_call(nodeclock, _, #state{nodeclock=NodeClock} = State) ->
    {reply, NodeClock, State};

handle_call(do_gc, _, #state{id=Id, nodeclock=NodeClock, config=Config} = State0) ->
    Watermark = update_watermark_internal(Id, NodeClock, State0),
    UpdateWatermark_DBop = update_watermark_db_op(Watermark),
    db_write(Config, [UpdateWatermark_DBop]),
    {reply, ok, incremental_gc(State0#state{watermark=Watermark})};

handle_call(dump, _, #state{config=Config} = State) ->
    {reply, dump_tables(Config), State};
handle_call(dump_dotkeymap, _, #state{dotkeymap=DKM} = State) ->
    {reply, vmq_swc_dkm:dump(DKM), State};

handle_call({set_broadcast, IsBroadcastEnabled}, _From, State) ->
    {reply, ok, State#state{broadcast_enabled=IsBroadcastEnabled}};

handle_call(Request, From, State) ->
    lager:error("Replica ~p: Received invalid call ~p from ~p", [State#state.group, Request, From]),
    {reply, {error, wrong_request}, State}.

handle_cast({set_group_members, UpdatedPeerList}, State) ->
    {noreply, set_peers(UpdatedPeerList, State)};

handle_cast({swc_broadcast, FromPeer, Objects}, #state{peers=Peers, config=Config} = State0) ->
    case lists:member(FromPeer, Peers) of
        true ->
            {DbOps, #state{nodeclock=NodeClock} = State1} =
            lists:foldl(fun(Object, Acc) ->
                                process_replicate_op(Object, Acc)
                        end, {[], State0}, Objects),
            UpdateNodeClock_DBOp = update_nodeclock_db_op(NodeClock),
            db_write(Config, [UpdateNodeClock_DBOp | lists:reverse(DbOps)]),
            {noreply, cache_node_clock(State1)};
        false ->
            % drop broadcast
            {noreply, State0}
    end;
handle_cast(Request, State) ->
    lager:error("Replica ~p: Received invalid cast ~p", [State#state.group, Request]),
    {noreply, State}.

handle_info(do_gc, #state{id=Id, nodeclock=NodeClock, config=Config} = State0) ->
    Watermark = update_watermark_internal(Id, NodeClock, State0),
    UpdateWatermark_DBop = update_watermark_db_op(Watermark),
    db_write(Config, [UpdateWatermark_DBop]),
    State1 = incremental_gc(State0#state{watermark=Watermark}),
    erlang:send_after(application:get_env(vmq_swc, gc_interval, 15000), self(), do_gc),
    {noreply, State1};

handle_info({sync_with, Peer}, #state{config=Config} = State) ->
    %% only used for testing purposes
    vmq_swc_exchange_sup:start_exchange(Config, Peer, application:get_env(vmq_swc, sync_timeout, 60000)),
    {noreply, State};

handle_info(sync, #state{config=Config, sync_lock=undefined, peers=Peers} = State) ->
    case random_peer(proplists:get_keys(Peers), fun(N) -> lists:member(N, nodes()) end) of
        {ok, SyncNode} ->
            vmq_swc_exchange_sup:start_exchange(Config, SyncNode, application:get_env(vmq_swc, sync_timeout, 60000));
        {error, no_peer_available} ->
            lager:debug("Replica ~p: Can't initialize AE exchange due to no peer available", [State#state.group]),
            ignore
    end,
    maybe_schedule_sync(),
    {noreply, State};

handle_info(sync, State) ->
    maybe_schedule_sync(),
    {noreply, State};

handle_info({'DOWN', MRef, process, Pid, _Info}, #state{sync_lock={Pid, MRef}} = State) ->
    {noreply, State#state{sync_lock=undefined}};

handle_info(Info, State) ->
    lager:error("Replica ~p: Received invalid info ~p", [State#state.group, Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_, _, State) ->
    {ok, State}.

%% INTERNAL
set_peers(NewPeers, #state{id=Id, config=Config, dotkeymap=DKM, nodeclock=LocalClock0, watermark=WM0} = State) ->
    OldPeers = swc_node:ids(LocalClock0),
    AddedPeers = NewPeers -- OldPeers,
    LeftPeers = OldPeers -- NewPeers,
    lager:debug("vmq_swc_store:set_peers/2: AddedPeers ~p~n", [AddedPeers]),
    lager:debug("vmq_swc_store:set_peers/2: LeftPeers ~p~n", [LeftPeers]),

    {NodeClock, Watermark} =
    case {AddedPeers, LeftPeers} of
        {[], []} ->
            {LocalClock0, WM0};
        _ ->
            %% Ensure the node clock contains all required peers
            %% Adding {Peer, 0} won't override an existing entry
            TmpClock0 = lists:foldl(fun(Peer, AccClock) -> swc_node:add(AccClock, {Peer, 0}) end, LocalClock0, NewPeers),

            {TmpClock0, fix_watermark(WM0, NewPeers)}
    end,

    UpdateWatermark_DBop = update_watermark_db_op(Watermark),
    UpdateNodeClock_DBop = update_nodeclock_db_op(NodeClock),


    db_write(Config, [UpdateWatermark_DBop, UpdateNodeClock_DBop]),
    cache_node_clock(State#state{nodeclock=NodeClock, watermark=Watermark, peers=NewPeers -- [Id]}).

% maybe_retire_peer(WM, OldID, NewID) ->
%     NewWatermark0 = swc_watermark:retire_peer(WM, OldID, NewID),
%     % reset the entire watermark
%     swc_watermark:reset_counters(NewWatermark0).
cache_node_clock(#state{config=#swc_config{store=StoreName}, nodeclock=NodeClock} = State) ->
    ets:insert(StoreName, {node_clock, NodeClock}),
    State.

get_cached_node_clock(#swc_config{store=StoreName}) ->
    [{_, NodeClock}] = ets:lookup(StoreName, node_clock),
    NodeClock.

maybe_get_cached_object(#swc_config{r_o_w_cache=CacheName} = Config, SKey) ->
    case ets:lookup(CacheName, SKey) of
        [] ->
            get_obj_for_key(Config, SKey);
        [{_, Obj}] ->
            Obj
    end.

r_o_w_cache_insert_object(#swc_config{r_o_w_cache=CacheName}, SKey, Obj) ->
    ets:insert(CacheName, {SKey, Obj}).

r_o_w_cache_clear(#swc_config{r_o_w_cache=CacheName}) ->
    ets:delete_all_objects(CacheName).

random_peer([], _Filter) -> {error, no_peer_available};
random_peer(Peers, FilterFun) ->
    FilteredPeers = lists:filter(FilterFun, Peers),
    case FilteredPeers of
        [] ->
            {error, no_peer_available};
        _ ->
            {ok, lists:nth(rand:uniform(length(FilteredPeers)), FilteredPeers)}
    end.

fix_watermark({W, R} = Watermark, Peers) ->
    Watermark0 = 
    lists:foldl(
      fun(Peer, WMAcc0) ->
              % This will reset all counters to zero
              WMAcc1 = swc_watermark:add_peer(WMAcc0, Peer, Peers),
              % set the resetted counters to its old value,
              % new nodes will have a counter of '
              lists:foldl(
                fun(P, A) ->
                        swc_watermark:update_cell(A, Peer, P, swc_watermark:get(Watermark, Peer, P))
                end, WMAcc1, Peers)
      end, swc_watermark:new(), Peers),
      {W1, _} = Watermark0,
      {W1, R}.

update_watermark_internal(RemoteNode, RemoteClock, #state{id=Id, watermark=Watermark0, nodeclock=NodeClock}) ->
    % Store the knowledge the other node has about us
    % update my watermark with what I know, based on my node clock
    Watermark1 = swc_watermark:update_peer(Watermark0, Id, NodeClock),
    % update my watermark with what my peer knows, based on its node clock
    swc_watermark:update_peer(Watermark1, RemoteNode, RemoteClock).

update_watermark_after_sync(Watermark0, RemoteWatermark, Id, RemoteId, NodeClock, RemoteClock) ->
    % update my watermark with what I know, based on my node clock
    Watermark1 = maps:fold(
            fun (I, _, Acc) ->
                    case I == Id of
                        false -> Acc;
                        true -> swc_watermark:update_peer(Acc, I, NodeClock)
                    end
            end, Watermark0, NodeClock),
    % update my watermark with what my peer knows, based on its node clock
    Watermark2 = maps:fold(
            fun (I, _, Acc) ->
                    case I == RemoteId of
                        false -> Acc;
                        true -> swc_watermark:update_peer(Acc, I, RemoteClock)
                    end
            end, Watermark1, RemoteClock),
    % update the watermark to reflect what the asking peer has about its peers
    swc_watermark:left_join(Watermark2, RemoteWatermark).

sync_clocks(RemoteID, RemoteNodeClock0, NodeClock) ->
    % replace the current entry in the node clock for the responding clock with
    % the current knowledge it's receiving
    %{RemoteNode, _Actor} = RemoteID,
    RemoteNodeClock1 = maps:filter(fun(Id, _) -> Id == RemoteID end, RemoteNodeClock0),
    %RemoteNodeClock1 = maps:filter(fun({RemoteNode, _Actor}, _) -> RemoteNode == node() end, RemoteNodeClock0),
    % the merge will delete all entries, where the latest dot is {0,0}!
    % check if this is what we want.
    swc_node:merge(NodeClock, RemoteNodeClock1).
   % swc_node:merge(NodeClock, RemoteNodeClock0).

fill_strip_save_batch(MissingObjects, RemoteNodeClock, #state{config=Config, nodeclock=NodeClock0} = State0) ->
    {NodeClock1, RealMissing} =
    lists:foldl(
      fun({SKey, Obj}, {NodeClockAcc0, AccRealMissing0} = Acc) ->
              % fill the object with the sending node clock
              FilledObj = swc_kv:fill(Obj, RemoteNodeClock),
              % get the local object corresponding to the received object and fill the causal history
              {D1, _} = Local = swc_kv:fill(get_obj_for_key(Config, SKey), NodeClock0),
              % synchronize / merge the remote and local object
              {D2, _} = Synced = swc_kv:sync(FilledObj, Local),
              % filter out the object that is not missing after all
              case (D1 =/= D2) orelse ((map_size(D1) == 0) andalso (map_size(D2) == 0)) of
                  true ->
                      % add each new dot to our node clock
                      NodeClockAcc1 = swc_kv:add(NodeClockAcc0, Synced),
                      MissingObj = {SKey, Synced, Local},
                      {NodeClockAcc1, [MissingObj|AccRealMissing0]};
                  false ->
                      Acc
              end
      end, {NodeClock0, []}, MissingObjects),
    % save the synced objects and strip their causal history
    State1 = State0#state{nodeclock=NodeClock1},
    FinalDBOps = strip_save_batch(RealMissing, [], State1, sync_resp),
    {FinalDBOps, State1}.

strip_save_batch([], DBOps, _State, _DbgCategory) ->
    DBOps;
strip_save_batch([{SKey, Obj, OldObj}|Rest], DBOps0, #state{nodeclock=NodeClock, dotkeymap=DKM} = State, DbgCategory) ->
    DBOps1 = add_object_to_log(State#state.dotkeymap, SKey, Obj, DBOps0),
    % remove unnecessary causality from the Obj, based on the current node clock
    {Values0, Context} = _StrippedObj0 = swc_kv:strip(Obj, NodeClock),
    Values1 = maps:filter(fun(_D, V) -> V =/= '$deleted' end, Values0),
    StrippedObj1 = {Values1, Context},
    % The resulting Obj is one of the following:
    % 0 - it has no value but has causal history -> it's a delete, but still must be persisted
    % 1 - it has no value and no causal history -> can be deleted
    % 2 - has values, with causal history -> it's a normal write and must be persisted
    % 3 - has values, but no causal history -> it's the final form for this write
    DBOps2  =
    case {map_size(Values1), map_size(Context)} of
        {0, C} when (C == 0) or (State#state.peers == []) -> % case 1
            vmq_swc_dkm:mark_for_gc(DKM, SKey),
            event(deleted, SKey, undefined, OldObj, State),
            [delete_obj_db_op(SKey, [DbgCategory, strip_save_batch])|DBOps1];
        {0, _} -> % case 0
            vmq_swc_dkm:mark_for_gc(DKM, SKey),
            event(deleted, SKey, undefined, OldObj, State),
            [update_obj_db_op(SKey, StrippedObj1, [DbgCategory, strip_save_batch])|DBOps1];
        _ -> % case 2 & 3
            event(updated, SKey, StrippedObj1, OldObj, State),
            [update_obj_db_op(SKey, StrippedObj1, [DbgCategory, strip_save_batch])|DBOps1]
    end,
    strip_save_batch(Rest, DBOps2, State, DbgCategory).

event(Type, SKey, NewObj, OldObj, #state{subscriptions=Subscriptions}) ->
    {FullPrefix, Key} = sext:decode(SKey),
    OldValues = swc_kv:values(OldObj),
    SubsForPrefix = maps:get(FullPrefix, Subscriptions, []),
    lists:foreach(
      fun
          ({Pid, ConvertFun}) when Type == deleted ->
              Pid ! ConvertFun({deleted, FullPrefix, Key, OldValues});
          ({Pid, ConvertFun}) when Type == updated ->
              Pid ! ConvertFun({updated, FullPrefix, Key, OldValues, swc_kv:values(NewObj)})
      end, SubsForPrefix).

process_write_op({Key, Value, MaybeContext}, {AccReplicate0, AccDBOps0, #state{config=Config, id=Id, nodeclock=NodeClock0} = State0}) ->
    % sext encode key
    SKey = sext:encode(Key),
    % get and fill the causal history of the local key
    DiskObj = swc_kv:fill(get_obj_for_key(Config, SKey), NodeClock0),

    Context =
    case MaybeContext of
        undefined ->
            swc_kv:context(DiskObj);
        _ ->
            MaybeContext
    end,

    % discard obsolete values wrt. the causal context
    DiscardObj = swc_kv:discard(DiskObj, Context),
    % generate a new dot for this write/delete and add it to the node clock
    {Counter, NodeClock1} = swc_node:event(NodeClock0, Id),
    % test if this is a delete; if not, add dot-value to the Obj
    NewObj =
    case Value of
        ?DELETED ->
            swc_kv:add(DiscardObj, {Id, Counter}, ?DELETED);
        _ ->
            swc_kv:add(DiscardObj, {Id, Counter}, Value)
    end,
    % save the new k/v and remove unnecessary causal information
    AccDBOps1 = strip_save_batch([{SKey, NewObj, DiskObj}], AccDBOps0, State0, write_op),
    AccReplicate1 = [{SKey, NewObj}|AccReplicate0],
    State1 = State0#state{nodeclock=NodeClock1},
    r_o_w_cache_insert_object(Config, SKey, NewObj),
    {AccReplicate1, AccDBOps1, State1}.

process_replicate_op({SKey, Obj}, {AccDBOps0, #state{config=Config, nodeclock=NodeClock0} = State0}) ->
    NodeClock1 = swc_kv:add(NodeClock0, Obj),
    State1 = State0#state{nodeclock=NodeClock1},
    % get and fill the causal history of the local key
    DiskObj = swc_kv:fill(get_obj_for_key(Config, SKey), NodeClock0),
    % synchronize both objects
    FinalObj = swc_kv:sync(Obj, DiskObj),
    % save the new object, while stripping the unnecessary causality
    AccDBOps1 = strip_save_batch([{SKey, FinalObj, DiskObj}], AccDBOps0, State0, replicate_op),
    {AccDBOps1, State1}.

add_object_to_log(DKM, SKey, Obj, AccDbOps) ->
    {Dots, _} = Obj,
    maps:fold(
      fun(_Dot={Id, Counter}, _, Acc) ->
              vmq_swc_dkm:insert(DKM, Id, Counter, SKey) ++ Acc
      end, AccDbOps, Dots).

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

-spec get_obj_for_key(config(), db_key()) -> object().
get_obj_for_key(Config, SKey) ->
    case vmq_swc_db:get(Config, ?DB_OBJ, SKey) of
        {ok, BObj} -> binary_to_term(BObj);
        not_found -> swc_kv:new()
    end.

incremental_gc(#state{config=Config, watermark=Watermark, dotkeymap=DKM} = State) ->
    DBOps = vmq_swc_dkm:prune(DKM, Watermark, []),
    db_write(Config, DBOps),
    State.

% -spec remove_logs_for_peer(config(), dotkeymap(), peer()) -> ok.
% remove_logs_for_peer(Config, DKM, Peer) ->
%     DBOps = vmq_swc_dkm:prune_for_peer(DKM, Peer),
%     db_write(Config, DBOps).

-spec init_dotkeymap(config()) -> dotkeymap().
init_dotkeymap(Config) ->
    DKM = vmq_swc_dkm:init(),
    vmq_swc_db:fold(Config, ?DB_DKM,
                    fun(LogKey, SKey, _Acc) ->
                            {Id, Counter} = sext:decode(LogKey),
                            _ = vmq_swc_dkm:insert(DKM, Id, Counter, SKey)
                    end, ok),
    vmq_swc_db:fold(Config, ?DB_OBJ,
                    fun(SKey, BVal, _Acc) ->
                            {Values, _} = binary_to_term(BVal),
                            case map_size(Values) of
                                0 -> % deleted
                                    vmq_swc_dkm:mark_for_gc(DKM, SKey);
                                _ ->
                                    ignore
                            end
                    end, ok),
    DKM.

dump_tables(Config) ->
    #{obj => dump_table(Config, ?DB_OBJ, fun(K) -> sext:decode(K) end, fun(V) -> binary_to_term(V) end),
      dkm => dump_table(Config, ?DB_DKM, fun(K) -> sext:decode(K) end, fun(V) -> sext:decode(V) end),
      default => dump_table(Config, ?DB_DEFAULT, fun(K) -> K end, fun(V) -> (V) end)}.

dump_table(Config, Type, KeyDecoder, ValDecoder) ->
    {NumItems, KeyBytes, DataBytes, Data} =
    vmq_swc_db:fold(Config, Type,
      fun(Key, Val, {N, KeySpaceSize, DataSize, Acc}) ->
              {N + 1, byte_size(Key) + KeySpaceSize, byte_size(Val) + DataSize, [{KeyDecoder(Key), ValDecoder(Val)}|Acc]}
      end, {0, 0, 0, []}),
    #{n => NumItems, key_memory => KeyBytes, data_memory => DataBytes, data => Data}.

-spec update_obj_db_op(db_key(), object(), any()) -> db_op().
update_obj_db_op(SKey, Obj, _Category) ->
    ?DBG_OP("PUT Obj DB[~p] ~p~n", [_Category, SKey]),
    {?DB_OBJ, SKey, term_to_binary(Obj)}.

-spec delete_obj_db_op(db_key(), any()) -> db_op().
delete_obj_db_op(SKey, _Category) ->
    ?DBG_OP("DEL Obj DB[~p] ~p~n", [_Category, SKey]),
    {?DB_OBJ, SKey, ?DELETED}.

-spec update_nodeclock_db_op(nodeclock()) -> db_op().
update_nodeclock_db_op(NodeClock) ->
    {?DB_DEFAULT, <<"BVV">>, term_to_binary(NodeClock)}.

-spec update_watermark_db_op(watermark()) -> db_op().
update_watermark_db_op(Watermark) ->
    {?DB_DEFAULT, <<"KVV">>, term_to_binary(Watermark)}.

-spec get_nodeclock(config()) -> nodeclock().
get_nodeclock(Config) -> 
  case vmq_swc_db:get(Config, ?DB_DEFAULT, <<"BVV">>) of
        {ok, BNodeClock} -> 
        % if we find a nodeclock on disk, we trying to ensure unique swc_id
        % {ok, LocalState} = vmq_swc_peer_service_manager:get_local_state(),
        % {ok, Actor} = vmq_swc_peer_service_manager:get_actor(),
        % {ok, Merged} = riak_dt_orswot:update({update, [
        %                                                {add, node()}]}, Actor, LocalState),
        % _ = gen_server:cast(vmq_swc_peer_service_gossip, {receive_state, Merged}),
            NodeClock = binary_to_term(BNodeClock),
            io:format("Loaded NodeClock fromDisk ~p~n", [NodeClock]),
            NodeClock;
        not_found -> NodeClock0 = swc_node:new(),
                     NodeClock0
    end.

-spec get_watermark(config()) -> watermark().
get_watermark(Config) ->
    case vmq_swc_db:get(Config, ?DB_DEFAULT, <<"KVV">>) of
        {ok, BWatermark} -> binary_to_term(BWatermark);
        not_found -> swc_watermark:new()
    end.

-spec db_write(config(), list(db_op())) -> ok.
db_write(Config, DbOps) ->
    vmq_swc_db:put_many(Config, DbOps).

maybe_schedule_sync() ->
    case application:get_env(vmq_swc, sync_interval, {15000, 2500}) of
        0 -> ok;
        {FixedInt, RandInt} ->
            erlang:send_after(FixedInt + rand:uniform(RandInt), self(), sync)
    end.
