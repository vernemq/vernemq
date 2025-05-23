%% Copyright 2018 Octavo Labs AG Zurich Switzerland (https://octavolabs.com)
%% Copyright 2018-2025 Octavo Labs/VerneMQ (https://vernemq.com/)
%% and Individual Contributors.
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

-module(vmq_swc_exchange_fsm).
-include_lib("kernel/include/logger.hrl").
-include("vmq_swc.hrl").
-behaviour(gen_statem).

% API
-export([start_link/3]).
% State Functions
-export([
    prepare/3,
    update_local/3,
    local_sync_repair/3,
    initial_sync_new/3
]).

-export([init/1, terminate/3, code_change/4, callback_mode/0]).

-record(state, {
    config,
    group,
    peer,
    timeout,
    local_clock,
    remote_clock,
    remote_watermark,
    obj_cnt = 0,
    batch_size,
    missing_dots,
    start_ts
}).

start_link(#swc_config{} = Config, Peer, Timeout) ->
    gen_statem:start_link(?MODULE, [Config, Peer, Timeout], []).

% State functions
prepare(
    internal, start, #state{config = Config, group = Group, peer = Peer, timeout = Timeout} = State
) ->
    case vmq_swc_store:lock(Config) of
        ok ->
            %% get remote lock
            ?LOG_DEBUG("Replica ~p: AE exchange with ~p successfully acquired local lock", [
                Group, Peer
            ]),
            NodeClock = vmq_swc_store:node_clock(Config),
            remote_clock_request(Config, Peer),
            {next_state, prepare, State#state{local_clock = NodeClock}, [
                {state_timeout, Timeout, remote_node_clock}
            ]};
        {error, already_locked} ->
            ?LOG_DEBUG(
                "Replica ~p: AE exchange with ~p terminated due to local store is locked", [
                    Group, Peer
                ]
            ),
            {stop, normal, State};
        Error ->
            ?LOG_ERROR("Replica ~p: AE exchange with ~p can't acquire local lock due to ~p", [
                Group, Peer, Error
            ]),
            {stop, normal, State}
    end;
prepare(state_timeout, PrepStep, #state{group = Group, peer = Peer} = State) ->
    ?LOG_WARNING("Replica ~p: AE exchange with ~p prepare step timed out in ~p", [
        Group, Peer, PrepStep
    ]),
    {stop, normal, State};
prepare(cast, {remote_node_clock, {error, Reason}}, #state{group = Group, peer = Peer} = State) ->
    %% Failed to get remote node clock
    ?LOG_WARNING("Replica ~p: AE exchange with ~p couldn't request remote node clock due to ~p", [
        Group, Peer, Reason
    ]),
    teardown(State);
prepare(
    cast,
    {remote_node_clock, NodeClock},
    #state{group = Group, peer = Peer, config = Config, timeout = Timeout} = State0
) ->
    ?LOG_DEBUG("Replica ~p: AE exchange with ~p successfully requested remote node clock", [
        Group, Peer
    ]),
    remote_watermark_request(Config, Peer),
    {next_state, prepare, State0#state{remote_clock = NodeClock}, [
        {state_timeout, Timeout, remote_watermark}
    ]};
prepare(cast, {remote_watermark, {error, Reason}}, #state{group = Group, peer = Peer} = State) ->
    %% Failed to get remote watermark
    ?LOG_WARNING("Replica ~p: AE exchange with ~p couldn't request remote watermark due to ~p", [
        Group, Peer, Reason
    ]),
    teardown(State);
prepare(cast, {remote_watermark, Watermark}, #state{group = Group, peer = Peer} = State0) ->
    ?LOG_DEBUG("Replica ~p: AE exchange with ~p successfully requested remote watermark", [
        Group, Peer
    ]),
    {next_state, update_local, State0#state{remote_watermark = Watermark}, [
        {next_event, internal, start}
    ]}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Initial Synchronisation of Keys/Values
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

initial_sync_new(
    internal,
    init_sync,
    #state{
        config = Config,
        peer = RemotePeer,
        batch_size = BatchSize
    } = State
) ->
    ?LOG_DEBUG("initial_sync new:init_sync from RemotePeer ~p~n", [RemotePeer]),
    as_event(
        fun() ->
            R = vmq_swc_store:remote_sync_missing_init(Config, RemotePeer, BatchSize, first),
            case R of
                {first, []} -> {sync_finished, no_keys};
                {last, N, ObjsA} -> {last, N, ObjsA};
                {NextKey, Objs} -> {NextKey, Objs}
            end
        end
    ),
    {next_state, initial_sync_new, State#state{start_ts = erlang:monotonic_time(millisecond)}, [
        {state_timeout, State#state.timeout, init_sync}
    ]};
initial_sync_new(
    cast,
    {sync_finished, Remaining},
    #state{
        obj_cnt = ObjCnt
    } = State
) ->
    case Remaining of
        no_keys -> teardown_initial_no_jump(State);
        _ -> teardown_initial(State#state{obj_cnt = ObjCnt + Remaining})
    end;
initial_sync_new(
    cast,
    {last, N, RemainingObjs},
    #state{
        config = Config,
        peer = RemotePeer,
        remote_clock = RemoteClock,
        remote_watermark = RemoteWatermark,
        obj_cnt = ObjCount
    } =
        State
) ->
    as_event(fun() ->
        vmq_swc_store:sync_repair_init(
            Config,
            RemainingObjs,
            RemotePeer,
            swc_node:base(RemoteClock),
            {true, RemoteWatermark}
        ),
        {sync_finished, 0}
    end),
    {next_state, initial_sync_new, State#state{obj_cnt = ObjCount + N}, [
        {state_timeout, State#state.timeout, init_sync}
    ]};
initial_sync_new(
    cast,
    {continue, NextKey1},

    #state{config = Config, peer = RemotePeer, batch_size = BatchSize} =
        State
) ->
    as_event(
        fun() ->
            R = vmq_swc_store:remote_sync_missing_init(
                Config, RemotePeer, BatchSize, NextKey1
            ),
            case R of
                {last, N, ObjsA} -> {last, N, ObjsA};
                {NextKey2, Objs} -> {NextKey2, Objs}
            end
        end
    ),
    {next_state, initial_sync_new, State, [
        {state_timeout, State#state.timeout, init_sync}
    ]};
initial_sync_new(
    cast,
    {NextKey, Objs},
    #state{
        config = Config,
        peer = RemotePeer,
        remote_clock = RemoteClock,
        batch_size = BatchSize,
        obj_cnt = LOCount
    } = State
) ->
    as_event(fun() ->
        vmq_swc_store:sync_repair_init(
            Config, Objs, RemotePeer, swc_node:base(RemoteClock), false
        ),
        {continue, NextKey}
    end),
    {next_state, initial_sync_new, State#state{obj_cnt = LOCount + BatchSize}, [
        {state_timeout, State#state.timeout, init_sync}
    ]};
initial_sync_new(cast, Msg, #state{group = Group, peer = Peer} = State) ->
    ?LOG_ERROR(
        "Replica ~p: AE exchange with ~p received unknown message during local init sync repair: ~p",
        [
            Group, Peer, Msg
        ]
    ),
    teardown_initial(State);
initial_sync_new(state_timeout, init_sync, #state{group = Group, peer = Peer} = State) ->
    ?LOG_WARNING(
        "Replica ~p: AE exchange with ~p couldn't sync repair (init) local store due to timeout", [
            Group, Peer
        ]
    ),
    % The next exchange FSM will try init sync again.
    teardown(State).

update_local(
    internal,
    start,
    #state{
        group = Group, config = Config, local_clock = NodeClock, remote_clock = RemoteClock
    } = State
) ->
    % calculate the dots missing on this node but existing on remote node
    Init = vmq_swc_store:get_init_sync(Config),
    GlobalInit = persistent_term:get({vmq_swc_group_coordinator, init_sync}, 0),
    case Init of
        false when GlobalInit == 0 ->
            {next_state, initial_sync_new,
                State#state{
                    start_ts = erlang:monotonic_time(millisecond)
                },
                [
                    {next_event, internal, init_sync}
                ]};
        true when GlobalInit == 0 ->
            vmq_swc_group_coordinator:group_initialized(Group, true),
            MissingDots = swc_node:missing_dots(RemoteClock, NodeClock, swc_node:ids(RemoteClock)),
            {next_state, local_sync_repair,
                State#state{
                    missing_dots = MissingDots, start_ts = erlang:monotonic_time(millisecond)
                },
                [
                    {next_event, internal, start}
                ]};
        true when GlobalInit == 1 ->
            MissingDots = swc_node:missing_dots(RemoteClock, NodeClock, swc_node:ids(RemoteClock)),
            {next_state, local_sync_repair,
                State#state{
                    missing_dots = MissingDots, start_ts = erlang:monotonic_time(millisecond)
                },
                [
                    {next_event, internal, start}
                ]};
        _ ->
            % not yet allowed to AE sync
            {stop, normal, State}
    end.

local_sync_repair(
    internal,
    start,
    #state{config = Config, peer = RemotePeer, missing_dots = MissingDots, batch_size = BatchSize} =
        State
) ->
    {Rest, BatchOfDots} = sync_repair_batch(MissingDots, BatchSize),
    as_event(
        fun() ->
            MissingObjects = vmq_swc_store:remote_sync_missing(Config, RemotePeer, BatchOfDots),
            {ok, MissingObjects}
        end
    ),
    {next_state, local_sync_repair, State#state{missing_dots = Rest}, [
        {state_timeout, State#state.timeout, sync_repair}
    ]};
local_sync_repair(
    cast,
    {ok, MissingObjects},
    #state{
        config = Config,
        peer = RemotePeer,
        remote_clock = RemoteClock,
        remote_watermark = RemoteWatermark,
        obj_cnt = ObjCnt
    } = State
) ->
    case State#state.missing_dots of
        [] ->
            vmq_swc_store:sync_repair(
                Config,
                MissingObjects,
                RemotePeer,
                swc_node:base(RemoteClock),
                {true, RemoteWatermark}
            ),
            teardown(State#state{obj_cnt = ObjCnt + length(MissingObjects)});
        _ ->
            vmq_swc_store:sync_repair(
                Config, MissingObjects, RemotePeer, swc_node:base(RemoteClock), false
            ),
            {next_state, local_sync_repair, State#state{obj_cnt = ObjCnt + length(MissingObjects)},
                [{next_event, internal, start}]}
    end;
local_sync_repair(cast, Msg, #state{group = Group, peer = Peer} = State) ->
    ?LOG_ERROR(
        "Replica ~p: AE exchange with ~p received unknown message during local sync repair: ~p", [
            Group, Peer, Msg
        ]
    ),
    teardown(State);
local_sync_repair(state_timeout, sync_repair, #state{group = Group, peer = Peer} = State) ->
    ?LOG_WARNING(
        "Replica ~p: AE exchange with ~p couldn't sync repair local store due to timeout", [
            Group, Peer
        ]
    ),
    teardown(State).

teardown(#state{group = Group, peer = Peer, obj_cnt = ObjCnt, start_ts = Start} = State) ->
    case State#state.obj_cnt > 0 of
        true ->
            End = erlang:monotonic_time(millisecond),
            SyncTime = End - Start,
            ?LOG_INFO("Replica ~p: AE exchange with ~p synced ~p objects in ~p milliseconds", [
                Group, Peer, ObjCnt, SyncTime
            ]);
        false ->
            ?LOG_DEBUG("Replica ~p: AE exchange with ~p, nothing to synchronize", [Group, Peer])
    end,
    {stop, normal, State}.

teardown_initial(
    #state{
        config = Config,
        group = Group,
        peer = Peer,
        obj_cnt = ObjCnt,
        remote_clock = RemoteClock,
        start_ts = Start
    } = State
) ->
    case State#state.obj_cnt > 0 of
        true ->
            End = erlang:monotonic_time(millisecond),
            SyncTime = End - Start,
            ?LOG_INFO(
                "Replica ~p: finished initial sync with ~p synced ~p objects in ~p milliseconds", [
                    Group, Peer, ObjCnt, SyncTime
                ]
            ),
            vmq_swc_store:jump_old_clocks(Config, RemoteClock);
        false ->
            ?LOG_DEBUG("Replica ~p: initial sync with ~p, nothing to synchronize", [Group, Peer])
    end,
    set_init_sync(Config, true),
    ok = vmq_swc_store:reset_iterator_by_groupname(Group),
    {stop, normal, State}.

teardown_initial_no_jump(
    #state{
        group = Group,
        config = Config,
        peer = Peer,
        obj_cnt = ObjCnt,
        start_ts = Start
    } = State
) ->
    End = erlang:monotonic_time(millisecond),
    SyncTime = End - Start,
    ?LOG_INFO(
        "Replica ~p: AE initial sync (no jump) with ~p synced ~p objects in ~p milliseconds", [
            Group, Peer, ObjCnt, SyncTime
        ]
    ),
    set_init_sync(Config, true),
    ok = vmq_swc_store:reset_iterator_by_groupname(Group),
    {stop, normal, State}.

%% Mandatory gen_statem callbacks
callback_mode() -> state_functions.

init([Config, Peer, Timeout]) ->
    BatchSize = application:get_env(vmq_swc, exchange_batch_size, 100),
    {ok, prepare,
        #state{
            config = Config,
            group = Config#swc_config.group,
            batch_size = BatchSize,
            peer = Peer,
            timeout = Timeout
        },
        [{next_event, internal, start}]}.

terminate(Reason, StateName, #state{group = Group, peer = Peer}) ->
    ?LOG_DEBUG(
        "Replica ~p: AE exchange with ~p terminates with reason ~p in state ~p",
        [Group, Peer, Reason, StateName]
    ),
    ok.

code_change(_Vsn, State, Data, _Extra) ->
    {ok, State, Data}.

%% internal
sync_repair_batch(MissingDots, BatchSize) ->
    sync_repair_batch(MissingDots, [], 0, BatchSize).

sync_repair_batch(Rest, Batch, BatchSize, BatchSize) ->
    {Rest, Batch};
sync_repair_batch([], Batch, _, _) ->
    {[], Batch};
sync_repair_batch([{_Id, []} | RestMissingDots], Batch, N, BatchSize) ->
    sync_repair_batch(RestMissingDots, Batch, N, BatchSize);
sync_repair_batch([{Id, [Dot | Dots]} | RestMissingDots], Batch, N, BatchSize) ->
    sync_repair_batch([{Id, Dots} | RestMissingDots], [{Id, Dot} | Batch], N + 1, BatchSize).

remote_clock_request(Config, Peer) ->
    as_event(fun() ->
        Res = vmq_swc_store:remote_node_clock(Config, Peer),
        {remote_node_clock, Res}
    end).

remote_watermark_request(Config, Peer) ->
    as_event(fun() ->
        Res = vmq_swc_store:remote_watermark(Config, Peer),
        {remote_watermark, Res}
    end).

as_event(F) ->
    Self = self(),
    spawn_link(fun() ->
        Result = F(),
        gen_statem:cast(Self, Result)
    end),
    ok.

set_init_sync(#swc_config{group = Group} = Config, Bool) ->
    ok = vmq_swc_store:set_init_sync_by_groupname(Group, Bool),
    ok = vmq_swc_db:put(Config, default, <<"ISY">>, term_to_binary(Bool)),
    vmq_swc_group_coordinator:group_initialized(Group, Bool).
