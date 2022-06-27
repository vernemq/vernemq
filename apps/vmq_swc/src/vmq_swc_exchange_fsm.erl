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

-module(vmq_swc_exchange_fsm).
-include("vmq_swc.hrl").
-behaviour(gen_statem).

% API
-export([start_link/3]).
% State Functions
-export([
    prepare/3,
    update_local/3,
    local_sync_repair/3
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
    missing_dots
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
            lager:debug("Replica ~p: AE exchange with ~p successfully acquired local lock", [
                Group, Peer
            ]),
            NodeClock = vmq_swc_store:node_clock(Config),
            remote_clock_request(Config, Peer),
            {next_state, prepare, State#state{local_clock = NodeClock}, [
                {state_timeout, Timeout, remote_node_clock}
            ]};
        {error, already_locked} ->
            lager:debug(
                "Replica ~p: AE exchange with ~p terminated due to local store is locked", [
                    Group, Peer
                ]
            ),
            {stop, normal, State};
        Error ->
            lager:error("Replica ~p: AE exchange with ~p can't acquire local lock due to ~p", [
                Group, Peer, Error
            ]),
            {stop, normal, State}
    end;
prepare(state_timeout, PrepStep, #state{group = Group, peer = Peer} = State) ->
    lager:warning("Replica ~p: AE exchange with ~p prepare step timed out in ~p", [
        Group, Peer, PrepStep
    ]),
    {stop, normal, State};
prepare(cast, {remote_node_clock, {error, Reason}}, #state{group = Group, peer = Peer} = State) ->
    %% Failed to get remote node clock
    lager:warning("Replica ~p: AE exchange with ~p couldn't request remote node clock due to ~p", [
        Group, Peer, Reason
    ]),
    teardown(State);
prepare(
    cast,
    {remote_node_clock, NodeClock},
    #state{group = Group, peer = Peer, config = Config, timeout = Timeout} = State0
) ->
    lager:debug("Replica ~p: AE exchange with ~p successfully requested remote node clock", [
        Group, Peer
    ]),
    remote_watermark_request(Config, Peer),
    {next_state, prepare, State0#state{remote_clock = NodeClock}, [
        {state_timeout, Timeout, remote_watermark}
    ]};
prepare(cast, {remote_watermark, {error, Reason}}, #state{group = Group, peer = Peer} = State) ->
    %% Failed to get remote node clock
    lager:warning("Replica ~p: AE exchange with ~p couldn't request remote watermark due to ~p", [
        Group, Peer, Reason
    ]),
    teardown(State);
prepare(cast, {remote_watermark, Watermark}, #state{group = Group, peer = Peer} = State0) ->
    lager:debug("Replica ~p: AE exchange with ~p successfully requested remote watermark", [
        Group, Peer
    ]),
    {next_state, update_local, State0#state{remote_watermark = Watermark}, [
        {next_event, internal, start}
    ]}.

update_local(
    internal,
    start,
    #state{
        config = _Config, peer = _RemotePeer, local_clock = NodeClock, remote_clock = RemoteClock
    } = State
) ->
    %vmq_swc_store:update_watermark(Config, RemotePeer, RemoteClock),
    % calculate the dots missing on this node but exist on remote node
    MissingDots = swc_node:missing_dots(RemoteClock, NodeClock, swc_node:ids(RemoteClock)),
    {next_state, local_sync_repair, State#state{missing_dots = MissingDots}, [
        {next_event, internal, start}
    ]}.

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
    lager:error(
        "Replica ~p: AE exchange with ~p received unknown message during local sync repair: ~p", [
            Group, Peer, Msg
        ]
    ),
    teardown(State);
local_sync_repair(state_timeout, sync_repair, #state{group = Group, peer = Peer} = State) ->
    lager:warning(
        "Replica ~p: AE exchange with ~p couldn't sync repair local store due to timeout", [
            Group, Peer
        ]
    ),
    teardown(State).

teardown(#state{group = Group, peer = Peer, obj_cnt = ObjCnt} = State) ->
    case State#state.obj_cnt > 0 of
        true ->
            lager:info("Replica ~p: AE exchange with ~p synced ~p objects", [Group, Peer, ObjCnt]);
        false ->
            lager:debug("Replica ~p: AE exchange with ~p, nothing to synchronize", [Group, Peer])
    end,
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
    lager:debug(
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
