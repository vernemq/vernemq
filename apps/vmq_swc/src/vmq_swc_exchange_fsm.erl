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
-export([prepare/3,
         update_local/3,
         update_remote/3,
         local_sync_repair/3,
         remote_sync_repair/3]).

-export([init/1, terminate/3, code_change/4, callback_mode/0]).

-record(state, {config, peer, timeout, local_clock, remote_clock, batch_size=100, missing_dots}).

start_link(#swc_config{} = Config, Peer, Timeout) ->
    gen_statem:start_link(?MODULE, [Config, Peer, Timeout], []).

% State functions
prepare(internal, start, #state{config=Config, peer=Peer, timeout=Timeout} = State) ->
    case vmq_swc_store:lock(Config) of
        ok ->
            %% get remote lock
            remote_lock_request(Config, Peer),
            {keep_state_and_data, [{state_timeout, Timeout, remote_lock}]};
        _Error ->
            {stop, normal, State}
    end;

prepare(state_timeout, PrepStep, #state{peer=Peer} = State) ->
    lager:error("swc exchange with ~p timed out in ~p", [Peer, PrepStep]),
    % only need to unlock local lock
    vmq_swc_store:unlock(State#state.config),
    {stop, normal, State};

prepare(cast, {remote_lock, ok}, #state{config=Config, peer=Peer, timeout=Timeout} = State0) ->
    NodeClock = vmq_swc_store:node_clock(Config),
    remote_clock_request(Config, Peer),
    {next_state, prepare, State0#state{local_clock=NodeClock},
     [{state_timeout, Timeout, remote_node_clock}]};

prepare(cast, {remote_lock, Error}, State) ->
    %% Failed to get remote lock
    lager:debug("swc exchange with ~p failed aquiring locks ~p", [State#state.peer, Error]),
    % only need to unlock local lock
    vmq_swc_store:unlock(State#state.config),
    {stop, normal, State};

prepare(cast, {remote_node_clock, {error, Reason}}, State) ->
    %% Failed to get remote node clock
    lager:warning("swc exchange with ~p failed aquiring remote node clock ~p", [State#state.peer, Reason]),
    terminate(State);

prepare(cast, {remote_node_clock, NodeClock}, State0) ->
    {next_state, update_local, State0#state{remote_clock=NodeClock},
     [{next_event, internal, start}]}.

update_local(internal, start, #state{config=Config, peer=RemotePeer, local_clock=NodeClock, remote_clock=RemoteClock} = State) ->
    vmq_swc_store:update_watermark(Config, RemotePeer, RemoteClock),
    % calculate the dots missing on this node but exist on remote node
    MissingDots = swc_node:missing_dots(RemoteClock, NodeClock, swc_node:ids(RemoteClock)),
    {next_state, local_sync_repair, State#state{missing_dots=MissingDots},
     [{next_event, internal, start}]}.

update_remote(internal, start, #state{config=Config, peer=RemotePeer, local_clock=NodeClock, remote_clock=RemoteClock} = State) ->
    vmq_swc_store:remote_update_watermark(Config, RemotePeer, NodeClock),
    MissingDots = swc_node:missing_dots(NodeClock, RemoteClock, swc_node:ids(NodeClock)),
    {next_state, remote_sync_repair, State#state{missing_dots=MissingDots},
     [{next_event, internal, start}]}.

local_sync_repair(internal, start, #state{config=Config, peer=RemotePeer, remote_clock=RemoteClock, missing_dots=MissingDots, batch_size=BatchSize} = State) ->
    {Rest, BatchOfDots} = sync_repair_batch(MissingDots, BatchSize),
    as_event(
      fun() ->
              case vmq_swc_store:remote_sync_missing(Config, RemotePeer, BatchOfDots) of
                  {error, _Reason} = E -> E;
                  MissingObjects ->
                      vmq_swc_store:sync_repair(Config, MissingObjects, RemotePeer, swc_node:base(RemoteClock), Rest == [])
              end
      end),
    {next_state, local_sync_repair, State#state{missing_dots=Rest},
     [{state_timeout, State#state.timeout, sync_repair}]};
local_sync_repair(cast, ok, State) ->
    case State#state.missing_dots of
        [] ->
            {next_state, update_remote, State,
             [{next_event, internal, start}]};
        _ ->
            {next_state, local_sync_repair, State,
             [{next_event, internal, start}]}
    end;
local_sync_repair(cast, E, State) ->
    lager:warning("local sync repair error ~p", [E]),
    terminate(State);
local_sync_repair(state_timeout, sync_repair, State) ->
    lager:warning("local sync repair timeout", []),
    terminate(State).

remote_sync_repair(internal, start, #state{config=Config, peer=RemotePeer, local_clock=LocalClock, missing_dots=MissingDots, batch_size=BatchSize} = State) ->
    {Rest, BatchOfDots} = sync_repair_batch(MissingDots, BatchSize),
    as_event(
      fun() ->
              case vmq_swc_store:sync_missing(Config, BatchOfDots) of
                  {error, _Reason} = E -> E;
                  MissingObjects ->
                      vmq_swc_store:remote_sync_repair(Config, MissingObjects, RemotePeer, swc_node:base(LocalClock), Rest == [])
              end
      end),
    {next_state, remote_sync_repair, State#state{missing_dots=Rest},
     [{state_timeout, State#state.timeout, sync_repair}]};
remote_sync_repair(cast, ok, State) ->
    case State#state.missing_dots of
        [] ->
            terminate(State);
        _ ->
            {next_state, remote_sync_repair, State,
             [{next_event, internal, start}]}
    end;
remote_sync_repair(cast, E, State) ->
    lager:warning("remote sync repair error ~p", [E]),
    terminate(State);
remote_sync_repair(state_timeout, sync_repair, State) ->
    lager:warning("remote sync repair timeout", []),
    terminate(State).

terminate(State) ->
    vmq_swc_store:remote_unlock(State#state.config, State#state.peer),
    vmq_swc_store:unlock(State#state.config),
    {stop, normal, State}.

%% Mandatory gen_statem callbacks
callback_mode() -> state_functions.

init([Config, Peer, Timeout]) ->
    {ok, prepare, #state{config=Config,
                         peer=Peer,
                         timeout=Timeout}, [{next_event, internal, start}]}.

terminate(_Reason, _State, _Data) ->
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
    sync_repair_batch([{Id, Dots}|RestMissingDots], [{Id, Dot}|Batch], N + 1, BatchSize).

remote_clock_request(Config, Peer) ->
    as_event(fun() ->
                     Res = vmq_swc_store:remote_node_clock(Config, Peer),
                     {remote_node_clock, Res}
             end).

remote_lock_request(Config, Peer) ->
    as_event(fun() ->
                     Res = vmq_swc_store:remote_lock(Config, Peer),
                     {remote_lock, Res}
             end).

as_event(F) ->
    Self = self(),
    spawn_link(fun() ->
                       Result = F(),
                       gen_statem:cast(Self, Result)
               end),
    ok.
