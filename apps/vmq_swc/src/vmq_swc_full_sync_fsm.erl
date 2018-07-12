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

-module(vmq_swc_full_sync_fsm).
-include("vmq_swc.hrl").
-behaviour(gen_statem).

% API
-export([start_link/3]).
% State Functions
-export([prepare/3,
         sync/3]).

-export([init/1, terminate/3, code_change/4, callback_mode/0]).

% Remote Iterator
-export([start_iterator/3,
         init_iterator/1]).

-export([rpc_batch/4,
         rpc_start_iterator/3]).

-record(state, {config,
                peer, timeout, built, local_clock, remote_clock, batch_size=100}).

start_link(Config, Peer, Timeout) ->
    gen_statem:start_link(?MODULE, [Config, Peer, Timeout], []).

% State functions
prepare(state_timeout, acquire_lock, #state{config=Config, peer=Peer} = State0) ->
    case vmq_swc_store:lock(Config) of
        ok ->
            NodeClock = vmq_swc_store:node_clock(Config),
            remote_clock_request(Config, Peer),
            {next_state, prepare, State0#state{local_clock=NodeClock},
             [{state_timeout, State0#state.timeout, remote_node_clock}]};
        _Error ->
            {keep_state_and_data, [{state_timeout, 1000, acquire_lock}]}
    end;

prepare(cast, {remote_node_clock, NodeClock}, State0) ->
    {next_state, sync, State0#state{remote_clock=NodeClock},
     [{next_event, internal, start}]}.

sync(internal, start, #state{config=Config, peer=Peer, batch_size=BatchSize} = State) ->
    case start_remote_iterator(Config, Peer, self(), BatchSize) of
        {ok, _UnusablePid} ->
            {keep_state_and_data, [{state_timeout, State#state.timeout, sync}]};
        E ->
            lager:error("Could not start remote iterator on ~p due to ~p", [Peer, E]),
            terminate(normal, State)

    end;
sync(state_timeout, sync, #state{peer=Peer} = State) ->
    lager:error("Sync timeout with ~p", [Peer]),
    terminate(normal, State);

sync({call, From}, {batch, MissingObjects0, AllData}, #state{config=Config,
                                                             peer=Peer,
                                                             remote_clock=RemoteClock} = State) ->
    MissingObjects1 =
    lists:map(fun({SKey, BDCC}) ->
                      DCC0 = binary_to_term(BDCC),
                      DCC1 = swc_kv:fill(DCC0, State#state.local_clock),
                      {SKey, DCC1}
              end, MissingObjects0),
    vmq_swc_store:sync_repair(Config, MissingObjects1, Peer, swc_node:base(RemoteClock), AllData),
    gen_statem:reply(From, ok),
    case AllData of
        true ->
            terminate(normal, State);
        false ->
            keep_state_and_data
    end.

terminate(Reason, #state{config=Config} = State) ->
    vmq_swc_store:unlock(Config),
    {stop, Reason, State}.

callback_mode() -> state_functions.

init([Config, Peer, Timeout]) ->
    {ok, prepare, #state{
                     config=Config,
                     peer=Peer, built=0, timeout=Timeout}, [{state_timeout, 0, acquire_lock}]}.

terminate(_Reason, _State, _Data) ->
    ok.

code_change(_Vsn, State, Data, _Extra) ->
    {ok, State, Data}.


start_remote_iterator(#swc_config{transport=TMod, peer=Peer} = Config,
                      SyncNode, FsmRef, BatchSize) ->
    TMod:rpc(Config, SyncNode, ?MODULE, rpc_start_iterator,
                     [Peer, [FsmRef, BatchSize]]).

rpc_start_iterator(RemotePeer, Args, Config) ->
    %% Called Remotely, Config is injected by Diameter handle_request
    vmq_swc_exchange_sup:start_full_exchange_iterator(Config, RemotePeer, Args).

start_iterator(Config, RemotePeer, [FsmRef, BatchSize]) ->
    proc_lib:start_link(?MODULE, init_iterator, [[self(), Config, RemotePeer, FsmRef, BatchSize]]).

rpc_batch(FsmRef, Batch, AllData, _Config) ->
    gen_statem:call(FsmRef, {batch, Batch, AllData}, infinity).

init_iterator([Parent, #swc_config{transport=TMod} = Config, RemotePeer, FsmRef, BatchSize]) ->
    proc_lib:init_ack(Parent, {ok, self()}),
    SendFun = fun(Batch, AllData) ->
                      TMod:rpc(Config, RemotePeer, ?MODULE, rpc_batch, [FsmRef, Batch, AllData])
              end,
    Iterator = vmq_swc_db:iterator(Config, dcc),
    loop_iterator(Parent, vmq_swc_db:iterator_next(Iterator), [], 0, BatchSize, SendFun).

loop_iterator(Parent, ItrRet, Batch, N, BatchSize, SendFun) ->
    receive
        {system, From, Request} ->
            sys:handle_system_msg(Request, From, Parent, ?MODULE, [], {Batch, N, BatchSize, SendFun});
        Msg ->
            lager:error("vmq_swc_full_sync_fsm iterator received unexpected message ~p", [Msg])
    after
        0 ->
            loop_iterator_(Parent, ItrRet, Batch, N, BatchSize, SendFun)
    end.

loop_iterator_(_, '$end_of_table', Batch, _, _, SendFun) ->
    SendFun(Batch, true);
loop_iterator_(Parent, {_Value, _Iterator} = ItrRet, Batch, BatchSize, BatchSize, SendFun) ->
    SendFun(Batch, false),
    loop_iterator(Parent, ItrRet, [], 0, BatchSize, SendFun);
loop_iterator_(Parent, {{SKey, BDCC}, Iterator}, Batch, N, BatchSize, SendFun) ->
    loop_iterator(Parent, vmq_swc_db:iterator_next(Iterator), [{SKey, BDCC}|Batch], N + 1, BatchSize, SendFun).


remote_clock_request(Config, Peer) ->
    as_event(fun() ->
                     NodeClock = vmq_swc_store:remote_node_clock(Config, Peer),
                     {remote_node_clock, NodeClock}
             end).

as_event(F) ->
    Self = self(),
    spawn_link(fun() ->
                       Result = F(),
                       gen_statem:cast(Self, Result)
               end),
    ok.
