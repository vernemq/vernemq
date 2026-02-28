%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Helium Systems, Inc.  All Rights Reserved.
%% Copyright 2018-2024 Octavo Labs/VerneMQ (https://vernemq.com/)
%% and Individual Contributors.
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(vmq_swc_peer_service_gossip).
-include_lib("kernel/include/logger.hrl").

-behavior(gen_server).

-define(DEFAULT_GOSSIP_INTERVAL, 15000).
-define(DEFAULT_FAST_GOSSIP_INTERVAL, 2000).
-define(DEFAULT_FAST_GOSSIP_DURATION, 30000).

-record(state, {
    mode = normal :: normal | fast,
    timer_ref :: reference() | undefined,
    fast_deadline = 0 :: integer()
}).

-export([start_link/0, stop/0]).
-export([receive_state/1, notify_membership_change/0]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%%%==================================================================
%%% gen_server api
%%%==================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:call(?MODULE, stop).

receive_state(PeerState) ->
    gen_server:cast(?MODULE, {receive_state, PeerState}).

notify_membership_change() ->
    gen_server:cast(?MODULE, trigger_fast_gossip).

%%%===============================================================
%%% gen_server callbacks
%%%===============================================================

init([]) ->
    TimerRef = schedule_gossip(normal),
    {ok, #state{timer_ref = TimerRef}}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(send_state, _From, State) ->
    {ok, LocalState} = vmq_swc_peer_service_manager:get_local_state(),
    {reply, {ok, LocalState}, State}.

handle_cast({receive_state, PeerState}, State) ->
    {ok, LocalState} = vmq_swc_peer_service_manager:get_local_state(),
    case riak_dt_orswot:equal(PeerState, LocalState) of
        true ->
            {noreply, State};
        false ->
            Merged = riak_dt_orswot:merge(PeerState, LocalState),
            vmq_swc_peer_service_manager:update_state(Merged),
            vmq_swc_peer_service_events:update(Merged),
            {noreply, enter_fast_mode(State)}
    end;
handle_cast(trigger_fast_gossip, State) ->
    {noreply, enter_fast_mode(State)};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(gossip, #state{mode = fast} = State) ->
    case maybe_exit_fast_mode(State) of
        {normal, NewState} ->
            _ = do_gossip(),
            {noreply, NewState};
        {fast, State1} ->
            _ = do_gossip_all(),
            TimerRef = schedule_gossip(fast),
            {noreply, State1#state{timer_ref = TimerRef}}
    end;
handle_info(gossip, #state{mode = normal} = State) ->
    _ = do_gossip(),
    TimerRef = schedule_gossip(normal),
    {noreply, State#state{timer_ref = TimerRef}};
handle_info(_Info, State) ->
    ?LOG_INFO("Unexpected: ~p,~p.~n", [_Info, State]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ?LOG_INFO("terminate ~p, ~p.~n", [_Reason, _State]),
    {ok, _State}.

code_change(_OldVsn, [], _Extra) ->
    TimerRef = schedule_gossip(normal),
    {ok, #state{timer_ref = TimerRef}};
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===============================================================
%%% private functions
%%%===============================================================

enter_fast_mode(#state{timer_ref = OldTimerRef} = State) ->
    cancel_timer(OldTimerRef),
    _ = do_gossip_all(),
    Deadline = erlang:monotonic_time(millisecond) + fast_gossip_duration(),
    TimerRef = schedule_gossip(fast),
    State#state{mode = fast, timer_ref = TimerRef, fast_deadline = Deadline}.

maybe_exit_fast_mode(#state{fast_deadline = Deadline} = State) ->
    Now = erlang:monotonic_time(millisecond),
    case Now >= Deadline of
        true ->
            TimerRef = schedule_gossip(normal),
            {normal, State#state{mode = normal, timer_ref = TimerRef, fast_deadline = 0}};
        false ->
            {fast, State}
    end.

%% @doc send local state to all peers
do_gossip_all() ->
    {ok, Local} = vmq_swc_peer_service_manager:get_local_state(),
    case get_peers(Local) of
        [] ->
            {error, singleton};
        Peers ->
            _ = [gen_server:cast({?MODULE, P}, {receive_state, Local}) || P <- Peers],
            ok
    end.

%% @doc initiate gossip to a single random peer
do_gossip() ->
    {ok, Local} = vmq_swc_peer_service_manager:get_local_state(),
    case get_peers(Local) of
        [] ->
            {error, singleton};
        Peers ->
            {ok, Peer} = random_peer(Peers),
            gen_server:cast({?MODULE, Peer}, {receive_state, Local})
    end.

%% @doc returns a list of peer nodes
get_peers(Local) ->
    Members = riak_dt_orswot:value(Local),
    [X || X <- Members, X /= node()].

%% @doc return random peer from nodelist
random_peer(Peers) ->
    Idx = rand:uniform(length(Peers)),
    Peer = lists:nth(Idx, Peers),
    {ok, Peer}.

schedule_gossip(normal) ->
    erlang:send_after(gossip_interval(), self(), gossip);
schedule_gossip(fast) ->
    erlang:send_after(fast_gossip_interval(), self(), gossip).

cancel_timer(undefined) ->
    ok;
cancel_timer(Ref) ->
    _ = erlang:cancel_timer(Ref),
    receive
        gossip -> ok
    after 0 ->
        ok
    end.

gossip_interval() ->
    application:get_env(vmq_swc, gossip_interval, ?DEFAULT_GOSSIP_INTERVAL).

fast_gossip_interval() ->
    application:get_env(vmq_swc, fast_gossip_interval, ?DEFAULT_FAST_GOSSIP_INTERVAL).

fast_gossip_duration() ->
    application:get_env(vmq_swc, fast_gossip_duration, ?DEFAULT_FAST_GOSSIP_DURATION).
