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

-module(vmq_swc_group_membership).
-include("vmq_swc.hrl").

-behaviour(gen_server).

%% API
-export([start_link/3,
         set_members/2,
         get_members/1,
         get_actors/1,
         swc_ids/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {config, members, strategy, event_mgr_pid}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(#swc_config{membership=Name} = Config, Strategy, Transport) ->
    gen_server:start_link({local, Name}, ?MODULE, [Config, Strategy, Transport], []).

set_members(#swc_config{membership=Name}, GroupMembers) ->
    gen_server:cast(Name, {set_members, GroupMembers}).

get_members(#swc_config{membership=Name}) ->
    gen_server:call(Name, get_members, infinity).

get_actors(#swc_config{membership=Name}) ->
    gen_server:call(Name, get_actors, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([#swc_config{transport=TMod} = Config,
      Strategy, {TMod, Opts} = _T]) ->
    Members =
    case Strategy of
        auto ->
            {ok, LocalState} = vmq_swc_peer_service_manager:get_local_state(),
            riak_dt_orswot:value(LocalState);
        manual ->
            []
    end,
    schedule_register_peer_events(0),
    TMod:transport_init(Config, Opts),
    ok = connect_members(Config, Members),
    {ok, #state{config=Config,
                members=Members,
                strategy=Strategy}}.

handle_call(get_members, _From, #state{members=Members} = State) ->
    {reply, Members, State};

handle_call(get_actors, _From, #state{members=_Members} = State) ->
    Actors = vmq_swc_peer_service_manager:get_actors(),
    {reply, Actors, State}.

handle_cast({set_members, NewMembers},  #state{config=Config, members=OldMembers} = State) ->
    % NewActors = get_actors(Config),
    MembersToAdd = NewMembers -- OldMembers,
    % MembersToAdd = NewActors -- OldMembers,
    MembersToDel = OldMembers -- NewMembers,
    ok = connect_members(Config, MembersToAdd), % need the nodenames here
    ok = disconnect_members(Config, MembersToDel),
    vmq_swc_store:set_group_members(Config, swc_ids(NewMembers)), % need the swc_id here: {peer(), actor()}
    {noreply, State#state{members=NewMembers}}.

swc_ids(Peers) ->
    [{Peer, vmq_swc_peer_service_manager:get_actor_for_peer(Peer)} || Peer <- Peers].

handle_info(register_peer_events, #state{config=Config, strategy=Strategy} = State) ->
    register_peer_events(Strategy, Config),
    {noreply, State#state{event_mgr_pid=event_mgr_pid(Strategy)}};
handle_info({'EXIT', EventMgrPid, _Reason}, #state{event_mgr_pid=EventMgrPid} = State) ->
    schedule_register_peer_events(1000),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
        {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
connect_members(_Config, []) -> ok;
connect_members(#swc_config{peer=Member} = Config, [Member|Rest]) ->
    connect_members(Config, Rest);
connect_members(#swc_config{transport=TMod} = Config, [Member|Rest]) ->
    TMod:start_connection(Config, Member),
    connect_members(Config, Rest).

disconnect_members(_Config, []) -> ok;
disconnect_members(#swc_config{transport=TMod} = Config, [Member|Rest]) ->
    TMod:stop_connection(Config, Member),
    disconnect_members(Config, Rest).

schedule_register_peer_events(T) ->
    erlang:send_after(T, self(), register_peer_events).

register_peer_events(auto, Config) ->
    vmq_swc_peer_service_events:add_sup_callback(
      fun(Update) ->
              set_members(Config, riak_dt_orswot:value(Update))
      end);
register_peer_events(_UnknownStrategy, _Config) -> ignore.

event_mgr_pid(auto) ->
    whereis(vmq_swc_peer_service_events);
event_mgr_pid(_UnknownStrategy) ->
    unknown_strategy.
