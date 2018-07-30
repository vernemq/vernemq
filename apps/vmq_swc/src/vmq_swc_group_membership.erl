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
         get_alive_members/1,
         peer_event/2]).

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

get_alive_members(#swc_config{membership=Name}) ->
    %% the table name equals to the gen_server name
    ets:foldl(fun({Peer, _Cnt}, Acc) -> [Peer|Acc] end, [], Name).

peer_event(#swc_config{membership=Name}, Event) ->
    gen_server:cast(Name, {peer_event, Event}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([#swc_config{membership=Membership, transport=TMod} = Config,
      Strategy, {TMod, Addr, Port, Opts} = _T]) ->
    Members =
    case Strategy of
        plumtree ->
            {ok, LocalState} = plumtree_peer_service_manager:get_local_state(),
            [atom_to_binary(M, utf8) || M <- riak_dt_orswot:value(LocalState)];
        manual ->
            []
    end,
    schedule_register_peer_events(0),
    ets:new(Membership, [public, named_table, {read_concurrency, true}]),
    TMod:init(Config),

    process_flag(trap_exit, true),

    TMod:start_listener(Config, Port, [{ip, Addr}|Opts]),

    {ok, #state{config=Config,
                members=connect_members(Config, Members, Strategy),
                strategy=Strategy}}.

handle_call(get_members, _From, #state{members=MembersWithTransport} = State) ->
    {Members, _} = lists:unzip(MembersWithTransport),
    {reply, Members, State}.

handle_cast({set_members, NewMembers},  #state{config=Config, members=OldMembersWithTransport, strategy=Strategy} = State) ->
    {OldMembers, _} = lists:unzip(OldMembersWithTransport),
    MembersToAdd = NewMembers -- OldMembers,
    MembersToDel = OldMembers -- NewMembers,
    MembersToDelWithTransport = lists:filter(
                                  fun({O, _}) ->
                                          lists:member(O, MembersToDel)
                                  end, OldMembersWithTransport),

    MembersToAddWithTransport = connect_members(Config, MembersToAdd, Strategy),
    disconnect_members(Config, MembersToDelWithTransport, Strategy),
    NewMembersWithTransport = ((OldMembersWithTransport -- MembersToDelWithTransport)
                               ++ MembersToAddWithTransport),

    vmq_swc_store:set_group_members(Config, NewMembers),
    {noreply, State#state{members=NewMembersWithTransport}};

handle_cast({peer_event, Event}, #state{config=Config} = State) ->
    #swc_config{membership=Membership} = Config,
    case Event of
        {up, Peer} ->
            ets:insert_new(Membership, {Peer, 0}),
            ets:update_counter(Membership, Peer, {2, 1});
        {down, Peer} ->
            ets:insert_new(Membership, {Peer, 0}),
            case ets:update_counter(Membership, Peer, {2, -1, 0, 0}) of
                0 -> ets:delete(Membership, Peer);
                _ -> ignore
            end
    end,
    {noreply, State}.

handle_info(register_peer_events, #state{config=Config, strategy=Strategy} = State) ->
    register_peer_events(Strategy, Config),
    {noreply, State#state{event_mgr_pid=event_mgr_pid(Strategy)}};
handle_info({'EXIT', EventMgrPid, _Reason}, #state{event_mgr_pid=EventMgrPid} = State) ->
    schedule_register_peer_events(1000),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{config=Config} =_State) ->
    #swc_config{transport=TMod} = Config,
    catch TMod:terminate(Config),
    ok.

code_change(_OldVsn, State, _Extra) ->
        {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
connect_members(Config, Members, Strategy) ->
    connect_members(Config, Members, Strategy, []).
connect_members(_Config, [], _, Acc) -> Acc;
connect_members(#swc_config{peer=Member} = Config, [Member|Rest], plumtree, Acc) ->
    connect_members(Config, Rest, plumtree, [{Member, reconnect}|Acc]);
connect_members(#swc_config{transport=TMod, group=SwcGroup} = Config, [Member|Rest], plumtree, Acc) ->
    case rpc:call(binary_to_atom(Member, utf8), TMod, listener_address_port, [SwcGroup]) of
        {ok, {IP, Port}} ->
            case TMod:start_connection(Config, IP, Port, []) of
                {ok, TRef} ->
                    connect_members(Config, Rest, plumtree, [{Member, TRef}|Acc]);
                {error, _Reason} ->
                    connect_members(Config, Rest, plumtree, [{Member, reconnect}|Acc])
            end;
        _ ->
        %{error, _Reason} ->
        %{badrpc, _Reason} ->
            connect_members(Config, Rest, plumtree, [{Member, reconnect}|Acc])
    end.

disconnect_members(_Config, [], _) -> ok;
disconnect_members(Config, [{_Member, reconnect}|Rest], Strategy) ->
    disconnect_members(Config, Rest, Strategy);
disconnect_members(#swc_config{transport=TMod} = Config, [{_Member,  TRef}|Rest], Strategy) ->
    TMod:stop_connection(Config, TRef),
    disconnect_members(Config, Rest, Strategy).

schedule_register_peer_events(T) ->
    erlang:send_after(T, self(), register_peer_events).

register_peer_events(plumtree, Config) ->
    plumtree_peer_service_events:add_sup_callback(
      fun(Update) ->
              set_members(Config, [atom_to_binary(M, utf8) || M <- riak_dt_orswot:value(Update)])
      end);
register_peer_events(_UnknownStrategy, _Config) -> ignore.

event_mgr_pid(plumtree) ->
    whereis(plumtree_peer_service_events);
event_mgr_pid(_UnknownStrategy) ->
    unknown_strategy.
