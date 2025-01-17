%% Copyright 2018 Octavo Labs AG Zurich Switzerland (https://octavolabs.com)
%% Copyright 2018-2024 Octavo Labs/VerneMQ (https://vernemq.com/)
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

-module(vmq_swc_group_coordinator).
-include("vmq_swc.hrl").
-include_lib("kernel/include/logger.hrl").

-behaviour(gen_server).

%% API
-export([
    start_link/0,
    group_initialized/2,
    sync_state/0
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER, ?MODULE).

-record(state, {sync_state}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

group_initialized(Group, Bool) ->
    gen_server:call(?MODULE, {change_init, Group, Bool}).

sync_state() ->
    gen_server:call(?MODULE, get_sync_state).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {_, Groups} = persistent_term:get({vmq_swc_plugin, swc}),
    GroupsInitState = [{G, false} || G <- Groups],
    {ok, #state{sync_state = maps:from_list(GroupsInitState)}}.

handle_call(get_sync_state, _From, #state{sync_state = SyncState} = State) ->
    {reply, SyncState, State};
handle_call({change_init, Group, Bool}, _From, #state{sync_state = SyncState} = State) ->
    M = maps:update(Group, Bool, SyncState),
    check_sync_state(M),
    {reply, ok, State#state{sync_state = M}}.

handle_cast(_Info, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

check_sync_state(M) ->
    L = maps:to_list(M),
    case lists:all(fun({_, B}) -> B == true end, L) of
        % all partitions are now init synced, we set a global marker
        true ->
            persistent_term:put({?MODULE, init_sync}, 1),
            ok = enable_broadcast(L);
        _ ->
            ok
    end.

enable_broadcast(L) ->
    ?LOG_DEBUG("Enable full broadcast for Groups ~p~n", [L]),
    lists:foldl(
        fun({Group, _}, _Acc) -> ok = vmq_swc_store:set_broadcast_by_groupname(Group, true) end,
        ok,
        L
    ).
