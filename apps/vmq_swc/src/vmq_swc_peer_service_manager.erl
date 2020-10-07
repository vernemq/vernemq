%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Helium Systems, Inc.  All Rights Reserved.
%%
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

-module(vmq_swc_peer_service_manager).

-define(TBL, swc_cluster_state).

-export([init/0, get_local_state/0, get_actor/0, get_old_actor/0, get_old_actor_from_state/2, get_actors/0, get_peers/0, 
        get_actors_and_peers/0, get_actor_for_peer/1, get_peers_for_actors/1, update_state/1, delete_state/0]).

init() ->
    %% setup ETS table for cluster_state
    _ = try ets:new(?TBL, [named_table, public, set, {keypos, 1}]) of
            _Res ->
                gen_actor(),
                maybe_load_state_from_disk(),
                ok
        catch
            error:badarg ->
                lager:warning("Table ~p already exists", [?TBL])
                %%TODO rejoin logic
        end,
    _ = try ets:new(old_actor_tab, [named_table, public, set, {keypos, 1}]) of
            _Res1 ->
                ok
        catch
            error:badarg ->
                lager:warning("Table ~p already exists", [old_actor])
                %%TODO rejoin logic
    end,
    ok.

%% @doc return local node's view of cluster membership
get_local_state() ->
   case hd(ets:lookup(?TBL, cluster_state)) of
       {cluster_state, State} ->
           {ok, State};
       _Else ->
           {error, _Else}
   end.

%% @doc return local node's current actor
get_actor() ->
    case hd(ets:lookup(?TBL, actor)) of
        {actor, Actor} ->
            {ok, Actor};
        _Else ->
            {error, _Else}
    end.

get_old_actor() ->
    case hd(ets:lookup(old_actor_tab, old_actor)) of
        {old_actor, Actor} ->
            {ok, Actor};
        _Else ->
            {error, _Else}
    end.

get_actors() ->
    {ok, LocalState} = get_local_state(),
    actors(LocalState).

get_actors_and_peers() ->
    {ok, LocalState} = get_local_state(),
    actors_and_vals(LocalState).

actors_and_vals({_Clock, Entries, _Deferred}) when is_list(Entries) ->
    [{K, Dots} || {K, Dots} <- Entries];
actors_and_vals({_Clock, Entries, _Deferred}) ->
    lists:sort([{K, Actor} || {K, [{[{actor, Actor}],_}]} <- dict:to_list(Entries)]).

get_peers() ->
    {ok, LocalState} = get_local_state(),
    {_Clock, Entries, _Deferred} = LocalState,
    lists:sort([K || {K, [{[{actor, _}], _}]} <- dict:to_list(Entries)]).

get_peers_for_actors(Actors) ->
    {ok, LocalState} = get_local_state(),
    {_, Entries, _} = LocalState,
    lists:sort([K || {K, [{[{actor, Actor}],_}]} <- dict:to_list(Entries), lists:member(Actor, Actors)]).

get_actor_for_peer(Peer) ->
    {ok, LocalState} = get_local_state(),
    proplists:get_value(Peer, actors_and_vals(LocalState)).

get_old_actor_from_state(Peer, State) ->
     proplists:get_value(Peer, actors_and_vals(State)).

actors({_Clock, Entries, _Deferred}) when is_list(Entries) ->
        [{K, Dots} || {K, Dots} <- Entries];
actors({_Clock, Entries, _Deferred}) ->
        lists:sort([Actor || {K, [{[{actor, Actor}],_}]} <- dict:to_list(Entries)]).

%% @doc update cluster_state
update_state(State) ->
    write_state_to_disk(State),
    ets:insert(?TBL, {cluster_state, State}).

delete_state() ->
    delete_state_from_disk().

%%% ------------------------------------------------------------------
%%% internal functions
%%% ------------------------------------------------------------------

%% @doc initialize singleton cluster
add_self() ->
    Initial = riak_dt_orswot:new(),
    Actor = ets:lookup(?TBL, actor),
    {ok, LocalState} = riak_dt_orswot:update({add, node()}, Actor, Initial),
    update_state(LocalState).

%% @doc generate an actor for this node while alive
gen_actor() ->
    Node = atom_to_list(node()),
    {M, S, U} = erlang:timestamp(),
    TS = integer_to_list(M * 1000 * 1000 * 1000 * 1000 + S * 1000 * 1000 + U),
    Term = Node ++ TS,
    Actor = crypto:hash(sha, Term),
    ets:insert(?TBL, {actor, Actor}).

data_root() ->
    application:get_env(vmq_swc, data_dir,
                        "./" ++ atom_to_list(node()) ++ "/peer_service").

write_state_to_disk(State) ->
    case data_root() of
        undefined ->
            ok;
        Dir ->
            File = filename:join(Dir, "cluster_state"),
            ok = filelib:ensure_dir(File),
            lager:info("writing state ~p to disk ~p",
                       [State, riak_dt_orswot:to_binary(State)]),
            ok = file:write_file(File,
                                 riak_dt_orswot:to_binary(State))
    end.

delete_state_from_disk() ->
    case data_root() of
        undefined ->
            ok;
        Dir ->
            File = filename:join(Dir, "cluster_state"),
            ok = filelib:ensure_dir(File),
            case file:delete(File) of
                ok ->
                    lager:info("Leaving cluster, removed cluster_state");
                {error, Reason} ->
                    lager:info("Unable to remove cluster_state for reason ~p", [Reason])
            end
    end.

maybe_load_state_from_disk() ->
    case data_root() of
        undefined ->
            add_self();
        Dir ->
            case filelib:is_regular(filename:join(Dir, "cluster_state")) of
                true ->
                    {ok, Bin} = file:read_file(filename:join(Dir,
                                                             "cluster_state")),
                    {ok, State} = riak_dt_orswot:from_binary(Bin),
                    OldActor = get_old_actor_from_state(node(), State),
                    ets:insert(?TBL, {old_actor, OldActor}),
                %  OldActor = get_old_actor_from_state(node(), State),
                %  ets:insert(old_actor_tab, {old_actor, OldActor}),
                    Actor = ets:lookup(?TBL, actor),
                    {ok, State1} = riak_dt_orswot:update({add, node()}, Actor, State), % we always want to save the Actor for SWC
                %  _ = gen_server:cast(vmq_swc_peer_service_gossip, {receive_state, Merged}),
                    lager:info("read state from file ~p~n", [State1]),
                    update_state(State1);
                false ->
                    add_self()
            end
    end.
