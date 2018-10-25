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

-module(vmq_swc_log_index).

-export([init/1,
         insert/4,
         remove/4,
         find_gc_candidates/3,
         find_gc_candidates/4,
         find_all/2,
         remove_gc_candidate/4]).
-define(TABLE, ?MODULE).

init(TableName) ->
    ets:new(TableName, [named_table]).

insert(TableName, Peer, SKey, Counter) ->
    TKey = {Peer, SKey},
    case ets:lookup(TableName, TKey) of
        [] ->
            ets:insert(TableName, {TKey, Counter, [Counter]});
        [{_, CurrentMin, Counters}] when CurrentMin < Counter ->
            ets:insert(TableName, {TKey, CurrentMin, [Counter|Counters]});
        [{_, _CurrentMin, Counters}] ->
            ets:insert(TableName, {TKey, Counter, [Counter|Counters]})
    end.

remove_gc_candidate(TableName, Peer, SKey, Counters) ->
    lists:foreach(fun(C) ->
                          remove(TableName, Peer, SKey, C)
                  end, Counters).

remove(TableName, Peer, SKey, Counter) ->
    TKey = {Peer, SKey},
    case ets:lookup(TableName, TKey) of
        [{_, _, Counters0}] ->
            case Counters0 -- [Counter] of
                [] ->
                    ets:delete(TableName, TKey);
                Counters1 ->
                    [NewCurrentMin|_] = Counters2 = lists:sort(Counters1),
                    ets:insert(TableName, {TKey, NewCurrentMin, Counters2})
            end;
        [] ->
            ok
    end.

find_gc_candidates(TableName, Peer, Min) ->
    find_gc_candidates(TableName, Peer, '_', Min).

find_gc_candidates(TableName, PeerPat, SKeyPat, Min) ->
    MatchSpec = [{{{PeerPat, SKeyPat}, '$1', '_'},[{'=<', '$1', Min}], ['$_']}],
    Matches = ets:select(TableName, MatchSpec), % TODO: use continuation limit
    lists:foldl(
      fun({{Peer, SKey}, _, Counters}, Acc) ->
              [{Peer, SKey, [C || C <- Counters, C =< Min]} | Acc]
      end, [], Matches).

find_all(TableName, PeerPat) ->
    MatchSpec = [{{{PeerPat, '_'}, '_', '_'},[], ['$_']}],
    Matches = ets:select(TableName, MatchSpec), % TODO: use continuation limit
    lists:foldl(
      fun({{Peer, SKey}, _, Counters}, Acc) ->
              [{Peer, SKey, [C || C <- Counters]} | Acc]
      end, [], Matches).
