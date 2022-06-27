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
-module(vmq_swc_dkm).
-include("vmq_swc.hrl").

%% The vmq_swc_dkm implements a high performant and scalable DotKeyMap (DKM).
%% The DKM is the index structure that is used by the incremental Garbage
%% Collection that is kicked off after every Anti Entropy Exchange.
%%
%% The original implementation of the DKM in swc_dotkeymap uses orddicts
%% to store the dots. For performance reasons we use a dotkeymap that is backed
%% by multiple ETS tables. Unlike to the 'official' dotkeymap this implementation
%% keeps track of the active set of dots, which are dots that relate to undeleted
%% objects. Therefore a `prune` operation will only delete a dot that relates to
%% either a deleted object or the dot was superseded by a newer dot.
%%
%% An example:
%%
%% 1: insert(KeyA, (a, 1))
%% 2: insert(KeyA, (a, 2)) ----> incremental delete (a, 1)
%% 3: insert(KeyA, (b, 1))
%% 4: prune(a, 2) ----------> deletes (a, 2)
%% 5: insert(KeyA, (c, 1))
%% 6: mark_for_gc(KeyA)
%% 7: prune(b, 1) ----------> deletes (b, 1)
%% 8: prune(c, 1) ----------> deletes (c, 1) and object with key KeyA
%%
%% Such a DKM that manages all active dots can grow substantially. The size
%% of the active set of dots is at least the size of all the keys in the system.
%% A naive approach would require a full table scan to find all dots that are out
%% of date. To reduce the complexity of incremental GC this dotkeymap requires
%% three different ETS tables, `latest`, `latest_candidates`, and `gc_candidates`.
%% During dot insertion and pruning dots move from one table to the others depending on
%% if they are superseded by a new dot of the same origin.
%%
%% The `latest` and `latest_candidates` tables contain the set of most recent dots.
%% Therefore if a dot gets superseded it creates an incremental delete.
%% In case pruning would select a dot that is currently set as latest dot for a keey
%% it will replace such a dot with a candidate dot from `latest_candidates`.
%% If no suitable candidate exists the dot doesn't get pruned. The GC will do an
%% ets:select on the `latest_candidates` table. The size of the `latest_candidates`
%% table depends on the number of unpruned concurrent writes.
%%
%% By definition the largest table is the `latest`, which is why ets:select is not
%% an option for incremental GC. Instead a different table `gc_candidates` is used
%% to mark 'to-be-deleted' Keys. As all elements in the `latest` set are active they
%% are only subject to GC if marked as a `gc_candidate`.

-export([
    init/0,
    insert/4,
    mark_for_gc/2,
    prune/3,
    % testing
    prune/4,
    prune_for_peer/2,
    destroy/1,
    test/0,
    dump/1,
    % used by test
    dkm/1,
    info/2
]).

-record(dkm, {
    latest = ets:new(?MODULE, [public]),
    latest_candidates = ets:new(?MODULE, [public]),
    gc_candidates = ets:new(?MODULE, [public])
}).

-type dkm() :: #dkm{}.
-export_type([dkm/0]).

-spec init() -> dotkeymap().
init() ->
    #dkm{}.

info(DKM, object_count) ->
    ets:info(DKM#dkm.latest, size);
info(DKM, tombstone_count) ->
    ets:info(DKM#dkm.gc_candidates, size);
info(#dkm{latest = LT, latest_candidates = LTC, gc_candidates = GCT}, memory) ->
    ets:info(LT, memory) + ets:info(LTC, memory) + ets:info(GCT, memory).

dump(#dkm{latest = LT, gc_candidates = GCT, latest_candidates = LTC}) ->
    #{
        latest => dump(LT),
        latest_candidates => dump(LTC),
        gc_candidates => dump(GCT)
    };
dump(T) ->
    ets:foldl(
        fun(Obj, Acc) ->
            [Obj | Acc]
        end,
        [],
        T
    ).

dkm(#dkm{latest = LT, latest_candidates = LTC}) ->
    LatestObjects =
        ets:foldl(
            fun({Key, Dot}, Acc) ->
                maps:put(Dot, Key, Acc)
            end,
            #{},
            LT
        ),
    ets:foldl(
        fun
            ({{_, _} = Dot, Key}, Acc) ->
                maps:put(Dot, Key, Acc);
            ({_Key, _Dots}, Acc) ->
                Acc
        end,
        LatestObjects,
        LTC
    ).

-spec insert(dotkeymap(), peer(), counter(), db_key()) -> [db_op()].
insert(#dkm{latest = LT, gc_candidates = GCT} = DKM, Id, Cnt, Key) ->
    Dot = {Id, Cnt},
    case ets:insert_new(LT, {Key, Dot}) of
        true ->
            [{dkm, sext:encode(Dot), Key}];
        false ->
            ReplacedDots =
                case ets:lookup(LT, Key) of
                    [{_, {Id, CurrentCnt} = CurrentDot}] when Cnt > CurrentCnt ->
                        % remove possible GC candidate
                        ets:delete(GCT, Key),
                        TmpDot = replace_candidate_or_gc(DKM, Dot, CurrentDot, Key),
                        % dot newer than current dot -> replace
                        ets:insert(LT, {Key, Dot}),
                        [TmpDot];
                    [{_, {Id, _} = CurrentDot}] ->
                        % new dot is older than current dot -> replace candidate or gc
                        [replace_candidate_or_gc(DKM, Dot, CurrentDot, Key)];
                    [{_, CurrentDot}] ->
                        % remove possible GC candidate
                        ets:delete(GCT, Key),
                        % this is a new latest candidate
                        insert_candidates(DKM, Dot, CurrentDot, Key)
                end,
            {ShouldInsertDot, DbOps} =
                lists:foldl(
                    fun
                        (D, {_Flag, Acc}) when D == Dot ->
                            {false, Acc};
                        (D, {Flag, Acc}) ->
                            {Flag, [{?DB_DKM, sext:encode(D), ?DELETED} | Acc]}
                    end,
                    {true, []},
                    ReplacedDots
                ),
            case ShouldInsertDot of
                true ->
                    [{?DB_DKM, sext:encode(Dot), Key} | DbOps];
                false ->
                    DbOps
            end
    end.

replace_candidate_or_gc(
    #dkm{latest_candidates = LTC}, {Id, NewCnt} = Dot, {Id, OldCnt} = CurrentDot, Key
) ->
    case ets:lookup(LTC, Key) of
        [] ->
            % no candidate available, ignore this entry
            % GC current dot
            CurrentDot;
        [{_, Dots}] ->
            case maps:get(Id, Dots) of
                OldCnt when NewCnt > OldCnt ->
                    % replace latest candidate
                    OldDot = {Id, OldCnt},
                    ets:insert(LTC, [{Key, maps:put(Id, NewCnt, Dots)}, {Dot, Key}]),
                    ets:delete(LTC, OldDot),
                    % GC Old Dot
                    OldDot;
                OldCnt ->
                    % GC Dot
                    Dot
            end
    end.

insert_candidates(#dkm{latest_candidates = LTC}, Dot, CurrentDot, Key) ->
    case ets:lookup(LTC, Key) of
        [] ->
            ets:insert(LTC, [
                {Key, maps:from_list([Dot, CurrentDot])}, {Dot, Key}, {CurrentDot, Key}
            ]),
            [];
        [{_, Dots}] ->
            {Dots0, NewDots0, UnusedDots0} = insert_candidate_dot(Dot, Dots, [], []),
            {Dots1, NewDots1, UnusedDots1} = insert_candidate_dot(
                CurrentDot, Dots0, NewDots0, UnusedDots0
            ),
            case NewDots1 of
                [] ->
                    % latest candidates didn't change
                    ok;
                _ ->
                    ets:insert(LTC, [{Key, Dots1} | [{D, Key} || D <- NewDots1]])
            end,
            _ = [ets:delete(LTC, D) || D <- UnusedDots1],
            UnusedDots1
    end.

insert_candidate_dot({Id, Cnt} = Dot, Dots, NewDots, UnusedDots) ->
    case maps:get(Id, Dots, undefined) of
        undefined ->
            {maps:put(Id, Cnt, Dots), [Dot | NewDots], UnusedDots};
        Cnt ->
            {Dots, NewDots, UnusedDots};
        TmpCnt when TmpCnt < Cnt ->
            {maps:put(Id, Cnt, Dots), [Dot | NewDots], [{Id, TmpCnt} | UnusedDots]};
        _ ->
            {Dots, NewDots, [Dot | UnusedDots]}
    end.

-spec mark_for_gc(dotkeymap(), db_key()) -> ok.
mark_for_gc(#dkm{latest = LT, gc_candidates = GCT}, Key) ->
    case ets:lookup(LT, Key) of
        [] -> ok;
        _ -> ets:insert(GCT, {Key})
    end,
    ok.

-spec prune(dotkeymap(), watermark(), [db_op()]) -> [db_op()].
prune(#dkm{} = DKM, Watermark, DbOps) ->
    lists:foldl(
        fun(Id, Acc) ->
            Min = swc_watermark:min(Watermark, Id),
            prune(DKM, Id, Min, Acc)
        end,
        DbOps,
        swc_watermark:peers(Watermark)
    ).

prune(#dkm{gc_candidates = GCT} = DKM, Id, Min, DbOps) ->
    ets:foldl(
        fun({Key}, Acc) ->
            prune_latest_dot(DKM, Key, Id, Min, Acc, true)
        end,
        prune_candidates(DKM, Id, Min, DbOps),
        GCT
    ).

prune_candidates(#dkm{latest_candidates = LTC, latest = LT}, Id, Min, DbOps) ->
    MatchSpec = [{{{Id, '$1'}, '_'}, [{'=<', '$1', Min}], ['$_']}],
    % TODO: use continuation limit
    case ets:select(LTC, MatchSpec) of
        [] ->
            DbOps;
        Matches ->
            lists:foldl(
                fun({Dot, Key}, Acc) ->
                    [{_, Dots0}] = ets:lookup(LTC, Key),
                    Dots1 = maps:remove(Id, Dots0),
                    case maps:to_list(Dots1) of
                        [LastCandidateDot] ->
                            % replace latest with last available candidate
                            ets:insert(LT, {Key, LastCandidateDot}),
                            ets:delete(LTC, Dot),
                            ets:delete(LTC, LastCandidateDot),
                            ets:delete(LTC, Key),
                            [{?DB_DKM, sext:encode(Dot), ?DELETED} | Acc];
                        [NextCandidateDot | _] ->
                            % replace latest with next available candidate
                            ets:insert(LT, {Key, NextCandidateDot}),
                            ets:delete(LTC, Dot),
                            ets:insert(LTC, {Key, Dots1}),
                            [{?DB_DKM, sext:encode(Dot), ?DELETED} | Acc]
                    end
                end,
                DbOps,
                Matches
            )
    end.

prune_latest_dot(
    #dkm{latest = LT, gc_candidates = GCT, latest_candidates = LTC}, Key, Id, Min, Acc, IsGC
) ->
    case ets:lookup(LT, Key) of
        [{_, {Id, CurrentCnt} = CurrentDot}] when CurrentCnt =< Min ->
            % can we replace the latest with a candidate
            case ets:lookup(LTC, Key) of
                [] when IsGC ->
                    ets:delete(LT, Key),
                    ets:delete(GCT, Key),
                    % no candidate available, this object can be deleted
                    [
                        {?DB_DKM, sext:encode(CurrentDot), ?DELETED},
                        {?DB_OBJ, Key, ?DELETED}
                        | Acc
                    ];
                _ ->
                    Acc
            end;
        _ ->
            Acc
    end.

prune_for_peer(#dkm{latest = LT} = DKM, Id) ->
    ets:foldl(
        fun
            ({Key, {I, Cnt}}, Acc0) when I == Id ->
                Acc1 = prune_candidates(DKM, Id, Cnt, Acc0),
                prune_latest_dot(DKM, Key, Id, Cnt, Acc1, false);
            (_, Acc) ->
                Acc
        end,
        [],
        LT
    ).

destroy(#dkm{latest = LT, gc_candidates = GCT}) ->
    ets:delete(LT),
    ets:delete(GCT),
    ok.

test() ->
    DKM = init(),
    Dot1 = sext:encode({a, 1}),
    Dot2 = sext:encode({a, 2}),
    Dot3 = sext:encode({b, 1}),
    insert(DKM, a, 1, <<"hello">>),
    insert(DKM, b, 1, <<"hello">>),
    [] = prune(DKM, a, 1, []),
    insert(DKM, a, 2, <<"hello">>),
    [{?DB_DKM, Dot1, ?DELETED}] = prune(DKM, a, 1, []),
    mark_for_gc(DKM, <<"hello">>),
    [] = prune(DKM, a, 1, []),
    [{?DB_DKM, Dot2, ?DELETED}] = prune(DKM, a, 2, []),
    [
        {?DB_DKM, Dot3, ?DELETED},
        {?DB_OBJ, hello, ?DELETED}
    ] = prune(DKM, b, 1, []).
