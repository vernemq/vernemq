%% Copyright 2026- Octavo Labs/VerneMQ (https://vernemq.com/)
%% and Individual Contributors.
%% Licensed under the Apache License, Version 2.0.

-module(vmq_fanout_shard_sup).

-behaviour(supervisor).

-export([
    start_link/0,
    worker_count/0,
    fold/5,
    async_fold/5
]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

worker_count() ->
    persistent_term:get({?MODULE, worker_count}, 1).

fold(Key, SubscriberId, FoldFun, Acc, ShardCount) ->
    fold(Key, SubscriberId, FoldFun, Acc, 0, ShardCount).

async_fold(Key, SubscriberId, FoldFun, Acc, ShardCount) ->
    async_fold(Key, SubscriberId, FoldFun, Acc, 0, ShardCount).

init([]) ->
    Count = configured_worker_count(),
    persistent_term:put({?MODULE, worker_count}, Count),
    {ok, {{one_for_one, 5, 10}, child_specs(Count)}}.

child_specs(Count) when Count > 1 ->
    [
        {worker_id(N), {vmq_fanout_shard_worker, start_link, [N]}, permanent, 5000, worker, [
            vmq_fanout_shard_worker
        ]}
     || N <- lists:seq(0, Count - 1)
    ];
child_specs(_) ->
    [].

configured_worker_count() ->
    case application:get_env(vmq_server, fanout_shard_count, 1) of
        Count when is_integer(Count), Count > 1 -> Count;
        _ -> 1
    end.

fold(_Key, _SubscriberId, _FoldFun, Acc, Shard, ShardCount) when Shard >= ShardCount ->
    Acc;
fold(Key, SubscriberId, FoldFun, Acc, Shard, ShardCount) ->
    Acc1 = vmq_fanout_shard_worker:fold(Shard, Key, SubscriberId, FoldFun, Acc),
    fold(Key, SubscriberId, FoldFun, Acc1, Shard + 1, ShardCount).

async_fold(_Key, _SubscriberId, _FoldFun, _Acc, Shard, ShardCount) when Shard >= ShardCount ->
    ok;
async_fold(Key, SubscriberId, FoldFun, Acc, Shard, ShardCount) ->
    ok = vmq_fanout_shard_worker:async_fold(Shard, Key, SubscriberId, FoldFun, Acc),
    async_fold(Key, SubscriberId, FoldFun, Acc, Shard + 1, ShardCount).

worker_id(Index) ->
    list_to_atom("vmq_fanout_shard_worker_" ++ integer_to_list(Index)).
