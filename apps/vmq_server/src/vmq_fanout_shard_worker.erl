%% Copyright 2026- Octavo Labs/VerneMQ (https://vernemq.com/)
%% and Individual Contributors.
%% Licensed under the Apache License, Version 2.0.

-module(vmq_fanout_shard_worker).

-behaviour(gen_server).

-export([
    start_link/1,
    fold/5,
    async_fold/5
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

start_link(Index) ->
    gen_server:start_link({local, worker_name(Index)}, ?MODULE, [Index], []).

fold(Index, Key, SubscriberId, FoldFun, Acc) ->
    gen_server:call(worker_name(Index), {fold, Key, SubscriberId, FoldFun, Acc}, infinity).

async_fold(Index, Key, SubscriberId, FoldFun, Acc) ->
    gen_server:cast(worker_name(Index), {async_fold, Key, SubscriberId, FoldFun, Acc}).

init([Index]) ->
    {ok, Index}.

handle_call({fold, Key, SubscriberId, FoldFun, Acc}, _From, Shard) ->
    {reply, vmq_reg_trie:fold_fanout_shard(Shard, Key, SubscriberId, FoldFun, Acc), Shard};
handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({async_fold, Key, SubscriberId, FoldFun, Acc}, Shard) ->
    _ = vmq_reg_trie:fold_fanout_shard(Shard, Key, SubscriberId, FoldFun, Acc),
    {noreply, Shard};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

worker_name(Index) ->
    list_to_atom("vmq_fanout_shard_worker_" ++ integer_to_list(Index)).
