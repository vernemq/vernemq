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

-module(vmq_swc_db_rocksdb).
-include("vmq_swc.hrl").
-behaviour(vmq_swc_db).
-behaviour(gen_server).

%for vmq_swc_db behaviour
-export([childspecs/2,
         write/3,
         read/4,
         fold/5]).

-export([start_link/2,
        init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).

-record(db, {handle, default_cf, obj_cf, dkm_cf}).
-record(state, {db}).


% vmq_swc_db impl
childspecs(#swc_config{group=SwcGroup} = Config, Opts) ->
    [#{id => {?MODULE, SwcGroup},
       start => {?MODULE, start_link, [Config, Opts]}}].

-spec write(config(), list(db_op()), opts()) -> ok.
write(#swc_config{db=DBName}, Objects, Opts) ->
    [{_, DB}] = ets:lookup(DBName, refs),
    DBOps =
    lists:map(fun({Type, Key, ?DELETED}) ->
                      CF = db_cf(Type, DB),
                      {delete, CF, Key};
                 ({Type, Key, Value}) ->
                      CF = db_cf(Type, DB),
                      {put, CF, Key, Value}
              end, Objects),
    rocksdb:write(DB#db.handle, DBOps, Opts).

-spec read(config(), type(), db_key(), opts()) -> {ok, db_value()} | not_found.
read(#swc_config{db=DBName}, Type, Key, Opts) ->
    [{_, DB}] = ets:lookup(DBName, refs),
    CF = db_cf(Type, DB),
    rocksdb:get(DB#db.handle, CF, Key, Opts).

-spec fold(config(), type(), foldfun(), any(), first | db_key()) -> any().
fold(#swc_config{db=DBName}, Type, FoldFun, Acc0, FirstKey) ->
    [{_, #db{handle=Handle} = DB}] = ets:lookup(DBName, refs),
    CF = db_cf(Type, DB),
    {ok, Snapshot} = rocksdb:snapshot(Handle),
    {ok, Iterator} = rocksdb:iterator(Handle, CF, [{snapshot, Snapshot}]),
    Acc1 = fold_loop(Snapshot, Iterator, FirstKey, FoldFun, Acc0),
    try
        rocksdb:iterator_close(Iterator),
        rocksdb:release_snapshot(Snapshot)
    catch
        _:_ ->
            ok
    end,
    Acc1.

fold_loop(Snapshot, Iterator, NextItrAction, FoldFun, Acc0) ->
    case rocksdb:iterator_move(Iterator, NextItrAction) of
        {ok, Key, Value} ->
            case FoldFun(Key, Value, Acc0) of
                stop ->
                    Acc0;
                Acc1 ->
                   fold_loop(Snapshot, Iterator, next, FoldFun, Acc1)
            end;
        {error, _} ->
            % iterator is already closed at this point, release snapshot
            rocksdb:release_snapshot(Snapshot),
            Acc0
    end.

%% gen_server impl
start_link(#swc_config{db=DBName} = Config, Opts) ->
    gen_server:start_link({local, DBName}, ?MODULE, [Config | Opts], []).

init([#swc_config{peer={Peer, _Actor}, group=SwcGroup, db=DBName} = _Config|Opts]) ->
    DefaultDataDir = filename:join(filename:join(<<".">>, Peer), SwcGroup),

    DataDir = proplists:get_value(data_dir, Opts,
                                  application:get_env(vmq_swc, data_dir, binary_to_list(DefaultDataDir))),
    DataDir2 = filename:join(DataDir, SwcGroup),
    DbPath = filename:absname(DataDir2),
    filelib:ensure_dir(DbPath),

    ColumnFamilies = [{"default", []}, {"obj", []}, {"dkm", []}],
    CreateIfMissing = proplists:get_value(create_if_missing, Opts, true),
    CreateMissingCF = proplists:get_value(create_missing_column_families, Opts, true),
    % TODO Support further Rocksdb opts

    DbOpts = [{create_if_missing, CreateIfMissing},
              {create_missing_column_families, CreateMissingCF}],

    process_flag(trap_exit, true),
    case open_db(DbPath, DbOpts, ColumnFamilies, []) of
        {ok, #db{} = DBConfig} ->
            ets:new(DBName, [named_table, public, {read_concurrency, true}]),
            ets:insert(DBName, {refs, DBConfig}),
            {ok, #state{db=DBConfig}};
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call(_Req, _From, State) ->
    {reply, {error, not_implemented}, State}.

handle_cast(_Req, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{db=#db{handle=DbHandle}} =_State) ->
    catch rocksdb:close(DbHandle),
    ok.

code_change(_OldVsn, _NewVsn, State) ->
    State.

db_cf(?DB_DEFAULT, #db{default_cf=Default_CF}) -> Default_CF;
db_cf(?DB_OBJ, #db{obj_cf=Obj_CF}) -> Obj_CF;
db_cf(?DB_DKM, #db{dkm_cf=DKM_CF}) -> DKM_CF.

open_db(DbPath, DbOpts, ColumnFamilies, Opts) ->
    RetriesLeft = proplists:get_value(open_retries, Opts, 30),
    open_db(DbPath, DbOpts, ColumnFamilies, Opts, max(1, RetriesLeft), undefined).

open_db(_DbPath, _DbOpts, _ColumnFamilies, _Opts, 0, LastError) ->
    {error, LastError};
open_db(DbPath, DbOpts, ColumnFamilies, Opts, RetriesLeft, _) ->
    case  rocksdb:open_with_cf(DbPath, DbOpts, ColumnFamilies) of
        {ok, DbHandle, [Default_CF, Obj_CF, DKM_CF]} ->
            DBConfig = #db{handle=DbHandle, default_cf=Default_CF, obj_cf=Obj_CF, dkm_cf=DKM_CF},
            {ok, DBConfig};
        %% Check specifically for lock error, this can be caused if
        %% a crashed instance takes some time to flush leveldb information
        %% out to disk.  The process is gone, but the NIF resource cleanup
        %% may not have completed.
        {error, {db_open, OpenErr} = Reason} ->
            case lists:prefix("IO error: lock ", OpenErr) of
                true ->
                    SleepFor = proplists:get_value(open_retry_delay, Opts, 2000),
                    lager:debug("VerneMQ SWC RocksDB backend retrying ~p in ~p ms after error ~s\n",
                                [DbPath, SleepFor, OpenErr]),
                    timer:sleep(SleepFor),
                    open_db(DbPath, DbOpts, ColumnFamilies, Opts, RetriesLeft - 1, Reason);
                false ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.
