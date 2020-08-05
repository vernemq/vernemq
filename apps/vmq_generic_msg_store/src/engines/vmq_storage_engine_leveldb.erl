%% Copyright 2019 Octavo Labs AG Zurich Switzerland (http://octavolabs.com)
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

-module(vmq_storage_engine_leveldb).

-export([open/2, close/1, write/2, read/2, fold/3, fold/4]).

-record(state, {
          ref :: undefined | eleveldb:db_ref(),
          data_root :: string(),
          open_opts = [],
          config :: config(),
          read_opts = [],
          write_opts = [],
          fold_opts = [{fill_cache, false}]
         }).

-type state() :: #state{}.
-type config() :: [{atom(), term()}].
-type key() :: binary().
-type value() :: binary().
-type write_op() :: {put, key(), value()} | {delete, key()}.

% API

open(DataRoot, Opts) ->
    RetriesLeft = proplists:get_value(open_retries, Opts, 30),
    State = init_state(DataRoot, Opts),
    open_db(Opts, State, max(1, RetriesLeft), undefined).

-spec write(state(), [write_op()]) -> ok.
write(#state{ref=EngineRef, write_opts=WriteOpts}, WriteOps) ->
    eleveldb:write(EngineRef, WriteOps, WriteOpts).

-spec read(state(), key()) -> {ok, value()} | not_found.
read(#state{ref=EngineRef, read_opts=ReadOpts}, Key) ->
    eleveldb:get(EngineRef, Key, ReadOpts).

fold(#state{ref=EngineRef, fold_opts=FoldOpts}, Fun, Acc) ->
    {ok, Itr} = eleveldb:iterator(EngineRef, FoldOpts),
    fold_iterate(eleveldb:iterator_move(Itr, first), Itr, Fun, Acc).

fold(#state{ref=EngineRef, fold_opts=FoldOpts}, Fun, Acc, FirstKey) ->
    {ok, Itr} = eleveldb:iterator(EngineRef, FoldOpts),
    fold_iterate(eleveldb:iterator_move(Itr, FirstKey), Itr, Fun, Acc).

fold_iterate({error, _}, _Itr, _Fun, Acc) ->
    %% no need to close the iterator
    Acc;
fold_iterate({ok, Key, Value}, Itr, Fun, Acc0) ->
   try Fun(Key, Value, Acc0) of
       Acc1 ->
           fold_iterate(eleveldb:iterator_move(Itr, prefetch), Itr, Fun, Acc1)
   catch
       throw:_Throw ->
           eleveldb:iterator_close(Itr),
           Acc0
   end.

close(#state{ref=EngineRef}) ->
    eleveldb:close(EngineRef).


% Internal
init_state(DataRoot, Config) ->
    %% Get the data root directory
    filelib:ensure_dir(filename:join(DataRoot, "msg_store_dummy")),

    %% Merge the proplist passed in from Config with any values specified by the
    %% eleveldb app level; precedence is given to the Config.
    MergedConfig = orddict:merge(fun(_K, VLocal, _VGlobal) -> VLocal end,
                                 orddict:from_list(Config), % Local
                                 orddict:from_list(application:get_all_env(eleveldb))), % Global

    %% Use a variable write buffer size in order to reduce the number
    %% of vnodes that try to kick off compaction at the same time
    %% under heavy uniform load...
    WriteBufferMin = config_value(write_buffer_size_min, MergedConfig, 30 * 1024 * 1024),
    WriteBufferMax = config_value(write_buffer_size_max, MergedConfig, 60 * 1024 * 1024),
    WriteBufferSize = WriteBufferMin + rand:uniform(1 + WriteBufferMax - WriteBufferMin),

    %% Update the write buffer size in the merged config and make sure create_if_missing is set
    %% to true
    FinalConfig = orddict:store(write_buffer_size, WriteBufferSize,
                                orddict:store(create_if_missing, true, MergedConfig)),

    %% Parse out the open/read/write options
    {OpenOpts, _BadOpenOpts} = eleveldb:validate_options(open, FinalConfig),
    {ReadOpts, _BadReadOpts} = eleveldb:validate_options(read, FinalConfig),
    {WriteOpts, _BadWriteOpts} = eleveldb:validate_options(write, FinalConfig),

    %% Use read options for folding, but FORCE fill_cache to false
    FoldOpts = lists:keystore(fill_cache, 1, ReadOpts, {fill_cache, false}),

    %% Warn if block_size is set
    SSTBS = proplists:get_value(sst_block_size, OpenOpts, false),
    BS = proplists:get_value(block_size, OpenOpts, false),
    case BS /= false andalso SSTBS == false of
        true ->
            lager:warning("eleveldb block_size has been renamed sst_block_size "
                          "and the current setting of ~p is being ignored.  "
                          "Changing sst_block_size is strongly cautioned "
                          "against unless you know what you are doing.  Remove "
                          "block_size from app.config to get rid of this "
                          "message.\n", [BS]);
        _ ->
            ok
    end,

    %% Generate a debug message with the options we'll use for each operation
    lager:debug("datadir ~s options for LevelDB: ~p\n",
                [DataRoot, [{open, OpenOpts}, {read, ReadOpts}, {write, WriteOpts}, {fold, FoldOpts}]]),
    #state { data_root = DataRoot,
             open_opts = OpenOpts,
             read_opts = ReadOpts,
             write_opts = WriteOpts,
             fold_opts = FoldOpts,
             config = FinalConfig }.

config_value(Key, Config, Default) ->
    case orddict:find(Key, Config) of
        error ->
            Default;
        {ok, Value} ->
            Value
    end.

open_db(_Opts, _State0, 0, LastError) ->
    {error, LastError};
open_db(Opts, State0, RetriesLeft, _) ->
    DataRoot = State0#state.data_root,
    case eleveldb:open(DataRoot, State0#state.open_opts) of
        {ok, Ref} ->
            lager:info("Opening LevelDB database at ~p~n", [DataRoot]),
            {ok, State0#state { ref = Ref }};
        %% Check specifically for lock error, this can be caused if
        %% a crashed instance takes some time to flush leveldb information
        %% out to disk.  The process is gone, but the NIF resource cleanup
        %% may not have completed.
        {error, {db_open, OpenErr}=Reason} ->
            case lists:prefix("Corruption: truncated record ", OpenErr) of
                true ->
                    lager:info("VerneMQ LevelDB Message Store backend repair attempt for store ~p, after error ~s. LevelDB will put unusable .log and MANIFEST filest in 'lost' folder.\n",
                            [DataRoot, OpenErr]),
                    case eleveldb:repair(DataRoot, []) of
                        ok -> % LevelDB will put unusable .log and MANIFEST files in 'lost' folder.
                            open_db(Opts, State0, RetriesLeft - 1, Reason);
                        {error, Reason} -> {error, Reason}
                    end;
                
                false ->
                    case lists:prefix("IO error: lock ", OpenErr) of
                        true ->
                            SleepFor = proplists:get_value(open_retry_delay, Opts, 2000),
                            lager:info("VerneMQ LevelDB Message Store backend retrying ~p in ~p ms after error ~s\n",
                                        [DataRoot, SleepFor, OpenErr]),
                            timer:sleep(SleepFor),
                            open_db(Opts, State0, RetriesLeft - 1, Reason);
                        false ->
                        {error, Reason}
                    end
            end;
        {error, Reason} ->
            {error, Reason}
    end.