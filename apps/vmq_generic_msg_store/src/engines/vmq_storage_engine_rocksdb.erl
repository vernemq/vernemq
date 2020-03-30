%% Copyright 2020 Octavo Labs AG Zurich Switzerland (http://octavolabs.com)
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

-module(vmq_storage_engine_rocksdb).

-export([open/2, close/1, write/2, read/2, fold/3, fold/4]).

-record(state, {
          ref :: undefined | rocksdb:db_handle(),
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


-define(COMPRESSION_ENUM, [snappy, zlib, bzip2, lz4, lz4h, zstd, none]).
-define(EXPIRY_ENUM, [unlimited, integer]).

%% RocksDB specific types, copied from rocksdb.erl for now

-record(db_path, {path        :: file:filename_all(),
          target_size :: non_neg_integer()}).

-record(cf_descriptor, {name    :: string(),
                        options :: cf_options()}).

-type cache_type() :: lru | clock.
-type compression_type() :: snappy | zlib | bzip2 | lz4 | lz4h | zstd | none.
-type compaction_style() :: level | universal | fifo | none.
-type compaction_pri() :: compensated_size | oldest_largest_seq_first | oldest_smallest_seq_first.
-type access_hint() :: normal | sequential | willneed | none.
-type wal_recovery_mode() :: tolerate_corrupted_tail_records |
               absolute_consistency |
               point_in_time_recovery |
               skip_any_corrupted_records.

-opaque env_handle() :: reference() | binary().
-opaque sst_file_manager() :: reference() | binary().
-opaque db_handle() :: reference() | binary().
-opaque cf_handle() :: reference() | binary().
-opaque itr_handle() :: reference() | binary().
-opaque snapshot_handle() :: reference() | binary().
-opaque batch_handle() :: reference() | binary().
-opaque transaction_handle() :: reference() | binary().
-opaque backup_engine() :: reference() | binary().
-opaque cache_handle() :: reference() | binary().
-opaque rate_limiter_handle() :: reference() | binary().
-opaque write_buffer_manager() :: reference() | binary().

-type column_family() :: cf_handle() | default_column_family.

-type env_type() :: default | memenv.
-opaque env() :: env_type() | env_handle().
-type env_priority() :: priority_high | priority_low.

-type block_based_table_options() :: [{no_block_cache, boolean()} |
                                      {block_size, pos_integer()} |
                                      {block_cache, cache_handle()} |
                                      {block_cache_size, pos_integer()} |
                                      {bloom_filter_policy, BitsPerKey :: pos_integer()} |
                                      {format_version, 0 | 1 | 2} |
                                      {cache_index_and_filter_blocks, boolean()}].

-type merge_operator() :: erlang_merge_operator |
                          bitset_merge_operator |
                          {bitset_merge_operator, non_neg_integer()} |
                          counter_merge_operator.

-type cf_options() :: [{block_cache_size_mb_for_point_lookup, non_neg_integer()} |
                       {memtable_memory_budget, pos_integer()} |
                       {write_buffer_size,  pos_integer()} |
                       {max_write_buffer_number,  pos_integer()} |
                       {min_write_buffer_number_to_merge,  pos_integer()} |
                       {compression,  compression_type()} |
                       {num_levels,  pos_integer()} |
                       {level0_file_num_compaction_trigger,  integer()} |
                       {level0_slowdown_writes_trigger,  integer()} |
                       {level0_stop_writes_trigger,  integer()} |
                       {max_mem_compaction_level,  pos_integer()} |
                       {target_file_size_base,  pos_integer()} |
                       {target_file_size_multiplier,  pos_integer()} |
                       {max_bytes_for_level_base,  pos_integer()} |
                       {max_bytes_for_level_multiplier,  pos_integer()} |
                       {max_compaction_bytes,  pos_integer()} |
                       {soft_rate_limit,  float()} |
                       {hard_rate_limit,  float()} |
                       {arena_block_size,  integer()} |
                       {disable_auto_compactions,  boolean()} |
                       {purge_redundant_kvs_while_flush,  boolean()} |
                       {compaction_style,  compaction_style()} |
                       {compaction_pri,  compaction_pri()} |
                       {filter_deletes,  boolean()} |
                       {max_sequential_skip_in_iterations,  pos_integer()} |
                       {inplace_update_support,  boolean()} |
                       {inplace_update_num_locks,  pos_integer()} |
                       {table_factory_block_cache_size, pos_integer()} |
                       {in_memory_mode, boolean()} |
                       {block_based_table_options, block_based_table_options()} |
                       {level_compaction_dynamic_level_bytes, boolean()} |
                       {optimize_filters_for_hits, boolean()} |
                       {prefix_transform, {fixed_prefix_transform, integer()} | 
                                           {capped_prefix_transform, integer()}} |
                       {merge_operator, merge_operator()}
                      ].

-type db_options() :: [{env, env()} |
                       {total_threads, pos_integer()} |
                       {create_if_missing, boolean()} |
                       {create_missing_column_families, boolean()} |
                       {error_if_exists, boolean()} |
                       {paranoid_checks, boolean()} |
                       {max_open_files, integer()} |
                       {max_total_wal_size, non_neg_integer()} |
                       {use_fsync, boolean()} |
                       {db_paths, list(#db_path{})} |
                       {db_log_dir, file:filename_all()} |
                       {wal_dir, file:filename_all()} |
                       {delete_obsolete_files_period_micros, pos_integer()} |
                       {max_background_jobs, pos_integer()} |
                       {max_background_compactions, pos_integer()} |
                       {max_background_flushes, pos_integer()} |
                       {max_log_file_size, non_neg_integer()} |
                       {log_file_time_to_roll, non_neg_integer()} |
                       {keep_log_file_num, pos_integer()} |
                       {max_manifest_file_size, pos_integer()} |
                       {table_cache_numshardbits, pos_integer()} |
                       {wal_ttl_seconds, non_neg_integer()} |
                       {manual_wal_flush, boolean()} |
                       {wal_size_limit_mb, non_neg_integer()} |
                       {manifest_preallocation_size, pos_integer()} |
                       {allow_mmap_reads, boolean()} |
                       {allow_mmap_writes, boolean()} |
                       {is_fd_close_on_exec, boolean()} |
                       {skip_log_error_on_recovery, boolean()} |
                       {stats_dump_period_sec, non_neg_integer()} |
                       {advise_random_on_open, boolean()} |
                       {access_hint, access_hint()} |
                       {compaction_readahead_size, non_neg_integer()} |
                       {new_table_reader_for_compaction_inputs, boolean()} |
                       {use_adaptive_mutex, boolean()} |
                       {bytes_per_sync, non_neg_integer()} |
                       {skip_stats_update_on_db_open, boolean()} |
                       {wal_recovery_mode, wal_recovery_mode()} |
                       {allow_concurrent_memtable_write, boolean()} |
                       {enable_write_thread_adaptive_yield, boolean()} |
                       {db_write_buffer_size, non_neg_integer()}  |
                       {in_memory, boolean()} |
                       {rate_limiter, rate_limiter_handle()} |
                       {sst_file_manager, sst_file_manager()} |
                       {write_buffer_manager, write_buffer_manager()} |
                       {max_subcompactions, non_neg_integer()} |
                       {atomic_flush, boolean()}].

-type options() :: db_options() | cf_options().

-type read_options() :: [{verify_checksums, boolean()} |
                         {fill_cache, boolean()} |
                         {iterate_upper_bound, binary()} |
                         {iterate_lower_bound, binary()} |
                         {tailing, boolean()} |
                         {total_order_seek, boolean()} |
                         {prefix_same_as_start, boolean()} |
                         {snapshot, snapshot_handle()}].

-type write_options() :: [{sync, boolean()} |
                          {disable_wal, boolean()} |
                          {ignore_missing_column_families, boolean()} |
                          {no_slowdown, boolean()} |
                          {low_pri, boolean()}].

-type write_actions() :: [{put, Key::binary(), Value::binary()} |
                          {put, ColumnFamilyHandle::cf_handle(), Key::binary(), Value::binary()} |
                          {delete, Key::binary()} |
                          {delete, ColumnFamilyHandle::cf_handle(), Key::binary()} |
                          {single_delete, Key::binary()} |
                          {single_delete, ColumnFamilyHandle::cf_handle(), Key::binary()} |
                          clear].

-type compact_range_options()  :: [{exclusive_manual_compaction, boolean()} |
                                   {change_level, boolean()} |
                                   {target_level, integer()} |
                                   {allow_write_stall, boolean()} |
                                   {max_subcompactions, non_neg_integer()}].

-type flush_options() :: [{wait, boolean()} |
                          {allow_write_stall, boolean()}].

-type iterator_action() :: first | last | next | prev | binary() | {seek, binary()} | {seek_for_prev, binary()}.

-type backup_info() :: #{
  id := non_neg_integer(),
  timestamp := non_neg_integer(),
  size := non_neg_integer(),
  number_files := non_neg_integer()
}.

-type size_approximation_flag() :: none | include_memtables | include_files | include_both.
-type range() :: {Start::binary(), Limit::binary()}.


% API

open(DataRoot, Opts) ->
    RetriesLeft = proplists:get_value(open_retries, Opts, 30),
    State = init_state(DataRoot, Opts),
    open_db(Opts, State, max(1, RetriesLeft), undefined).

-spec write(state(), [write_op()]) -> ok.
write(#state{ref=EngineRef, write_opts=WriteOpts}, WriteOps) ->
    {ok, Batch} = rocksdb:batch(),
    ops_to_batch(Batch, WriteOps),
    case rocksdb:write_batch(EngineRef, Batch, WriteOpts) of
        ok -> 
                rocksdb:release_batch(Batch);
        {error, Reason} ->  
                lager:error("Error in RocksDB Message Store Write for Batch: ~p\n", [Batch]),
                rocksdb:release_batch(Batch),
                {error, Reason}
    end.

ops_to_batch(Batch, [H|T] = WriteOps) ->
    case H of 
        {put, Key, Value} -> rocksdb:batch_put(Batch, Key, Value),
                            ops_to_batch(Batch, T);
        {delete, Key} -> rocksdb:batch_delete(Batch, Key), ops_to_batch(Batch, T)
    end;
ops_to_batch(_Batch, []) -> ok.

-spec read(state(), key()) -> {ok, value()} | not_found.
read(#state{ref=EngineRef, read_opts=ReadOpts}, Key) ->
    rocksdb:get(EngineRef, Key, ReadOpts).

fold(#state{ref=EngineRef, fold_opts=FoldOpts}, Fun, Acc) ->
    {ok, Itr} = rocksdb:iterator(EngineRef, FoldOpts),
    fold_iterate(rocksdb:iterator_move(Itr, first), Itr, Fun, Acc).

fold(#state{ref=EngineRef, fold_opts=FoldOpts}, Fun, Acc, FirstKey) ->
    {ok, Itr} = rocksdb:iterator(EngineRef, FoldOpts),
    fold_iterate(rocksdb:iterator_move(Itr, FirstKey), Itr, Fun, Acc).

fold_iterate({error, invalid_iterator}, _Itr, _Fun, Acc0) ->
            Acc0;
fold_iterate({error, _Error}, _Itr, _Fun, Acc0) ->
    Acc0;
fold_iterate({ok, Key}, Itr, Fun, Acc0) ->
    try Fun(Key, Acc0) of
        Acc1 ->
            fold_iterate(rocksdb:iterator_move(Itr, next), Itr, Fun, Acc1)
    catch
        throw:_Throw ->
            rocksdb:iterator_close(Itr),
            Acc0
    end;

fold_iterate({ok, Key, Value}, Itr, Fun, Acc0) ->
   try Fun(Key, Value, Acc0) of
       Acc1 ->
           fold_iterate(rocksdb:iterator_move(Itr, next), Itr, Fun, Acc1)
   catch
       throw:_Throw ->
           rocksdb:iterator_close(Itr),
           Acc0
   end.

close(#state{ref=EngineRef}) ->
    rocksdb:close(EngineRef).

% Internal
init_state(DataRoot, Config) ->
    %% Get the data root directory
    filelib:ensure_dir(filename:join(DataRoot, "msg_store_dummy")),

    %% Merge the proplist passed in from Config with any values specified by the
    %% eleveldb app level; precedence is given to the Config.
    FinalConfig = orddict:merge(fun(_K, VLocal, _VGlobal) -> VLocal end,
                                 orddict:from_list(Config), % Local
                                 orddict:from_list(application:get_all_env(rocksdb))), % Global
    
    %% Parse out the open/read/write options
    {OpenOpts, _BadOpenOpts} = validate_options(db_options, FinalConfig),
    {ReadOpts, _BadReadOpts} = validate_options(read, FinalConfig),
    {WriteOpts, _BadWriteOpts} = validate_options(write, FinalConfig),

    %% Use read options for folding, but FORCE fill_cache to false
    FoldOpts = lists:keystore(fill_cache, 1, ReadOpts, {fill_cache, false}),
    
    %% Generate a debug message with the options we'll use for each operation
    lager:debug("Datadir ~s options for RocksDB Message Store: ~p\n",
                [DataRoot, [{db_options, OpenOpts}, {read, ReadOpts}, {write, WriteOpts}, {fold, FoldOpts}]]),
    #state { data_root = DataRoot,
             open_opts = OpenOpts,
             read_opts = ReadOpts,
             write_opts = WriteOpts,
             fold_opts = FoldOpts,
             config = FinalConfig }.

-spec option_types(db_options | read | write) -> [{atom(), bool | integer | [compression_type()] | any}].

option_types(read) ->
    [{verify_checksums, bool},
    {fill_cache, bool},
    {iterate_upper_bound, any},
    {iterate_lower_bound, any},
    {tailing, bool},
    {total_order_seek, bool},
    {prefix_same_as_start, bool},
    {snapshot, any},
    {iterator_refresh, bool}];

option_types(write) ->
     [{sync, bool},
     {disable_wal, bool},
     {ignore_missing_column_families, bool},
     {no_slowdown, bool},
     {low_pri, bool}];

option_types(db_options) ->
     [{env, any},
     {total_threads, pos_integer},
     {create_if_missing, bool},
     {create_missing_column_families, bool},
     {error_if_exists, bool},
     {paranoid_checks, bool},
     {max_open_files, integer},
     {max_total_wal_size, non_neg_integer},
     {use_fsync, bool},
     {db_paths, any},
     {db_log_dir, any},
     {wal_dir, any},
     {db_paths, any},
     {db_log_dir, any},
     {wal_dir, any},
     {delete_obsolete_files_period_micros, pos_integer},
     {max_background_jobs, pos_integer},
     {max_background_compactions, pos_integer},
     {max_background_flushes, pos_integer},
     {max_log_file_size, non_neg_integer},
     {log_file_time_to_roll, non_neg_integer},
     {keep_log_file_num, pos_integer},
     {max_manifest_file_size, pos_integer},
     {table_cache_numshardbits, pos_integer},
     {wal_ttl_seconds, non_neg_integer},
     {manual_wal_flush, bool},
     {wal_size_limit_mb, non_neg_integer},
     {manifest_preallocation_size, pos_integer},
     {allow_mmap_reads, bool},
     {allow_mmap_writes, bool},
     {is_fd_close_on_exec, bool},
     {skip_log_error_on_recovery, bool},
     {stats_dump_period_sec, non_neg_integer},
     {advise_random_on_open, bool},
     {access_hint, any},
     {compaction_readahead_size, non_neg_integer},
     {new_table_reader_for_compaction_inputs, bool},
     {use_adaptive_mutex, bool},
     {bytes_per_sync, non_neg_integer},
     {skip_stats_update_on_db_open, bool},
     {wal_recovery_mode, any},
     {allow_concurrent_memtable_write, bool},
     {enable_write_thread_adaptive_yield, bool},
     {db_write_buffer_size, non_neg_integer},
     {in_memory, bool},
     {rate_limiter, any},
     {sst_file_manager, any},
     {write_buffer_manager, any},
     {max_subcompactions, any},
     {atomic_flush, bool}].
  
-spec validate_options(db_options | read | write, [{atom(), any()}]) ->
                              {[{atom(), any()}], [{atom(), any()}]}.

validate_options(Type, Opts) ->
    Types = option_types(Type),
    Opts1 = proplists:get_value(Type, Opts),
    lists:partition(fun({K, V}) ->
                            KType = lists:keyfind(K, 1, Types),
                            validate_type(KType, V)
                    end, Opts1).

validate_type({_Key, bool}, true)                                  -> true;
validate_type({_Key, bool}, false)                                 -> true;
validate_type({_Key, integer}, Value) when is_integer(Value)       -> true;
validate_type({_Key, non_neg_integer}, Value) when is_integer(Value), Value >= 0 -> true;
validate_type({_Key, pos_integer}, Value) when is_integer(Value), Value > 0 -> true;
validate_type({_Key, any}, _Value)                                 -> true;
validate_type(_, _)                                                -> false.

open_db(_Opts, _State0, 0, LastError) ->
    {error, LastError};
open_db(Opts, State0, RetriesLeft, _) ->
    case rocksdb:open(State0#state.data_root, State0#state.open_opts) of
        {ok, Ref} ->
            {ok, State0#state { ref = Ref }};
        %% Check specifically for lock error, this can be caused if
        %% a crashed instance takes some time to flush leveldb information
        %% out to disk.  The process is gone, but the NIF resource cleanup
        %% may not have completed.
        {error, {db_open, OpenErr}=Reason} ->
            case lists:prefix("IO error: lock ", OpenErr) of
                true ->
                    SleepFor = proplists:get_value(open_retry_delay, Opts, 2000),
                    lager:debug("VerneMQ RocksDB Message Store backend retrying reconnect to RocksDB ~p in ~p ms after error ~s\n",
                                [State0#state.data_root, SleepFor, OpenErr]),
                    timer:sleep(SleepFor),
                    open_db(Opts, State0, RetriesLeft - 1, Reason);
                false ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

