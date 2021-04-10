-module(vmq_generic_msg_store_SUITE).
-include("src/vmq_generic_msg_store.hrl").
-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0,
         groups/0
        ]).

-export([insert_delete_test/1,
         ref_delete_test/1,
         message_compat_pre_test/1,
         idx_compat_pre_test/1]).


%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(StorageEngine, Config) ->
    [{engine, StorageEngine}|Config].

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(message_compat_pre_test, Config) ->
    Config;
init_per_testcase(idx_compat_pre_test, Config) ->
    Config;
init_per_testcase(_Case, Config) ->
    StorageEngine = proplists:get_value(engine, Config),
    case StorageEngine of
        vmq_storage_engine_rocksdb -> application:set_env(rocksdb_env());
    _ -> ok
    end,    
    application:load(vmq_generic_msg_store),
    application:set_env(vmq_generic_msg_store, msg_store_engine, StorageEngine),
    application:ensure_all_started(vmq_generic_msg_store),
    Config.

end_per_testcase(message_compat_pre_test, Config) ->
    Config;
end_per_testcase(idx_compat_pre_test, Config) ->
    Config;
end_per_testcase(_, Config) ->
    application:stop(vmq_generic_msg_store),
    Config.

all() ->
    [
     {group, vmq_storage_engine_leveldb},
     %{group, vmq_storage_engine_dets},
     {group, vmq_storage_engine_ets},
     {group, basic}
    ].

groups() ->
    StorageTests = [
                    insert_delete_test,
                    ref_delete_test,
                    message_compat_pre_test,
                    idx_compat_pre_test],
    BasicTests = [
                  message_compat_pre_test,
                  idx_compat_pre_test
                 ],
    [
     {vmq_storage_engine_leveldb, [shuffle], StorageTests},
     {vmq_storage_engine_dets, [shuffle], StorageTests},
     {vmq_storage_engine_ets, [shuffle], StorageTests},
     {vmq_storage_engine_rocksdb, [shuffle], lists:flatten([BasicTests|StorageTests])},
     {basic, [shuffle], BasicTests}
    ].


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
insert_delete_test(Config) ->
    {0,0} = store_summary(),
    Msgs = generate_msgs(1000, []),
    Refs = [Ref || #vmq_msg{msg_ref=Ref} <- Msgs],
    ok = store_msgs({"", "foo"}, Msgs),

    {1000,1000} = store_summary(),

    1000 = refcount(Refs, 0),
    %% we should get back the exact same list
    {ok, Refs} = vmq_generic_msg_store:msg_store_find({"", "foo"}, other),
    {ok, Refs} = vmq_generic_msg_store:msg_store_find({"", "foo"}, queue_init),
    %% delete all
    ok = delete_msgs({"", "foo"}, Msgs),
    {ok, []} = vmq_generic_msg_store:msg_store_find({"", "foo"}, other),
    {ok, []} = vmq_generic_msg_store:msg_store_find({"", "foo"}, queue_init),

    0 = refcount(Refs, 0),
    {0,0} = store_summary(),
    Config.

ref_delete_test(Config) ->
    {0,0} = store_summary(),
    Msgs = generate_msgs(1000, []),
    Refs = [Ref || #vmq_msg{msg_ref=Ref} <- Msgs],
    ok = store_msgs({"", "foo0"}, Msgs),
    ok = store_msgs({"", "foo1"}, Msgs),
    ok = store_msgs({"", "foo2"}, Msgs),
    ok = store_msgs({"", "foo3"}, Msgs),
    ok = store_msgs({"", "foo4"}, Msgs),
    ok = store_msgs({"", "foo5"}, Msgs),
    ok = store_msgs({"", "foo6"}, Msgs),
    ok = store_msgs({"", "foo7"}, Msgs),
    ok = store_msgs({"", "foo8"}, Msgs),
    ok = store_msgs({"", "foo9"}, Msgs),

    10000 = refcount(Refs, 0),
    {1000,10000} = store_summary(),

    {ok, Refs} = vmq_generic_msg_store:msg_store_find({"", "foo0"}, other),
    {ok, Refs} = vmq_generic_msg_store:msg_store_find({"", "foo0"}, queue_init),
    {ok, Msgs} = read_msgs({"", "foo0"}, Refs),
    ok = delete_msgs({"", "foo0"}, Msgs),
    {ok, []} = vmq_generic_msg_store:msg_store_find({"", "foo0"}, other),
    {ok, []} = vmq_generic_msg_store:msg_store_find({"", "foo0"}, queue_init),

    9000 = refcount(Refs, 0),
    {1000,9000} = store_summary(),

    {ok, Refs} = vmq_generic_msg_store:msg_store_find({"", "foo1"}, other),
    {ok, Refs} = vmq_generic_msg_store:msg_store_find({"", "foo1"}, queue_init),
    {ok, Msgs} = read_msgs({"", "foo1"}, Refs),
    ok = delete_msgs({"", "foo1"}, Msgs),
    {ok, []} = vmq_generic_msg_store:msg_store_find({"", "foo1"}, other),
    {ok, []} = vmq_generic_msg_store:msg_store_find({"", "foo1"}, queue_init),

    8000 = refcount(Refs, 0),
    {1000,8000} = store_summary(),

    {ok, Refs} = vmq_generic_msg_store:msg_store_find({"", "foo2"}, other),
    {ok, Refs} = vmq_generic_msg_store:msg_store_find({"", "foo2"}, queue_init),
    {ok, Msgs} = read_msgs({"", "foo2"}, Refs),
    ok = delete_msgs({"", "foo2"}, Msgs),
    {ok, []} = vmq_generic_msg_store:msg_store_find({"", "foo2"}, other),
    {ok, []} = vmq_generic_msg_store:msg_store_find({"", "foo2"}, queue_init),

    7000 = refcount(Refs, 0),
    {1000,7000} = store_summary(),

    {ok, Refs} = vmq_generic_msg_store:msg_store_find({"", "foo3"}, other),
    {ok, Refs} = vmq_generic_msg_store:msg_store_find({"", "foo3"}, queue_init),
    {ok, Msgs} = read_msgs({"", "foo3"}, Refs),
    ok = delete_msgs({"", "foo3"}, Msgs),
    {ok, []} = vmq_generic_msg_store:msg_store_find({"", "foo3"}, other),
    {ok, []} = vmq_generic_msg_store:msg_store_find({"", "foo3"}, queue_init),

    6000 = refcount(Refs, 0),
    {1000,6000} = store_summary(),

    {ok, Refs} = vmq_generic_msg_store:msg_store_find({"", "foo4"}, other),
    {ok, Refs} = vmq_generic_msg_store:msg_store_find({"", "foo4"}, queue_init),
    {ok, Msgs} = read_msgs({"", "foo4"}, Refs),
    ok = delete_msgs({"", "foo4"}, Msgs),
    {ok, []} = vmq_generic_msg_store:msg_store_find({"", "foo4"}, other),
    {ok, []} = vmq_generic_msg_store:msg_store_find({"", "foo4"}, queue_init),

    5000 = refcount(Refs, 0),
    {1000,5000} = store_summary(),

    {ok, Refs} = vmq_generic_msg_store:msg_store_find({"", "foo5"}, other),
    {ok, Refs} = vmq_generic_msg_store:msg_store_find({"", "foo5"}, queue_init),
    {ok, Msgs} = read_msgs({"", "foo5"}, Refs),
    ok = delete_msgs({"", "foo5"}, Msgs),
    {ok, []} = vmq_generic_msg_store:msg_store_find({"", "foo5"}, other),
    {ok, []} = vmq_generic_msg_store:msg_store_find({"", "foo5"}, queue_init),

    4000 = refcount(Refs, 0),
    {1000,4000} = store_summary(),

    {ok, Refs} = vmq_generic_msg_store:msg_store_find({"", "foo6"}, other),
    {ok, Refs} = vmq_generic_msg_store:msg_store_find({"", "foo6"}, queue_init),
    {ok, Msgs} = read_msgs({"", "foo6"}, Refs),
    ok = delete_msgs({"", "foo6"}, Msgs),
    {ok, []} = vmq_generic_msg_store:msg_store_find({"", "foo6"}, other),
    {ok, []} = vmq_generic_msg_store:msg_store_find({"", "foo6"}, queue_init),

    3000 = refcount(Refs, 0),
    {1000,3000} = store_summary(),

    {ok, Refs} = vmq_generic_msg_store:msg_store_find({"", "foo7"}, other),
    {ok, Refs} = vmq_generic_msg_store:msg_store_find({"", "foo7"}, queue_init),
    {ok, Msgs} = read_msgs({"", "foo7"}, Refs),
    ok = delete_msgs({"", "foo7"}, Msgs),
    {ok, []} = vmq_generic_msg_store:msg_store_find({"", "foo7"}, other),
    {ok, []} = vmq_generic_msg_store:msg_store_find({"", "foo7"}, queue_init),

    2000 = refcount(Refs, 0),
    {1000,2000} = store_summary(),

    {ok, Refs} = vmq_generic_msg_store:msg_store_find({"", "foo8"}, other),
    {ok, Refs} = vmq_generic_msg_store:msg_store_find({"", "foo8"}, queue_init),
    {ok, Msgs} = read_msgs({"", "foo8"}, Refs),
    ok = delete_msgs({"", "foo8"}, Msgs),
    {ok, []} = vmq_generic_msg_store:msg_store_find({"", "foo8"}, other),
    {ok, []} = vmq_generic_msg_store:msg_store_find({"", "foo8"}, queue_init),

    1000 = refcount(Refs, 0),
    {1000,1000} = store_summary(),

    {ok, Refs} = vmq_generic_msg_store:msg_store_find({"", "foo9"}, other),
    {ok, Refs} = vmq_generic_msg_store:msg_store_find({"", "foo9"}, queue_init),
    {ok, Msgs} = read_msgs({"", "foo9"}, Refs),
    ok = delete_msgs({"", "foo9"}, Msgs),
    {ok, []} = vmq_generic_msg_store:msg_store_find({"", "foo9"}, other),
    {ok, []} = vmq_generic_msg_store:msg_store_find({"", "foo9"}, queue_init),

    0 = refcount(Refs, 0),
    {0,0} = store_summary(),

    Config.

%% @doc test that the message store functions can parse and serialize
%% data from the future as well as the pre versioning format.
message_compat_pre_test(_Cfg) ->
    %% We can serialize and parse msg vals from before versioning was added
    PreVersion = {[<<"routing">>, <<"key">>], %% routing_key = [<<"routing">>, <<"key">>]
                   <<"payload">>},
    PreVersion = vmq_generic_msg_store:parse_p_msg_val_pre(
                   vmq_generic_msg_store:serialize_p_msg_val_pre(PreVersion)),

    %% We can also serialize / parse something from the future:
    FutureVersion = {1, %% version
                     [<<"routing">>, <<"key">>], <<"payload">>, <<"something else">>},

    PreVersion = vmq_generic_msg_store:parse_p_msg_val_pre(
                   vmq_generic_msg_store:serialize_p_msg_val_pre(FutureVersion)),
    ok.

idx_compat_pre_test(_Cfg) ->
    %% We can serialize and parse idx vals from before versioning was added
    PreVersion = {p_idx_val,
                  {1529,586954,257209}, %% ts = {1529,586954,257209}
                  false, %% dup = false
                  2 %% qos = 2
                 },
    PreVersion = vmq_generic_msg_store:parse_p_idx_val_pre(
                   vmq_generic_msg_store:serialize_p_idx_val_pre(PreVersion)),

    %% We can also serialize / parse something from the future:
    FutureVersion = {p_idx_val,
                     1, %% version = 1
                     {1529,586954,257209}, %% ts = {1529,586954,257209}
                     false, %% dup = false
                     2, %% qos = 2
                     "something unknown1",
                     "something unknown2"
                    },

    PreVersion = vmq_generic_msg_store:parse_p_idx_val_pre(
                   vmq_generic_msg_store:serialize_p_idx_val_pre(FutureVersion)),
    ok.

generate_msgs(0, Acc) -> Acc;
generate_msgs(N, Acc) ->
    Msg = #vmq_msg{msg_ref= msg_ref(),
                   routing_key= rand_bytes(10),
                   payload = rand_bytes(100),
                   mountpoint = "",
                   dup = random_flag(),
                   qos = random_qos(),
                   properties=#{"CorrelationData" => rand_bytes(8)},
                   persisted=true},
    generate_msgs(N - 1, [Msg|Acc]).

store_msgs(SId, [Msg|Rest]) ->
    ok = vmq_generic_msg_store:msg_store_write(SId, Msg),
    store_msgs(SId, Rest);
store_msgs(_, []) -> ok.

delete_msgs(_, []) -> ok;
delete_msgs(SId, [#vmq_msg{msg_ref=Ref}|Rest]) ->
    ok = vmq_generic_msg_store:msg_store_delete(SId, Ref),
    delete_msgs(SId, Rest).

read_msgs(SId, Refs) ->
    read_msgs(SId, Refs, []).
read_msgs(_, [], Acc) -> {ok, lists:reverse(Acc)};
read_msgs(SId, [Ref|Refs], Acc) ->
    {ok, Msg} = vmq_generic_msg_store:msg_store_read(SId, Ref),
    read_msgs(SId, Refs, [Msg|Acc]).

refcount([Ref|Refs], Cnt) ->
    refcount(Refs, Cnt + vmq_generic_msg_store:refcount(Ref));
refcount([], Cnt) -> Cnt.


random_flag() ->
    rand:uniform(10) > 5.

random_qos() ->
    rand:uniform(3) - 1.

store_summary() ->
    vmq_generic_msg_store_utils:full_table_scan(
      fun
          ({msg, _, _, _, _}, {NumMsgs, NumIdxs}) ->
              {NumMsgs + 1, NumIdxs};
          ({idx, _, _, _, _}, {NumMsgs, NumIdxs}) ->
              {NumMsgs, NumIdxs + 1}
      end, {0,0}).

rand_bytes(N) ->
    crypto:strong_rand_bytes(N).

msg_ref() ->
    erlang:md5(term_to_binary({node(), self(), erlang:timestamp(), rand_bytes(10)})).

rocksdb_env() ->
        [{rocksdb, [
            {db_options,   % https://github.com/facebook/rocksdb/blob/master/examples/rocksdb_option_file_example.ini
               [{env, default}, % oneof: default | memenv
                {total_threads, 4},
                {create_if_missing, true}, % RB default: false
                {create_missing_column_families, true}, % RB default: false
                {error_if_exists, false}, % RB default: false
                {paranoid_checks, true}, % RB default: true
                {max_open_files, -1}, % set to -1 to always keep all files open. RB default: -1
                {max_total_wal_size, 0}, % RB default: 0
                {use_fsync, false}, % RB default: false
                {db_paths, []},
                {db_log_dir, ""},
                {wal_dir, ""},
                {delete_obsolete_files_period_micros, 21600000000}, %micros RB default: 6 hours
                {max_background_jobs, 4}, % RB default: 2. (background jobs = compactions + flushes)
                {max_background_compactions, 4}, % set to number of available CPUs. RB default: -1
                {max_background_flushes, 1}, % max_background_jobs = max_background_compactions + max_background_flushes. Not supported anymore?
                {max_log_file_size, 0}, % RB default: 0
                {log_file_time_to_roll, 0}, %RB default: 0
                {keep_log_file_num, 1000}, %RB default: 1000
                {max_manifest_file_size, 1073741824}, % RB default: 1GB
                {table_cache_numshardbits, 4}, % table cache sharding. RB default: 6
                {wal_ttl_seconds, 0}, % RB default: 0
                {manual_wal_flush, false}, % RB default:
                {wal_size_limit_mb, 0}, % RB default: 0
                {manifest_preallocation_size, 4194304}, % % RB default: 4MB
                {allow_mmap_reads, false}, % RB default: false
                {allow_mmap_writes, false}, % RB default: false
                {is_fd_close_on_exec, true}, % RB default: true
                {skip_log_error_on_recovery, false}, % RB comment: this option is no longer used!
                {stats_dump_period_sec, 600}, % seconds. RB default: 600
                {advise_random_on_open, true}, % RB default: true
                {access_hint, normal}, % one of: normal | sequential | willneed | none. RB default: normal
                {compaction_readahead_size, 0}, % RB default: 0
                {new_table_reader_for_compaction_inputs, true}, % RB default: false. Plan to delete option
                {use_adaptive_mutex, false}, % RB default: false
                {bytes_per_sync, 8388608}, % taken from examlpe.ini. RB default: 0 (turned off)
                {skip_stats_update_on_db_open, true}, % faster DB open, on spinning disks. RB default: false
                {wal_recovery_mode, point_in_time_recovery}, %oneof: tolerate_corrupted_tail_records | absolute_consistency
                                                             % | point_in_time_recovery | skip_any_corrupted_records
                {allow_concurrent_memtable_write, true}, % RB default: true
                {enable_write_thread_adaptive_yield, true}, % RB default: true
                {db_write_buffer_size, 0}, % RB default: 0 (disabled). this is the max that a databases CF write buffers are allowed to total.
                % note that the CF write buffer is set via cf_options
                % For message store, Verne has 12 Databases per Node, each using the default Column Family
                % For meta data, Verne has 1 Database with 10 column families per Node.
                {in_memory, false}, % RB default: false
             %   {rate_limiter, rate_limiter_handle()}, %?
             %   {sst_file_manager, sst_file_manager()},
             %   {write_buffer_manager, write_buffer_manager()},
                {max_subcompactions, 4}, % RB default: 1
                {atomic_flush, false}
               ]},
              {read,
                     [{verify_checksums, true}, %RB default: true
                      {fill_cache, true}, % RB default: true
                     % {iterate_upper_bound, any}, % RB default: nullptr
                     % {iterate_lower_bound, any}, % RB default: nullptr
                      {tailing, false}, % RB default: false
                      {total_order_seek, false}, %RB default: ?
                      {prefix_same_as_start, false}, % RB default: false
                     % {snapshot, any},
                      {iterator_refresh, false}]}, % non RB config?
  
               {write,
                     [{sync, false}, % RB default: false (?)
                      {disable_wal, false}, % RB default:
                      {ignore_missing_column_families, false}, % RB default: ?
                      {no_slowdown, false}, % https://github.com/facebook/rocksdb/wiki/Write-Stalls
                      {low_pri, false}]} % https://github.com/facebook/rocksdb/wiki/Low-Priority-Write
             ]
   }].