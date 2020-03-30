%% Copyright 2020 Octavo Labs AG Zurich Switzerland (https://octavolabs.com)
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

-module(vmq_storage_engine_leveled).

-export([open/2, close/1, write/2, read/2, fold/3, fold/4]).

-record(state, {
          ref :: undefined | pid(),
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


-ifndef(STD_TAG).
-define(STD_TAG, o). % from leveled.hrl
-endif.

-define(BUCKET, <<"default">>).

% API

open(DataRoot, Opts) ->
    {module, leveled_bookie} = code:ensure_loaded('leveled_bookie'),
    RetriesLeft = proplists:get_value(open_retries, Opts, 30),
    State = init_state(DataRoot, Opts),
    open_db(Opts, State, max(1, RetriesLeft), undefined).

-spec write(state(), [write_op()]) -> ok.
    write(#state{ref=EngineRef}, WriteOps) ->
            ops_to_bookie(EngineRef, WriteOps).
    
ops_to_bookie(Bookie, [H|T] = _WriteOps) ->
    % Bookie is the same as EngineRef, it's a pid
        case H of 
            {put, Key, Value} -> leveled_bookie:book_put(Bookie, ?BUCKET, Key, Value, []),
                                ops_to_bookie(Bookie, T);
            {delete, Key} -> leveled_bookie:book_delete(Bookie, ?BUCKET, Key, []), ops_to_bookie(Bookie, T)
        end;
    ops_to_bookie(_Bookie, []) -> ok.

-spec read(state(), key()) -> {ok, value()} | not_found.
read(#state{ref=EngineRef}, Key) ->
    leveled_bookie:book_get(EngineRef, ?BUCKET, Key).

% book_put(Pid, Bucket, Key, Object, IndexSpecs)
close(#state{ref=EngineRef}) ->
        catch leveled_bookie:book_close(EngineRef),
        ok.

% Internal
init_state(DataRoot, Config) ->
    %% Get the data root directory
    filelib:ensure_dir(filename:join(DataRoot, "msg_store_dummy")),

    %% Merge the proplist passed in from Config with any values specified by the
    %% eleveldb app level; precedence is given to the Config.
    FinalConfig = orddict:merge(fun(_K, VLocal, _VGlobal) -> VLocal end,
                                 orddict:from_list(Config), % Local
                                 orddict:from_list(application:get_all_env(leveled))), % Global
    ReadOpts = [],
    WriteOpts = [],
    FoldOpts = [],

    StartOptions =  
    [{root_path, DataRoot},
    {cache_size, 2500},
    {max_journalsize, 100000000},
    {sync_strategy, none},
    {head_only, false},
    {waste_retention_period, undefined},
    {max_run_length, undefined},
    {singlefile_compactionpercentage, 50.0},
    {maxrunlength_compactionpercentage, 70.0},
    {reload_strategy, []},
    {max_pencillercachesize, 28000},
    {compression_method, lz4},
    {compression_point, on_receipt}],

    %% Generate a debug message with the options we'll use for each operation
    lager:debug("Datadir ~s options for Leveled Message Store: ~p\n",
                [DataRoot, [{open, StartOptions}, {read, ReadOpts}, {write, WriteOpts}, {fold, FoldOpts}]]),
    #state { data_root = DataRoot,
             open_opts = StartOptions,
             read_opts = ReadOpts,
             write_opts = WriteOpts,
             fold_opts = FoldOpts,
             config = FinalConfig }.

open_db(_Opts, _State0, 0, LastError) ->
            {error, LastError};
        open_db(Opts, State0, RetriesLeft, _) ->
        case leveled_bookie:book_start(State0#state.open_opts) of
                {ok, EngineRef} ->
                    {ok, State0#state { ref = EngineRef }};
                %% Check specifically for lock error, this can be caused if
                %% a crashed instance takes some time to flush leveldb information
                %% out to disk.  The process is gone, but the NIF resource cleanup
                %% may not have completed.
                {error, {db_open, OpenErr}=Reason} ->
                    io:format("error case db_open"),
                    case lists:prefix("IO error: lock ", OpenErr) of
                        true ->
                            SleepFor = proplists:get_value(open_retry_delay, Opts, 2000),
                            lager:debug("VerneMQ Leveled Message Store backend trying to reopen ~p in ~p ms after error ~s\n",
                                        [State0#state.data_root, SleepFor, OpenErr]),
                            timer:sleep(SleepFor),
                            open_db(Opts, State0, RetriesLeft - 1, Reason);
                        false ->
                            {error, Reason}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end.

%-spec fold(State, foldfun(), list()) -> any().
%-spec fold(State, foldfun(), list(), FirstKey) -> any().
fold(State, FoldFun, Acc) ->
    fold(State, FoldFun, Acc, first).

fold(#state{ref=EngineRef}, FoldFun, Acc, FirstKey) ->
    FoldFunWrapper = foldfun_wrapper(FoldFun),
    {async, Runner} =
    case FirstKey of
        first ->
            leveled_bookie:book_objectfold(EngineRef, ?STD_TAG, ?BUCKET,
                                           all, {FoldFunWrapper, Acc}, false);
        _ ->
            leveled_bookie:book_objectfold(EngineRef, ?STD_TAG, ?BUCKET,
                                           {FirstKey, <<"$all">>}, {FoldFunWrapper, Acc}, false)
    end,
    try
        Runner()
    catch
        throw:{return, ReturnAcc} ->
            ReturnAcc
    end.

foldfun_wrapper(FoldFun) ->
            fun(_Bucket, Key, Value, AccAcc) ->
                try FoldFun(Key, Value, AccAcc) 
                catch   
                        throw:finished ->
                            throw({return, AccAcc});
                        AccAccAcc ->
                            AccAccAcc
                    end
                end.
        
% Options, copied from Leveled

%-type open_options() :: 
% for an explanation of the options, cf Leveled
%    [{root_path, string()|undefined} |
%     {snapshot_bookie, undefined|pid()} |

%        {cache_size, pos_integer()} |
%        {max_journalsize, pos_integer()} |
%        {max_journalobjectcount, pos_integer()} |
%        {max_sstslots, pos_integer()} |
%        {sync_strategy, sync_mode()} |
%        {head_only, false|with_lookup|no_lookup} |
%        {waste_retention_period, undefined|pos_integer()} |
%        {max_run_length, undefined|pos_integer()} |
%        {singlefile_compactionpercentage, float()} |
%        {maxrunlength_compactionpercentage, float()} |
%        {reload_strategy, list()} |
%        {max_pencillercachesize, pos_integer()|undefined} |
%        {ledger_preloadpagecache_level, pos_integer()} |
%        {compression_method, native|lz4} |
%        {compression_point, on_compact|on_receipt} |
%        {log_level, debug|info|warn|error|critical} |
%        {forced_logs, list(string())} |
%        {database_id, non_neg_integer()} |
%        {override_functions, list(leveled_head:appdefinable_function_tuple())} |
%        {snapshot_timeout_short, pos_integer()} |
%        {snapshot_timeout_long, pos_integer()}
%        ].;