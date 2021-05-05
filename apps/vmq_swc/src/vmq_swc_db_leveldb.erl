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

-module(vmq_swc_db_leveldb).
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

-record(state, {ref :: undefined | eleveldb:db_ref(),
                data_root :: string(),
                open_opts = [],
                config,
                read_opts = [],
                write_opts = [],
                fold_opts = [{fill_cache, false}],
                refs = ets:new(?MODULE, [])
               }).

% vmq_swc_db impl
childspecs(#swc_config{group=SwcGroup} = Config, Opts) ->
    [#{id => {?MODULE, SwcGroup},
       start => {?MODULE, start_link, [Config, Opts]}}].

-spec write(config(), list(db_op()), opts()) -> ok.
write(#swc_config{db=DBName}, Objects, _Opts) ->
    gen_server:call(DBName, {write, Objects}, infinity).

-spec read(config(), type(), db_key(), opts()) -> {ok, db_value()} | not_found.
read(#swc_config{db=DBName}, Type, Key, _Opts) ->
    gen_server:call(DBName, {read, Type, Key}, infinity).

-spec fold(config(), type(), foldfun(), any(), first | db_key()) -> any().
fold(#swc_config{db=DBName}, Type, FoldFun, Acc, FirstKey) ->
    gen_server:call(DBName, {fold, Type, FoldFun, Acc, FirstKey}, infinity).


%% gen_server impl
start_link(#swc_config{db=DBName} = Config, Opts) ->
    gen_server:start_link({local, DBName}, ?MODULE, [Config | Opts], []).

init([#swc_config{peer=SWC_ID, group=SwcGroup} = _Config|Opts]) ->
    %% Initialize random seed
    {Peer, _Actor} = SWC_ID,
    rand:seed(exsplus, os:timestamp()),
    DefaultDataDir = filename:join(<<".">>, Peer),

    DataDir = proplists:get_value(data_dir, Opts,
                                  application:get_env(vmq_swc, data_dir, binary_to_list(DefaultDataDir))),

    %% Initialize state
    S0 = init_state(filename:join(DataDir, SwcGroup), Opts),
    process_flag(trap_exit, true),
    case open_db(Opts, S0) of
        {ok, State} ->
            {ok, State};
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call({write, Objects}, _From, State) ->
    DBOps =
    lists:map(fun({Type, Key, ?DELETED}) ->
                      {delete, key(Type, Key)};
                 ({Type, Key, Value}) ->
                      {put, key(Type, Key), Value}
              end, Objects),
    eleveldb:write(State#state.ref, DBOps, State#state.write_opts),
    {reply, ok, State};
handle_call({read, Type, Key}, _From, State) ->
    {reply, eleveldb:get(State#state.ref, key(Type, Key), State#state.read_opts), State};

handle_call({fold, Type, FoldFun, Acc, FirstKey0}, From, State) ->
    spawn_link(
      fun() ->
              {ok, Itr} = eleveldb:iterator(State#state.ref, State#state.fold_opts),
              FirstKey1 =
                  case FirstKey0 of
                      first ->
                          key(Type, <<>>);
                      _ ->
                          key(Type, FirstKey0)
                  end,
              KeyPrefix = key_prefix(Type),
              KeyPrefixSize = byte_size(KeyPrefix),
              Result = iterate(FoldFun, Acc, Itr, key_prefix(Type), KeyPrefixSize, eleveldb:iterator_move(Itr, FirstKey1)),
              gen_server:reply(From, Result)
      end),
    {noreply, State}.

handle_cast(_Req, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{ref=Ref} =_State) ->
    eleveldb:close(Ref),
    ok.

code_change(_OldVsn, _NewVsn, State) ->
    State.

key(Type, Key) ->
    <<(key_prefix(Type))/binary, Key/binary>>.

key_prefix(?DB_OBJ) -> <<"obj#">>;
key_prefix(?DB_DKM) -> <<"dkm#">>;
key_prefix(?DB_DEFAULT) -> <<"default#">>.

iterate(FoldFun, Acc0, Itr, KeyPrefix, PrefixSize, {ok, PrefixedKey, Value}) ->
    case PrefixedKey of
        <<KeyPrefix:PrefixSize/binary, Key/binary>> ->
            case FoldFun(Key, Value, Acc0) of
                stop ->
                    eleveldb:iterator_close(Itr),
                    Acc0;
                Acc1 ->
                    iterate(FoldFun, Acc1, Itr, KeyPrefix, PrefixSize, eleveldb:iterator_move(Itr, prefetch))
            end;
        _ ->
            eleveldb:iterator_close(Itr),
            Acc0
    end;
iterate(_, Acc, _Itr, _, _, {error, _}) ->
    %% no need to close the iterator
    Acc.

init_state(DataRoot, Config) ->
    %% Get the data root directory
    filelib:ensure_dir(filename:join(DataRoot, "swc_store_dummy")),

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

open_db(Opts, State) ->
    RetriesLeft = proplists:get_value(open_retries, Opts, 30),
    open_db(Opts, State, max(1, RetriesLeft), undefined).

open_db(_Opts, _State0, 0, LastError) ->
    {error, LastError};
open_db(Opts, State0, RetriesLeft, _) ->
    DataRoot = State0#state.data_root,
    case eleveldb:open(DataRoot, State0#state.open_opts) of
        {ok, Ref} ->
            lager:info("Opening LevelDB SWC database at ~p~n", [DataRoot]),
            {ok, State0#state { ref = Ref }};
        %% Check specifically for lock error, this can be caused if
        %% a crashed instance takes some time to flush leveldb information
        %% out to disk.  The process is gone, but the NIF resource cleanup
        %% may not have completed.
        {error, {db_open, OpenErr}=Reason} ->
            case lists:prefix("Corruption: truncated record ", OpenErr) of
                true ->
                    lager:info("VerneMQ LevelDB SWC Store backend repair attempt for store ~p, after error ~s. LevelDB will put unusable .log and MANIFEST filest in 'lost' folder.\n",
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
                            lager:info("VerneMQ LevelDB SWC Store backend retrying ~p in ~p ms after error ~s\n",
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


