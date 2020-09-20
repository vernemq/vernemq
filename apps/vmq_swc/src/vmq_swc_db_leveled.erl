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

-module(vmq_swc_db_leveled).
-include("vmq_swc.hrl").
-behaviour(vmq_swc_db).
-behaviour(gen_server).

-ifndef(STD_TAG).
-define(STD_TAG, o). % from leveled.hrl
-endif.

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

-record(state, {db}).

% vmq_swc_db impl
childspecs(#swc_config{group=SwcGroup} = Config, Opts) ->
    [#{id => {?MODULE, SwcGroup},
       start => {?MODULE, start_link, [Config, Opts]}}].

-spec write(config(), list(db_op()), opts()) -> ok.
write(#swc_config{db=DBName}, Objects, _Opts) ->
    [{_, Bookie}] = ets:lookup(DBName, refs),
    lists:foreach(fun({Type, Key, ?DELETED}) ->
                          CF = db_cf(Type),
                          _ = leveled_bookie:book_delete(Bookie, CF, Key, []);
                     ({Type, Key, Value}) ->
                          CF = db_cf(Type),
                          _ = leveled_bookie:book_put(Bookie, CF, Key, Value, [])
                  end, Objects).

-spec read(config(), type(), db_key(), opts()) -> {ok, db_value()} | not_found.
read(#swc_config{db=DBName}, Type, Key, _Opts) ->
    [{_, Bookie}] = ets:lookup(DBName, refs),
    CF = db_cf(Type),
    leveled_bookie:book_get(Bookie, CF, Key).

-spec fold(config(), type(), foldfun(), any(), first | db_key()) -> any().
fold(#swc_config{db=DBName}, Type, FoldFun, Acc, FirstKey) ->
    [{_, Bookie}] = ets:lookup(DBName, refs),
    CF = db_cf(Type),
    FoldFunWrapper = foldfun_wrapper(FoldFun),
    {async, Runner} =
    case FirstKey of
        first ->
            leveled_bookie:book_objectfold(Bookie, ?STD_TAG, CF,
                                           all, {FoldFunWrapper, Acc}, false);
        _ ->
            leveled_bookie:book_objectfold(Bookie, ?STD_TAG, CF,
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
            case FoldFun(Key, Value, AccAcc) of
                stop ->
                    throw({return, AccAcc});
                AccAccAcc ->
                    AccAccAcc
            end
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

    Options =  [{root_path, DbPath},
                {cache_size, 2500},
                {max_journalsize, 100000000},
                {sync_strategy, none},
                {head_only, false}, %% has to be false, otherwise only head request api
                {waste_retention_period, undefined},
                {max_run_length, undefined},
                {singlefile_compactionpercentage, 50.0},
                {maxrunlength_compactionpercentage, 70.0},
                {reload_strategy, []},
                {max_pencillercachesize, 28000},
                {compression_method, lz4},
                {compression_point, on_receipt}],


    {ok, Bookie} = leveled_bookie:book_start(Options),

    process_flag(trap_exit, true),

    ets:new(DBName, [named_table, public, {read_concurrency, true}]),
    ets:insert(DBName, {refs, Bookie}),
    {ok, #state{db=Bookie}}.

handle_call(_Req, _From, State) ->
    {reply, {error, not_implemented}, State}.

handle_cast(_Req, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{db=Bookie} =_State) ->
    catch leveled_bookie:book_close(Bookie),
    ok.

code_change(_OldVsn, _NewVsn, State) ->
    State.

db_cf(?DB_DEFAULT) -> <<"default">>;
db_cf(?DB_OBJ) -> <<"obj">>;
db_cf(?DB_DKM) -> <<"dkm">>.
