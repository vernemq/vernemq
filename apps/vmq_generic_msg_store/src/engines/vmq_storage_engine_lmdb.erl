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

-module(vmq_storage_engine_lmdb).

-export([open/2, close/1, write/2, read/2, fold/3, fold/4]).

-record(state, {ref, env}).

open(DataRoot, Opts) ->
    filelib:ensure_dir(filename:join(DataRoot, "msg_store_dummy")),
    {ok, Env} = elmdb:env_open(DataRoot, Opts),
    {ok, Dbi} = elmdb:db_open(Env, [create]),
    {ok, #state{ref=Dbi, env=Env}}.

close(#state{env=Env}) ->
    ok = elmdb:env_close(Env).

write(#state{ref=Ref}, WriteOps) ->
    lists:foreach(fun({put, Key, Val}) ->
                          elmdb:put(Ref, Key, Val);
                     ({delete, Key}) ->
                          elmdb:delete(Ref, Key)
                  end, WriteOps).

read(#state{ref=Ref}, Key) ->
    case elmdb:get(Ref, Key) of
        {ok, Val} -> {ok, Val};
        _ -> not_found
    end.

fold(#state{ref=Ref,env=Env}, Fun, Acc) ->
    {ok, Txn} = elmdb:ro_txn_begin(Env),
    {ok, Cursor} = elmdb:ro_txn_cursor_open(Txn, Ref),
    case elmdb:ro_txn_cursor_get(Cursor, first) of
          {ok, FirstKey, _Val} -> fold_iterate(Cursor, FirstKey, Ref, Fun, Acc);
          not_found -> not_found
    end.

fold(#state{ref=Ref, env=Env}, Fun, Acc, FirstKey) ->
    {ok, Txn} = elmdb:ro_txn_begin(Env),
    {ok, Cursor} = elmdb:ro_txn_cursor_open(Txn, Ref),
   
    case elmdb:ro_txn_cursor_get(Cursor, {set_range, FirstKey}) of 
        not_found -> not_found;
        {ok, FirstKey1, _Val} ->
           % {ok, NextKey, _} = elmdb:ro_txn_cursor_get(Cursor, next),
            fold_iterate(Cursor, FirstKey1, Ref, Fun, Acc)
    end.    

fold_iterate(Cursor, 'not_found', _Ref, _Fun, Acc) ->
    ok = elmdb:ro_txn_cursor_close(Cursor),
    Acc;
fold_iterate(Cursor, '$end_of_table', _Ref, _Fun, Acc) ->
    ok = elmdb:ro_txn_cursor_close(Cursor),
    Acc;
fold_iterate(Cursor, Key, Ref, Fun, Acc0) ->
   {ok, Value} = elmdb:get(Ref, Key),
   try Fun(Key, Value, Acc0) of
       Acc1 ->
           case elmdb:ro_txn_cursor_get(Cursor, next) of
                not_found -> Acc1;
                {ok, NextKey, _} ->
                                    fold_iterate(Cursor, NextKey, Ref, Fun, Acc1)
            end
   catch
       throw:_Throw ->
           ok = elmdb:ro_txn_cursor_close(Cursor),
           Acc0
   end.