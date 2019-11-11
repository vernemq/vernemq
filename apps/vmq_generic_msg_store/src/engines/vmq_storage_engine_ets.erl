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

-module(vmq_storage_engine_ets).

-export([open/2, close/1, write/2, read/2, fold/3, fold/4]).

-record(state, {ref}).

open(_DataRoot, _Opts) ->
    Tid = ets:new(?MODULE, [public, ordered_set]),
    {ok, #state{ref=Tid}}.

close(#state{ref=Ref}) ->
    ets:delete(Ref).

write(#state{ref=Ref}, WriteOps) ->
    lists:foreach(fun({put, Key, Val}) ->
                          ets:insert(Ref, {Key, Val});
                     ({delete, Key}) ->
                          ets:delete(Ref, Key)
                  end, WriteOps).

read(#state{ref=Ref}, Key) ->
    case ets:lookup(Ref, Key) of
        [{Key, Val}] -> {ok, Val};
        [] -> not_found
    end.

fold(#state{ref=Ref}, Fun, Acc) ->
    fold_iterate(ets:first(Ref), Ref, Fun, Acc).

fold(#state{ref=Ref}, Fun, Acc, FirstKey) ->
    fold_iterate(ets:next(Ref, FirstKey), Ref, Fun, Acc).

fold_iterate('$end_of_table', _Ref, _Fun, Acc) ->
    Acc;
fold_iterate(Key, Tab, Fun, Acc0) ->
   [{Key, Value}] = ets:lookup(Tab, Key),
   try Fun(Key, Value, Acc0) of
       Acc1 ->
           fold_iterate(ets:next(Tab, Key), Tab, Fun, Acc1)
   catch
       throw:_Throw ->
           Acc0
   end.
