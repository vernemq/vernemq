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

-module(vmq_swc_db).
-include("vmq_swc.hrl").

-export([childspecs/3,
         backend/0,
         put/4,
         put/5,
         put/6,
         put_many/2,
         put_many/3,
         put_many/4,
         get/3,
         get/4,
         get/5,
         delete/3,
         delete/4,
         iterator/2,
         iterator/3,
         iterator/4,
         iterator/5,
         iterator_next/1,
         iterator_close/1
        ]).

-type iterator_state() :: any().

-callback childspecs(config(), any()) -> list().
-callback write(config(), list(kv()), opts()) -> ok.
-callback read(config(), type(), key(), opts()) -> {ok, value()} | not_found.
-callback iterator(config(), type(), key() | first, opts()) -> iterator_state().
-callback iterator_next(any()) -> {{key(), value()}, iterator_state()} | '$end_of_table' | {error, invalid_iterator}.
-callback iterator_close(iterator_state()) -> ok.


-spec childspecs(backend(), config(), opts()) -> list().
childspecs({backend, Backend}, #swc_config{} = Config, Opts) ->
    Backend:childspecs(Config, Opts).

-spec backend() -> backend().
backend() ->
    ?DEFAULT_BACKEND.

-spec put(config(), type(), key(), value() | ?DELETED) -> ok.
put(Config, Type, Key, Value) ->
    put(Config, ?DEFAULT_BACKEND, Type, Key, Value).

-spec put(config(), backend(), type(), key(), value() | ?DELETED) -> ok.
put(Config, Backend, Type, Key, Value) ->
    put(Config, Backend, Type, Key, Value, []).

-spec put(config(), backend(), type(), key(), value() | ?DELETED, opts()) -> ok.
put(Config, Backend, Type, Key, Value, Opts) ->
    put_many(Config, Backend, [{Type, Key, Value}], Opts).

-spec put_many(config(), list(kv())) -> ok.
put_many(Config, PutOps) ->
    put_many(Config, ?DEFAULT_BACKEND, PutOps).

-spec put_many(config(), backend(), list(kv())) -> ok.
put_many(Config, Backend, PutOps) ->
    put_many(Config, Backend, PutOps, []).

-spec put_many(config(), backend(), list(kv()), opts()) -> ok.
put_many(#swc_config{} = Config, {backend, Backend}, PutOps, Opts) ->
    Backend:write(Config, PutOps, Opts).

-spec get(config(), type(), key()) -> {ok, value()} | not_found.
get(Config, Type, Key) ->
    get(Config, ?DEFAULT_BACKEND, Type, Key).

-spec get(config(), backend(), type(), key()) -> {ok, value()} | not_found.
get(Config, Backend, Type, Key) ->
    get(Config, Backend, Type, Key, []).

-spec get(config(), backend(), type(), key(), opts()) -> {ok, value()} | not_found.
get(#swc_config{} = Config, {backend, Backend}, Type, Key, Opts) ->
    Backend:read(Config, Type, Key, Opts).

-spec delete(config(), type(), key()) -> ok.
delete(Config, Type, Key) ->
    delete(Config, ?DEFAULT_BACKEND, Type, Key).

-spec delete(config(), backend(), type(), key()) -> ok.
delete(Config, Backend, Type, Key) ->
    put(Config, Backend, Type, Key, ?DELETED).


-spec iterator(config(), type()) -> vmq_swc_db_iterator:iterator().
iterator(Config, Type) ->
    iterator(Config, ?DEFAULT_BACKEND, Type, first, []).

-spec iterator(config(), backend(), type()) -> vmq_swc_db_iterator:iterator().
iterator(Config, Backend, Type) ->
    iterator(Config, Backend, Type, first, []).

-spec iterator(config(), backend(), type(), key() | first) -> vmq_swc_db_iterator:iterator().
iterator(Config, Backend, Type, StartKey) ->
    iterator(Config, Backend, Type ,StartKey, []).

-spec iterator(config(), backend(), type(), key() | first, opts()) -> vmq_swc_db_iterator:iterator().
iterator(Config, {backend, Backend}, Type, StartKey, Opts) ->
    vmq_swc_db_iterator:iterator(Config, Backend, Type, StartKey, Opts).

-spec iterator_next(vmq_swc_db_iterator:iterator()) -> {{key(), value()}, vmq_swc_db_iterator:iterator()} | '$end_of_table'.
iterator_next(Iterator) ->
    vmq_swc_db_iterator:iterator_next(Iterator).

-spec iterator_close(vmq_swc_db_iterator:iterator()) -> ok.
iterator_close(Iterator) ->
    vmq_swc_db_iterator:iterator_close(Iterator).

