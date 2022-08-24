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

-module(vmq_swc_store_batcher).
-include("vmq_swc.hrl").

-export([
    start_link/1,
    init/1,
    add_to_batch/2
]).

-define(BATCH_SIZE, 1000).

start_link(Config) ->
    proc_lib:start_link(?MODULE, init, [[self(), Config]]).

add_to_batch(#swc_config{batcher = BatcherName}, Op) ->
    BatcherName ! Op,
    ok.

init([Parent, #swc_config{batcher = BatcherName} = Config]) ->
    register(BatcherName, self()),
    proc_lib:init_ack({ok, self()}),
    loop_blocking(Parent, Config, 0, []).

loop_blocking(Parent, Config, Size, Queue) ->
    receive
        {system, From, Request} ->
            sys:handle_system_msg(Request, From, Parent, ?MODULE, [], {Size, Queue});
        Msg ->
            loop_accumulating(Parent, Config, Size + 1, [Msg | Queue])
    end.

loop_accumulating(Parent, #swc_config{store = StoreName} = Config, Size, Queue) when
    Size < ?BATCH_SIZE
->
    receive
        Msg ->
            loop_accumulating(Parent, Config, Size + 1, [Msg | Queue])
    after 0 ->
        vmq_swc_store:process_batch(StoreName, lists:reverse(Queue)),
        loop_blocking(Parent, Config, 0, [])
    end;
loop_accumulating(Parent, #swc_config{store = StoreName} = Config, Size, Queue0) ->
    {Batch, Queue1} = lists:split(?BATCH_SIZE, Queue0),
    vmq_swc_store:process_batch(StoreName, lists:reverse(Batch)),
    loop_accumulating(Parent, Config, Size - ?BATCH_SIZE, Queue1).
