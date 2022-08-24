%% Copyright 2018 Erlio GmbH Basel Switzerland (http://erl.io)
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

-module(vmq_generic_msg_store_sup).

-behaviour(supervisor).

%% API
-export([
    start_link/0,
    get_bucket_pid/1,
    get_bucket_pids/0,
    register_bucket_pid/2
]).

%% Supervisor callbacks
-export([init/1]).

-include("vmq_generic_msg_store.hrl").
-define(TABLE, vmq_generic_msg_store_buckets).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    {ok, Pid} = supervisor:start_link({local, ?MODULE}, ?MODULE, []),
    ok = init_msg_init_tables(),
    Pids =
        [
            begin
                {ok, ChildPid} = supervisor:start_child(Pid, child_spec(I)),
                ChildPid
            end
         || I <- lists:seq(1, ?NR_OF_BUCKETS)
        ],

    ok = wait_until_initialized(Pids),
    {ok, Pid}.

init_msg_init_tables() ->
    lists:foreach(
        fun(I) ->
            %% register them by name to make it easier to inspect
            %% them.
            Name =
                list_to_atom("vmq_generic_msg_store_init_msg_idx_" ++ integer_to_list(I)),
            Ref = ets:new(Name, [
                public,
                named_table,
                ordered_set,
                {read_concurrency, true}
            ]),
            %% use persistent terms to fetch the references when
            %% mapping from the subscriberid.
            persistent_term:put({?TBL_MSG_INIT, I}, Ref)
        end,
        lists:seq(1, ?NR_OF_BUCKETS)
    ),
    ok.

wait_until_initialized(Pids) ->
    lists:foreach(
        fun(Pid) ->
            initialized = vmq_generic_msg_store:get_state(Pid)
        end,
        Pids
    ).

get_bucket_pid(Key) when is_binary(Key) ->
    Id = (erlang:phash2(Key) rem ?NR_OF_BUCKETS) + 1,
    case ets:lookup(?TABLE, Id) of
        [] ->
            {error, no_bucket_found};
        [{Id, Pid}] ->
            {ok, Pid}
    end.

get_bucket_pids() ->
    [Pid || [{_, Pid}] <- ets:match(?TABLE, '$1')].

register_bucket_pid(BucketId, BucketPid) ->
    %% Called from vmq_generic_msg_store:init
    ets:insert(?TABLE, {BucketId, BucketPid}),
    ok.

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    _ = ets:new(?TABLE, [public, named_table, {read_concurrency, true}]),
    {ok, {{one_for_one, 5, 10}, []}}.

child_spec(I) ->
    {
        {vmq_generic_msg_store_bucket, I},
        {vmq_generic_msg_store, start_link, [I]},
        permanent,
        5000,
        worker,
        [vmq_generic_msg_store]
    }.
