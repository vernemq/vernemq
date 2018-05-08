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

-module(vmq_plumtree).
-export([metadata_put/3,
         metadata_get/2,
         metadata_delete/2,
         metadata_fold/3,
         metadata_subscribe/1]).

-define(TOMBSTONE, '$deleted').

metadata_put(FullPrefix, Key, Value) ->
    plumtree_metadata:put(FullPrefix, Key, Value).

metadata_get(FullPrefix, Key) ->
    plumtree_metadata:get(FullPrefix, Key).

metadata_delete(FullPrefix, Key) ->
    plumtree_metadata:delete(FullPrefix, Key).

metadata_fold(FullPrefix, Fun, Acc) ->
    plumtree_metadata:fold(Fun, Acc, FullPrefix, [{resolver, lww}]).

metadata_subscribe(FullPrefix) ->
    plumtree_metadata_manager:subscribe(FullPrefix).

