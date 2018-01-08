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

-module(vmq_subscriber_db).
-include("vmq_server.hrl").

-export([store/2,
         read/1, read/2,
         fold/2,
         delete/1,
         subscribe_db_events/0]).

-define(SUBSCRIBER_DB, {vmq, subscriber}).
-define(TOMBSTONE, '$deleted').

-spec store(subscriber_id(), vmq_subscriber:subs()) -> ok.
store(SubscriberId, Subs) ->
    plumtree_metadata:put(?SUBSCRIBER_DB, SubscriberId, Subs).

-spec read(subscriber_id()) -> undefined |vmq_subscriber:subs().
read(SubscriberId) ->
    read(SubscriberId, undefined).

-spec read(subscriber_id(), any()) -> any() |vmq_subscriber:subs().
read(SubscriberId, Default) ->
    case plumtree_metadata:get(?SUBSCRIBER_DB, SubscriberId) of
        undefined -> Default;
        Subs ->
            vmq_subscriber:check_format(Subs)
    end.

-spec delete(subscriber_id()) -> ok.
delete(SubscriberId) ->
    plumtree_metadata:delete(?SUBSCRIBER_DB, SubscriberId).

fold(FoldFun, Acc) ->
    plumtree_metadata:fold(
      fun ({_, ?TOMBSTONE}, AccAcc) -> AccAcc;
          ({SubscriberId, Subs}, AccAcc) ->
              FoldFun({SubscriberId, vmq_subscriber:check_format(Subs)}, AccAcc)
      end, Acc, ?SUBSCRIBER_DB,
      [{resolver, lww}]).

subscribe_db_events() ->
    plumtree_metadata_manager:subscribe(?SUBSCRIBER_DB),
    fun
        ({deleted, ?SUBSCRIBER_DB, _, Val})
          when (Val == ?TOMBSTONE) or (Val == undefined) ->
            ignore;
        ({deleted, ?SUBSCRIBER_DB, SubscriberId, Subscriptions}) ->
            Deleted = vmq_subscriber:get_changes(Subscriptions),
            {delete, SubscriberId, Deleted};
        ({updated, ?SUBSCRIBER_DB, SubscriberId, OldVal, NewSubs})
          when (OldVal == ?TOMBSTONE) or (OldVal == undefined) ->
            Added = vmq_subscriber:get_changes(NewSubs),
            {update, SubscriberId, [], Added};
        ({updated, ?SUBSCRIBER_DB, SubscriberId, OldSubs, NewSubs}) ->
            {Removed, Added} = vmq_subscriber:get_changes(OldSubs, NewSubs),
            {update, SubscriberId, Removed, Added};
        (_) ->
            ignore
    end.


