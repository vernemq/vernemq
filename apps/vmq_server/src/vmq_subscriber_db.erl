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

-import(vmq_subscriber, [check_format/1]).

-define(SUBSCRIBER_DB, {vmq, subscriber}).
-define(TOMBSTONE, '$deleted').

-spec store(subscriber_id(), vmq_subscriber:subs()) -> ok.
store(SubscriberId, Subs) ->
    vmq_metadata:put(?SUBSCRIBER_DB, SubscriberId, Subs).

-spec read(subscriber_id()) -> undefined |vmq_subscriber:subs().
read(SubscriberId) ->
    read(SubscriberId, undefined).

-spec read(subscriber_id(), any()) -> any() |vmq_subscriber:subs().
read(SubscriberId, Default) ->
    case vmq_metadata:get(?SUBSCRIBER_DB, SubscriberId) of
        undefined -> Default;
        Subs ->
            check_format(Subs)
    end.

-spec delete(subscriber_id()) -> ok.
delete(SubscriberId) ->
    vmq_metadata:delete(?SUBSCRIBER_DB, SubscriberId).

fold(FoldFun, Acc) ->
    vmq_metadata:fold(?SUBSCRIBER_DB,
      fun ({_, ?TOMBSTONE}, AccAcc) -> AccAcc;
          ({SubscriberId, Subs}, AccAcc) ->
              FoldFun({SubscriberId, check_format(Subs)}, AccAcc)
      end, Acc).

subscribe_db_events() ->
    vmq_metadata:subscribe(?SUBSCRIBER_DB),
    fun
        ({deleted, ?SUBSCRIBER_DB, _, Val})
          when (Val == ?TOMBSTONE) or (Val == undefined) ->
            ignore;
        ({deleted, ?SUBSCRIBER_DB, SubscriberId, Subscriptions}) ->
            {delete, SubscriberId, check_format(Subscriptions)};
        ({updated, ?SUBSCRIBER_DB, SubscriberId, OldVal, NewSubs})
          when (OldVal == ?TOMBSTONE) or (OldVal == undefined) ->
            {update, SubscriberId, [], check_format(NewSubs)};
        ({updated, ?SUBSCRIBER_DB, SubscriberId, OldSubs, NewSubs}) ->
            {update, SubscriberId, check_format(OldSubs), check_format(NewSubs)};
        (_) ->
            ignore
    end.


