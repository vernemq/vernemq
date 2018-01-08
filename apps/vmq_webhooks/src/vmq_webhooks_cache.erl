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

%% @doc A cache for `vmq_webhooks` used to cache the hooks
%% `auth_on_register`, `auth_on_publish` and `auth_on_subscribe` as
%% authentication data is often static.
-module(vmq_webhooks_cache).

-export([
         new/0
        ,reset_stats/0
        ,lookup/3
        ,insert/5
        ,stats/0
        ,purge_all/0
        ]).

-define(CACHE, vmq_webhooks_cache).
-define(STATS, vmq_webhooks_cache_stats).

%% API
new() ->
    ets:new(?CACHE, [public, bag, named_table, {read_concurrency, true}]),
    ets:new(?STATS, [public, ordered_set, named_table, {write_concurrency, true}]),
    ok.

reset_stats() ->
    ets:delete_all_objects(?STATS).

purge_all() ->
    ets:delete_all_objects(?CACHE),
    reset_stats().    

lookup(Endpoint, Hook, Args) ->
    case lookup_(Endpoint, Hook, Args) of
        not_found ->
            miss(Endpoint, Hook),
            not_found;
        Val ->
            hit(Endpoint, Hook),
            Val
    end.

insert(Endpoint, Hook, Args, ExpiryInSecs, Modifiers) ->
    SubscriberId =
        {proplists:get_value(mountpoint, Args),
         proplists:get_value(client_id, Args)},
    ExpirationTs = ts_from_now(ExpiryInSecs),
    %% Remove the payload from cache, as it doesn't make sense to
    %% cache that.
    Key = {Endpoint, Hook, lists:keydelete(payload, 1, Args)},
    %% do not store the payload modifier
    Row = {Key, SubscriberId, ExpirationTs, lists:keydelete(payload, 1, Modifiers)},
    true = ets:insert(?CACHE, Row),
    incr_entry(Endpoint, Hook),
    ok.

%% internal functions.
lookup_(Endpoint, Hook, Args) ->
    %% The payload is not part of the key, so we remove it.
    Key = {Endpoint, Hook, lists:keydelete(payload, 1, Args)},
    case ets:lookup(?CACHE, Key) of
        [] ->
            not_found;
        [{{_EP,_H,_Args},_Sid,ExpirationTs, Modifiers}] ->
            case expired(ExpirationTs) of
                true ->
                    ets:delete(?CACHE, Key),
                    decr_entry(Endpoint, Hook),
                    not_found;
                false ->
                    Modifiers
            end
    end.

decr_entry(Endpoint, Hook) ->
    update_entries(Endpoint, Hook, -1).

incr_entry(Endpoint, Hook) ->
    update_entries(Endpoint, Hook, 1).

update_entries(Endpoint, Hook, Val) ->
    Key = {entries, Endpoint, Hook},
    ets:update_counter(?STATS, Key, Val, {Key, 0}).

miss(Endpoint, Hook) ->
    Key = {misses, Endpoint, Hook},
    ets:update_counter(?STATS, Key, 1, {Key, 0}).

hit(Endpoint, Hook) ->
    Key = {hits, Endpoint, Hook},
    ets:update_counter(?STATS, Key, 1, {Key, 0}).

expired(ExpirationTs) ->
    ExpirationTs < trunc(erlang:system_time()/1000000000).

ts_from_now(MaxAge) ->
    trunc(erlang:system_time()/1000000000) + MaxAge.

stats() ->
    maps:from_list(ets:tab2list(?STATS)).
