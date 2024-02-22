%% Copyright 2018-2024 Octavo Labs/VerneMQ (https://vernemq.com/)
%% and Individual Contributors.
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

-module(vmq_ssl_psk).
-include_lib("kernel/include/logger.hrl").

-export([
    init/1,
    opts/1,
    psk_lookup/3,
    psk_support_enabled/1,
    ciphers/1
]).

-define(TABLE, ?MODULE).

%
% API
%

psk_support_enabled(Opts) ->
    proplists:get_value(psk_support, Opts, false).

opts(Opts) ->
    case psk_support_enabled(Opts) of
        false ->
            [];
        _ ->
            [
                {psk_identity, proplists:get_value(psk_identity_hint, Opts, "VMQ")},
                {user_lookup_fun, {fun psk_lookup/3, #{}}}
            ]
    end.

psk_lookup(psk, PskIdentity, _) ->
    case ets:lookup(?TABLE, PskIdentity) of
        [{_, PSK, _}] -> {ok, PSK};
        _ -> {error, no_psk_found_for_identity}
    end.

ciphers(Opts) ->
    case psk_support_enabled(Opts) of
        true ->
            [
                "PSK-AES256-GCM-SHA384",
                "PSK-AES256-CBC-SHA",
                "PSK-AES128-GCM-SHA256",
                "PSK-AES128-CBC-SHA"
            ];
        _ ->
            []
    end.

%%
%% INTERNAL FILE HANDLING
%%
%% The Pre-Shared-Keys are read from a file
%%

init(Opts) ->
    case psk_support_enabled(Opts) of
        true ->
            case lists:member(?TABLE, ets:all()) of
                true ->
                    ok;
                false ->
                    ets:new(?TABLE, [public, named_table, {read_concurrency, true}]),
                    load_from_file(
                        proplists:get_value(pskfile, Opts),
                        proplists:get_value(pskfile_separator, Opts, ":")
                    )
            end,
            ok;
        _ ->
            ok
    end.

load_from_file(File, Separator) ->
    case file:open(File, [read, binary]) of
        {ok, Fd} ->
            age_entries(),
            F = fun
                (FF, read) -> {FF, rl(Fd)};
                (_, close) -> file:close(Fd)
            end,
            parse_psk_line(F(F, read), Separator),
            del_aged_entries();
        {error, _Reason} ->
            ?LOG_ERROR("Failed to open PSK file: ~p, reason: ~p", [File, _Reason]),
            {error, _Reason}
    end.

parse_psk_line({F, eof}, _) ->
    F(F, close),
    ok;
parse_psk_line({F, <<"\n">>}, Separator) ->
    parse_psk_line(F(F, read), Separator);
parse_psk_line({F, Line}, Separator) ->
    [Ident, PSK] = binary:split(Line, list_to_binary(Separator)),
    Item = {Ident, PSK, 1},
    ets:insert(?TABLE, Item),
    parse_psk_line(F(F, read), Separator).

age_entries() ->
    iterate(fun(K) -> ets:update_element(?TABLE, K, {3, 2}) end).

del_aged_entries() ->
    ets:match_delete(?TABLE, {'_', '_', 2}),
    ok.

iterate(Fun) ->
    iterate(Fun, ets:first(?TABLE)).
iterate(_, '$end_of_table') ->
    ok;
iterate(Fun, K) ->
    Fun(K),
    iterate(Fun, ets:next(?TABLE, K)).

rl({ok, Data}) -> string:trim(Data, trailing, "\n");
rl({error, Reason}) -> exit(Reason);
rl(eof) -> eof;
rl(Fd) -> rl(file:read_line(Fd)).
