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

-module(vmq_passwd).
-behaviour(auth_on_register_hook).
-behaviour(auth_on_register_m5_hook).
-behaviour(on_config_change_hook).

-export([
    start/0,
    stop/0,
    init/0,
    check/2,
    hash/1, hash/2,
    load_from_file/1,
    load_from_list/1
]).
-export([
    change_config/1,
    auth_on_register/5,
    auth_on_register_m5/6
]).

-define(TABLE, ?MODULE).
-define(SALT_LEN, 12).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Plugin Callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start() ->
    {ok, _} = application:ensure_all_started(vmq_passwd),
    vmq_passwd_cli:register(),
    ok.

stop() ->
    application:stop(vmq_passwd).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
change_config(Configs) ->
    case lists:keyfind(vmq_passwd, 1, Configs) of
        false ->
            ok;
        _ ->
            vmq_passwd_reloader:change_config_now()
    end.

auth_on_register(_Peer, _SubscriberId, User, Password, _CleanSession) ->
    check(User, Password).

auth_on_register_m5(_Peer, _SubscriberId, User, Password, _CleanStart, _Properties) ->
    check(User, Password).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init() ->
    case lists:member(?TABLE, ets:all()) of
        true ->
            ok;
        false ->
            ets:new(?TABLE, [public, named_table, {read_concurrency, true}])
    end,
    ok.

load_from_file(File) ->
    case file:open(File, [read, binary]) of
        {ok, Fd} ->
            age_entries(),
            F = fun
                (FF, read) -> {FF, rl(Fd)};
                (_, close) -> file:close(Fd)
            end,
            parse_passwd_line(F(F, read)),
            del_aged_entries();
        {error, _Reason} ->
            ok
    end.

load_from_list(List) ->
    age_entries(),
    put(vmq_passwd_list, List),
    F = fun
        (FF, read) ->
            case get(vmq_passwd_list) of
                [I | Rest] ->
                    put(vmq_passwd_list, Rest),
                    {FF, I};
                [] ->
                    {FF, eof}
            end;
        (_, close) ->
            put(vmq_passwd_list, undefined),
            ok
    end,
    parse_passwd_line(F(F, read)),
    del_aged_entries().

check(undefined, _) ->
    next;
check(_, undefined) ->
    next;
check(User, Password) ->
    case ets:lookup(?TABLE, ensure_binary(User)) of
        [{_, SaltB64, EncPassword, _}] ->
            case hash(Password, base64:decode(SaltB64)) == EncPassword of
                true -> ok;
                false -> {error, invalid_credentials}
            end;
        [] ->
            next
    end.

parse_passwd_line({F, eof}) ->
    F(F, close),
    ok;
parse_passwd_line({F, <<"\n">>}) ->
    parse_passwd_line(F(F, read));
parse_passwd_line({F, Line}) ->
    [User, Rest] = re:split(Line, ":"),
    [<<>>, _, Salt, EncPasswd] = binary:split(Rest, <<"$">>, [global]),
    L = byte_size(EncPasswd) - 1,
    EncPasswdNew =
        case EncPasswd of
            <<E:L/binary, "\n">> -> E;
            _ -> EncPasswd
        end,
    Item = {User, Salt, EncPasswdNew, 1},
    ets:insert(?TABLE, Item),
    parse_passwd_line(F(F, read)).

age_entries() ->
    iterate(fun(K) -> ets:update_element(?TABLE, K, {4, 2}) end).

del_aged_entries() ->
    ets:match_delete(?TABLE, {'_', '_', '_', 2}),
    ok.

iterate(Fun) ->
    iterate(Fun, ets:first(?TABLE)).
iterate(_, '$end_of_table') ->
    ok;
iterate(Fun, K) ->
    Fun(K),
    iterate(Fun, ets:next(?TABLE, K)).

rl({ok, Data}) -> Data;
rl({error, Reason}) -> exit(Reason);
rl(eof) -> eof;
rl(Fd) -> rl(file:read_line(Fd)).

ensure_binary(L) when is_list(L) -> list_to_binary(L);
ensure_binary(B) when is_binary(B) -> B.

hash(Password) ->
    hash(Password, crypto:strong_rand_bytes(?SALT_LEN)).

hash(Password, Salt) ->
    Ctx1 = crypto:hash_init(sha512),
    Ctx2 = crypto:hash_update(Ctx1, Password),
    Ctx3 = crypto:hash_update(Ctx2, Salt),
    Digest = crypto:hash_final(Ctx3),
    base64:encode(Digest).
