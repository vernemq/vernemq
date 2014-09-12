-module(emqttd_passwd).

-export([init/1,
         check/2,
         hash/1, hash/2,
         load_from_file/1,
         load_from_list/1]).
-export([auth_on_register/4]).

-define(TABLE, ?MODULE).
-define(SALT_LEN, 12).

-register_hook({auth_on_register, {?MODULE, auth_on_register, 4}}).

init(AllowAnonymous) ->
    ets:new(?TABLE, [public, named_table, {read_concurrency, true}]),
    ets:insert(?TABLE, {anonymous, AllowAnonymous}),
    ok.

load_from_file(File) ->
    age_entries(),
    {ok, Fd} = file:open(File, [read, binary]),
    F = fun(FF, read) -> {FF, rl(Fd)};
           (_, close) -> file:close(Fd)
        end,
    parse_passwd_line(F(F,read)),
    del_aged_entries().

load_from_list(List) ->
    age_entries(),
    put(emqttd_passwd_list, List),
    F = fun(FF, read) ->
                case get(emqttd_passwd_list) of
                    [I|Rest] ->
                        put(emqttd_passwd_list, Rest),
                        {FF, I};
                    [] ->
                        {FF, eof}
                end;
           (_, close) ->
                put(emqttd_passwd_list, undefined),
                ok
        end,
    parse_passwd_line(F(F, read)),
    del_aged_entries().

check(undefined, _) ->
    %% check if we allow anonymous connections
    [{_, Res}] = ets:lookup(?TABLE, anonymous),
    Res;
check(User, Password) ->
    case ets:lookup(?TABLE, ensure_binary(User)) of
        [{_, SaltB64, EncPassword, _}] ->
            hash(Password, base64:decode(SaltB64)) == EncPassword;
        [] ->
            {error, not_found}
    end.

auth_on_register(_Peer, _ClientId, User, Password) ->
    case check(User, Password) of
        true -> ok;
        false -> {error, invalid_credentials};
        {error, not_found} -> next
    end.

parse_passwd_line({F, eof}) ->
    F(F,close),
    ok;
parse_passwd_line({F, <<"\n">>}) ->
    parse_passwd_line(F(F,read));
parse_passwd_line({F, Line}) ->
    [User, Rest] = re:split(Line, ":"),
    [<<>>, _, Salt, EncPasswd] = binary:split(Rest, <<"$">>, [global]),
    L = byte_size(EncPasswd) -1,
    EncPasswdNew =
    case EncPasswd of
        <<E:L/binary, "\n">> -> E;
        _ -> EncPasswd
    end,
    Item = {User, Salt, EncPasswdNew, 1},
    ets:insert(?TABLE, Item),
    parse_passwd_line(F(F,read)).

age_entries() ->
    iterate(fun(K) -> ets:update_element(?TABLE, K, {4,2}) end).

del_aged_entries() ->
    ets:match_delete(?TABLE, {'_', '_', '_', 2}),
    ok.

iterate(Fun) ->
    iterate(Fun, ets:first(?TABLE)).
iterate(_, '$end_of_table') -> ok;
iterate(Fun, anonymous) ->
    iterate(Fun, ets:next(?TABLE, anonymous));
iterate(Fun, K) ->
    Fun(K),
    iterate(Fun, ets:next(?TABLE, K)).


rl({ok, Data}) -> Data;
rl({error, Reason}) -> exit(Reason);
rl(eof) -> eof;
rl(Fd) ->
    rl(file:read_line(Fd)).

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
