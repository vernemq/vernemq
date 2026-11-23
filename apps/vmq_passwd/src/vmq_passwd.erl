%% Copyright 2018 Erlio GmbH Basel Switzerland (http://erl.io)
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

-module(vmq_passwd).
-behaviour(auth_on_register_hook).
-behaviour(auth_on_register_m5_hook).
-behaviour(on_config_change_hook).

-export([
    start/0,
    stop/0,
    init/0,
    check/2,
    check/3,
    check/4,
    hash/1, hash/2,
    load_from_file/1,
    load_from_list/1,
    load_client_ids_from_file/1,
    load_client_ids_from_list/1,
    clear_client_ids/0,
    invalidate_client_ids/0
]).
-export([
    change_config/1,
    auth_on_register/5,
    auth_on_register_m5/6
]).

-define(TABLE, ?MODULE).
-define(CLIENT_ID_TABLE, vmq_passwd_client_ids).
-define(CLIENT_ID_USER_TABLE, vmq_passwd_client_id_users).
-define(CLIENT_ID_OWNER_TABLE, vmq_passwd_client_id_owners).
-define(CLIENT_ID_META_TABLE, vmq_passwd_client_id_meta).
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

auth_on_register(_Peer, {Mountpoint, ClientId}, User, Password, _CleanSession) ->
    check(Mountpoint, User, ClientId, Password).

auth_on_register_m5(_Peer, {Mountpoint, ClientId}, User, Password, _CleanStart, _Properties) ->
    check(Mountpoint, User, ClientId, Password).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init() ->
    ensure_table(?TABLE),
    ensure_table(?CLIENT_ID_TABLE),
    ensure_table(?CLIENT_ID_USER_TABLE),
    ensure_table(?CLIENT_ID_OWNER_TABLE),
    ensure_table(?CLIENT_ID_META_TABLE),
    ets:insert_new(?CLIENT_ID_META_TABLE, {active, unavailable, undefined, undefined}),
    ok.

ensure_table(Table) ->
    case lists:member(Table, ets:all()) of
        true -> ok;
        false -> ets:new(Table, [public, named_table, {read_concurrency, true}])
    end.

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

load_client_ids_from_file(File) ->
    case file:read_file(File) of
        {ok, Data} ->
            load_client_id_lines(binary:split(Data, <<"\n">>, [global]));
        {error, Reason} ->
            invalidate_client_ids(),
            {error, Reason}
    end.

load_client_ids_from_list(List) ->
    load_client_id_lines(List).

clear_client_ids() ->
    set_client_id_state(disabled, undefined, undefined),
    clear_client_id_entries(),
    ok.

invalidate_client_ids() ->
    set_client_id_state(unavailable, undefined, undefined),
    clear_client_id_entries(),
    ok.

clear_client_id_entries() ->
    ets:delete_all_objects(?CLIENT_ID_TABLE),
    ets:delete_all_objects(?CLIENT_ID_USER_TABLE),
    ets:delete_all_objects(?CLIENT_ID_OWNER_TABLE),
    ok.

set_client_id_state(Status, Generation, PreviousGeneration) ->
    true = ets:insert(
        ?CLIENT_ID_META_TABLE, {active, Status, Generation, PreviousGeneration}
    ),
    ok.

check(undefined, _) ->
    next;
check(_, undefined) ->
    next;
check(User, Password) ->
    PasswordPlain =
        case Password of
            {encrypted, _} -> credentials_obfuscation:decrypt(Password);
            A -> A
        end,

    case ets:lookup(?TABLE, ensure_binary(User)) of
        [{_, SaltB64, EncPassword, _}] ->
            case hash(PasswordPlain, base64:decode(SaltB64)) == EncPassword of
                true -> ok;
                false -> {error, invalid_credentials}
            end;
        [] ->
            next
    end.

check(User, ClientId, Password) ->
    check(<<>>, User, ClientId, Password).

check(Mountpoint, User, ClientId, Password) ->
    case check(User, Password) of
        ok -> check_client_id(Mountpoint, User, ClientId);
        Result -> Result
    end.

check_client_id(Mountpoint, User, ClientId) ->
    case application:get_env(vmq_passwd, client_id_binding, disabled) of
        disabled ->
            ok;
        Mode ->
            case ets:lookup(?CLIENT_ID_META_TABLE, active) of
                [{active, loaded, Generation, _PreviousGeneration}] ->
                    check_loaded_client_ids(Mode, Generation, Mountpoint, User, ClientId);
                _ ->
                    {error, invalid_credentials}
            end
    end.

check_loaded_client_ids(required, Generation, Mountpoint, User, ClientId) ->
    check_client_id_binding(Generation, Mountpoint, User, ClientId);
check_loaded_client_ids(optional, Generation, Mountpoint, User, ClientId) ->
    MountpointBin = ensure_binary(Mountpoint),
    MountpointUser = {Generation, MountpointBin, ensure_binary(User)},
    case ets:member(?CLIENT_ID_USER_TABLE, MountpointUser) of
        true -> check_client_id_binding(Generation, Mountpoint, User, ClientId);
        false -> check_unassigned_client_id(Generation, MountpointBin, ClientId)
    end.

check_unassigned_client_id(_Generation, _Mountpoint, undefined) ->
    {error, invalid_credentials};
check_unassigned_client_id(Generation, Mountpoint, ClientId) ->
    Key = {Generation, Mountpoint, ensure_binary(ClientId)},
    case ets:member(?CLIENT_ID_OWNER_TABLE, Key) of
        true -> {error, invalid_credentials};
        false -> ok
    end.

check_client_id_binding(_Generation, _Mountpoint, _User, undefined) ->
    {error, invalid_credentials};
check_client_id_binding(Generation, Mountpoint, User, ClientId) ->
    Key = {
        Generation, ensure_binary(Mountpoint), ensure_binary(User), ensure_binary(ClientId)
    },
    case ets:member(?CLIENT_ID_TABLE, Key) of
        true -> ok;
        false -> {error, invalid_credentials}
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

load_client_id_lines(Lines) ->
    case parse_client_id_lines(Lines, 1, #{}, #{}, #{}) of
        {ok, Bindings, Users, ClientOwners} when map_size(Bindings) > 0 ->
            activate_client_ids(Bindings, Users, ClientOwners);
        {ok, _Bindings, _Users, _ClientOwners} ->
            invalidate_client_ids(),
            {error, empty_client_id_binding_file};
        {error, _Reason} = Error ->
            invalidate_client_ids(),
            Error
    end.

parse_client_id_lines([], _LineNumber, Bindings, Users, _ClientOwners) ->
    {ok, Bindings, Users, _ClientOwners};
parse_client_id_lines([Line0 | Rest], LineNumber, Bindings, Users, ClientOwners) ->
    Line = remove_endline(Line0),
    case Line of
        <<>> ->
            parse_client_id_lines(Rest, LineNumber + 1, Bindings, Users, ClientOwners);
        <<"#", _/binary>> ->
            parse_client_id_lines(Rest, LineNumber + 1, Bindings, Users, ClientOwners);
        _ ->
            case parse_client_id_binding(Line) of
                {ok, {Mountpoint, User, ClientId} = Binding} ->
                    Client = {Mountpoint, ClientId},
                    case maps:get(Client, ClientOwners, User) of
                        User ->
                            parse_client_id_lines(
                                Rest,
                                LineNumber + 1,
                                Bindings#{Binding => true},
                                Users#{{Mountpoint, User} => true},
                                ClientOwners#{Client => User}
                            );
                        _OtherUser ->
                            {error, {
                                invalid_client_id_binding,
                                LineNumber,
                                client_id_assigned_to_multiple_users
                            }}
                    end;
                {error, Reason} ->
                    {error, {invalid_client_id_binding, LineNumber, Reason}}
            end
    end.

parse_client_id_binding(Line) ->
    case binary:split(Line, <<"\t">>, [global]) of
        [<<>>, _User, _ClientId] ->
            {error, empty_mountpoint};
        [_Mountpoint, <<>>, _ClientId] ->
            {error, empty_username};
        [_Mountpoint, _User, <<>>] ->
            {error, empty_client_id};
        [Mountpoint0, User, ClientId] ->
            {ok, {parse_mountpoint(Mountpoint0), User, ClientId}};
        _ ->
            {error, expected_three_tab_separated_fields}
    end.

activate_client_ids(Bindings, Users, ClientOwners) ->
    Generation = erlang:unique_integer([monotonic, positive]),
    PreviousGeneration = active_client_id_generation(),
    true = ets:insert(?CLIENT_ID_TABLE, [
        {{Generation, Mountpoint, User, ClientId}, true}
     || {Mountpoint, User, ClientId} <- maps:keys(Bindings)
    ]),
    true = ets:insert(?CLIENT_ID_USER_TABLE, [
        {{Generation, Mountpoint, User}, true}
     || {Mountpoint, User} <- maps:keys(Users)
    ]),
    true = ets:insert(?CLIENT_ID_OWNER_TABLE, [
        {{Generation, Mountpoint, ClientId}, User}
     || {{Mountpoint, ClientId}, User} <- maps:to_list(ClientOwners)
    ]),
    set_client_id_state(loaded, Generation, PreviousGeneration),
    delete_old_client_id_generations([Generation, PreviousGeneration]),
    ok.

active_client_id_generation() ->
    case ets:lookup(?CLIENT_ID_META_TABLE, active) of
        [{active, loaded, Generation, _PreviousGeneration}] -> Generation;
        _ -> undefined
    end.

delete_old_client_id_generations(Keep) ->
    delete_old_client_id_generations(?CLIENT_ID_TABLE, Keep, ets:first(?CLIENT_ID_TABLE)),
    delete_old_client_id_generations(
        ?CLIENT_ID_USER_TABLE, Keep, ets:first(?CLIENT_ID_USER_TABLE)
    ),
    delete_old_client_id_generations(
        ?CLIENT_ID_OWNER_TABLE, Keep, ets:first(?CLIENT_ID_OWNER_TABLE)
    ).

delete_old_client_id_generations(_Table, _Keep, '$end_of_table') ->
    ok;
delete_old_client_id_generations(Table, Keep, Key) ->
    Next = ets:next(Table, Key),
    Generation = element(1, Key),
    case lists:member(Generation, Keep) of
        true -> ok;
        false -> ets:delete(Table, Key)
    end,
    delete_old_client_id_generations(Table, Keep, Next).

parse_mountpoint(<<"\"\"">>) ->
    <<>>;
parse_mountpoint(Mountpoint) ->
    Mountpoint.

remove_endline(<<>>) ->
    <<>>;
remove_endline(Line) ->
    Size = byte_size(Line) - 1,
    case Line of
        <<Rest:Size/binary, C>> when C =:= $\n; C =:= $\r -> remove_endline(Rest);
        _ -> Line
    end.

age_entries() ->
    iterate(?TABLE, fun(K) -> ets:update_element(?TABLE, K, {4, 2}) end).

del_aged_entries() ->
    ets:match_delete(?TABLE, {'_', '_', '_', 2}),
    ok.

iterate(Table, Fun) ->
    iterate(Table, Fun, ets:first(Table)).
iterate(_, _, '$end_of_table') ->
    ok;
iterate(Table, Fun, K) ->
    Fun(K),
    iterate(Table, Fun, ets:next(Table, K)).

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
