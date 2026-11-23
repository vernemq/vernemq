%%%-------------------------------------------------------------------
-module(vmq_passwd_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("stdlib/include/assert.hrl").

all() ->
    [
        disabled_binding,
        required_binding,
        mountpoint_binding,
        optional_binding,
        unavailable_binding_file,
        authentication_hooks,
        client_id_file_format,
        malformed_binding_file,
        conflicting_client_id_owners,
        empty_binding_file,
        client_id_file_load,
        binding_reload
    ].

init_per_testcase(_Case, Config) ->
    ok = vmq_passwd:init(),
    application:set_env(vmq_passwd, client_id_binding, disabled),
    ok = vmq_passwd:load_from_list([]),
    ok = vmq_passwd:clear_client_ids(),
    Config.

end_per_testcase(_Case, _Config) ->
    application:set_env(vmq_passwd, client_id_binding, disabled),
    ok.

disabled_binding(_) ->
    load_user(<<"user">>, <<"secret">>),
    application:set_env(vmq_passwd, client_id_binding, disabled),
    ?assertEqual(ok, vmq_passwd:check(<<"user">>, <<"any-client">>, <<"secret">>)).

required_binding(_) ->
    load_user(<<"user">>, <<"secret">>),
    ok = vmq_passwd:load_client_ids_from_list([<<"\"\"\tuser\tclient-1\n">>]),
    application:set_env(vmq_passwd, client_id_binding, required),
    ?assertEqual(ok, vmq_passwd:check(<<"user">>, <<"client-1">>, <<"secret">>)),
    ?assertEqual(
        {error, invalid_credentials},
        vmq_passwd:check(<<"user">>, <<"client-2">>, <<"secret">>)
    ),
    ?assertEqual(
        {error, invalid_credentials},
        vmq_passwd:check(<<"user">>, <<"client-1">>, <<"wrong">>)
    ),
    ?assertEqual(
        {error, invalid_credentials}, vmq_passwd:check(<<"user">>, undefined, <<"secret">>)
    ),
    ?assertEqual(next, vmq_passwd:check(<<"unknown">>, <<"client-1">>, <<"secret">>)).

mountpoint_binding(_) ->
    load_user(<<"user">>, <<"secret">>),
    ok = vmq_passwd:load_client_ids_from_list([
        <<"\"\"\tuser\tdefault-client\n">>,
        <<"tenant-a\tuser\ttenant-client\n">>
    ]),
    application:set_env(vmq_passwd, client_id_binding, required),
    ?assertEqual(
        ok, vmq_passwd:check(<<>>, <<"user">>, <<"default-client">>, <<"secret">>)
    ),
    ?assertEqual(
        ok, vmq_passwd:check("tenant-a", <<"user">>, <<"tenant-client">>, <<"secret">>)
    ),
    ?assertEqual(
        {error, invalid_credentials},
        vmq_passwd:check(<<"tenant-b">>, <<"user">>, <<"tenant-client">>, <<"secret">>)
    ),
    ?assertEqual(
        {error, invalid_credentials},
        vmq_passwd:check(<<>>, <<"user">>, <<"tenant-client">>, <<"secret">>)
    ).

optional_binding(_) ->
    load_users([{<<"bound">>, <<"secret">>}, {<<"legacy">>, <<"secret">>}]),
    ok = vmq_passwd:load_client_ids_from_list([
        <<"\"\"\tbound\tclient-1\n">>, <<"tenant-a\tlegacy\ttenant-client\n">>
    ]),
    application:set_env(vmq_passwd, client_id_binding, optional),
    ?assertEqual(ok, vmq_passwd:check(<<"bound">>, <<"client-1">>, <<"secret">>)),
    ?assertEqual(
        {error, invalid_credentials},
        vmq_passwd:check(<<"bound">>, <<"other">>, <<"secret">>)
    ),
    ?assertEqual(ok, vmq_passwd:check(<<"legacy">>, <<"other">>, <<"secret">>)),
    ?assertEqual(
        {error, invalid_credentials},
        vmq_passwd:check(<<"legacy">>, <<"client-1">>, <<"secret">>)
    ),
    ?assertEqual(
        {error, invalid_credentials},
        vmq_passwd:check(<<"tenant-a">>, <<"legacy">>, <<"other">>, <<"secret">>)
    ).

unavailable_binding_file(_) ->
    load_user(<<"user">>, <<"secret">>),
    ok = vmq_passwd:invalidate_client_ids(),
    application:set_env(vmq_passwd, client_id_binding, optional),
    ?assertEqual(
        {error, invalid_credentials},
        vmq_passwd:check(<<"user">>, <<"any-client">>, <<"secret">>)
    ),
    application:set_env(vmq_passwd, client_id_binding, required),
    ?assertEqual(
        {error, invalid_credentials},
        vmq_passwd:check(<<"user">>, <<"any-client">>, <<"secret">>)
    ),
    application:set_env(vmq_passwd, client_id_binding, disabled),
    ?assertEqual(ok, vmq_passwd:check(<<"user">>, <<"any-client">>, <<"secret">>)).

authentication_hooks(_) ->
    load_user(<<"user">>, <<"secret">>),
    ok = vmq_passwd:load_client_ids_from_list([
        <<"\"\"\tuser\tclient-1\n">>, <<"tenant-a\tuser\ttenant-client">>
    ]),
    application:set_env(vmq_passwd, client_id_binding, required),
    ?assertEqual(
        ok,
        vmq_passwd:auth_on_register(
            {{127, 0, 0, 1}, 1883}, {<<>>, <<"client-1">>}, <<"user">>, <<"secret">>, true
        )
    ),
    ?assertEqual(
        ok,
        vmq_passwd:auth_on_register_m5(
            {{127, 0, 0, 1}, 1883},
            {<<>>, <<"client-1">>},
            <<"user">>,
            <<"secret">>,
            true,
            #{}
        )
    ),
    ?assertEqual(
        ok,
        vmq_passwd:auth_on_register(
            {{127, 0, 0, 1}, 1883},
            {"tenant-a", <<"tenant-client">>},
            <<"user">>,
            <<"secret">>,
            true
        )
    ).

client_id_file_format(_) ->
    load_user(<<"user">>, <<"secret">>),
    ok = vmq_passwd:load_client_ids_from_list([
        <<"# comment\r\n">>,
        <<"\r\n">>,
        <<"\"\"\tuser\tclient:with:colons\r\n">>
    ]),
    application:set_env(vmq_passwd, client_id_binding, required),
    ?assertEqual(
        ok, vmq_passwd:check(<<"user">>, <<"client:with:colons">>, <<"secret">>)
    ).

malformed_binding_file(_) ->
    load_user(<<"user">>, <<"secret">>),
    ok = vmq_passwd:load_client_ids_from_list([<<"\"\"\tuser\told-client\n">>]),
    application:set_env(vmq_passwd, client_id_binding, optional),
    ?assertEqual(ok, vmq_passwd:check(<<"user">>, <<"old-client">>, <<"secret">>)),
    ?assertMatch(
        {error, {invalid_client_id_binding, 2, expected_three_tab_separated_fields}},
        vmq_passwd:load_client_ids_from_list([
            <<"\"\"\tuser\tnew-client\n">>, <<"spaces are not separators\n">>
        ])
    ),
    ?assertEqual(
        {error, invalid_credentials},
        vmq_passwd:check(<<"user">>, <<"old-client">>, <<"secret">>)
    ),
    ?assertEqual(
        {error, invalid_credentials},
        vmq_passwd:check(<<"user">>, <<"new-client">>, <<"secret">>)
    ).

conflicting_client_id_owners(_) ->
    load_users([{<<"alice">>, <<"secret">>}, {<<"bob">>, <<"secret">>}]),
    application:set_env(vmq_passwd, client_id_binding, required),
    ?assertMatch(
        {error, {
            invalid_client_id_binding, 3, client_id_assigned_to_multiple_users
        }},
        vmq_passwd:load_client_ids_from_list([
            <<"tenant-a\talice\tdevice-1\n">>,
            <<"tenant-a\talice\tdevice-1\n">>,
            <<"tenant-a\tbob\tdevice-1\n">>
        ])
    ),
    ?assertEqual(
        {error, invalid_credentials},
        vmq_passwd:check(<<"tenant-a">>, <<"alice">>, <<"device-1">>, <<"secret">>)
    ),
    ok = vmq_passwd:load_client_ids_from_list([
        <<"tenant-a\talice\tdevice-1\n">>, <<"tenant-b\tbob\tdevice-1\n">>
    ]),
    ?assertEqual(
        ok, vmq_passwd:check(<<"tenant-a">>, <<"alice">>, <<"device-1">>, <<"secret">>)
    ),
    ?assertEqual(
        ok, vmq_passwd:check(<<"tenant-b">>, <<"bob">>, <<"device-1">>, <<"secret">>)
    ).

empty_binding_file(_) ->
    load_user(<<"user">>, <<"secret">>),
    application:set_env(vmq_passwd, client_id_binding, optional),
    ?assertEqual(
        {error, empty_client_id_binding_file},
        vmq_passwd:load_client_ids_from_list([<<"# no bindings\n">>, <<"\n">>])
    ),
    ?assertEqual(
        {error, invalid_credentials},
        vmq_passwd:check(<<"user">>, <<"any-client">>, <<"secret">>)
    ).

client_id_file_load(Config) ->
    load_user(<<"user">>, <<"secret">>),
    File = filename:join(proplists:get_value(priv_dir, Config), "vmq.clientids"),
    ok = file:write_file(File, <<"\"\"\tuser\tfile-client\n">>),
    ok = vmq_passwd:load_client_ids_from_file(File),
    application:set_env(vmq_passwd, client_id_binding, required),
    ?assertEqual(ok, vmq_passwd:check(<<"user">>, <<"file-client">>, <<"secret">>)).

binding_reload(_) ->
    load_user(<<"user">>, <<"secret">>),
    application:set_env(vmq_passwd, client_id_binding, required),
    ok = vmq_passwd:load_client_ids_from_list([<<"\"\"\tuser\told-client">>]),
    ?assertEqual(ok, vmq_passwd:check(<<"user">>, <<"old-client">>, <<"secret">>)),
    ok = vmq_passwd:load_client_ids_from_list([<<"\"\"\tuser\tnew-client">>]),
    ?assertEqual(
        {error, invalid_credentials},
        vmq_passwd:check(<<"user">>, <<"old-client">>, <<"secret">>)
    ),
    ?assertEqual(ok, vmq_passwd:check(<<"user">>, <<"new-client">>, <<"secret">>)),
    ?assertEqual(2, ets:info(vmq_passwd_client_ids, size)).

load_user(User, Password) ->
    load_users([{User, Password}]).

load_users(Users) ->
    Lines =
        lists:map(
            fun({User, Password}) ->
                Salt = crypto:strong_rand_bytes(12),
                Salt64 = base64:encode(Salt),
                Hash = vmq_passwd:hash(Password, Salt),
                <<User/binary, ":$6$", Salt64/binary, "$", Hash/binary, "\n">>
            end,
            Users
        ),
    vmq_passwd:load_from_list(Lines).
