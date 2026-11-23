%%%-------------------------------------------------------------------
-module(vmq_passwd_integration_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("vmq_commons/include/vmq_types.hrl").

all() ->
    [
        mqtt3_default_and_mountpoint_bindings,
        mqtt5_default_and_mountpoint_bindings,
        unavailable_and_malformed_files,
        binding_reload,
        mqtt5_assigned_client_id,
        allow_anonymous_bypasses_bindings
    ].

init_per_suite(Config) ->
    application:ensure_started(credentials_obfuscation),
    credentials_obfuscation:set_secret(<<"Testing">>),
    vmq_test_utils:setup(),
    DefaultPort = vmq_test_utils:get_free_port(),
    {ok, _} = vmq_server_cmd:listener_start(
        DefaultPort, "127.0.0.1", [{allowed_protocol_versions, "3,4,5"}]
    ),
    TenantPort = vmq_test_utils:get_free_port(),
    {ok, _} = vmq_server_cmd:listener_start(
        TenantPort,
        "127.0.0.1",
        [{allowed_protocol_versions, "3,4,5"}, {mountpoint, "tenant-a"}]
    ),
    TableOwner = start_table_owner(),
    ok = vmq_plugin_mgr:enable_module_plugin(
        auth_on_register, vmq_passwd, auth_on_register, 5
    ),
    ok = vmq_plugin_mgr:enable_module_plugin(
        auth_on_register_m5, vmq_passwd, auth_on_register_m5, 6
    ),
    [
        {default_port, DefaultPort},
        {tenant_port, TenantPort},
        {table_owner, TableOwner}
        | Config
    ].

end_per_suite(Config) ->
    ok = vmq_plugin_mgr:disable_module_plugin(
        auth_on_register, vmq_passwd, auth_on_register, 5
    ),
    ok = vmq_plugin_mgr:disable_module_plugin(
        auth_on_register_m5, vmq_passwd, auth_on_register_m5, 6
    ),
    vmq_server_cmd:listener_stop(port(default_port, Config), "127.0.0.1", false),
    vmq_server_cmd:listener_stop(port(tenant_port, Config), "127.0.0.1", false),
    stop_table_owner(proplists:get_value(table_owner, Config)),
    vmq_test_utils:teardown(),
    logger:remove_handler(test_logger_file),
    application:set_env(vmq_passwd, client_id_binding, disabled),
    application:set_env(vmq_plugin, wait_for_proc, init),
    application:set_env(vmq_plugin, default_schema_dir, []),
    application:unload(vmq_plugin),
    ok.

init_per_testcase(_Case, Config) ->
    vmq_server_cmd:set_config(allow_anonymous, false),
    vmq_config:configure_node(),
    application:set_env(vmq_passwd, client_id_binding, required),
    load_users([{<<"sensor">>, <<"secret">>}]),
    ok = vmq_passwd:load_client_ids_from_list([
        <<"\"\"\tsensor\tdefault-client\n">>,
        <<"tenant-a\tsensor\ttenant-client\n">>,
        <<"tenant-a\tsensor\told-client\n">>
    ]),
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

mqtt3_default_and_mountpoint_bindings(Config) ->
    mqtt3_connect(default_port, "default-client", 0, Config),
    mqtt3_connect(default_port, "tenant-client", 4, Config),
    mqtt3_connect(tenant_port, "tenant-client", 0, Config),
    mqtt3_connect(tenant_port, "default-client", 4, Config).

mqtt5_default_and_mountpoint_bindings(Config) ->
    mqtt5_connect(default_port, "default-client", ?M5_SUCCESS, Config),
    mqtt5_connect(default_port, "tenant-client", ?M5_BAD_USERNAME_OR_PASSWORD, Config),
    mqtt5_connect(tenant_port, "tenant-client", ?M5_SUCCESS, Config),
    mqtt5_connect(tenant_port, "default-client", ?M5_BAD_USERNAME_OR_PASSWORD, Config).

unavailable_and_malformed_files(Config) ->
    MissingFile = filename:join(proplists:get_value(priv_dir, Config), "missing.clientids"),
    _ = file:delete(MissingFile),
    {error, enoent} = vmq_passwd:load_client_ids_from_file(MissingFile),
    mqtt3_connect(default_port, "default-client", 4, Config),
    {error, {invalid_client_id_binding, 1, expected_three_tab_separated_fields}} =
        vmq_passwd:load_client_ids_from_list([<<"not tab separated\n">>]),
    mqtt5_connect(default_port, "default-client", ?M5_BAD_USERNAME_OR_PASSWORD, Config).

binding_reload(Config) ->
    mqtt3_connect(tenant_port, "old-client", 0, Config),
    ok = vmq_passwd:load_client_ids_from_list([
        <<"\"\"\tsensor\tdefault-client\n">>, <<"tenant-a\tsensor\tnew-client\n">>
    ]),
    mqtt3_connect(tenant_port, "old-client", 4, Config),
    mqtt5_connect(tenant_port, "new-client", ?M5_SUCCESS, Config).

mqtt5_assigned_client_id(Config) ->
    Connect = packetv5:gen_connect(empty, [
        {clean_start, true}, {username, "sensor"}, {password, "secret"}
    ]),
    Connack = packetv5:gen_connack(0, ?M5_BAD_USERNAME_OR_PASSWORD),
    {ok, Socket} = packetv5:do_client_connect(Connect, Connack, [
        {port, port(default_port, Config)}
    ]),
    ok = gen_tcp:close(Socket).

allow_anonymous_bypasses_bindings(Config) ->
    vmq_server_cmd:set_config(allow_anonymous, true),
    vmq_config:configure_node(),
    Connect = packet:gen_connect("unbound-client", [{proto_ver, 4}]),
    {ok, Socket} = packet:do_client_connect(
        Connect, packet:gen_connack(0), [{port, port(default_port, Config)}]
    ),
    ok = gen_tcp:close(Socket).

mqtt3_connect(PortName, ClientId, ReturnCode, Config) ->
    Connect = packet:gen_connect(ClientId, [
        {proto_ver, 4}, {username, "sensor"}, {password, "secret"}
    ]),
    {ok, Socket} = packet:do_client_connect(
        Connect, packet:gen_connack(ReturnCode), [{port, port(PortName, Config)}]
    ),
    ok = gen_tcp:close(Socket).

mqtt5_connect(PortName, ClientId, ReasonCode, Config) ->
    Connect = packetv5:gen_connect(ClientId, [
        {clean_start, true}, {username, "sensor"}, {password, "secret"}
    ]),
    Connack = packetv5:gen_connack(0, ReasonCode),
    {ok, Socket} = packetv5:do_client_connect(Connect, Connack, [
        {port, port(PortName, Config)}
    ]),
    ok = gen_tcp:close(Socket).

port(Name, Config) ->
    proplists:get_value(Name, Config).

start_table_owner() ->
    Parent = self(),
    Pid = spawn(fun() ->
        ok = vmq_passwd:init(),
        Parent ! {self(), ready},
        receive
            stop -> ok
        end
    end),
    receive
        {Pid, ready} -> Pid
    end.

stop_table_owner(Pid) ->
    Monitor = monitor(process, Pid),
    Pid ! stop,
    receive
        {'DOWN', Monitor, process, Pid, normal} -> ok
    end.

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
