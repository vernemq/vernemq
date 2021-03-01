-module(vmq_connect_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(Config) ->
    cover:start(),
    vmq_test_utils:setup(),
    [{ct_hooks, vmq_cth}|Config].

end_per_suite(_Config) ->
    vmq_test_utils:teardown(),
    _Config.

init_per_group(mqtts, Config) ->
    Config1 = [{type, tcp},{port, 1889}, {address, "127.0.0.1"}|Config],
    start_listener(Config1);
init_per_group(mqttws, Config) ->
    Config1 = [{type, ws},{port, 1890}, {address, "127.0.0.1"}|Config],
    start_listener(Config1);
init_per_group(mqttwsp, Config) ->
    Config1 = [{type, ws},{port, 1891}, {address, "127.0.0.1"}, {proxy_protocol, true}|Config],
    start_listener(Config1);
init_per_group(mqttv4, Config) ->
    Config1 = [{type, tcp},{port, 1888}, {address, "127.0.0.1"}|Config],
    [{protover, 4}|start_listener(Config1)];
init_per_group(mqttv5, Config) ->
    Config1 = [{type, tcp},{port, 1887}, {address, "127.0.0.1"}|Config],
    [{protover, 5}|start_listener(Config1)].


end_per_group(_Group, Config) ->
    stop_listener(Config),
    ok.

init_per_testcase(_Case, Config) ->
    %% reset config before each test
    vmq_server_cmd:set_config(allow_anonymous, false),
    vmq_server_cmd:set_config(max_client_id_size, 23),
    vmq_config:configure_node(),
    Config.

end_per_testcase(_, Config) ->
    Config.

all() ->
    [
     {group, mqtts},
     {group, mqttws},
     {group, mqttwsp}, % ws with proxy protocol
     {group, mqttv4},
     {group, mqttv5}
    ].

groups() ->
    Tests =
        [anon_denied_test,
         anon_success_test,
         invalid_id_0_test,
         invalid_id_0_311_test,
         invalid_id_missing_test,
         invalid_id_24_test,
         invalid_protonum_test,
         uname_no_password_denied_test,
         uname_password_denied_test,
         uname_password_success_test,
         change_subscriber_id_test
        ],
    [
     {mqttv4, [shuffle,sequence],
      [auth_on_register_change_username_test|Tests]},
     {mqtts, [], Tests},
     {mqttws, [], [ws_protocols_list_test, ws_no_known_protocols_test] ++ Tests},
     {mqttwsp, [], [ws_proxy_protocol_v1_test, ws_proxy_protocol_v2_test,
                    ws_proxy_protocol_localcommand_v1_test, ws_proxy_protocol_localcommand_v2_test]},
     {mqttv5, [auth_on_register_change_username_test]}
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
anon_denied_test(Config) ->
    Connect = packet:gen_connect("connect-anon-test", [{keepalive,10}]),
    Connack = packet:gen_connack(5),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, conn_opts(Config)),
    ok = close(Socket, Config).

anon_success_test(Config) ->
    vmq_server_cmd:set_config(allow_anonymous, true),
    vmq_config:configure_node(),
    %% allow_anonymous is proxied through vmq_config.erl
    Connect = packet:gen_connect("connect-success-test", [{keepalive,10}]),
    Connack = packet:gen_connack(0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, conn_opts(Config)),
    ok = close(Socket, Config).

invalid_id_0_test(Config) ->
    Connect = packet:gen_connect(empty, [{keepalive,10}]),
    Connack = packet:gen_connack(2),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, conn_opts(Config)),
    ok = close(Socket, Config).

invalid_id_0_311_test(Config) ->
    Connect = packet:gen_connect(empty, [{keepalive,10},{proto_ver,4}]),
    Connack = packet:gen_connack(0),
    ok = vmq_plugin_mgr:enable_module_plugin(
      auth_on_register, ?MODULE, hook_empty_client_id_proto_4, 5),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, conn_opts(Config)),
    ok = vmq_plugin_mgr:disable_module_plugin(
      auth_on_register, ?MODULE, hook_empty_client_id_proto_4, 5),
    ok = close(Socket, Config).

invalid_id_missing_test(Config) ->
    Connect = packet:gen_connect(undefined, [{keepalive,10}]),
    {error, closed} = packet:do_client_connect(Connect, <<>>, conn_opts(Config)).

invalid_id_24_test(Config) ->
    Connect = packet:gen_connect("connect-invalid-id-test-", [{keepalive,10}]),
    Connack = packet:gen_connack(2), %% client id longer than 23 Characters
    {ok, Socket} = packet:do_client_connect(Connect, Connack, conn_opts(Config)),
    ok = close(Socket, Config).

invalid_protonum_test(Config) ->
    %% mosq_test.gen_connect("test", keepalive=10, proto_ver=0)
    Connect = <<16#10,16#12,16#00,16#06,16#4d,16#51,16#49,16#73,
                16#64,16#70,16#00,16#02,16#00,16#0a,16#00,16#04,
                16#74,16#65,16#73,16#74>>,
    Connack = packet:gen_connack(1),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, conn_opts(Config)),
    ok = close(Socket, Config).

uname_no_password_denied_test(Config) ->
    Connect = packet:gen_connect("connect-uname-test-", [{keepalive,10}, {username, "user"}]),
    Connack = packet:gen_connack(4),
    ok = vmq_plugin_mgr:enable_module_plugin(
      auth_on_register, ?MODULE, hook_uname_no_password_denied, 5),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, conn_opts(Config)),
    ok = vmq_plugin_mgr:disable_module_plugin(
      auth_on_register, ?MODULE, hook_uname_no_password_denied, 5),
    ok = close(Socket, Config).

uname_password_denied_test(Config) ->
    Connect = packet:gen_connect("connect-uname-pwd-test", [{keepalive,10}, {username, "user"},
                                                            {password, "password9"}]),
    Connack = packet:gen_connack(4),
    ok = vmq_plugin_mgr:enable_module_plugin(
      auth_on_register, ?MODULE, hook_uname_password_denied, 5),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, conn_opts(Config)),
    ok = vmq_plugin_mgr:disable_module_plugin(
      auth_on_register, ?MODULE, hook_uname_password_denied, 5),
    ok = close(Socket, Config).

uname_password_success_test(Config) ->
    Connect = packet:gen_connect("connect-uname-pwd-test", [{keepalive,10}, {username, "user"},
                                                            {password, "password9"}]),
    Connack = packet:gen_connack(0),
    ok = vmq_plugin_mgr:enable_module_plugin(
      auth_on_register, ?MODULE, hook_uname_password_success, 5),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, conn_opts(Config)),
    ok = vmq_plugin_mgr:disable_module_plugin(
      auth_on_register, ?MODULE, hook_uname_password_success, 5),
    ok = close(Socket, Config).

change_subscriber_id_test(Config) ->
    Connect = packet:gen_connect("change-sub-id-test",
                                 [{keepalive,10}, {username, "whatever"},
                                  {password, "whatever"}]),
    Connack = packet:gen_connack(0),
    ok = vmq_plugin_mgr:enable_module_plugin(
      auth_on_register, ?MODULE, hook_change_subscriber_id, 5),
    ok = vmq_plugin_mgr:enable_module_plugin(
      on_register, ?MODULE, hook_on_register_changed_subscriber_id, 3),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, conn_opts(Config)),
    ok = vmq_plugin_mgr:disable_module_plugin(
      on_register, ?MODULE, hook_on_register_changed_subscriber_id, 3),
    ok = vmq_plugin_mgr:disable_module_plugin(
      auth_on_register, ?MODULE, hook_change_subscriber_id, 5),
    ok = close(Socket, Config).

auth_on_register_change_username_test(Config) ->
    Connect = mqtt5_v4compat:gen_connect("change-username-test",
                                         [{keepalive,10}, {username, "old_username"},
                                          {password, "whatever"}], Config),
    Connack = mqtt5_v4compat:gen_connack(success, Config),
    ok = vmq_plugin_mgr:enable_module_plugin(
      auth_on_register, ?MODULE, hook_change_username, 5),
    ok = vmq_plugin_mgr:enable_module_plugin(
      on_register, ?MODULE, hook_on_register_changed_username, 3),

    ok = vmq_plugin_mgr:enable_module_plugin(
      auth_on_register_m5, ?MODULE, hook_change_username_m5, 6),
    ok = vmq_plugin_mgr:enable_module_plugin(
      on_register_m5, ?MODULE, hook_on_register_changed_username_m5, 4),

    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, conn_opts(Config), Config),

    ok = vmq_plugin_mgr:disable_module_plugin(
      auth_on_register_m5, ?MODULE, hook_change_username_m5, 6),
    ok = vmq_plugin_mgr:disable_module_plugin(
      on_register_m5, ?MODULE, hook_on_register_changed_username_m5, 4),

    ok = vmq_plugin_mgr:disable_module_plugin(
      on_register, ?MODULE, hook_on_register_changed_username, 3),
    ok = vmq_plugin_mgr:disable_module_plugin(
      auth_on_register, ?MODULE, hook_change_username, 5),
    ok = close(Socket, Config).

ws_protocols_list_test(Config) ->
    Connect = packet:gen_connect("ws_protocols_list_test", [{keepalive,10}]),
    Connack = packet:gen_connack(5),
    WSOpt  = {conn_opts, [{ws_protocols, ["foo", "mqtt", "bar"]}]},
    ConnOpts = [WSOpt | conn_opts(Config)],
    {ok, Socket} = packet:do_client_connect(Connect, Connack, ConnOpts),
    ok = close(Socket, Config).

ws_no_known_protocols_test(Config) ->
    Connect = packet:gen_connect("ws_no_known_protocols_test", [{keepalive,10}]),
    Connack = packet:gen_connack(5),
    WSOpt  = {conn_opts, [{ws_protocols, ["foo", "bar", "baz"]}]},
    ConnOpts = [WSOpt | conn_opts(Config)],
    {error, unknown_websocket_protocol} = packet:do_client_connect(Connect, Connack, ConnOpts),
    ok.

ws_proxy_protocol_v1_test(Config) ->
    ProxyInfo = 
        #{version => 1, command => proxy,
		transport_family => ipv4,
		transport_protocol => stream,
		src_address => {127, 0, 0, 1}, src_port => 80,
		dest_address => {127, 0, 0, 1}, dest_port => 81},
    Connect = packet:gen_connect("ws_proxy_protocol_test", [{keepalive,10}]),
    Connack = packet:gen_connack(5),
    WSOpt  = {conn_opts, [{ws_protocols, ["mqtt"]}]},
    ConnOpts = [WSOpt | conn_opts(Config)],
    {ok, Socket} = packet:do_client_connect(Connect, Connack, [{proxy_info, ProxyInfo}|ConnOpts]),
    ok = close(Socket, Config).

ws_proxy_protocol_v2_test(Config) ->
    ProxyInfo = 
        #{version => 2, command => proxy,
		transport_family => ipv4,
		transport_protocol => stream,
		src_address => {127, 0, 0, 1}, src_port => 80,
		dest_address => {127, 0, 0, 1}, dest_port => 81},
    Connect = packet:gen_connect("ws_proxy_protocol_test", [{keepalive,10}]),
    Connack = packet:gen_connack(5),
    WSOpt  = {conn_opts, [{ws_protocols, ["mqtt"]}]},
    ConnOpts = [WSOpt | conn_opts(Config)],
    {ok, Socket} = packet:do_client_connect(Connect, Connack, [{proxy_info, ProxyInfo}|ConnOpts]),
    ok = close(Socket, Config).

ws_proxy_protocol_localcommand_v1_test(Config) ->
    ProxyInfo = 
        #{version => 1, command => local,
		transport_family => ipv4,
		transport_protocol => stream,
		src_address => {127, 0, 0, 1}, src_port => 80,
		dest_address => {127, 0, 0, 1}, dest_port => 81},
    Connect = packet:gen_connect("ws_proxy_protocol_test", [{keepalive,10}]),
    Connack = packet:gen_connack(5),
    WSOpt  = {conn_opts, [{ws_protocols, ["mqtt"]}]},
    ConnOpts = [WSOpt | conn_opts(Config)],
    {ok, Socket} = packet:do_client_connect(Connect, Connack, [{proxy_info, ProxyInfo}|ConnOpts]),
    ok = close(Socket, Config).

ws_proxy_protocol_localcommand_v2_test(Config) ->
    ProxyInfo = 
        #{version => 2, command => local,
		transport_family => ipv4,
		transport_protocol => stream,
		src_address => {127, 0, 0, 1}, src_port => 80,
		dest_address => {127, 0, 0, 1}, dest_port => 81},
    Connect = packet:gen_connect("ws_proxy_protocol_test", [{keepalive,10}]),
    Connack = packet:gen_connack(5),
    WSOpt  = {conn_opts, [{ws_protocols, ["mqtt"]}]},
    ConnOpts = [WSOpt | conn_opts(Config)],
    {ok, Socket} = packet:do_client_connect(Connect, Connack, [{proxy_info, ProxyInfo}|ConnOpts]),
    ok = close(Socket, Config).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
hook_empty_client_id_proto_4(_, _RandomId, _, _, _) -> ok.
hook_uname_no_password_denied(_, {"", <<"connect-uname-test-">>}, <<"user">>, undefined, _) -> {error, invalid_credentials}.
hook_uname_password_denied(_, {"", <<"connect-uname-pwd-test">>}, <<"user">>, <<"password9">>, _) -> {error, invalid_credentials}.
hook_uname_password_success(_, {"", <<"connect-uname-pwd-test">>}, <<"user">>, <<"password9">>, _) -> ok.
hook_change_subscriber_id(_, {"", <<"change-sub-id-test">>}, _, _, _) ->
    {ok, [{subscriber_id, {"newmp", <<"changed-client-id">>}}]}.
hook_on_register_changed_subscriber_id(_, {"newmp", <<"changed-client-id">>}, _) ->
    ok.

hook_change_username(_, _, <<"old_username">>, _, _) ->
    {ok, [{username, <<"new_username">>}]}.
hook_on_register_changed_username(_, _, <<"new_username">>) ->
    ok.

hook_change_username_m5(_, _, <<"old_username">>, _, _, _) ->
    {ok, #{username => <<"new_username">>}}.

hook_on_register_changed_username_m5(_,_, <<"new_username">>, _) ->
    ok.

%% Helpers
stop_listener(Config) ->
    Port = proplists:get_value(port, Config),
    Address = proplists:get_value(address, Config),
    vmq_server_cmd:listener_stop(Port, Address, false).


close(Socket, Config) ->
    (transport(Config)):close(Socket).

transport(Config) ->
    case lists:keyfind(type, 1, Config) of
        {type, tcp} ->
            gen_tcp;
        {type, ssl} ->
            ssl;
        {type, ws} ->
            gen_tcp;
        {type, wss} ->
            ssl
    end.

conn_opts(Config) ->
    {port, Port} = lists:keyfind(port, 1, Config),
    {address, Address} = lists:keyfind(address, 1, Config),
    {type, Type} = lists:keyfind(type, 1, Config),
    TransportOpts =
        case Type of
            tcp ->
                [{transport, gen_tcp}, {conn_opts, []}];
            ssl ->
                [{transport, ssl},
                 {conn_opts,
                  [
                   {cacerts, load_cacerts()}
                  ]}];
            ws ->
                [{transport, vmq_ws_transport}, {conn_opts, []}]
        end,
    [{port, Port},{hostname, Address},
     lists:keyfind(propver, 1, Config)|TransportOpts].

load_cacerts() ->
    IntermediateCA = ssl_path("test-signing-ca.crt"),
    RootCA = ssl_path("test-root-ca.crt"),
    load_cert(RootCA) ++ load_cert(IntermediateCA).

load_cert(Cert) ->
    {ok, Bin} = file:read_file(Cert),
    case filename:extension(Cert) of
        ".der" ->
            %% no decoding necessary
            [Bin];
        _ ->
            %% assume PEM otherwise
            Contents = public_key:pem_decode(Bin),
            [DER || {Type, DER, Cipher} <-
                    Contents, Type == 'Certificate',
                    Cipher == 'not_encrypted']
    end.

start_listener(Config) ->
    {port, Port} = lists:keyfind(port, 1, Config),
    {address, Address} = lists:keyfind(address, 1, Config),
    {type, Type} = lists:keyfind(type, 1, Config),
    ProtVers = {allowed_protocol_versions, "3,4,5"},

    Opts1 =
        case Type of
            ssl ->
                [{ssl, true},
                 {nr_of_acceptors, 5},
                 {cafile, ssl_path("all-ca.crt")},
                 {certfile, ssl_path("server.crt")},
                 {keyfile, ssl_path("server.key")},
                 {tls_version, "tlsv1.2"}];
            tcp ->
                [];
            ws ->
                [{websocket,true}];
            wss -> [{ssl, true},
                 {nr_of_acceptors, 5},
                 {cafile, ssl_path("all-ca.crt")},
                 {certfile, ssl_path("server.crt")},
                 {keyfile, ssl_path("server.key")},
                 {tls_version, "tlsv1.2"},
                 {websocket, true}]
        end,
    {ok, _} = vmq_server_cmd:listener_start(Port, Address, [ProtVers | Opts1]),
    [{address, Address},{port, Port},{opts, Opts1}|Config].

ssl_path(File) ->
    Path = filename:dirname(
             proplists:get_value(source, ?MODULE:module_info(compile))),
    filename:join([Path, "ssl", File]).
