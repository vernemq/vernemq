-module(vmq_proxy_protocol_SUITE).
-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([proxy_test/1,
          proxy_local_command_test/1,
          proxy_use_cn_as_username_on/1,
          proxy_use_cn_as_username_off/1,
          proxy_forward_connection_opts_test/1,
          proxy_protocol_v2_client_cert_metadata_test/1,
          proxy_trusted_proxy_test/1,
          proxy_untrusted_proxy_test/1,
          tls_proxy_protocol_v2_client_cert_metadata_test/1,
          tls_proxy_protocol_v2_untrusted_proxy_test/1,
          ws_proxy_protocol_v2_use_cn_as_username_on_test/1,
          ws_proxy_protocol_v2_use_cn_as_username_off_test/1]).

-export([hook_proxy_register/5,
          hook_proxy_register_with_metadata/6,
          hook_proxy_register_with_extended_metadata/6,
          hook_tls_proxy_register_with_metadata/6,
          hook_proxy_register_use_identity_as_username_on/5,
          hook_proxy_register_use_identity_as_username_off/5]).

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(_Config) ->
    cover:start(),
    _Config.

end_per_suite(_Config) ->
    _Config.

init_per_testcase(_Case, Config) ->
    vmq_test_utils:setup(),
    vmq_server_cmd:set_config(allow_anonymous, false),
    vmq_server_cmd:set_config(max_client_id_size, 50),
    vmq_server_cmd:listener_start(1888, [{proxy_protocol, true},
                                         {proxy_protocol_use_cn_as_username, false}]),
    vmq_server_cmd:listener_start(1889, [{proxy_protocol, true}
                                         %% proxy_protocol_use_cn_as_username
                                         %% defaults to true as this
                                         %% was the default behaviour
                                         %% before the setting was
                                         %% introduced.
                                         %% {proxy_protocol_use_cn_as_username, true}
                                        ]),
    vmq_server_cmd:listener_start(1893, "127.0.0.1", [
        {proxy_protocol, true},
        {proxy_protocol_use_cn_as_username, false},
        {forward_connection_opts, true}
    ]),
    vmq_server_cmd:listener_start(1896, "127.0.0.1", [
        {proxy_protocol, true},
        {proxy_protocol_trusted_proxy, "127.0.0.1"}
    ]),
    vmq_server_cmd:listener_start(1897, "127.0.0.1", [
        {proxy_protocol, true},
        {proxy_protocol_trusted_proxy, "127.0.0.2"}
    ]),
    vmq_server_cmd:listener_start(1894, "127.0.0.1", [
        {ssl, true},
        {proxy_protocol, true},
        {proxy_protocol_trusted_proxy, "127.0.0.1"},
        {proxy_protocol_use_cn_as_username, true},
        {forward_connection_opts, true},
        {cafile, ssl_path("all-ca.crt")},
        {certfile, ssl_path("server.crt")},
        {keyfile, ssl_path("server.key")},
        {tls_version, "tlsv1.2"}
    ]),
    vmq_server_cmd:listener_start(1895, "127.0.0.1", [
        {ssl, true},
        {proxy_protocol, true},
        {proxy_protocol_trusted_proxy, "127.0.0.2"},
        {cafile, ssl_path("all-ca.crt")},
        {certfile, ssl_path("server.crt")},
        {keyfile, ssl_path("server.key")},
        {tls_version, "tlsv1.2"}
    ]),
   vmq_server_cmd:listener_start(1891, "127.0.0.1", [{websocket,true}, {proxy_protocol, true},
                                        {proxy_protocol_use_cn_as_username, true},
                                        {allowed_protocol_versions, "3,4,5"}
                                       ]),
   vmq_server_cmd:listener_start(1892, "127.0.0.1", [{websocket,true}, {proxy_protocol, true},
                                      {proxy_protocol_use_cn_as_username, false},
                                      {allowed_protocol_versions, "3,4,5"}
                                     ]),
    Config.

end_per_testcase(_, Config) ->
    vmq_test_utils:teardown(),
    Config.

all() ->
    [proxy_test,
     proxy_local_command_test,
     proxy_use_cn_as_username_on,
     proxy_use_cn_as_username_off,
     proxy_forward_connection_opts_test,
     proxy_protocol_v2_client_cert_metadata_test,
     proxy_trusted_proxy_test,
     proxy_untrusted_proxy_test,
     tls_proxy_protocol_v2_client_cert_metadata_test,
     tls_proxy_protocol_v2_untrusted_proxy_test,
     ws_proxy_protocol_v2_use_cn_as_username_on_test,
     ws_proxy_protocol_v2_use_cn_as_username_off_test].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
proxy_test(_) ->
    Connect = packet:gen_connect("connect-proxy-test", [{keepalive,10}]),
    Connack = packet:gen_connack(0),
    Host = {127,0,0,1},
    Port = 1888,
    vmq_plugin_mgr:enable_module_plugin(
      auth_on_register, ?MODULE, hook_proxy_register, 5),
    ProxyInfo =
        #{version => 2,
          src_address => {1,1,1,1},
          src_port => 1234,
          dest_address => {2,2,2,2},
          dest_port => 4321,
          command => proxy,
          transport_family => ipv4,
          transport_protocol => stream},
    {ok, Socket} = gen_tcp:connect(Host, Port, [binary, {reuseaddr, true},
                                                {active, false}, {packet, raw}]),
    ok = gen_tcp:send(Socket, ranch_proxy_header:header(ProxyInfo)),
    ok = gen_tcp:send(Socket, Connect),
    ok = packet:expect_packet(Socket, connack, Connack),
    vmq_plugin_mgr:disable_module_plugin(
      auth_on_register, ?MODULE, hook_proxy_register, 5),
    ok = gen_tcp:close(Socket).

proxy_local_command_test(_) ->
    Connect = packet:gen_connect("connect-proxy-local-test", [{keepalive,10}]),
    Host = {127,0,0,1},
    Port = 1888,
    vmq_plugin_mgr:enable_module_plugin(
      auth_on_register, ?MODULE, hook_proxy_register, 5),
    ProxyInfo =
        #{version => 2,
          src_address => {1,1,1,1},
          src_port => 1234,
          dest_address => {2,2,2,2},
          dest_port => 4321,
          command => local,
          transport_family => ipv4,
          transport_protocol => stream},
    {ok, Socket} = gen_tcp:connect(Host, Port, [binary, {reuseaddr, true},
                                                {active, false}, {packet, raw}]),
    ok = gen_tcp:send(Socket, ranch_proxy_header:header(ProxyInfo)),
    ok = gen_tcp:send(Socket, Connect),
    % don't wait for a Connack here, as this doesn't go up to MQTT level
    vmq_plugin_mgr:disable_module_plugin(
      auth_on_register, ?MODULE, hook_proxy_register, 5),
    ok = gen_tcp:close(Socket).

proxy_use_cn_as_username_on(_) ->
    Connect = packet:gen_connect("connect-proxy-test", [{keepalive,10},
                                                        {username, <<"username">>},
                                                        {password, <<"password">>}]),
    Connack = packet:gen_connack(0),
    Host = {127,0,0,1},
    Port = 1889,
    vmq_plugin_mgr:enable_module_plugin(
      auth_on_register, ?MODULE, hook_proxy_register_use_identity_as_username_on, 5),
    {ok, Socket} = gen_tcp:connect(Host, Port,
                                   [binary, {active, false}, {packet, raw}]),
    ProxyInfo =
        #{version => 2,
          src_address => {1,2,3,4},
          src_port => 5555,
          dest_address => {6,7,8,9},
          dest_port => 10101,
          command => proxy,
          ssl => #{
                   client => [ssl],
                   verified => true,
                   version => <<"tlsv1.2">>,
                   cn => <<"sni_hostname">>},
          transport_family => ipv4,
          transport_protocol => stream},
    ok = gen_tcp:send(Socket, ranch_proxy_header:header(ProxyInfo)),
    gen_tcp:send(Socket, Connect),
    ok = packet:expect_packet(Socket, connack, Connack),
    vmq_plugin_mgr:disable_module_plugin(
      auth_on_register, ?MODULE, hook_proxy_register_use_identity_as_username_on, 5),
    ok = gen_tcp:close(Socket).

proxy_use_cn_as_username_off(_) ->
    Connect = packet:gen_connect("connect-proxy-test", [{keepalive,10},
                                                        {username, <<"username">>},
                                                        {password, <<"password">>}]),
    Connack = packet:gen_connack(0),
    Host = {127,0,0,1},
    Port = 1888,
    vmq_plugin_mgr:enable_module_plugin(
      auth_on_register, ?MODULE, hook_proxy_register_use_identity_as_username_off, 5),
    {ok, Socket} = gen_tcp:connect(Host, Port,
                                   [binary, {active, false}, {packet, raw}]),
    ProxyInfo =
        #{version => 2,
          src_address => {2,3,4,5},
          src_port => 6666,
          dest_address => {7,8,9, 10},
          dest_port => 11111,
          command => proxy,
          ssl => #{
                   client => [ssl],
                   verified => true,
                   version => <<"tlsv1.2">>,
                   cn => <<"sni_hostname">>},
          transport_family => ipv4,
          transport_protocol => stream},
    ok = gen_tcp:send(Socket, ranch_proxy_header:header(ProxyInfo)),
    gen_tcp:send(Socket, Connect),
    ok = packet:expect_packet(Socket, connack, Connack),
    vmq_plugin_mgr:disable_module_plugin(
      auth_on_register, ?MODULE, hook_proxy_register_use_identity_as_username_off, 5),
    ok = gen_tcp:close(Socket).

proxy_forward_connection_opts_test(_) ->
    Connect = packet:gen_connect("connect-proxy-metadata-test", [{keepalive,10}]),
    Connack = packet:gen_connack(0),
    Host = {127,0,0,1},
    Port = 1893,
    vmq_plugin_mgr:enable_module_plugin(
      auth_on_register, ?MODULE, hook_proxy_register_with_metadata, 6),
    ProxyInfo =
        #{version => 2,
          src_address => {1,1,1,1},
          src_port => 1234,
          dest_address => {2,2,2,2},
          dest_port => 4321,
          command => proxy,
          transport_family => ipv4,
          transport_protocol => stream},
    {ok, Socket} = gen_tcp:connect(Host, Port,
                                    [binary, {active, false}, {packet, raw}]),
    ok = gen_tcp:send(Socket, ranch_proxy_header:header(ProxyInfo)),
    ok = gen_tcp:send(Socket, Connect),
    ok = packet:expect_packet(Socket, connack, Connack),
    vmq_plugin_mgr:disable_module_plugin(
      auth_on_register, ?MODULE, hook_proxy_register_with_metadata, 6),
    ok = gen_tcp:close(Socket).

proxy_protocol_v2_client_cert_metadata_test(_) ->
    ClientCert = load_cert("client.crt"),
    Connect = packet:gen_connect("connect-proxy-v2-client-cert-metadata-test", [{keepalive,10}]),
    Connack = packet:gen_connack(0),
    Host = {127,0,0,1},
    Port = 1893,
    vmq_plugin_mgr:enable_module_plugin(
      auth_on_register, ?MODULE, hook_proxy_register_with_extended_metadata, 6),
    {ok, Socket} = gen_tcp:connect(Host, Port,
                                    [binary, {active, false}, {packet, raw}]),
    ok = gen_tcp:send(Socket, proxy_v2_ssl_header(ClientCert, Port)),
    ok = gen_tcp:send(Socket, Connect),
    ok = packet:expect_packet(Socket, connack, Connack),
    vmq_plugin_mgr:disable_module_plugin(
      auth_on_register, ?MODULE, hook_proxy_register_with_extended_metadata, 6),
    ok = gen_tcp:close(Socket).

proxy_trusted_proxy_test(_) ->
    Connect = packet:gen_connect("connect-proxy-test", [{keepalive,10}]),
    Connack = packet:gen_connack(0),
    Host = {127,0,0,1},
    Port = 1896,
    vmq_plugin_mgr:enable_module_plugin(
      auth_on_register, ?MODULE, hook_proxy_register, 5),
    ProxyInfo = #{
        version => 2,
        src_address => {1,1,1,1},
        src_port => 1234,
        dest_address => {2,2,2,2},
        dest_port => 4321,
        command => proxy,
        transport_family => ipv4,
        transport_protocol => stream
    },
    {ok, Socket} = gen_tcp:connect(Host, Port, [binary, {active, false}, {packet, raw}]),
    ok = gen_tcp:send(Socket, ranch_proxy_header:header(ProxyInfo)),
    ok = gen_tcp:send(Socket, Connect),
    ok = packet:expect_packet(Socket, connack, Connack),
    vmq_plugin_mgr:disable_module_plugin(
      auth_on_register, ?MODULE, hook_proxy_register, 5),
    ok = gen_tcp:close(Socket).

proxy_untrusted_proxy_test(_) ->
    Host = {127,0,0,1},
    Port = 1897,
    ProxyInfo = #{
        version => 2,
        src_address => {1,1,1,1},
        src_port => 1234,
        dest_address => {2,2,2,2},
        dest_port => 4321,
        command => proxy,
        transport_family => ipv4,
        transport_protocol => stream
    },
    {ok, Socket} = gen_tcp:connect(Host, Port, [binary, {active, false}, {packet, raw}]),
    ok = gen_tcp:send(Socket, ranch_proxy_header:header(ProxyInfo)),
    {error, closed} = gen_tcp:recv(Socket, 0, 6000).

tls_proxy_protocol_v2_client_cert_metadata_test(_) ->
    ClientCert = load_cert("client.crt"),
    Connect = packet:gen_connect("connect-tls-proxy-metadata-test", [
        {keepalive, 10},
        {username, <<"username">>},
        {password, <<"password">>}
    ]),
    Connack = packet:gen_connack(0),
    Host = {127,0,0,1},
    Port = 1894,
    vmq_plugin_mgr:enable_module_plugin(
        auth_on_register, ?MODULE, hook_tls_proxy_register_with_metadata, 6),
    {ok, Socket} = gen_tcp:connect(Host, Port, [binary, {active, false}, {packet, raw}]),
    ProxyHeader = proxy_v2_ssl_header(ClientCert),
    ok = gen_tcp:send(Socket, ProxyHeader),
    {ok, SSock} = ssl:connect(
        Socket,
        [binary, {active, false}, {packet, raw}, {verify, verify_none}, {versions, ['tlsv1.2']}],
        6000
    ),
    ok = ssl:send(SSock, Connect),
    ok = packet:expect_packet(ssl, SSock, connack, Connack),
    vmq_plugin_mgr:disable_module_plugin(
        auth_on_register, ?MODULE, hook_tls_proxy_register_with_metadata, 6),
    ok = ssl:close(SSock).

tls_proxy_protocol_v2_untrusted_proxy_test(_) ->
    ClientCert = load_cert("client.crt"),
    Host = {127,0,0,1},
    Port = 1895,
    {ok, Socket} = gen_tcp:connect(Host, Port, [binary, {active, false}, {packet, raw}]),
    ok = gen_tcp:send(Socket, proxy_v2_ssl_header(ClientCert, Port)),
    {error, closed} = ssl:connect(
        Socket,
        [binary, {active, false}, {packet, raw}, {verify, verify_none}, {versions, ['tlsv1.2']}],
        6000
    ).

ws_proxy_protocol_v2_use_cn_as_username_on_test(_) ->
  Connect = packet:gen_connect("connect-proxy-test", [{keepalive,60},{username, <<"username">>},
    {password, <<"password">>}]),
  Connack = packet:gen_connack(0),
  Host = {127,0,0,1},
  Port = 1891,

  vmq_plugin_mgr:enable_module_plugin(
    auth_on_register, ?MODULE, hook_proxy_register_use_identity_as_username_on, 5),

  ProxyInfo =
    #{version => 2,
      src_address => {1,2,3,4},
      src_port => 5555,
      dest_address => {6,7,8,9},
      dest_port => 10151,
      command => proxy,
      ssl => #{
               client => [ssl],
               verified => true,
               version => <<"tlsv1.2">>,
               cn => <<"sni_hostname">>},
      transport_family => ipv4,
      transport_protocol => stream},
  WSProtocols = ["mqtt", "mqtt3.1"],
  {ok, Socket} = vmq_ws_transport:connect(Host, Port,
                                 [binary, {active, false}, {packet, raw}, {proxy_info, ProxyInfo},
                                  {ws_protocols, WSProtocols}], 6000),

  vmq_ws_transport:send(Socket, Connect),
  ok = packet:expect_packet(vmq_ws_transport, Socket, connack, Connack),

  vmq_plugin_mgr:disable_module_plugin(
        auth_on_register, ?MODULE, hook_proxy_register_use_identity_as_username_on, 5),
  ok = gen_tcp:close(Socket).



  ws_proxy_protocol_v2_use_cn_as_username_off_test(_) ->
    Connect = packet:gen_connect("connect-proxy-test", [{keepalive,60},{username, <<"username">>},
      {password, <<"password">>}]),
    Connack = packet:gen_connack(0),
    Host = {127,0,0,1},
    Port = 1892,

    vmq_plugin_mgr:enable_module_plugin(
      auth_on_register, ?MODULE, hook_proxy_register_use_identity_as_username_off, 5),
      
    ProxyInfo =
    #{version => 2,
      src_address => {2,3,4,5},
      src_port => 6666,
      dest_address => {6,7,8,9},
      dest_port => 10151,
      command => proxy,
      ssl => #{
               client => [ssl],
               verified => true,
               version => <<"tlsv1.2">>,
               cn => <<"sni_hostname">>},
      transport_family => ipv4,
      transport_protocol => stream},
    WSProtocols = ["mqtt", "mqtt3.1"],
    {ok, Socket} = vmq_ws_transport:connect(Host, Port,
                                 [binary, {active, false}, {packet, raw}, {proxy_info, ProxyInfo},
                                  {ws_protocols, WSProtocols}], 6000),

    vmq_ws_transport:send(Socket, Connect),
    ok = packet:expect_packet(vmq_ws_transport, Socket, connack, Connack),
    vmq_plugin_mgr:disable_module_plugin(
         auth_on_register, ?MODULE, hook_proxy_register_use_identity_as_username_off, 5),
    ok = gen_tcp:close(Socket).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
hook_proxy_register({{1,1,1,1}, 1234}, _, _, _, _) -> ok.

hook_proxy_register_with_metadata(
    {{1,1,1,1}, 1234},
    {[], <<"connect-proxy-metadata-test">>},
    _,
    _,
    _,
    #{
        listener_addr := {127,0,0,1},
        listener_port := 1893,
        listener_type := mqtt,
        proxy_protocol := #{
            version := 2,
            command := proxy,
            src_address := {1,1,1,1},
            src_port := 1234,
            dest_address := {2,2,2,2},
            dest_port := 4321
        }
    }
) ->
    ok.

hook_proxy_register_with_extended_metadata(
    {{3,4,5,6}, 7777},
    {[], <<"connect-proxy-v2-client-cert-metadata-test">>},
    _,
    _,
    _,
    #{
        listener_addr := {127,0,0,1},
        listener_port := 1893,
        listener_type := mqtt,
        proxy_protocol := #{
            raw_tlvs := [{16#ea, <<1, "test-id">>}],
            ssl := #{
                cn := <<"proxy client">>,
                client_cert := ClientCert,
                group := <<"secp256r1">>,
                sig_scheme := <<"rsa_pss_rsae_sha256">>,
                raw_tlvs := [{16#29, <<"future-ssl-tlv">>}]
            }
        }
    }
) ->
    ClientCert = load_cert("client.crt"),
    ok.

hook_tls_proxy_register_with_metadata(
    {{3,4,5,6}, 7777},
    {[], <<"connect-tls-proxy-metadata-test">>},
    <<"proxy client">>,
    _,
    _,
    #{
        listener_addr := {127,0,0,1},
        listener_port := 1894,
        listener_type := mqtts,
        proxy_protocol := #{
            raw_tlvs := [{16#ea, <<1, "test-id">>}],
            ssl := #{
                cn := <<"proxy client">>,
                client_cert := ClientCert,
                group := <<"secp256r1">>,
                sig_scheme := <<"rsa_pss_rsae_sha256">>,
                raw_tlvs := [{16#29, <<"future-ssl-tlv">>}]
            }
        }
    }
) when is_binary(ClientCert) ->
    ClientCert = load_cert("client.crt"),
    ok.

hook_proxy_register_use_identity_as_username_on({{1,2,3,4},5555},{[], <<"connect-proxy-test">>},<<"sni_hostname">>,_,_) ->
    ok.

hook_proxy_register_use_identity_as_username_off({{2,3,4,5},6666},{[], <<"connect-proxy-test">>},<<"username">>,_,_) ->
    ok.

proxy_v2_ssl_header(ClientCert) ->
    proxy_v2_ssl_header(ClientCert, 1894).

proxy_v2_ssl_header(ClientCert, DestPort) ->
    CN = <<"proxy client">>,
    Version = <<"TLSv1.2">>,
    Cipher = <<"ECDHE-RSA-AES256-GCM-SHA384">>,
    Group = <<"secp256r1">>,
    SigScheme = <<"rsa_pss_rsae_sha256">>,
    SSLSubTLVs = [
        tlv(16#21, Version),
        tlv(16#22, CN),
        tlv(16#23, Cipher),
        tlv(16#26, Group),
        tlv(16#27, SigScheme),
        tlv(16#28, ClientCert),
        tlv(16#29, <<"future-ssl-tlv">>)
    ],
    SSLSubTLVsLen = iolist_size(SSLSubTLVs),
    SSLTLV = [<<16#20, (SSLSubTLVsLen + 5):16, 3, 0:32>>, SSLSubTLVs],
    TopLevelCustomTLV = tlv(16#ea, <<1, "test-id">>),
    Addresses = <<3,4,5,6, 7,8,9,10, 7777:16, DestPort:16>>,
    Len = byte_size(Addresses) + iolist_size(SSLTLV) + iolist_size(TopLevelCustomTLV),
    [<<"\r\n\r\n\0\r\nQUIT\n", 2:4, 1:4, 1:4, 1:4, Len:16>>, Addresses, SSLTLV, TopLevelCustomTLV].

tlv(Type, Value) ->
    <<Type, (byte_size(Value)):16, Value/binary>>.

load_cert(File) ->
    {ok, Bin} = file:read_file(ssl_path(File)),
    [{_, DER, _}] = public_key:pem_decode(Bin),
    DER.

ssl_path(File) ->
    Path = filename:dirname(
        proplists:get_value(source, ?MODULE:module_info(compile))
    ),
    filename:join([Path, "ssl", File]).
