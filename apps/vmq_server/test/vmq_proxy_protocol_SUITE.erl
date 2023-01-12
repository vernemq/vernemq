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
         proxy_use_cn_as_username_off/1]).

-export([hook_proxy_register/5,
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
    vmq_server_cmd:set_config(max_client_id_size, 23),
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
    Config.

end_per_testcase(_, Config) ->
    vmq_test_utils:teardown(),
    Config.

all() ->
    [proxy_test,
     proxy_local_command_test,
     proxy_use_cn_as_username_on,
     proxy_use_cn_as_username_off].

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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
hook_proxy_register({{1,1,1,1}, 1234}, _, _, _, _) -> ok.

hook_proxy_register_use_identity_as_username_on({{1,2,3,4},5555},{[], <<"connect-proxy-test">>},<<"sni_hostname">>,_,_) ->
    ok.

hook_proxy_register_use_identity_as_username_off({{2,3,4,5},6666},{[], <<"connect-proxy-test">>},<<"username">>,_,_) ->
    ok.
