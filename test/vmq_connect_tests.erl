-module(vmq_connect_tests).
-include_lib("eunit/include/eunit.hrl").

-define(setup(F), {setup, fun setup/0, fun teardown/1, F}).
-define(listener(Port), {{{127,0,0,1}, Port}, [{max_connections, infinity},
                                               {nr_of_acceptors, 10},
                                               {mountpoint, ""}]}).
-export([hook_empty_client_id_proto_4/4,
         hook_uname_no_password_denied/4,
         hook_uname_password_denied/4,
         hook_uname_password_success/4]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Tests Descriptions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
connect_test_() ->
    [{"A anonymous client is not allowed to connect",
      ?setup(fun anon_denied/1)}
     ,{"A anonymous client is allowed to connect",
       ?setup(fun anon_success/1)}
     ,{"A client with an empty Client Id is not allowed to connect",
       ?setup(fun invalid_id_0/1)}
     ,{"A client can connect with a missing Client Id if Protocol Version 4",
       ?setup(fun invalid_id_0_311/1)}
     ,{"A client can connect with a missing Client Id",
       ?setup(fun invalid_id_missing/1)}
     ,{"A strict implementation doesn't allow client ids longer than 23 Characters",
       ?setup(fun invalid_id_24/1)}
     ,{"We only support Protocol Version 3,4",
       ?setup(fun invalid_protonum/1)}
     ,{"A client with a username but no password is not allowed to connect",
       ?setup(fun uname_no_password_denied/1)}
     ,{"A client with a username and wrong password is not allowed to connect",
       ?setup(fun uname_password_denied/1)}
     ,{"A client with a username and proper password is allowed to connect",
       ?setup(fun uname_password_success/1)}
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Setup Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
setup() ->
    application:load(vmq_server),
    application:set_env(vmq_server, allow_anonymous, false),
    application:set_env(vmq_server, listeners,
                        {[?listener(1888)],[],[],[]}),
    vmq_server:start_no_auth(),
    wait_til_ready().
teardown(_) -> vmq_server:stop().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
anon_denied(_) ->
    Connect = packet:gen_connect("connect-anon-test", [{keepalive,10}]),
    Connack = packet:gen_connack(5),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ?_assertEqual(ok, gen_tcp:close(Socket)).

anon_success(_) ->
    application:set_env(vmq_server, allow_anonymous, true),
    vmq_config:reset(),
    %% allow_anonymous is proxied through vmq_config.erl
    Connect = packet:gen_connect("connect-success-test", [{keepalive,10}]),
    Connack = packet:gen_connack(0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ?_assertEqual(ok, gen_tcp:close(Socket)).

invalid_id_0(_) ->
    Connect = packet:gen_connect("", [{keepalive,10}]),
    Connack = packet:gen_connack(2),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ?_assertEqual(ok, gen_tcp:close(Socket)).

invalid_id_0_311(_) ->
    Connect = packet:gen_connect("", [{keepalive,10},{proto_ver,4}]),
    Connack = packet:gen_connack(0),
    vmq_plugin_mgr:enable_module_plugin(
      auth_on_register, ?MODULE, hook_empty_client_id_proto_4, 4),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    vmq_plugin_mgr:disable_module_plugin(
      auth_on_register, ?MODULE, hook_empty_client_id_proto_4, 4),
    ?_assertEqual(ok, gen_tcp:close(Socket)).

invalid_id_missing(_) ->
    %% mosq_test.gen_connect(None, keepalive=10)
    Connect = <<16#10,16#0c,16#00,16#06,
                16#4d,16#51,16#49,16#73,
                16#64,16#70,16#03,16#02,
                16#00,16#0a>>,
    ?_assertEqual({error, closed}, packet:do_client_connect(Connect, <<>>, [])).

invalid_id_24(_) ->
    Connect = packet:gen_connect("connect-invalid-id-test-", [{keepalive,10}]),
    Connack = packet:gen_connack(2), %% client id longer than 23 Characters
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ?_assertEqual(ok, gen_tcp:close(Socket)).

invalid_protonum(_) ->
    %% mosq_test.gen_connect("test", keepalive=10, proto_ver=0)
    Connect = <<16#10,16#12,16#00,16#06,16#4d,16#51,16#49,16#73,
                16#64,16#70,16#00,16#02,16#00,16#0a,16#00,16#04,
                16#74,16#65,16#73,16#74>>,
    Connack = packet:gen_connack(1),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ?_assertEqual(ok, gen_tcp:close(Socket)).

uname_no_password_denied(_) ->
    Connect = packet:gen_connect("connect-uname-test-", [{keepalive,10}, {username, "user"}]),
    Connack = packet:gen_connack(4),
    ok = vmq_plugin_mgr:enable_module_plugin(
      auth_on_register, ?MODULE, hook_uname_no_password_denied, 4),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = vmq_plugin_mgr:disable_module_plugin(
      auth_on_register, ?MODULE, hook_uname_no_password_denied, 4),
    ?_assertEqual(ok, gen_tcp:close(Socket)).

uname_password_denied(_) ->
    Connect = packet:gen_connect("connect-uname-pwd-test", [{keepalive,10}, {username, "user"},
                                                            {password, "password9"}]),
    Connack = packet:gen_connack(4),
    ok = vmq_plugin_mgr:enable_module_plugin(
      auth_on_register, ?MODULE, hook_uname_password_denied, 4),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = vmq_plugin_mgr:disable_module_plugin(
      auth_on_register, ?MODULE, hook_uname_password_denied, 4),
    ?_assertEqual(ok, gen_tcp:close(Socket)).

uname_password_success(_) ->
    Connect = packet:gen_connect("connect-uname-pwd-test", [{keepalive,10}, {username, "user"},
                                                            {password, "password9"}]),
    Connack = packet:gen_connack(0),
    vmq_plugin_mgr:enable_module_plugin(
      auth_on_register, ?MODULE, hook_uname_password_success, 4),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    vmq_plugin_mgr:disable_module_plugin(
      auth_on_register, ?MODULE, hook_uname_password_success, 4),
    ?_assertEqual(ok, gen_tcp:close(Socket)).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
hook_empty_client_id_proto_4(_, _RandomId, undefined, undefined) -> ok.
hook_uname_no_password_denied(_,"connect-uname-test-", "user", undefined) -> {error, invalid_credentials}.
hook_uname_password_denied(_,"connect-uname-pwd-test", "user", "password9") -> {error, invalid_credentials}.
hook_uname_password_success(_,"connect-uname-pwd-test", "user", "password9") -> ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Helper
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
wait_til_ready() ->
    wait_til_ready(vmq_cluster:is_ready(), 100).
wait_til_ready(true, _) -> ok;
wait_til_ready(false, I) when I > 0 ->
    timer:sleep(5),
    wait_til_ready(vmq_cluster:is_ready(), I - 1);
wait_til_ready(_, _) -> exit(not_ready).
