-module(vmq_ssl_tests).
-include_lib("eunit/include/eunit.hrl").

-define(setup(F), {setup, fun setup/0, fun teardown/1, F}).
-define(listener(Port), {{{127,0,0,1}, Port}, [{max_connections, infinity},
                                               {nr_of_acceptors, 10},
                                               {mountpoint, ""},
                                               {cafile, "../test/ssl/all-ca.crt"},
                                               {certfile, "../test/ssl/server.crt"},
                                               {keyfile, "../test/ssl/server.key"},
                                               {tls_version, tlsv1}]}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Tests Descriptions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
ssl_test_() ->
    [
     {"Check SSL Connection no auth",
      ?setup(fun connect_no_auth/1)}
    ,{"Check SSL Connection no auth wrong CA",
      ?setup(fun connect_no_auth_wrong_ca/1)}
    ,{"Check SSL Connection Cert Auth",
      {setup, fun setup_c/0, fun teardown/1, fun connect_cert_auth/1}}
    ,{"Check SSL Connection Cert Auth Without",
      {setup, fun setup_c/0, fun teardown/1, fun connect_cert_auth_without/1}}
    ,{"Check SSL Connection Cert Auth Expired",
      {setup, fun setup_c/0, fun teardown/1, fun connect_cert_auth_expired/1}}
    ,{"Check SSL Connection Cert Auth Revoked",
      {setup, fun setup_r/0, fun teardown/1, fun connect_cert_auth_revoked/1}}
    ,{"Check SSL Connection Cert Auth with CRL Check",
      {setup, fun setup_r/0, fun teardown/1, fun connect_cert_auth_crl/1}}
    ,{"Check SSL Connection using Identity from Cert",
      {setup, fun setup_i/0, fun teardown/1, fun connect_identity/1}}
    ,{"Check SSL Connection using Identity from Cert, but no Client Cert provided",
      {setup, fun setup_i/0, fun teardown/1, fun connect_no_identity/1}}
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Setup Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
setup() ->
    application:load(vmq_server),
    application:set_env(vmq_server, allow_anonymous, true),
    application:set_env(vmq_server, listeners,
                        {[],[?listener(1888)],[]}),
    vmq_server:start_no_auth(),
    wait_til_ready().

setup_c() ->
    application:load(vmq_server),
    application:set_env(vmq_server, allow_anonymous, true),
    application:set_env(vmq_server, listeners,
                        {[],[
                             {{{127,0,0,1}, 1888}, [{max_connections, infinity},
                                                    {nr_of_acceptors, 10},
                                                    {mountpoint, ""},
                                                    {cafile, "../test/ssl/all-ca.crt"},
                                                    {certfile, "../test/ssl/server.crt"},
                                                    {keyfile, "../test/ssl/server.key"},
                                                    {tls_version, 'tlsv1.2'},
                                                    {require_certificate, true}]}
                            ], [], []}),
    vmq_server:start_no_auth(),
    wait_til_ready().

setup_r() ->
    application:load(vmq_server),
    application:set_env(vmq_server, allow_anonymous, true),
    application:set_env(vmq_server, listeners,
                        {[],[
                             {{{127,0,0,1}, 1888}, [{max_connections, infinity},
                                                    {nr_of_acceptors, 10},
                                                    {mountpoint, ""},
                                                    {cafile, "../test/ssl/all-ca.crt"},
                                                    {certfile, "../test/ssl/server.crt"},
                                                    {keyfile, "../test/ssl/server.key"},
                                                    {tls_version, 'tlsv1.2'},
                                                    {require_certificate, true},
                                                    {crlfile, "../test/ssl/crl.pem"}]}
                            ], []}),
    vmq_server:start_no_auth(),
    wait_til_ready().

setup_i() ->
    application:load(vmq_server),
    application:set_env(vmq_server, listeners,
                        {[],[
                             {{{127,0,0,1}, 1888}, [{max_connections, infinity},
                                                    {nr_of_acceptors, 10},
                                                    {mountpoint, ""},
                                                    {cafile, "../test/ssl/all-ca.crt"},
                                                    {certfile, "../test/ssl/server.crt"},
                                                    {keyfile, "../test/ssl/server.key"},
                                                    {tls_version, 'tlsv1.2'},
                                                    {require_certificate, true},
                                                    {use_identity_as_username, true}]}
                            ], []}),
    vmq_server:start_no_auth(),
    wait_til_ready().

teardown(_) ->
    vmq_server:stop().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
connect_no_auth(_) ->
    Connect = packet:gen_connect("connect-success-test", [{keepalive, 10}]),
    Connack = packet:gen_connack(0),
    {ok, SSock} = ssl:connect("localhost", 1888,
                              [binary, {active, false}, {packet, raw},
                               {cacertfile, "../test/ssl/test-root-ca.crt"},
                               {versions, [tlsv1]}]),
    ok = ssl:send(SSock, Connect),
    ok = packet:expect_packet(ssl, SSock, "connack", Connack),
    ?_assertEqual(ok, ssl:close(SSock)).

connect_no_auth_wrong_ca(_) ->
    ?_assertEqual({error,{tls_alert,"unknown ca"}},
                  ssl:connect("localhost", 1888,
                              [binary, {active, false}, {packet, raw},
                               {verify, verify_peer},
                               {cacertfile, "../test/ssl/test-alt-ca.crt"},
                               {versions, [tlsv1]}])).

connect_cert_auth(_) ->
    Connect = packet:gen_connect("connect-success-test", [{keepalive, 10}]),
    Connack = packet:gen_connack(0),
    {ok, SSock} = ssl:connect("localhost", 1888,
                              [binary, {active, false}, {packet, raw},
                               {verify, verify_peer},
                               {cacertfile, "../test/ssl/test-root-ca.crt"},
                               {certfile, "../test/ssl/client.crt"},
                               {keyfile, "../test/ssl/client.key"}]),
    ok = ssl:send(SSock, Connect),
    ok = packet:expect_packet(ssl, SSock, "connack", Connack),
    ?_assertEqual(ok, ssl:close(SSock)).

connect_cert_auth_without(_) ->
    ?_assertEqual({error,{tls_alert,"handshake failure"}},
                  ssl:connect("localhost", 1888,
                              [binary, {active, false}, {packet, raw},
                               {verify, verify_peer},
                               {cacertfile, "../test/ssl/test-root-ca.crt"}])).

connect_cert_auth_expired(_) ->
    ?_assertEqual({error,{tls_alert,"certificate expired"}},
                  ssl:connect("localhost", 1888,
                              [binary, {active, false}, {packet, raw},
                               {verify, verify_peer},
                               {cacertfile, "../test/ssl/test-root-ca.crt"},
                               {certfile, "../test/ssl/client-expired.crt"},
                               {keyfile, "../test/ssl/client.key"}])).

connect_cert_auth_revoked(_) ->
    ?_assertEqual({error,{tls_alert,"certificate revoked"}},
                  ssl:connect("localhost", 1888,
                              [binary, {active, false}, {packet, raw},
                               {verify, verify_peer},
                               {cacertfile, "../test/ssl/test-root-ca.crt"},
                               {certfile, "../test/ssl/client-revoked.crt"},
                               {keyfile, "../test/ssl/client.key"}])).

connect_cert_auth_crl(_) ->
    Connect = packet:gen_connect("connect-success-test", [{keepalive, 10}]),
    Connack = packet:gen_connack(0),
    {ok, SSock} = ssl:connect("localhost", 1888,
                              [binary, {active, false}, {packet, raw},
                               {verify, verify_peer},
                               {cacertfile, "../test/ssl/test-root-ca.crt"},
                               {certfile, "../test/ssl/client.crt"},
                               {keyfile, "../test/ssl/client.key"}]),
    ok = ssl:send(SSock, Connect),
    ok = packet:expect_packet(ssl, SSock, "connack", Connack),
    ?_assertEqual(ok, ssl:close(SSock)).

connect_identity(_) ->
    Connect = packet:gen_connect("connect-success-test", [{keepalive, 10}]),
    Connack = packet:gen_connack(0),
    {ok, SSock} = ssl:connect("localhost", 1888,
                              [binary, {active, false}, {packet, raw},
                               {verify, verify_peer},
                               {cacertfile, "../test/ssl/test-root-ca.crt"},
                               {certfile, "../test/ssl/client.crt"},
                               {keyfile, "../test/ssl/client.key"}]),
    ok = ssl:send(SSock, Connect),
    ok = packet:expect_packet(ssl, SSock, "connack", Connack),
    ?_assertEqual(ok, ssl:close(SSock)).

connect_no_identity(_) ->
    ?_assertEqual({error,{tls_alert,"handshake failure"}},
                  ssl:connect("localhost", 1888,
                              [binary, {active, false}, {packet, raw},
                               {verify, verify_peer},
                               {cacertfile, "../test/ssl/test-root-ca.crt"}])).

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
