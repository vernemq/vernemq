-module(vmq_ssl_SUITE).
-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([connect_no_auth_test/1,
         connect_no_auth_wrong_ca_test/1,
         connect_cert_auth_test/1,
         connect_cert_auth_without_test/1,
         connect_cert_auth_expired_test/1,
         connect_cert_auth_revoked_test/1,
         connect_cert_auth_crl_test/1,
         connect_identity_test/1,
         connect_no_identity_test/1,
         connect_identity_allow_anonymous_override_on_test/1,
         connect_identity_allow_anonymous_override_off_test/1,
         connect_psk_test/1,
         connect_psk_wrong_identity/1,
         connect_no_auth_test_passwd/1]).

-export([hook_preauth_success/5]).

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(_Config) ->
    cover:start(),
    _Config.

end_per_suite(_Config) ->
    _Config.

init_per_testcase(Case, Config) ->
    vmq_test_utils:setup(),
    case {lists:member(Case, all_no_auth()),
          lists:member(Case, all_cert_auth()),
          lists:member(Case, all_cert_auth_tlsv1_3()),
          lists:member(Case, all_cert_auth_revoked()),
          lists:member(Case, all_cert_auth_revoked_tlsv1_3()),
          lists:member(Case, all_cert_auth_identity()),
          lists:member(Case, all_cert_auth_identity_tlsv1_3()),
          lists:member(Case, all_cert_auth_identity_allow_anonymous_override_on()),
          lists:member(Case, all_cert_auth_identity_allow_anonymous_override_off()),
          lists:member(Case, all_psk_auth()),
          lists:member(Case, all_no_auth_encrypted_keyfile())} of
        {true, _, _, _, _, _, _, _, _, _, _} ->
            {ok, _} = vmq_server_cmd:set_config(allow_anonymous, true),
            {ok, _} = vmq_server_cmd:listener_start(1888, [{ssl, true},
                                                           {nr_of_acceptors, 5},
                                                           {cafile, ssl_path("all-ca.crt")},
                                                           {certfile, ssl_path("server.crt")},
                                                           {keyfile, ssl_path("server.key")},
                                                           {tls_version, "tlsv1.2"}]);
        {_, true, _, _, _, _, _, _, _, _, _} ->
            {ok, _} = vmq_server_cmd:set_config(allow_anonymous, true),
            {ok, _} = vmq_server_cmd:listener_start(1888, [{ssl, true},
                                                           {nr_of_acceptors, 5},
                                                           {cafile, ssl_path("all-ca.crt")},
                                                           {certfile, ssl_path("server.crt")},
                                                           {keyfile, ssl_path("server.key")},
                                                           {tls_version, "tlsv1.2"},
                                                           {require_certificate, true}]);
        {_, _, true, _, _, _, _, _, _, _, _} ->
            {ok, _} = vmq_server_cmd:set_config(allow_anonymous, true),
            {ok, _} = vmq_server_cmd:listener_start(1888, [{ssl, true},
                                                           {nr_of_acceptors, 5},
                                                           {cafile, ssl_path("all-ca.crt")},
                                                           {certfile, ssl_path("server.crt")},
                                                           {keyfile, ssl_path("server.key")},
                                                           {tls_version, "tlsv1.3"},
                                                           {require_certificate, true}]);
        {_, _, _, true, _, _, _, _, _, _, _} ->
            {ok, _} = vmq_server_cmd:set_config(allow_anonymous, true),
            {ok, _} = vmq_server_cmd:listener_start(1888, [{ssl, true},
                                                           {nr_of_acceptors, 5},
                                                           {cafile, ssl_path("all-ca.crt")},
                                                           {certfile, ssl_path("server.crt")},
                                                           {keyfile, ssl_path("server.key")},
                                                           {tls_version, "tlsv1.2"},
                                                           {require_certificate, true},
                                                           {crlfile, ssl_path("crl.pem")}]);
        {_, _, _, _, true, _, _, _, _, _, _} ->
            {ok, _} = vmq_server_cmd:set_config(allow_anonymous, true),
            {ok, _} = vmq_server_cmd:listener_start(1888, [{ssl, true},
                {nr_of_acceptors, 5},
                {cafile, ssl_path("all-ca.crt")},
                {certfile, ssl_path("server.crt")},
                {keyfile, ssl_path("server.key")},
                {tls_version, "tlsv1.3"},
                {require_certificate, true},
                {crlfile, ssl_path("crl.pem")}]);
        {_, _, _, _, _, true, _, _, _, _, _} ->
            {ok, _} = vmq_server_cmd:set_config(allow_anonymous, false),
            {ok, _} = vmq_server_cmd:listener_start(1888, [{ssl, true},
                                                           {nr_of_acceptors, 5},
                                                           {cafile, ssl_path("all-ca.crt")},
                                                           {certfile, ssl_path("server.crt")},
                                                           {keyfile, ssl_path("server.key")},
                                                           {tls_version, "tlsv1.2"},
                                                           {require_certificate, true},
                                                           {crlfile, ssl_path("crl.pem")},
                                                           {use_identity_as_username, true}]),
            vmq_plugin_mgr:enable_module_plugin(
              auth_on_register, ?MODULE, hook_preauth_success, 5);
        {_, _, _, _, _, _, true, _, _, _, _} ->
            {ok, _} = vmq_server_cmd:set_config(allow_anonymous, false),
            {ok, _} = vmq_server_cmd:listener_start(1888, [{ssl, true},
                {nr_of_acceptors, 5},
                {cafile, ssl_path("all-ca.crt")},
                {certfile, ssl_path("server.crt")},
                {keyfile, ssl_path("server.key")},
                {tls_version, "tlsv1.3"},
                {require_certificate, true},
                {crlfile, ssl_path("crl.pem")},
                {use_identity_as_username, true}]),
            vmq_plugin_mgr:enable_module_plugin(
                auth_on_register, ?MODULE, hook_preauth_success, 5);
        {_, _, _, _, _, _, _, true, _, _, _} ->
                {ok, _} = vmq_server_cmd:set_config(allow_anonymous, false),
                {ok, _} = vmq_server_cmd:listener_start(1888, [{ssl, true},
                                                               {nr_of_acceptors, 5},
                                                               {allow_anonymous_override, true},
                                                               {cafile, ssl_path("all-ca.crt")},
                                                               {certfile, ssl_path("server.crt")},
                                                               {keyfile, ssl_path("server.key")},
                                                               {tls_version, "tlsv1.2"},
                                                               {require_certificate, true},
                                                               {crlfile, ssl_path("crl.pem")},
                                                               {use_identity_as_username, true}]);
        {_, _, _, _, _, _, _, _, true, _, _} ->
                {ok, _} = vmq_server_cmd:set_config(allow_anonymous, false),
                {ok, _} = vmq_server_cmd:listener_start(1888, [{ssl, true},
                                                               {nr_of_acceptors, 5},
                                                               {allow_anonymous_override, false},
                                                               {cafile, ssl_path("all-ca.crt")},
                                                               {certfile, ssl_path("server.crt")},
                                                               {keyfile, ssl_path("server.key")},
                                                               {tls_version, "tlsv1.2"},
                                                               {require_certificate, true},
                                                               {crlfile, ssl_path("crl.pem")},
                                                               {use_identity_as_username, true}]);
        {_, _, _, _, _, _, _, _, _, true, _} ->
            {ok, _} = vmq_server_cmd:set_config(allow_anonymous, true),
            {ok, _} = vmq_server_cmd:listener_start(1888, [{ssl, true},
                                                           {nr_of_acceptors, 5},
                                                           {psk_support, true},
                                                           {pskfile, ssl_path("test-psk.psk")},
                                                           {tls_version, "tlsv1.2"}]);
        {_, _, _, _, _, _, _, _, _, _, true} ->
            {ok, _} = vmq_server_cmd:set_config(allow_anonymous, true),
            {ok, _} = vmq_server_cmd:listener_start(1888, [{ssl, true},
                {nr_of_acceptors, 5},
                {cafile, ssl_path("all-ca.crt")},
                {certfile, ssl_path("server.crt")},
                {keyfile, ssl_path("server.encrypted.key")},
                {keypasswd, "VerneMQ123"},
                {tls_version, "tlsv1.2"}]),
            {ok, _} = vmq_server_cmd:listener_start(1889, [{ssl, true},
                {nr_of_acceptors, 5},
                {cafile, ssl_path("all-ca.crt")},
                {certfile, ssl_path("server.crt")},
                {keyfile, ssl_path("server.encrypted.key")},
                {keypasswd, "VerneMQ123Wrong"},
                {tls_version, "tlsv1.2"}])
    end,
    Config.

end_per_testcase(_, Config) ->
    vmq_test_utils:teardown(),
    Config.

all() ->
    all_no_auth()
    ++ all_cert_auth()
    ++ all_cert_auth_tlsv1_3()
    ++ all_cert_auth_revoked()
    ++ all_cert_auth_revoked_tlsv1_3()
    ++ all_cert_auth_identity()
    ++ all_cert_auth_identity_tlsv1_3()
    ++ all_cert_auth_identity_allow_anonymous_override_on()
    ++ all_cert_auth_identity_allow_anonymous_override_off()
    ++ all_psk_auth()
    ++ all_no_auth_encrypted_keyfile().

all_no_auth() ->
    [connect_no_auth_test,
     connect_no_auth_wrong_ca_test].

all_cert_auth() ->
    [connect_cert_auth_test,
     connect_cert_auth_without_test,
     connect_cert_auth_expired_test].

all_cert_auth_tlsv1_3() ->
    [connect_cert_auth_test,
     connect_cert_auth_without_test,
     connect_cert_auth_expired_test].

all_cert_auth_revoked() ->
    [connect_cert_auth_revoked_test,
     connect_cert_auth_crl_test].

all_cert_auth_revoked_tlsv1_3() ->
    [connect_cert_auth_revoked_test,
     connect_cert_auth_crl_test].

all_cert_auth_identity() ->
    [connect_identity_test,
     connect_no_identity_test].

all_cert_auth_identity_tlsv1_3() ->
    [connect_identity_test,
     connect_no_identity_test].

all_cert_auth_identity_allow_anonymous_override_on() ->
        [connect_identity_allow_anonymous_override_on_test].

all_cert_auth_identity_allow_anonymous_override_off() ->
        [connect_identity_allow_anonymous_override_on_test].

all_psk_auth() ->
  [connect_psk_test,
   connect_psk_wrong_identity].

all_no_auth_encrypted_keyfile() ->
    [connect_no_auth_test_passwd].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

connect_psk_test(_) ->
    Connect = packet:gen_connect("connect-success-test", [{keepalive, 10}]),
    Connack = packet:gen_connack(0),
    {ok, SSock} = ssl:connect("localhost", 1888,
                               [binary, {active, false}, {packet, raw},
                               {psk_identity, "psk_identity_1"},
                               {versions, ['tlsv1.2']},
                               {protocol, tls},
                               {verify, verify_none},
                               {user_lookup_fun, {fun psk_lookup/3, #{}}},
                               {ciphers, ["PSK-AES256-GCM-SHA384"]}]),
    ok = ssl:send(SSock, Connect),
    ok = packet:expect_packet(ssl, SSock, "connack", Connack),
    ok = ssl:close(SSock).

connect_psk_wrong_identity(_) ->
    assert_error_or_closed([{error,{tls_alert,"illegal_parameter"}},
                            {error,
                             {tls_alert,
                              {illegal_parameter,
                               "TLS client: In state cipher received SERVER ALERT: Fatal - Illegal Parameter"}}}],
                               ssl:connect("localhost", 1888,
                               [binary, {active, false}, {packet, raw},
                               {psk_identity, "it2"},
                               {versions, ['tlsv1.2']},
                               {verify, verify_none},
                               {protocol, tls},
                               {user_lookup_fun, {fun psk_lookup/3, #{}}},
                               {ciphers, ["PSK-AES256-GCM-SHA384"]}])).

connect_no_auth_test(_) ->
    Connect = packet:gen_connect("connect-success-test", [{keepalive, 10}]),
    Connack = packet:gen_connack(0),
    {ok, SSock} = ssl:connect("localhost", 1888,
                              [binary, {active, false}, {packet, raw},
                               {cacerts, load_cacerts()}]),
    ok = ssl:send(SSock, Connect),
    ok = packet:expect_packet(ssl, SSock, "connack", Connack),
    ok, ssl:close(SSock).

connect_no_auth_test_passwd(K) ->
   % good case
   connect_no_auth_test(K),
    % wrong passwd
   _Connect = packet:gen_connect("connect-success-test", [{keepalive, 10}]),
   _Connack = packet:gen_connack(0),
    {error, closed} = ssl:connect("localhost", 1889,
        [binary, {active, false}, {packet, raw},
            {cacerts, load_cacerts()}]).


    connect_no_auth_wrong_ca_test(_) ->
    assert_error_or_closed([{error,{tls_alert,"unknown ca"}},
                            {error, %% OTP 21.3+
                             {tls_alert,
                              {unknown_ca,"received CLIENT ALERT: Fatal - Unknown CA"}}}],
                  ssl:connect("localhost", 1888,
                              [binary, {active, false}, {packet, raw},
                               {verify, verify_peer},
                               {cacertfile, ssl_path("test-alt-ca.crt")}])).

connect_cert_auth_test(_) ->
    Connect = packet:gen_connect("connect-success-test", [{keepalive, 10}]),
    Connack = packet:gen_connack(0),
    {ok, SSock} = ssl:connect("localhost", 1888,
                              [binary, {active, false}, {packet, raw},
                               {verify, verify_peer},
                               {cacerts, load_cacerts()},
                               {certfile, ssl_path("client.crt")},
                               {keyfile, ssl_path("client.key")}]),
    ok = ssl:send(SSock, Connect),
    ok = packet:expect_packet(ssl, SSock, "connack", Connack),
    ok = ssl:close(SSock).

connect_cert_auth_without_test(_) ->
    assert_error_or_closed([{error,{tls_alert,"handshake failure"}},
                            {error, %% OTP 21.3+
                             {tls_alert,
                              {handshake_failure,
                               "received SERVER ALERT: Fatal - Handshake Failure"}}},
                            {error,
                             {tls_alert,
                              {handshake_failure,
                               "received CLIENT ALERT: Fatal - Handshake Failure"}}}],
                  ssl:connect("localhost", 1888,
                              [binary, {active, false}, {packet, raw},
                               {verify, verify_peer},
                               {cacerts, load_cacerts()}])).

connect_cert_auth_expired_test(_) ->
    assert_error_or_closed([{error,{tls_alert,"certificate expired"}},
                            {error, %% OTP 21.3+
                             {tls_alert,
                              {certificate_expired,
                               "received SERVER ALERT: Fatal - Certificate Expired"}}},
                            {error,
                             {tls_alert,
                              {certificate_expired,
                               "received CLIENT ALERT: Fatal - Certificate Expired"}}}],
                  ssl:connect("localhost", 1888,
                              [binary, {active, false}, {packet, raw},
                               {verify, verify_peer},
                               {cacerts, load_cacerts()},
                               {certfile, ssl_path("client-expired.crt")},
                               {keyfile, ssl_path("client.key")}])).

connect_cert_auth_revoked_test(_) ->
    assert_error_or_closed([{error,{tls_alert,"certificate revoked"}}, % pre OTP 18
                            {error,{tls_alert,"handshake failure"}}, % Erlang 18
                            {error, %% OTP 21.3+
                             {tls_alert,
                              {handshake_failure,
                               "received SERVER ALERT: Fatal - Handshake Failure"}}},
                            {error,
                             {tls_alert,
                              {handshake_failure,
                               "received CLIENT ALERT: Fatal - Handshake Failure"}}}],
                  ssl:connect("localhost", 1888,
                              [binary, {active, false}, {packet, raw},
                               {verify, verify_peer},
                               {cacerts, load_cacerts()},
                               {certfile, ssl_path("client-revoked.crt")},
                               {keyfile, ssl_path("client.key")}])).

connect_cert_auth_crl_test(_) ->
    Connect = packet:gen_connect("connect-success-test", [{keepalive, 10}]),
    Connack = packet:gen_connack(0),
    {ok, SSock} = ssl:connect("localhost", 1888,
                               [binary, {active, false}, {packet, raw},
                               {verify, verify_peer},
                               {cacerts, load_cacerts()},
                               {certfile, ssl_path("client.crt")},
                               {keyfile, ssl_path("client.key")}]),
    ok = ssl:send(SSock, Connect),
    ok = packet:expect_packet(ssl, SSock, "connack", Connack),
    ok = ssl:close(SSock).

connect_identity_test(_) ->
    Connect = packet:gen_connect("connect-success-test", [{keepalive, 10}]),
    Connack = packet:gen_connack(0),
    {ok, SSock} = ssl:connect("localhost", 1888,
                              [binary, {active, false}, {packet, raw},
                               {verify, verify_peer},
                               {cacerts, load_cacerts()},
                               {certfile, ssl_path("client.crt")},
                               {keyfile, ssl_path("client.key")}]),
    ok = ssl:send(SSock, Connect),
    ok = packet:expect_packet(ssl, SSock, "connack", Connack),
    ok = ssl:close(SSock).

connect_identity_allow_anonymous_override_on_test(_) ->
    Connect = packet:gen_connect("connect-success-test_aa_override", [{keepalive, 10}]),
    Connack = packet:gen_connack(0), % RC = 0 is the actual test here
    {ok, SSock} = ssl:connect("localhost", 1888,
                              [binary, {active, false}, {packet, raw},
                               {verify, verify_peer},
                               {cacerts, load_cacerts()},
                               {certfile, ssl_path("client.crt")},
                               {keyfile, ssl_path("client.key")}]),
    ok = ssl:send(SSock, Connect),
    ok = packet:expect_packet(ssl, SSock, "connack", Connack),
    ok = ssl:close(SSock).


connect_identity_allow_anonymous_override_off_test(_) ->
    Connect = packet:gen_connect("connect-success-test_aa_override_off", [{keepalive, 10}]),
    Connack = packet:gen_connack(5), % RC = 5 is the actual test here
    {ok, SSock} = ssl:connect("localhost", 1888,
                              [binary, {active, false}, {packet, raw},
                               {verify, verify_peer},
                               {cacerts, load_cacerts()},
                               {certfile, ssl_path("client.crt")},
                               {keyfile, ssl_path("client.key")}]),
    ok = ssl:send(SSock, Connect),
    ok = packet:expect_packet(ssl, SSock, "connack", Connack),
    ok = ssl:close(SSock).

connect_no_identity_test(_) ->
    assert_error_or_closed([{error,{tls_alert,"handshake failure"}},
                            {error,
                             {tls_alert,
                              {handshake_failure,
                               "received CLIENT ALERT: Fatal - Handshake Failure"}}},
                            {error,
                             {tls_alert,
                              {handshake_failure,
                               "received SERVER ALERT: Fatal - Handshake Failure"}}}],
                  ssl:connect("localhost", 1888,
                              [binary, {active, false}, {packet, raw},
                               {verify, verify_peer},
                               {cacerts, load_cacerts()}])).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
hook_preauth_success(_, {"", <<"connect-success-test">>}, <<"test client">>, undefined, _) -> ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Helper
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-compile({inline, [assert_error_or_closed/2]}).
assert_error_or_closed([], Val) -> exit({error, {no_matching_error_message, Val}});
assert_error_or_closed([Error|Rest], Val) ->
    case catch assert_error_or_closed(Error, Val) of
        true ->
            true;
        _ ->
            assert_error_or_closed(Rest, Val)
    end;
assert_error_or_closed(Error, Val) ->
    {ExpectedAlert, _ExpectedTxt} = case Error of
         {error, {tls_alert, Alert}} -> Alert;
         {ssl_error, _, {tls_alert, Alert}} -> Alert
         end,
    true = case Val of
               {error, closed} -> true;
               {error, {tls_alert, {ExpectedAlert, _}}} ->
                  true;
               {ok, SSLSocket} = E ->
                   ssl:close(SSLSocket),
                   E;
               Other -> Other
           end, true.

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

ssl_path(File) ->
    Path = filename:dirname(
             proplists:get_value(source, ?MODULE:module_info(compile))),
    filename:join([Path, "ssl", File]).

psk_lookup(psk, _, _) ->
    {ok, <<"test123">>}.
