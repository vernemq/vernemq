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
         connect_cert_auth_test_tlsv12/1,
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
         connect_no_auth_test_passwd/1,
         connect_forward_conn_opts_test/1,
         connect_forward_conn_opts_no_sni_test/1,
         connect_partial_chain_full_chain_test/1,
         connect_partial_chain_leaf_only_test/1,
         connect_partial_chain_other_intermediate_test/1,
         connect_partial_chain_other_intermediate_chain_test/1,
         connect_partial_chain_leaf_in_cafile_test/1,
         connect_partial_chain_cafile_reload_test/1,
         connect_partial_chain_tlsv1_3_test/1,
         partial_chain_requires_cafile_test/1,
         connect_partial_chain_off_test/1]).

-export([hook_preauth_success/5, hook_conn_opts_handler/5, hook_conn_opts_handler/6]).

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
          lists:member(Case, all_no_auth_encrypted_keyfile()),
          lists:member(Case, all_forward_conn_opts()),
          lists:member(Case, all_partial_chain()),
          lists:member(Case, all_partial_chain_off())} of
        {true, _, _, _, _, _, _, _, _, _, _,_, _, _} ->
            {ok, _} = vmq_server_cmd:set_config(allow_anonymous, true),
            {ok, _} = vmq_server_cmd:listener_start(1888, [{ssl, true},
                                                           {nr_of_acceptors, 5},
                                                           {cafile, ssl_path("all-ca.crt")},
                                                           {certfile, ssl_path("server.crt")},
                                                           {keyfile, ssl_path("server.key")},
                                                           {tls_version, "tlsv1.2"}]);
        {_, true, _, _, _, _, _, _, _, _, _,_, _, _} ->
            {ok, _} = vmq_server_cmd:set_config(allow_anonymous, true),
            {ok, _} = vmq_server_cmd:listener_start(1888, [{ssl, true},
                                                           {nr_of_acceptors, 5},
                                                           {cafile, ssl_path("all-ca.crt")},
                                                           {certfile, ssl_path("server.crt")},
                                                           {keyfile, ssl_path("server.key")},
                                                           {tls_version, "tlsv1.2"},
                                                           {require_certificate, true}]);
        {_, _, true, _, _, _, _, _, _, _, _,_, _, _} ->
            {ok, _} = vmq_server_cmd:set_config(allow_anonymous, true),
            {ok, _} = vmq_server_cmd:listener_start(1888, [{ssl, true},
                                                           {nr_of_acceptors, 5},
                                                           {cafile, ssl_path("all-ca.crt")},
                                                           {certfile, ssl_path("server.crt")},
                                                           {keyfile, ssl_path("server.key")},
                                                           {tls_version, "tlsv1.3"},
                                                           {require_certificate, true}]);
        {_, _, _, true, _, _, _, _, _, _, _,_, _, _} ->
            {ok, _} = vmq_server_cmd:set_config(allow_anonymous, true),
            {ok, _} = vmq_server_cmd:listener_start(1888, [{ssl, true},
                                                           {nr_of_acceptors, 5},
                                                           {cafile, ssl_path("all-ca.crt")},
                                                           {certfile, ssl_path("server.crt")},
                                                           {keyfile, ssl_path("server.key")},
                                                           {tls_version, "tlsv1.2"},
                                                           {require_certificate, true},
                                                           {crlfile, ssl_path("crl.pem")}]);
        {_, _, _, _, true, _, _, _, _, _, _,_, _, _} ->
            {ok, _} = vmq_server_cmd:set_config(allow_anonymous, true),
            {ok, _} = vmq_server_cmd:listener_start(1888, [{ssl, true},
                {nr_of_acceptors, 5},
                {cafile, ssl_path("all-ca.crt")},
                {certfile, ssl_path("server.crt")},
                {keyfile, ssl_path("server.key")},
                {tls_version, "tlsv1.3"},
                {require_certificate, true},
                {crlfile, ssl_path("crl.pem")}]);
        {_, _, _, _, _, true, _, _, _, _, _,_, _, _} ->
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
        {_, _, _, _, _, _, true, _, _, _, _,_, _, _} ->
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
        {_, _, _, _, _, _, _, true, _, _, _,_, _, _} ->
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
        {_, _, _, _, _, _, _, _, true, _, _,_, _, _} ->
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
        {_, _, _, _, _, _, _, _, _, true, _,_, _, _} ->
            {ok, _} = vmq_server_cmd:set_config(allow_anonymous, true),
            {ok, _} = vmq_server_cmd:listener_start(1888, [{ssl, true},
                                                           {nr_of_acceptors, 5},
                                                           {psk_support, true},
                                                           {pskfile, ssl_path("test-psk.psk")},
                                                           {tls_version, "tlsv1.2"}]);
        {_, _, _, _, _, _, _, _, _, _, true,_, _, _} ->
            {ok, _} = vmq_server_cmd:set_config(allow_anonymous, true),
            {ok, _} = vmq_server_cmd:listener_start(1888, [{ssl, true},
                {nr_of_acceptors, 5},
                {cafile, ssl_path("all-ca.crt")},
                {certfile, ssl_path("server.crt")},
                {keyfile, ssl_path("server.encrypted.key")},
                {keypasswd, "VerneMQ123"},
                {tls_version, "tlsv1.2"}]),
            case vmq_server_cmd:listener_start(1889, [{ssl, true},
                {nr_of_acceptors, 5},
                {cafile, ssl_path("all-ca.crt")},
                {certfile, ssl_path("server.crt")},
                {keyfile, ssl_path("server.encrypted.key")},
                {keypasswd, "VerneMQ123Wrong"},
                {tls_version, "tlsv1.2"}]) of
                {ok, _} ->
                    ok;
                {error, _} ->
                    ok
            end;
        {_, _, _, _, _, _, _, _, _, _, _,true, _, _} ->
            {ok, _} = vmq_server_cmd:set_config(allow_anonymous, false),
            {ok, _} = vmq_server_cmd:listener_start(1888, [{ssl, true},
                        {nr_of_acceptors, 5},
                        {cafile, ssl_path("all-ca.crt")},
                        {certfile, ssl_path("server.crt")},
                        {keyfile, ssl_path("server.key")},
                        {tls_version, "tlsv1.2"},
                        {require_certificate, true},
                        {crlfile, ssl_path("crl.pem")},
                        {use_identity_as_username, true},
                        {forward_connection_opts, true}]),
            ok = vmq_plugin_mgr:enable_module_plugin(
                        auth_on_register, ?MODULE, hook_conn_opts_handler, 6);
        {_, _, _, _, _, _, _, _, _, _, _,_, true, _} ->
            %% only the intermediate CA is trusted and partial_chain set to true
            {ok, _} = vmq_server_cmd:set_config(allow_anonymous, true),
            {ok, _} = vmq_server_cmd:listener_start(1888, [{ssl, true},
                        {nr_of_acceptors, 5},
                        {cafile, ssl_path("test-signing-ca.crt")},
                        {certfile, ssl_path("server.crt")},
                        {keyfile, ssl_path("server.key")},
                        {tls_version, "tlsv1.2"},
                        {require_certificate, true},
                        {partial_chain, true}]);
        {_, _, _, _, _, _, _, _, _, _, _,_, _, true} ->
            %% only the intermediate CA is trusted and partial_chain set to false
            {ok, _} = vmq_server_cmd:set_config(allow_anonymous, true),
            {ok, _} = vmq_server_cmd:listener_start(1888, [{ssl, true},
                        {nr_of_acceptors, 5},
                        {cafile, ssl_path("test-signing-ca.crt")},
                        {certfile, ssl_path("server.crt")},
                        {keyfile, ssl_path("server.key")},
                        {tls_version, "tlsv1.2"},
                        {require_certificate, true},
                        {partial_chain, false}])
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
    ++ all_no_auth_encrypted_keyfile()
    ++ all_forward_conn_opts()
    ++ all_partial_chain()
    ++ all_partial_chain_off().

all_no_auth() ->
    [connect_no_auth_test,
     connect_no_auth_wrong_ca_test].

all_cert_auth() ->
    [connect_cert_auth_test,
     connect_cert_auth_without_test,
     connect_cert_auth_expired_test].

all_cert_auth_tlsv1_3() ->
    [connect_cert_auth_test,
     connect_cert_auth_test_tlsv12,
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
        [connect_identity_allow_anonymous_override_off_test].

all_psk_auth() ->
  [connect_psk_test,
   connect_psk_wrong_identity].

all_no_auth_encrypted_keyfile() ->
    [connect_no_auth_test_passwd].

all_forward_conn_opts() ->
    [connect_forward_conn_opts_test,
     connect_forward_conn_opts_no_sni_test].

all_partial_chain() ->
    [connect_partial_chain_full_chain_test,
     connect_partial_chain_leaf_only_test,
     connect_partial_chain_other_intermediate_test,
     connect_partial_chain_other_intermediate_chain_test,
     connect_partial_chain_leaf_in_cafile_test,
     connect_partial_chain_cafile_reload_test,
     connect_partial_chain_tlsv1_3_test,
     partial_chain_requires_cafile_test].

all_partial_chain_off() ->
    [connect_partial_chain_off_test].

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
    case ssl:connect("localhost", 1889,
        [binary, {active, false}, {packet, raw},
            {cacerts, load_cacerts()}]) of
        {error, closed} ->
            ok;
        {error, econnrefused} ->
            ok;
        {error, {tls_alert, _}} ->
            ok
    end.


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

connect_cert_auth_test_tlsv12(_) ->
    {error, {tls_alert, {protocol_version, _}}} = ssl:connect("localhost", 1888,
                              [binary, {active, false}, {packet, raw},
                               {versions, ['tlsv1.2']},
                               {verify, verify_peer},
                               {cacerts, load_cacerts()},
                               {certfile, ssl_path("client.crt")},
                               {keyfile, ssl_path("client.key")}]).

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
    {error, {tls_alert, {certificate_revoked, _}}} =
        ssl:connect("localhost", 1888,
                    [binary, {active, false}, {packet, raw},
                     {verify, verify_peer},
                     {cacerts, load_cacerts()},
                     {certfile, ssl_path("client-revoked.crt")},
                     {keyfile, ssl_path("client.key")}]).

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

connect_forward_conn_opts_test(_) ->
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

connect_forward_conn_opts_no_sni_test(_) ->
    Connect = packet:gen_connect("connect-no-sni-test", [{keepalive, 10}]),
    Connack = packet:gen_connack(0),
    {ok, SSock} = ssl:connect("localhost", 1888,
                              [binary, {active, false}, {packet, raw},
                               {verify, verify_peer},
                               {server_name_indication, disable},
                               {cacerts, load_cacerts()},
                               {certfile, ssl_path("client.crt")},
                               {keyfile, ssl_path("client.key")}]),
    ok = ssl:send(SSock, Connect),
    ok = packet:expect_packet(ssl, SSock, "connack", Connack),
    ok = ssl:close(SSock).

connect_partial_chain_full_chain_test(_) ->
    Connect = packet:gen_connect("connect-success-test", [{keepalive, 10}]),
    Connack = packet:gen_connack(0),
    {ok, SSock} = ssl:connect("localhost", 1888,
                              [binary, {active, false}, {packet, raw},
                               {verify, verify_peer},
                               {cacerts, load_cacerts()},
                               {certfile, ssl_path("client-chain.crt")},
                               {keyfile, ssl_path("client.key")}]),
    ok = ssl:send(SSock, Connect),
    ok = packet:expect_packet(ssl, SSock, "connack", Connack),
    ok = ssl:close(SSock).

connect_partial_chain_leaf_only_test(_) ->
    Connect = packet:gen_connect("connect-success-test", [{keepalive, 10}]),
    Connack = packet:gen_connack(0),
    {ok, SSock} = ssl:connect("localhost", 1888,
                              [binary, {active, false}, {packet, raw},
                               {verify, verify_none},
                               {certfile, ssl_path("client.crt")},
                               {keyfile, ssl_path("client.key")}]),
    ok = ssl:send(SSock, Connect),
    ok = packet:expect_packet(ssl, SSock, "connack", Connack),
    ok = ssl:close(SSock).

connect_partial_chain_other_intermediate_test(_) ->
    assert_error_or_closed([{error, {tls_alert, {unknown_ca, ""}}}],
                  ssl:connect("localhost", 1888,
                              [binary, {active, false}, {packet, raw},
                               {verify, verify_none},
                               {certfile, ssl_path("client-alt.crt")},
                               {keyfile, ssl_path("client-alt.key")}])).

connect_partial_chain_other_intermediate_chain_test(_) ->
    %% the client sends its intermediate CA along, none of the two
    %% certificates is contained in the cafile
    assert_error_or_closed([{error, {tls_alert, {unknown_ca, ""}}}],
                           partial_chain_connect(1888, "client-alt-chain.crt", "client-alt.key")).

connect_partial_chain_leaf_in_cafile_test(_) ->
    %% a client certificate in the cafile is no trust anchor
    start_partial_chain_listener(1889, ssl_path("client.crt"), "tlsv1.2"),
    assert_error_or_closed([{error, {tls_alert, {unknown_ca, ""}}}],
                           partial_chain_connect(1889, "client-chain.crt", "client.key")).

connect_partial_chain_cafile_reload_test(Config) ->
    CAFile = filename:join(proplists:get_value(priv_dir, Config), "partial-chain-ca.crt"),
    {ok, _} = file:copy(ssl_path("test-signing-ca.crt"), CAFile),
    start_partial_chain_listener(1889, CAFile, "tlsv1.2"),
    ok = partial_chain_connect_success(1889, "client-chain.crt", "client.key"),
    assert_error_or_closed([{error, {tls_alert, {unknown_ca, ""}}}],
                           partial_chain_connect(1889, "client-alt-chain.crt", "client-alt.key")),
    %% trust the alternative intermediate CA instead of the signing CA
    {ok, _} = file:copy(ssl_path("test-alt-ca.crt"), CAFile),
    ok = vmq_server_cli:command(["vmq-admin", "tls", "clear-pem-cache"]),
    ok = partial_chain_connect_success(1889, "client-alt-chain.crt", "client-alt.key"),
    assert_error_or_closed([{error, {tls_alert, {unknown_ca, ""}}}],
                           partial_chain_connect(1889, "client-chain.crt", "client.key")).

connect_partial_chain_tlsv1_3_test(_) ->
    start_partial_chain_listener(1889, ssl_path("test-signing-ca.crt"), "tlsv1.3"),
    ok = partial_chain_connect_success(1889, "client-chain.crt", "client.key"),
    ok = partial_chain_connect_success(1889, "client.crt", "client.key"),
    %% with TLS 1.3 the client learns about the rejected certificate
    %% only after its own handshake is done
    Res =
        case partial_chain_connect(1889, "client-alt.crt", "client-alt.key") of
            {ok, SSock} -> ssl:recv(SSock, 0, 5000);
            Error -> Error
        end,
    assert_error_or_closed([{error, {tls_alert, {unknown_ca, ""}}}], Res).

partial_chain_requires_cafile_test(_) ->
    {error, _} = vmq_server_cmd:listener_start(1889, [{ssl, true},
                        {nr_of_acceptors, 5},
                        {certfile, ssl_path("server.crt")},
                        {keyfile, ssl_path("server.key")},
                        {tls_version, "tlsv1.2"},
                        {require_certificate, true},
                        {partial_chain, true}]),
    {error, econnrefused} = gen_tcp:connect("localhost", 1889, []).

connect_partial_chain_off_test(_) ->
    %% same intermediate-only cafile, partial_chain off: must be rejected
    assert_error_or_closed([{error, {tls_alert, {unknown_ca, ""}}}],
                  ssl:connect("localhost", 1888,
                              [binary, {active, false}, {packet, raw},
                               {verify, verify_none},
                               {certfile, ssl_path("client-chain.crt")},
                               {keyfile, ssl_path("client.key")}])).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
hook_preauth_success(_, {"", <<"connect-success-test">>}, <<"test client">>, undefined, _) -> ok.

hook_conn_opts_handler(_, {"", <<"connect-success-test">>}, <<"test client">>, _, _,
                        #{listener_addr := {127,0,0,1},
                          listener_port := 1888,
                          listener_type := mqtts,
                          tls_sni := <<"localhost">>} = ConnOpts) when is_map(ConnOpts) ->
    ClientCert = maps:get(client_cert, ConnOpts, undefined),
    % just check whether the client cert is a binary (Pem)
    case ClientCert of
        C when is_binary(C) -> ok;
        _ -> next
    end;
hook_conn_opts_handler(_, {"", <<"connect-no-sni-test">>}, <<"test client">>, _, _,
                        #{listener_addr := {127,0,0,1},
                          listener_port := 1888,
                          listener_type := mqtts,
                          tls_sni := undefined}) -> ok;
hook_conn_opts_handler(_,_,_,_,_,_) ->
    next.
hook_conn_opts_handler(_,_,_,_,_) -> next.

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

start_partial_chain_listener(Port, CAFile, TLSVersion) ->
    {ok, _} = vmq_server_cmd:listener_start(Port, [{ssl, true},
                        {nr_of_acceptors, 5},
                        {cafile, CAFile},
                        {certfile, ssl_path("server.crt")},
                        {keyfile, ssl_path("server.key")},
                        {tls_version, TLSVersion},
                        {require_certificate, true},
                        {partial_chain, true}]).

partial_chain_connect(Port, CertFile, KeyFile) ->
    ssl:connect("localhost", Port,
                [binary, {active, false}, {packet, raw},
                 {verify, verify_none},
                 {certfile, ssl_path(CertFile)},
                 {keyfile, ssl_path(KeyFile)}]).

partial_chain_connect_success(Port, CertFile, KeyFile) ->
    Connect = packet:gen_connect("connect-success-test", [{keepalive, 10}]),
    Connack = packet:gen_connack(0),
    {ok, SSock} = partial_chain_connect(Port, CertFile, KeyFile),
    ok = ssl:send(SSock, Connect),
    ok = packet:expect_packet(ssl, SSock, "connack", Connack),
    ok = ssl:close(SSock).

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
