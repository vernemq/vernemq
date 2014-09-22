%%%-------------------------------------------------------------------
%%% @author graf
%%% @copyright (C) 2014, graf
%%% @doc
%%%
%%% @end
%%% Created : 2014-09-03 15:49:38.393546
%%%-------------------------------------------------------------------
-module(emqttd_endpoint_sup).
-include_lib("public_key/include/public_key.hrl").
-behaviour(supervisor).

%% API
-export([start_link/0,
         add_endpoint/5,
         add_ws_endpoint/5]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).
-define(APP, emqttd_server).

-define(SSL_EC, []).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

add_endpoint(Ip, Port, MaxConnections, NrOfAcceptors, MountPoint) ->
    [ChildSpec] = generate_childspecs([{{Ip,Port}, [{max_connections, MaxConnections},
                                                    {nr_of_acceptors, NrOfAcceptors},
                                                    {mountpoint, MountPoint}] }], ranch_tcp,
                                      emqttd_tcp),
    supervisor:start_child(?SERVER, ChildSpec).

add_ws_endpoint(Ip, Port, MaxConnections, NrOfAcceptors, MountPoint) ->
    [ChildSpec] = generate_childspecs([{{Ip,Port}, [{max_connections, MaxConnections},
                                                    {nr_of_acceptors, NrOfAcceptors},
                                                    {mountpoint, MountPoint}] }], ranch_tcp,
                                      cowboy_protocol),
    supervisor:start_child(?SERVER, ChildSpec).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @spec init(Args) -> {ok, {SupFlags, [ChildSpec]}} |
%%                     ignore |
%%                     {error, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    {ok, {TCPListeners, SSLListeners, WSListeners}} = application:get_env(?APP, listeners),
    MQTTEndpoints = generate_childspecs(TCPListeners, ranch_tcp, emqttd_tcp),
    MQTTSEndpoints = generate_childspecs(SSLListeners, ranch_ssl, emqttd_tcp),
    MQTTWSEndpoints = generate_childspecs(WSListeners, ranch_tcp, cowboy_protocol),
    CRLSrv = [{emqttd_crl_srv,
               {emqttd_crl_srv, start_link, []},
               permanent, 5000, worker, [emqttd_crl_srv]}],
    {ok, { {one_for_one, 5, 10}, CRLSrv ++ MQTTEndpoints ++ MQTTSEndpoints ++ MQTTWSEndpoints}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
generate_childspecs(Listeners, Transport, Protocol) ->
    [ranch:child_spec(list_to_atom("mqtt_"++integer_to_list(Port)),
                      proplists:get_value(nr_of_acceptors, Opts), Transport,
                      [{ip, case is_list(Addr) of
                                true -> {ok, Ip} = inet:parse_address(Addr),
                                        Ip;
                                false -> Addr
                            end }, {port, Port},
                       {max_connections, proplists:get_value(max_connections, Opts)} |
                       case Transport of
                           ranch_ssl ->
                               CACerts = load_cert(proplists:get_value(cafile, Opts)),
                               [{cacerts, CACerts},
                                {certfile, proplists:get_value(certfile, Opts)},
                                {keyfile, proplists:get_value(keyfile, Opts)},
                                {ciphers, ciphersuite_transform(
                                            proplists:get_value(support_elliptic_curves, Opts, true),
                                            proplists:get_value(ciphers, Opts, [])
                                           )},
                                {fail_if_no_peer_cert, proplists:get_value(require_certificate, Opts, false)},
                                {verify, case
                                             proplists:get_value(require_certificate, Opts) or
                                             proplists:get_value(use_identity_as_username, Opts)
                                         of
                                             true -> verify_peer;
                                             _ -> verify_none
                                         end},
                                {verify_fun, {fun verify_ssl_peer/3, proplists:get_value(crlfile, Opts, no_crl)}},
                                {versions, [proplists:get_value(tls_version, Opts, 'tlsv1.2')]}
                               ];
                           _ ->
                               []
                       end
                      ],
                      Protocol, handler_opts(Protocol, Opts))
     || {{Addr, Port}, Opts} <- Listeners].

handler_opts(cowboy_protocol, Opts) ->
    Dispatch = cowboy_router:compile(
                 [
                  {'_', [
                         {"/mqtt", emqttd_ws, handler_opts(emqttd_tcp, Opts)}
                        ]}
                 ]),
    [{env, [{dispatch, Dispatch}]}];

handler_opts(emqttd_tcp, Opts) ->
    {ok, MsgLogHandler} = application:get_env(?APP, msg_log_handler),
    {ok, MaxClientIdSize} = application:get_env(?APP, max_client_id_size),
    {ok, RetryInterval} = application:get_env(?APP, retry_interval),
    [{msg_log_handler, MsgLogHandler},
     {max_client_id_size, MaxClientIdSize},
     {retry_interval, RetryInterval}
     |Opts];

handler_opts(emqttd_ssl, Opts) ->
    TCPOpts = handler_opts(emqttd_tcp, Opts),
    UseIdentityAsUserName = proplists:get_value(use_identity_as_username, Opts, false),
    case proplists:get_value(psk_hint, Opts) of
        undefined ->
            [{use_identity_as_username, UseIdentityAsUserName}|TCPOpts];
        PskHint ->
            [{psk_hin, PskHint},
             {use_identity_as_username, UseIdentityAsUserName}
             |TCPOpts]
    end.


ciphersuite_transform(SupportEC, []) ->
    unbroken_cipher_suites(all_ciphers(SupportEC));
ciphersuite_transform(_, CiphersString) when is_list(CiphersString) ->
    Ciphers = string:tokens(CiphersString, ":"),
    unbroken_cipher_suites(ciphersuite_transform_(Ciphers, [])).

ciphersuite_transform_([CipherString|Rest], Acc) ->
    Cipher = string:tokens(CipherString, "-"),
    case cipher_ex_transform(Cipher) of
        {ok, CipherSuite} ->
            ciphersuite_transform_(Rest, [CipherSuite|Acc]);
        {error, Reason} ->
            error_logger:warning_msg("error parsing ciphersuite ~p, ~p~n", [CipherString, Reason]),
            ciphersuite_transform_(Rest, Acc)
    end;
ciphersuite_transform_([], Acc) -> Acc.

cipher_ex_transform(["ECDH", "ANON"|Rest]) -> cipher_ci_transform(ecdh_anon, Rest);
cipher_ex_transform(["ECDH", "ECDSA"|Rest]) -> cipher_ci_transform(ecdh_ecdsa, Rest);
cipher_ex_transform(["ECDHE", "ECDSA"|Rest]) -> cipher_ci_transform(ecdhe_ecdsa, Rest);
cipher_ex_transform(["ECDH", "RSA"|Rest]) -> cipher_ci_transform(ecdh_rsa, Rest);
cipher_ex_transform(["ECDHE", "RSA"|Rest]) -> cipher_ci_transform(ecdhe_rsa, Rest);
cipher_ex_transform(["DHE", "DSS"|Rest]) -> cipher_ci_transform(dhe_dss, Rest);
cipher_ex_transform(["DHE", "RSA"|Rest]) -> cipher_ci_transform(dhe_rsa, Rest);
cipher_ex_transform(["DH", "ANON"|Rest]) -> cipher_ci_transform(dh_anon, Rest);
cipher_ex_transform(["DHE", "PSK"|Rest]) -> cipher_ci_transform(dhe_psk, Rest);
cipher_ex_transform(["RSA", "PSK"|Rest]) -> cipher_ci_transform(rsa_psk, Rest);
cipher_ex_transform(["SRP", "ANON"|Rest]) -> cipher_ci_transform(srp_anon, Rest);
cipher_ex_transform(["SRP", "DSS"|Rest]) -> cipher_ci_transform(srp_dss, Rest);
cipher_ex_transform(["SRP", "RSA"|Rest]) -> cipher_ci_transform(srp_rsa, Rest);
cipher_ex_transform(["RSA"|Rest]) -> cipher_ci_transform(rsa, Rest);
cipher_ex_transform(["PSK"|Rest]) -> cipher_ci_transform(psk, Rest);
cipher_ex_transform([_|Rest]) -> cipher_ex_transform(Rest);
cipher_ex_transform([]) -> {unknown_keyexchange}.

cipher_ci_transform(KeyEx, ["3DES", "EDE", "CBC"|Rest]) ->
    cipher_hash_transform(KeyEx, '3des_ede_cbc', Rest);
cipher_ci_transform(KeyEx, ["AES128", "CBC"|Rest]) ->
    cipher_hash_transform(KeyEx, aes_128_cbc, Rest);
cipher_ci_transform(KeyEx, ["AES256", "CBC"|Rest]) ->
    cipher_hash_transform(KeyEx, aes_256_cbc, Rest);
cipher_ci_transform(KeyEx, ["RC4", "128"|Rest]) ->
    cipher_hash_transform(KeyEx, rc4_128, Rest);
cipher_ci_transform(KeyEx, ["DES", "CBC"|Rest]) ->
    cipher_hash_transform(KeyEx, des_cbc, Rest);
cipher_ci_transform(KeyEx, [_|Rest]) ->
    cipher_ci_transform(KeyEx, Rest);
cipher_ci_transform(_, []) -> {error, unknown_cipher}.

cipher_hash_transform(KeyEx, Cipher, ["MD5"]) -> {ok, {KeyEx, Cipher, md5}};
cipher_hash_transform(KeyEx, Cipher, ["SHA"]) -> {ok, {KeyEx, Cipher, sha}};
cipher_hash_transform(KeyEx, Cipher, ["SHA256"]) -> {ok, {KeyEx, Cipher, sha256}};
cipher_hash_transform(KeyEx, Cipher, ["SHA384"]) -> {ok, {KeyEx, Cipher, sha384}};
cipher_hash_transform(KeyEx, Cipher, [_|Rest]) ->
    cipher_hash_transform(KeyEx, Cipher, Rest);
cipher_hash_transform(_, _, []) -> {error, unknown_hash}.

all_ciphers(UseEc) ->
    ECExchanges = [ecdh_anon, ecdh_ecdsa, ecdhe_ecdsa, ecdh_rsa, ecdhe_rsa],
    [CS || {Ex, _, _} = CS <- ssl:cipher_suites(), case UseEc of
                                                       true -> true;
                                                       false ->
                                                           not lists:member(Ex, ECExchanges)
                                                   end].

unbroken_cipher_suites(CipherSuites) ->
    %% from ranch
    case proplists:get_value(ssl_app, ssl:versions()) of
        Version when Version =:= "5.3"; Version =:= "5.3.1" ->
            lists:filter(fun(Suite) ->
                                 string:left(atom_to_list(element(1, Suite)), 4) =/= "ecdh"
                         end, CipherSuites);
        _ ->
            CipherSuites
    end.

verify_ssl_peer(_, {bad_cert, _} = Reason, _) ->
    {fail, Reason};
verify_ssl_peer(_,{extension, _}, UserState) ->
    {unknown, UserState};
verify_ssl_peer(_, valid, UserState) ->
    {valid, UserState};
verify_ssl_peer(Cert, valid_peer, UserState) ->
    case public_key:pkix_is_self_signed(Cert) of
        true ->
            {fail, is_self_signed};
        false ->
            case UserState of
                no_crl ->
                    {valid, UserState};
                CrlFile ->
                    case emqttd_crl_srv:check_crl(CrlFile, Cert) of
                        true ->
                            {valid, UserState};
                        false ->
                            {fail, {bad_cert, cert_revoked}};
                        {error, Reason} ->
                            {fail, Reason}
                    end
            end
    end.


load_cert(Cert) ->
    {ok, Bin} = file:read_file(Cert),
    case filename:extension(Cert) of
        ".der" ->
            %% no decoding necessary
            [Bin];
        _ ->
            %% assume PEM otherwise
            Contents = public_key:pem_decode(Bin),
            [DER || {Type, DER, Cipher} <- Contents, Type == 'Certificate', Cipher == 'not_encrypted']
    end.

%check_crl(Cert, State) ->
%    %% pull the CRL distribution point(s) out of the certificate, if any
%    case pubkey_cert:select_extension(
%           ?'id-ce-cRLDistributionPoints',
%           pubkey_cert:extensions_list(Cert#'OTPCertificate'.tbsCertificate#'OTPTBSCertificate'.extensions)) of
%        undefined ->
%            %% fail; we can't validate if there's no CRL
%            no_crl;
%        CRLExtension ->
%            CRLDistPoints = CRLExtension#'Extension'.extnValue,
%            DPointsAndCRLs = lists:foldl(fun(Point, Acc) ->
%                                                 %% try to read the CRL over http or from a
%                                                 %% local file
%                                                 case fetch_point(Point) of
%                                                     not_available ->
%                                                         Acc;
%                                                     Res ->
%                                                         [{Point, Res} | Acc]
%                                                 end
%                                         end, [], CRLDistPoints),
%            public_key:pkix_crls_validate(Cert,
%                                          DPointsAndCRLs,
%                                          [{issuer_fun,
%                                            {fun issuer_function/4, State}}])
%    end.
%%% @doc Given a list of distribution points for CRLs, certificates and
%%% both trusted and intermediary certificates, attempt to build and
%%% authority chain back via build_chain to verify that it is valid.
%%%
%issuer_function(_DP, CRL, _Issuer, {TrustedCAs, IntermediateCerts}) ->
%    %% XXX the 'Issuer' we get passed here is the AuthorityKeyIdentifier,
%    %% which we are not currently smart enough to understand
%    %% Read the CA certs out of the file
%    Certs = [public_key:pkix_decode_cert(DER, otp) || DER <- TrustedCAs],
%    %% get the real issuer out of the CRL
%    Issuer = public_key:pkix_normalize_name(
%               pubkey_cert_records:transform(
%                 CRL#'CertificateList'.tbsCertList#'TBSCertList'.issuer, decode)),
%    %% assume certificates are ordered from root to tip
%    case find_issuer(Issuer, IntermediateCerts ++ Certs) of
%        undefined ->
%            error;
%        IssuerCert ->
%            case build_chain({public_key:pkix_encode('OTPCertificate',
%                                                     IssuerCert,
%                                                     otp),
%                              IssuerCert}, IntermediateCerts, Certs, []) of
%                undefined ->
%                    error;
%                {OTPCert, Path} ->
%                    {ok, OTPCert, Path}
%            end
%    end.
%
%%% @doc Attempt to build authority chain back using intermediary
%%% certificates, falling back on trusted certificates if the
%%% intermediary chain of certificates does not fully extend to the
%%% root.
%%%
%%% Returns: {RootCA :: #OTPCertificate{}, Chain :: [der_encoded()]}
%%%
%build_chain({DER, Cert}, IntCerts, TrustedCerts, Acc) ->
%    %% check if this cert is self-signed, if it is, we've reached the
%    %% root of the chain
%    Issuer = public_key:pkix_normalize_name(
%               Cert#'OTPCertificate'.tbsCertificate#'OTPTBSCertificate'.issuer),
%    Subject = public_key:pkix_normalize_name(
%                Cert#'OTPCertificate'.tbsCertificate#'OTPTBSCertificate'.subject),
%    case Issuer == Subject of
%        true ->
%            case find_issuer(Issuer, TrustedCerts) of
%                undefined ->
%                    io:format("self-signed certificate is NOT trusted~n"),
%                    undefined;
%                TrustedCert ->
%                    %% return the cert from the trusted list, to prevent
%                    %% issuer spoofing
%                    {TrustedCert,
%                     [public_key:pkix_encode(
%                        'OTPCertificate', TrustedCert, otp)|Acc]}
%            end;
%        false ->
%            Match = lists:foldl(
%                      fun(C, undefined) ->
%                              S = public_key:pkix_normalize_name(C#'OTPCertificate'.tbsCertificate#'OTPTBSCertificate'.subject),
%                              %% compare the subject to the current issuer
%                              case Issuer == S of
%                                  true ->
%                                      %% we've found our man
%                                      {public_key:pkix_encode('OTPCertificate', C, otp), C};
%                                  false ->
%                                      undefined
%                              end;
%                         (_E, A) ->
%                              %% already matched
%                              A
%                      end, undefined, IntCerts),
%            case Match of
%                undefined when IntCerts /= TrustedCerts ->
%                    %% continue the chain by using the trusted CAs
%                    io:format("Ran out of intermediate certs, switching to trusted certs~n"),
%                    build_chain({DER, Cert}, TrustedCerts, TrustedCerts, Acc);
%                undefined ->
%                    io:format("Can't construct chain of trust beyond ~p~n",
%                              [get_common_name(Cert)]),
%                    %% can't find the current cert's issuer
%                    undefined;
%                Match ->
%                    build_chain(Match, IntCerts, TrustedCerts, [DER|Acc])
%            end
%    end.
%%% @doc Given a certificate and a list of trusted or intermediary
%%% certificates, attempt to find a match in the list or bail with
%%% undefined.
%find_issuer(Issuer, Certs) ->
%    lists:foldl(
%      fun(OTPCert, undefined) ->
%              %% check if this certificate matches the issuer
%              Normal = public_key:pkix_normalize_name(
%                         OTPCert#'OTPCertificate'.tbsCertificate#'OTPTBSCertificate'.subject),
%              case Normal == Issuer of
%                  true ->
%                      OTPCert;
%                  false ->
%                      undefined
%              end;
%         (_E, Acc) ->
%              %% already found a match
%              Acc
%      end, undefined, Certs).
%
%get_crl(File) ->
%    {ok, Bin} = file:read_file(File),
%    %% assume PEM
%    [{'CertificateList', DER, _}=CertList] = public_key:pem_decode(Bin),
%    {DER, public_key:pem_entry_decode(CertList)}.
%
%
%%% get the common name attribute out of an OTPCertificate record
%get_common_name(OTPCert) ->
%    %% You'd think there'd be an easier way than this giant mess, but I
%    %% couldn't find one.
%    {rdnSequence, Subject} = OTPCert#'OTPCertificate'.tbsCertificate#'OTPTBSCertificate'.subject,
%    case [Attribute#'AttributeTypeAndValue'.value || [Attribute] <- Subject,
%                                                     Attribute#'AttributeTypeAndValue'.type == ?'id-at-commonName'] of
%        [Att] ->
%            case Att of
%                {teletexString, Str} -> Str;
%                {printableString, Str} -> Str;
%                {utf8String, Bin} -> binary_to_list(Bin)
%            end;
%        _ ->
%            unknown
%    end.
