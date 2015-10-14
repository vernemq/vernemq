-module(vmq_ssl).
-include_lib("public_key/include/public_key.hrl").
-export([socket_to_common_name/1,
         opts/1]).

socket_to_common_name(Socket) ->
    case ssl:peercert(Socket) of
        {error, no_peercert} ->
            undefined;
        {ok, Cert} ->
            OTPCert = public_key:pkix_decode_cert(Cert, otp),
            TBSCert = OTPCert#'OTPCertificate'.tbsCertificate,
            Subject = TBSCert#'OTPTBSCertificate'.subject,
            extract_cn(Subject)
    end.

-spec extract_cn({'rdnSequence', list()}) -> undefined | binary().
extract_cn({rdnSequence, List}) ->
    extract_cn2(List).

-spec extract_cn2(list()) -> undefined | list().
extract_cn2([[#'AttributeTypeAndValue'{
                 type=?'id-at-commonName',
                 value={utf8String, CN}}]|_]) ->
    list_to_binary(unicode:characters_to_list(CN));
extract_cn2([_|Rest]) ->
    extract_cn2(Rest);
extract_cn2([]) -> undefined.

opts(Opts) ->
    [{cacerts, case proplists:get_value(cafile, Opts) of
                   undefined -> undefined;
                   CAFile -> load_cert(CAFile)
               end},
     {certfile, proplists:get_value(certfile, Opts)},
     {keyfile, proplists:get_value(keyfile, Opts)},
     {ciphers, ciphersuite_transform(
                 proplists:get_value(support_elliptic_curves, Opts, true),
                 proplists:get_value(ciphers, Opts, [])
                )},
     {fail_if_no_peer_cert, proplists:get_value(require_certificate,
                                                Opts, false)},
     {verify, case
                  proplists:get_value(require_certificate, Opts, false) or
                  proplists:get_value(use_identity_as_username, Opts, false)
              of
                  true -> verify_peer;
                  _ -> verify_none
              end},
     {verify_fun, {fun verify_ssl_peer/3,
                   proplists:get_value(crlfile, Opts, no_crl)}},
     {versions, [proplists:get_value(tls_version, Opts, 'tlsv1.2')]}
     |
     []
     %% TODO: support for flexible partial chain functions
     % case support_partial_chain() of
     %     true ->
     %         [{partial_chain, fun([DerCert|_]) ->
     %                                  {trusted_ca, DerCert}
     %                          end}];
     %     false ->
     %         []
     % end
    ].


-spec ciphersuite_transform(boolean(), string()) -> [{atom(), atom(), atom()}].
ciphersuite_transform(SupportEC, []) ->
    unbroken_cipher_suites(all_ciphers(SupportEC));
ciphersuite_transform(_, CiphersString) when is_list(CiphersString) ->
    Ciphers = string:tokens(CiphersString, ":"),
    unbroken_cipher_suites(ciphersuite_transform_(Ciphers, [])).

-spec ciphersuite_transform_([string()],
                             [{atom(), atom(), atom()}]) ->
    [{atom(), atom(), atom()}].
ciphersuite_transform_([CipherString|Rest], Acc) ->
    Cipher = string:tokens(CipherString, "-"),
    case cipher_ex_transform(Cipher) of
        {ok, CipherSuite} ->
            ciphersuite_transform_(Rest, [CipherSuite|Acc]);
        {error, Reason} ->
            lager:error("error parsing ciphersuite ~p, ~p~n",
                        [CipherString, Reason]),
            ciphersuite_transform_(Rest, Acc)
    end;
ciphersuite_transform_([], Acc) -> Acc.

-spec cipher_ex_transform([string()]) ->
    {ok, {atom(), atom(), atom()}} |
    {error, unknown_keyexchange |
     unknown_cipher | unknown_cipher}.
cipher_ex_transform(["ECDH", "ANON"|Rest]) ->
    cipher_ci_transform(ecdh_anon, Rest);
cipher_ex_transform(["ECDH", "ECDSA"|Rest]) ->
    cipher_ci_transform(ecdh_ecdsa, Rest);
cipher_ex_transform(["ECDHE", "ECDSA"|Rest]) ->
    cipher_ci_transform(ecdhe_ecdsa, Rest);
cipher_ex_transform(["ECDH", "RSA"|Rest]) ->
    cipher_ci_transform(ecdh_rsa, Rest);
cipher_ex_transform(["ECDHE", "RSA"|Rest]) ->
    cipher_ci_transform(ecdhe_rsa, Rest);
cipher_ex_transform(["DHE", "DSS"|Rest]) -> cipher_ci_transform(dhe_dss, Rest);
cipher_ex_transform(["DHE", "RSA"|Rest]) -> cipher_ci_transform(dhe_rsa, Rest);
cipher_ex_transform(["DH", "ANON"|Rest]) -> cipher_ci_transform(dh_anon, Rest);
cipher_ex_transform(["DHE", "PSK"|Rest]) -> cipher_ci_transform(dhe_psk, Rest);
cipher_ex_transform(["RSA", "PSK"|Rest]) -> cipher_ci_transform(rsa_psk, Rest);
cipher_ex_transform(["SRP", "ANON"|Rest]) ->
    cipher_ci_transform(srp_anon, Rest);
cipher_ex_transform(["SRP", "DSS"|Rest]) -> cipher_ci_transform(srp_dss, Rest);
cipher_ex_transform(["SRP", "RSA"|Rest]) -> cipher_ci_transform(srp_rsa, Rest);
cipher_ex_transform(["RSA"|Rest]) -> cipher_ci_transform(rsa, Rest);
cipher_ex_transform(["PSK"|Rest]) -> cipher_ci_transform(psk, Rest);
cipher_ex_transform([_|Rest]) -> cipher_ex_transform(Rest);
cipher_ex_transform([]) -> {error, unknown_keyexchange}.

-spec cipher_ci_transform(atom(), [string()]) ->
    {ok, {atom(), atom(), atom()}} |
    {error, unknown_hash | unknown_cipher}.
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

-spec cipher_hash_transform(atom(), atom(), [string()]) ->
    {ok, {atom(), atom(), atom()}} |
    {error, unknown_hash}.
cipher_hash_transform(KeyEx, Cipher, ["MD5"]) -> {ok, {KeyEx, Cipher, md5}};
cipher_hash_transform(KeyEx, Cipher, ["SHA"]) -> {ok, {KeyEx, Cipher, sha}};
cipher_hash_transform(KeyEx, Cipher, ["SHA256"]) ->
    {ok, {KeyEx, Cipher, sha256}};
cipher_hash_transform(KeyEx, Cipher, ["SHA384"]) ->
    {ok, {KeyEx, Cipher, sha384}};
cipher_hash_transform(KeyEx, Cipher, [_|Rest]) ->
    cipher_hash_transform(KeyEx, Cipher, Rest);
cipher_hash_transform(_, _, []) -> {error, unknown_hash}.

-spec all_ciphers(boolean()) -> [{atom(), atom(), atom()}].
all_ciphers(UseEc) ->
    ECExchanges = [ecdh_anon, ecdh_ecdsa, ecdhe_ecdsa, ecdh_rsa, ecdhe_rsa],
    lists:foldl(fun({Ex, _, _} = CS, Acc) ->
                        case UseEc of
                            true -> [CS|Acc];
                            false ->
                                case lists:member(Ex, ECExchanges) of
                                    true -> Acc;
                                    false -> [CS|Acc]
                                end
                        end;
                   (_, Acc) -> Acc
                end, [], ssl:cipher_suites()).

-spec unbroken_cipher_suites([atom()]) -> [{atom(), atom(), atom()}].
unbroken_cipher_suites(CipherSuites) ->
    %% from ranch
    case proplists:get_value(ssl_app, ssl:versions()) of
        Version when Version =:= "5.3"; Version =:= "5.3.1" ->
            lists:filter(fun(Suite) ->
                                 string:left(
                                   atom_to_list(element(1,
                                                        Suite)), 4) =/= "ecdh"
                         end, CipherSuites);
        _ ->
            CipherSuites
    end.

-spec verify_ssl_peer(_, 'valid' | 'valid_peer' |
                      {'bad_cert', _} |
                      {'extension', _}, _) ->
    {'fail', 'is_self_signed' |
     {'bad_cert', _}} |
    {'unknown', _} | {'valid', _}.
verify_ssl_peer(_, {bad_cert, _} = Reason, _) ->
    {fail, Reason};
verify_ssl_peer(_, {extension, _}, UserState) ->
    {unknown, UserState};
verify_ssl_peer(_, valid, UserState) ->
    {valid, UserState};
verify_ssl_peer(Cert, valid_peer, UserState) ->
    case public_key:pkix_is_self_signed(Cert) of
        true ->
            {fail, is_self_signed};
        false ->
            check_user_state(UserState, Cert)
    end.

check_user_state(UserState, Cert) ->
    case UserState of
        no_crl ->
            {valid, UserState};
        CrlFile ->
            case vmq_crl_srv:check_crl(CrlFile, Cert) of
                true ->
                    {valid, UserState};
                false ->
                    {fail, {bad_cert, cert_revoked}}
            end
    end.

-spec load_cert(string()) -> [binary()].
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


