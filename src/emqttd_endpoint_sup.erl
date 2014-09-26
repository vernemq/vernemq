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
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-export([change_config_now/3]).

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

change_config_now(_New, Changed, _Deleted) ->
    %% we are only interested if the config changes
    {OldListeners, NewListeners} = proplists:get_value(listeners, Changed, {[],[]}),
    {OldTCP, OldSSL, OldWS} = OldListeners,
    {NewTcp, NewSSL, NewWS} = NewListeners,
    maybe_change_listener(ranch_tcp, OldTCP, NewTcp),
    maybe_change_listener(ranch_ssl, OldSSL, NewSSL),
    maybe_change_listener(ranch_tcp, OldWS, NewWS),
    maybe_new_listener(ranch_tcp, emqttd_tcp, OldTCP, NewTcp),
    maybe_new_listener(ranch_ssl, emqttd_tcp, OldSSL, NewSSL),
    maybe_new_listener(ranch_tcp, cowboy_protocol, OldWS, NewWS).

maybe_change_listener(_, Old, New) when Old == New -> ok;
maybe_change_listener(Transport, Old, New) ->
    lists:foreach(
      fun({{Ip,Port} = IpAddr, Opts}) ->
              case proplists:get_value(IpAddr, New) of
                  undefined ->
                      %% delete listener
                      ranch:stop_listener(listener_name(Ip, Port));
                  Opts -> ok; %% no change;
                  NewOpts ->
                      %% New Opts for existing listener
                      %% teardown listener and restart
                      %% TODO: improve!
                      ok = ranch:set_protocol_options(listener_name(Ip, Port), transport_opts(Transport, NewOpts))
              end
      end, Old).

maybe_new_listener(Transport, Protocol, Old, New) ->
    lists:foreach(
      fun({{Addr, Port} = IpAddr, Opts}) ->
              case proplists:get_value(IpAddr, Old) of
                  undefined ->
                      %% start new listener
                      start_listener(Transport, Protocol, Addr, Port, Opts);
                  _ ->
                      ok
              end
      end, New).

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
    start_listeners(TCPListeners, ranch_tcp, emqttd_tcp),
    start_listeners(SSLListeners, ranch_ssl, emqttd_tcp),
    start_listeners(WSListeners, ranch_tcp, cowboy_protocol),
    CRLSrv = [{emqttd_crl_srv,
               {emqttd_crl_srv, start_link, []},
               permanent, 5000, worker, [emqttd_crl_srv]}],
    {ok, { {one_for_one, 5, 10}, CRLSrv}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
start_listeners(Listeners, Transport, Protocol) ->
    [start_listener(Transport, Protocol, Addr, Port, Opts) || {{Addr, Port}, Opts} <- Listeners].

start_listener(Transport, Protocol, Addr, Port, Opts) ->
    Ref = listener_name(Addr, Port),
    NrOfAcceptors = proplists:get_value(nr_of_acceptors, Opts),
    {ok, _Pid} = ranch:start_listener(Ref, NrOfAcceptors, Transport,
                                      [{ip, case is_list(Addr) of
                                                true -> {ok, Ip} = inet:parse_address(Addr),
                                                        Ip;
                                                false -> Addr
                                            end }, {port, Port},
                                       {max_connections, proplists:get_value(max_connections, Opts)}
                                       | transport_opts(Transport, Opts)],
                                      Protocol, handler_opts(Protocol, Opts)).

listener_name(Ip, Port) ->
    {emqttd_listener, Ip, Port}.

transport_opts(ranch_ssl, Opts) ->
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
     {fail_if_no_peer_cert, proplists:get_value(require_certificate, Opts, false)},
     {verify, case
                  proplists:get_value(require_certificate, Opts) or
                  proplists:get_value(use_identity_as_username, Opts)
              of
                  true -> verify_peer;
                  _ -> verify_none
              end},
     %{psk_identity, proplists:get_value(psk_hint, Opts)},
     %{user_lookup_fun, case proplists:get_value(psk_hint, Opts) of
     %                      undefined -> undefined;
     %                      _ ->
     %                          {fun(psk, _PSKIdent, _) ->
     %                                   {ok, <<"deadbeef">>}
     %                           end, []}
     %                  end},
     {verify_fun, {fun verify_ssl_peer/3, proplists:get_value(crlfile, Opts, no_crl)}},
     {versions, [proplists:get_value(tls_version, Opts, 'tlsv1.2')]}
    ];
transport_opts(ranch_tcp, _Opts) -> [].

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
    case file:read_file(Cert) of
        {error, Reason} ->
            error_logger:warning_msg("can't load certificate ~p due to Error: ~p", [Cert, Reason]),
            undefined;
        {ok, Bin} ->
            case filename:extension(Cert) of
                ".der" ->
                    %% no decoding necessary
                    [Bin];
                _ ->
                    %% assume PEM otherwise
                    Contents = public_key:pem_decode(Bin),
                    [DER || {Type, DER, Cipher} <- Contents, Type == 'Certificate', Cipher == 'not_encrypted']
            end
    end.
