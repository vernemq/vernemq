%% Copyright 2014 Erlio GmbH Basel Switzerland (http://erl.io)
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(vmq_listener_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,
         start_listeners/0,
         change_config_now/3]).

%% Supervisor callbacks
-export([init/1]).

-define(CHILD(Id, Mod, Type, Args), {Id, {Mod, start_link, Args},
                                     permanent, 5000, Type, [Mod]}).

-define(APP, vmq_server).
-define(SUP, ?MODULE).
-define(TABLE, vmq_listeners).

-type listener_mod() :: vmq_tcp_listener
                      | vmq_ws_listener
                      | vmq_ssl_listener
                      | vmq_wss_listener.
%%%===================================================================
%%% API functions
%%%===================================================================
start_link() ->
    {ok, Pid} = supervisor:start_link({local, ?SUP}, ?MODULE, []),
    start_listeners(),
    {ok, Pid}.

-spec change_config_now(_, [any()], _) -> 'ok'.
change_config_now(_New, Changed, _Deleted) ->
    %% we are only interested if the config changes
    {OldListeners, NewListeners} = proplists:get_value(listeners,
                                                       Changed, {[], []}),
    {OldTCP, OldSSL, OldWS, OldWSS} = OldListeners,
    {NewTcp, NewSSL, NewWS, NewWSS} = NewListeners,
    maybe_change_listener(vmq_tcp_listener, OldTCP, NewTcp),
    maybe_change_listener(vmq_ssl_listener, OldSSL, NewSSL),
    maybe_change_listener(vmq_ws_listener, OldWS, NewWS),
    maybe_change_listener(vmq_wss_listener, OldWSS, NewWSS),
    maybe_new_listener(vmq_tcp_listener, OldTCP, NewTcp),
    maybe_new_listener(vmq_ssl_listener, OldSSL, NewSSL),
    maybe_new_listener(vmq_ws_listener, OldWS, NewWS),
    maybe_new_listener(vmq_wss_listener, OldWSS, NewWSS).

-spec start_listeners() -> ok.
start_listeners() ->
    {ok, {TCPListeners, SSLListeners,
          WSListeners, WSSListeners}} = application:get_env(?APP, listeners),
    start_listeners(TCPListeners, vmq_tcp_listener),
    start_listeners(SSLListeners, vmq_ssl_listener),
    start_listeners(WSListeners, vmq_ws_listener),
    start_listeners(WSSListeners, vmq_wss_listener),
    ok.

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
    {ok, {{one_for_one, 5, 10}, []}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec maybe_change_listener(listener_mod(), _, _) -> 'ok'.
maybe_change_listener(_, Old, New) when Old == New -> ok;
maybe_change_listener(Transport, Old, New) ->
    lists:foreach(
      fun({{Ip, Port} = IpAddr, Opts}) ->
              Id = listener_name(Ip, Port),
              case proplists:get_value(IpAddr, New) of
                  undefined ->
                      %% delete listener
                      supervisor:terminate_child(?SUP, Id);
                  Opts -> ok; %% no change;
                  NewOpts ->
                      {_, Pid, _, _} =
                      lists:keyfind(Id, 1, supervisor:which_children(?SUP)),
                      vmq_listener:change_config(Pid, transport_opts(
                                                        Transport, NewOpts))
              end
      end, Old).

-spec maybe_new_listener(listener_mod(), _, _) -> ok.
maybe_new_listener(ListenerMod, Old, New) ->
    lists:foreach(
      fun({{Addr, Port} = IpAddr, Opts}) ->
              case proplists:get_value(IpAddr, Old) of
                  undefined ->
                      %% start new listener
                      start_listener(ListenerMod, Addr, Port, Opts);
                  _ ->
                      ok
              end
      end, New).


-spec start_listeners([any()], listener_mod()) -> ok.
start_listeners(Listeners, ListenerMod) ->
    lists:foreach(fun({{Addr, Port}, Opts}) ->
                          start_listener(ListenerMod, Addr, Port, Opts)
                  end, Listeners).

-spec start_listener(listener_mod(),
                     string() | inet:ip_address(), inet:port_number(),
                     [any()]) -> {'ok',pid()}.
start_listener(ListenerMod, Addr, Port, Opts) ->
    Ref = listener_name(Addr, Port),
    AAddr = case is_list(Addr) of
                true ->
                    {ok, Ip} = inet:parse_address(Addr),
                    Ip;
                false ->
                    Addr
            end,
    TransportOpts = transport_opts(ListenerMod, Opts),
    HandlerOpts = handler_opts(Opts),
    ChildSpec = {Ref,
                 {vmq_listener, start_link, [Port, AAddr,
                                            TransportOpts,
                                            ListenerMod,
                                            HandlerOpts]},
                 permanent, 5000, worker, [ListenerMod]},
              % {max_connections,
              %  proplists:get_value(max_connections, Opts)}
    case supervisor:start_child(?SUP, ChildSpec) of
        {ok, _} ->
            lager:info("started ~p on ~p:~p", [ListenerMod, Addr, Port]);
        {error, Reason} ->
            FReason = inet:format_error(Reason),
            lager:error("can't start ~p on ~p:~p due to ~p",
                        [ListenerMod, Addr, Port, FReason])
    end.


-spec listener_name(string() |
                    inet:ip_address(),
                    inet:port_number()) ->
                           {'vmq_listener',
                            inet:ip_address(), inet:port_number()}.
listener_name(Ip, Port) ->
    {vmq_listener, Ip, Port}.

-spec transport_opts(listener_mod(),_) -> [{atom(), any()}].
transport_opts(vmq_ssl_listener, Opts) ->
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
     case support_partial_chain() of
         true ->
             [{partial_chain, fun([DerCert|_]) ->
                                      {trusted_ca, DerCert}
                              end}];
         false ->
             []
     end
    ];
transport_opts(_, _Opts) -> [].

handler_opts(Opts) ->
    {ok, MsgLogHandler} = application:get_env(?APP, msg_log_handler),
    {ok, MaxClientIdSize} = application:get_env(?APP, max_client_id_size),
    {ok, RetryInterval} = application:get_env(?APP, retry_interval),
    [{msg_log_handler, MsgLogHandler},
     {max_client_id_size, MaxClientIdSize},
     {retry_interval, RetryInterval}
     |Opts].

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
    [CS || {Ex, _, _} =
               CS <- ssl:cipher_suites(),
           case UseEc of
               true -> true;
               false ->
                   not lists:member(Ex, ECExchanges)
           end].

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
    case file:read_file(Cert) of
        {error, Reason} ->
            lager:error("can't load certificate ~p due to Error: ~p",
                        [Cert, Reason]),
            undefined;
        {ok, Bin} ->
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
            end
    end.

-spec support_partial_chain() -> boolean().
support_partial_chain() ->
    {ok, VSN} = application:get_key(ssl, vsn),
    VSNTuple = list_to_tuple(
                 [list_to_integer(T)
                  || T <- string:tokens(VSN, ".")]),
    VSNTuple >= {5, 3, 6}.
