%% Copyright 2018 Erlio GmbH Basel Switzerland (http://erl.io)
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
-module(vmq_schema).

-export([translate_listeners/1]).

translate_listeners(Conf) ->
    %% cuttlefish messes up with the tree-like configuration style if
    %% it cannot find either configured values or defaults in the
    %% more specific leafs of the tree. That's why we always provide
    %% a default value and take care of them by ourselfs.
    InfIntVal = fun(Name, Val1, Def) ->
                        case Val1 of
                            infinity -> infinity;
                            undefined -> Def;
                            -1 -> Def;
                            Int when is_integer(Int) -> Int;
                            _ -> cuttlefish:invalid(Name ++ "  should be an integer")
                        end
                end,
    MPVal = fun(Name, Val2, Def) -> case Val2 of
                                        "off" -> "";
                                        "" -> Def;
                                        S when is_list(S) -> S;
                                        _ -> cuttlefish:invalid(Name ++ "should be a string")
                                    end
            end,

    StrVal = fun(_, "", Def) -> Def;
                (_, S, _) when is_list(S) -> S;
                (_, undefined, Def) -> Def end,
    BoolVal = fun(_, B, _) when is_boolean(B) -> B;
                 (_, undefined, Def) -> Def end,
    AtomVal = fun(_, A, _) when is_atom(A) -> A;
                 (_, undefined, Def) -> Def end,
    IntVal = fun(_, I, _) when is_integer(I) -> I;
                (_, undefined, Def) -> Def end,


    MZip = fun([H|_] = ListOfLists) ->
                   Size = length(H), %% get default size
                   ListOfLists = [L || L <- ListOfLists, length(L) == Size],
                   [
                    lists:reverse(
                      lists:foldl(
                        fun(L, Acc) ->
                                [lists:nth(I, L)|Acc]
                        end, [], ListOfLists))
                    || I <- lists:seq(1, Size)]
           end,

    {TCPIPs, TCPMaxConns} = lists:unzip(extract("listener.tcp", "max_connections", InfIntVal, Conf)),
    {SSLIPs, SSLMaxConns} = lists:unzip(extract("listener.ssl", "max_connections", InfIntVal, Conf)),
    {WSIPs, WSMaxConns} = lists:unzip(extract("listener.ws", "max_connections", InfIntVal, Conf)),
    {WS_SSLIPs, WS_SSLMaxConns} = lists:unzip(extract("listener.wss", "max_connections", InfIntVal, Conf)),
    {VMQIPs, VMQMaxConns} = lists:unzip(extract("listener.vmq", "max_connections", InfIntVal, Conf)),
    {VMQ_SSLIPs, VMQ_SSLMaxConns} = lists:unzip(extract("listener.vmqs", "max_connections", InfIntVal, Conf)),
    {HTTPIPs, HTTPMaxConns} = lists:unzip(extract("listener.http", "max_connections", InfIntVal, Conf)),
    {HTTP_SSLIPs, HTTP_SSLMaxConns} = lists:unzip(extract("listener.https", "max_connections", InfIntVal, Conf)),

    {TCPIPs, TCPNrOfAcceptors} = lists:unzip(extract("listener.tcp", "nr_of_acceptors", InfIntVal, Conf)),
    {SSLIPs, SSLNrOfAcceptors} = lists:unzip(extract("listener.ssl", "nr_of_acceptors", InfIntVal, Conf)),
    {WSIPs, WSNrOfAcceptors} = lists:unzip(extract("listener.ws", "nr_of_acceptors", InfIntVal, Conf)),
    {WS_SSLIPs, WS_SSLNrOfAcceptors} = lists:unzip(extract("listener.wss", "nr_of_acceptors", InfIntVal, Conf)),
    {VMQIPs, VMQNrOfAcceptors} = lists:unzip(extract("listener.vmq", "nr_of_acceptors", InfIntVal, Conf)),
    {VMQ_SSLIPs, VMQ_SSLNrOfAcceptors} = lists:unzip(extract("listener.vmqs", "nr_of_acceptors", InfIntVal, Conf)),
    {HTTPIPs, HTTPNrOfAcceptors} = lists:unzip(extract("listener.http", "nr_of_acceptors", InfIntVal, Conf)),
    {HTTP_SSLIPs, HTTP_SSLNrOfAcceptors} = lists:unzip(extract("listener.https", "nr_of_acceptors", InfIntVal, Conf)),

    {TCPIPs, TCPMountPoint} = lists:unzip(extract("listener.tcp", "mountpoint", MPVal, Conf)),
    {SSLIPs, SSLMountPoint} = lists:unzip(extract("listener.ssl", "mountpoint", MPVal, Conf)),
    {WSIPs, WSMountPoint} = lists:unzip(extract("listener.ws", "mountpoint", MPVal, Conf)),
    {WS_SSLIPs, WS_SSLMountPoint} = lists:unzip(extract("listener.wss", "mountpoint", MPVal, Conf)),
    {VMQIPs, VMQMountPoint} = lists:unzip(extract("listener.vmq", "mountpoint", MPVal, Conf)),
    {VMQ_SSLIPs, VMQ_SSLMountPoint} = lists:unzip(extract("listener.vmqs", "mountpoint", MPVal, Conf)),
    {TCPIPs, TCPProxyProto} = lists:unzip(extract("listener.tcp", "proxy_protocol", BoolVal, Conf)),
    {WSIPs, WSProxyProto} = lists:unzip(extract("listener.ws", "proxy_protocol", BoolVal, Conf)),
    {HTTPIPs, HTTPProxyProto} = lists:unzip(extract("listener.http", "proxy_protocol", BoolVal, Conf)),

    {HTTPIPs, HTTPConfigMod} = lists:unzip(extract("listener.http", "config_mod", AtomVal, Conf)),
    {HTTPIPs, HTTPConfigFun} = lists:unzip(extract("listener.http", "config_fun", AtomVal, Conf)),
    {HTTP_SSLIPs, HTTP_SSLConfigMod} = lists:unzip(extract("listener.https", "config_mod", AtomVal, Conf)),
    {HTTP_SSLIPs, HTTP_SSLConfigFun} = lists:unzip(extract("listener.https", "config_fun", AtomVal, Conf)),

                                                % SSL
    {SSLIPs, SSLCAFiles} = lists:unzip(extract("listener.ssl", "cafile", StrVal, Conf)),
    {SSLIPs, SSLCAPaths} = lists:unzip(extract("listener.ssl", "capath", StrVal, Conf)),
    {SSLIPs, SSLDepths} = lists:unzip(extract("listener.ssl", "depth", IntVal, Conf)),
    {SSLIPs, SSLCertFiles} = lists:unzip(extract("listener.ssl", "certfile", StrVal, Conf)),
    {SSLIPs, SSLCiphers} = lists:unzip(extract("listener.ssl", "ciphers", StrVal, Conf)),
    {SSLIPs, SSLCrlFiles} = lists:unzip(extract("listener.ssl", "crlfile", StrVal, Conf)),
    {SSLIPs, SSLKeyFiles} = lists:unzip(extract("listener.ssl", "keyfile", StrVal, Conf)),
    {SSLIPs, SSLRequireCerts} = lists:unzip(extract("listener.ssl", "require_certificate", BoolVal, Conf)),
    {SSLIPs, SSLVersions} = lists:unzip(extract("listener.ssl", "tls_version", AtomVal, Conf)),
    {SSLIPs, SSLUseIdents} = lists:unzip(extract("listener.ssl", "use_identity_as_username", BoolVal, Conf)),

                                                % WSS
    {WS_SSLIPs, WS_SSLCAFiles} = lists:unzip(extract("listener.wss", "cafile", StrVal, Conf)),
    {WS_SSLIPs, WS_SSLCAPaths} = lists:unzip(extract("listener.wss", "capath", StrVal, Conf)),
    {WS_SSLIPs, WS_SSLDepths} = lists:unzip(extract("listener.wss", "depth", IntVal, Conf)),
    {WS_SSLIPs, WS_SSLCertFiles} = lists:unzip(extract("listener.wss", "certfile", StrVal, Conf)),
    {WS_SSLIPs, WS_SSLCiphers} = lists:unzip(extract("listener.wss", "ciphers", StrVal, Conf)),
    {WS_SSLIPs, WS_SSLCrlFiles} = lists:unzip(extract("listener.wss", "crlfile", StrVal, Conf)),
    {WS_SSLIPs, WS_SSLKeyFiles} = lists:unzip(extract("listener.wss", "keyfile", StrVal, Conf)),
    {WS_SSLIPs, WS_SSLRequireCerts} = lists:unzip(extract("listener.wss", "require_certificate", BoolVal, Conf)),
    {WS_SSLIPs, WS_SSLVersions} = lists:unzip(extract("listener.wss", "tls_version", AtomVal, Conf)),
    {WS_SSLIPs, WS_SSLUseIdents} = lists:unzip(extract("listener.wss", "use_identity_as_username", BoolVal, Conf)),

                                                % VMQS
    {VMQ_SSLIPs, VMQ_SSLCAFiles} = lists:unzip(extract("listener.vmqs", "cafile", StrVal, Conf)),
    {VMQ_SSLIPs, VMQ_SSLCAPaths} = lists:unzip(extract("listener.vmqs", "capath", StrVal, Conf)),
    {VMQ_SSLIPs, VMQ_SSLDepths} = lists:unzip(extract("listener.vmqs", "depth", IntVal, Conf)),
    {VMQ_SSLIPs, VMQ_SSLCertFiles} = lists:unzip(extract("listener.vmqs", "certfile", StrVal, Conf)),
    {VMQ_SSLIPs, VMQ_SSLCiphers} = lists:unzip(extract("listener.vmqs", "ciphers", StrVal, Conf)),
    {VMQ_SSLIPs, VMQ_SSLCrlFiles} = lists:unzip(extract("listener.vmqs", "crlfile", StrVal, Conf)),
    {VMQ_SSLIPs, VMQ_SSLKeyFiles} = lists:unzip(extract("listener.vmqs", "keyfile", StrVal, Conf)),
    {VMQ_SSLIPs, VMQ_SSLRequireCerts} = lists:unzip(extract("listener.vmqs", "require_certificate", BoolVal, Conf)),
    {VMQ_SSLIPs, VMQ_SSLVersions} = lists:unzip(extract("listener.vmqs", "tls_version", AtomVal, Conf)),

                                                % HTTPS
    {HTTP_SSLIPs, HTTP_SSLCAFiles} = lists:unzip(extract("listener.https", "cafile", StrVal, Conf)),
    {HTTP_SSLIPs, HTTP_SSLCAPaths} = lists:unzip(extract("listener.https", "capath", StrVal, Conf)),
    {HTTP_SSLIPs, HTTP_SSLDepths} = lists:unzip(extract("listener.https", "depth", IntVal, Conf)),
    {HTTP_SSLIPs, HTTP_SSLCertFiles} = lists:unzip(extract("listener.https", "certfile", StrVal, Conf)),
    {HTTP_SSLIPs, HTTP_SSLCiphers} = lists:unzip(extract("listener.https", "ciphers", StrVal, Conf)),
    {HTTP_SSLIPs, HTTP_SSLCrlFiles} = lists:unzip(extract("listener.https", "crlfile", StrVal, Conf)),
    {HTTP_SSLIPs, HTTP_SSLKeyFiles} = lists:unzip(extract("listener.https", "keyfile", StrVal, Conf)),
    {HTTP_SSLIPs, HTTP_SSLRequireCerts} = lists:unzip(extract("listener.https", "require_certificate", BoolVal, Conf)),
    {HTTP_SSLIPs, HTTP_SSLVersions} = lists:unzip(extract("listener.https", "tls_version", AtomVal, Conf)),

    TCP = lists:zip(TCPIPs, MZip([TCPMaxConns,
                                  TCPNrOfAcceptors,
                                  TCPMountPoint,
                                  TCPProxyProto])),
    WS = lists:zip(WSIPs, MZip([WSMaxConns,
                                WSNrOfAcceptors,
                                WSMountPoint,
                                WSProxyProto])),
    VMQ = lists:zip(VMQIPs, MZip([VMQMaxConns,
                                  VMQNrOfAcceptors,
                                  VMQMountPoint])),
    HTTP = lists:zip(HTTPIPs, MZip([HTTPMaxConns,
                                    HTTPNrOfAcceptors,
                                    HTTPConfigMod,
                                    HTTPConfigFun,
                                    HTTPProxyProto])),

    SSL = lists:zip(SSLIPs, MZip([SSLMaxConns,
                                  SSLNrOfAcceptors,
                                  SSLMountPoint,
                                  SSLCAFiles,
                                  SSLCAPaths,
                                  SSLDepths,
                                  SSLCertFiles,
                                  SSLCiphers,
                                  SSLCrlFiles,
                                  SSLKeyFiles,
                                  SSLRequireCerts,
                                  SSLVersions,
                                  SSLUseIdents])),
    WSS = lists:zip(WS_SSLIPs, MZip([WS_SSLMaxConns,
                                     WS_SSLNrOfAcceptors,
                                     WS_SSLMountPoint,
                                     WS_SSLCAFiles,
                                     WS_SSLCAPaths,
                                     WS_SSLDepths,
                                     WS_SSLCertFiles,
                                     WS_SSLCiphers,
                                     WS_SSLCrlFiles,
                                     WS_SSLKeyFiles,
                                     WS_SSLRequireCerts,
                                     WS_SSLVersions,
                                     WS_SSLUseIdents])),
    VMQS = lists:zip(VMQ_SSLIPs, MZip([VMQ_SSLMaxConns,
                                       VMQ_SSLNrOfAcceptors,
                                       VMQ_SSLMountPoint,
                                       VMQ_SSLCAFiles,
                                       VMQ_SSLCAPaths,
                                       VMQ_SSLDepths,
                                       VMQ_SSLCertFiles,
                                       VMQ_SSLCiphers,
                                       VMQ_SSLCrlFiles,
                                       VMQ_SSLKeyFiles,
                                       VMQ_SSLRequireCerts,
                                       VMQ_SSLVersions])),
    HTTPS = lists:zip(HTTP_SSLIPs, MZip([HTTP_SSLMaxConns,
                                         HTTP_SSLNrOfAcceptors,
                                         HTTP_SSLCAFiles,
                                         HTTP_SSLCAPaths,
                                         HTTP_SSLDepths,
                                         HTTP_SSLCertFiles,
                                         HTTP_SSLCiphers,
                                         HTTP_SSLCrlFiles,
                                         HTTP_SSLKeyFiles,
                                         HTTP_SSLRequireCerts,
                                         HTTP_SSLVersions,
                                         HTTP_SSLConfigMod,
                                         HTTP_SSLConfigFun])),

    DropUndef = fun(L) ->
                        [{K, [I || {_, V} = I  <- SubL, V /= undefined]} || {K, SubL} <- L]
                end,
    [{mqtt, DropUndef(TCP)},
     {mqtts, DropUndef(SSL)},
     {mqttws, DropUndef(WS)},
     {mqttwss, DropUndef(WSS)},
     {vmq, DropUndef(VMQ)},
     {vmqs, DropUndef(VMQS)},
     {http, DropUndef(HTTP)},
     {https, DropUndef(HTTPS)}
    ].

extract(Prefix, Suffix, Val, Conf) ->
    Mappings = ["max_connections", "nr_of_acceptors", "mountpoint"],
    ExcludeRootSuffixes
        = [%% ssl listener specific
           "cafile", "capath", "depth", "certfile", "ciphers", "crlfile",
           "keyfile", "require_certificate", "tls_version",
           "use_identity_as_username",
           %% http listener specific
           "config_mod", "config_fun",
           %% other
           "proxy_protocol"
          ],

    %% get default from root of the tree for listeners
    RootDefault =
        case lists:member(Suffix, ExcludeRootSuffixes) of
            true ->
                undefined;
            false ->
                cuttlefish:conf_get(lists:flatten(["listener.", Suffix]), Conf)
        end,
    Default = cuttlefish:conf_get(lists:flatten([Prefix, ".", Suffix]), Conf, RootDefault),
    %% get the name value pairs
    NameSubPrefix = lists:flatten([Prefix, ".$name"]),
    [begin
         {ok, Addr} = inet:parse_address(StrAddr),
         Prefix4 = lists:flatten([Prefix, ".", Name, ".", Suffix]),
         V1 = Val(Name, RootDefault, undefined),
         V2 = Val(Name, RootDefault,V1),
         V3 = Val(Name, cuttlefish:conf_get(Prefix4, Conf, Default),V2),

         AddrPort = {Addr, Port},
         {AddrPort, {list_to_atom(Suffix), V3}}
     end
     || {[_, _, Name], {StrAddr, Port}} <- lists:filter(
                                             fun({K, _V}) ->
                                                     cuttlefish_variable:is_fuzzy_match(K, string:tokens(NameSubPrefix, "."))
                                             end, Conf), not lists:member(Name, Mappings ++ ExcludeRootSuffixes)].
