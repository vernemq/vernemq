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

-module(vmq_listener_cli).
-export([register_server_cli/0]).

-define(SUP, vmq_tcp_listener_sup).

register_server_cli() ->
    clique:register_usage(["vmq-admin", "listener"], vmq_listener_usage()),
    clique:register_usage(["vmq-admin", "listener", "start"],
                          vmq_listener_start_usage()),
    clique:register_usage(["vmq-admin", "listener", "stop"],
                          vmq_listener_stop_usage()),
    vmq_listener_start_cmd(),
    vmq_listener_stop_cmd(),
    vmq_listener_show_cmd().

vmq_listener_start_cmd() ->
    Cmd = ["vmq-admin", "listener", "start"],
    KeySpecs = [{port, [{typecast, fun(StrP) ->
                                           case catch list_to_integer(StrP) of
                                               P when (P >= 0) and (P=<65535) -> P;
                                               _ -> {error, {invalid_flag_value,
                                                             {port, StrP}}}
                                           end
                                   end}]}],
    FlagSpecs = [{address, [{shortname, "a"},
                            {longname, "address"},
                            {typecast, fun(A) ->
                                               case inet:parse_address(A) of
                                                   {ok, Ip} -> Ip;
                                                   {error, einval} ->
                                                       {error, {invalid_flag_value,
                                                                {address, A}}}
                                               end
                                       end}]},
                 {mountpoint, [{shortname, "m"},
                               {longname, "mountpoint"},
                               {typecast, fun(MP) -> MP end}]},
                 {websocket, [{shortname, "ws"},
                              {longname, "websocket"}]},
                 {ssl, [{longname, "ssl"}]},
                 {cafile, [{longname, "cafile"},
                           {typecast, fun(FileName) ->
                                              case filelib:is_file(FileName) of
                                                  true -> FileName;
                                                  false ->
                                                      {error, {invalid_flag_value,
                                                               {cafile, FileName}}}
                                              end
                                      end}]},
                 {certfile, [{longname, "certfile"},
                             {typecast, fun(FileName) ->
                                                case filelib:is_file(FileName) of
                                                    true -> FileName;
                                                    false ->
                                                        {error, {invalid_flag_value,
                                                                 {certfile, FileName}}}
                                                end
                                        end}]},
                 {keyfile, [{longname, "keyfile"},
                            {typecast, fun(FileName) ->
                                               case filelib:is_file(FileName) of
                                                   true -> FileName;
                                                   false ->
                                                       {error, {invalid_flag_value,
                                                                {keyfile, FileName}}}
                                               end
                                       end}]},
                 {ciphers, [{longname, "ciphers"},
                            {typecast, fun(C) -> C end}]},
                 {crlfile, [{longname, "crlfile"},
                            {typecast, fun(FileName) ->
                                               case filelib:is_file(FileName) of
                                                   true -> FileName;
                                                   false ->
                                                       {error, {invalid_flag_value,
                                                                {crlfile, FileName}}}
                                               end
                                       end}]},
                 {require_certificate, [{longname, "require-certificate"}]},
                 {tls_version, [{longname, "tls-version"},
                                {typespec, fun("sslv3") -> sslv3;
                                              ("tlsv1") -> tlsv1;
                                              ("tlsv1.1") -> 'tlsv1.1';
                                              ("tlsv1.2") -> 'tlsv1.2';
                                              (V) ->
                                                   {error, {invalid_flag_value,
                                                            {'tls-version', V}}}
                                           end}]},
                 {use_identity_as_username, [{longname, "use-identity-as-username"}]}
                ],
    Callback =
    fun([{port, Port}], Flags) ->
            Addr = proplists:get_value(address, Flags, {0,0,0,0}),
            IsWebSocket = lists:keymember(websocket, 1, Flags),
            IsSSL = lists:keymember(ssl, 1, Flags),
            TCPListenOptions = vmq_config:get_env(tcp_listen_options),
            Listeners = supervisor:which_children(?SUP),
            NewOpts1 = lists:keydelete(address, 1, lists:keydelete(port, 1, Flags)),
            NewOpts2 = lists:keyreplace(require_certificate, 1, NewOpts1,
                                        {require_certificate, true}),
            NewOpts3 = lists:keyreplace(use_identity_as_username, 1, NewOpts2,
                                        {use_identity_as_username, true}),
            Ret =
            case IsSSL of
                true ->
                    case ssl_mandatory_opts([cafile, certfile, keyfile],
                                            NewOpts3, []) of
                        ok when IsWebSocket ->
                            {ok, vmq_wss_transport};
                        ok ->
                            {ok, vmq_ssl_transport};
                        {error, Alerts} ->
                            {error, Alerts}
                    end;
                false when IsWebSocket ->
                    {ok, vmq_ws_transport};
                false ->
                    {ok, vmq_tcp_transport}
            end,
            case Ret of
                {ok, Transport} ->
                    case vmq_tcp_listener_sup:reconfigure_single_listener(
                           Transport, Listeners,
                           Addr, Port, NewOpts3,
                           TCPListenOptions) of
                        ok ->
                            [clique_status:text("Done")];
                        {error, Reason} ->
                            Text = io_lib:format("can't configure listener due to '~p'", [Reason]),
                            [clique_status:alert([clique_status:text(Text)])]
                    end;
                {error, Texts} when is_list(Texts) ->
                    [clique_status:alert(Texts)]
            end
    end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

vmq_listener_stop_cmd() ->
    Cmd = ["vmq-admin", "listener", "stop"],
    KeySpecs = [],
    FlagSpecs = [{port, [{shortname, "p"},
                         {longname, "port"},
                         {typecast, fun(StrP) ->
                                            case catch list_to_integer(StrP) of
                                                P when (P >= 0) and (P=<65535) -> P;
                                                _ -> {error, {invalid_flag_value,
                                                              {port, StrP}}}
                                            end
                                    end}]},
                 {address, [{shortname, "a"},
                            {longname, "address"},
                            {typecast, fun(A) ->
                                               case inet:parse_address(A) of
                                                   {ok, Ip} -> Ip;
                                                   {error, einval} ->
                                                       {error, {invalid_flag_value,
                                                                {address, A}}}
                                               end
                                       end}]}],
    Callback =
    fun([], Flags) ->
            Port = proplists:get_value(port, Flags, 1883),
            Addr = proplists:get_value(address, Flags, {0,0,0,0}),
            Ref = vmq_tcp_listener_sup:listener_name(
                    vmq_tcp_listener_sup:addr(Addr), Port),
            case supervisor:terminate_child(?SUP, Ref) of
                ok ->
                    case supervisor:delete_child(?SUP, Ref) of
                        ok ->
                            [clique_status:text("Done")];
                        {error, Reason} ->
                            Text = io_lib:format("can't stop listener due to '~p'", [Reason]),
                            [clique_status:alert([clique_status:text(Text)])]
                    end;
                {error, Reason} ->
                    Text = io_lib:format("can't stop listener due to '~p'", [Reason]),
                    [clique_status:alert([clique_status:text(Text)])]
            end
    end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

vmq_listener_show_cmd() ->
    Cmd = ["vmq-admin", "listener", "show"],
    KeySpecs = [],
    FlagSpecs = [{port, [{shortname, "p"},
                         {longname, "port"},
                         {typecast, fun(StrP) ->
                                            case catch list_to_integer(StrP) of
                                                P when (P >= 0) and (P=<65535) -> P;
                                                _ -> {error, {invalid_flag_value,
                                                              {port, StrP}}}
                                            end
                                    end}]},
                 {address, [{shortname, "a"},
                            {longname, "address"},
                            {typecast, fun(A) ->
                                               case inet:parse_address(A) of
                                                   {ok, Ip} -> Ip;
                                                   {error, einval} ->
                                                       {error, {invalid_flag_value,
                                                                {address, A}}}
                                               end
                                       end}]}],
    Callback =
    fun([], Flags) ->
            _Port = proplists:get_value(port, Flags, undefined),
            _Addr = proplists:get_value(address, Flags, undefined),
            Table =
            lists:foldl(
              fun({{vmq_tcp_listener, Ip, Port}, Pid, worker, _}, Acc) ->
                      StrIp = inet:ntoa(Ip),
                      StrPort = integer_to_list(Port),
                      case vmq_listener:getopts(Pid) of
                          {vmq_tcp_transport, MountPoint, _} ->
                              [[{type, 'TCP'}, {ip, StrIp},
                               {port, StrPort}, {mountpoint, MountPoint}]|Acc];
                          {vmq_ssl_transport, MountPoint, _} ->
                              [[{type, 'SSL'}, {ip, StrIp},
                               {port, StrPort}, {mountpoint, MountPoint}]|Acc];
                          {vmq_ws_transport, MountPoint, _} ->
                              [[{type, 'WS'}, {ip, StrIp},
                               {port, StrPort}, {mountpoint, MountPoint}]|Acc];
                          {vmq_wss_transport, MountPoint, _} ->
                              [[{type, 'WSS'}, {ip, StrIp},
                               {port, StrPort}, {mountpoint, MountPoint}]|Acc]
                      end
              end, [], supervisor:which_children(?SUP)),
              [clique_status:table(Table)]
    end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

vmq_listener_usage() ->
    ["vmq-admin listener <sub-command>\n\n",
     "  starts, modifies, and stops listeners.\n\n",
     "  Sub-commands:\n",
     "    start       Start the server application\n",
     "    stop        Stop the server application\n",
     "  Use --help after a sub-command for more details.\n"
    ].

vmq_listener_start_usage() ->
    ["vmq-admin listener start port=1883\n\n",
     "  Starts a new listener or modifies an existing listener. If no option\n",
     "  is specified a TCP listener is started listening on the given port\n\n",
     "General Options\n\n",
     "  -a, --address=IpAddress\n",
     "  -m, --mountpoint=Mountpoint\n\n",
     "SSL Options\n\n",
     "  --ssl\n",
     "      use SSL for this listener, without this option, all other SSL\n",
     "      are ignored\n",
     "  --cafile=CaFile\n",
     "      The path to the cafile containing the PEM encoded CA certificates\n" ,
     "      that are trusted by the server.\n",
     "  --certfile=CertificateFile\n",
     "      The path to the PEM encoded server certificate\n",
     "  --keyfile=KeyFile\n",
     "      The path to the PEM encoded key file\n",
     "  --ciphers=CiphersList\n",
     "      The list of allowed ciphers, each separated by a colon\n",
     "  --crlfile=CRLFile\n",
     "      If --require-certificate is set, you can use a certificate\n",
     "      revocation list file to revoke access to particular client\n",
     "      certificates. The file has to be PEM encoded.\n",
     "  --tls-version=TLSVersion\n",
     "      use this TLS version for the listener\n",
     "  --require_certificate\n",
     "      Use client certificates to authenticate your clients\n",
     "  --use-identity-as-username\n",
     "      If --require-certificate is set, the CN value from the client\n",
     "      certificate is used as the username for authentication\n\n"
    ].

vmq_listener_stop_usage() ->
    ["vmq-admin listener stop\n\n",
     "  Stops a running listener. If no option is given, the listener\n",
     "  listening on 0.0.0.0:1883 is stopped\n\n",
     "Options\n\n",
     "  -p, --port=PortNr\n",
     "  -a, --address=IpAddress\n\n"
    ].


ssl_mandatory_opts([Opt|Rest], Flags, Acc) ->
    case proplists:get_value(Opt, Flags, not_found) of
        not_found ->
            Text = io_lib:format("mandatory param '~p' not specified", [Opt]),
            ssl_mandatory_opts(Rest, Flags, [clique_status:text(Text)|Acc]);
        _ ->
            ssl_mandatory_opts(Rest, Flags, Acc)
    end;
ssl_mandatory_opts([], _, []) -> ok;
ssl_mandatory_opts([], _, Alerts) -> {error, Alerts}.
