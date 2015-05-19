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

register_server_cli() ->
    clique:register_usage(["vmq-admin", "listener"], vmq_listener_usage()),
    clique:register_usage(["vmq-admin", "listener", "start"],
                          vmq_listener_start_usage()),
    clique:register_usage(["vmq-admin", "listener", "stop"],
                          vmq_listener_stop_usage()),
    clique:register_usage(["vmq-admin", "listener", "delete"],
                          vmq_listener_delete_usage()),
    clique:register_usage(["vmq-admin", "listener", "restart"],
                          vmq_listener_restart_usage()),
    vmq_listener_start_cmd(),
    vmq_listener_stop_cmd(),
    vmq_listener_delete_cmd(),
    vmq_listener_restart_cmd(),
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
                 {max_connections, [{longname, "max_connections"},
                               {typecast, fun
                                              ("infinity") -> infinity;
                                              (N) ->
                                                    list_to_integer(N)
                                          end}]},
                 {nr_of_acceptors, [{longname, "nr_of_acceptors"},
                                    {typecast, fun(N) -> list_to_integer(N) end}]},
                 {websocket, [{shortname, "ws"},
                              {longname, "websocket"}]},
                 {http, [{shortname, "http"},
                         {longname, "http"}]},
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
                 {require_certificate, [{longname, "require_certificate"}]},
                 {tls_version, [{longname, "tls_version"},
                                {typecast, fun("sslv3") -> sslv3;
                                              ("tlsv1") -> tlsv1;
                                              ("tlsv1.1") -> 'tlsv1.1';
                                              ("tlsv1.2") -> 'tlsv1.2';
                                              (V) ->
                                                   {error, {invalid_flag_value,
                                                            {'tls-version', V}}}
                                           end}]},
                 {use_identity_as_username, [{longname, "use_identity_as_username"}]},
                 {config_mod, [{longname, "config_mod"},
                               {typecast, fun(M) -> list_to_existing_atom(M) end}]},
                 {config_fun, [{longname, "config_fun"},
                               {typecast, fun(F) -> list_to_existing_atom(F) end}]}
                ],
    Callback =
    fun ([], _) ->
            Text = lists:flatten(vmq_listener_start_usage()),
            [clique_status:alert([clique_status:text(Text)])];
        ([{port, Port}], Flags) ->
            Addr = proplists:get_value(address, Flags, {0,0,0,0}),
            IsWebSocket = lists:keymember(websocket, 1, Flags),
            IsPlainHTTP = lists:keymember(http, 1, Flags),
            IsSSL = lists:keymember(ssl, 1, Flags),
            NewOpts1 = lists:keydelete(address, 1,
                                       lists:keydelete(port, 1,
                                                       cleanup_flags(Flags, []))),

            case IsSSL of
                true when IsWebSocket ->
                    start_listener(mqttwss, Addr, Port, NewOpts1);
                true when IsPlainHTTP ->
                    start_listener(https, Addr, Port, NewOpts1);
                true ->
                    start_listener(mqtts, Addr, Port, NewOpts1);
                false when IsWebSocket ->
                    start_listener(mqttws, Addr, Port, NewOpts1);
                false when IsPlainHTTP ->
                    start_listener(http, Addr, Port, NewOpts1);
                false ->
                    start_listener(mqtt, Addr, Port, NewOpts1)
            end
    end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

start_listener(Type, Addr, Port, Opts) ->
    case vmq_ranch_config:start_listener(Type, Addr, Port, Opts) of
        ok ->
            ListenerKey = {Addr, Port},
            ListenerConfig = vmq_config:get_env(listeners),
            ConfigForType = proplists:get_value(Type, ListenerConfig, []),
            NewConfigForType = [{ListenerKey, Opts}
                                |lists:keydelete(ListenerKey, 1, ConfigForType)],
            NewListenerConfig = [{Type, NewConfigForType}
                                 |lists:keydelete(Type, 1, ListenerConfig)],
            vmq_config:set_env(listeners, NewListenerConfig, false),
            [clique_status:text("Done")];
        {error, Reason} ->
            Text = io_lib:format("can't start listener due to '~p'", [Reason]),
            [clique_status:alert([clique_status:text(Text)])]
    end.

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
                                       end}]},
                 {kill, [{shortname, "k"},
                         {longname, "kill_sessions"}]}],
    Callback =
    fun([], Flags) ->
            Port = proplists:get_value(port, Flags, 1883),
            Addr = proplists:get_value(address, Flags, {0,0,0,0}),
            IsKill = lists:keymember(kill, 1, Flags),
            case vmq_ranch_config:stop_listener(Addr, Port, IsKill) of
                ok ->
                    [clique_status:text("Done")];
                {error, Reason} ->
                    Text = io_lib:format("can't stop listener due to '~p'", [Reason]),
                    [clique_status:alert([clique_status:text(Text)])]
            end
    end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

vmq_listener_delete_cmd() ->
    Cmd = ["vmq-admin", "listener", "delete"],
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
            vmq_ranch_config:delete_listener(Addr, Port),
            ListenerKey = {Addr, Port},
            ListenerConfig = vmq_config:get_env(listeners),
            NewListenerConfig =
            lists:foldl(
              fun({Type, ConfigForType}, Acc) ->
                      NewConfigForType = lists:keydelete(ListenerKey, 1, ConfigForType),
                      [{Type, NewConfigForType}|Acc]
              end, [], ListenerConfig),
            vmq_config:set_env(listeners, NewListenerConfig, false),
            [clique_status:text("Done")]
    end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

vmq_listener_restart_cmd() ->
    Cmd = ["vmq-admin", "listener", "restart"],
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
            case vmq_ranch_config:restart_listener(Addr, Port) of
                ok ->
                    [clique_status:text("Done")];
                {error, Reason} ->
                    Text = io_lib:format("can't restart listener due to '~p'", [Reason]),
                    [clique_status:alert([clique_status:text(Text)])]
            end
    end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

vmq_listener_show_cmd() ->
    Cmd = ["vmq-admin", "listener", "show"],
    KeySpecs = [],
    FlagSpecs = [],
    Callback =
    fun([], []) ->
            Table =
            lists:foldl(
              fun({Type, Ip, Port, Status, MP, MaxConns}, Acc) ->
                      [[{type, Type}, {status, Status}, {ip, Ip},
                        {port, Port}, {mountpoint, MP}, {max_conns, MaxConns}]
                       |Acc]
              end, [], vmq_ranch_config:listeners()),
              [clique_status:table(Table)]
    end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

cleanup_flags([{K, undefined}|Rest], Acc) ->
    %% e.g. from --websocket or --ssl
    cleanup_flags(Rest, [{K, true}|Acc]);
cleanup_flags([KV|Rest], Acc) ->
    cleanup_flags(Rest, [KV|Acc]);
cleanup_flags([], Acc) -> Acc.

vmq_listener_usage() ->
    ["vmq-admin listener <sub-command>\n\n",
     "  starts, modifies, and stops listeners.\n\n",
     "  Sub-commands:\n",
     "    start       Starts or modifies a listener\n",
     "    stop        Stops a listener\n",
     "    delete      Deletes a stopped listener\n",
     "    show        Shows all intalled listeners\n",
     "  Use --help after a sub-command for more details.\n"
    ].

vmq_listener_start_usage() ->
    ["vmq-admin listener start port=1883\n\n",
     "  Starts a new listener or modifies an existing listener. If no option\n",
     "  is specified a TCP listener is started listening on the given port\n\n",
     "General Options\n\n",
     "  -a, --address=IpAddress\n",
     "  -m, --mountpoint=Mountpoint\n",
     "  --nr_of_acceptors=NrOfAcceptors\n",
     "  --max_connections=[infinity | MaxConnections]\n\n",
     "WebSocket Options\n\n",
     "  --websocket\n",
     "      use the Websocket protocol as the underlying transport\n\n",
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
     "  --tls_version=TLSVersion\n",
     "      use this TLS version for the listener\n",
     "  --require_certificate\n",
     "      Use client certificates to authenticate your clients\n",
     "  --use_identity_as_username\n",
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

vmq_listener_delete_usage() ->
    ["vmq-admin listener delete\n\n",
     "  Deletes a stopped listener. If no option is given, the listener\n",
     "  listening on 0.0.0.0:1883 is deleted\n\n",
     "Options\n\n",
     "  -p, --port=PortNr\n",
     "  -a, --address=IpAddress\n\n"
    ].

vmq_listener_restart_usage() ->
    ["vmq-admin listener restart\n\n",
     "  Restarts a stopped listener. If no option is given, the listener\n",
     "  listening on 0.0.0.0:1883 is restarted\n\n",
     "Options\n\n",
     "  -p, --port=PortNr\n",
     "  -a, --address=IpAddress\n\n"
    ].
