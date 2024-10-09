-module(vmq_null_chars_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(Config) ->
    cover:start(),
    vmq_test_utils:setup(),
    [{ct_hooks, vmq_cth} | Config].

end_per_suite(_Config) ->
    vmq_test_utils:teardown(),
    _Config.

init_per_group(mqttv4, Config) ->
    Config1 = [{type, tcp}, {port, 1888}, {address, "127.0.0.1"} | Config],
    [{protover, 4} | start_listener(Config1)];
init_per_group(mqttv5, Config) ->
    Config1 = [{type, tcp}, {port, 1887}, {address, "127.0.0.1"} | Config],
    [{protover, 5} | start_listener(Config1)].

end_per_group(_Group, Config) ->
    stop_listener(Config),
    ok.

init_per_testcase(_Case, Config) ->
    %% reset config before each test
    vmq_server_cmd:set_config(allow_anonymous, true),
    vmq_config:configure_node(),
    Config.

end_per_testcase(_, Config) ->
    Config.

all() ->
    [
        {group, mqttv4},
        {group, mqttv5}
    ].

groups() ->
    Tests =
        [connect_null_id],
    [
        {mqttv4, Tests},
        {mqttv5, Tests}
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
connect_null_id(Config) ->
    Connect = packet:gen_connect(<<0>>, []),
    Connack = packet:gen_connack(0),
    try 
        case packet:do_client_connect(Connect, Connack, conn_opts(Config)) of
            {ok, Socket} ->
                ok = close(Socket, Config),
                {error, unexpected_connection}; 
            {error, Reason} ->
                {error, Reason}
        end
    catch
        error:undef -> 
            {ok, invalid_utf8_warning}
    end.


%% Helpers
stop_listener(Config) ->
    Port = proplists:get_value(port, Config),
    Address = proplists:get_value(address, Config),
    vmq_server_cmd:listener_stop(Port, Address, false).

close(Socket, Config) ->
    (transport(Config)):close(Socket).

transport(Config) ->
    case lists:keyfind(type, 1, Config) of
        {type, tcp} ->
            gen_tcp;
        {type, ssl} ->
            ssl;
        {type, ws} ->
            gen_tcp;
        {type, wss} ->
            ssl;
        {type, wsx} ->
            gen_tcp
    end.

conn_opts(Config) ->
    {port, Port} = lists:keyfind(port, 1, Config),
    {address, Address} = lists:keyfind(address, 1, Config),
    {type, Type} = lists:keyfind(type, 1, Config),
    TransportOpts =
        case Type of
            tcp ->
                [{transport, gen_tcp}, {conn_opts, []}];
            ssl ->
                [
                    {transport, ssl},
                    {conn_opts, [
                        {cacerts, load_cacerts()}
                    ]}
                ];
            ws ->
                [{transport, vmq_ws_transport}, {conn_opts, []}];
            wsx ->
                [{transport, vmq_ws_transport}, {conn_opts, []}]
        end,
    [
        {port, Port},
        {hostname, Address},
        lists:keyfind(propver, 1, Config)
        | TransportOpts
    ].

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
            [
                DER
             || {Type, DER, Cipher} <-
                    Contents,
                Type == 'Certificate',
                Cipher == 'not_encrypted'
            ]
    end.

start_listener(Config) ->
    {port, Port} = lists:keyfind(port, 1, Config),
    {address, Address} = lists:keyfind(address, 1, Config),
    {type, Type} = lists:keyfind(type, 1, Config),
    ProtVers = {allowed_protocol_versions, "3,4,5"},

    Opts1 =
        case Type of
            ssl ->
                [
                    {ssl, true},
                    {nr_of_acceptors, 5},
                    {cafile, ssl_path("all-ca.crt")},
                    {certfile, ssl_path("server.crt")},
                    {keyfile, ssl_path("server.key")},
                    {tls_version, "tlsv1.2"}
                ];
            tcp ->
                [];
            ws ->
                [{websocket, true}];
            wsx ->
                [
                    {websocket, true},
                    {proxy_xff_support, "true"},
                    {proxy_xff_trusted_intermediate, "127.0.0.1"},
                    {proxy_xff_use_cn_as_username, "true"},
                    {proxy_xff_cn_header, "x-ssl-client-cn"}
                ];
            wss ->
                [
                    {ssl, true},
                    {nr_of_acceptors, 5},
                    {cafile, ssl_path("all-ca.crt")},
                    {certfile, ssl_path("server.crt")},
                    {keyfile, ssl_path("server.key")},
                    {tls_version, "tlsv1.2"},
                    {websocket, true}
                ]
        end,
    {ok, _} = vmq_server_cmd:listener_start(Port, Address, [ProtVers | Opts1]),
    [{address, Address}, {port, Port}, {opts, Opts1} | Config].

ssl_path(File) ->
    Path = filename:dirname(
        proplists:get_value(source, ?MODULE:module_info(compile))
    ),
    filename:join([Path, "ssl", File]).
