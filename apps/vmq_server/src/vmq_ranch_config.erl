%% Copyright 2018 Erlio GmbH Basel Switzerland (http://erl.io)
%% Copyright 2018-2024 Octavo Labs/VerneMQ (https://vernemq.com/)
%% and Individual Contributors.
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

-module(vmq_ranch_config).

-behaviour(gen_server).
-include_lib("kernel/include/logger.hrl").

%% API
-export([
    start_link/0,
    start_listener/4,
    reconfigure_listeners/1,
    stop_listener/2,
    stop_listener/3,
    stop_all_mqtt_listeners/1,
    delete_listener/2,
    restart_listener/2,
    get_listener_config/2,
    listeners/0,
    listeners/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop_all_mqtt_listeners(KillSessions) ->
    lists:foreach(
        fun
            ({mqtt, Addr, Port, _, _, _, _, _}) -> stop_listener(Addr, Port, KillSessions);
            ({mqtts, Addr, Port, _, _, _, _, _}) -> stop_listener(Addr, Port, KillSessions);
            ({mqttws, Addr, Port, _, _, _, _, _}) -> stop_listener(Addr, Port, KillSessions);
            ({mqttwss, Addr, Port, _, _, _, _, _}) -> stop_listener(Addr, Port, KillSessions);
            (_) -> ignore
        end,
        listeners()
    ).

stop_listener(Addr, Port) ->
    stop_listener(Addr, Port, false).
stop_listener(Addr, Port, KillSessions) when is_list(Port) ->
    stop_listener(Addr, list_to_integer(Port), KillSessions);
stop_listener(Addr, Port, KillSessions) ->
    AAddr = addr(Addr),
    Ref = listener_name(AAddr, Port),
    case ranch_server:get_listener_sup(Ref) of
        Pid when KillSessions, is_pid(Pid) ->
            ranch:stop_listener(Ref);
        Pid when is_pid(Pid) ->
            ranch:suspend_listener(Ref)
    end.

restart_listener(Addr, Port) ->
    AAddr = addr(Addr),
    Ref = listener_name(AAddr, Port),
    ranch:resume_listener(Ref).

delete_listener(Addr, Port) ->
    AAddr = addr(Addr),
    Ref = listener_name(AAddr, Port),
    delete_listener(Ref).

delete_listener(Ref) ->
    ranch:stop_listener(Ref).

start_listener_clear(Ref, TransportMod, TransportOptions, ProtocolOpts) ->
    Ret =
        case TransportMod of
            ranch_tcp ->
                cowboy:start_clear(
                    Ref,
                    TransportOptions,
                    ProtocolOpts
                );
            ranch_ssl ->
                cowboy:start_tls(
                    Ref,
                    TransportOptions,
                    ProtocolOpts
                )
        end,

    case Ret of
        {ok, _} -> ok;
        Error -> Error
    end.

start_listener(Type, Addr, Port, Opts) when is_list(Opts) ->
    TCPOpts = vmq_config:get_env(tcp_listen_options),
    SocketOpts = TCPOpts ++ socket_opts_for_type(Type, Opts),
    start_listener(Type, Addr, Port, {SocketOpts, Opts});
start_listener(Type, Addr, Port, {SocketOpts, Opts}) ->
    AAddr = addr(Addr),
    Ref = listener_name(AAddr, Port),
    MaxConns = proplists:get_value(
        max_connections,
        Opts,
        vmq_config:get_env(max_connections)
    ),
    vmq_ssl_psk:init(Opts),
    NrOfAcceptors = proplists:get_value(
        nr_of_acceptors,
        Opts,
        vmq_config:get_env(nr_of_acceptors)
    ),
    ProtocolOpts = protocol_opts_for_type(Type, Opts),
    TransportMod = transport_for_type(Type),
    TransportOptions = maps:from_list(
        [
            {socket_opts, [{ip, AAddr}, {port, Port} | SocketOpts]},
            {num_acceptors, NrOfAcceptors},
            {max_connections, MaxConns}
            | transport_opts_for_type(Type, Opts)
        ]
    ),
    case protocol_for_type(Type) of
        cowboy_clear ->
            start_listener_clear(Ref, TransportMod, TransportOptions, ProtocolOpts);
        _ ->
            case
                ranch:start_listener(
                    Ref,
                    TransportMod,
                    TransportOptions,
                    protocol_for_type(Type),
                    ProtocolOpts
                )
            of
                {ok, _} -> ok;
                Error -> Error
            end
    end.
listeners() ->
    listeners(select, false, false).

listeners(with_tls) ->
    listeners(select, true, false);
listeners(with_mqtt) ->
    listeners(select, false, true);
listeners(with_tls_and_mqtt) ->
    listeners(select, true, true).

listeners(select, AddTLSInfo, AddMQTTInfo) ->
    maps:fold(
        fun({Ip, Port}, ConfigMap, Acc) ->
            {ok, {Type, Opts}} = get_listener_config(Ip, Port),
            MountPoint = proplists:get_value(mountpoint, Opts, ""),
            MaxConnections = proplists:get_value(
                max_connections,
                Opts,
                vmq_config:get_env(max_connections)
            ),
            ActiveConnections = maps:get(active_connections, ConfigMap),
            % the highest number of connections seen
            AllConnections = maps:get(all_connections, ConfigMap),
            Status = maps:get(status, ConfigMap),
            StrIp =
                case Ip of
                    {local, FS} -> {local, FS};
                    _ -> inet:ntoa(Ip)
                end,
            StrPort = integer_to_list(Port),
            AllowedProtocolVersionsList = proplists:get_value(allowed_protocol_versions, Opts, ""),
            AllowedProtocolVersions = vmq_util:mqtt_version_to_string(AllowedProtocolVersionsList),
            AllowAnonymousOverride = proplists:get_value(allow_anonymous_override, Opts, ""),
            % TLS
            CertFile = proplists:get_value(certfile, Opts, ""),
            TLS = proplists:get_value(tls_version, Opts, ""),
            CAFile = proplists:get_value(ca_file, Opts, ""),
            KeyFile = proplists:get_value(keyfile, Opts, ""),
            RequireCertificate = proplists:get_value(require_certificate, Opts, ""),
            UseIDAsUsername = proplists:get_value(use_identity_as_username, Opts, ""),
            PSKSupport = proplists:get_value(psk_support, Opts, ""),
            PSKFile = proplists:get_value(pskfile, Opts, ""),

            Base = [
                Type,
                StrIp,
                StrPort,
                Status,
                MountPoint,
                MaxConnections,
                ActiveConnections,
                AllConnections
            ],
            Base2 =
                case AddTLSInfo of
                    true ->
                        Base ++
                            [
                                TLS,
                                CertFile,
                                CAFile,
                                KeyFile,
                                RequireCertificate,
                                UseIDAsUsername,
                                PSKSupport,
                                PSKFile
                            ];
                    _ ->
                        Base
                end,
            Base3 =
                case AddMQTTInfo of
                    true ->
                        Base2 ++ [AllowedProtocolVersions, AllowAnonymousOverride];
                    _ ->
                        Base2
                end,
            [
                list_to_tuple(Base3)
                | Acc
            ]
        end,
        [],
        ranch:info()
    ).

get_listener_config(Addr, Port) ->
    get_listener_config(Addr, Port, vmq_config:get_env(listeners)).
get_listener_config(Addr, Port, [{T, Listeners} | Rest]) ->
    case lists:keyfind({Addr, Port}, 1, Listeners) of
        false ->
            get_listener_config(Addr, Port, Rest);
        {_, Opts} ->
            {ok, {T, Opts}}
    end;
get_listener_config(_, _, []) ->
    {error, not_found}.

reconfigure_listeners(Config) ->
    TCPListenOptions = proplists:get_value(
        tcp_listen_options,
        Config,
        vmq_config:get_env(tcp_listen_options)
    ),
    ListenerConfig = proplists:get_value(
        listeners,
        Config,
        vmq_config:get_env(listeners)
    ),
    Listeners = ranch_server:get_listener_sups(),
    reconfigure_listeners(TCPListenOptions, ListenerConfig, Listeners).
reconfigure_listeners(TCPListenOptions, [{T, Config} | Rest], Listeners) ->
    NewListeners = reconfigure_listeners_for_type(T, Config, TCPListenOptions, Listeners),
    reconfigure_listeners(TCPListenOptions, Rest, NewListeners);
reconfigure_listeners(_, [], ListenersToDelete) ->
    lists:foreach(
        fun({Ref, _, _, _}) ->
            delete_listener(Ref)
        end,
        ListenersToDelete
    ).

addr(Addr) when is_list(Addr) ->
    {ok, Ip} = inet:parse_address(Addr),
    Ip;
addr(Addr) ->
    Addr.

reconfigure_listeners_for_type(Type, [{{Addr, Port}, Opts} | Rest], TCPOpts, Listeners) ->
    SocketOpts = TCPOpts ++ socket_opts_for_type(Type, Opts),
    case start_listener(Type, Addr, Port, {SocketOpts, Opts}) of
        ok ->
            ok;
        {error, Reason} ->
            ?LOG_ERROR(
                "can't reconfigure ~p listener(~p, ~p) with Options ~p due to ~p",
                [Type, Addr, Port, Opts, Reason]
            )
    end,
    Key = {ranch_listener_sup, listener_name(addr(Addr), Port)},
    reconfigure_listeners_for_type(Type, Rest, TCPOpts, lists:keydelete(Key, 1, Listeners));
reconfigure_listeners_for_type(_, [], _, Listeners) ->
    Listeners.

listener_name(Ip, Port) ->
    {Ip, Port}.

transport_for_type(mqtt) -> ranch_tcp;
transport_for_type(mqtts) -> ranch_ssl;
transport_for_type(mqttws) -> ranch_tcp;
transport_for_type(mqttwss) -> ranch_ssl;
transport_for_type(http) -> ranch_tcp;
transport_for_type(https) -> ranch_ssl;
transport_for_type(vmq) -> ranch_tcp;
transport_for_type(vmqs) -> ranch_ssl.

protocol_for_type(mqtt) -> vmq_ranch;
protocol_for_type(mqtts) -> vmq_ranch;
protocol_for_type(mqttws) -> cowboy_clear;
protocol_for_type(mqttwss) -> cowboy_clear;
protocol_for_type(http) -> cowboy_clear;
protocol_for_type(https) -> cowboy_clear;
protocol_for_type(vmq) -> vmq_cluster_com;
protocol_for_type(vmqs) -> vmq_cluster_com.

socket_opts_for_type(Type, Opts) ->
    socket_opts(transport_for_type(Type), Opts).
socket_opts(ranch_tcp, _) -> [];
socket_opts(ranch_ssl, Opts) -> vmq_ssl:opts(Opts).

transport_opts_for_type(Type, Opts) ->
    transport_opts(transport_for_type(Type), Opts).
transport_opts(ranch_tcp, _) ->
    [];
transport_opts(ranch_ssl, Opts) ->
    HandshakeTimeout = proplists:get_value(
        tls_handshake_timeout,
        Opts,
        vmq_config:get_env(tls_handshake_timeout, 5000)
    ),
    [{handshake_timeout, HandshakeTimeout}].

protocol_opts_for_type(Type, Opts) ->
    protocol_opts(protocol_for_type(Type), Type, Opts).
protocol_opts(vmq_ranch, _, Opts) ->
    case proplists:get_value(proxy_protocol, Opts, false) of
        false -> default_session_opts(Opts);
        true -> [{proxy_header, true} | default_session_opts(Opts)]
    end;
protocol_opts(cowboy_clear, Type, Opts) when
    (Type == mqttws) or (Type == mqttwss)
->
    MOpts =
        case proplists:get_value(proxy_protocol, Opts, false) of
            false -> default_session_opts(Opts);
            true -> [{proxy_header, true} | default_session_opts(Opts)]
        end,
    maps:merge(maps:from_list(MOpts), #{
        env => #{dispatch => dispatch(Type, Opts)},
        stream_handlers => [vmq_cowboy_websocket_h, cowboy_stream_h]
    });
protocol_opts(cowboy_clear, _, Opts) ->
    Apply = fun(Mod, Fun, Param) ->
        try
            apply(Mod, Fun, Param)
        catch
            E:R ->
                ?LOG_ERROR("can't setup HTTP modules due to ~p:~p", [E, R]),
                []
        end
    end,
    {_, ConfigMod} = lists:keyfind(config_mod, 1, Opts),
    {_, ConfigFun} = lists:keyfind(config_fun, 1, Opts),
    Routes =
        case
            {ConfigMod, ConfigFun,
                lists:member({ConfigFun, 1}, apply(ConfigMod, module_info, [exports]))}
        of
            {Mod, Fun, false} -> Apply(Mod, Fun, []);
            {Mod, Fun, true} -> Apply(Mod, Fun, [Opts])
        end,
    CowboyRoutes = [{'_', Routes}],
    Dispatch = cowboy_router:compile(CowboyRoutes),
    MaxRequestLength = proplists:get_value(max_request_line_length, Opts, 8000),
    #{env => #{dispatch => Dispatch}, max_request_line_length => MaxRequestLength, opts => Opts};
protocol_opts(vmq_cluster_com, _, Opts) ->
    Opts.

default_session_opts(Opts) ->
    MaybeSSLDefaults =
        case lists:keyfind(use_identity_as_username, 1, Opts) of
            false -> [];
            {_, V} -> [{use_identity_as_username, V}]
        end,
    MaybeProxyDefaults =
        case lists:keyfind(proxy_protocol_use_cn_as_username, 1, Opts) of
            false -> MaybeSSLDefaults;
            {_, V1} -> [{proxy_protocol_use_cn_as_username, V1} | MaybeSSLDefaults]
        end,
    MaybeProxyDefaults2 =
        case lists:keyfind(proxy_xff_trusted_intermediate, 1, Opts) of
            false ->
                MaybeProxyDefaults;
            {_, V2} ->
                [
                    {xff_proxy, proplists:get_value(proxy_xff_support, Opts, false)},
                    {proxy_xff_trusted_intermediate, V2},
                    {xff_cn_header, proplists:get_value(proxy_xff_cn_header, Opts, undefined)},
                    {xff_use_cn_as_username,
                        proplists:get_value(proxy_xff_use_cn_as_username, Opts, false)}
                    | MaybeProxyDefaults
                ]
        end,
    AllowedProtocolVersions = proplists:get_value(allowed_protocol_versions, Opts, [3, 4]),
    MaxConnectionLifeTime = proplists:get_value(max_connection_lifetime, Opts, 0),
    AllowAnonymousOverride = proplists:get_value(allow_anonymous_override, Opts, false),
    BufferSizes = proplists:get_value(buffer_sizes, Opts, undefined),
    [
        {mountpoint, proplists:get_value(mountpoint, Opts, "")},
        {allowed_protocol_versions, AllowedProtocolVersions},
        {max_connection_lifetime, MaxConnectionLifeTime},
        {allow_anonymous_override, AllowAnonymousOverride},
        {buffer_sizes, BufferSizes}
        | MaybeProxyDefaults2
    ].

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    process_flag(trap_exit, true),
    Env = vmq_config:get_all_env(vmq_server),
    _ = configure_listeners(Env, []),
    {ok, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({last_val, Key, Val}, _From, LastVals) ->
    case lists:keyfind(Key, 1, LastVals) of
        false ->
            {reply, nil, [{Key, Val} | LastVals]};
        {Key, Val} ->
            %% unchanged
            {reply, Val, LastVals};
        {Key, OldVal} ->
            {reply, OldVal, [{Key, Val} | lists:keydelete(Key, 1, LastVals)]}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    %% Stop the all listeners to prevent clients from connecting
    %% while shutting down.
    KillSessions = false,
    lists:foreach(
        fun
            ({ranch_server, _, _, _}) ->
                ok;
            ({{ranch_listener_sup, {Ip, Port}}, _Status, supervisor, _}) ->
                stop_listener(Ip, Port, KillSessions)
        end,
        supervisor:which_children(ranch_sup)
    ),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
configure_listeners([{listeners, _} = Item | Rest], Acc) ->
    configure_listeners(Rest, [Item | Acc]);
configure_listeners([{tcp_listen_options, _} = Item | Rest], Acc) ->
    configure_listeners(Rest, [Item | Acc]);
configure_listeners([_ | Rest], Acc) ->
    configure_listeners(Rest, Acc);
configure_listeners([], []) ->
    %% no need to reconfigure listeners
    ok;
configure_listeners([], Acc) ->
    vmq_ranch_config:reconfigure_listeners(Acc).

dispatch(Type, Opts) ->
    maybe_proxy(proplists:get_value(proxy_protocol, Opts, false), Type, Opts).
maybe_proxy(false, Type, Opts) ->
    cowboy_router:compile(
        [{'_', [{"/mqtt", vmq_websocket, [{type, Type} | default_session_opts(Opts)]}]}]
    );
maybe_proxy(true, Type, Opts) ->
    cowboy_router:compile(
        [
            {'_', [
                {"/mqtt", vmq_websocket, [
                    {proxy_header, true} | [{type, Type} | default_session_opts(Opts)]
                ]}
            ]}
        ]
    ).
