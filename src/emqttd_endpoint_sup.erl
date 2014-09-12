%%%-------------------------------------------------------------------
%%% @author graf
%%% @copyright (C) 2014, graf
%%% @doc
%%%
%%% @end
%%% Created : 2014-09-03 15:49:38.393546
%%%-------------------------------------------------------------------
-module(emqttd_endpoint_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,
         add_endpoint/5,
         add_ws_endpoint/5]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).
-define(APP, emqttd_server).

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
    [ChildSpec] = generate_childspecs([{{Ip,Port}, {MaxConnections, NrOfAcceptors, MountPoint}}], ranch_tcp,
                                      emqttd_tcp),
    supervisor:start_child(?SERVER, ChildSpec).

add_ws_endpoint(Ip, Port, MaxConnections, NrOfAcceptors, MountPoint) ->
    [ChildSpec] = generate_childspecs([{{Ip,Port}, {MaxConnections, NrOfAcceptors, MountPoint}}], ranch_tcp,
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
    {ok, {TCPListeners, _SSLListeners, WSListeners}} = application:get_env(?APP, listeners),

    MQTTEndpoints = generate_childspecs(TCPListeners, ranch_tcp, emqttd_tcp),
    %MQTTSEndpoints = generate_childspecs(NrOfAcceptors, SSLListeners, ranch_tcp, emqttd_tcp, handler_opts(mqtt)),
    MQTTSEndpoints = [],
    MQTTWSEndpoints = generate_childspecs(WSListeners, ranch_tcp, cowboy_protocol),
    {ok, { {one_for_one, 5, 10}, MQTTEndpoints ++ MQTTSEndpoints ++ MQTTWSEndpoints}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
generate_childspecs(Listeners, Transport, Protocol) ->
    [ranch:child_spec(list_to_atom("mqtt_"++integer_to_list(Port)),
                      NrOfAcceptors, Transport,
                      [{ip, case is_list(Addr) of
                                true -> {ok, Ip} = inet:parse_address(Addr),
                                        Ip;
                                false -> Addr
                            end }, {port, Port}, {max_connections, MaxConnections}],
                      Protocol, handler_opts(Protocol, [{mountpoint, MountPoint}]))
     || {{Addr, Port}, {MaxConnections, NrOfAcceptors, MountPoint}} <- Listeners].

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
    [{msg_log_handler, MsgLogHandler}|Opts].




