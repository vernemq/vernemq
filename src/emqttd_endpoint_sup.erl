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
         add_endpoint/1,
         add_ws_endpoint/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

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

add_endpoint(Endpoint) ->
    [ChildSpec] = generate_childspecs([Endpoint], ranch_tcp, emqttd_tcp, handler_opts(mqtt)),
    supervisor:start_child(?SERVER, ChildSpec).

add_ws_endpoint(Endpoint) ->
    [ChildSpec] = generate_childspecs([Endpoint], ranch_tcp, cowboy_protocol, handler_opts(mqttws)),
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
    MQTTEndpoints =
    case application:get_env(emqttd, mqtt_endpoints) of
        {ok, Endpoints} ->
            generate_childspecs(Endpoints, ranch_tcp, emqttd_tcp, handler_opts(mqtt));
        _ ->
            []
    end,
    MQTTWSEndpoints =
    case application:get_env(emqttd, mqttws_endpoints) of
        {ok, EndpointsWS} ->
            generate_childspecs(EndpointsWS, ranch_tcp, cowboy_protocol, handler_opts(mqttws));
        _ ->
            []
    end,
    {ok, { {one_for_one, 5, 10}, MQTTEndpoints ++ MQTTWSEndpoints}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
generate_childspecs(Endpoints, Transport, Protocol, Opts) ->
    [ranch:child_spec(list_to_atom("mqtt_"++integer_to_list(Port)),
                      NrOfAcceptors, Transport,
                      [{ip, Ip}, {port, Port}],
                      Protocol, Opts)
     || {Ip, Port, NrOfAcceptors} <- Endpoints].

handler_opts(mqttws) ->
    Dispatch = cowboy_router:compile([
                                      {'_', [
                                             {"/mqtt", emqttd_ws, handler_opts(mqtt)}
                                            ]}
                                     ]),
    [{env, [{dispatch, Dispatch}]}];

handler_opts(mqtt) ->
    {ok, AuthProviders} = application:get_env(emqttd, auth_providers),
    {ok, MsgLogHandler} = application:get_env(emqttd, msg_log_handler),
    [{auth_providers, AuthProviders},
     {msg_log_handler, MsgLogHandler}].



