-module(emqttd_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    {ok, AuthProviders} = application:get_env(auth_providers),
    {ok, _} = ranch:start_listener(tcp_mqtt, 1,
                                   ranch_tcp, [{port, 1883}], emqttd_handler_fsm,
                                   [{auth_providers, AuthProviders}]),
    emqttd_sup:start_link().

stop(_State) ->
    ok.
