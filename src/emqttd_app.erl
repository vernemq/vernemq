-module(emqttd_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    {ok, AuthProviders} = application:get_env(auth_providers),
    Port =
    case proplists:get_value(emqttd_port, init:get_arguments()) of
        [StringPort] -> list_to_integer(StringPort);
        undefined -> case application:get_env(emqttd_port) of
                  {ok, P} -> P;
                  _ -> 1883
              end
    end,
    {ok, MsgLogHandler} = application:get_env(msg_log_handler),
    {ok, _} = ranch:start_listener(tcp_mqtt, 1,
                                   ranch_tcp, [{port, Port}], emqttd_handler_fsm,
                                   [{auth_providers, AuthProviders},
                                    {msg_log_handler, MsgLogHandler}]),
    emqttd_sup:start_link().

stop(_State) ->
    ok.
