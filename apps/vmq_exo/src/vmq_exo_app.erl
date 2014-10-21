-module(vmq_exo_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    ExometerConfig = application:get_env(vmq_exo, report, []),
    application:stop(exometer),
    application:set_env(exometer, report, ExometerConfig),
    application:start(exometer),

    vmq_exo_sup:start_link().

stop(_State) ->
    ok.
