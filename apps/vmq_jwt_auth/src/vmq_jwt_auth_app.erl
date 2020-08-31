-module(vmq_jwt_auth_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    vmq_jwt_auth_sup:start_link().

stop(_State) ->
    ok.
