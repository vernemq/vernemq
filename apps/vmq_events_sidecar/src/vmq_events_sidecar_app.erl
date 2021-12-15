%% Copyright Gojek

%%%-------------------------------------------------------------------
%% @doc vmq_events_sidecar public API
%% @end
%%%-------------------------------------------------------------------

-module(vmq_events_sidecar_app).
-include("../include/vmq_events_sidecar.hrl").

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    vmq_events_sidecar_cli:register_cli(),
    vmq_events_sidecar_sup:start_link().

%%--------------------------------------------------------------------
stop(_State) ->
    shackle_pool:stop(?APP),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
