%%%-------------------------------------------------------------------
%% @doc vmq_metric_plus public API
%% @end
%%%-------------------------------------------------------------------

-module(vmq_metrics_plus_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    vmq_metrics_plus_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
