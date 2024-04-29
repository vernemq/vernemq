%%%-------------------------------------------------------------------
%% @doc vmq_metric_plus top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(vmq_metrics_plus_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    {ok, {{one_for_one, 5, 10}, [?CHILD(vmq_metrics_plus, worker)]}}.
