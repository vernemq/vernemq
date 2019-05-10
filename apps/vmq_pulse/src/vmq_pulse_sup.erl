%%%-------------------------------------------------------------------
%% @doc vmq_pulse top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(vmq_pulse_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).
-define(CHILD(Id, Mod, Type, Args), {Id, {Mod, start_link, Args},
                                     permanent, 5000, Type, [Mod]}).


%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    {ok, { {one_for_all, 0, 1}, [?CHILD(vmq_pulse_exporter, vmq_pulse_exporter, worker, [])]} }.

%%====================================================================
%% Internal functions
%%====================================================================
