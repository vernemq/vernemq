%%%-------------------------------------------------------------------
%% @doc vmq_webhooks top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(vmq_webhooks_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

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
    SupFlags =
        #{strategy => one_for_one, intensity => 1, period => 5},
    ChildSpecs =
        [#{id => vmq_webhooks_plugin,
           start => {vmq_webhooks_plugin, start_link, []},
           restart => permanent,
           type => worker,
           modules => [vmq_webhooks_plugin]}],
    {ok, {SupFlags, ChildSpecs}}.
%%====================================================================
%% Internal functions
%%====================================================================
