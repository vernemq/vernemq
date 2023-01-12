-module(vmq_generic_offline_msg_store_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    SupFlags =
        #{strategy => one_for_one, intensity => 1, period => 5},
    ChildSpecs =
        [#{id => vmq_generic_offline_msg_store,
            start => {vmq_generic_offline_msg_store, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [vmq_generic_offline_msg_store]}],
    {ok, {SupFlags, ChildSpecs}}.
