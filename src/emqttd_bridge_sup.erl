-module(emqttd_bridge_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type, Args), {I, {I, start_link, [Args]}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, RegistryMFA} = application:get_env(emqttd_bridge, registry_mfa),
    {ok, Configs} = application:get_env(emqttd_bridge, config),
    ChildSpecs =
    [?CHILD({?MODULE, Name}, worker, [RegistryMFA, BridgeConfig, ClientOpts])
     || {Name, {BridgeConfig, ClientOpts}} <- Configs],
    {ok, { {one_for_one, 5, 10}, ChildSpecs} }.

