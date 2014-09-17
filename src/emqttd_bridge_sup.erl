-module(emqttd_bridge_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(Id, Mod, Type, Args), {Id, {Mod, start_link, Args}, permanent, 5000, Type, [Mod]}).

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
    {ok, {TCPConfig, _SSLConfig}} = application:get_env(emqttd_bridge, config),
    ChildSpecs =
    [begin
         ClientOpts = [{host, Host},
                       {port, Port},
                       {username, UserName},
                       {password, Password},
                       {client, ClientId},
                       {clean_session, CleanSession},
                       {keepalive_interval, KeepAliveTime},
                       {reconnect_timeout, RestartTimeout}
                       |case TryPrivate of
                            true ->
                                [{proto_version, 131}]; %% non-spec
                            false ->
                                []
                        end],
         ?CHILD(list_to_atom(lists:flatten(["emqttd_bridge-", Host, ":", integer_to_list(Port)])),
                emqttd_bridge, worker, [RegistryMFA, Topics, ClientOpts])
     end || {{Host, Port}, {CleanSession, ClientId, KeepAliveTime, RestartTimeout, UserName, Password, TryPrivate, Topics}}
        <- TCPConfig],
    {ok, { {one_for_one, 5, 10}, ChildSpecs} }.

