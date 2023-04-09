%%%-------------------------------------------------------------------
%% @doc vmq_events_sidecar top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(vmq_events_sidecar_sup).
-include("../include/vmq_events_sidecar.hrl").

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================
-spec start_link() -> 'ignore' | {'error', any()} | {'ok', pid()}.
start_link() ->
    case supervisor:start_link({local, ?SERVER}, ?MODULE, []) of
        {ok, _} = Ret ->
            spawn(fun() ->
                Hooks = application:get_env(vmq_events_sidecar, hooks, "[]"),
                HooksList = vmq_schema_util:parse_list(Hooks),
                lists:foreach(
                    fun(Hook) -> vmq_events_sidecar_plugin:enable_event(Hook) end, HooksList
                )
            end),
            Ret;
        E ->
            E
    end.

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    SupFlags =
        #{strategy => one_for_one, intensity => 1, period => 5},
    ChildSpecs =
        [
            #{
                id => vmq_events_sidecar_plugin,
                start => {vmq_events_sidecar_plugin, start_link, []},
                restart => permanent,
                type => worker,
                modules => [vmq_events_sidecar_plugin]
            }
        ],

    Hostname = application:get_env(vmq_events_sidecar, hostname, "127.0.0.1"),
    Port = application:get_env(vmq_events_sidecar, port, 8890),
    PoolSize = application:get_env(vmq_events_sidecar, pool_size, 100),
    BacklogSize = application:get_env(vmq_events_sidecar, backlog_size, 4096),

    ClientOpts = [
        {address, Hostname},
        {port, Port},
        {protocol, shackle_tcp}
    ],
    PoolOtps = [
        {backlog_size, BacklogSize},
        {pool_size, PoolSize}
    ],
    ok = shackle_pool:start(?APP, ?CLIENT, ClientOpts, PoolOtps),
    {ok, {SupFlags, ChildSpecs}}.
