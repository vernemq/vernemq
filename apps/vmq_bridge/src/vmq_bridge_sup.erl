%% Copyright 2018 Erlio GmbH Basel Switzerland (http://erl.io)
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(vmq_bridge_sup).

-behaviour(supervisor).
-behaviour(on_config_change_hook).

%% API
-export([start_link/0]).
-export([change_config/1]).

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
change_config(Configs) ->
    case lists:keyfind(vmq_bridge, 1, application:which_applications()) of
        false ->
            %% vmq_bridge app is loaded but not started
            ok;
        _ ->
            %% vmq_bridge app is started
            {vmq_bridge, BridgeConfig} = lists:keyfind(vmq_bridge, 1, Configs),
            {TCP, SSL} = proplists:get_value(config, BridgeConfig, {[], []}),
            Bridges = supervisor:which_children(?MODULE),
            reconfigure_bridges(tcp, Bridges, TCP),
            reconfigure_bridges(ssl, Bridges, SSL),
            stop_and_delete_unused(Bridges, lists:flatten([TCP, SSL]))
    end.

reconfigure_bridges(Type, Bridges, [{HostString, Opts}|Rest]) ->
    {Host,PortString} = list_to_tuple(string:tokens(HostString, ":")),
    {Port,_}=string:to_integer(PortString),
    Ref = ref(Host, Port),
    case lists:keyfind(Ref, 1, Bridges) of
        false ->
            start_bridge(Type, Ref, Host, Port, Opts);
        {_, Pid, _, _} when is_pid(Pid) -> % change existing bridge
            vmq_bridge:setopts(Pid, Type, Opts);
        _ ->
            ok
    end,
    reconfigure_bridges(Type, Bridges, Rest);
reconfigure_bridges(_, _, []) -> ok.

start_bridge(Type, Ref, Host, Port, Opts) ->
    {ok, RegistryMFA} = application:get_env(vmq_bridge, registry_mfa),
    ChildSpec = {Ref,
                 {vmq_bridge, start_link, [Host, Port, RegistryMFA]},
                 permanent, 5000, worker, [vmq_bridge]},
    case supervisor:start_child(?MODULE, ChildSpec) of
        {ok, Pid} ->
            case vmq_bridge:setopts(Pid, Type, Opts) of
                ok ->
                    {ok, Pid};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

stop_and_delete_unused(Bridges, Config) ->
    BridgesToDelete =
    lists:foldl(fun({HostString, _}, Acc) ->
                        {Host,PortString} = list_to_tuple(string:tokens(HostString, ":")),
                        {Port,_}=string:to_integer(PortString),
                        Ref = ref(Host, Port),
                        lists:keydelete(Ref, 1, Acc)
                end, Bridges, Config),
    lists:foreach(fun({Ref, _, _, _}) ->
                          supervisor:terminate_child(?MODULE, Ref),
                          supervisor:delete_child(?MODULE, Ref)
                  end, BridgesToDelete).

ref(Host, Port) ->
    {vmq_bridge, Host, Port}.

init([]) ->
    {ok, { {one_for_one, 5, 10}, []}}.
