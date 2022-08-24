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
-export([
    start_link/0,
    bridge_info/0,
    metrics/0,
    metrics_for_tests/0
]).
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

bridge_info() ->
    lists:map(
        fun({{_, Name, Host, Port}, Pid, _, _}) ->
            case vmq_bridge:info(Pid) of
                {error, not_started} ->
                    #{
                        name => Name,
                        host => Host,
                        port => Port,
                        out_buffer_size => $-,
                        out_buffer_max_size => $-,
                        out_buffer_dropped => $-,
                        process_mailbox_size => $-
                    };
                {ok, #{
                    out_queue_size := Size,
                    out_queue_max_size := Max,
                    out_queue_dropped := Dropped,
                    process_mailbox_size := MailboxSize
                }} ->
                    #{
                        name => Name,
                        host => Host,
                        port => Port,
                        out_buffer_size => Size,
                        out_buffer_max_size => Max,
                        out_buffer_dropped => Dropped,
                        process_mailbox_size => MailboxSize
                    }
            end
        end,
        supervisor:which_children(vmq_bridge_sup)
    ).
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

reconfigure_bridges(Type, Bridges, [{HostString, Opts} | Rest]) ->
    {Name, Host, PortString} = list_to_tuple(string:tokens(HostString, ":")),
    {Port, _} = string:to_integer(PortString),
    Ref = ref(Name, Host, Port),
    case lists:keyfind(Ref, 1, Bridges) of
        false ->
            start_bridge(Type, Ref, Host, Port, Opts);
        % maybe change existing bridge
        {_, Pid, _, _} when is_pid(Pid) ->
            case supervisor:get_childspec(?MODULE, Ref) of
                {ok, #{start := {vmq_bridge, start_link, [Type, Host, Port, _RegistryMFA, Opts]}}} ->
                    % bridge is unchanged
                    ignore;
                _ ->
                    %% restart the bridge
                    ok = stop_bridge(Ref),
                    start_bridge(Type, Ref, Host, Port, Opts)
            end;
        _ ->
            ok
    end,
    reconfigure_bridges(Type, Bridges, Rest);
reconfigure_bridges(_, _, []) ->
    ok.

start_bridge(Type, Ref, Host, Port, Opts) ->
    {ok, RegistryMFA} = application:get_env(vmq_bridge, registry_mfa),
    {_, Name, _, _} = Ref,
    ChildSpec =
        {Ref, {vmq_bridge, start_link, [Type, Host, Port, RegistryMFA, [{name, Name} | Opts]]},
            permanent, 5000, worker, [vmq_bridge]},
    case supervisor:start_child(?MODULE, ChildSpec) of
        {ok, Pid} ->
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

stop_and_delete_unused(Bridges, Config) ->
    BridgesToDelete =
        lists:foldl(
            fun({HostString, _}, Acc) ->
                {Name, Host, PortString} = list_to_tuple(string:tokens(HostString, ":")),
                {Port, _} = string:to_integer(PortString),
                Ref = ref(Name, Host, Port),
                lists:keydelete(Ref, 1, Acc)
            end,
            Bridges,
            Config
        ),
    lists:foreach(
        fun({Ref, _, _, _}) ->
            stop_bridge(Ref)
        end,
        BridgesToDelete
    ).

stop_bridge(Ref) ->
    ets:delete(vmq_bridge_meta, Ref),
    supervisor:terminate_child(?MODULE, Ref),
    supervisor:delete_child(?MODULE, Ref).

ref(Name, Host, Port) ->
    {vmq_bridge, Name, Host, Port}.

init([]) ->
    vmq_bridge_meta = ets:new(vmq_bridge_meta, [named_table, set, public]),
    {ok, {{one_for_one, 5, 10}, []}}.

% only for bridge test suite compatibility
metrics_for_tests() ->
    ets:foldl(
        fun({ClientPid}, Acc) ->
            case gen_mqtt_client:stats(ClientPid) of
                undefined ->
                    ets:delete(vmq_bridge_meta, ClientPid),
                    Acc;
                #{dropped := Dropped} ->
                    [
                        {counter, [], {vmq_bridge_queue_drop, ClientPid}, vmq_bridge_dropped_msgs,
                            <<"The number of dropped messages (queue full)">>, Dropped}
                        | Acc
                    ]
            end
        end,
        [],
        vmq_bridge_meta
    ).

metrics() ->
    Drops =
        ets:foldl(
            fun({ClientPid}, Acc) ->
                case gen_mqtt_client:stats(ClientPid) of
                    undefined ->
                        ets:delete(vmq_bridge_meta, ClientPid),
                        Acc;
                    #{dropped := Dropped} ->
                        [
                            {counter, [], {vmq_bridge_queue_drop, ClientPid},
                                vmq_bridge_dropped_msgs,
                                <<"The number of dropped messages (queue full)">>, Dropped}
                            | Acc
                        ]
                end
            end,
            [],
            vmq_bridge_meta
        ),

    lists:foldl(
        fun({{_, Name, _Host, _Port}, BridgePid, _, _}, Acc2) ->
            case vmq_bridge:get_metrics(BridgePid) of
                undefined ->
                    Acc2;
                {ok, #{
                    vmq_bridge_publish_out_0 := PO0,
                    vmq_bridge_publish_out_1 := PO1,
                    vmq_bridge_publish_out_2 := PO2,
                    vmq_bridge_publish_in_0 := PI0,
                    vmq_bridge_publish_in_1 := PI1,
                    vmq_bridge_publish_in_2 := PI2
                }} ->
                    lists:flatten([
                        [
                            [
                                {counter, [], {vmq_bridge_publish_out_0, BridgePid},
                                    label(Name, vmq_bridge_publish_out_0),
                                    <<"The number of QoS 0 messages the bridge has (re)-published (TCP)">>,
                                    PO0},
                                {counter, [], {vmq_bridge_publish_out_1, BridgePid},
                                    label(Name, vmq_bridge_publish_out_1),
                                    <<"The number of QoS 1 messages the bridge has (re)-published (TCP)">>,
                                    PO1},
                                {counter, [], {vmq_bridge_publish_out_2, BridgePid},
                                    label(Name, vmq_bridge_publish_out_2),
                                    <<"The number of QoS 2 messages the bridge has (re)-published (TCP)">>,
                                    PO2},
                                {counter, [], {vmq_bridge_publish_in_0, BridgePid},
                                    label(Name, vmq_bridge_publish_in_0),
                                    <<"The number of QoS 0 messages the bridge has consumed (TCP)">>,
                                    PI0},
                                {counter, [], {vmq_bridge_publish_in_1, BridgePid},
                                    label(Name, vmq_bridge_publish_in_1),
                                    <<"The number of QoS 1 messages the bridge has consumed (TCP)">>,
                                    PI1},
                                {counter, [], {vmq_bridge_publish_in_2, BridgePid},
                                    label(Name, vmq_bridge_publish_in_2),
                                    <<"The number of QoS 2 messages the bridge has consumed (TCP)">>,
                                    PI2}
                            ]
                            | Acc2
                        ]
                        | Drops
                    ])
            end
        end,
        [],
        supervisor:which_children(vmq_bridge_sup)
    ).

label(Name, Metric) ->
    % this will create 6 labels (atoms) per bridge for the metrics above. atoms will be the same in every call after that.
    list_to_atom(lists:concat([Name, "_", Metric])).
