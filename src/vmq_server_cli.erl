%% Copyright 2014 Erlio GmbH Basel Switzerland (http://erl.io)
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

-module(vmq_server_cli).
-export([init_registry/0]).
-export([command/1,
         command/2,
         register_cli/0]).

-behaviour(clique_handler).

init_registry() ->
    F = fun() -> vmq_cluster:nodes() end,
    clique:register_node_finder(F),
    clique:register([?MODULE, vmq_plugin_cli]).

command(Cmd) ->
    command(Cmd, true).
command(Cmd, true) ->
    clique:run(Cmd);
command(Cmd, false) ->
    M0 = clique_command:match(Cmd),
    M1 = clique_parser:parse(M0),
    M2 = clique_parser:extract_global_flags(M1),
    M3 = clique_parser:validate(M2),
    {Res, _, _} = clique_command:run(M3),
    parse_res(Res, Res).

parse_res(error, error) ->
    {error, unhandled_clique_error};
parse_res([{alert, _}|_], Res) ->
    {error, Res};
parse_res([{_, _}|Rest], Res) ->
    parse_res(Rest, Res);
parse_res([], Res) ->
    {ok, Res}.




register_cli() ->
    vmq_config_cli:register_config(),
    register_cli_usage(),
    vmq_server_start_cmd(),
    vmq_server_stop_cmd(),
    vmq_server_status_cmd(),
    vmq_server_metrics_cmd(),
    vmq_server_metrics_reset_cmd(),
    vmq_cluster_join_cmd(),
    vmq_cluster_leave_cmd(),
    vmq_cluster_upgrade_cmd(),

    vmq_session_list_cmd(),

    vmq_listener_cli:register_server_cli(),
    ok.

register_cli_usage() ->
    clique:register_usage(["vmq-admin"], usage()),
    clique:register_usage(["vmq-admin", "node"], node_usage()),
    clique:register_usage(["vmq-admin", "node", "start"], start_usage()),
    clique:register_usage(["vmq-admin", "node", "stop"], stop_usage()),
    clique:register_usage(["vmq-admin", "node", "upgrade"], upgrade_usage()),

    clique:register_usage(["vmq-admin", "cluster"], cluster_usage()),
    clique:register_usage(["vmq-admin", "cluster", "join"], join_usage()),
    clique:register_usage(["vmq-admin", "cluster", "leave"], leave_usage()),

    clique:register_usage(["vmq-admin", "session"], session_usage()),
    clique:register_usage(["vmq-admin", "session", "list"], vmq_session_list_usage()),

    clique:register_usage(["vmq-admin", "metrics"], metrics_usage()),
    ok.

vmq_server_stop_cmd() ->
    Cmd = ["vmq-admin", "node", "stop"],
    Callback = fun(_, _, _) ->
                       _ = ensure_all_stopped(vmq_server),
                       [clique_status:text("Done")]
               end,
    clique:register_command(Cmd, [], [], Callback).

vmq_server_start_cmd() ->
    Cmd = ["vmq-admin", "node", "start"],
    Callback = fun(_, _, _) ->
                       _ = application:ensure_all_started(vmq_server),
                       [clique_status:text("Done")]
               end,
    clique:register_command(Cmd, [], [], Callback).

vmq_server_status_cmd() ->
    Cmd = ["vmq-admin", "cluster", "status"],
    Callback = fun(_, _, _) ->
                       VmqStatus = vmq_cluster:status(),
                       NodeTable =
                       lists:foldl(fun({NodeName, IsReady}, Acc) ->
                                           [[{'Node', NodeName},
                                             {'Running', IsReady}]|Acc]
                                   end, [], VmqStatus),
                       [clique_status:table(NodeTable)]
               end,
    clique:register_command(Cmd, [], [], Callback).

vmq_server_metrics_cmd() ->
    Cmd = ["vmq-admin", "metrics", "show"],
    Callback = fun(_, _, _) ->
                       lists:foldl(
                         fun({Type, Metric, Val}, Acc) ->
                                 SType = atom_to_list(Type),
                                 SMetric = atom_to_list(Metric),
                                 SVal =
                                 case Val of
                                     V when is_integer(V) ->
                                         integer_to_list(V);
                                     V when is_float(V) ->
                                         float_to_list(V)
                                 end,
                                 Line = [SType, ".", SMetric, " = ", SVal],
                                 [clique_status:text(lists:flatten(Line))|Acc]
                         end, [], vmq_metrics:metrics())
               end,
    clique:register_command(Cmd, [], [], Callback).

vmq_server_metrics_reset_cmd() ->
    Cmd = ["vmq-admin", "metrics", "reset"],
    Callback = fun(_, _, _) ->
                       vmq_metrics:reset_counters(),
                       [clique_status:text("Done")]
               end,
    clique:register_command(Cmd, [], [], Callback).



vmq_cluster_leave_cmd() ->
    %% is must be ensured that the leaving node has NO online session e.g. by
    %% 1. stopping the listener via vmq-admin listener stop --kill_sessions
    %% 2. the killed sessions will reconnect to another node (external load balancer)
    %% 3. the reconnected sessions on the other nodes remap subscriptions and
    Cmd = ["vmq-admin", "cluster", "leave"],
    KeySpecs = [{node, [{typecast, fun (Node) ->
                                           list_to_atom(Node)
                                   end}]}],
    FlagSpecs = [{kill, [{shortname, "k"},
                         {longname, "kill_sessions"}]},
                 {summary_interval, [{shortname, "i"},
                                     {longname, "summary-interval"},
                                     {typecast,
                                      fun(StrI) ->
                                              case catch list_to_integer(StrI) of
                                                  I when is_integer(I), I > 0 ->
                                                      I * 1000;
                                                  _ -> {{error, {invalid_flag_value,
                                                                 {'summary-interval', StrI}}}}
                                              end
                                      end}]},
                 {timeout, [{shortname, "t"},
                            {longname, "timeout"},
                            {typecast,
                             fun (StrI) -> case catch list_to_integer(StrI) of
                                            I when is_integer(I), I > 0 ->
                                                   I * 1000;
                                            _ ->
                                                {error, {invalid_flag_value,
                                                         {timeout, StrI}}}
                                        end
                             end}]}],
    Callback = fun(_, [], _) ->
                       Text = clique_status:text("You have to provide a node"),
                       [clique_status:alert([Text])];
                  (_, [{node, Node}], Flags) ->
                       IsKill = lists:keymember(kill, 1, Flags),
                       Interval = proplists:get_value(summary_interval, Flags, 5000),
                       Timeout = proplists:get_value(timeout, Flags, 60000),
                       N = Timeout div Interval,
                       {ok, Local} = plumtree_peer_service_manager:get_local_state(),
                       TargetNodes = riak_dt_orswot:value(Local) -- [Node],
                       Text =
                       case net_adm:ping(Node) of
                           pang ->
                               %% node is offline, we've to ensure sessions are
                               %% remapped and all necessary queues exist.
                               leave_cluster(Node),
                               case check_cluster_consistency(TargetNodes, Timeout div 1000) of
                                   true ->
                                       vmq_reg:fix_dead_queues([Node], TargetNodes),
                                       "Done";
                                   false ->
                                       "Can't fix queues because cluster is inconsistent, retry!"
                               end;
                           pong when IsKill ->
                               LeaveFun =
                               fun() ->
                                       %% stop all MQTT sessions on Node
                                       %% Note: ensure loadbalancing will put them on other nodes
                                       vmq_ranch_config:stop_all_mqtt_listeners(true),

                                       %% At this point, client reconnect and will drain
                                       %% their queues located at 'Node' migrating them to
                                       %% their new node.
                                       case wait_till_all_offline(Interval, N) of
                                           ok ->
                                               %% There is no guarantee that all clients will
                                               %% reconnect on time; we've to force migrate all
                                               %% offline queues.
                                               vmq_reg:migrate_offline_queues(TargetNodes),
                                               %% node is online, we'll go the proper route
                                               %% instead of calling leave_cluster('Node')
                                               %% directly
                                               _ = plumtree_peer_service:leave(unused_arg),
                                               init:stop();
                                           error ->
                                               exit("error, still online queues, check the logs, and retry!")
                                       end
                               end,
                               ProcName = {?MODULE, vmq_server_migration},
                               case global:whereis_name(ProcName) of
                                   undefined ->
                                       case check_cluster_consistency(TargetNodes, Timeout div 1000) of
                                           true ->
                                               Pid = spawn(Node, LeaveFun),
                                               MRef = monitor(process, Pid),
                                               receive
                                                   {'DOWN', MRef, process, Pid, normal} ->
                                                       "Done";
                                                   {'DOWN', MRef, process, Pid, Reason} ->
                                                       Reason
                                               end;
                                           false ->
                                               "Can't migrate queues because cluster is inconsistent, retry!"
                                       end;
                                   Pid ->
                                       io_lib:format("Migration already started! ~p", [Pid])
                               end;
                           pong ->
                               %% stop accepting new connections on Node
                               %% Note: ensure new connections get balanced to other nodes
                               rpc:call(Node, vmq_ranch_config, stop_all_mqtt_listeners, [false]),
                               "Done! Use -k to teardown and migrate existing sessions!"
                       end,
                       [clique_status:text(Text)]
               end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).


leave_cluster(Node) ->
    {ok, Local} = plumtree_peer_service_manager:get_local_state(),
    {ok, Actor} = plumtree_peer_service_manager:get_actor(),
    case riak_dt_orswot:update({remove, Node}, Actor, Local) of
        {error,{precondition,{not_present, Node}}} ->
            io_lib:format("Node ~p wasn't part of the cluster~n", [Node]);
        {ok, Merged} ->
            _ = gen_server:cast(plumtree_peer_service_gossip, {receive_state, Merged}),
            {ok, Local2} = plumtree_peer_service_manager:get_local_state(),
            Local2List = riak_dt_orswot:value(Local2),
            case [P || P <- Local2List, P =:= Node] of
                [] ->
                    "Done";
                _ ->
                    leave_cluster(Node)
            end
    end.

check_cluster_consistency([Node|Nodes] = All, NrOfRetries) ->
    case rpc:call(Node, vmq_cluster, is_ready, []) of
        true ->
            check_cluster_consistency(Nodes, NrOfRetries);
        false ->
            timer:sleep(1000),
            check_cluster_consistency(All, NrOfRetries - 1)
    end;
check_cluster_consistency(_, 0) -> false;
check_cluster_consistency([], _) -> true.




wait_till_all_offline(_, 0) -> error;
wait_till_all_offline(Sleep, N) ->
    case vmq_queue_sup:summary() of
        {0, 0, Drain, Offline, Msgs} ->
            lager:info("ALL QUEUES OFFLINE: ~p draining, ~p offline, ~p msgs",
                       [Drain, Offline, Msgs]),
            ok;
        {Online, WaitForOffline, Drain, Offline, Msgs} ->
            lager:info("QUEUE SUMMARY: ~p online, ~p wait_for_offline, ~p draining, ~p offline, ~p msgs", [Online, WaitForOffline, Drain, Offline, Msgs]),
            timer:sleep(Sleep),
            wait_till_all_offline(Sleep, N - 1)
    end.

vmq_cluster_join_cmd() ->
    Cmd = ["vmq-admin", "cluster", "join"],
    KeySpecs = [{'discovery-node', [{typecast, fun(Node) ->
                                                       list_to_atom(Node)
                                               end}]}],
    FlagSpecs = [],
    Callback = fun (_, [], []) ->
                       Text = clique_status:text("You have to provide a discovery node (example discovery-node=vernemq1@127.0.0.1)"),
                       [clique_status:alert([Text])];
                   (_, [{'discovery-node', Node}], _) ->
                       case plumtree_peer_service:join(Node) of
                           ok ->
                               vmq_cluster:recheck(),
                               [clique_status:text("Done")];
                           {error, Reason} ->
                               Text = io_lib:format("Couldn't join cluster due to ~p~n", [Reason]),
                               [clique_status:alert([clique_status:text(Text)])]
                       end
               end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

vmq_cluster_upgrade_cmd() ->
    Cmd = ["vmq-admin", "node", "upgrade"],
    KeySpecs = [],
    FlagSpecs = [{'upgrade-now', [{longname, "upgrade-now"}]},
                 {'instruction-file', [{longname, "instruction-file"},
                                       {typecast, fun(F) ->
                                                          case filelib:is_file(F) of
                                                              true ->
                                                                  F;
                                                              false ->
                                                                  {error, {invalid_flag_value,
                                                                           {'instruction-file', F}}}
                                                          end
                                                  end}]}],
    Callback = fun(_, [], Flags) ->
                       IsUpgradeNow = lists:keymember('upgrade-now', 1, Flags),
                       {Function, Args} =
                       case lists:keyfind('instruction-file', 1, Flags) of
                           false when IsUpgradeNow ->
                               {run, []};
                           false ->
                               {dry_run, []};
                           {_, F} when IsUpgradeNow ->
                               {run, [F]};
                           {_, F} ->
                               {dry_run, [F]}
                       end,
                       Ret = apply(vmq_updo, Function, Args),
                       Text = io_lib:format("~p upgrade: ~p~n", [Function, Ret]),
                       [clique_status:text([Text])]
               end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).


vmq_session_list_cmd() ->
    Cmd = ["vmq-admin", "session", "list"],
    KeySpecs = [],
    ValidInfoItems = vmq_mqtt_fsm:info_items(),
    FlagSpecs = [{I, [{longname, atom_to_list(I)}]} || I <- ValidInfoItems],
    Callback = fun(_, [], Flags) ->
                       InfoItems = [I || {I, undefined} <- Flags],
                       Table =
                       vmq_mqtt_fsm:list_sessions(InfoItems,
                                                 fun(_, Infos, AccAcc) ->
                                                         case Infos of
                                                             [] ->
                                                                 AccAcc;
                                                             _ ->
                                                                 [Infos|AccAcc]
                                                         end
                                                 end, []),
                       [clique_status:table(Table)]
               end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

vmq_session_list_usage() ->
    Options = [io_lib:format("  --~p\n", [Item])
               || Item <- vmq_mqtt_fsm:info_items()],
    ["vmq-admin session list\n\n",
     "  Prints some information on running sessions\n\n",
     "Options\n\n" | Options
    ].



start_usage() ->
    ["vmq-admin node start\n\n",
     "  Starts the server application within this node. This is typically\n",
     "  not necessary since the server application is started automatically\n",
     "  when starting the service.\n"
    ].

stop_usage() ->
    ["vmq-admin node stop\n\n",
     "  Stops the server application within this node. This is typically \n"
     "  not necessary since the server application is stopped automatically\n",
     "  when the service is stopped.\n"
    ].

join_usage() ->
    ["vmq-admin cluster join discovery-node=<Node>\n\n",
     "  The discovery node will be used to find out about the \n",
     "  nodes in the cluster.\n\n"
    ].

leave_usage() ->
    ["vmq-admin cluster leave node=<Node> [-k | --kill_sessions]\n\n",
     "  Graceful cluster-leave and shudown of a cluster node. \n\n",
     "  If <Node> is already offline its cluster membership gets removed,\n",
     "  and the queues of the subscribers that have been connected at shutdown\n",
     "  will be recreated on other cluster nodes. This might involve\n",
     "  the disconnecting of clients that have already reconnected.\n",
     "  \n",
     "  If <Node> is still online all its MQTT listeners (including websockets)\n",
     "  are stopped and wont therefore accept new connections. Established\n",
     "  connections aren't canceled at this point. Use --kill_sessions to get\n",
     "  into the second phase of the graceful shutdown.\n",
     "  \n",
     "   --kill_sessions, -k,\n",
     "       terminates all open MQTT connections, and migrates the queues of\n",
     "       the clients that used 'clean_session=false' to other cluster nodes.\n",
     "   --summary-interval=<IntervalInSecs>, -i\n",
     "       logs the status of an ongoing migration every <IntervalInSecs>\n",
     "       seconds, defaults to 5 seconds.\n",
     "   --timeout=<TimeoutInSecs>, -t\n",
     "       stops the migration process afer <TimeoutInSecs> seconds, defaults\n",
     "       to 60 seconds. The command can be reissued in case of a timeout.",
     "\n\n"
    ].

upgrade_usage() ->
    ["vmq-admin node upgrade [--upgrade-now]\n\n",
     "  Performs a dry run of a hot code upgrade. Use the --upgrade-now flag only after\n",
     "  a dry run. Make sure you are aware any possible consequences of the upgrade. Doing a dry run\n"
     "  can show which parts of the systems will be touched by the upgrade.\n\n",
     "Options\n\n",
     "  --upgrade-now\n",
     "      Perform & apply a hot code upgrade. Erlang is good at it, but many things\n",
     "      can still fail upgrading this way. We generally don't\n",
     "      recommend it. YOU SHOULD KNOW WHAT YOU ARE DOING AT THIS POINT\n\n"
    ].

usage() ->
    ["vmq-admin <sub-command>\n\n",
     "  Administrate the cluster.\n\n",
     "  Sub-commands:\n",
     "    node        Manage this node\n",
     "    cluster     Manage this node's cluster membership\n",
     "    session     Retrieve session information\n",
     "    plugin      Manage plugin system\n",
     "    listener    Manage listener interfaces\n",
     "    metrics     Retrieve System Metrics\n",
     "  Use --help after a sub-command for more details.\n"
    ].
node_usage() ->
    ["vmq-admin node <sub-command>\n\n",
     "  Administrate this VerneMQ node.\n\n",
     "  Sub-commands:\n",
     "    start       Start the server application\n",
     "    stop        Stop the server application\n\n",
     "  Use --help after a sub-command for more details.\n"
    ].

cluster_usage() ->
    ["vmq-admin cluster <sub-command>\n\n",
     "  Administrate cluster membership for this particular VerneMQ node.\n\n",
     "  Sub-commands:\n",
     "    status      Prints cluster status information\n",
     "    join        Join a cluster\n",
     "    leave       Leave the cluster\n\n",
     "  Use --help after a sub-command for more details.\n"
    ].

session_usage() ->
    ["vmq-admin session <sub-command>\n\n",
     "  Retrieve information on live sessions.\n\n",
     "  Sub-commands:\n",
     "    list        lists all running sessions\n",
     "  Use --help after a sub-command for more details.\n"
    ].

metrics_usage() ->
    ["vmq-admin metrics <sub-command>\n\n",
     "  Interact with the metrics subsystem.\n\n",
     "  Sub-commands:\n",
     "    show        Prints all available metrics for this VerneMQ node.\n",
     "    reset       Resets all counter metrics.\n"
    ].

ensure_all_stopped(App)  ->
    ensure_all_stopped([App], []).

ensure_all_stopped([kernel|Apps], Res)  ->
    ensure_all_stopped(Apps, Res);
ensure_all_stopped([stdlib|Apps], Res)  ->
    ensure_all_stopped(Apps, Res);
ensure_all_stopped([sasl|Apps], Res) ->
    ensure_all_stopped(Apps, Res);
ensure_all_stopped([lager|Apps], Res)  ->
    ensure_all_stopped(Apps, Res);
ensure_all_stopped([clique|Apps], Res)  ->
    ensure_all_stopped(Apps, Res);
ensure_all_stopped([vmq_plugin|Apps], Res) ->
    vmq_plugin_mgr:stop(), %% this will stop all plugin applications
    application:stop(vmq_plugin),
    ensure_all_stopped(Apps, [vmq_plugin|Res]);
ensure_all_stopped([App|Apps], Res)  ->
    {ok, Deps} = application:get_key(App, applications),
    _ = application:stop(App),
    Stopped = ensure_all_stopped(lists:reverse(Deps), []),
    ensure_all_stopped(Apps -- Stopped, [[App|Stopped]|Res]);
ensure_all_stopped([], Res) -> Res.

