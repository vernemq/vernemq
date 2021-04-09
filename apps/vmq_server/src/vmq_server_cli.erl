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

-module(vmq_server_cli).
-export([init_registry/0]).
-export([command/1,
         command/2,
         register_cli/0]).

-include("vmq_metrics.hrl").

-behaviour(clique_handler).

init_registry() ->
    F = fun() -> vmq_cluster:nodes() end,
    clique:register_node_finder(F),
    clique:register([?MODULE, vmq_plugin_cli]),
    clique_writer:register("human", vmq_cli_human_writer),
    clique_writer:register("json", vmq_cli_json_writer).

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

parse_res({error,_}, {error,_}) ->
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
    vmq_server_show_cmd(),
    vmq_server_metrics_cmd(),
    vmq_server_metrics_reset_cmd(),
    vmq_cluster_join_cmd(),
    vmq_cluster_leave_cmd(),
    vmq_cluster_upgrade_cmd(),

    vmq_mgmt_add_api_key_cmd(),
    vmq_mgmt_create_api_key_cmd(),
    vmq_mgmt_delete_api_key_cmd(),
    vmq_mgmt_list_api_keys_cmd(),

    vmq_listener_cli:register_server_cli(),
    vmq_info_cli:register_cli(),

    vmq_tracer_cli:register_cli(),
    ok.

register_cli_usage() ->
    clique:register_usage(["vmq-admin"], fun usage/0),
    clique:register_usage(["vmq-admin", "node"], node_usage()),
    clique:register_usage(["vmq-admin", "node", "start"], start_usage()),
    clique:register_usage(["vmq-admin", "node", "stop"], stop_usage()),
    clique:register_usage(["vmq-admin", "node", "upgrade"], upgrade_usage()),

    clique:register_usage(["vmq-admin", "cluster"], cluster_usage()),
    clique:register_usage(["vmq-admin", "cluster", "join"], join_usage()),
    clique:register_usage(["vmq-admin", "cluster", "leave"], leave_usage()),


    clique:register_usage(["vmq-admin", "metrics"], metrics_usage()),
    clique:register_usage(["vmq-admin", "metrics", "show"], fun metrics_show_usage/0),

    clique:register_usage(["vmq-admin", "api-key"], api_usage()),
    clique:register_usage(["vmq-admin", "api-key", "delete"], api_delete_key_usage()),
    clique:register_usage(["vmq-admin", "api-key", "add"], api_add_key_usage()),
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

vmq_server_show_cmd() ->
    Cmd = ["vmq-admin", "cluster", "show"],
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
    ValidLabels = vmq_metrics:get_label_info(),
    LabelFlagSpecs =
        [{I, [{longname, atom_to_list(I)}]} || {I, _} <- ValidLabels],
    FixedFlagSpecs =
        [{describe, [{shortname, "d"},
                     {longname, "with-descriptions"}]},
         {aggregate, [{shortname, "a"},
                      {longname, "aggregate"},
                      {typecast, fun("true") -> true;
                                    (_) -> false
                                 end}]}],
    FlagSpecs = FixedFlagSpecs ++ LabelFlagSpecs,
    Callback =
        fun(_, _, Flags) ->
                Describe = lists:keymember(describe, 1, Flags),
                Aggregate = case proplists:get_value(aggregate, Flags, true) of
                                undefined -> true;
                                Val -> Val
                            end,
                LabelFlags = get_label_flags(Flags, LabelFlagSpecs),
                TextLines =
                lists:foldl(
                  fun({#metric_def{type = Type,
                                   labels = _Labels,
                                   name = Metric,
                                   description = Description}, Val}, Acc) ->
                          SType = atom_to_list(Type),
                          SMetric = atom_to_list(Metric),
                          Lines =
                              case Val of
                                  V when is_number(V) ->
                                      [SType, ".", SMetric, " = ", number_to_list(V)];
                                  {Count, Sum, Buckets} when is_map(Buckets) ->
                                      HistogramLines =
                                      [
                                       [SType, ".", SMetric, "_count = ", number_to_list(Count), "\n"],
                                       [SType, ".", SMetric, "_sum = ", number_to_list(Sum), "\n"]
                                      ],

                                      maps:fold(
                                        fun
                                            (Bucket, BucketCnt, HistAcc) ->
                                                [[SType, ".", SMetric, "_bucket_",
                                                  case Bucket of
                                                      infinity -> "infinity";
                                                      _ -> number_to_list(Bucket)
                                                  end ," = ", number_to_list(BucketCnt), "\n"]  | HistAcc]
                                        end, HistogramLines, Buckets)
                              end,
                          case Describe of
                              true ->
                                  ["# ", Description, "\n", Lines,"\n\n"| Acc];
                              false ->
                                  [Lines, "\n" | Acc]
                          end
                  end,
                  [],
                  vmq_metrics:metrics(#{labels => LabelFlags,
                                        aggregate => Aggregate})),
                [clique_status:text(lists:flatten(TextLines))]
        end,
    clique:register_command(Cmd, [], FlagSpecs, Callback).

get_label_flags(Flags, LabelFlagSpecs) ->
    lists:filter(
      fun({FlagName, _Val}) ->
              lists:keymember(FlagName, 1, LabelFlagSpecs)
      end, Flags).

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
                       %% Make sure Iterations > 0 to it will be
                       %% checked at least once if queue migration is complete.
                       Iterations = max(Timeout div Interval, 1),
                       TargetNodes = vmq_peer_service:members() -- [Node],
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
                               Caller = self(),
                               CRef = make_ref(),
                               LeaveFun =
                               fun() ->
                                       %% stop all MQTT sessions on Node
                                       %% Note: ensure loadbalancing will put them on other nodes
                                       vmq_ranch_config:stop_all_mqtt_listeners(true),

                                       %% At this point, client reconnect and will drain
                                       %% their queues located at 'Node' migrating them to
                                       %% their new node.
                                       case wait_till_all_offline(Interval, Iterations) of
                                           ok ->
                                               %% There is no guarantee that all clients will
                                               %% reconnect on time; we've to force migrate all
                                               %% offline queues.
                                               migrate_offline_queues(Caller, CRef, TargetNodes, 1000),
                                               %% node is online, we'll go the proper route
                                               %% instead of calling leave_cluster('Node')
                                               %% directly
                                               _ = vmq_peer_service:leave(Node),
                                               Caller ! {done, CRef},
                                               init:stop();
                                           error ->
                                               Caller ! {stop, CRef, "error, still online queues, check the logs, and retry!"}
                                       end
                               end,
                               ProcName = {?MODULE, vmq_server_migration},
                               case global:whereis_name(ProcName) of
                                   undefined ->
                                       case check_cluster_consistency(TargetNodes, Timeout div 1000) of
                                           true ->
                                               Pid = spawn(Node, LeaveFun),
                                               MRef = monitor(process, Pid),
                                               receive_loop(CRef, MRef, Pid);
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

receive_loop(CRef, MRef, Pid) ->
    receive
        {msg, CRef, Msg} ->
            io:format("~s~n", [lists:flatten(Msg)]),
            receive_loop(CRef, MRef, Pid);
        {stop, CRef, Msg} ->
            Msg;
        {done, CRef} ->
            "Done";
        {'DOWN', MRef, process, Pid, Reason} ->
            "Unknown error: " ++ atom_to_list(Reason)
    end.

migrate_offline_queues(Caller, CRef, TargetNodes, MaxConcurrency) ->
    S = vmq_reg:prepare_offline_queue_migration(TargetNodes),
    migrate(Caller, CRef, S, MaxConcurrency).

migrate(Caller, CRef, S, MaxConcurrency) ->
    {Progress, #{migrated_queue_cnt := MQCnt,
                 migrated_msg_cnt := MMCnt,
                 total_queue_count := TQCnt,
                 total_msg_count := TMCnt} = S1} =
        vmq_reg:migrate_offline_queues(S, MaxConcurrency),
    Msg = io_lib:format("Migrated ~p/~p queues and ~p/~p messages", [MQCnt, TQCnt, MMCnt, TMCnt]),
    Caller ! {msg, CRef, Msg},
    case Progress of
        cont ->
            timer:sleep(100),
            migrate(Caller, CRef, S1, MaxConcurrency);
        done ->
            ok
    end.

leave_cluster(Node) ->
    case vmq_peer_service:leave(Node) of
        ok ->
            "Done";
        {error, not_present} ->
            io_lib:format("Node ~p wasn't part of the cluster~n", [Node])
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
    case vmq_queue_sup_sup:summary() of
        {0, 0, Drain, Offline, Msgs} ->
            lager:info("all queues offline: ~p draining, ~p offline, ~p msgs",
                       [Drain, Offline, Msgs]),
            ok;
        {Online, WaitForOffline, Drain, Offline, Msgs} ->
            lager:info("intermediate queue summary: ~p online, ~p wait_for_offline, ~p draining, ~p offline, ~p msgs", [Online, WaitForOffline, Drain, Offline, Msgs]),
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
                       case vmq_peer_service:join(Node) of
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

vmq_mgmt_create_api_key_cmd() ->
    Cmd = ["vmq-admin", "api-key", "create"],
    KeySpecs = [],
    FlagSpecs = [],
    Callback = fun(_, [], []) ->
                       ApiKey = vmq_http_mgmt_api:create_api_key(),
                       [clique_status:text([binary_to_list(ApiKey)])]
               end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

vmq_mgmt_add_api_key_cmd() ->
    Cmd = ["vmq-admin", "api-key", "add"],
    KeySpecs = [{'key', [{typecast, fun(Key) ->
                                            list_to_binary(Key)
                                    end}]}],
    FlagSpecs = [],
    Callback = fun(_, [{'key', Key}], _) ->
                       vmq_http_mgmt_api:add_api_key(Key),
                       [clique_status:text("Done")];
                  (_,_,_) ->
                       Text = clique_status:text(api_add_key_usage()),
                       [clique_status:alert([Text])]
               end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

vmq_mgmt_delete_api_key_cmd() ->
    Cmd = ["vmq-admin", "api-key", "delete"],
    KeySpecs = [{'key', [{typecast, fun(Key) ->
                                            list_to_binary(Key)
                                    end}]}],
    FlagSpecs = [],
    Callback = fun(_, [{'key', Key}], _) ->
                       vmq_http_mgmt_api:delete_api_key(Key),
                       [clique_status:text("Done")];
                  (_,_,_) ->
                       Text = clique_status:text(api_delete_key_usage()),
                       [clique_status:alert([Text])]
               end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

vmq_mgmt_list_api_keys_cmd() ->
    Cmd = ["vmq-admin", "api-key", "show"],
    KeySpecs = [],
    FlagSpecs = [],
    Callback = fun(_, _, _) ->
                       Keys = vmq_http_mgmt_api:list_api_keys(),
                       KeyTable =
                       lists:foldl(fun(Key, Acc) ->
                                           [[{'Key', Key}]|Acc]
                                   end, [], Keys),
                       [clique_status:table(KeyTable)]
               end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).


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
     "  Graceful cluster-leave and shutdown of a cluster node. \n\n",
     "  If <Node> is already offline its cluster membership gets removed,\n",
     "  and the queues of the subscribers that have been connected at shutdown\n",
     "  will be recreated on other cluster nodes. This might involve\n",
     "  the disconnecting of clients that have already reconnected.\n",
     "  \n",
     "  If <Node> is still online all its MQTT listeners (including websockets)\n",
     "  are stopped and wont therefore accept new connections. Established\n",
     "  connections aren't cancelled at this point. Use --kill_sessions to get\n",
     "  into the second phase of the graceful shutdown.\n",
     "  \n",
     "   --kill_sessions, -k,\n",
     "       terminates all open MQTT connections, and migrates the queues of\n",
     "       the clients that used 'clean_session=false' to other cluster nodes.\n",
     "   --summary-interval=<IntervalInSecs>, -i\n",
     "       logs the status of an ongoing migration every <IntervalInSecs>\n",
     "       seconds, defaults to 5 seconds.\n",
     "   --timeout=<TimeoutInSecs>, -t\n",
     "       stops the migration process after <TimeoutInSecs> seconds, defaults\n",
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
     "    retain      Show and filter MQTT retained messages\n",
     "    plugin      Manage plugin system\n",
     "    listener    Manage listener interfaces\n",
     "    metrics     Retrieve System Metrics\n",
     "    api-key     Manage API keys for the HTTP management interface\n",
     "    trace       Trace various aspects of VerneMQ\n",
     remove_ok(vmq_plugin_mgr:get_usage_lead_lines()),
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
     "    show        Prints cluster information\n",
     "    join        Join a cluster\n",
     "    leave       Leave the cluster\n\n",
     "  Use --help after a sub-command for more details.\n"
    ].

metrics_usage() ->
    ["vmq-admin metrics <sub-command>\n\n",
     "  Interact with the metrics subsystem.\n\n",
     "  Sub-commands:\n",
     "    show        Prints all available metrics for this VerneMQ node.\n"
    ].

metrics_show_usage() ->
    Labels =
        [io_lib:format("      --~p=[~s]\n", [Key,  string:join(Values, "|")])
         || {Key, Values}  <- vmq_metrics:get_label_info()],
    ["vmq-admin metrics show\n\n",
     "  Show and filter metrics for this VerneMQ node.\n\n",
     "Options\n\n",
     "  --with-descriptions, -d,\n"
     "    Show metrics annotated with descriptions.\n\n"
     "Labels\n\n"
     "    Filter metrics by labels. Accepts multiple comma separated labels.\n"
     "    If no labels are applied all metrics are shown.\n"
     "    Metrics are shown if any of the listed labels match,\n\n"
     "    Available labels and values (--labelname=[val1|val2|...]):\n"
     | [Labels
     |
     "\n    Example: --mqtt_version=4\n\n"]
    ].

api_usage() ->
    ["vmq-admin api-key <sub-command>\n\n",
     "  Create, add, delete, and show API keys for the HTTP management interface.\n\n",
     "  Sub-commands:\n",
     "    create      Creates a new API key.\n",
     "    add         Adds a new API key.\n",
     "    delete      Deletes an existing API key.\n",
     "    show        Shows all API keys.\n"
    ].

api_delete_key_usage() ->
    ["vmq-admin api-key delete key=<API Key>\n\n",
     "  Deletes an existing API Key.\n\n"
    ].

api_add_key_usage() ->
    ["vmq-admin api-key add key=<API Key>\n\n",
     "  Adds an API Key.\n\n"
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


remove_ok({ok, Res}) ->
    Res.

number_to_list(N) when is_integer(N) -> integer_to_list(N);
number_to_list(N) when is_float(N) -> float_to_list(N).
