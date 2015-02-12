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
         register_cli/0]).

-behaviour(clique_handler).

init_registry() ->
    F = fun() -> vmq_cluster:nodes() end,
    clique:register_node_finder(F),
    clique:register([?MODULE, vmq_plugin_cli]).

command(Cmd) ->
    clique:run(Cmd).

register_cli() ->
    vmq_config_cli:register_config(),
    register_cli_usage(),
    vmq_server_start_cmd(),
    vmq_server_stop_cmd(),
    vmq_server_status_cmd(),
    vmq_cluster_reset_cmd(),
    vmq_cluster_chtype_cmd(),
    vmq_cluster_join_cmd(),
    vmq_cluster_remove_cmd(),
    vmq_cluster_upgrade_cmd(),

    vmq_session_list_cmd(),

    vmq_listener_cli:register_server_cli().

register_cli_usage() ->
    clique:register_usage(["vmq-admin", "node"], node_usage()),
    clique:register_usage(["vmq-admin", "node", "start"], start_usage()),
    clique:register_usage(["vmq-admin", "node", "stop"], stop_usage()),
    clique:register_usage(["vmq-admin", "node", "reset"], reset_usage()),
    clique:register_usage(["vmq-admin", "node", "chtype"], chtype_usage()),
    clique:register_usage(["vmq-admin", "node", "join"], join_usage()),
    clique:register_usage(["vmq-admin", "node", "remove"], remove_usage()),
    clique:register_usage(["vmq-admin", "node", "upgrade"], upgrade_usage()),

    clique:register_usage(["vmq-admin", "session"], session_usage()),
    clique:register_usage(["vmq-admin", "session", "list"], fun vmq_session_list_usage/0).

vmq_server_stop_cmd() ->
    Cmd = ["vmq-admin", "node", "stop"],
    Callback = fun(_, _) ->
                       ensure_all_stopped(vmq_server),
                       [clique_status:text("Done")]
               end,
    clique:register_command(Cmd, [], [], Callback).

vmq_server_start_cmd() ->
    Cmd = ["vmq-admin", "node", "start"],
    Callback = fun(_, _) ->
                       application:ensure_all_started(vmq_server),
                       [clique_status:text("Done")]
               end,
    clique:register_command(Cmd, [], [], Callback).

vmq_server_status_cmd() ->
    Cmd = ["vmq-admin", "node", "status"],
    Callback = fun(_, _) ->
                       VmqNodes = vmq_cluster:nodes(),
                       Status = mnesia_cluster_utils:status(),
                       Nodes = proplists:get_value(nodes, Status),
                       RunningNodes = proplists:get_value(running_nodes, Status),
                       TypedNodes = lists:flatten([[{Type, Node}|| Node <- NodesWithType]
                                                   || {Type, NodesWithType} <- Nodes]),
                       NodeTable =
                       lists:foldl(fun({NodeType, NodeName}, Acc) ->
                                           IsRunning = lists:member(NodeName, RunningNodes),
                                           VmqReady = lists:member(NodeName, VmqNodes),
                                           [[{'Node', NodeName},
                                             {'Type', NodeType},
                                             {'Database-Ready', IsRunning},
                                             {'Server-Ready', VmqReady}]|Acc]
                                   end, [], TypedNodes),
                       [clique_status:table(NodeTable)]
               end,
    clique:register_command(Cmd, [], [], Callback).

mnesia_op(Op, What) ->
    case catch Op() of
        ok ->
            [clique_status:text("Done")];
        {error, {_, Descr}} ->
            Text = clique_status:text(Descr),
            [clique_status:alert([Text])];
        {error, mnesia_unexpectedly_running} ->
            Text = clique_status:text("This node is currently running, use 'vmq-admin server stop' to stop it."),
            [clique_status:alert([Text])];
        {error, Reason} ->
            Text = io_lib:format("Couldn't ~s cluster due to ~p~n", [What, Reason]),
            [clique_status:alert([clique_status:text(Text)])]
    end.

vmq_cluster_reset_cmd() ->
    Cmd = ["vmq-admin", "node", "reset"],
    KeySpecs = [],
    FlagSpecs = [{forcefully, [{shortname, "f"},
                               {longname, "forcefully"}]}],
    Callback = fun([], []) ->
                       mnesia_op(fun() -> mnesia_cluster_utils:reset() end, "reset");
                  ([], _) ->
                       mnesia_op(fun() -> mnesia_cluster_utils:force_reset() end, "force-reset")
               end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

vmq_cluster_chtype_cmd() ->
    Cmd = ["vmq-admin", "node", "chtype"],
    KeySpecs = [],
    FlagSpecs = [{'node-type', [{shortname, "t"},
                                {longname, "node-type"},
                                {typecast, fun("disc") -> disc;
                                              ("ram") -> ram;
                                              (E) -> {error, {invalid_flag_value, {'node-type', E}}}
                                           end}]}],
    Callback = fun([], []) ->
                       Text = clique_status:text("You have to provide a node type"),
                       [clique_status:alert([Text])];
                  ([], Flags) ->
                       {ok, Type} = proplists:get_value('node-type', Flags),
                       %% TODO: we should also change the way we store messages...
                       mnesia_op(fun() ->
                                         mnesia_cluster_utils:change_cluster_node_type(Type)
                                 end, "change node-type")
               end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

vmq_cluster_remove_cmd() ->
    Cmd = ["vmq-admin", "node", "remove"],
    KeySpecs = [{node, [{typecast, fun clique_typecast:to_node/1}]}],
    FlagSpecs = [{offline, [{longname, "offline"}]}],
    Callback = fun([], _) ->
                       Text = clique_status:text("You have to provide a node"),
                       [clique_status:alert([Text])];
                  ([{node, Node}], []) ->
                       mnesia_op(fun() ->
                                         mnesia_cluster_utils:forget_cluster_node(Node, false)
                                 end, "remove cluster node");
                  ([{node, Node}], [{offline, _}]) ->
                       mnesia_op(fun() ->
                                         mnesia_cluster_utils:forget_cluster_node(Node, true)
                                 end, "remove cluster node")
               end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).


vmq_cluster_join_cmd() ->
    Cmd = ["vmq-admin", "node", "join"],
    KeySpecs = [{'discovery-node', [{typecast, fun clique_typecast:to_node/1}]}],
    FlagSpecs = [{'node-type', [{shortname, "t"},
                                {longname, "node-type"},
                                {typecast, fun("disc") -> disc;
                                              ("ram") -> ram;
                                              (E) -> {error, {invalid_flag_value, {'node-type', E}}}
                                           end}]}],
    Callback = fun ([], []) ->
                       Text = clique_status:text("You have to provide a discovery node"),
                       [clique_status:alert([Text])];
                   ([{'discovery-node', Node}], Flags) ->
                       Type = proplists:get_value('node-type', Flags, disc),
                       case catch mnesia_cluster_utils:join_cluster(Node, Type) of
                           ok ->
                               [clique_status:text("Done")];
                           {ok, already_member} ->
                               Text = clique_status:text("Already a cluster member"),
                               [clique_status:alert([Text])];
                           {error, {_, Descr}} ->
                               Text = clique_status:text(Descr),
                               [clique_status:alert([Text])];
                           {error, mnesia_unexpectedly_running} ->
                               Text = clique_status:text("This node is currently running, use vmq-admin stop-server to stop it."),
                               [clique_status:alert([Text])];
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
    Callback = fun([], Flags) ->
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


vmq_session_info_items() ->
    [pid, client_id, user, peer_host, peer_port, state,
     mountpoint, node, protocol, timeout, retry_timeout,
     recv_cnt, send_cnt, waiting_acks].

vmq_session_list_cmd() ->
    Cmd = ["vmq-admin", "session", "list"],
    KeySpecs = [],
    ValidInfoItems = vmq_session_info_items(),
    FlagSpecs = [{I, [{longname, atom_to_list(I)}]} || I <- ValidInfoItems],
    Callback = fun([], Flags) ->
                       InfoItems = [I || {I, undefined} <- Flags],
                       Table =
                       vmq_session:list_sessions(InfoItems,
                                                 fun(_, Infos, AccAcc) ->
                                                         [Infos|AccAcc]
                                                 end, []),
                       [clique_status:table(Table)]
               end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

vmq_session_list_usage() ->
    Options = [io_lib:format("  --~p\n", [Item])
               || Item <- vmq_session_info_items()],
    ["vmq-admin session list\n\n",
     "  Prints the information of currently running sessions\n\n",
     "Options\n\n" | Options
    ].



start_usage() ->
    ["vmq-admin node start\n\n",
     "  Starts the server application within this node. This is typically\n",
     "  not necessary since the server application is started automatically\n",
     "  when starting the node.\n"
    ].

stop_usage() ->
    ["vmq-admin node stop\n\n",
     "  Stops the server application within this node. This is typically\n",
     "  only necessary previously to the 'join', 'reset' and 'chtype'\n",
     "  commands.\n"
    ].

reset_usage() ->
    ["vmq-admin node reset\n\n",
     "  Return a node to its virgin state, where is it not a member of any\n",
     "  cluster, has no cluster configuration, no local database, and no\n"
     "  persisted messages.\n\n",
     "Options\n\n",
     "  --forcefully, -f\n",
     "     Resets the node regardless of the current state and cluster\n",
     "     configuration. Should only be used if the database or cluster\n",
     "     configuration has been corrupted.\n\n"
    ].

chtype_usage() ->
    ["vmq-admin node chtype\n\n",
     "  Changes the type of the cluster node. The node must be stopped for\n",
     "  this operation to succeed, and when turning a node into a RAM node the\n",
     "  node must not be the only disc node in the cluster.\n"
    ].

join_usage() ->
    ["vmq-admin node join discovery-node=<Node> [--node-type=disc|ram]\n\n",
     "  Make the node join a cluster. The node will be reset automatically\n",
     "  before we actually cluster it. The discovery node provided will be\n",
     "  used to find out about the nodes in the cluster.\n\n",
     "Options\n\n",
     "  --node-type, -t=disc|ram\n",
     "      Specifies whether the new node joins as a 'disc' or 'ram' node.\n",
     "      The default value is 'disc'.\n\n"
    ].

remove_usage() ->
    ["vmq-admin node remove node=<Node> [--offline]\n\n",
     "  Removes a cluster node remotely. The node that is being removed must\n",
     "  be offline, while the node we are removing from must be online, except\n",
     "  when using the --offline flag.\n\n",
     "Options\n\n",
     "  --offline\n",
     "      Enables node removal from an offline node. This is only useful in\n",
     "      the situation where all the nodes are offline and the last node to\n",
     "      go down cannot be brought online, thus preventing the whole cluster\n",
     "      from starting. It should not be used in any other circumstances\n",
     "      since it can lead to inconsistencies.\n\n"
    ].

upgrade_usage() ->
    ["vmq-admin node upgrade [--upgrade-now]\n\n",
     "  Performs a dry run of a hot code upgrade. Use the --upgrade-now flag if\n",
     "  you are aware of the consequences of the upgrade. It is recommended to\n"
     "  previously perform a dry run to see which parts of the systems would\n"
     "  be touched by the upgrade\n\n",
     "Options\n\n",
     "  --upgrade-now\n",
     "      Perform the hot code upgrade, Erlang is good at it, but many things\n",
     "      could fail using this way to perform an upgrade. We generally don't\n",
     "      recommend it. YOU SHOULD KNOW WHAT YOU ARE DOING AT THIS POINT\n\n"
    ].

node_usage() ->
    ["vmq-admin node <sub-command>\n\n",
     "  administrate this VerneMQ cluster node.\n\n",
     "  Sub-commands:\n",
     "    start       Start the server application\n",
     "    stop        Stop the server application\n",
     "    status      Prints cluster status information\n",
     "    reset       Reset the server state\n",
     "    chtype      Change the type of the node\n",
     "    join        Join a cluster\n",
     "    remove      Remove a cluster node\n",
     "    upgrade     Upgrade a cluster node\n\n",
     "  Use --help after a sub-command for more details.\n"
    ].

session_usage() ->
    ["vmq-admin session <sub-command>\n\n",
     "  retrieve information about the sessions.\n\n",
     "  Sub-commands:\n",
     "    list        list the currently running sessions\n",
     "  Use --help after a sub-command for more details.\n"
    ].


ensure_all_stopped(App)  ->
    ensure_all_stopped([App], []).

ensure_all_stopped([kernel|Apps], Res)  ->
    ensure_all_stopped(Apps, Res);
ensure_all_stopped([stdlib|Apps], Res)  ->
    ensure_all_stopped(Apps, Res);
ensure_all_stopped([lager|Apps], Res)  ->
    ensure_all_stopped(Apps, Res);
ensure_all_stopped([clique|Apps], Res)  ->
    ensure_all_stopped(Apps, Res);
ensure_all_stopped([App|Apps], Res)  ->
    {ok, Deps} = application:get_key(App, applications),
    application:stop(App),
    Stopped = ensure_all_stopped(lists:reverse(Deps), []),
    ensure_all_stopped(Apps -- Stopped, [[App|Stopped]|Res]);
ensure_all_stopped([], Res) -> Res.

