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
    {Res, _} = clique_command:run(M3),
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
    clique:register_usage(["vmq-admin", "node", "join"], join_usage()),
    clique:register_usage(["vmq-admin", "node", "leave"], leave_usage()),
    clique:register_usage(["vmq-admin", "node", "upgrade"], upgrade_usage()),

    clique:register_usage(["vmq-admin", "session"], session_usage()),
    clique:register_usage(["vmq-admin", "session", "list"], vmq_session_list_usage()),
    ok.

vmq_server_stop_cmd() ->
    Cmd = ["vmq-admin", "node", "stop"],
    Callback = fun(_, _) ->
                       _ = ensure_all_stopped(vmq_server),
                       [clique_status:text("Done")]
               end,
    clique:register_command(Cmd, [], [], Callback).

vmq_server_start_cmd() ->
    Cmd = ["vmq-admin", "node", "start"],
    Callback = fun(_, _) ->
                       _ = application:ensure_all_started(vmq_server),
                       [clique_status:text("Done")]
               end,
    clique:register_command(Cmd, [], [], Callback).

vmq_server_status_cmd() ->
    Cmd = ["vmq-admin", "node", "status"],
    Callback = fun(_, _) ->
                       VmqStatus = vmq_cluster:status(),
                       NodeTable =
                       lists:foldl(fun({NodeName, IsReady}, Acc) ->
                                           [[{'Node', NodeName},
                                             {'Running', IsReady}]|Acc]
                                   end, [], VmqStatus),
                       [clique_status:table(NodeTable)]
               end,
    clique:register_command(Cmd, [], [], Callback).

vmq_cluster_leave_cmd() ->
    Cmd = ["vmq-admin", "node", "leave"],
    KeySpecs = [{node, [{typecast, fun clique_typecast:to_node/1}]}],
    FlagSpecs = [],
    Callback = fun([], _) ->
                       Text = clique_status:text("You have to provide a node"),
                       [clique_status:alert([Text])];
                  ([{node, Node}], []) ->
                       plumtree_peer_service:leave(Node),
                       [clique_status:text("Done")]
               end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).


vmq_cluster_join_cmd() ->
    Cmd = ["vmq-admin", "node", "join"],
    KeySpecs = [{'discovery-node', [{typecast, fun(Node) ->
                                                       list_to_atom(Node)
                                               end}]}],
    FlagSpecs = [],
    Callback = fun ([], []) ->
                       Text = clique_status:text("You have to provide a discovery node"),
                       [clique_status:alert([Text])];
                   ([{'discovery-node', Node}], _) ->
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


vmq_session_list_cmd() ->
    Cmd = ["vmq-admin", "session", "list"],
    KeySpecs = [],
    ValidInfoItems = vmq_session:info_items(),
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
               || Item <- vmq_session:info_items()],
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
     "  Stops the server application within this node. This is typically\n\n"
    ].

join_usage() ->
    ["vmq-admin node join discovery-node=<Node> [--node-type=disc|ram]\n\n",
     "  Make the node join a cluster. The node will be reset automatically\n",
     "  before we actually cluster it. The discovery node provided will be\n",
     "  used to find out about the nodes in the cluster.\n\n"
    ].

leave_usage() ->
    ["vmq-admin node leave\n\n",
     "  Leaves this cluster.\n\n"
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

usage() ->
    ["vmq-admin <sub-command>\n\n",
     "  administrate the cluster.\n\n",
     "  Sub-commands:\n",
     "    node        Manage the cluster node\n",
     "    session     Retrieve session information\n",
     "    config      Manage the internal configuration store\n",
     "    plugin      Manage plugin system\n",
     "  Use --help after a sub-command for more details.\n"
    ].
node_usage() ->
    ["vmq-admin node <sub-command>\n\n",
     "  administrate this VerneMQ cluster node.\n\n",
     "  Sub-commands:\n",
     "    start       Start the server application\n",
     "    stop        Stop the server application\n",
     "    status      Prints cluster status information\n",
     "    join        Join a cluster\n",
     "    leave       Leave the cluster\n",
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

