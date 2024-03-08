%% Copyright 2018 Erlio GmbH Basel Switzerland (http://erl.io)
%% Copyright 2018-2024 Octavo Labs/VerneMQ (https://vernemq.com/)
%% and Individual Contributors.
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

-module(vmq_info_cli).

-include("vmq_server.hrl").

-export([register_cli/0]).

register_cli() ->
    vmq_session_list_cmd(),
    vmq_session_disconnect_cmd(),
    vmq_session_disconnect_clients_cmd(),
    vmq_session_disconnect_batch_cmd(),
    vmq_session_reauthorize_cmd(),
    vmq_retain_show_cmd(),
    vmq_retain_delete_cmd(),
    vmq_loq_show_cmd(),
    vmq_log_level_cmd(),

    clique:register_usage(["vmq-admin", "session"], session_usage()),
    clique:register_usage(["vmq-admin", "session", "show"], vmq_session_show_usage()),
    clique:register_usage(["vmq-admin", "session", "disconnect"], vmq_session_disconnect_usage()),
    clique:register_usage(
        ["vmq-admin", "session", "disconnect", "clients"], vmq_session_disconnect_clients_usage()
    ),
    clique:register_usage(
        ["vmq-admin", "session", "disconnect", "batch"], vmq_session_disconnect_batch_usage()
    ),
    clique:register_usage(["vmq-admin", "session", "reauthorize"], vmq_session_reauthorize_usage()),
    clique:register_usage(["vmq-admin", "retain"], retain_usage()),
    clique:register_usage(["vmq-admin", "retain", "show"], retain_show_usage()),
    clique:register_usage(["vmq-admin", "retain", "delete"], retain_delete_usage()),
    clique:register_usage(["vmq-admin", "log"], log_usage()),
    clique:register_usage(["vmq-admin", "log", "level"], log_level_usage()).

vmq_retain_show_cmd() ->
    Cmd = ["vmq-admin", "retain", "show"],
    KeySpecs = [],
    FlagSpecs =
        [
            {N, [{longname, atom_to_list(N)}]}
         || N <- [limit, rowtimeout, payload, topic, mountpoint]
        ],
    DefaultFields = ["topic", "payload"],
    Callback = vmq_ql_callback("retain_srv", DefaultFields, [{nodes, [node()]}]),
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

vmq_retain_delete_cmd() ->
    Cmd = ["vmq-admin", "retain", "delete"],
    KeySpecs = [],
    FlagSpecs = [
        {mountpoint, [
            {shortname, "m"},
            {longname, "mountpoint"},
            {typecast, fun(Mountpoint) -> Mountpoint end}
        ]},

        {topic, [
            {shortname, "t"},
            {longname, "topic"},
            {typecast, fun(Topic) -> vmq_topic:word(Topic) end}
        ]}
    ],
    Callback = fun
        (_, [], Flags) ->
            Mountpoint =
                case proplists:get_value(mountpoint, Flags) of
                    % no --mountpoint flag given, we take the default mountpoint
                    undefined -> "";
                    MP -> MP
                end,
            case proplists:get_value(topic, Flags) of
                undefined ->
                    Text = clique_status:text("No valid topic given."),
                    [clique_status:alert([Text])];
                Topic ->
                    vmq_retain_srv:delete(Mountpoint, Topic),
                    [clique_status:text("Done")]
            end;
        (_, _, _) ->
            Text = clique_status:text(retain_delete_usage()),
            [clique_status:alert([Text])]
    end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

vmq_session_list_cmd() ->
    Cmd = ["vmq-admin", "session", "show"],
    KeySpecs = [],
    ValidInfoItems = vmq_info:session_info_items(),

    DefaultFields =
        ["peer_port", "peer_host", "user", "mountpoint", "client_id", "is_online"],
    FlagSpecs = [{I, [{longname, atom_to_list(I)}]} || I <- [limit, rowtimeout | ValidInfoItems]],
    Callback = vmq_ql_callback("sessions", DefaultFields, []),
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

vmq_session_disconnect_cmd() ->
    Cmd = ["vmq-admin", "session", "disconnect"],
    KeySpecs = [{'client-id', [{typecast, fun(ClientId) -> ClientId end}]}],
    FlagSpecs = [
        {cleanup, [
            {shortname, "c"},
            {longname, "cleanup"}
        ]},
        {mountpoint, [
            {shortname, "m"},
            {longname, "mountpoint"},
            {typecast, fun(Mountpoint) -> Mountpoint end}
        ]}
    ],

    Callback = fun
        (_, [{'client-id', ClientId}], Flags) ->
            DoCleanup = lists:keymember(cleanup, 1, Flags),
            QueryString0 = "SELECT queue_pid FROM sessions WHERE client_id =\"" ++ ClientId ++ "\"",
            case proplists:get_value(mountpoint, Flags, "") of
                undefined ->
                    %% Unparsable mountpoint or without value
                    Text = clique_status:text("Invalid mountpoint value"),
                    [clique_status:alert([Text])];
                Mountpoint ->
                    QueryString1 = QueryString0 ++ " AND mountpoint=\"" ++ Mountpoint ++ "\"",
                    vmq_ql_query_mgr:fold_query(
                        fun(Row, _) ->
                            QueuePid = maps:get(queue_pid, Row),
                            vmq_queue:force_disconnect(QueuePid, ?ADMINISTRATIVE_ACTION, DoCleanup)
                        end,
                        ok,
                        QueryString1
                    ),
                    [clique_status:text("Done")]
            end;
        (_, _, _) ->
            Text = clique_status:text(vmq_session_disconnect_usage()),
            [clique_status:alert([Text])]
    end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

vmq_session_disconnect_clients_cmd() ->
    Cmd = ["vmq-admin", "session", "disconnect", "clients"],
    KeySpecs = [{'client-ids', [{typecast, fun(ClientId) -> ClientId end}]}],
    FlagSpecs = [
        {cleanup, [
            {shortname, "c"},
            {longname, "cleanup"}
        ]},
        {mountpoint, [
            {shortname, "m"},
            {longname, "mountpoint"},
            {typecast, fun(Mountpoint) -> Mountpoint end}
        ]}
    ],

    Callback = fun
        (_, [{'client-ids', ClientId}], Flags) ->
            DoCleanup = lists:keymember(cleanup, 1, Flags),
            ClientIdList = string:tokens(ClientId, ","),
            OrConditions = lists:map(fun(Id) -> "client_id = \"" ++ Id ++ "\"" end, ClientIdList),
            QueryString0 =
                "SELECT queue_pid FROM sessions WHERE (" ++ string:join(OrConditions, " OR "),
            case proplists:get_value(mountpoint, Flags, "") of
                undefined ->
                    %% Unparsable mountpoint or without value
                    Text = clique_status:text("Invalid mountpoint value"),
                    [clique_status:alert([Text])];
                Mountpoint ->
                    QueryString1 = QueryString0 ++ ") AND mountpoint=\"" ++ Mountpoint ++ "\"",
                    vmq_ql_query_mgr:fold_query(
                        fun(Row, _) ->
                            QueuePid = maps:get(queue_pid, Row),
                            vmq_queue:force_disconnect(QueuePid, ?ADMINISTRATIVE_ACTION, DoCleanup)
                        end,
                        ok,
                        QueryString1
                    ),
                    [clique_status:text("Done")]
            end;
        (_, _, _) ->
            Text = clique_status:text(vmq_session_disconnect_usage()),
            [clique_status:alert([Text])]
    end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

vmq_session_disconnect_batch_cmd() ->
    Cmd = ["vmq-admin", "session", "disconnect", "batch"],
    KeySpecs = [{'count', [{typecast, fun(Limit) -> Limit end}]}],
    FlagSpecs = [
        {cleanup, [
            {shortname, "c"},
            {longname, "cleanup"}
        ]},
        {mountpoint, [
            {shortname, "m"},
            {longname, "mountpoint"},
            {typecast, fun(Mountpoint) -> Mountpoint end}
        ]},
        {filterIDs, [
            {shortname, "f"},
            {longname, "filter-client-ids"},
            {typecast, fun(ClientId) -> ClientId end}
        ]},
        {node, [
            {shortname, "n"},
            {longname, "node"},
            {typecast, fun(Node) -> Node end}
        ]}
    ],

    Callback = fun
        (_, [{'count', Limit}], Flags) ->
            DoCleanup = lists:keymember(cleanup, 1, Flags),
            ClientId = proplists:get_value(filterIDs, Flags, ""),
            QueryWhereFilter =
                case ClientId of
                    undefined ->
                        "";
                    "" ->
                        "";
                    _ ->
                        ClientIdList = string:tokens(ClientId, ","),
                        OrConditions = lists:map(
                            fun(Id) -> "client_id != \"" ++ Id ++ "\"" end, ClientIdList
                        ),
                        "(" ++ string:join(OrConditions, " AND ") ++ ") AND "
                end,
            Node = proplists:get_value(node, Flags, ""),
            QueryWhereNodeFilter =
                case Node of
                    undefined ->
                        "";
                    "" ->
                        "";
                    _ ->
                        NodeIdList = string:tokens(Node, ","),
                        OrConditions2 = lists:map(
                            fun(Id) -> "node = \"" ++ Id ++ "\"" end, NodeIdList
                        ),
                        "(" ++ string:join(OrConditions2, " AND ") ++ ") AND "
                end,
            QueryString0 =
                "SELECT queue_pid FROM sessions WHERE " ++ QueryWhereFilter ++ QueryWhereNodeFilter,

            case proplists:get_value(mountpoint, Flags, "") of
                undefined ->
                    %% Unparsable mountpoint or without value
                    Text = clique_status:text("Invalid mountpoint value"),
                    [clique_status:alert([Text])];
                Mountpoint ->
                    QueryString1 =
                        QueryString0 ++ "mountpoint=\"" ++ Mountpoint ++ "\" LIMIT " ++ Limit,
                    vmq_ql_query_mgr:fold_query(
                        fun(Row, _) ->
                            QueuePid = maps:get(queue_pid, Row),
                            vmq_queue:force_disconnect(QueuePid, ?ADMINISTRATIVE_ACTION, DoCleanup)
                        end,
                        ok,
                        QueryString1
                    ),
                    [clique_status:text("Done")]
            end;
        (_, _, _) ->
            Text = clique_status:text(vmq_session_disconnect_usage()),
            [clique_status:alert([Text])]
    end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

vmq_session_reauthorize_cmd() ->
    Cmd = ["vmq-admin", "session", "reauthorize"],
    KeySpecs = [
        {'username', [{typecast, fun(UserName) -> UserName end}]},
        {'client-id', [{typecast, fun(ClientId) -> ClientId end}]}
    ],
    FlagSpecs = [
        {mountpoint, [
            {shortname, "m"},
            {longname, "mountpoint"},
            {typecast, fun(Mountpoint) -> Mountpoint end}
        ]}
    ],

    Callback = fun(_, Args, Flags) ->
        case lists:keysort(1, Args) of
            [{'client-id', ClientId}, {username, Username}] ->
                case proplists:get_value(mountpoint, Flags, "") of
                    undefined ->
                        %% Unparsable mountpoint or without value
                        Text = clique_status:text("Invalid mountpoint value"),
                        [clique_status:alert([Text])];
                    Mountpoint ->
                        Ret = vernemq_dev_api:reauthorize_subscriptions(
                            list_to_binary(Username),
                            {Mountpoint, list_to_binary(ClientId)},
                            []
                        ),
                        case Ret of
                            {[], []} ->
                                [clique_status:text("Unchanged")];
                            _ ->
                                [clique_status:text("Done")]
                        end
                end;
            _ ->
                Text = clique_status:text(vmq_session_reauthorize_usage()),
                [clique_status:alert([Text])]
        end
    end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

vmq_ql_callback(Table, DefaultFields, Opts) ->
    fun(_, [], Flags) ->
        Limit = proplists:get_value(limit, Flags, "100"),
        RowTimeout = proplists:get_value(rowtimeout, Flags, "100"),
        _ = list_to_integer(Limit),
        _ = list_to_integer(RowTimeout),
        {Fields, WhereEqual, WhereMatch} =
            case Flags of
                [] ->
                    {DefaultFields, [], []};
                _ ->
                    lists:foldl(
                        fun
                            ({limit, _}, Acc) ->
                                Acc;
                            ({rowtimeout, _}, Acc) ->
                                Acc;
                            ({Flag, undefined}, {AccFields, AccWhere, AccMatch}) ->
                                {[atom_to_list(Flag) | AccFields], AccWhere, AccMatch};
                            ({Flag, Val}, {AccFields, AccWhere, AccMatch}) ->
                                case string:slice(Val, 0, 1) of
                                    "~" ->
                                        {AccFields, AccWhere, [
                                            {atom_to_list(Flag), v(Flag, string:slice(Val, 1))}
                                            | AccMatch
                                        ]};
                                    _ ->
                                        {AccFields, [{atom_to_list(Flag), v(Flag, Val)} | AccWhere],
                                            AccMatch}
                                end
                        end,
                        {[], [], []},
                        Flags
                    )
            end,
        FFields =
            case Fields of
                [] ->
                    string:join(DefaultFields, ",");
                _ ->
                    string:join(Fields, ",")
            end,
        WWhere =
            case {WhereEqual, WhereMatch} of
                {[], []} ->
                    [];
                _ ->
                    "WHERE " ++
                        lists:flatten(
                            string:join([[W, "=", V] || {W, V} <- WhereEqual], " AND ")
                        ) ++
                        case {WhereEqual, WhereMatch} of
                            {[], _A} -> [];
                            {_A, []} -> [];
                            _ -> " AND "
                        end ++
                        lists:flatten(
                            string:join([[W, " MATCH ", V] || {W, V} <- WhereMatch], " AND ")
                        )
            end,
        QueryString =
            "SELECT " ++ FFields ++ " FROM " ++ Table ++ " " ++ WWhere ++ " LIMIT " ++ Limit ++
                " ROWTIMEOUT " ++ RowTimeout,
        ResultTable =
            vmq_ql_query_mgr:fold_query(
                fun(Row, Acc) ->
                    [maps:to_list(Row) | Acc]
                end,
                [],
                QueryString,
                Opts
            ),
        [clique_status:table(ResultTable)]
    end.
v(_, "true" = V) ->
    V;
v(_, "false" = V) ->
    V;
v(client_id, V) ->
    "\"" ++ V ++ "\"";
v(user, V) ->
    "\"" ++ V ++ "\"";
v(_, V) ->
    try
        _ = list_to_integer(V),
        V
    catch
        _:_ ->
            "\"" ++ V ++ "\""
    end.

get_nested_value([], Map) ->
    Map;
get_nested_value([Key | Rest], Map) when is_map(Map) ->
    case maps:get(Key, Map, undefined) of
        undefined -> "-";
        Value -> get_nested_value(Rest, Value)
    end.
logger_info(Logger) ->
    case logger:get_handler_config(Logger) of
        {ok, Config} ->
            [
                {logger, get_nested_value([id], Config)},
                {level, get_nested_value([level], Config)},
                {type, get_nested_value([config, type], Config)},
                {file, get_nested_value([config, file], Config)}
            ];
        _ ->
            []
    end.

vmq_loq_show_cmd() ->
    Cmd = ["vmq-admin", "log", "show", "config"],
    KeySpecs = [],
    FlagSpecs = [],
    Callback = fun(_, [], _) ->
        Table = lists:map(fun logger_info/1, logger:get_handler_ids()),
        [clique_status:table(Table)]
    end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

vmq_log_level_cmd() ->
    Cmd = ["vmq-admin", "log", "level"],
    KeySpecs = [
        {'logger', [{typecast, fun(Logger) -> Logger end}]},
        {'level', [{typecast, fun(Level) -> Level end}]}
    ],
    FlagSpecs = [],
    Callback =
        fun
            (_, [{'logger', Logger}, {'level', Level}], _) ->
                vmq_log:set_loglevel(Logger, Level);
            (_, _, _) ->
                Text = clique_status:text(log_level_usage()),
                [clique_status:alert([Text])]
        end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

session_usage() ->
    [
        "vmq-admin session <sub-command>\n\n",
        "  Manage MQTT sessions.\n\n",
        "  Sub-commands:\n",
        "    show        Show and filter running sessions\n",
        "    disconnect  Forcefully disconnect a session\n",
        "    reauthorize Reauthorize subscriptions of a session\n",
        "  Use --help after a sub-command for more details.\n"
    ].

vmq_session_show_usage() ->
    Options = [
        io_lib:format("  --~p\n", [Item])
     || Item <- vmq_info:session_info_items()
    ],
    [
        "vmq-admin session show\n\n",
        "  Show and filter information about MQTT sessions\n\n",
        "Default options:\n"
        "  --client_id --is_online --mountpoint --peer_host --peer_port --user\n\n"
        "Filter:\n"
        "  You can filter for any option like --client_id=test, you can also do a regex search \n"
        "  as in --client_id=~test.*3\n\n"
        "Options\n\n"
        "  --limit=<NumberOfResults>\n"
        "      Limit the number of results returned from each node in the cluster.\n"
        "      Defaults is 100.\n\n"
        "  --rowtimeout=<NumberOfMilliseconds>\n"
        "      Limits the time spent when fetching a single row.\n"
        "      Default is 100 milliseconds.\n"
        | Options
    ].

vmq_session_disconnect_usage() ->
    [
        "vmq-admin session disconnect client-id=<ClientId>\n\n",
        "  Forcefully disconnects a client from the cluster. \n\n",
        "  --mountpoint=<Mountpoint>, -m\n",
        "      specifies the mountpoint, defaults to the default mountpoint\n",
        "  --cleanup, -c\n",
        "      removes the stored cluster state of this client like stored\n",
        "      messages and subscriptions.",
        "\n\n"
        "  Sub-commands:\n",
        "    clients     Forcefully disconnect multiple running sessions\n",
        "    batch       Forcefully disconnect a number of (random) session\n",
        "\n\n"
    ].
vmq_session_disconnect_clients_usage() ->
    [
        "vmq-admin session disconnect clients client-ids=<Comma Seperated List of ClientId>\n\n",
        "  Forcefully disconnects a number of clients from the cluster. \n\n",
        "  --mountpoint=<Mountpoint>, -m\n",
        "      specifies the mountpoint, defaults to the default mountpoint\n",
        "  --cleanup, -c\n",
        "      removes the stored cluster state of this client like stored\n",
        "      messages and subscriptions.",
        "\n\n"
    ].

vmq_session_disconnect_batch_usage() ->
    [
        "vmq-admin session disconnect batch count=Number of clients to be disconnected\n\n",
        "  Forcefully disconnects a number of (random) clients from the cluster. \n\n",
        "  --mountpoint=<Mountpoint>, -m\n",
        "      specifies the mountpoint, defaults to the default mountpoint\n",
        "  --cleanup, -c\n",
        "      removes the stored cluster state of this client like stored\n",
        "      messages and subscriptions.\n",
        "  --filter-client-ids=<list of client-ids>, -f\n"
        "      the clients will be filtered and not disconnected from the cluster\n",
        "  --node=<list of nodes>, -n\n"
        "      limits disconnects to certain nodes\n",
        "\n\n"
    ].

vmq_session_reauthorize_usage() ->
    [
        "vmq-admin session reauthorize username=<Username> client-id=<ClientId>\n\n",
        "  Reauthorizes all current subscriptions of an existing client session. \n\n",
        "  --mountpoint=<Mountpoint>, -m\n",
        "      specifies the mountpoint, defaults to the default mountpoint",
        "\n\n"
    ].

retain_usage() ->
    [
        "vmq-admin retain <sub-command>\n\n",
        "  Inspect MQTT retained messages.\n\n",
        "  Sub-commands:\n",
        "    show        Show and filter running sessions\n",
        "    delete      Delete retained message\n",
        "  Use --help after a sub-command for more details.\n"
    ].

retain_show_usage() ->
    Options = [
        io_lib:format("  --~p\n", [Item])
     || Item <- [payload, topic, mountpoint]
    ],
    [
        "vmq-admin retain show\n\n",
        "  Show and filter MQTT retained messages.\n\n",
        "Default options:\n"
        "  --payload --topic\n\n"
        "Options\n\n"
        "  --limit=<NumberOfResults>\n"
        "      Limit the number of results returned. Defaults is 100.\n"
        "  --mountpoint\n"
        "      Show retained messages for mountpoint.\n"
        "      If no mountpoint is given, the default (empty) mountpoint\n"
        "      is assumed."
        | Options
    ].

retain_delete_usage() ->
    Options = [
        io_lib:format("  --~p\n", [Item])
     || Item <- [topic, mountpoint]
    ],
    [
        "vmq-admin retain delete\n\n",
        "  Delete the retained MQTT message for a topic and mountpoint.\n\n",
        "Default options:\n"
        "  --mountpoint --topic\n\n"
        "If --mountpoint is not set, the default empty mountpoint\n"
        "is assumed.\n\n"
        | Options
    ].

log_usage() ->
    [
        "vmq-admin log <sub-command>\n\n",
        "  Manage VerneMQ log sub-system.\n\n",
        "  Sub-commands:\n",
        "    show config   Shows logging configuration\n",
        "    level         Set log level during runtime\n",
        "  Use --help after a sub-command for more details.\n"
    ].

log_level_usage() ->
    [
        "vmq-admin log level logger=<logger> level=<level>\n\n",
        "  Sets log level for logger\n\n"
    ].
