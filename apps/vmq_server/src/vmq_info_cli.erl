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

-module(vmq_info_cli).

-include("vmq_server.hrl").

-export([register_cli/0]).

register_cli() ->
    vmq_session_list_cmd(),
    vmq_session_disconnect_cmd(),
    vmq_session_reauthorize_cmd(),
    vmq_retain_show_cmd(),

    clique:register_usage(["vmq-admin", "session"], session_usage()),
    clique:register_usage(["vmq-admin", "session", "show"], vmq_session_show_usage()),
    clique:register_usage(["vmq-admin", "session", "disconnect"], vmq_session_disconnect_usage()),
    clique:register_usage(["vmq-admin", "session", "reauthorize"], vmq_session_reauthorize_usage()),
    clique:register_usage(["vmq-admin", "retain"], retain_usage()),
    clique:register_usage(["vmq-admin", "retain"], retain_show_usage()).

vmq_retain_show_cmd() ->
    Cmd = ["vmq-admin", "retain", "show"],
    KeySpecs = [],
    FlagSpecs =
        [{N, [{longname, atom_to_list(N)}]} || N <- [limit,rowtimeout,payload,topic,mountpoint]],
    DefaultFields = ["topic", "payload"],
    Callback = vmq_ql_callback("retain_srv", DefaultFields, [{nodes, [node()]}]),
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

vmq_session_list_cmd() ->
    Cmd = ["vmq-admin", "session", "show"],
    KeySpecs = [],
    ValidInfoItems = vmq_info:session_info_items(),

    DefaultFields =
        ["peer_port", "peer_host", "user", "mountpoint", "client_id", "is_online"],
    FlagSpecs = [{I, [{longname, atom_to_list(I)}]} || I <- [limit,rowtimeout|ValidInfoItems]],
    Callback = vmq_ql_callback("sessions", DefaultFields, []),
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

vmq_session_disconnect_cmd() ->
    Cmd = ["vmq-admin", "session", "disconnect"],
    KeySpecs = [{'client-id', [{typecast, fun(ClientId) -> ClientId end}]}],
    FlagSpecs = [{cleanup, [{shortname, "c"},
                            {longname, "cleanup"}]},
                 {mountpoint, [{shortname, "m"},
                               {longname, "mountpoint"},
                               {typecast,
                                fun(Mountpoint) -> Mountpoint end}]}],

    Callback = fun(_, [{'client-id', ClientId}], Flags) ->
                       DoCleanup = lists:keymember(cleanup, 1, Flags),
                       QueryString0 = "SELECT queue_pid FROM sessions WHERE client_id =\"" ++ ClientId ++ "\"",
                       case proplists:get_value(mountpoint, Flags, "") of
                           undefined ->
                               %% Unparseable mountpoint or without value
                               Text = clique_status:text("Invalid mountpoint value"),
                               [clique_status:alert([Text])];
                           Mountpoint ->
                               QueryString1 = QueryString0 ++ " AND mountpoint=\"" ++ Mountpoint ++ "\"",
                               vmq_ql_query_mgr:fold_query(
                                 fun(Row, _) ->
                                         QueuePid = maps:get(queue_pid, Row),
                                         vmq_queue:force_disconnect(QueuePid, ?ADMINISTRATIVE_ACTION, DoCleanup)
                                 end, ok, QueryString1),
                               [clique_status:text("Done")]
                       end;
                  (_,_,_) ->
                       Text = clique_status:text(vmq_session_disconnect_usage()),
                       [clique_status:alert([Text])]
               end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

vmq_session_reauthorize_cmd() ->
    Cmd = ["vmq-admin", "session", "reauthorize"],
    KeySpecs = [{'username', [{typecast, fun(UserName) -> UserName end}]},
                {'client-id', [{typecast, fun(ClientId) -> ClientId end}]}],
    FlagSpecs = [{mountpoint, [{shortname, "m"},
                               {longname, "mountpoint"},
                               {typecast,
                                fun(Mountpoint) -> Mountpoint end}]}],

    Callback = fun(_, Args, Flags) ->
                       case lists:keysort(1, Args) of
                           [{'client-id', ClientId}, {username, Username}] ->
                               case proplists:get_value(mountpoint, Flags, "") of
                                   undefined ->
                                       %% Unparseable mountpoint or without value
                                       Text = clique_status:text("Invalid mountpoint value"),
                                       [clique_status:alert([Text])];
                                   Mountpoint ->
                                       Ret = vernemq_dev_api:reauthorize_subscriptions(
                                             list_to_binary(Username),
                                             {Mountpoint, list_to_binary(ClientId)},
                                             []),
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
            {Fields, Where} =
                case Flags of
                    [] ->
                        {DefaultFields, []};
                    _ ->
                        lists:foldl(
                          fun({limit, _}, Acc) ->
                                  Acc;
                             ({rowtimeout, _}, Acc) ->
                                  Acc;
                             ({Flag, undefined}, {AccFields, AccWhere}) ->
                                  {[atom_to_list(Flag)|AccFields], AccWhere};
                             ({Flag, Val}, {AccFields, AccWhere}) ->
                                  {AccFields, [{atom_to_list(Flag), v(Flag, Val)}|AccWhere]}
                          end, {[],[]}, Flags)
                end,
            FFields =
                case Fields of
                    [] ->
                        string:join(DefaultFields, ",");
                    _ ->
                        string:join(Fields, ",")
                end,
            WWhere =
                case Where of
                    [] ->
                        [];
                    _ ->
                        "WHERE " ++ lists:flatten(
                                      string:join([[W, "=", V] || {W,V} <- Where], " AND "))
                end,
            QueryString = "SELECT " ++ FFields ++ " FROM " ++ Table ++ " " ++ WWhere ++ " LIMIT " ++ Limit ++ " ROWTIMEOUT " ++ RowTimeout,
            ResultTable =
                vmq_ql_query_mgr:fold_query(
                  fun(Row, Acc) ->
                          [maps:to_list(Row)|Acc]
                  end, [], QueryString, Opts),
            [clique_status:table(ResultTable)]
    end.
v(_, "true" = V) -> V;
v(_, "false" = V) -> V;
v(client_id, V) -> "\"" ++ V ++ "\"";
v(user, V) -> "\"" ++ V ++ "\"";
v(_, V) ->
    try
        _ = list_to_integer(V),
        V
    catch
        _:_ ->
            "\"" ++ V ++ "\""
    end.

session_usage() ->
    ["vmq-admin session <sub-command>\n\n",
     "  Manage MQTT sessions.\n\n",
     "  Sub-commands:\n",
     "    show        Show and filter running sessions\n",
     "    disconnect  Forcefully disconnect a session\n",
     "    reauthorize Reauthorize subscriptions of a session\n",
     "  Use --help after a sub-command for more details.\n"
    ].

vmq_session_show_usage() ->
    Options = [io_lib:format("  --~p\n", [Item])
               || Item <- vmq_info:session_info_items()],
    ["vmq-admin session show\n\n",
     "  Show and filter information about MQTT sessions\n\n",
     "Default options:\n"
     "  --client_id --is_online --mountpoint --peer_host --peer_port --user\n\n"
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
    ["vmq-admin session disconnect client-id=<ClientId>\n\n",
     "  Forcefully disconnects a client from the cluster. \n\n",
     "  --mountpoint=<Mountpoint>, -m\n",
     "      specifies the mountpoint, defaults to the default mountpoint\n",
     "  --cleanup, -c\n",
     "      removes the stored cluster state of this client like stored\n",
     "      messages and subscriptions.",
     "\n\n"
    ].

vmq_session_reauthorize_usage() ->
    ["vmq-admin session reauthorize username=<Username> client-id=<ClientId>\n\n",
     "  Reauthorizes all current subscriptions of an existing client session. \n\n",
     "  --mountpoint=<Mountpoint>, -m\n",
     "      specifies the mountpoint, defaults to the default mountpoint",
     "\n\n"
    ].

retain_usage() ->
    ["vmq-admin retain <sub-command>\n\n",
     "  Inspect MQTT retained messages.\n\n",
     "  Sub-commands:\n",
     "    show        Show and filter running sessions\n",
     "  Use --help after a sub-command for more details.\n"
    ].

retain_show_usage() ->
    Options = [io_lib:format("  --~p\n", [Item])
               || Item <- [payload, topic, mountpoint]],
    ["vmq-admin retain show\n\n",
     "  Show and filter MQTT retained messages.\n\n",
     "Default options:\n"
     "  --payload --topic\n\n"
     "Options\n\n"
     "  --limit=<NumberOfResults>\n"
     "      Limit the number of results returned. Defaults is 100.\n"
     | Options
    ].
