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

-export([register_cli/0]).

register_cli() ->
    vmq_session_list_cmd(),

    clique:register_usage(["vmq-admin", "session"], session_usage()),
    clique:register_usage(["vmq-admin", "session", "show"], vmq_session_show_usage()).

vmq_session_list_cmd() ->
    Cmd = ["vmq-admin", "session", "show"],
    KeySpecs = [],
    ValidInfoItems = vmq_info:session_info_items(),

    DefaultFields =
        ["peer_port", "peer_host", "user", "mountpoint", "client_id", "is_online"],
    FlagSpecs = [{I, [{longname, atom_to_list(I)}]} || I <- [limit,rowtimeout|ValidInfoItems]],
    Callback = fun(_, [], Flags) ->
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
                                         {AccFields, [{atom_to_list(Flag), v(Val)}|AccWhere]}
                                 end, {[],[]}, Flags)
                       end,
                       FFields =
                       case Fields of
                           [] ->
                               "*";
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
                       QueryString = "SELECT " ++ FFields ++ " FROM sessions " ++ WWhere ++ " LIMIT " ++ Limit ++ " ROWTIMEOUT " ++ RowTimeout,
                       Table =
                       vmq_ql_query_mgr:fold_query(
                         fun(Row, Acc) ->
                                 [maps:to_list(Row)|Acc]
                         end, [], QueryString),
                       [clique_status:table(Table)]
               end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

v("true" = V) -> V;
v("false" = V) -> V;
v(V) ->
    try
        _ = list_to_integer(V),
        V
    catch
        _:_ ->
            "\"" ++ V ++ "\""
    end.

session_usage() ->
    ["vmq-admin session <sub-command>\n\n",
     "  Retrieve information on live sessions.\n\n",
     "  Sub-commands:\n",
     "    show        Show and filter running sessions\n",
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
