%% Copyright 2016 Erlio GmbH Basel Switzerland (http://erl.io)
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

-export([register_cli/0,
         vmql_repl/1]).

register_cli() ->
    vmq_session_list_cmd(),
    vmql_cli_cmd(),

    clique:register_usage(["vmq-admin", "session"], session_usage()),
    clique:register_usage(["vmq-admin", "session", "list"], vmq_session_list_usage()).

vmq_session_list_cmd() ->
    Cmd = ["vmq-admin", "session", "list"],
    KeySpecs = [],
    ValidInfoItems = vmq_info:session_info_items(),
    FlagSpecs = [{I, [{longname, atom_to_list(I)}]} || I <- [limit|ValidInfoItems]],
    Callback = fun(_, [], Flags) ->
                       Limit = proplists:get_value(limit, Flags, "100"),
                       _ = list_to_integer(Limit),
                       Fields =
                       case Flags of
                           [] ->
                               "*";
                           _ ->
                               lists:flatten(string:join([[atom_to_list(I)] || {I, undefined} <- Flags], ","))
                       end,
                       QueryString = "SELECT " ++ Fields ++ " FROM sessions LIMIT " ++Limit,
                       Ret = vmq_info:query(QueryString),
                       Table =
                       lists:foldl(fun(Row, Acc) ->
                                           [maps:to_list(Row)|Acc]
                                   end, [], Ret),
                       [clique_status:table(Table)]
               end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

session_usage() ->
    ["vmq-admin session <sub-command>\n\n",
     "  Retrieve information on live sessions.\n\n",
     "  Sub-commands:\n",
     "    list        lists all running sessions\n",
     "  Use --help after a sub-command for more details.\n"
    ].

vmq_session_list_usage() ->
    Options = [io_lib:format("  --~p\n", [Item])
               || Item <- vmq_info:session_info_items()],
    ["vmq-admin session list\n\n",
     "  Prints some information on running sessions\n\n",
     "Options\n\n" | Options
    ].

vmql_cli_cmd() ->
    Cmd = ["vmq-admin", "vmql"],
    KeySpecs = [],
    FlagSpecs = [],
    Callback = fun(_, _, _) ->
                       io:put_chars(lists:flatten([
"   _   ____  _______    __  \n",
"  | | / /  |/  / __ \\  / /  \n",
"  | |/ / /|_/ / /_/ / / /__ \n",
"  |___/_/  /_/\\___\\_\\/____/ \n",
"                            \n"
                                                  ])),
                       vmql_repl()
               end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

vmql_repl(_Args) ->
    vmql_repl().
vmql_repl() ->
    QueryString = io:get_line("vmql> "),
    try
        Ret = vmq_info:query(QueryString),
        Table =
        lists:foldl(fun(Row, Acc) ->
                            [maps:to_list(Row)|Acc]
                    end, [], Ret),
        {StdOut, _} = clique_writer:write([clique_status:table(Table)], "human"),
        io:put_chars(StdOut)
    catch
        E:R ->
            io:format("~p:~p~n", [E, R])
    end,
    vmql_repl().

