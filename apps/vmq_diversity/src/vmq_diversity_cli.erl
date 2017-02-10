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

-module(vmq_diversity_cli).
-export([register_cli/0]).
-behaviour(clique_handler).

register_cli() ->
    register_config(),
    register_cli_usage(),
    status_cmd(),
    load_cmd(),
    reload_cmd(),
    unload_cmd().

register_config() ->
    ConfigKeys =
    ["vmq_diversity.keep_state"],
    [clique:register_config([Key], fun register_config_callback/3)
     || Key <- ConfigKeys],
    ok = clique:register_config_whitelist(ConfigKeys).

register_config_callback(_, _, _) ->
    ok.


register_cli_usage() ->
    clique:register_usage(["vmq-admin", "script"], script_usage()),
    clique:register_usage(["vmq-admin", "script", "load"], load_usage()),
    clique:register_usage(["vmq-admin", "script", "reload"], reload_usage()),
    clique:register_usage(["vmq-admin", "script", "unload"], unload_usage()),
    clique:register_usage(["vmq-admin", "script", "status"], status_usage()).

status_cmd() ->
    Cmd = ["vmq-admin", "script", "status"],
    Callback = fun(_, _, _) ->
                       AllStats = vmq_diversity_script_sup:stats(),
                       lists:foldl(fun({Script, Stats}, Acc) ->
                                          [clique_status:table([[{script, Script}|Stats]])
                                           |Acc]
                                   end, [], AllStats)
               end,
    clique:register_command(Cmd, [], [], Callback).

load_cmd() ->
    Cmd = ["vmq-admin", "script", "load"],
    KeySpecs = [path_keyspec()],
    Callback = fun(_, [{path, Path}], _) ->
                       case vmq_diversity:load_script(Path) of
                           {error, _} ->
                               [clique_status:text("Can't load script!")];
                           {ok, _} ->
                               [clique_status:text("Script successfully loaded!")]
                       end
               end,
    clique:register_command(Cmd, KeySpecs, [], Callback).

reload_cmd() ->
    Cmd = ["vmq-admin", "script", "reload"],
    KeySpecs = [path_keyspec()],
    Callback = fun(_, [{path, Path}], _) ->
                       case vmq_diversity:reload_script(Path) of
                           {error, _} ->
                               [clique_status:text("Can't reload script!")];
                           ok ->
                               [clique_status:text("Script successfully reloaded!")]
                       end
               end,
    clique:register_command(Cmd, KeySpecs, [], Callback).

unload_cmd() ->
    Cmd = ["vmq-admin", "script", "unload"],
    KeySpecs = [path_keyspec()],
    Callback = fun(_, [{path, Path}], _) ->
                       case vmq_diversity:unload_script(Path) of
                           {error, _} ->
                               [clique_status:text("Can't unload script!")];
                           ok ->
                               [clique_status:text("Script successfully unloaded!")]
                       end
               end,
    clique:register_command(Cmd, KeySpecs, [], Callback).

script_usage() ->
    ["vmq-admin script <sub-command>\n\n",
     "  Manage the plugin scripts on this VerneMQ node.\n\n",
     "  Sub-commands:\n",
     "    status      Show the status of loaded scripts\n",
     "    load        Load a script file\n",
     "    unload      Unload a script file\n\n",
     "  Use --help after a sub-command for more details.\n"
    ].

load_usage() ->
    ["vmq-admin script load path=<PathToScript>\n\n",
     "  Loads the script at <PathToScript> into VerneMQ.",
     "\n\n"
    ].

reload_usage() ->
    ["vmq-admin script reload path=<PathToScript>\n\n",
     "  Reloads the script at <PathToScript> into VerneMQ.",
     "\n\n"
    ].

unload_usage() ->
    ["vmq-admin script unload path=<PathToScript>\n\n",
     "  Unloads the script at <PathToScript>.",
     "\n\n"
    ].

status_usage() ->
    ["vmq-admin script status\n\n",
     "  Shows the information of the loaded scripts.",
     "\n\n"
    ].

path_keyspec() ->
    {path, [{typecast, fun(Path) ->
                               case filelib:is_file(Path) of
                                   true -> Path;
                                   false ->
                                       {error, {invalid_value, Path}}
                               end
                       end}]}.

