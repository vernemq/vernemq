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

-module(vmq_web_ui_cli).
-export([register_cli/0]).
-behaviour(clique_handler).

register_cli() ->
    clique:register_usage(["vmq-admin", "webui"], webui_usage()),
    show_cmd(),
    install_cmd().

show_cmd() ->
    Cmd = ["vmq-admin", "webui", "show"],
    Callback =
        fun
            (_, [], []) ->
                Table =
                    [
                        [
                            {'Frontend path', code:priv_dir(vmq_web_ui)},
                            {'Frontend version', "Preview"}
                        ]
                    ],
                [clique_status:table(Table)];
            (_, _, _) ->
                Text = clique_status:text(webui_usage()),
                [clique_status:alert([Text])]
        end,
    clique:register_command(Cmd, [], [], Callback).

install_cmd() ->
    Cmd = ["vmq-admin", "webui", "install"],
    Callback =
        fun(_, _, _) ->
            Text = clique_status:text(webui_usage()),
            [clique_status:alert([Text])]
        end,
    clique:register_command(Cmd, [], [], Callback).

webui_usage() ->
    [
        "vmq-admin webui <sub-command>\n\n",
        "  Manage VerneMQ Web UI\n\n",
        "  Sub-commands:\n",
        "    show        Show information about web ui\n",
        "    install     Install/update frontend  \n"
        "  Use --help after a sub-command for more details.\n"
    ].
