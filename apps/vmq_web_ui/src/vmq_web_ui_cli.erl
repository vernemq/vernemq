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
    clique:register_usage(["vmq-admin", "webui", "show"], webui_show_usage()),
    clique:register_usage(["vmq-admin", "webui", "install"], webui_install_usage()),
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
                            {'Item', <<"Frontend Path">>},
                            {'Value', code:priv_dir(vmq_web_ui) ++ "/www"}
                        ],
                        [{'Item', <<"Frontend Version">>}, {'Value', "Preview"}]
                    ],
                [clique_status:table(Table)];
            (_, _, _) ->
                Text = clique_status:text(webui_usage()),
                [clique_status:alert([Text])]
        end,
    clique:register_command(Cmd, [], [], Callback).

install_cmd() ->
    Cmd = ["vmq-admin", "webui", "install"],
    FlagSpecs = [
        {version, [
            {shortname, "v"},
            {longname, "version"},
            {typecast, fun(Version) -> Version end}
        ]},
        {repository, [
            {shortname, "r"},
            {longname, "repo"},
            {typecast, fun(Repro) -> Repro end}
        ]}
    ],
    Callback =
        fun(_, [], Flags) ->
            Version = proplists:get_value(version, Flags, "v0.0.1"),
            Rep = proplists:get_value(repository, Flags, "vernemq"),

            Link =
                "https://github.com/" ++ Rep ++ "/vmq_webadmin/releases/download/" ++ Version ++
                    "/frontend.zip",
            Response = httpc:request(get, {Link, []}, [], []),
            Text =
                case Response of
                    {ok, {{_, 200, "OK"}, _Headers, Body}} ->
                        ok = file:write_file("/tmp/vmq_web_ui.zip", Body),
                        ok = file:del_dir_r(code:priv_dir(vmq_web_ui) ++ "/www"),
                        {ok, _} = zip:extract("/tmp/vmq_web_ui.zip", [
                            {'cwd', code:priv_dir(vmq_web_ui) ++ "/www"}
                        ]),
                        ["Frontend installed. Please clear browser cache and try it out..."];
                    _ ->
                        ["Error downloading file"]
                end,
            TextC = clique_status:text(Text),
            [clique_status:alert([TextC])]
        end,
    clique:register_command(Cmd, [], FlagSpecs, Callback).

webui_usage() ->
    [
        "vmq-admin webui <sub-command>\n\n",
        "  Manage VerneMQ Web UI\n\n",
        "  Sub-commands:\n",
        "    show        Show information about web ui\n",
        "    install     Install/update frontend  \n"
        "  Use --help after a sub-command for more details.\n"
    ].

webui_show_usage() ->
    [
        "vmq-admin webui shown\n",
        "  Shows VerneMQ Web UI informatiom\n\n",
        "  Use --help after a sub-command for more details.\n"
    ].

webui_install_usage() ->
    [
        "vmq-admin webui install --repro vernemq --version v0.0.1\n",
        "  Installs a new frontend version. --repro and --version are optional.\n\n",
        "  Use --help after a sub-command for more details.\n"
    ].
