%% Copyright 2018 Octavo Labs AG Basel Switzerland (http://erl.io)
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

-module(vmq_bridge_cli).
-export([register_cli/0]).
-behaviour(clique_handler).

register_cli() ->
    clique:register_usage(["vmq-admin", "bridge"], bridge_usage()),
    show_cmd(),
    start_cmd().

show_cmd() ->
    Cmd = ["vmq-admin", "bridge", "show"],
    Callback =
        fun
            (_, [], []) ->
                Table =
                    [
                        [
                            {'name', Name},
                            {endpoint, iolist_to_binary([Host, $:, integer_to_binary(Port)])},
                            {'buffer size', Size},
                            {'buffer max', Max},
                            {'buffer dropped msgs', Dropped},
                            {'MQTT process mailbox len', MailboxSize}
                        ]
                     || #{
                            name := Name,
                            host := Host,
                            port := Port,
                            out_buffer_size := Size,
                            out_buffer_max_size := Max,
                            out_buffer_dropped := Dropped,
                            process_mailbox_size := MailboxSize
                        } <- bridge_info()
                    ],
                [clique_status:table(Table)];
            (_, _, _) ->
                Text = clique_status:text(bridge_usage()),
                [clique_status:alert([Text])]
        end,
    clique:register_command(Cmd, [], [], Callback).

start_cmd() ->
    Cmd = ["vmq-admin", "bridge", "start"],
    Callback =
        fun
            (_, [], []) ->
                Config = application:get_all_env(vmq_bridge),
                vmq_bridge_sup:change_config([{vmq_bridge, Config}]),
                Text = clique_status:text("VerneMQ Bridges restarted."),
                [clique_status:alert([Text])];
            (_, _, _) ->
                Text = clique_status:text(bridge_usage()),
                [clique_status:alert([Text])]
        end,
    clique:register_command(Cmd, [], [], Callback).

bridge_usage() ->
    [
        "vmq-admin bridge <sub-command>\n\n",
        "  Manage MQTT bridges.\n\n",
        "  Sub-commands:\n",
        "    show        Show information about bridges\n\n",
        "    start       Start the VerneMQ bridges. Useful after enabling the bridge plugin dynamically. \n\n"
        "  Use --help after a sub-command for more details.\n"
    ].

bridge_info() ->
    vmq_bridge_sup:bridge_info().
