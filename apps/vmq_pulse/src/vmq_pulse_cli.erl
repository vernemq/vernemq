%% Copyright 2019 Octavo Labs AG Switzerland (http://octavolabs.com)
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
%%
-module(vmq_pulse_cli).
-export([register_cli/0]).

register_cli() ->
    ConfigKeys = [
        "vmq_pulse.api_key",
        "vmq_pulse.url",
        "vmq_pulse.push_interval",
        "vmq_pulse.connect_timeout"
    ],
    ok = clique:register_config_whitelist(ConfigKeys),
    vmq_pulse_start_cluster_cmd(),
    vmq_pulse_stop_cluster_cmd(),
    vmq_pulse_info_cluster_cmd(),
    vmq_pulse_cli_usage(),
    ok.

vmq_pulse_cli_usage() ->
    Usage =
        [
            "vmq-admin pulse setup | reset\n\n",
            "  Mangage Pulse. The VerneMQ support team can ask you to start a pulse\n",
            "  to gather information and metrics about the VerneMQ cluster. A started\n",
            "  pulse survives the restart of the node.\n",
            "  The following data is periodically sent over HTTP(S) to the Pulse server:\n",
            "    - Cluster status, similar to 'vmq-admin cluster show'\n",
            "    - Plugins, similar to 'vmq-admin plugin show --internal'\n",
            "    - Metrics, similar to 'vmq-admin metrics show'\n",
            "    - Names and versions of loaded OTP applications\n",
            "    - Erlang System Version\n",
            "    - OS Kernel information, output of 'uname -a'\n",
            "    - CPU, RAM, and disk usage\n",
            "    - Pulse Version\n\n"
            "  Sub-commands:\n",
            "    start         Start Pulse for this cluster\n",
            "    stop          Stop Pulse for this cluster\n",
            "    info          Show Pulse info for this cluster\n"
        ],
    clique:register_usage(["vmq-admin", "pulse"], Usage).

vmq_pulse_info_cluster_cmd() ->
    Cmd = ["vmq-admin", "pulse", "info"],
    KeySpecs = [],
    FlagSpecs = [],
    Callback =
        fun(_, _, _) ->
            case vmq_pulse:get_cluster_id() of
                undefined ->
                    Text = io_lib:format("Pulse isn't started, run 'vmq-admin pulse start'", []),
                    [clique_status:alert([clique_status:text(Text)])];
                ClusterId ->
                    Text = io_lib:format("cluster-id: ~s", [ClusterId]),
                    [clique_status:text(Text)]
            end
        end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

vmq_pulse_start_cluster_cmd() ->
    Cmd = ["vmq-admin", "pulse", "start"],
    KeySpecs = [],
    FlagSpecs = [
        {id, [
            {longname, "id"},
            {typecast, fun(Id) -> list_to_binary(Id) end}
        ]}
    ],
    Callback =
        fun(_, [], Flags) ->
            case vmq_pulse:get_cluster_id() of
                undefined ->
                    ClusterId = proplists:get_value(id, Flags, vmq_pulse:generate_cluster_id()),
                    case vmq_pulse:set_cluster_id(ClusterId) of
                        ok ->
                            Text = io_lib:format("Done, cluster-id: ~s", [ClusterId]),
                            [clique_status:text(Text)];
                        {error, Reason} ->
                            Text = io_lib:format("Can't start Pulse due to ~p", [Reason]),
                            [clique_status:alert([clique_status:text(Text)])]
                    end;
                ClusterId ->
                    Text = io_lib:format("Pulse already started, cluster-id: ~s", [ClusterId]),
                    [clique_status:alert([clique_status:text(Text)])]
            end
        end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

vmq_pulse_stop_cluster_cmd() ->
    Cmd = ["vmq-admin", "pulse", "stop"],
    KeySpecs = [],
    FlagSpecs = [],
    Callback =
        fun(_, [], []) ->
            _ = vmq_pulse:del_cluster_id(),
            [clique_status:text("Done")]
        end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).
