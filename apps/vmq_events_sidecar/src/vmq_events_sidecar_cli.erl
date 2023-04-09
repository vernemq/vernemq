%% Copyright Gojek

-module(vmq_events_sidecar_cli).
-export([register_cli/0]).
-behaviour(clique_handler).

register_cli() ->
    register_config(),
    register_cli_usage(),
    status_cmd(),
    enable_cmd(),
    disable_cmd().

register_config() ->
    ConfigKeys =
        [
            "vmq_events_sidecar.hostname",
            "vmq_events_sidecar.port",
            "vmq_events_sidecar.pool_size",
            "vmq_events_sidecar.backlog_size"
        ],
    [
        clique:register_config([Key], fun register_config_callback/3)
     || Key <- ConfigKeys
    ],
    ok = clique:register_config_whitelist(ConfigKeys).

register_config_callback(_, _, _) ->
    ok.

status_cmd() ->
    Cmd = ["vmq-admin", "events", "show"],
    Callback =
        fun
            (_, [], []) ->
                Table =
                    [[{hook, Hook}] || {Hook} <- vmq_events_sidecar_plugin:all_hooks()],
                [clique_status:table(Table)];
            (_, _, _) ->
                Text = clique_status:text(events_usage()),
                [clique_status:alert([Text])]
        end,
    clique:register_command(Cmd, [], [], Callback).

enable_cmd() ->
    Cmd = ["vmq-admin", "events", "enable"],
    KeySpecs = [hook_keyspec()],
    FlagSpecs = [],
    Callback =
        fun
            (_, [{hook, Hook}], _Flags) ->
                case vmq_events_sidecar_plugin:enable_event(Hook) of
                    ok ->
                        [clique_status:text("Done")];
                    {error, Reason} ->
                        lager:warning(
                            "can't enable event ~p due to ~p",
                            [Hook, Reason]
                        ),
                        Text = io_lib:format("can't enable event due to '~p'", [Reason]),
                        [clique_status:alert([clique_status:text(Text)])]
                end;
            (_, _, _) ->
                Text = clique_status:text(enable_usage()),
                [clique_status:alert([Text])]
        end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

disable_cmd() ->
    Cmd = ["vmq-admin", "events", "disable"],
    KeySpecs = [hook_keyspec()],
    FlagSpecs = [],
    Callback =
        fun
            (_, [{hook, Hook}], []) ->
                case vmq_events_sidecar_plugin:disable_event(Hook) of
                    ok ->
                        [clique_status:text("Done")];
                    {error, Reason} ->
                        lager:warning(
                            "can't disable event ~p due to ~p",
                            [Hook, Reason]
                        ),
                        Text = io_lib:format("can't disable event due to '~p'", [Reason]),
                        [clique_status:alert([clique_status:text(Text)])]
                end;
            (_, _, _) ->
                Text = clique_status:text(disable_usage()),
                [clique_status:alert([Text])]
        end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

hook_keyspec() ->
    {hook, [
        {typecast, fun
            (Hook) when is_list(Hook) ->
                case
                    lists:member(
                        Hook,
                        [
                            "on_register",
                            "on_publish",
                            "on_subscribe",
                            "on_unsubscribe",
                            "on_deliver",
                            "on_offline_message",
                            "on_client_wakeup",
                            "on_client_offline",
                            "on_client_gone",
                            "on_delivery_complete",
                            "on_session_expired"
                        ]
                    )
                of
                    true ->
                        binary_to_atom(list_to_binary(Hook), utf8);
                    _ ->
                        {error, {invalid_value, Hook}}
                end;
            (Hook) ->
                {error, {invalid_value, Hook}}
        end}
    ]}.

register_cli_usage() ->
    clique:register_usage(["vmq-admin", "events"], events_usage()),
    clique:register_usage(["vmq-admin", "events", "enable"], enable_usage()),
    clique:register_usage(["vmq-admin", "events", "disable"], disable_usage()),
    clique:register_usage(["vmq-admin", "events", "show"], show_usage()).

events_usage() ->
    [
        "vmq-admin events <sub-command>\n\n",
        "  Manage VerneMQ Events Sidecar.\n\n",
        "  Sub-commands:\n",
        "    show        Show all registered events\n",
        "    enable      Enable an event\n",
        "    disable     Disable an event\n",
        "  Use --help after a sub-command for more details.\n"
    ].

enable_usage() ->
    [
        "vmq-admin events enable hook=<Hook>\n\n",
        "  Enables an event for hook.",
        "\n\n"
    ].

disable_usage() ->
    [
        "vmq-admin events disable hook=<Hook>\n\n",
        "  Disables an event for hook.",
        "\n\n"
    ].

show_usage() ->
    [
        "vmq-admin events show\n\n",
        "  Shows the information of the registered events.",
        "\n\n"
    ].
