%% Copyright Gojek

-module(vmq_events_sidecar_cli).
-export([register_cli/0]).
-behaviour(clique_handler).

register_cli() ->
    register_config(),
    register_cli_usage(),
    status_cmd(),
    enable_cmd(),
    disable_cmd(),
    show_sampling_cmd(),
    enable_sampling_cmd(),
    disable_sampling_cmd().

register_config() ->
    ConfigKeys =
        [
            "vmq_events_sidecar.hostname",
            "vmq_events_sidecar.port",
            "vmq_events_sidecar.pool_size",
            "vmq_events_sidecar.backlog_size"
        ],
    [
        clique:register_config([Key], fun register_config_callback/2)
     || Key <- ConfigKeys
    ],
    ok = clique:register_config_whitelist(ConfigKeys).

register_config_callback(_, _) ->
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

show_sampling_cmd() ->
    Cmd = ["vmq-admin", "events", "sampling", "show"],
    KeySpecs = [hook_sampling_keyspec()],
    FlagSpecs = [],
    Callback =
        fun
            (_, [{hook, Hook}], []) ->
                Table =
                    [
                        [{acl_name, binary_to_atom(C)}, {'Percentage', P}]
                     || [C, P] <- vmq_events_sidecar_plugin:list_sampling_conf(Hook)
                    ],
                [clique_status:table(Table)];
            (_, _, _) ->
                Text = clique_status:text(show_sampling_usage()),
                [clique_status:alert([Text])]
        end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

get_value(Key, List) ->
    case lists:keyfind(Key, 1, List) of
        false -> undefined;
        {_, Value} -> Value
    end.

enable_sampling_cmd() ->
    Cmd = ["vmq-admin", "events", "sampling", "enable"],
    KeySpecs = [
        hook_sampling_keyspec(), sampling_percentage_keyspec(), sampling_acl_name_keyspec()
    ],
    FlagSpecs = [],
    Callback =
        fun
            (_, [_, _, _] = List, []) ->
                Hook = get_value(hook, List),
                P = get_value(percentage, List),
                ACL = get_value(acl_name, List),
                vmq_events_sidecar_plugin:enable_sampling(Hook, ACL, P),
                [clique_status:text("Done")];
            (_, _, _) ->
                Text = clique_status:text(enable_sampling_usage()),
                [clique_status:alert([Text])]
        end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

disable_sampling_cmd() ->
    Cmd = ["vmq-admin", "events", "sampling", "disable"],
    KeySpecs = [hook_sampling_keyspec(), sampling_acl_name_keyspec()],
    FlagSpecs = [],
    Callback =
        fun
            (_, [_, _] = List, []) ->
                Hook = get_value(hook, List),
                ACL = get_value(acl_name, List),
                case vmq_events_sidecar_plugin:disable_sampling(Hook, ACL) of
                    ok ->
                        [clique_status:text("Done")];
                    {error, Reason} ->
                        Text = io_lib:format(
                            "can't disable sampling for hook: ~p criterion: ~p due to '~p'", [
                                Hook, ACL, Reason
                            ]
                        ),
                        [clique_status:alert([clique_status:text(Text)])]
                end;
            (_, _, _) ->
                Text = clique_status:text(disable_sampling_usage()),
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
                            "on_session_expired",
                            "on_message_drop"
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

hook_sampling_keyspec() ->
    {hook, [
        {typecast, fun
            (Hook) when is_list(Hook) ->
                case
                    lists:member(
                        Hook,
                        [
                            "on_publish",
                            "on_deliver",
                            "on_delivery_complete"
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

sampling_acl_name_keyspec() ->
    sampling_criterion_flagspec(acl_name).

sampling_criterion_flagspec(CName) ->
    {CName, [
        {longname, atom_to_list(CName)},
        {typecast, fun
            (C) when is_list(C) ->
                list_to_binary(C);
            (C) ->
                {error, {invalid_flag, C}}
        end}
    ]}.

sampling_percentage_keyspec() ->
    {percentage, [
        {typecast, fun(StrP) ->
            case catch list_to_integer(StrP) of
                P when (P >= 0) and (P =< 100) -> P;
                _ -> {error, {invalid_args, [{percentage, StrP}]}}
            end
        end}
    ]}.

register_cli_usage() ->
    clique:register_usage(["vmq-admin", "events"], events_usage()),
    clique:register_usage(["vmq-admin", "events", "enable"], enable_usage()),
    clique:register_usage(["vmq-admin", "events", "disable"], disable_usage()),
    clique:register_usage(["vmq-admin", "events", "show"], show_usage()),
    clique:register_usage(["vmq-admin", "events", "sampling"], events_sampling_usage()),
    clique:register_usage(
        ["vmq-admin", "events", "sampling", "enable"], enable_sampling_usage()
    ),
    clique:register_usage(
        ["vmq-admin", "events", "sampling", "disable"],
        disable_sampling_usage()
    ),
    clique:register_usage(
        ["vmq-admin", "events", "sampling", "show"], show_sampling_usage()
    ).

events_usage() ->
    [
        "vmq-admin events <sub-command>\n\n",
        "  Manage VerneMQ Events Sidecar.\n\n",
        "  Sub-commands:\n",
        "    show        Show all registered events\n",
        "    enable      Enable an event\n",
        "    disable     Disable an event\n",
        "    sampling    Allows sampling of enabled events\n"
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

events_sampling_usage() ->
    [
        "vmq-admin events sampling \n\n",
        "  Allows sampling of hook specific events based on acl_name/user\n\n",
        "  Sub-commands:\n",
        "    show        Shows all the hook specific sampling configurations\n",
        "    enable      Enables sampling for events based on acl/user\n",
        "    disable     Disables sampling\n",
        "  Use --help after a sub-command for more details.\n"
    ].

enable_sampling_usage() ->
    [
        "vmq-admin events sampling enable hook=<Hook> percentage=<Percentage> acl_name=<ACL>\n\n",
        "  Enables sampling based on acl_name/label for on_publish, on_deliver & on_delivery_complete.",
        "\n\n"
    ].

disable_sampling_usage() ->
    [
        "vmq-admin events sampling disable hook=<Hook> acl_name=<ACL>\n\n",
        "  Disables sampling for specified hook based on the acl_name/label.",
        "\n\n"
    ].

show_sampling_usage() ->
    [
        "vmq-admin events sampling show hook=<Hook>\n\n",
        "  Shows all the hook specific sampling configurations.",
        "\n\n"
    ].
