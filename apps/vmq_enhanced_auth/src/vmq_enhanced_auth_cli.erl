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

-module(vmq_enhanced_auth_cli).
-export([register/0]).

register() ->
    register_config(),
    register_cli_usage(),
    show_ratelimit_cmd(),
    enable_ratelimit_cmd(),
    disable_ratelimit_cmd().

register_config() ->
    ConfigKeys =
        [
            "vmq_enhanced_auth.acl_file",
            "vmq_enhanced_auth.acl_reload_interval",
            "vmq_enhanced_auth.enable_jwt_auth",
            "vmq_enhanced_auth.enable_acl_hooks",
            "vmq_enhanced_auth.secret_key"
        ],
    [
        clique:register_config([Key], fun register_config_callback/2)
     || Key <- ConfigKeys
    ],
    ok = clique:register_config_whitelist(ConfigKeys).

register_config_callback(_, _) ->
    vmq_enhanced_auth_reloader:change_config_now().

show_ratelimit_cmd() ->
    Cmd = ["vmq-admin", "publish-ratelimit", "show"],
    Callback =
        fun
            (_, [], []) ->
                Rates = vmq_enhanced_auth_rate_limiter:list_rates(),
                Table =
                    [[{acl_name, AclName}, {rate, Rate}] || {AclName, Rate} <- Rates],
                [clique_status:table(Table)];
            (_, _, _) ->
                Text = clique_status:text(show_ratelimit_usage()),
                [clique_status:alert([Text])]
        end,
    clique:register_command(Cmd, [], [], Callback).

enable_ratelimit_cmd() ->
    Cmd = ["vmq-admin", "publish-ratelimit", "enable"],
    KeySpecs = [acl_name_keyspec(), rate_keyspec()],
    FlagSpecs = [],
    Callback =
        fun
            (_, [_, _] = List, []) ->
                AclName = get_value(acl_name, List),
                Rate = get_value(rate, List),
                case {AclName, Rate} of
                    {undefined, _} ->
                        Text = clique_status:text(enable_ratelimit_usage()),
                        [clique_status:alert([Text])];
                    {_, undefined} ->
                        Text = clique_status:text(enable_ratelimit_usage()),
                        [clique_status:alert([Text])];
                    _ ->
                        vmq_enhanced_auth_rate_limiter:set_rate(AclName, Rate),
                        [clique_status:text("Done")]
                end;
            (_, _, _) ->
                Text = clique_status:text(enable_ratelimit_usage()),
                [clique_status:alert([Text])]
        end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

disable_ratelimit_cmd() ->
    Cmd = ["vmq-admin", "publish-ratelimit", "disable"],
    KeySpecs = [acl_name_keyspec()],
    FlagSpecs = [],
    Callback =
        fun
            (_, [{acl_name, AclName}], []) ->
                case vmq_enhanced_auth_rate_limiter:delete_rate(AclName) of
                    ok ->
                        [clique_status:text("Done")];
                    {error, not_found} ->
                        Text = io_lib:format(
                            "no rate limit configured for acl_name '~s'", [AclName]
                        ),
                        [clique_status:alert([clique_status:text(Text)])]
                end;
            (_, _, _) ->
                Text = clique_status:text(disable_ratelimit_usage()),
                [clique_status:alert([Text])]
        end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

get_value(Key, List) ->
    case lists:keyfind(Key, 1, List) of
        false -> undefined;
        {_, Value} -> Value
    end.

acl_name_keyspec() ->
    {acl_name, [
        {typecast, fun
            (N) when is_list(N) ->
                list_to_binary(N);
            (N) ->
                {error, {invalid_value, N}}
        end}
    ]}.

rate_keyspec() ->
    {rate, [
        {typecast, fun(StrR) ->
            try list_to_integer(StrR) of
                R when R > 0 -> R;
                _ -> {error, {invalid_args, [{rate, StrR}]}}
            catch
                error:badarg -> {error, {invalid_args, [{rate, StrR}]}}
            end
        end}
    ]}.

register_cli_usage() ->
    clique:register_usage(
        ["vmq-admin", "publish-ratelimit"], ratelimit_usage()
    ),
    clique:register_usage(
        ["vmq-admin", "publish-ratelimit", "show"], show_ratelimit_usage()
    ),
    clique:register_usage(
        ["vmq-admin", "publish-ratelimit", "enable"], enable_ratelimit_usage()
    ),
    clique:register_usage(
        ["vmq-admin", "publish-ratelimit", "disable"], disable_ratelimit_usage()
    ).

ratelimit_usage() ->
    [
        "vmq-admin publish-ratelimit <sub-command>\n\n",
        "  Manage per-acl_name publish rate limits.\n\n",
        "  Sub-commands:\n",
        "    show      Show all configured publish rate limits\n",
        "    enable    Enable a publish rate limit for an acl_name\n",
        "    disable   Disable a publish rate limit for an acl_name\n"
    ].

show_ratelimit_usage() ->
    [
        "vmq-admin publish-ratelimit show\n\n",
        "  Show all configured per-acl_name publish rate limits.\n"
    ].

enable_ratelimit_usage() ->
    [
        "vmq-admin publish-ratelimit enable acl_name=<acl_name> rate=<pub_per_sec>\n\n",
        "  Enable the publish rate limit for an acl_name with the specified rate.\n\n",
        "  Options:\n",
        "    acl_name   The ACL name to rate limit\n",
        "    rate       Maximum publishes per second (positive integer)\n"
    ].

disable_ratelimit_usage() ->
    [
        "vmq-admin publish-ratelimit disable acl_name=<acl_name>\n\n",
        "  Disable the publish rate limit for an acl_name.\n\n",
        "  Options:\n",
        "    acl_name   The ACL name to disable the rate limit for\n"
    ].
