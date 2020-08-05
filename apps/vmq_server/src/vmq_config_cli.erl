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

-module(vmq_config_cli).
-export([register_config/0]).

-type config_error() :: 'bad_wildcard_placement'.

-spec register_config() -> ok | {error, config_error()}.
register_config() ->
    ok = clique_config:load_schema([code:priv_dir(vmq_server)]),
    register_config_(),
    register_cli_usage(),
    vmq_config_show_cmd(),
    vmq_config_reset_cmd().

-spec register_config_() -> ok.
register_config_() ->
    ConfigKeys =
    ["allow_anonymous",
     "allow_register_during_netsplit",
     "allow_publish_during_netsplit",
     "allow_subscribe_during_netsplit",
     "allow_unsubscribe_during_netsplit",
     "allow_multiple_sessions",
     "queue_deliver_mode",
     "queue_type",
     "retry_interval",
     "max_client_id_size",
     "persistent_client_expiration",
     "max_inflight_messages",
     "max_online_messages",
     "max_offline_messages",
     "queue_deliver_mode",
     "queue_type",
     "max_message_rate",
     "max_message_size",
     "upgrade_outgoing_qos",
     "systree_enabled",
     "systree_interval",
     "graphite_enabled",
     "graphite_host",
     "graphite_port",
     "graphite_interval",
     "graphite_include_labels",
     "shared_subscription_policy",
     "remote_enqueue_timeout",
     "topic_alias_max_client",
     "topic_alias_max_broker",
     "max_last_will_delay",
     "receive_max_client",
     "receive_max_broker",
     "suppress_lwt_on_session_takeover",
     "coordinate_registrations",
     "mqtt_connect_timeout"
    ],
    _ = [clique:register_config([Key], fun register_config_callback/3)
         || Key <- ConfigKeys],
    ok = clique:register_config_whitelist(ConfigKeys).

-spec register_cli_usage() -> true.
register_cli_usage() ->
    clique:register_usage(["vmq-admin", "config"], config_usage()),
    clique:register_usage(["vmq-admin", "config", "show"], show_usage()),
    clique:register_usage(["vmq-admin", "config", "reset"], reset_usage()).

-spec register_config_callback([string(), ...], _, [{all, _} | {node, atom()}]) -> any().
register_config_callback([StrKey], _, [{all, _}]) ->
    %% the callback is called, after the application environment is set
    Key = list_to_existing_atom(StrKey),
    {ok, Val} = application:get_env(vmq_server, Key),
    vmq_config:set_global_env(vmq_server, Key, Val, false),
    vmq_config:configure_nodes();
register_config_callback([StrKey], _, [{node, Node}]) ->
    %% the callback is called, after the application environment is set
    Key = list_to_existing_atom(StrKey),
    {ok, Val} = application:get_env(vmq_server, Key),
    vmq_config:set_env(Node, vmq_server, Key, Val, false),
    vmq_config:configure_node(Node);
register_config_callback([StrKey], _, []) ->
    %% the callback is called, after the application environment is set
    Key = list_to_existing_atom(StrKey),
    {ok, Val} = application:get_env(vmq_server, Key),
    vmq_config:set_env(vmq_server, Key, Val, false),
    vmq_config:configure_node().

-spec vmq_config_show_cmd() -> ok | {error, config_error()}.
vmq_config_show_cmd() ->
    Cmd = ["vmq-admin", "config", "show"],
    KeySpecs = [{app, [{typecast, fun(A) -> list_to_existing_atom(A) end}]}],
    FlagSpecs = [{key, [{longname, "key"},
                        {shortname, "k"},
                        {typecast, fun(K) -> list_to_existing_atom(K) end}]}],
    Callback =
    fun(_, [{app, App}], Flags) ->
            FilterKey = lists:keyfind(key, 1, Flags),
            case application:get_env(App, vmq_config_enabled, false) of
                true ->
                    case FilterKey of
                        false ->
                            ConfigsForApp =
                            lists:foldl(
                              fun ({_, vmq_config_enabled, _}, AppAcc) -> AppAcc;
                                  ({_, vmq_plugin_hooks, _}, AppAcc) -> AppAcc;
                                  ({_, included_applications, _}, AppAcc) -> AppAcc;
                                  ({Scope, Key, Val}, AppAcc) ->
                                      %VVal = io_lib:format("~p", [Val]),
                                      VVal = complex_type(Val),
                                      Row =
                                      case Scope of
                                          env ->
                                              [{'Config', Key},
                                               {'Env', VVal},
                                               {'Node', ""},
                                               {'Global', ""}];
                                          node ->
                                              [{'Config', Key},
                                               {'Env', ""},
                                               {'Node', VVal},
                                               {'Global', ""}];
                                          global ->
                                              [{'Config', Key},
                                               {'Env', ""},
                                               {'Node', ""},
                                               {'Global', VVal}]
                                      end,
                                      [Row|AppAcc]
                              end, [], vmq_config:get_prefixed_all_env(App)),
                            [clique_status:table(ConfigsForApp)];
                        {_, Key} ->
                            case vmq_config:get_prefixed_env(App, Key) of
                                {Scope, Key, Val} ->
                                    Text = io_lib:format("Scope: ~p~nValue: ~p", [Scope, Val]),
                                    [clique_status:text(Text)];
                                {error, not_found} ->
                                    [clique_status:alert([clique_status:text("not found")])]
                            end
                    end;
                false ->
                    [clique_status:alert([
                        clique_status:text("App not configured via vmq_config")])]
            end;
       (_, [], _) ->
            [clique_status:alert([
                                  clique_status:text("Please provide an app=<App>")])]

    end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).


-spec complex_type(_) -> atom() | [integer()] | number().
complex_type(Val) when is_boolean(Val) -> Val;
complex_type(Val) when is_number(Val) -> Val;
complex_type(Val) when is_atom(Val) -> Val;
complex_type(Val) when is_list(Val) ->
    is_string(Val, []);
complex_type(_) -> too_complex.

-spec is_string(maybe_improper_list(), [integer()]) -> too_complex | [integer()].
is_string([I|Rest], Acc) when is_integer(I) ->
    is_string(Rest, [I|Acc]);   
is_string([_|_], _) -> too_complex;
is_string([], Acc) -> lists:reverse(Acc).

-spec vmq_config_reset_cmd() -> ok | {error, config_error()}.
vmq_config_reset_cmd() ->
    Cmd = ["vmq-admin", "config", "reset"],
    KeySpecs = [{app, [{typecast, fun(A) -> list_to_existing_atom(A) end}]}],
    FlagSpecs = [{key, [{longname, "key"},
                        {typecast, fun(K) -> list_to_existing_atom(K) end}]},
                 {'config-file', [{longname, "config-file"},
                                  {typecast, fun(F) ->
                                                     case filelib:is_file(F) of
                                                         true -> F;
                                                         false ->
                                                             {error, {invalid_flag_value,
                                                                      {'config-file', F}}}
                                                     end
                                             end}]},
                 {'config-val', [{longname, "config-val"},
                                 {typecast, fun(S) ->
                                                    {ok, T, _} = erl_scan:string(S ++ "."),
                                                    {ok, Term} = erl_parse:parse_term(T),
                                                    Term
                                            end}]},
                 {local, [{longname, "local"}]},
                 {global, [{longname, "global"}]}],

    Callback =
    fun(_, [{app, App}], Flags) ->
            IsLocal = lists:keyfind(local, 1, Flags) =/= false,
            IsGlobal = lists:keyfind(global, 1, Flags) =/= false,
            UseKey = lists:keyfind(key, 1, Flags) =/= false,
            UseConfigFile = lists:keyfind('config-file', 1, Flags) =/= false,
            UseConfigVal = lists:keyfind('config-val', 1, Flags) =/= false,
            case application:get_env(App, vmq_config_enabled, false) of
                true ->
                    case UseKey of
                        true ->
                            %% we only reset given key
                            {_, Key} = lists:keyfind(key, 1, Flags),
                            config_reset(App, Key, IsLocal, IsGlobal),
                            case {UseConfigFile, UseConfigVal} of
                                {true, false} ->
                                    {_, ConfigFile} = lists:keyfind('config-file', 1, Flags),
                                    set_new_config_val_from_file(App, Key, ConfigFile);
                                {false, true} ->
                                    {_, ConfigVal} = lists:keyfind('config-val', 1, Flags),
                                    set_new_config_val(App, Key, ConfigVal);
                                {false, false} ->
                                    %% we leave it as it is
                                    vmq_config:configure_node(),
                                    [clique_status:text("Done")];
                                {true, true} ->
                                    [clique_status:alert(
                                       [clique_status:text("Specify config file or config value")])]
                            end;
                        false ->
                            %% we reset all keys for given app
                            lists:foreach(
                              fun({Key, _}) ->
                                      config_reset(App, Key, IsLocal, IsGlobal)
                              end, application:get_all_env(App)),
                            case UseConfigFile of
                                true ->
                                    {_, ConfigFile} = lists:keyfind('config-file', 1, Flags),
                                    set_new_config_from_file(App, ConfigFile);
                                false ->
                                    vmq_config:configure_node(),
                                    [clique_status:text("Done")]
                            end
                    end;
                false ->
                    [clique_status:alert([
                        clique_status:text("App not configured via vmq_config")])]
            end;
       (_,_,_) ->
            Text = clique_status:text(reset_usage()),
            [clique_status:alert([Text])]
    end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

-spec config_reset(App :: atom(), Key :: any(), boolean(), boolean()) -> ok.
config_reset(App, Key, true, true) ->
    vmq_config:unset_local_env(App, Key),
    vmq_config:unset_global_env(App, Key);
config_reset(App, Key, true, false) ->
    vmq_config:unset_global_env(App, Key);
config_reset(App, Key, _, _) ->
    vmq_config:unset_local_env(App, Key).

-spec set_new_config_val(App :: atom(), Key :: atom(), _) ->
    [{text,
        maybe_improper_list(binary() |
                            maybe_improper_list(any(), binary() | []) |
                            byte(),
                            binary() | [])}, ...].
set_new_config_val(App, Key, Val) ->
    application:set_env(App, Key, Val),
    vmq_config:configure_node(),
    [clique_status:text("Done")].

-spec set_new_config_val_from_file(App :: atom(),
                                   _,
                                   atom() | binary() | [atom() | [any()] | char()]) ->
                                      [{alert, [{_, _} | {_, _, _}]} |
                                       {text,
                                        maybe_improper_list(binary() |
                                                            maybe_improper_list(any(),
                                                                                binary() | []) |
                                                            byte(),
                                                            binary() | [])}, ...].
set_new_config_val_from_file(App, Key, ConfigFile) ->
    case get_app_config(App, ConfigFile) of
        {ok, Config} ->
            case lists:keyfind(Key, 1, Config) of
                false ->
                    [clique_status:alert([
                        clique_status:text("config item not found")])];
                {_, Val} ->
                    application:set_env(App, Key, Val),
                    vmq_config:configure_node(),
                    [clique_status:text("Done")]
            end;
        {error, Reason} ->
            Text = io_lib:format("No config found for app, due to ~p", [Reason]),
            [clique_status:alert([clique_status:text(Text)])]
    end.

set_new_config_from_file(App, ConfigFile) ->
    case get_app_config(App, ConfigFile) of
        {ok, Config} ->
            lists:foreach(
              fun({Key, Val}) ->
                      application:set_env(App, Key, Val)
              end, Config),
            vmq_config:configure_node(),
            [clique_status:text("Done")];
        {error, Reason} ->
            Text = io_lib:format("No config found for app, due to ~p", [Reason]),
            [clique_status:alert([clique_status:text(Text)])]
    end.


-spec get_app_config(App::atom(),
    Key::atom() | binary() | [atom() | [any()] | char()]) ->
       {error, atom() | {non_neg_integer(), atom(), _}} | {ok, _}.
get_app_config(App, ConfigFile) ->
    case file:consult(ConfigFile) of
        {ok, [{application, App, AppConf}]} ->
            %% case a .app file is provided
            {ok, proplists:get_value(env, AppConf, [])};
        {ok, [SysConf]} ->
            %% case sys config provided
            {ok, proplists:get_value(App, SysConf, [])};
        {error, Reason} ->
            {error, Reason}
    end.


show_usage() ->
    ["vmq-admin config show app=<App> [--key=<Key>]\n\n",
     "  Shows the INTERNAL configuration values for the specified application.\n",
     "  Since multiple configuration sources are allowed, this command lets you\n",
     "  inspect whether the config value belongs to the config file or the\n",
     "  distributed configuration store. For values in the configuration store it also\n",
     "  indicates their local or global scope.\n\n",
     "Options\n\n",
     "  --key=<Key>\n",
     "      In some cases the config value is too complex for tabular display.\n",
     "      This is indicated with 'too_complex'. Use the --key flag to\n",
     "      manuallky extract the config value.\n\n"
    ].

reset_usage() ->
    ["vmq-admin config reset app=<App>\n\n",
     "  Resets the INTERNAL configuration values present in the configuration\n",
     "  store. This is done for all config values of an application, but can be\n",
     "  limited to a specific item by providing --key=ConfigKey.\n\n",
     "Options\n\n",
     "  --key=<Key>\n",
     "      Resets a config value of a specific item rather than the whole app config\n",
     "  --local\n",
     "      Only resets the config value specified for this node. A global\n",
     "      config value is used if present. If a global config value is not\n",
     "      present, the currently installed value is kept unless you provide\n",
     "      a new value via --config-val or --config-file flags\n",
     "  --global\n",
     "      Only resets the globally specified config value. If a local config\n"
     "      value is not present, the currently installed value is kept unless\n",
     "      you provide a new value via --config-val or --config-file flags\n",
     "  --config-file = <File>\n",
     "      uses the config provided by the specified config file as the\n",
     "      new default values for either a single item (--key) or the whole app.\n",
     "  --config-val = <Value>\n",
     "      uses the specified config value as the new config value for the item\n",
     "      specified with --key\n\n"
    ].

config_usage() ->
    ["vmq-admin config <sub-command>\n\n",
     "  ADVANCED! Manages internal configuration values. Only use if you know\n",
     "  what you are doing! If you don't please use 'vmq-admin set' command.\n\n",
     "  Sub-commands:\n",
     "    show        shows interal configuration\n",
     "    reset       resets internal configuration\n",
     "  Use --help after a sub-command for more details.\n"
    ].
