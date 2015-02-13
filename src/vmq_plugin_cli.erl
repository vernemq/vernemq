%% Copyright 2014 Erlio GmbH Basel Switzerland (http://erl.io)
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

-module(vmq_plugin_cli).
-behaviour(clique_handler).

-export([register_cli/0]).

register_cli() ->
    vmq_plugin_cli_usage(),
    vmq_plugin_show_cmd(),
    vmq_plugin_enable_cmd(),
    vmq_plugin_disable_cmd().

vmq_plugin_cli_usage() ->
    clique:register_usage(["vmq-admin", "plugin"], plugin_usage()),
    clique:register_usage(["vmq-admin", "plugin", "show"], plugin_show_usage()),
    clique:register_usage(["vmq-admin", "plugin", "enable"], plugin_enable_usage()),
    clique:register_usage(["vmq-admin", "plugin", "disable"], plugin_disable_usage()).

vmq_plugin_show_cmd() ->
    Cmd = ["vmq-admin", "plugin", "show"],
    KeySpecs = [],
    FlagSpecs = [{plugin, [{longname, "plugin"},
                           {typecast, fun(P) -> list_to_atom(P) end}]},
                 {hook, [{longname, "hook"},
                         {typecast, fun(H) -> list_to_atom(H) end}]}],
    Callback =
    fun([], Flags) ->
            PF = case lists:keyfind(plugin, 1, Flags) of
                               false -> [];
                               {_, P} -> [P]
                           end,
            HF = case lists:keyfind(hook, 1, Flags) of
                               false -> [];
                               {_, H} -> [H]
                           end,
            Plugins =
            lists:foldl(
              fun({Hook, Plugin, _, Arity}, [{Plugin, Hooks}|Acc])
                    when ((PF == []) or (PF == [Plugin]))
                         and
                         ((HF == []) or (HF == [Hook])) ->
                      [{Plugin, [{Hook, Arity}|Hooks]}|Acc];
                 ({Hook, Plugin, _, Arity}, Acc)
                   when ((PF == []) or (PF == [Plugin]))
                        and
                        ((HF == []) or (HF == [Hook])) ->
                      [{Plugin, [{Hook, Arity}]}|Acc];
                 (_, Acc) ->
                      Acc
              end, [], lists:keysort(2, vmq_plugin:info(all))),
            Table =
            lists:foldl(
              fun({Plugin, Hooks}, Acc) ->
                      HooksTxt =
                      lists:flatten([io_lib:format("~p/~p~n", [Hook, Arity])
                                     || {Hook, Arity} <- Hooks]),
                      [[{'Plugin', Plugin},
                        {'Hooks', HooksTxt ++ "\n"}]
                       |Acc]
              end, [], Plugins),
            [clique_status:table(Table)]
    end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).


vmq_plugin_flag_specs() ->
    [{name, [{shortname, "n"},
             {longname, "name"},
             {typecast, fun(V) -> list_to_atom(V) end}]},
     {path, [{shortname, "p"},
             {longname, "path"},
             {typecast, fun(P) ->
                                case filelib:is_dir(P) of
                                    true -> P;
                                    false ->
                                        {error, {invalid_flag_value,
                                                 {path, P}}}
                                end
                        end}]},
     {module, [{shortname, "m"},
               {longname, "module"},
               {typecast, fun(M) -> list_to_atom(M) end}]},
     {function, [{shortname, "f"},
                 {longname, "function"},
                 {typecast, fun(F) -> list_to_atom(F) end}]},
     {arity, [{shortname, "a"},
              {longname, "arity"},
              {typecast, fun(A) -> list_to_integer(A) end}]},
     {hook, [{shortname, "h"},
             {longname, "hook"},
             {typecast, fun(H) -> list_to_atom(H) end}]}].
vmq_plugin_enable_cmd() ->
    Cmd = ["vmq-admin", "plugin", "enable"],
    KeySpecs = [],
    FlagSpecs = vmq_plugin_flag_specs(),
    Callback =
    fun([], Flags) ->
            Path =
            case lists:keyfind(path, 1, Flags) of
                false -> auto;
                {_, P} -> P
            end,
            case lists:keyfind(name, 1, Flags) of
                {_, Name} ->
                    case vmq_plugin_mgr:enable_plugin(Name, Path) of
                        ok ->
                            [clique_status:text("Done")];
                        {error, Reason} ->
                            Text = io_lib:format("Can't enable plugin: ~p due to ~p", [Name, Reason]),
                            [clique_status:alert([clique_status:text(Text)])]
                    end;
                false ->
                    Module = lists:keyfind(module, 1, Flags),
                    Function = lists:keyfind(function, 1, Flags),
                    Arity = lists:keyfind(arity, 1, Flags),
                    case lists:member(false, [Module, Function, Arity]) of
                        true ->
                            Text = "Incomplete plugin specification",
                            [clique_status:alert([clique_status:text(Text)])];
                        false ->
                            %% default hook name is the function name
                            HookName = proplists:get_value(hook, Flags, Function),
                            case vmq_plugin_mgr:enable_module_plugin(
                                   HookName, Module, Function, Arity) of
                                ok ->
                                    [clique_status:text("Done")];
                                {error, Reason} ->
                                    Text = io_lib:format("Can't enable module plugin: ~p due to ~p",
                                                         [{Module, Function, Arity}, Reason]),
                                    [clique_status:alert([clique_status:text(Text)])]
                            end
                    end
            end
    end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

vmq_plugin_disable_cmd() ->
    Cmd = ["vmq-admin", "plugin", "disable"],
    KeySpecs = [],
    FlagSpecs = vmq_plugin_flag_specs(),
    Callback =
    fun([], Flags) ->
            case lists:keyfind(name, 1, Flags) of
                {_, Name} ->
                    case vmq_plugin_mgr:disable_plugin(Name) of
                        ok ->
                            [clique_status:text("Done")];
                        {error, Reason} ->
                            Text = io_lib:format("Can't disable plugin: ~p due to ~p", [Name, Reason]),
                            [clique_status:alert([clique_status:text(Text)])]
                    end;
                false ->
                    Module = lists:keyfind(module, 1, Flags),
                    Function = lists:keyfind(function, 1, Flags),
                    Arity = lists:keyfind(arity, 1, Flags),
                    case lists:member(false, [Module, Function, Arity]) of
                        true ->
                            Text = "Incomplete plugin specification",
                            [clique_status:alert([clique_status:text(Text)])];
                        false ->
                            %% default hook name is the function name
                            HookName = proplists:get_value(hook, Flags, Function),
                            case vmq_plugin_mgr:disable_module_plugin(
                                   HookName, Module, Function, Arity) of
                                ok ->
                                    [clique_status:text("Done")];
                                {error, Reason} ->
                                    Text = io_lib:format("Can't disable module plugin: ~p due to ~p",
                                                         [{Module, Function, Arity}, Reason]),
                                    [clique_status:alert([clique_status:text(Text)])]
                            end
                    end
            end
    end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

plugin_usage() ->
    ["vmq-admin plugin <sub-command>\n\n",
     "  Mangage plugins.\n\n",
     "  Sub-commands:\n",
     "    show          show/filter plugin information\n",
     "    enable        enable plugin\n",
     "    disable       disable plugin\n",
     "  Use --help after a sub-command for more details.\n"
    ].

plugin_show_usage() ->
    ["vmq-admin plugin show\n\n",
     "  Shows the currently running plugins.\n\n",
     "Options\n\n",
     "  --plugin\n",
     "      Only shows the hooks for the specified plugin\n",
     "  --hook\n",
     "      Only shows the plugins that provide callbacks for the specified hook\n"
    ].

plugin_enable_usage() ->
    ["vmq-admin plugin enable\n\n",
     "  Enables either an Application plugin or a module plugin. The application\n",
     "  plugins are bundled as Erlang OTP apps. If the application code is not yet\n"
     "  loaded you have to specify the --path=<PathToPlugin>. If the plugin is\n"
     "  implemented in a single Erlang module make sure that the module is loaded.\n\n",
     "Options\n\n",
     "  --name, -n\n",
     "      The name of the plugin application\n",
     "  --path, -p\n",
     "      The path to the plugin application\n",
     "  --module, -m\n",
     "      Name of the module that implements the plugin, not needed if --name is used\n",
     "  --function, -f\n",
     "      Name of the function that implements the hook, not needed if --name is used\n",
     "  --arity, -a\n",
     "      Nr of arguments the function specified in --function takes\n",
     "  --hook, -h\n",
     "      The hook name in case it differs from the function name (--function)\n"
    ].

plugin_disable_usage() ->
    ["vmq-admin plugin disable\n\n",
     "  Disables either an application plugin or a module plugin.\n\n",
     "Options\n\n",
     "  --name, -n\n",
     "      The name of the plugin application\n",
     "  --path, -p\n",
     "      The path to the plugin application\n",
     "  --module, -m\n",
     "      Name of the module that implements the plugin, not needed if --name is used\n",
     "  --function, -f\n",
     "      Name of the function that implements the hook, not needed if --name is used\n",
     "  --arity, -a\n",
     "      Nr of arguments the function specified in --function takes\n",
     "  --hook, -h\n",
     "      The hook name in case it differs from the function name (--function)\n"
    ].

