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

-module(vmq_plugin_cli).
-behaviour(clique_handler).

-include("vmq_plugin.hrl").

-export([register_cli/0]).

register_cli() ->
    vmq_plugin_cli_usage(),
    vmq_plugin_show_cmd(),
    vmq_plugin_enable_cmd(),
    vmq_plugin_disable_cmd(),
    ok.

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
                         {typecast, fun(H) -> list_to_atom(H) end}]},
                 {internal, [{longname, "internal"}]}],
    Callback =
    fun(_, [], Flags) ->
            ShowInternal = proplists:get_value(internal, Flags, false),
            Plugins = extract_table(vmq_plugin:info(raw), ShowInternal),
            PluginName = proplists:get_value(plugin, Flags, []),
            HookName = proplists:get_value(hook, Flags, []),
            FilteredPlugins =
                lists:filtermap(
                  fun({_, module, _}) when not ShowInternal ->
                          false;
                     ({_, _, _}=P) when (PluginName == []) and (HookName == []) ->
                          {true, show_internal_hooks(P, ShowInternal)};
                     ({PN, _,_}=P) when (PluginName == PN) and (HookName == []) ->
                          {true, show_internal_hooks(P, ShowInternal)};
                     ({PN,T,Hooks}) when (PluginName == []) and (HookName =/= []) ->
                          case [H|| {HN,_,_,_,_,_}=H<-Hooks, HN ==  HookName] of
                              [] -> false;
                              Hs -> show_internal_hooks({PN, T, Hs}, ShowInternal)
                          end;
                     ({PN,T,Hooks}) when (PluginName == PN) and (HookName =/= []) ->
                          case [H|| {HN,_,_,_,_,_}=H<-Hooks, HN == HookName] of
                              [] -> false;
                              Hs -> show_internal_hooks({PN, T, Hs}, ShowInternal)
                          end;
                     (_) -> false
                  end, Plugins),
            Table =
            lists:foldl(
              fun({Plugin, Type, Hooks}, Acc) ->
                      new_row(Plugin, Type, Hooks, Acc)
              end, [], FilteredPlugins),
            [clique_status:table(Table)]
    end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

show_internal_hooks({PN,T,Hs}, ShowInternal) ->
    {PN, T, [ Hook || {_,_,_,_,_,Opts}=Hook <-Hs,
                      show_internal_hook(T, Opts, ShowInternal)]}.

show_internal_hook(_, Opts, ShowInternal) ->
    case {ShowInternal, proplists:get_value(internal, Opts, false)} of
        {false, true} -> false;
        _ -> true
    end.

new_row(Plugin, Type, Hooks, Acc) ->
    [[{'Plugin', Plugin}, {'Type',  Type},
      {'Hook(s)', fmt_hooks(Hooks)}, {'M:F/A', fmt_mfas(Hooks)}] | Acc].

fmt_hooks(Hooks) ->
    lists:flatten([io_lib:format("~p~n", [H]) || {H,_,_,_,_,_} <- Hooks]).

fmt_mfas(Hooks) ->
    lists:flatten([io_lib:format("~p:~p/~p~n", [M,F,A]) || {_,M,F,A,_,_} <- Hooks]).

extract_table(Plugins, ShowInternal) ->
    lists:foldl(
      fun({module, Name, Opts}, Acc) ->
              [{Name, module, get_module_hooks(Name, proplists:get_value(hooks, Opts, []))} | Acc];
         ({application, Name, Opts}, Acc) ->
              case {proplists:get_value(internal, Opts, false), ShowInternal} of
                  {true, false} ->
                      %% skip plugins marked internal
                      Acc;
                  _ ->
                      [{Name, application, get_app_hooks(proplists:get_value(hooks, Opts, []))} | Acc]
              end
      end, [], Plugins).

get_module_hooks(Mod, Hooks) ->
    lists:map(fun(#hook{name = N, module = M,
                        function = F, arity =A,
                        compat = C, opts = Opts}) when M =:= Mod ->
                      {N, M, F, A, C, Opts}
              end, Hooks).

get_app_hooks(Hooks) ->
    lists:map(fun(
                #hook{name = N, module = M,
                      function = F, arity =A,
                      compat = C, opts = Opts}) ->
                      {N, M, F, A, C, Opts}
              end, Hooks).

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
    fun(_, [], Flags) ->
            Opts =
            case lists:keyfind(path, 1, Flags) of
                false -> [];
                {_, P} -> [{path, P}]
            end,
            case lists:keyfind(name, 1, Flags) of
                {_, Name} ->
                    case vmq_plugin_mgr:enable_plugin(Name, Opts) of
                        ok ->
                            [clique_status:text("Done")];
                        {error, Reason} ->
                            Text = io_lib:format("Can't enable plugin: ~p due to ~p", [Name, Reason]),
                            [clique_status:alert([clique_status:text(Text)])]
                    end;
                false ->
                    case {proplists:get_value(module, Flags, ""),
                          proplists:get_value(function, Flags, ""),
                          proplists:get_value(arity, Flags, "")} of
                        {M, F, A} when is_atom(M) and is_atom(F) and (A >= 0) ->
                            %% default hook name is the function name
                            HookName = proplists:get_value(hook, Flags, F),
                            case vmq_plugin_mgr:enable_module_plugin(
                                   HookName, M, F, A) of
                                ok ->
                                    [clique_status:text("Done")];
                                {error, Reason} ->
                                    Text = io_lib:format("Can't enable module plugin: ~p due to ~p",
                                                         [{M, F, A}, Reason]),
                                    [clique_status:alert([clique_status:text(Text)])]
                            end;
                        _ ->
                            Text = "Incomplete plugin specification",
                            [clique_status:alert([clique_status:text(Text)])]
                    end
            end
    end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

vmq_plugin_disable_cmd() ->
    Cmd = ["vmq-admin", "plugin", "disable"],
    KeySpecs = [],
    FlagSpecs = vmq_plugin_flag_specs(),
    Callback =
    fun(_, [], Flags) ->
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
                    case {proplists:get_value(module, Flags, ""),
                          proplists:get_value(function, Flags, ""),
                          proplists:get_value(arity, Flags, "")} of
                        {M, F, A} when is_atom(M) and is_atom(F) and (A >= 0) ->
                            %% default hook name is the function name
                            HookName = proplists:get_value(hook, Flags, F),
                            case vmq_plugin_mgr:disable_module_plugin(
                                   HookName, M, F, A) of
                                ok ->
                                    [clique_status:text("Done")];
                                {error, Reason} ->
                                    Text = io_lib:format("Can't disable module plugin: ~p due to ~p",
                                                         [{M, F, A}, Reason]),
                                    [clique_status:alert([clique_status:text(Text)])]
                            end;
                        _ ->
                            Text = "Incomplete plugin specification",
                            [clique_status:alert([clique_status:text(Text)])]
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
     "      Only shows the plugins that provide callbacks for the specified hook\n",
     "  --internal\n",
     "      Also show internal plugins\n"
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

