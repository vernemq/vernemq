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

-module(vmq_plugin_mgr).
-behaviour(gen_server).

%% API
-export([start_link/0,
         stop/0,
         enable_plugin/1,
         enable_plugin/2,
         enable_module_plugin/3,
         enable_module_plugin/4,
         disable_plugin/1,
         disable_module_plugin/3,
         disable_module_plugin/4]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([sample_hook/0,
         sample_hook/1,
         sample_hook/2,
         sample_hook/3,
         other_sample_hook_a/1,
         other_sample_hook_b/1,
         other_sample_hook_c/1,
         other_sample_hook_d/1,
         other_sample_hook_e/1,
         other_sample_hook_f/1,
         other_sample_hook_x/1
        ]).
-endif.

-record(state, {
          ready=false,
          plugin_dir,
          config_file,
          deferred_calls=[]}).

-type plugin() :: atom()
                | {atom(),
                   atom(),
                   atom(),
                   non_neg_integer()}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    %% only call after all application that call INTO a
    %% plugin are stopped...
    gen_server:call(?MODULE, stop, infinity).

-spec enable_plugin(atom()) -> ok | {error, _}.
enable_plugin(Plugin) ->
    enable_plugin(Plugin, []).
-spec enable_plugin(atom(), [string()]) -> ok | {error, _}.
enable_plugin(Plugin, Paths) when is_atom(Plugin) ->
    gen_server:call(?MODULE, {enable_plugin, Plugin, [{paths, Paths}]}, infinity).

-spec enable_module_plugin(atom(), atom(), non_neg_integer()) ->
    ok | {error, _}.
enable_module_plugin(Module, Fun, Arity) ->
    enable_module_plugin(Fun, Module, Fun, Arity).

-spec enable_module_plugin(atom(), atom(), atom(), non_neg_integer()) ->
    ok | {error, _}.
enable_module_plugin(HookName, Module, Fun, Arity) when
      is_atom(HookName) and is_atom(Module)
      and is_atom(Fun) and (Arity >= 0) ->
    gen_server:call(?MODULE, {enable_module_plugin, HookName, Module, Fun, Arity}, infinity).

-spec disable_module_plugin(atom(), atom(), non_neg_integer()) ->
    ok | {error, _}.
disable_module_plugin(Module, Fun, Arity) ->
    disable_module_plugin(Fun, Module, Fun, Arity).

-spec disable_module_plugin(atom(), atom(), atom(), non_neg_integer()) ->
    ok | {error, _}.
disable_module_plugin(HookName, Module, Fun, Arity) when
      is_atom(HookName) and is_atom(Module)
      and is_atom(Fun) and (Arity >= 0) ->
    disable_plugin({HookName, Module, Fun, Arity}).

-spec disable_plugin(plugin()) -> ok | {error, _}.
disable_plugin(Plugin) when is_atom(Plugin) or is_tuple(Plugin) ->
    gen_server:call(?MODULE, {disable_plugin, Plugin}, infinity).
%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    {ok, PluginDir} = application:get_env(vmq_plugin, plugin_dir),
    {ok, ConfigFileName} = application:get_env(vmq_plugin, plugin_config),
    case filelib:ensure_dir(PluginDir) of
        ok ->
            ConfigFile = filename:join(PluginDir, ConfigFileName),
            case init_from_config_file(#state{plugin_dir=PluginDir,
                                         config_file=ConfigFile}) of
                {ok, State} -> {ok, State};
                {error, Reason} -> {stop, Reason}
            end;
        {error, Reason} ->
            {stop, Reason}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(stop, _From, #state{config_file=ConfigFile} = State) ->
    case file:consult(ConfigFile) of
        {ok, [{plugins, Plugins}]} ->
            lists:foreach(
              fun
                  ({application, App, _}) ->
                      catch stop_plugin(App);
                  (_) ->
                      ignore
              end, Plugins);
        _ ->
            ignore
    end,
    {reply, ok, State};
handle_call(Call, _From, #state{ready=true} = State) ->
    handle_plugin_call(Call, State);
handle_call(Call, From, #state{deferred_calls=DeferredCalls} = State) ->
    {noreply, State#state{deferred_calls=[{Call, From}|DeferredCalls]}}.

handle_plugin_call({enable_plugin, Plugin, Path}, State) ->
    case enable_plugin_generic(
           {application, Plugin, Path}, State) of
        {ok, NewState} ->
            {reply, ok, NewState};
        {error, _} = E ->
            {reply, E, State}
    end;
handle_plugin_call({enable_module_plugin, HookName, Module, Fun, Arity}, State) ->
    case enable_plugin_generic(
           {module, {HookName, Module, Fun, Arity}}, State) of
        {ok, NewState} ->
            {reply, ok, NewState};
        {error, _} = E ->
            {reply, E, State}
    end;
handle_plugin_call({disable_plugin, PluginKey}, State) ->
    %% PluginKey is either the Application Name of the Plugin
    %% or {HookName, ModuleName} for Module Plugins
    case disable_plugin_generic(PluginKey, State) of
        {ok, NewState} ->
            case PluginKey of
                {_, _, _, _} -> ignore;
                _ -> stop_plugin(PluginKey)
            end,
            {reply, ok, NewState};
        {error, _} = E ->
            {reply, E, State}
    end.

handle_deferred_calls(#state{deferred_calls=[{Call, From}|Rest]} = State) ->
    {reply, Reply, NewState} = handle_plugin_call(Call, State#state{deferred_calls=Rest}),
    gen_server:reply(From, Reply),
    handle_deferred_calls(NewState);
handle_deferred_calls(#state{deferred_calls=[]} = State) -> State.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(ready, State) ->
    case init_from_config_file(State#state{ready=true}) of
        {ok, NewState} -> {noreply, NewState};
        {error, Reason} -> {stop, Reason}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
enable_plugin_generic(Plugin, #state{config_file=ConfigFile} = State) ->
    case file:consult(ConfigFile) of
        {ok, [{plugins, Plugins}]} ->
            Key = element(2, Plugin),
            NewPlugins =
            case lists:keyfind(Key, 2, Plugins) of
                false ->
                    Plugins ++ [Plugin];
                Plugin ->
                    Plugins;
                _OldInstance ->
                    lists:keyreplace(Key, 2, Plugins, Plugin)
            end,
            update_plugins(NewPlugins, State);
        {error, _} = E ->
            E
    end.

disable_plugin_generic(PluginKey, #state{config_file=ConfigFile} = State) ->
    case file:consult(ConfigFile) of
        {ok, [{plugins, Plugins}]} ->
            case lists:keyfind(PluginKey, 2, Plugins) of
                false ->
                    {error, plugin_not_found};
                _ ->
                    NewPlugins = lists:keydelete(PluginKey, 2, Plugins),
                    update_plugins(NewPlugins, State)
            end;
        {error, _} = E ->
            E
    end.

update_plugins(Plugins, #state{config_file=ConfigFile} = State) ->
    case load_plugins(Plugins, State) of
        {error, Reason} ->
            {error, Reason};
        {ok, NewState} ->
            ok = write_plugin_config(ConfigFile, Plugins),
            {ok, NewState}
    end.

write_plugin_config(ConfigFile, NewPlugins) ->
    NewS = io_lib:format("~p.", [{plugins, NewPlugins}]),
    ok = file:write_file(ConfigFile, NewS).

init_when_ready(MgrPid, RegisteredProcess) ->
    case whereis(RegisteredProcess) of
        undefined ->
            timer:sleep(10),
            init_when_ready(MgrPid, RegisteredProcess);
        _ ->
            MgrPid ! ready
    end.

init_from_config_file(#state{ready=false} = State) ->
    {ok, RegisteredProcess} = application:get_env(vmq_plugin, wait_for_proc),
    %% we start initializing the plugins as soon as
    %% the registered process is alive
    case whereis(RegisteredProcess) of
        undefined ->
            Self = self(),
            Pid = spawn_link(
                    fun() ->
                            init_when_ready(Self, RegisteredProcess)
                    end),
            {ok, State#state{ready={waiting, Pid}}};
        _ ->
            {ok, handle_deferred_calls(State#state{ready=true})}
    end;
init_from_config_file(#state{ready={waiting,_Pid}} = State) ->
    %% we currently wait for the registered process to become alive
    {ok, State};
init_from_config_file(#state{config_file=ConfigFile} = State) ->
    case file:consult(ConfigFile) of
        {ok, [{plugins, Plugins}]} ->
            load_plugins(Plugins, State);
        {ok, _} ->
            {error, incorrect_plugin_config};
        {error, enoent} ->
            ok = write_plugin_config(ConfigFile, []),
            {ok, handle_deferred_calls(State#state{ready=true})};
        {error, Reason} ->
            {error, Reason}
    end.

load_plugins(Plugins, State) ->
    case check_plugins(Plugins, []) of
        {ok, CheckedPlugins} ->
            ok = init_plugins_cli(CheckedPlugins),
            ok = start_plugins(CheckedPlugins),
            ok = compile_hooks(CheckedPlugins),
            {ok, handle_deferred_calls(State#state{ready=true})};
        {error, Reason} ->
            {error, Reason}
    end.

check_plugins([{plugins, Plugins}], Acc) ->
    check_plugins(Plugins, Acc);
check_plugins([{module, ModuleName, Options} = Plugin|Rest], Acc) ->
    case check_module_plugin(ModuleName, Options) of
        {error, Reason} ->
            lager:warning("can't load module plugin \"~p\": ~p", [ModuleName, Reason]),
            check_plugins(Rest, Acc);
         plugin_ok ->
            check_plugins(Rest, [Plugin|Acc])
    end;
check_plugins([{application, App, Options} = Plugin|Rest], Acc) ->
    case check_app_plugin(App, Options) of
        {error, Reason} ->
            lager:warning("can't load application plugin \"~p\": ~p", [App, Reason]),
            check_plugins(Rest, Acc);
        plugin_ok ->
            check_plugins(Rest, [Plugin|Acc])
    end;
check_plugins([], CheckedHooks) ->
    {ok, lists:reverse(CheckedHooks)}.

check_module_plugin(Module, Options) ->
    Hooks = proplists:get_value(hooks, Options, undefined),
    check_module_hooks(Module, Hooks).

check_module_hooks(Module, undefined) ->
    {error, {no_hooks_defined_for_module, Module}};
check_module_hooks(_, []) ->
    plugin_ok;
check_module_hooks(Module, [Hook|Rest]) ->
    case check_module_hook(Module, Hook) of
        {error, Reason} -> {error, Reason};
        ok -> check_module_hooks(Module, Rest)
    end.

check_module_hook(Module, {_HookName, Fun, Arity}) ->
    check_module_hook(Module, {Fun, Arity});
check_module_hook(Module, {Fun, Arity}) ->
    case catch apply(Module, module_info, [exports]) of
        {'EXIT', _} ->
            {error, {unknown_module, Module}};
        Exports ->
            case lists:member({Fun, Arity}, Exports) of
                true ->
                    ok;
                false ->
                    {error, {no_matching_fun_in_module, Module, Fun, Arity}}
            end
    end.

start_plugins([{module, _, _}|Rest]) ->
    start_plugins(Rest);
start_plugins([{application, App, _}|Rest]) ->
    start_plugin(App),
    start_plugins(Rest);
start_plugins([]) -> ok.

start_plugin(App) ->
    case lists:keyfind(App, 1, application:which_applications()) of
        false ->
            case lists:member(App, erlang:loaded()) of
                true ->
                    %% does the App Module specifies a custom
                    %% start/1 function
                    case lists:member({start, 0}, apply(App, module_info, [exports])) of
                        true ->
                            apply(App, start, []);
                        false ->
                            {ok, _} = application:ensure_all_started(App)
                    end;
                false ->
                    {ok, _} = application:ensure_all_started(App)
            end,
            ok;
        _ ->
            ok
    end.

init_plugins_cli(CheckedPlugins) ->
    init_plugins_cli(CheckedPlugins, [code:lib_dir()]).

init_plugins_cli([{module, _, _}|Rest], Acc) ->
    init_plugins_cli(Rest, Acc);
init_plugins_cli([{application, App, _}|Rest], Acc) ->
    case code:priv_dir(App) of
        {error, bad_name} ->
            init_plugins_cli(Rest, Acc);
        PrivDir ->
            init_plugins_cli(Rest, [PrivDir|Acc])
    end;
init_plugins_cli([], Acc) ->
    case clique_config:load_schema(Acc) of
        {error, schema_files_not_found} ->
            lager:debug("couldn't load cuttlefish schema");
        ok ->
            ok
    end.


stop_plugin(vmq_plugin) -> ok;
stop_plugin(App) ->
    case lists:member(App, erlang:loaded()) of
        true ->
            %% does the App Module specifies a custom
            %% stop/1 function
            case lists:member({stop, 0}, apply(App, module_info, [exports])) of
                true ->
                    apply(App, stop, []);
                false ->
                    application:stop(App)
            end;
        false ->
            application:stop(App)
    end,
    ok.

check_app_plugin(App, Options) ->
    AppPaths = proplists:get_value(paths, Options, auto),
    case create_paths(App, AppPaths) of
        [] ->
            lager:debug("can't create path ~p for app ~p~n", [AppPaths, App]),
            {error, cant_create_path};
        Paths ->
            code:add_paths(Paths),
            case application:load(App) of
                ok ->
                    Hooks = application:get_env(App, vmq_plugin_hooks, []),
                    check_app_hooks(App, Hooks);
                {error, {already_loaded, App}} ->
                    Hooks = application:get_env(App, vmq_plugin_hooks, []),
                    check_app_hooks(App, Hooks);
                E ->
                    lager:debug("can't load application ~p", [E]),
                    []
            end
    end.

create_paths(App, auto) ->
    case application:load(App) of
        ok ->
            create_paths(App, [code:lib_dir(App)]);
        {error, {already_loaded, App}} ->
            create_paths(App, [code:lib_dir(App)]);
        _ ->
            []
    end;
create_paths(_, Paths) ->
    lists:flatmap(fun(Path) -> create_paths(Path) end, Paths).

create_paths(Path) ->
    case filelib:is_dir(Path) of
        true ->
            EbinDir = filelib:wildcard(filename:join(Path, "ebin")),
            DepsEbinDir = filelib:wildcard(filename:join(Path, "deps/*/ebin")),
            lists:append(EbinDir, DepsEbinDir);
        false ->
            []
    end.

check_app_hooks(App, [{Module, Fun, Arity}|Rest]) ->
    check_app_hooks(App, [{Fun, Module, Fun, Arity}|Rest]);
check_app_hooks(App, [{_HookName, Module, Fun, Arity}|Rest]) ->
    case check_app_hook(Module, Fun, Arity) of
        hook_ok ->
            check_app_hooks(App, Rest);
        {error, Reason} ->
            lager:debug("can't load specified hook module ~p in app ~p due to ~p",
                        [Module, App, Reason]),
            check_app_hooks(App, Rest)
    end;
check_app_hooks(App, [_|Rest]) ->
    check_app_hooks(App, Rest);
check_app_hooks(_, []) -> plugin_ok.

check_app_hook(Module, Fun, Arity) ->
    case catch apply(Module, module_info, [exports]) of
        Exports when is_list(Exports) ->
            case lists:member({Fun, Arity}, Exports) of
                true ->
                    hook_ok;
                false ->
                    {error, not_exported}
            end;
        {'EXIT', Reason} ->
            {error, Reason}
    end.






extract_hooks(CheckedPlugins) ->
    extract_hooks(CheckedPlugins, []).

extract_hooks([], Acc) ->
    lists:reverse(lists:flatten(Acc));
extract_hooks([{module, Name, Options}|Rest], Acc) ->
    case proplists:get_value(hooks, Options, []) of
        [] -> extract_hooks(Rest, Acc);
        Hooks ->
            extract_hooks(Rest, [extract_module_hooks(Name, Hooks, []) | Acc])
    end;
extract_hooks([{application, Name, Options}|Rest], Acc) ->
    case proplists:get_value(hooks, Options, []) of
        [] ->
            case application:get_env(Name, vmq_plugin_hooks, []) of
                [] -> extract_hooks(Rest, Acc);
                Hooks -> extract_hooks(Rest, [extract_app_env_hooks(Hooks, []) | Acc])
            end;
        Hooks ->
                extract_hooks(Rest, [extract_app_hooks(Hooks, []) | Acc])
    end.

extract_app_hooks([{Mod, Fun, Arity}|Rest], Acc) ->
    extract_app_hooks(Rest, [{Fun, Mod, Fun, Arity}|Acc]);
extract_app_hooks([{_,_,_,_}=Hook|Rest], Acc) ->
    extract_app_hooks(Rest, [Hook|Acc]).

extract_app_env_hooks([{Mod, Fun, Arity}|Rest], Acc) ->
    extract_app_env_hooks(Rest, [{Fun, Mod, Fun, Arity}|Acc]).

extract_module_hooks(_, [], Acc) ->
    Acc;
extract_module_hooks(ModName, [{HookName, Fun, Arity}|Rest], Acc) ->
    extract_module_hooks(ModName, Rest, [{HookName, ModName, Fun, Arity}|Acc]);
extract_module_hooks(ModName, [{Fun, Arity}|Rest], Acc) ->
    extract_module_hooks(ModName, Rest, [{Fun, ModName, Fun, Arity}|Acc]).





compile_hooks(CheckedPlugins) ->
    CheckedHooks = extract_hooks(CheckedPlugins),
    %%{_, CheckedHooks} = lists:unzip(CheckedPlugins),
    compile_hook_module(
      lists:keysort(1, lists:flatten(CheckedHooks))).

compile_hook_module(Hooks) ->
    M1 = smerl:new(vmq_plugin),
    {OnlyClauses, OnlyInfo} = only_clauses(1, Hooks, {nil, nil}, [], []),
    {ok, M2} = smerl:add_func(M1, {function, 1, only, 2, OnlyClauses}),
    {AllClauses, AllInfo} = all_clauses(1, Hooks, [], []),
    {ok, M3} = smerl:add_func(M2, {function, 1, all, 2, AllClauses}),
    AllTillOkClauses = all_till_ok_clauses(1, Hooks, []),
    {ok, M4} = smerl:add_func(M3, {function, 1, all_till_ok, 2, AllTillOkClauses}),
    InfoOnlyClause = info_only_clause(OnlyInfo),
    InfoAllClause = info_all_clause(AllInfo),
    {ok, M5} = smerl:add_func(M4, {function, 1, info, 1, [InfoOnlyClause, InfoAllClause]}),
    smerl:compile(M5).

info_only_clause(Hooks) ->
    {clause, 1,
     [{atom, 1, only}],
     [],
     [list_const(true, Hooks)]
    }.
info_all_clause(Hooks) ->
    {clause, 2,
     [{atom, 1, all}],
     [],
     [list_const(true, Hooks)]
    }.


only_clauses(I, [{Name, _, _, Arity} | Rest], {Name, Arity} = Last, Acc, Info) ->
    %% we already serve this only-clause
    %% see head of Acc
    only_clauses(I, Rest, Last, Acc, Info);
only_clauses(I, [{Name, Module, Fun, Arity} = Hook | Rest], _, Acc, Info) ->
    Clause =
    clause(I, Name, Arity,
           [{call, 1, {atom, 1, apply},
             [{atom, 1, Module},
              {atom, 1, Fun},
              {var, 1, 'Params'}]
            }]),
    only_clauses(I + 1, Rest, {Name, Arity}, [Clause|Acc], [Hook|Info]);
only_clauses(I, [], _, Acc, Info) ->
    {lists:reverse([not_found_clause(I) | Acc]), lists:reverse(Info)}.

not_found_clause(I) ->
    {clause, I,
     [{var, 1, '_'}, {var, 1, '_'}],
     [],
     [{tuple, 1, [{atom, 1, error}, {atom, 1, no_matching_hook_found}]}]
    }.

all_clauses(I, [{Name, _, _, Arity} = Hook |Rest], Acc, Info) ->
    Hooks = [H || {N, _, _, A} = H <- Rest,
                  (N == Name) and (A == Arity)],
    Clause =
    clause(I, Name, Arity,
           [{call, 1, {atom, 1, apply},
             [{atom, 1, vmq_plugin_helper},
              {atom, 1, all},
              {cons, 1,
               list_const(false, [Hook|Hooks]),
               {cons, 1,
                {var, 1, 'Params'},
                {nil, 1}}}]
            }]),
    all_clauses(I + 1, Rest -- Hooks, [Clause|Acc], Info ++ [Hook|Hooks]);
all_clauses(I, [], Acc, Info) ->
    {lists:reverse([not_found_clause(I) | Acc]), Info}.


all_till_ok_clauses(I, [{Name, _, _, Arity} = Hook |Rest], Acc) ->
    Hooks = [H || {N, _, _, A} = H <- Rest,
                  (N == Name) and (A == Arity)],
    Clause =
    clause(I, Name, Arity,
           [{call, 1, {atom, 1, apply},
             [{atom, 1, vmq_plugin_helper},
              {atom, 1, all_till_ok},
              {cons, 1,
               list_const(false, [Hook|Hooks]),
               {cons, 1,
                {var, 1, 'Params'},
                {nil, 1}}}]
            }]),
    all_till_ok_clauses(I + 1, Rest -- Hooks, [Clause|Acc]);
all_till_ok_clauses(I, [], Acc) ->
    lists:reverse([not_found_clause(I) | Acc]).

clause(I, Name, Arity, Body) ->
    {clause, I,
     %% Function Header
     %% all(HookName, Params)
     %% only(HookName, Params)
     [{atom, 1, Name}, {var, 1, 'Params'}],
     %% Function Guard:
     %% all(HookName, Params) when is_list(Params)
     %%                            and (length(Params) == Arity) ->
     %% only(HookName, Params) when is_list(Params)
     %%                            and (length(Params) == Arity) ->
     [[{op, 1, 'and',
        {call, 1, {atom, 1, is_list},
         [{var, 1, 'Params'}]
        },
        {op, 1, '==',
         {call, 1, {atom, 1, length},
          [{var, 1, 'Params'}]
         },
         {integer, 1, Arity}
        }
       }]],
     %% Body
     Body}.

list_const(_, []) -> {nil, 1};
list_const(false, [{_, Module, Fun, _}|Rest]) ->
    {cons, 1,
     {tuple, 1,
      [{atom, 1, Module},
       {atom, 1, Fun}]
     }, list_const(false, Rest)};
list_const(true, [{Name, Module, Fun, Arity}|Rest]) ->
    {cons, 1,
     {tuple, 1,
      [{atom, 1, Name},
       {atom, 1, Module},
       {atom, 1, Fun},
       {integer, 1, Arity}]
     }, list_const(true, Rest)}.

-ifdef(TEST).
%%%===================================================================
%%% Tests
%%%===================================================================
sample_hook() ->
    io:format(user, "called sample_hook()~n", []),
    {sample_hook,0}.

sample_hook(A) ->
    io:format(user, "called sample_hook(~p)~n", [A]),
    {sample_hook,1,A}.

sample_hook(A, B) ->
    io:format(user, "called sample_hook(~p, ~p)~n", [A, B]),
    {sample_hook, 2, A, B}.

sample_hook(A, B, C) ->
    io:format(user, "called sample_hook(~p, ~p, ~p)~n", [A, B, C]),
    {sample_hook, 3, A, B, C}.

other_sample_hook_a(V) ->
    io:format(user, "called other_sample_hook_a(~p)~n", [V]),
    {other_sample_hook_a, 1, V}.

other_sample_hook_b(V) ->
    io:format(user, "called other_sample_hook_b(~p)~n", [V]),
    {other_sample_hook_b, 1, V}.

other_sample_hook_c(V) ->
    io:format(user, "called other_sample_hook_c(~p)~n", [V]),
    {other_sample_hook_c, 1, V}.

other_sample_hook_d(V) ->
    io:format(user, "called other_sample_hook_d(~p)~n", [V]),
    {error, not_ok}.

other_sample_hook_e(V) ->
    io:format(user, "called other_sample_hook_e(~p)~n", [V]),
    {error, not_ok}.

other_sample_hook_f(V) ->
    io:format(user, "called other_sample_hook_f(~p)~n", [V]),
    ok.

other_sample_hook_x(V) ->
    exit({other_sampl_hook_x_called_but_should_not, V}).

vmq_plugin_test() ->
    application:load(vmq_plugin),
    Hooks = [{?MODULE, sample_hook, 0},
             {?MODULE, sample_hook, 1},
             {?MODULE, sample_hook, 2},
             {?MODULE, sample_hook, 3},
             {sample_all_hook, ?MODULE, other_sample_hook_a, 1},
             {sample_all_hook, ?MODULE, other_sample_hook_b, 1},
             {sample_all_hook, ?MODULE, other_sample_hook_c, 1},
             {sample_all_till_ok_hook, ?MODULE, other_sample_hook_d, 1},
             {sample_all_till_ok_hook, ?MODULE, other_sample_hook_e, 1},
             {sample_all_till_ok_hook, ?MODULE, other_sample_hook_f, 1},
             {sample_all_till_ok_hook, ?MODULE, other_sample_hook_x, 1}
            ],
    application:set_env(vmq_plugin, vmq_plugin_hooks, Hooks),
    %% we have to step out .eunit
    application:set_env(vmq_plugin, plugin_dir, ".."),
    {ok, _} = application:ensure_all_started(vmq_plugin),
    %% no plugin is yet registered
    call_no_hooks(),

    %% ENABLE PLUGIN
    ?assertEqual(ok, vmq_plugin_mgr:enable_plugin(vmq_plugin, "..")),
    ?assert(lists:keyfind(vmq_plugin, 1, application:which_applications()) /= false),

    io:format(user, "info all ~p~n", [vmq_plugin:info(all)]),
    io:format(user, "info only ~p~n", [vmq_plugin:info(only)]),

    %% the plugins are sorted (stable) by the plugin name.
    ?assertEqual([{sample_all_hook,vmq_plugin_mgr,other_sample_hook_a,1},
                  {sample_all_hook,vmq_plugin_mgr,other_sample_hook_b,1},
                  {sample_all_hook,vmq_plugin_mgr,other_sample_hook_c,1},
                  {sample_all_till_ok_hook,vmq_plugin_mgr,other_sample_hook_d,1},
                  {sample_all_till_ok_hook,vmq_plugin_mgr,other_sample_hook_e,1},
                  {sample_all_till_ok_hook,vmq_plugin_mgr,other_sample_hook_f,1},
                  {sample_all_till_ok_hook,vmq_plugin_mgr,other_sample_hook_x,1},
                  {sample_hook,vmq_plugin_mgr,sample_hook,0},
                  {sample_hook,vmq_plugin_mgr,sample_hook,1},
                  {sample_hook,vmq_plugin_mgr,sample_hook,2},
                  {sample_hook,vmq_plugin_mgr,sample_hook,3}], vmq_plugin:info(all)),

    %% the plugins are sorted (stable) by the plugin name.
    ?assertEqual([{sample_all_hook,vmq_plugin_mgr,other_sample_hook_a,1},
                  {sample_all_till_ok_hook,vmq_plugin_mgr,other_sample_hook_d,1},
                  {sample_hook,vmq_plugin_mgr,sample_hook,0},
                  {sample_hook,vmq_plugin_mgr,sample_hook,1},
                  {sample_hook,vmq_plugin_mgr,sample_hook,2},
                  {sample_hook,vmq_plugin_mgr,sample_hook,3}], vmq_plugin:info(only)),

    call_hooks(),


    %% Disable Plugin
    ?assertEqual(ok, vmq_plugin_mgr:disable_plugin(vmq_plugin)),
    %% no plugin is registered
    call_no_hooks().

vmq_module_plugin_test() ->
    {ok, _} = application:ensure_all_started(vmq_plugin),
    call_no_hooks(),
    vmq_plugin_mgr:enable_module_plugin(?MODULE, sample_hook, 0),
    vmq_plugin_mgr:enable_module_plugin(?MODULE, sample_hook, 1),
    vmq_plugin_mgr:enable_module_plugin(?MODULE, sample_hook, 2),
    vmq_plugin_mgr:enable_module_plugin(?MODULE, sample_hook, 3),
    vmq_plugin_mgr:enable_module_plugin(sample_all_hook, ?MODULE, other_sample_hook_a, 1),
    vmq_plugin_mgr:enable_module_plugin(sample_all_hook, ?MODULE, other_sample_hook_b, 1),
    vmq_plugin_mgr:enable_module_plugin(sample_all_hook, ?MODULE, other_sample_hook_c, 1),
    %% ordering matters, we don't want other_sample_hook_x to be called
    vmq_plugin_mgr:enable_module_plugin(sample_all_till_ok_hook, ?MODULE, other_sample_hook_d, 1),
    vmq_plugin_mgr:enable_module_plugin(sample_all_till_ok_hook, ?MODULE, other_sample_hook_e, 1),
    vmq_plugin_mgr:enable_module_plugin(sample_all_till_ok_hook, ?MODULE, other_sample_hook_f, 1),
    vmq_plugin_mgr:enable_module_plugin(sample_all_till_ok_hook, ?MODULE, other_sample_hook_x, 1),

    %% the plugins are sorted (stable) by the plugin name.
    ?assertEqual([{sample_all_hook,vmq_plugin_mgr,other_sample_hook_a,1},
                  {sample_all_hook,vmq_plugin_mgr,other_sample_hook_b,1},
                  {sample_all_hook,vmq_plugin_mgr,other_sample_hook_c,1},
                  {sample_all_till_ok_hook,vmq_plugin_mgr,other_sample_hook_d,1},
                  {sample_all_till_ok_hook,vmq_plugin_mgr,other_sample_hook_e,1},
                  {sample_all_till_ok_hook,vmq_plugin_mgr,other_sample_hook_f,1},
                  {sample_all_till_ok_hook,vmq_plugin_mgr,other_sample_hook_x,1},
                  {sample_hook,vmq_plugin_mgr,sample_hook,0},
                  {sample_hook,vmq_plugin_mgr,sample_hook,1},
                  {sample_hook,vmq_plugin_mgr,sample_hook,2},
                  {sample_hook,vmq_plugin_mgr,sample_hook,3}], vmq_plugin:info(all)),

    %% the plugins are sorted (stable) by the plugin name.
    ?assertEqual([{sample_all_hook,vmq_plugin_mgr,other_sample_hook_a,1},
                  {sample_all_till_ok_hook,vmq_plugin_mgr,other_sample_hook_d,1},
                  {sample_hook,vmq_plugin_mgr,sample_hook,0},
                  {sample_hook,vmq_plugin_mgr,sample_hook,1},
                  {sample_hook,vmq_plugin_mgr,sample_hook,2},
                  {sample_hook,vmq_plugin_mgr,sample_hook,3}], vmq_plugin:info(only)),

    call_hooks(),

    % disable hooks
    vmq_plugin_mgr:disable_module_plugin(?MODULE, sample_hook, 0),
    vmq_plugin_mgr:disable_module_plugin(?MODULE, sample_hook, 1),
    vmq_plugin_mgr:disable_module_plugin(?MODULE, sample_hook, 2),
    vmq_plugin_mgr:disable_module_plugin(?MODULE, sample_hook, 3),
    vmq_plugin_mgr:disable_module_plugin(sample_all_hook, ?MODULE, other_sample_hook_a, 1),
    vmq_plugin_mgr:disable_module_plugin(sample_all_hook, ?MODULE, other_sample_hook_b, 1),
    vmq_plugin_mgr:disable_module_plugin(sample_all_hook, ?MODULE, other_sample_hook_c, 1),
    vmq_plugin_mgr:disable_module_plugin(sample_all_till_ok_hook, ?MODULE, other_sample_hook_d, 1),
    vmq_plugin_mgr:disable_module_plugin(sample_all_till_ok_hook, ?MODULE, other_sample_hook_e, 1),
    vmq_plugin_mgr:disable_module_plugin(sample_all_till_ok_hook, ?MODULE, other_sample_hook_f, 1),
    vmq_plugin_mgr:disable_module_plugin(sample_all_till_ok_hook, ?MODULE, other_sample_hook_x, 1),
    call_no_hooks().


call_no_hooks() ->
    ?assertEqual({error, no_matching_hook_found},
                 vmq_plugin:only(sample_hook, [])),
    ?assertEqual({error, no_matching_hook_found},
                 vmq_plugin:only(sample_hook, [1])),
    ?assertEqual({error, no_matching_hook_found},
                 vmq_plugin:only(sample_hook, [1, 2])),
    ?assertEqual({error, no_matching_hook_found},
                 vmq_plugin:only(sample_hook, [1, 2, 3])).


call_hooks() ->
    %% ONLY HOOK Tests
    ?assertEqual({sample_hook, 0}, vmq_plugin:only(sample_hook, [])),
    ?assertEqual({sample_hook, 1, 1}, vmq_plugin:only(sample_hook, [1])),
    ?assertEqual({sample_hook, 2, 1, 2}, vmq_plugin:only(sample_hook, [1, 2])),
    ?assertEqual({sample_hook, 3, 1, 2, 3}, vmq_plugin:only(sample_hook, [1, 2, 3])),

    %% call hook with wrong arity
    ?assertEqual({error, no_matching_hook_found},
                 vmq_plugin:only(sample_hook, [1, 2, 3, 4])),
    %% call unknown hook
    ?assertEqual({error, no_matching_hook_found},
                 vmq_plugin:only(unknown_hook, [])),

    %% ALL HOOK Tests
    ?assertEqual([{sample_hook, 0}], vmq_plugin:all(sample_hook, [])),

    %% call hook with wrong arity
    ?assertEqual({error, no_matching_hook_found},
                 vmq_plugin:all(sample_hook, [1, 2, 3, 4])),

    %% call unknown hook
    ?assertEqual({error, no_matching_hook_found},
                 vmq_plugin:all(unknown_hook, [])),

    %% hook order
    ?assertEqual([{other_sample_hook_a, 1, 10},
                  {other_sample_hook_b, 1, 10},
                  {other_sample_hook_c, 1, 10}], vmq_plugin:all(sample_all_hook, [10])),

    %% ALL_TILL_OK Hook Tests
    ?assertEqual(ok, vmq_plugin:all_till_ok(sample_all_till_ok_hook, [10])).
-endif.
