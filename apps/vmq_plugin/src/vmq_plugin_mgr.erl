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

-module(vmq_plugin_mgr).
-behaviour(gen_server).

-include("vmq_plugin.hrl").

%% API
-export([start_link/0,
         stop/0,
         enable_plugin/1,
         enable_plugin/2,
         enable_module_plugin/3,
         enable_module_plugin/4,
         enable_system_plugin/2,
         enable_module_plugin/5,
         disable_plugin/1,
         disable_module_plugin/3,
         disable_module_plugin/4,
         disable_module_plugin/5,
         get_usage_lead_lines/0]).

%% exported for testing purposes.
-export([get_plugins/0]).

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
         all_till_ok_next_1/1,
         all_till_ok_next_2/1,
         all_till_ok_ok_1/1,
         all_till_ok_error_1/1,
         all_till_ok_throw_1/1
        ]).
-endif.

-record(state, {
          ready :: boolean(),
          deferred_calls=[],
          plugins=[]}).

-type plugin() :: atom()
                | {atom(),
                   atom(),
                   atom(),
                   non_neg_integer()}.

-type raw_hook() :: {atom(), module(), atom(), non_neg_integer()} |
                    {compat,
                     N :: atom(),
                     M :: module(),
                     F :: atom(),
                     A :: non_neg_integer(),
                     CH :: atom(),
                     CM :: module(),
                     CF :: atom(),
                     CA :: non_neg_integer()}.

-type hook() :: #hook{}.

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

-spec enable_system_plugin(atom(), [any()]) -> ok | {error, _}.
enable_system_plugin(Plugin, Opts) ->
    gen_server:call(?MODULE, {enable_system_plugin, Plugin, Opts}, infinity).

-spec enable_plugin(atom()) -> ok | {error, _}.
enable_plugin(Plugin) ->
    enable_plugin(Plugin, []).
-spec enable_plugin(atom(), [any()]) -> ok | {error, _}.
enable_plugin(Plugin, Opts) when is_atom(Plugin) and is_list(Opts) ->
    gen_server:call(?MODULE, {enable_plugin, Plugin, Opts}, infinity).

-spec enable_module_plugin(atom(), atom(), non_neg_integer()) ->
    ok | {error, _}.
enable_module_plugin(Module, Fun, Arity) ->
    enable_module_plugin(Fun, Module, Fun, Arity, []).

-spec enable_module_plugin(atom(), atom(), atom(), non_neg_integer()) ->
    ok | {error, _}.
enable_module_plugin(HookName, Module, Fun, Arity) ->
    enable_module_plugin(HookName, Module, Fun, Arity, []).

-spec enable_module_plugin(atom(), atom(), atom(), non_neg_integer(), [any()]) ->
    ok | {error, _}.
enable_module_plugin(HookName, Module, Fun, Arity, Opts) when
      is_atom(HookName) and is_atom(Module)
      and is_atom(Fun) and (Arity >= 0) ->
    Hook = case lists:keyfind(compat, 1, Opts) of
               {compat, {CH, CM, CF, CA}} ->
                   #hook{name = HookName, module = Module, function = Fun, arity = Arity,
                         compat = {CH, CM, CF, CA}};
               false ->
                   #hook{name = HookName, module = Module, function = Fun, arity = Arity}
           end,
    gen_server:call(?MODULE, {enable_module_plugin, Hook}, infinity).

-spec disable_module_plugin(atom(), atom(), non_neg_integer()) ->
    ok | {error, _}.
disable_module_plugin(Module, Fun, Arity) ->
    disable_module_plugin(Fun, Module, Fun, Arity).

-spec disable_module_plugin(atom(), atom(), atom(), non_neg_integer()) ->
    ok | {error, _}.
disable_module_plugin(HookName, Module, Fun, Arity) ->
    disable_module_plugin(HookName, Module, Fun, Arity, []).

-spec disable_module_plugin(atom(), atom(), atom(), non_neg_integer(), [any()]) ->
    ok | {error, _}.
disable_module_plugin(HookName, Module, Fun, Arity, Opts) when
      is_atom(HookName) and is_atom(Module)
      and is_atom(Fun) and (Arity >= 0) ->
    Hook = case lists:keyfind(compat, 1, Opts) of
               {compat, {CH, CM, CF, CA}} ->
                   #hook{name = HookName, module = Module, function = Fun, arity = Arity,
                         compat = {CH, CM, CF, CA}};
               false ->
                   #hook{name = HookName, module = Module, function = Fun, arity = Arity}
           end,
    gen_server:call(?MODULE, {disable_plugin, Hook}, infinity).

-spec disable_plugin(plugin()) -> ok | {error, _}.
disable_plugin(Plugin) when is_atom(Plugin) ->
    gen_server:call(?MODULE, {disable_plugin, Plugin}, infinity);
disable_plugin({N,M,F,A}) ->
    Hook = #hook{name = N, module = M, function = F, arity = A},
    gen_server:call(?MODULE, {disable_plugin, Hook}, infinity).



get_usage_lead_lines() ->
    gen_server:call(?MODULE, get_usage_lead_lines).

get_plugins() ->
    gen_server:call(?MODULE, get_plugins).

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
    Plugins = application:get_env(vmq_plugin, plugins, []),
    wait_until_ready(#state{plugins = Plugins, ready=false}).

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
handle_call(stop, _From, #state{plugins=Plugins} = State) ->
    NewState =
        lists:foldl(
          fun
              ({application, App, _}, AccState) ->
                  stop_plugin(App, AccState);
              (_, AccState) ->
                  AccState
          end, State, Plugins),
    {reply, ok, NewState};
handle_call(get_plugins, _From, #state{plugins=Plugins} = State) ->
    {reply, {ok, Plugins}, State};
handle_call(get_usage_lead_lines, _From, #state{plugins=Plugins} = State) ->
    LeadLines =
        lists:filtermap(
          fun({application, App, _}) ->
                  case application:get_env(App, clique_lead_line, undefined) of
                      undefined -> false;
                      Line -> {true, Line}
                  end;
             (_) -> false
          end,
          Plugins),
    {reply, {ok, LeadLines}, State};
handle_call({enable_system_plugin, Plugin, Opts}, _From, State) ->
    % system plugins don't have to be deferred
    handle_plugin_call({enable_plugin, Plugin, Opts}, State);
handle_call(Call, _From, #state{ready=true} = State) ->
    handle_plugin_call(Call, State);
handle_call(Call, From, #state{deferred_calls=DeferredCalls} = State) ->
    {noreply, State#state{deferred_calls=[{Call, From}|DeferredCalls]}}.

handle_plugin_call({enable_plugin, Plugin, Opts}, State) ->
    case enable_plugin_generic({application, Plugin, Opts}, State) of
        {ok, NewState} ->
            {reply, ok, NewState};
        {error, _} = E ->
            {reply, E, State}
    end;
handle_plugin_call({enable_module_plugin, #hook{module=Module} = Hook}, State) ->
    case enable_plugin_generic({module, Module, [{hooks, [Hook]}]}, State) of
        {ok, NewState} ->
            {reply, ok, NewState};
        {error, _} = E ->
            {reply, E, State}
    end;
handle_plugin_call({disable_plugin, PluginKey}, State) ->
    %% PluginKey is either the Application Name of the Plugin or
    %% #hook{} record for Module Plugins
    case disable_plugin_generic(PluginKey, State) of
        {ok, NewState} ->
            NewState1 =
                case PluginKey of
                    #hook{} -> NewState;
                    _ -> stop_plugin(PluginKey, NewState)
                end,
            {reply, ok, NewState1};
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
    {ok, NewState} = wait_until_ready(State#state{ready=true}),
    {noreply, NewState};
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
enable_plugin_generic(Plugin, #state{plugins=Plugins} = State) ->
    case get_new_hooks(Plugin, Plugins) of
        none -> update_plugins(Plugins, State);
        {error, _} = E -> E;
        NewPlugin -> update_plugins(Plugins ++ [NewPlugin], State)
    end.

get_new_hooks({module, _Module, [{hooks, [Hook]}]} = NewPlugin, OldPlugins) ->
    case plugins_have_hook(Hook, OldPlugins) of
        false ->
            NewPlugin;
        true ->
            none
    end;
get_new_hooks({application, Name, Opts}, OldPlugins) ->
    HasAppHook = lists:any(fun({application, N, _}) -> Name =:= N;
                              (_) -> false
                           end,
                           OldPlugins),
    case HasAppHook of
        true ->
            %% Currently we do note overwrite application plugins.
            %% They need to be disabled and enabled again.
            {error, already_enabled};
        false -> {application, Name, Opts}
    end.

plugins_have_hook(#hook{name = Name, module = Module, function = Fun, arity = Arity, compat = Compat}, OldPlugins) ->
    lists:any(
      fun(#hook{name = H, module = M,
                function = F, arity = A, compat = C}) ->
              {H,M,F,A, C} =:= {Name, Module, Fun, Arity, Compat}
      end,
      extract_hooks(OldPlugins)).

disable_plugin_generic(PluginKey, #state{plugins=Plugins} = State) ->
    case delete_plugin(PluginKey, Plugins) of
        Plugins -> {error, plugin_not_found};
        NewPlugins -> update_plugins(NewPlugins, State)
    end.

delete_plugin(AppName, Plugins) when is_atom(AppName) ->
    lists:filter(fun({application, N, _}) ->
                         N =/= AppName;
                    (_) -> true
                 end, Plugins);
delete_plugin(#hook{}=Hook, Plugins) ->
    lists:filtermap(
      fun({module, Name, Opts}) ->
              Hooks = proplists:get_value(hooks, Opts, []),
              RemainingHooks = remove_module_hook(Hook, Hooks),
              case RemainingHooks of
                  [] -> false;
                  RemainingHooks ->
                      NewOpts = lists:keyreplace(hooks, 1, Opts, {hooks, RemainingHooks}),
                      {true, {module, Name, NewOpts}}
              end;
         (_) -> true
      end,
      Plugins).

remove_module_hook(#hook{name = Name, module = Module, function = Function, arity = Arity, compat = Compat}, Hooks) ->
    lists:filter(fun(#hook{name = N, module = M, function = F, arity = A, compat = C}) ->
                         {Name, Module, Function, Arity, Compat} =/= {N,M,F,A,C};
                    (_) -> true
                 end,
                 Hooks).

update_plugins(Plugins, State) ->
    case check_updated_plugins(Plugins, State) of
        {error, Reason} ->
            {error, Reason};
        {ok, NewState} ->
            {ok, NewState}
    end.

init_when_ready(MgrPid, RegisteredProcess) ->
    case whereis(RegisteredProcess) of
        undefined ->
            timer:sleep(10),
            init_when_ready(MgrPid, RegisteredProcess);
        _ ->
            MgrPid ! ready
    end.

wait_until_ready(#state{ready=false} = State) ->
    {ok, RegisteredProcess} = application:get_env(vmq_plugin, wait_for_proc),
    %% we start initializing the plugins as soon as
    %% the registered process is alive
    case erlang:whereis(RegisteredProcess) of
        undefined ->
            Self = self(),
            spawn_link(
              fun() ->
                      init_when_ready(Self, RegisteredProcess)
              end),
            {ok, State};
        _ ->
            {ok, handle_deferred_calls(State#state{ready=true})}
    end;
wait_until_ready(#state{ready=true} = State) ->
    {ok, handle_deferred_calls(State)}.

check_updated_plugins(Plugins, State) ->
    case check_plugins(Plugins, []) of
        {ok, CheckedPlugins} ->
            ok = init_plugins_cli(CheckedPlugins),
            ok = start_plugins(CheckedPlugins),
            ok = compile_hooks(CheckedPlugins),
            {ok, State#state{plugins=CheckedPlugins}};
        {error, Reason} ->
            {error, Reason}
    end.

check_plugins([{module, ModuleName, Options} = Plugin|Rest], Acc) ->
    case check_module_plugin(ModuleName, Options) of
        {error, Reason} ->
            lager:warning("can't load module plugin ~p due to ~p", [ModuleName, Reason]),
            {error, Reason};
         plugin_ok ->
            check_plugins(Rest, [Plugin|Acc])
    end;
check_plugins([{application, App, Options}|Rest], Acc) ->
    case check_app_plugin(App, Options) of
        {error, Reason} ->
            lager:warning("can't load application plugin ~p due to ~p", [App, Reason]),
            {error, Reason};
        CheckedPlugin ->
            check_plugins(Rest, [CheckedPlugin|Acc])
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

check_module_hook(Module, #hook{name=_HookName, module = Module, function = Fun, arity = Arity}) ->
    check_mfa(Module, Fun, Arity).

check_mfa(Module, Fun, Arity) ->
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
            case lists:keyfind(App, 1, application:loaded_applications()) of
                false ->
                    application:load(App);
                _ -> ok
            end,
            {ok, Mods} = application:get_key(App, modules),
            case lists:member(App, Mods) of
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
            load_app_modules(App),
            ok;
        _ ->
            ok
    end.

init_plugins_cli(CheckedPlugins) ->
    init_plugins_cli(CheckedPlugins, application:get_env(vmq_plugin, default_schema_dir, [])).

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


stop_plugin(vmq_plugin, State) -> State;
stop_plugin(App, State) ->
    case lists:member(App, erlang:loaded()) of
        true ->
            %% does the App Module specifies a custom
            %% stop/1 function
            case lists:member({stop, 0}, apply(App, module_info, [exports])) of
                true ->
                    catch apply(App, stop, []);
                false ->
                    application:stop(App)
            end;
        false ->
            application:stop(App)
    end,
    NewState = disable_app_module_plugins(App, State),
    purge_app_modules(App),
    application:unload(App),
    NewState.

disable_app_module_plugins(App, State) ->
    HookModules = vmq_plugin:info(all),
    {ok, AppMods} = application:get_key(App, modules),
    lists:foldl(fun({Name,Mod,Fun,Arity}, AccState) ->
                        case lists:member(Mod, AppMods) of
                            true ->
                                HM = #hook{name=Name,module=Mod,function=Fun,arity=Arity},
                                case disable_plugin_generic(HM, AccState) of
                                    {error, _Reason} -> AccState;
                                    {ok, OkState} -> OkState
                                end;
                            _ -> AccState
                        end
                end,
                State,
                HookModules).

check_app_plugin(App, Options) ->
    Path = proplists:get_value(path, Options, undefined),
    case create_paths(App, Path) of
        [] ->
            lager:debug("can't create paths for app ~p (path: ~p)", [App, Path]),
            {error, plugin_not_found};
        Paths ->
            code:add_pathsa(Paths),
            load_application(App, Options)
    end.

load_application(App, Options) ->
    case application:load(App) of
        ok ->
            case find_mod_conflicts(App) of
                [] ->
                    Hooks = application:get_env(App, vmq_plugin_hooks, []),
                    check_app_hooks(App, Hooks, Options);
                [Err|_] ->
                    application:unload(App),
                    {error, {module_conflict, Err}}
            end;
        {error, {already_loaded, App}} ->
            Hooks = application:get_env(App, vmq_plugin_hooks, []),
            check_app_hooks(App, Hooks, Options);
        E ->
            lager:debug("can't load application ~p", [E]),
            []
    end.

find_mod_conflicts(NewApp) ->
    LoadedApps = [A || {A,_,_} <- application:loaded_applications(), A =/= NewApp],
    {ok, NewMods} = application:get_key(NewApp, modules),
    find_mod_conflicts(NewMods, LoadedApps).

find_mod_conflicts(NewMods, LoadedApps) ->
    lists:filtermap(
      fun(App) ->
              {ok, Mods} = application:get_key(App, modules),
              case common_elems(NewMods, Mods) of
                  [] -> false;
                  [E|_] -> {true, {App, E}}
              end
      end, LoadedApps).

common_elems(L1, L2) ->
    S1=sets:from_list(L1), S2=sets:from_list(L2),
    sets:to_list(sets:intersection(S1,S2)).

create_paths(App, undefined) ->
    case application:load(App) of
        ok ->
            create_paths(code:lib_dir(App));
        {error, {already_loaded, App}} ->
            create_paths(code:lib_dir(App));
        _ ->
            []
    end;
create_paths(_, Path) ->
    create_paths(Path).

create_paths(Path) ->
    case filelib:is_dir(Path) of
        true ->
            %% rebar2 directory structure support
            EbinDir = filelib:wildcard(filename:join(Path, "ebin")),
            DepsEbinDir = filelib:wildcard(filename:join(Path, "deps/*/ebin")),
            %% rebar3 directory structure support
            LibEbinDir = filelib:wildcard(filename:join(Path, "lib/*/ebin")),
            lists:append(LibEbinDir,lists:append(EbinDir, DepsEbinDir));
        false ->
            []
    end.

purge_app_modules(App) ->
    {ok, Modules} = application:get_key(App, modules),
    lager:debug("purging modules: ~p", [Modules]),
    [code:purge(M) || M <- Modules].

load_app_modules(App) ->
    {ok, Modules} = application:get_key(App, modules),
    lager:debug("loading modules: ~p", [Modules]),
    [code:load_file(M) || M <- Modules].

check_app_hooks(App, Hooks, Options) ->
    Compat = proplists:get_value(compat, Options, undefined),
    ConvHooks = convert_to_rec(Hooks, Compat),
    case check_app_hooks(App, ConvHooks) of
        hooks_ok ->
            {application, App, [{hooks, ConvHooks}|Options]};
        {error, Reason} ->
            {error, Reason}
    end.

check_app_hooks(App, [#hook{module = Module, function = Fun, arity = Arity, opts = Opts}|Rest])
  when is_list(Opts) ->
    case check_mfa(Module, Fun, Arity) of
        ok ->
            check_app_hooks(App, Rest);
        {error, Reason} ->
            lager:debug("can't load specified hook module ~p in app ~p due to ~p",
                        [Module, App, Reason]),
            {error, Reason}
    end;
check_app_hooks(_, []) -> hooks_ok.

convert_to_rec(Hooks, Compat) ->
    lists:map(
      fun({M,F,A}) ->
              #hook{name = F, module = M, function = F, arity = A, opts = [], compat = Compat};
         ({M,F,A,Opts}) when is_list(Opts) ->
              #hook{name = F, module = M, function = F, arity = A, opts = Opts, compat = Compat};
         ({N,M,F,A}) ->
              #hook{name = N, module = M, function = F, arity = A, opts = [], compat = Compat};
         ({N,M,F,A,Opts}) ->
              #hook{name = N, module = M, function = F, arity = A, opts = Opts, compat = Compat}
      end, Hooks).

-spec extract_hooks([any()]) -> [hook()].
extract_hooks(CheckedPlugins) ->
    extract_hooks(CheckedPlugins, []).

extract_hooks([], Acc) ->
    lists:flatten(lists:reverse(Acc));
extract_hooks([{module, _Name, Options}|Rest], Acc) ->
    case proplists:get_value(hooks, Options, []) of
        [] -> extract_hooks(Rest, Acc);
        Hooks ->
            extract_hooks(Rest, [Hooks|Acc])
    end;
extract_hooks([{application, _Name, Options}|Rest], Acc) ->
    case proplists:get_value(hooks, Options, []) of
        [] -> extract_hooks(Rest, Acc);
        Hooks -> extract_hooks(Rest, [Hooks|Acc])
    end.

compile_hooks(CheckedPlugins) ->
    RawPlugins = extract_hooks(CheckedPlugins),

    Hooks = lists:sort(fun(#hook{name = LName}, #hook{name = RName}) ->
                               LName =< RName
                       end, lists:flatten(RawPlugins)),

    M1 = smerl:new(vmq_plugin),
    {OnlyClauses, OnlyInfo} = only_clauses(1, Hooks, {nil, nil}, [], []),
    {ok, M2} = smerl:add_func(M1, {function, 1, only, 2, OnlyClauses}),
    {AllClauses, AllInfo} = all_clauses(1, Hooks, [], []),
    {ok, M3} = smerl:add_func(M2, {function, 1, all, 2, AllClauses}),
    AllTillOkClauses = all_till_ok_clauses(1, Hooks, []),
    {ok, M4} = smerl:add_func(M3, {function, 1, all_till_ok, 2, AllTillOkClauses}),
    InfoOnlyClause = info_only_clause(OnlyInfo),
    InfoAllClause = info_all_clause(AllInfo),
    InfoRawClause = info_raw_clause(CheckedPlugins),
    {ok, M5} = smerl:add_func(M4, {function, 1, info, 1, [InfoOnlyClause, InfoAllClause, InfoRawClause]}),
    smerl:compile(M5).

info_raw_clause(Hooks) ->
    {clause, 1,
     [{atom, 1, raw}],
     [],
     [erl_parse:abstract(Hooks)]
    }.
info_only_clause(Hooks) ->
    {clause, 1,
     [{atom, 1, only}],
     [],
     [list_const(true, embed_hooks(Hooks))]
    }.
info_all_clause(Hooks) ->
    {clause, 2,
     [{atom, 1, all}],
     [],
     [list_const(true, embed_hooks(Hooks))]
    }.


only_clauses(I, [], _, Acc, Info) ->
    {lists:reverse([not_found_clause(I) | Acc]), lists:reverse(Info)};
only_clauses(I, Hooks, _, Acc, Info) ->
    {Name, Arity, EmbedHooks} = partition_hooks(Hooks),

    %% We only generate code for the first hook to reflect the `only`
    %% semantics. All other hooks of the same name & arity are
    %% ignored.
    [EmbedHook|_] = EmbedHooks,

    Args =
        case EmbedHook of
            #hook{name = OrigName, module = Module, function = Fun,
                  compat = {_CompatName, CompatMod, CompatFun, _CompatArity}} ->
                [{atom, 1, CompatMod},
                 {atom, 1, CompatFun},
                 {cons, 1,
                  {atom, 1, OrigName},
                  {cons, 1,
                   {atom, 1, Module},
                   {cons, 1,
                    {atom, 1, Fun},
                    {cons, 1,
                     {var, 1, 'Params'},
                     {nil, 1}}}}}];
            #hook{module = Module, function = Fun} ->
                [{atom, 1, Module},
                 {atom, 1, Fun},
                 {var, 1, 'Params'}]
        end,
    Clause =
    clause(I, Name, Arity,
           [{call, 1, {atom, 1, apply},
             Args
            }]),
    only_clauses(I + 1, Hooks -- EmbedHooks, {Name, Arity}, [Clause|Acc], [EmbedHook|Info]).

not_found_clause(I) ->
    {clause, I,
     [{var, 1, '_'}, {var, 1, '_'}],
     [],
     [{tuple, 1, [{atom, 1, error}, {atom, 1, no_matching_hook_found}]}]
    }.

all_clauses(I, [_|_] = Hooks, Acc, Info) ->
    {Name, Arity, EmbedHooks} = partition_hooks(Hooks),
    Clause =
    clause(I, Name, Arity,
           [{call, 1, {atom, 1, apply},
             [{atom, 1, vmq_plugin_helper},
              {atom, 1, all},
              {cons, 1,
               list_const(false, embed_hooks(EmbedHooks)),
               {cons, 1,
                {var, 1, 'Params'},
                {nil, 1}}}]
            }]),
    all_clauses(I + 1, Hooks -- EmbedHooks, [Clause|Acc], Info ++ EmbedHooks);
all_clauses(I, [], Acc, Info) ->
    {lists:reverse([not_found_clause(I) | Acc]), Info}.

all_till_ok_clauses(I, [_|_] = Hooks, Acc) ->
    {Name, Arity, EmbedHooks} = partition_hooks(Hooks),
    Clause =
    clause(I, Name, Arity,
           [{call, 1, {atom, 1, apply},
             [{atom, 1, vmq_plugin_helper},
              {atom, 1, all_till_ok},
              {cons, 1,
               list_const(false, embed_hooks(EmbedHooks)),
               {cons, 1,
                {var, 1, 'Params'},
                {nil, 1}}}]
            }]),
    all_till_ok_clauses(I + 1, Hooks -- EmbedHooks, [Clause|Acc]);
all_till_ok_clauses(I, [], Acc) ->
    lists:reverse([not_found_clause(I) | Acc]).

partition_hooks([#hook{compat = {Name, _, _, Arity}}|_] = Hooks) ->
    {Name, Arity, [H || #hook{compat = {N, _, _, A}} = H <- Hooks,
                        (N == Name) and (A == Arity)]};
partition_hooks([#hook{name = Name, arity = Arity}|_] = Hooks) ->
    {Name, Arity, [H || #hook{name = N, arity = A,
                              compat = undefined} = H <- Hooks,
                        (N == Name) and (A == Arity)]}.

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
list_const(false, [{compat, N, M, F, _A, _CH, CM, CF, _CA}|Rest]) ->
    {cons, 1,
     {tuple, 1,
      [{atom, 1, compat},
       {atom, 1, N},
       {atom, 1, CM},
       {atom, 1, CF},
       {atom, 1, M},
       {atom, 1, F}]
     }, list_const(false, Rest)};
list_const(false, [{_, Module, Fun, _}|Rest]) ->
    {cons, 1,
     {tuple, 1,
      [{atom, 1, Module},
       {atom, 1, Fun}]
     }, list_const(false, Rest)};
list_const(true, [{compat, _N, _M, _F, _A, CH, CM, CF, CA}|Rest]) ->
    {cons, 1,
     {tuple, 1,
      [{atom, 1, CH},
       {atom, 1, CM},
       {atom, 1, CF},
       {integer, 1, CA}]
     }, list_const(true, Rest)};
list_const(true, [{Name, Module, Fun, Arity}|Rest]) ->
    {cons, 1,
     {tuple, 1,
      [{atom, 1, Name},
       {atom, 1, Module},
       {atom, 1, Fun},
       {integer, 1, Arity}]
     }, list_const(true, Rest)}.

-spec embed_hooks([hook()]) -> [raw_hook()].
embed_hooks(Hooks) ->
    lists:map(
      fun(#hook{name = N, module = M, function = F, arity = A,
                compat = {CH, CM, CF, CA}})
            when CM =/= undefined,
                 CF =/= undefined ->
              {compat, N, M, F, A, CH, CM, CF, CA};
         (#hook{name = N, module = M, function = F, arity = A}) ->
              {N, M, F, A}
      end, Hooks).

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

all_till_ok_next_1(V) ->
    io:format(user, "called all_till_next_1(~p)~n", [V]),
    next.

all_till_ok_next_2(V) ->
    io:format(user, "called all_till_next_2(~p)~n", [V]),
    next.

all_till_ok_ok_1(V) ->
    io:format(user, "called all_till_ok_ok_1(~p)~n", [V]),
    ok.

all_till_ok_error_1(V) ->
    io:format(user, "called all_till_ok_ok_1(~p)~n", [V]),
    error.

%% Is never called as Ok is returned
all_till_ok_throw_1(V) ->
    exit({all_til_ok_throw_1_should_never_be_called, V}).


check_plugin_for_app_plugins_test() ->
    Hooks = [{?MODULE, sample_hook, 0, []},
             {?MODULE, sample_hook, 1, []},
             {?MODULE, sample_hook, 2, []},
             {?MODULE, sample_hook, 3, []},
             {sample_all_hook, ?MODULE, other_sample_hook_a, 1, []},
             {sample_all_hook, ?MODULE, other_sample_hook_b, 1, []},
             {sample_all_hook, ?MODULE, other_sample_hook_c, 1, []},
             {sample_all_till_ok_ok_hook, ?MODULE, all_till_ok_next_1, 1, []},
             {sample_all_till_ok_ok_hook, ?MODULE, all_till_ok_next_2, 1, []},
             {sample_all_till_ok_ok_hook, ?MODULE, all_till_ok_ok_1, 1, []},
             {sample_all_till_ok_ok_hook, ?MODULE, all_till_ok_throw_1, 1, []},
             {sample_all_till_ok_error_hook, ?MODULE, all_till_ok_next_1, 1, []},
             {sample_all_till_ok_error_hook, ?MODULE, all_till_ok_next_2, 1, []},
             {sample_all_till_ok_error_hook, ?MODULE, all_till_ok_error_1, 1, []},
             {sample_all_till_ok_error_hook, ?MODULE, all_till_ok_throw_1, 1, []}
            ],
    application:load(vmq_plugin),
    application:set_env(vmq_plugin, vmq_plugin_hooks, Hooks),
    {ok, CheckedPlugins} = check_plugins([{application, vmq_plugin, []}], []),
    ?assertEqual([{application,vmq_plugin,
                  [{hooks,
                    [#hook{name=sample_hook, module=vmq_plugin_mgr, function=sample_hook, arity=0},
                     #hook{name=sample_hook, module=vmq_plugin_mgr, function=sample_hook, arity=1},
                     #hook{name=sample_hook, module=vmq_plugin_mgr, function=sample_hook, arity=2},
                     #hook{name=sample_hook, module=vmq_plugin_mgr, function=sample_hook, arity=3},

                     #hook{name=sample_all_hook, module=vmq_plugin_mgr, function=other_sample_hook_a, arity=1},
                     #hook{name=sample_all_hook, module=vmq_plugin_mgr, function=other_sample_hook_b, arity=1},
                     #hook{name=sample_all_hook, module=vmq_plugin_mgr, function=other_sample_hook_c, arity=1},

                     #hook{name=sample_all_till_ok_ok_hook, module=vmq_plugin_mgr, function=all_till_ok_next_1, arity=1},
                     #hook{name=sample_all_till_ok_ok_hook, module=vmq_plugin_mgr, function=all_till_ok_next_2, arity=1},
                     #hook{name=sample_all_till_ok_ok_hook, module=vmq_plugin_mgr, function=all_till_ok_ok_1, arity=1},
                     #hook{name=sample_all_till_ok_ok_hook, module=vmq_plugin_mgr, function=all_till_ok_throw_1, arity=1},

                     #hook{name=sample_all_till_ok_error_hook, module=vmq_plugin_mgr, function=all_till_ok_next_1, arity=1},
                     #hook{name=sample_all_till_ok_error_hook, module=vmq_plugin_mgr, function=all_till_ok_next_2, arity=1},
                     #hook{name=sample_all_till_ok_error_hook, module=vmq_plugin_mgr, function=all_till_ok_error_1, arity=1},
                     #hook{name=sample_all_till_ok_error_hook, module=vmq_plugin_mgr, function=all_till_ok_throw_1, arity=1}
                    ]}]}],
                 CheckedPlugins),
    application:unload(vmq_plugin).

check_plugin_for_module_plugin_test() ->
    Plugins = [{module, ?MODULE, [{hooks, [#hook{name=sample_hook, module=?MODULE, function=sample_hook, arity=0},
                                           #hook{name=sample_hook, module=?MODULE, function=sample_hook, arity=1}]}]}],
    {ok, CheckedPlugins} = check_plugins(Plugins, []),
    ?assertEqual([{module, ?MODULE,
                   [{hooks, [#hook{name=sample_hook, module=?MODULE, function=sample_hook, arity=0},
                             #hook{name=sample_hook, module=?MODULE, function=sample_hook, arity=1}]}]}],
                 CheckedPlugins).

vmq_plugin_test() ->
    application:load(vmq_plugin),
    Hooks = [{?MODULE, sample_hook, 0, []},
             {?MODULE, sample_hook, 1, []},
             {?MODULE, sample_hook, 2, []},
             {?MODULE, sample_hook, 3, []},
             {sample_all_hook, ?MODULE, other_sample_hook_a, 1, []},
             {sample_all_hook, ?MODULE, other_sample_hook_b, 1, []},
             {sample_all_hook, ?MODULE, other_sample_hook_c, 1, []},
             {sample_all_till_ok_ok_hook, ?MODULE, all_till_ok_next_1, 1, []},
             {sample_all_till_ok_ok_hook, ?MODULE, all_till_ok_next_2, 1, []},
             {sample_all_till_ok_ok_hook, ?MODULE, all_till_ok_ok_1, 1, []},
             {sample_all_till_ok_ok_hook, ?MODULE, all_till_ok_throw_1, 1, []},
             {sample_all_till_ok_error_hook, ?MODULE, all_till_ok_next_1, 1, []},
             {sample_all_till_ok_error_hook, ?MODULE, all_till_ok_next_2, 1, []},
             {sample_all_till_ok_error_hook, ?MODULE, all_till_ok_error_1, 1, []},
             {sample_all_till_ok_error_hook, ?MODULE, all_till_ok_throw_1, 1, []}
            ],
    application:set_env(vmq_plugin, vmq_plugin_hooks, Hooks),
    %% we have to step out .eunit
    application:set_env(vmq_plugin, plugin_dir, "apps/vmq_plugin"),
    {ok, _} = application:ensure_all_started(vmq_plugin),
    %% no plugin is yet registered
    call_no_hooks(),

    %% ENABLE PLUGIN
    ?assertEqual(ok, vmq_plugin_mgr:enable_plugin(
                       vmq_plugin,
                       [code:lib_dir(vmq_plugin)])),
    ?assert(lists:keyfind(vmq_plugin, 1, application:which_applications()) /= false),

    io:format(user, "info all ~p~n", [vmq_plugin:info(all)]),
    io:format(user, "info only ~p~n", [vmq_plugin:info(only)]),

    %% the plugins are sorted (stable) by the plugin name.
    ?assertEqual([{sample_all_hook,vmq_plugin_mgr,other_sample_hook_a,1},
                  {sample_all_hook,vmq_plugin_mgr,other_sample_hook_b,1},
                  {sample_all_hook,vmq_plugin_mgr,other_sample_hook_c,1},
                  {sample_all_till_ok_error_hook,vmq_plugin_mgr,all_till_ok_next_1,1},
                  {sample_all_till_ok_error_hook,vmq_plugin_mgr,all_till_ok_next_2,1},
                  {sample_all_till_ok_error_hook,vmq_plugin_mgr,all_till_ok_error_1,1},
                  {sample_all_till_ok_error_hook,vmq_plugin_mgr,all_till_ok_throw_1,1},
                  {sample_all_till_ok_ok_hook,vmq_plugin_mgr,all_till_ok_next_1,1},
                  {sample_all_till_ok_ok_hook,vmq_plugin_mgr,all_till_ok_next_2,1},
                  {sample_all_till_ok_ok_hook,vmq_plugin_mgr,all_till_ok_ok_1,1},
                  {sample_all_till_ok_ok_hook,vmq_plugin_mgr,all_till_ok_throw_1,1},
                  {sample_hook,vmq_plugin_mgr,sample_hook,0},
                  {sample_hook,vmq_plugin_mgr,sample_hook,1},
                  {sample_hook,vmq_plugin_mgr,sample_hook,2},
                  {sample_hook,vmq_plugin_mgr,sample_hook,3}], vmq_plugin:info(all)),

    %% the plugins are sorted (stable) by the plugin name.
    ?assertEqual([{sample_all_hook,vmq_plugin_mgr,other_sample_hook_a,1},
                  {sample_all_till_ok_error_hook,vmq_plugin_mgr,all_till_ok_next_1,1},
                  {sample_all_till_ok_ok_hook,vmq_plugin_mgr,all_till_ok_next_1,1},
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
    application:load(vmq_plugin),
    application:set_env(vmq_plugin, plugin_dir, ".."),

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
    vmq_plugin_mgr:enable_module_plugin(sample_all_till_ok_ok_hook, ?MODULE, all_till_ok_next_1, 1),
    vmq_plugin_mgr:enable_module_plugin(sample_all_till_ok_ok_hook, ?MODULE, all_till_ok_next_2, 1),
    vmq_plugin_mgr:enable_module_plugin(sample_all_till_ok_ok_hook, ?MODULE, all_till_ok_ok_1, 1),
    vmq_plugin_mgr:enable_module_plugin(sample_all_till_ok_ok_hook, ?MODULE, all_till_ok_throw_1, 1),

    vmq_plugin_mgr:enable_module_plugin(sample_all_till_ok_error_hook, ?MODULE, all_till_ok_next_1, 1),
    vmq_plugin_mgr:enable_module_plugin(sample_all_till_ok_error_hook, ?MODULE, all_till_ok_next_2, 1),
    vmq_plugin_mgr:enable_module_plugin(sample_all_till_ok_error_hook, ?MODULE, all_till_ok_error_1, 1),
    vmq_plugin_mgr:enable_module_plugin(sample_all_till_ok_error_hook, ?MODULE, all_till_ok_throw_1, 1),

    %% the plugins are sorted (stable) by the plugin name.
    ?assertEqual([{sample_all_hook,vmq_plugin_mgr,other_sample_hook_a,1},
                  {sample_all_hook,vmq_plugin_mgr,other_sample_hook_b,1},
                  {sample_all_hook,vmq_plugin_mgr,other_sample_hook_c,1},
                  {sample_all_till_ok_error_hook,vmq_plugin_mgr,all_till_ok_next_1,1},
                  {sample_all_till_ok_error_hook,vmq_plugin_mgr,all_till_ok_next_2,1},
                  {sample_all_till_ok_error_hook,vmq_plugin_mgr,all_till_ok_error_1,1},
                  {sample_all_till_ok_error_hook,vmq_plugin_mgr,all_till_ok_throw_1,1},
                  {sample_all_till_ok_ok_hook,vmq_plugin_mgr,all_till_ok_next_1,1},
                  {sample_all_till_ok_ok_hook,vmq_plugin_mgr,all_till_ok_next_2,1},
                  {sample_all_till_ok_ok_hook,vmq_plugin_mgr,all_till_ok_ok_1,1},
                  {sample_all_till_ok_ok_hook,vmq_plugin_mgr,all_till_ok_throw_1,1},
                  {sample_hook,vmq_plugin_mgr,sample_hook,0},
                  {sample_hook,vmq_plugin_mgr,sample_hook,1},
                  {sample_hook,vmq_plugin_mgr,sample_hook,2},
                  {sample_hook,vmq_plugin_mgr,sample_hook,3}], vmq_plugin:info(all)),

    %% the plugins are sorted (stable) by the plugin name.
    ?assertEqual([{sample_all_hook,vmq_plugin_mgr,other_sample_hook_a,1},
                  {sample_all_till_ok_error_hook,vmq_plugin_mgr,all_till_ok_next_1,1},
                  {sample_all_till_ok_ok_hook,vmq_plugin_mgr,all_till_ok_next_1,1},
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
    vmq_plugin_mgr:disable_module_plugin(sample_all_till_ok_ok_hook, ?MODULE, all_till_ok_next_1, 1),
    vmq_plugin_mgr:disable_module_plugin(sample_all_till_ok_ok_hook, ?MODULE, all_till_ok_next_2, 1),
    vmq_plugin_mgr:disable_module_plugin(sample_all_till_ok_ok_hook, ?MODULE, all_till_ok_ok_1, 1),
    vmq_plugin_mgr:disable_module_plugin(sample_all_till_ok_ok_hook, ?MODULE, all_till_ok_trow_1, 1),
    vmq_plugin_mgr:disable_module_plugin(sample_all_till_ok_error_hook, ?MODULE, all_till_ok_next_1, 1),
    vmq_plugin_mgr:disable_module_plugin(sample_all_till_ok_error_hook, ?MODULE, all_till_ok_next_2, 1),
    vmq_plugin_mgr:disable_module_plugin(sample_all_till_ok_error_hook, ?MODULE, all_till_ok_error_1, 1),
    vmq_plugin_mgr:disable_module_plugin(sample_all_till_ok_error_hook, ?MODULE, all_till_ok_trow_1, 1),
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
    ?assertEqual(ok, vmq_plugin:all_till_ok(sample_all_till_ok_ok_hook, [10])),
    ?assertEqual({error,error}, vmq_plugin:all_till_ok(sample_all_till_ok_error_hook, [10])).
-endif.
