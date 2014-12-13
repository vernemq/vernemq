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

-module(vmq_config).

-behaviour(gen_server).

%% API
-export([start_link/0,
         change_config/3,
         configure_node/1,
         configure_node/2,
         table_defs/0,
         reset/0,
         get_env/1,
         get_env/2,
         get_env/3,
         set_env/2,
         set_env/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(vmq_config, {key, % {node(), app_name(), item_name()}
                     val,
                     short_descr,
                     long_descr,
                     vclock=unsplit_vclock:fresh()}).

-define(TABLE, ?MODULE).



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


configure_node(ChangeType) when
      (ChangeType == mnesia) or (ChangeType == env) ->
    case mnesia:transaction(fun() -> configure_apps_tx(ChangeType) end) of
        {atomic, {ChangeRef, AppChanges}} ->
            all_hook(change_config, [done, ChangeRef, AppChanges]),
            ok;
        {aborted, {config_change, {Step, ChangeRef, AppChanges}}} ->
            lager:error("cant configure node at ~p: ~p", [Step, AppChanges]),
            all_hook(change_config, [rollback, ChangeRef, AppChanges])
    end.

configure_node(Node, ChangeType) ->
    rpc:call(Node, ?MODULE, configure_node, [ChangeType], infinity).

configure_apps_tx(ChangeType) ->
    LoadedApplications = application:loaded_applications(),
    AppChanges = configure_apps(ChangeType, LoadedApplications, []),
    ChangeRef = make_ref(),
    case all_hook(change_config, [prepare, ChangeRef, AppChanges]) of
        ok ->
            case all_hook(change_config, [install, ChangeRef, AppChanges]) of
                ok ->
                    {ChangeRef, AppChanges};
                Error ->
                    lager:error("can't install configuration changes ~p", Error),
                    mnesia:abort({config_change, {install, ChangeRef, AppChanges}})
            end;
        {error, Reason} ->
            lager:error("can't prepare configuration changes ~p", [Reason]),
            mnesia:abort({config_change, {prepare, ChangeRef, AppChanges}})
    end.

all_hook(Hook, Args) ->
    case vmq_plugin:all(Hook, Args) of
        {error, no_matching_hook_found} ->
            lager:info("no matching hook found ~p ~p", [Hook, Args]),
            ok;
        Res ->
            case [R || R <- Res, R /= ok] of
                [] -> ok;
                Reason -> Reason
            end
    end.

configure_apps(ChangeType, [Application|LoadedApplications], Acc) ->
    App = element(1, Application),
    case application:get_env(App, vmq_config_enabled, false) of
        true ->
            Env = application:get_all_env(App),
            case init_config_items(ChangeType,  node(), App, Env, []) of
                [] ->
                    configure_apps(ChangeType, LoadedApplications, Acc);
                ChangedConfig ->
                    configure_apps(ChangeType, LoadedApplications,
                                   [{App, ChangedConfig}|Acc])
            end;
        false ->
            configure_apps(ChangeType, LoadedApplications, Acc)
    end;
configure_apps(_, [], Acc) -> Acc.

-spec table_defs() -> [{atom(), [{atom(), any()}]}].
table_defs() ->
    [
     {vmq_config,
      [
       {record_name, vmq_config},
       {attributes, record_info(fields, vmq_config)},
       {disc_copies, [node()]},
       {match, #vmq_config{_='_'}},
       {user_properties,
        [{unsplit_method, {unsplit_lib, vclock, [#vmq_config.vclock]}}]}
      ]}
    ].

reset() ->
    mnesia:clear_table(vmq_config).

get_env(Key) ->
    get_env(vmq_server, Key, undefined).

get_env(Key, Default) ->
    get_env(vmq_server, Key, Default).

get_env(Application, Key, Default) ->
    try mnesia:dirty_read(vmq_config, {node(), Application, Key}) of
        [#vmq_config{val=undefined}] -> Default;
        [#vmq_config{val=Val}] -> Val;
        [] ->
            AppEnvVal = application:get_env(vmq_server, Key, undefined),
            case AppEnvVal of
                undefined ->
                    Default;
                _ ->
                    AppEnvVal
            end
    catch
        _:_ ->
            %% in between gen_server restart
            application:get_env(vmq_server, Key, Default)
    end.

set_env(Key, Val) ->
    set_env(vmq_server, Key, Val).

set_env(App, Key, Val) ->
    KKey = {node(), App, Key},
    Config =
    case mnesia:dirty_read(vmq_configApp, KKey) of
        [#vmq_config{} = Conf] ->
            Conf#vmq_config{val=Val};
        [] ->
            #vmq_config{key=Key, val=Val}
    end,
    {atomic, ok} = mnesia:transaction(
                     fun() ->
                             write_config(Config#vmq_config{val=Val})
                     end),
    ok.


%%% VMQ_SERVER CONFIG HOOK
change_config(Step, ChangerRef, AppChanges) ->
    case lists:keyfind(vmq_server, 1, AppChanges) of
        false ->
            %% we don't care about this change
            ok;
        {_, Changes} ->
            change_my_config(Step, ChangerRef, Changes)
    end.

change_my_config(prepare, ChangeRef, Changes) ->
    SessionConfig = extract_session_config(Changes, []),
    ExpirerConfig = extract_expirer_config(Changes, []),
    ListenerConfig = extract_listener_config(Changes, []),
    gen_server:call(?MODULE, {prepare, ChangeRef,
                              lists:flatten([SessionConfig, ExpirerConfig,
                                             ListenerConfig])}, infinity);
change_my_config(install, ChangeRef, _) ->
    gen_server:call(?MODULE, {install, ChangeRef}, infinity);

change_my_config(rollback, ChangeRef, _) ->
    gen_server:call(?MODULE, {rollback, ChangeRef});

change_my_config(done, ChangeRef, _) ->
    gen_server:call(?MODULE, {done, ChangeRef}).


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
    vmq_plugin_mgr:enable_module_plugin(?MODULE, change_config, 3),
    {ok, waiting}.

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
handle_call({prepare, ChangeRef, Configs}, _From, waiting) ->
    {reply, ok, {prepared, ChangeRef, Configs}};
handle_call({install, ChangeRef}, _From, {prepared, ChangeRef, Configs}) ->
    case install_changes(Configs) of
        {ok, RollbackPlan} ->
            {reply, ok, {waiting_for_done, ChangeRef, RollbackPlan}};
        {error, Reason} ->
            {reply, {error, {abort_install, Reason}}, waiting}
    end;
handle_call({done, ChangeRef}, _From, {waiting_for_done, ChangeRef, _}) ->
    {reply, ok, waiting};
handle_call({rollback, ChangeRef}, _From, {prepared, ChangeRef, _}) ->
    {reply, ok, waiting};
handle_call({rollback, ChangeRef}, _From, {waiting_for_done, ChangeRef,
                                           RollbackPlan}) ->
    rollback_changes(RollbackPlan),
    {reply, ok, waiting};
handle_call({rollback, _}, _From, waiting) ->
    %% we'll receive this message if we caused the rollback, so
    %% we are already in waiting state and have nothing to rollback.
    {reply, ok, waiting};
handle_call(Msg, _From, State) ->
    {reply, {error, {unknown_msg, Msg, State}}, waiting}.

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
install_changes(Changes) ->
    try install_changes(Changes, [])
    catch
        Error:Reason ->
            lager:error("install changes ~p", [erlang:get_stacktrace()]),
            {error, {Error, Reason}}
    end.

rollback_changes(OldChanges) ->
    install_changes(OldChanges),
    ok.

install_changes([{vmq_session, SessionConfigs}|Rest], Acc) ->
    vmq_session_sup:reconfigure_sessions(SessionConfigs),
    install_changes(Rest, [{vmq_session, old_config(SessionConfigs)}|Acc]);
install_changes([{vmq_expirer, ExpirerConfig}|Rest], Acc) ->
    [{persistent_client_expiration, Duration}] = ExpirerConfig,
    vmq_session_expirer:change_duration(Duration),
    install_changes(Rest, [{vmq_expirer, old_config(ExpirerConfig)}|Acc]);
install_changes([{vmq_listener, ListenerConfig}|Rest], Acc) ->
    vmq_listener_sup:reconfigure_listeners(ListenerConfig),
    install_changes(Rest, [{vmq_listener, old_config(ListenerConfig)}|Acc]);
install_changes([], Acc) -> {ok, Acc}.


extract_session_config([{allow_anonymous, Val} = Item|Rest], Acc)
  when is_boolean(Val) ->
    extract_session_config(Rest, [Item|Acc]);
extract_session_config([{max_client_id_size, Val} = Item|Rest], Acc)
  when is_integer(Val) and Val >= 0 ->
    extract_session_config(Rest, [Item|Acc]);
extract_session_config([{retry_interval, Val} = Item|Rest], Acc)
  when is_integer(Val) and Val > 0 ->
    extract_session_config(Rest, [Item|Acc]);
extract_session_config([{max_inflight_messages, Val} = Item|Rest], Acc)
  when is_integer(Val) and Val >= 0 ->
    extract_session_config(Rest, [Item|Acc]);
extract_session_config([{message_size_limit, Val} = Item|Rest], Acc)
  when is_integer(Val) and Val >= 0 ->
    extract_session_config(Rest, [Item|Acc]);
extract_session_config([{upgrade_outgoing_qos, Val} = Item|Rest], Acc)
  when is_boolean(Val) ->
    extract_session_config(Rest, [Item|Acc]);
extract_session_config([_|Rest], Acc) ->
    extract_session_config(Rest, Acc);
extract_session_config([], []) -> [];
extract_session_config([], Acc) -> [{vmq_session, Acc}].

extract_expirer_config([{persistent_client_expiration, Val} = Item|Rest], Acc)
  when is_integer(Val) and Val >= 0 ->
    extract_expirer_config(Rest, [Item|Acc]);
extract_expirer_config([_|Rest], Acc) ->
    extract_expirer_config(Rest, Acc);
extract_expirer_config([], []) -> [];
extract_expirer_config([], Acc) -> [{vmq_expirer, Acc}].

extract_listener_config([{listeners, _} = Item|Rest], Acc) ->
    extract_listener_config(Rest, [Item|Acc]);
extract_listener_config([{tcp_listen_options, _} = Item|Rest], Acc) ->
    extract_listener_config(Rest, [Item|Acc]);
extract_listener_config([_|Rest], Acc) ->
    extract_listener_config(Rest, Acc);
extract_listener_config([], Acc) -> [{vmq_listener, Acc}].

old_config(NewConfigs) ->
    %% vmq_config:get_env will do a mnesia:dirty_read,
    %% and the transaction is not finished yet, so we'll
    %% get the old value.
   [{Key, get_env(Key)} || {Key, _} <- NewConfigs].

init_config_items(ChangeType, Node, Application, [{Key, Value} = Item|Env], Acc) ->
    ConfigKey = {Node, Application, Key},
    case read_config(ConfigKey) of
        {error, not_found} ->
            write_config(#vmq_config{key=ConfigKey,
                                     val=Value}),
            init_config_items(ChangeType, Node, Application, Env, [Item|Acc]);
        {ok, #vmq_config{val=Value}} ->
            %% value from sys.config (or app.src) is the same as in stored
            %% we can ignore
            init_config_items(ChangeType, Node, Application, Env, Acc);
        {ok, #vmq_config{val=OtherVal}} when ChangeType == mnesia ->
            %% value from Mnesia is different than from sys.config (or app.src)
            %% lets update
            application:set_env(Application, Key, OtherVal),
            init_config_items(ChangeType, Node, Application, Env, [{Key, OtherVal}|Acc]);
        {ok, #vmq_config{val=_OtherVal} = Conf} when ChangeType == env ->
            %% value from sys.config (or app.src) is different than from Mnesia
            %% lets update
            write_config(Conf#vmq_config{val=Value}),
            init_config_items(ChangeType, Node, Application, Env, [Item|Acc])
    end;
init_config_items(_, _, _, [], Acc) -> Acc.

read_config(ConfigKey) ->
    case mnesia:read(vmq_config, ConfigKey) of
        [] -> {error, not_found};
        [#vmq_config{} = Config] -> {ok, Config}
    end.

write_config(#vmq_config{vclock=VClock} = Config) ->
    mnesia:write(vmq_config, Config#vmq_config{
                               vclock=unsplit_vclock:increment(node(), VClock)
                              }, write).
