%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2018-2020 Erlio GmbH/Octavo Labs AG
%%% 
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     "https://www.apache.org/licenses/LICENSE-2.0"
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%% @reference <a href="https://www.apache.org/licenses/LICENSE-2.0">Apache 2 License</a>
%%%
%%% @doc Implements a server interface for the Config table and implements the
%%% `on_config_change_hook' behaviour.
%%%
%%% @TODO: Write more detailled documentation on the VerneMQ Config system.
%%% @end
%%%-----------------------------------------------------------------------------

-module(vmq_config).

-behaviour(gen_server).
-behaviour(on_config_change_hook).

%% API
-export([start_link/0, change_config/1, configure_node/0, configure_node/1,
         configure_nodes/0, get_env/1, get_env/2, get_env/3, set_env/3, set_env/4, set_env/5,
         set_global_env/4, get_all_env/1, get_prefixed_env/2, get_prefixed_all_env/1,
         unset_local_env/2, unset_global_env/2]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(vmq_config,
        {key, % {node(), app_name(), item_name()}
         val,
         short_descr,
         long_descr}).

-define(TABLE, vmq_config_cache).
-define(DB, {vmq, config}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the `vmq_config' server
%%
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> ignore | {error, atom()} | {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec get_env(Key :: any()) -> Val :: any() | undefined.
get_env(Key) ->
    get_env(vmq_server, Key, undefined).

get_env(Key, Default) ->
    get_env(vmq_server, Key, Default).

-spec get_env(App, Key, Default) -> Val when App::any(),
                                             Key::any(),
                                             Val::any(),
                                             Default::any().
get_env(App, Key, Default) ->
    IgnoreDBConfig =
        case ets:lookup(?TABLE, ignore_db_config) of
          [{_, true}] ->
              true;
          _ ->
              false
        end,
    get_env(App, Key, Default, IgnoreDBConfig).

-spec get_env(App :: atom(),
              Key :: any(),
              Default :: any(),
              IgnoreDBConfig :: boolean()) ->
                 Val :: any().
get_env(App, Key, Default, IgnoreDBConfig) ->
    case ets:lookup(?TABLE, {App, Key}) of
      [{_, Val}] ->
          Val;
      [] when IgnoreDBConfig == false ->
          Val =
              case vmq_metadata:get(?DB, {node(), App, Key}) of
                undefined ->
                    case vmq_metadata:get(?DB, {App, Key}) of
                      undefined ->
                          application:get_env(App, Key, Default);
                      #vmq_config{val = GlobalVal} ->
                          GlobalVal
                    end;
                #vmq_config{val = NodeVal} ->
                    NodeVal
              end,
          %% cache val
          ets:insert(?TABLE, {{App, Key}, Val}),
          application:set_env(App, Key, Val),
          Val;
      _ ->
          Default
    end.

-spec get_all_env(App::atom()) -> [{Key::any(), Val::any()}].
get_all_env(App) ->
    %% setting ignore_db_config to true is useful, if the broker is
    %% misconfigured and you need to cleanup first.
    IgnoreDBConfig = application:get_env(App, ignore_db_config, false),
    lists:foldl(fun ({Key, Val}, Acc) ->
                        [{Key, get_env(App, Key, Val, IgnoreDBConfig)} | Acc]
                end,
                [],
                application:get_all_env(App)).

-spec get_prefixed_env(App, Key) ->
                          {env, Key, Val} |
                          {global, Key, Val} |
                          {node, Key, Val} |
                          {error, not_found} when App::atom(), Key::any(), Val::any().
%% returns the config value in use for given key, together
%% with the scope.
get_prefixed_env(App, Key) ->
    case application:get_env(App, Key) of
      {ok, Val} ->
          case vmq_metadata:get(?DB, {node(), App, Key}) of
            undefined ->
                case vmq_metadata:get(?DB, {App, Key}) of
                  undefined ->
                      %% we only have what is stored inside the
                      %% application environment
                      {env, Key, Val};
                  #vmq_config{val = GlobalVal} ->
                      %% we have a value stored inside the application
                      %% environment, which is ignored, since we have
                      %% a value that is globally configured in the db
                      {global, Key, GlobalVal}
                end;
            #vmq_config{val = NodeVal} ->
                %% we have a value stored inside the application
                %% environment, which is ignored, since we have a
                %% value that is configured for this node in the db
                {node, Key, NodeVal}
          end;
      undefined ->
          {error, not_found}
    end.

-spec get_prefixed_all_env(App::atom()) -> [any()].
get_prefixed_all_env(App) ->
    lists:foldl(fun ({Key, _}, AccAcc) ->
                        [get_prefixed_env(App, Key) | AccAcc]
                end,
                [],
                application:get_all_env(App)).

-spec set_env(Key::any(), Val::any(), Durable::boolean()) -> ok.
set_env(Key, Val, Durable) ->
    set_env(vmq_server, Key, Val, Durable).

set_env(App, Key, Val, Durable) ->
    set_env(node(), App, Key, Val, Durable).

-spec set_env(Node::node(),
              App::atom(),
              Key::any(),
              Val::any(),
              Durable::boolean()) ->
                 ok | term() | {badrpc, term()}.
set_env(Node, App, Key, Val, false) when Node == node() ->
    ets:insert(?TABLE, {{App, Key}, Val}),
    application:set_env(App, Key, Val),
    ok;
set_env(Node, App, Key, Val, true) when Node == node() ->
    Rec =
        case vmq_metadata:get(?DB, {Node, App, Key}) of
          undefined ->
              #vmq_config{key = {Node, App, Key}, val = Val};
          Config ->
              Config#vmq_config{val = Val}
        end,
    vmq_metadata:put(?DB, {Node, App, Key}, Rec),
    ets:insert(?TABLE, {{App, Key}, Val}),
    application:set_env(App, Key, Val),
    ok;
% setting Keys on another Node:
set_env(Node, App, Key, Val, Durable) ->
    safe_rpc(Node, ?MODULE, set_env, [App, Key, Val, Durable]).

-spec safe_rpc(Node::node(), Mod::module(), Fun::atom(), [any()]) -> any().
safe_rpc(Node, Module, Fun, Args) ->
    try rpc:call(Node, Module, Fun, Args) of
      Result ->
          Result
    catch
      exit:{noproc, _NoProcDetails} ->
          {badrpc, rpc_process_down}
    end.

-spec set_global_env(App::atom(), Key::any(), Val::any(), Durable::boolean()) ->
                        ok.
set_global_env(App, Key, Val, false) ->
    ets:insert(?TABLE, {{App, Key}, Val}),
    application:set_env(App, Key, Val),
    ok;
set_global_env(App, Key, Val, true) ->
    Rec =
        case vmq_metadata:get(?DB, {App, Key}) of
          undefined ->
              #vmq_config{key = {App, Key}, val = Val};
          Config ->
              Config#vmq_config{val = Val}
        end,
    vmq_metadata:put(?DB, {App, Key}, Rec),
    ets:insert(?TABLE, {{App, Key}, Val}),
    application:set_env(App, Key, Val),
    ok.

-spec unset_local_env(_, _) -> ok.
unset_local_env(App, Key) ->
    vmq_metadata:delete(?DB, {node(), App, Key}),
    ok.

-spec unset_global_env(_, _) -> ok.
unset_global_env(App, Key) ->
    vmq_metadata:delete(?DB, {App, Key}),
    ok.

-spec configure_node() -> any().
configure_node() ->
    configure_node(node()).

-spec configure_node(Node::node()) -> ok | term() | {badrpc, term()}.
configure_node(Node) when Node == node() ->
    %% reset config
    ets:delete_all_objects(?TABLE),
    %% force cache initialization
    Configs = init_config_items(application:loaded_applications(), []),
    vmq_plugin:all(change_config, [Configs]);
configure_node(Node) ->
    safe_rpc(Node, ?MODULE, configure_node, []).

-spec configure_nodes() -> [any()].
configure_nodes() ->
    Nodes = vmq_cluster:nodes(),
    _ = [safe_rpc(Node, ?MODULE, configure_node, []) || Node <- Nodes].

-spec init_config_items([{App::atom(), [any()], [any()]}], [{atom(), [any()]}]) ->
                           [{App::atom(), [any()]}].
init_config_items([{App, _, _} | Rest], Acc) ->
    case application:get_env(App, vmq_config_enabled, false) of
      true ->
          init_config_items(Rest, [{App, get_all_env(App)} | Acc]);
      false ->
          init_config_items(Rest, Acc)
    end;
init_config_items([], Acc) ->
    Acc.

-spec change_config(Configs::[any()]) -> ok.
%%% VMQ_SERVER CONFIG HOOK
change_config(Configs) ->
    {vmq_server, VmqServerConfig} = lists:keyfind(vmq_server, 1, Configs),
    Env = filter_out_unchanged(VmqServerConfig, []),
    %% change reg configurations
    _ = validate_reg_config(Env, []),
    ok.

-spec filter_out_unchanged([{Key, Val}], [any()]) -> [{Key, Val}] when Key::any(),
                                                                       Val::any().
filter_out_unchanged([{Key, Val} = Item | Rest], Acc) ->
    case gen_server:call(?MODULE, {last_val, Key, Val}) of
      Val ->
          filter_out_unchanged(Rest, Acc);
      _ ->
          filter_out_unchanged(Rest, [Item | Acc])
    end;
filter_out_unchanged([], Acc) ->
    Acc.

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
    _ = ets:new(?TABLE, [public, named_table, {read_concurrency, true}]),
    case vmq_plugin_mgr:enable_module_plugin(?MODULE, change_config, 1) of
      ok ->
          {ok, []};
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
handle_call({last_val, Key, Val}, _From, LastVals) ->
    case lists:keyfind(Key, 1, LastVals) of
      false ->
          {reply, nil, [{Key, Val} | LastVals]};
      {Key, Val} ->
          %% unchanged
          {reply, Val, LastVals};
      {Key, OldVal} ->
          {reply, OldVal, [{Key, Val} | lists:keydelete(Key, 1, LastVals)]}
    end.

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
-spec validate_reg_config([{_, _}], [{reg_views, _}]) -> ok.
validate_reg_config([{reg_views, Val} = Item | Rest], Acc) when is_list(Val) ->
    case length([RV || RV <- Val, is_atom(RV)]) == length(Val) of
      true ->
          validate_reg_config(Rest, [Item | Acc]);
      false ->
          validate_reg_config(Rest, Acc)
    end;
validate_reg_config([_ | Rest], Acc) ->
    validate_reg_config(Rest, Acc);
validate_reg_config([], []) ->
    %% no need to reconfigure registry
    ok;
validate_reg_config([], Acc) ->
    vmq_reg_sup:reconfigure_registry(Acc).
