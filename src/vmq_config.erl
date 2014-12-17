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
         change_config/0,
         configure_node/0,
         table_defs/0,
         reset/0,
         get_env/1,
         get_env/2,
         get_env/3,
         get_all_env/1
        ]).

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

-define(TABLE, vmq_config_cache).
-define(MNESIA, ?MODULE).



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

get_env(Key) ->
    get_env(vmq_server, Key, undefined).

get_env(Key, Default) ->
    get_env(vmq_server, Key, Default).

get_env(App, Key, Default) ->
    case ets:lookup(?TABLE, {App, Key}) of
        [{_, Val}] -> Val;
        [] ->
            Val =
            case mnesia:dirty_read(?MNESIA, {node(), App, Key}) of
                [] ->
                    case mnesia:dirty_read(?MNESIA, {App, Key}) of
                        [] ->
                            application:get_env(App, Key, Default);
                        [#vmq_config{val=GlobalVal}] ->
                            GlobalVal
                    end;
                [#vmq_config{val=NodeVal}] ->
                    NodeVal
            end,
            %% cache val
            ets:insert(?TABLE, {{App, Key}, Val}),
            Val
    end.

get_all_env(App) ->
    lists:foldl(fun({Key, Val}, Acc) ->
                        [{Key, get_env(App, Key, Val)}|Acc]
                end, [], application:get_all_env(App)).

configure_node() ->
    %% reset config
    ets:delete_all_objects(?TABLE),
    %% force cache initialization
    init_config_items(application:loaded_applications()),
    vmq_plugin:all(change_config, []).

init_config_items([{App, _, _}|Rest]) ->
    case application:get_env(App, vmq_config_enabled, false) of
        true ->
            lists:foreach(fun({Key, Val}) ->
                                  %% initialize the config cache
                                  get_env(App, Key, Val)
                          end, application:get_all_env(App));
        false ->
            ok
    end,
    init_config_items(Rest);
init_config_items([]) -> ok.

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


%%% VMQ_SERVER CONFIG HOOK
change_config() ->
    Env = filter_out_unchanged(get_all_env(vmq_server), []),
    %% change session configurations
    case lists:keyfind(vmq_session, 1, validate_session_config(Env, [])) of
        {_, SessionConfigs} ->
            vmq_session_sup:reconfigure_sessions(SessionConfigs);
        _ ->
            ok
    end,
    case lists:keyfind(vmq_expirer, 1, validate_expirer_config(Env, [])) of
        {_, [{persistent_client_expiration, Duration}]} ->
            vmq_session_expirer:change_duration(Duration);
        _ ->
            ok
    end,
    case lists:keyfind(vmq_listener, 1, validate_listener_config(Env, [])) of
        {_, ListenerConfigs} ->
            vmq_listener_sup:reconfigure_listeners(ListenerConfigs);
        _ ->
            ok
    end.

filter_out_unchanged([{Key, Val} = Item|Rest], Acc) ->
    case gen_server:call(?MODULE, {last_val, Key, Val}) of
        Val ->
            filter_out_unchanged(Rest, Acc);
        _ ->
            filter_out_unchanged(Rest, [Item|Acc])
    end;
filter_out_unchanged([], Acc) -> Acc.

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
    ets:new(?TABLE, [public, named_table, {read_concurrency, true}]),
    vmq_plugin_mgr:enable_module_plugin(?MODULE, change_config, 0),
    {ok, []}.

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
            {reply, nil, [{Key, Val}|LastVals]};
        {Key, Val} ->
            %% unchanged
            {reply, Val, LastVals};
        {Key, OldVal} ->
            {reply, OldVal, [{Key, Val}|lists:keydelete(Key, 1, LastVals)]}
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
validate_session_config([{allow_anonymous, Val} = Item|Rest], Acc)
  when is_boolean(Val) ->
    validate_session_config(Rest, [Item|Acc]);
validate_session_config([{max_client_id_size, Val} = Item|Rest], Acc)
  when is_integer(Val) and Val >= 0 ->
    validate_session_config(Rest, [Item|Acc]);
validate_session_config([{retry_interval, Val} = Item|Rest], Acc)
  when is_integer(Val) and Val > 0 ->
    validate_session_config(Rest, [Item|Acc]);
validate_session_config([{max_inflight_messages, Val} = Item|Rest], Acc)
  when is_integer(Val) and Val >= 0 ->
    validate_session_config(Rest, [Item|Acc]);
validate_session_config([{message_size_limit, Val} = Item|Rest], Acc)
  when is_integer(Val) and Val >= 0 ->
    validate_session_config(Rest, [Item|Acc]);
validate_session_config([{upgrade_outgoing_qos, Val} = Item|Rest], Acc)
  when is_boolean(Val) ->
    validate_session_config(Rest, [Item|Acc]);
validate_session_config([_|Rest], Acc) ->
    validate_session_config(Rest, Acc);
validate_session_config([], []) -> [];
validate_session_config([], Acc) -> [{vmq_session, Acc}].

validate_expirer_config([{persistent_client_expiration, Val} = Item|Rest], Acc)
  when is_integer(Val) and Val >= 0 ->
    validate_expirer_config(Rest, [Item|Acc]);
validate_expirer_config([_|Rest], Acc) ->
    validate_expirer_config(Rest, Acc);
validate_expirer_config([], []) -> [];
validate_expirer_config([], Acc) -> [{vmq_expirer, Acc}].

validate_listener_config([{listeners, _} = Item|Rest], Acc) ->
    validate_listener_config(Rest, [Item|Acc]);
validate_listener_config([{tcp_listen_options, _} = Item|Rest], Acc) ->
    validate_listener_config(Rest, [Item|Acc]);
validate_listener_config([_|Rest], Acc) ->
    validate_listener_config(Rest, Acc);
validate_listener_config([], Acc) -> [{vmq_listener, Acc}].
