-module(vmq_diversity_plugin).

-behaviour(gen_server).
-behaviour(auth_on_register_hook).
-behaviour(auth_on_publish_hook).
-behaviour(auth_on_subscribe_hook).
-behaviour(on_register_hook).
-behaviour(on_publish_hook).
-behaviour(on_subscribe_hook).
-behaviour(on_unsubscribe_hook).
-behaviour(on_deliver_hook).
-behaviour(on_offline_message_hook).
-behaviour(on_client_wakeup_hook).
-behaviour(on_client_offline_hook).
-behaviour(on_client_gone_hook).

-export([auth_on_register/5,
         auth_on_publish/6,
         auth_on_subscribe/3,
         on_register/3,
         on_publish/6,
         on_subscribe/3,
         on_unsubscribe/3,
         on_deliver/4,
         on_offline_message/1,
         on_client_wakeup/1,
         on_client_offline/1,
         on_client_gone/1]).


%% API functions
-export([start_link/0,
         register_hook/1,
         unregister_hook/1
        ]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).
-define(TBL, vmq_diversity_hooks).

%%%===================================================================
%%% API functions
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

register_hook(HookName) ->
    gen_server:call(?MODULE, {register_hook, self(), HookName}, infinity).

unregister_hook(HookName) ->
    gen_server:call(?MODULE, {unregister_hook, self(), HookName}, infinity).

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
    ets:new(?TBL, [public, ordered_set, named_table, {read_concurrency, true}]),
    process_flag(trap_exit, true),
    {ok, #state{}}.

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
handle_call({register_hook, OwnerPid, Hook}, _From, State) ->
    HookName = list_to_atom(binary_to_list(Hook)),
    case ets:lookup(?TBL, Hook) of
        [] ->
            enable_hook(HookName),
            ets:insert(?TBL, {HookName, [OwnerPid]});
        [{_, ScriptOwners}] ->
            ets:insert(?TBL, {HookName, [OwnerPid|ScriptOwners]})
    end,
    monitor(process, OwnerPid),
    Reply = ok,
    {reply, Reply, State};
handle_call({unregister_hook, OwnerPid, Hook}, _From, State) ->
    HookName = list_to_atom(binary_to_list(Hook)),
    case ets:lookup(?TBL, HookName) of
        [] ->
            ignore;
        [{_, ScriptOwners}]->
            case lists:member(OwnerPid, ScriptOwners) of
                false ->
                    ignore;
                true ->
                    case lists:delete(OwnerPid, ScriptOwners) of
                        [] ->
                            disable_hook(HookName),
                            ets:delete(?TBL, HookName);
                        NewScriptOwners ->
                            ets:insert(?TBL, {HookName, NewScriptOwners})
                    end
            end
    end,
    Reply = ok,
    {reply, Reply, State}.

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
handle_info({'DOWN', _, process, Pid, _}, State) ->
    ets:foldl(fun({HookName, ScriptOwners}, _) ->
                      ets:insert(?TBL, {HookName, lists:delete(Pid, ScriptOwners)})
              end, ok, ?TBL),
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
    ets:foldl(fun({HookName, _}, _) ->
                      disable_hook(HookName)
              end, ok, ?TBL),
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
%%% Hook functions
%%%===================================================================
%% called as an all_till_ok hook
auth_on_register(Peer, SubscriberId, UserName, Password, CleanSession) ->
    {PPeer, Port} = peer(Peer),
    {MP, ClientId} = subscriber_id(SubscriberId),
    all_till_ok(auth_on_register, [{addr, PPeer},
                                   {port, Port},
                                   {mountpoint, MP},
                                   {client_id, ClientId},
                                   {username, UserName},
                                   {password, Password},
                                   {clean_session, CleanSession}]).

auth_on_publish(UserName, SubscriberId, QoS, Topic, Payload, IsRetain) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all_till_ok(auth_on_publish, [{username, UserName},
                                  {mountpoint, MP},
                                  {client_id, ClientId},
                                  {qos, QoS},
                                  {topic, unword(Topic)},
                                  {payload, Payload},
                                  {retain, IsRetain}]).

auth_on_subscribe(UserName, SubscriberId, Topics) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all_till_ok(auth_on_subscribe, [{username, UserName},
                                    {mountpoint, MP},
                                    {client_id, ClientId},
                                    {topics, [{unword(T), QoS}
                                              || {T, QoS} <- Topics]}]).

on_register(Peer, SubscriberId, UserName) ->
    {PPeer, Port} = peer(Peer),
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_register, [{addr, PPeer},
                           {port, Port},
                           {mountpoint, MP},
                           {client_id, ClientId},
                           {username, UserName}]).

on_publish(UserName, SubscriberId, QoS, Topic, Payload, IsRetain) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_publish, [{username, UserName},
                     {mountpoint, MP},
                     {client_id, ClientId},
                     {qos, QoS},
                     {topic, unword(Topic)},
                     {payload, Payload},
                     {retain, IsRetain}]).

on_subscribe(UserName, SubscriberId, Topics) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_subscribe, [{username, UserName},
                       {mountpoint, MP},
                       {client_id, ClientId},
                       {topics, [{unword(T), QoS}
                                 || {T, QoS} <- Topics]}]).

on_unsubscribe(UserName, SubscriberId, Topics) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all_till_ok(on_unsubscribe, [{username, UserName},
                                 {mountpoint, MP},
                                 {client_id, ClientId},
                                 {topics, [unword(T)
                                           || T <- Topics]}]).

on_deliver(UserName, SubscriberId, Topic, Payload) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all_till_ok(on_deliver, [{username, UserName},
                             {mountpoint, MP},
                             {client_id, ClientId},
                             {topic, unword(Topic)},
                             {payload, Payload}]).

on_offline_message(SubscriberId) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_offline_message, [{mountpoint, MP},
                             {client_id, ClientId}]).

on_client_wakeup(SubscriberId) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_client_wakeup, [{mountpoint, MP},
                           {client_id, ClientId}]).

on_client_offline(SubscriberId) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_client_offline, [{mountpoint, MP},
                            {client_id, ClientId}]).

on_client_gone(SubscriberId) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_client_gone, [{mountpoint, MP},
                         {client_id, ClientId}]).

%%%===================================================================
%%% Internal functions
%%%===================================================================
enable_hook(HookName) ->
    check_exported_callback(HookName, ?MODULE:module_info(exports)).

disable_hook(HookName) ->
    uncheck_exported_callback(HookName, ?MODULE:module_info(exports)).

check_exported_callback(HookName, [{HookName, Arity}|_]) ->
    vmq_plugin_mgr:enable_module_plugin(?MODULE, HookName, Arity);
check_exported_callback(HookName, [_|Exports]) ->
    check_exported_callback(HookName, Exports);
check_exported_callback(_, []) -> {error, no_matching_callback_found}.

uncheck_exported_callback(HookName, [{HookName, Arity}|_]) ->
    vmq_plugin_mgr:disable_module_plugin(?MODULE, HookName, Arity);
uncheck_exported_callback(HookName, [_|Exports]) ->
    uncheck_exported_callback(HookName, Exports);
uncheck_exported_callback(_, []) -> {error, no_matching_callback_found}.

all_till_ok(HookName, Args) ->
    case ets:lookup(?TBL, HookName) of
        [] -> next;
        [{_, ScriptOwners}] ->
            all_till_ok(ScriptOwners, HookName, Args)
    end.

all_till_ok([Pid|Rest], HookName, Args) ->
    case vmq_diversity_script:call_function(Pid, HookName, Args) of
        ok ->
            ok;
        {ok, Modifiers} ->
            case check_modifiers(HookName, Modifiers) of
                error ->
                    error;
                ValidatedModifiers ->
                    {ok, ValidatedModifiers}
            end;
        next ->
            all_till_ok(Rest, HookName, Args);
        error ->
            error
    end;
all_till_ok([], _, _) -> next.

all(HookName, Args) ->
    case ets:lookup(?TBL, HookName) of
        [] ->
            next;
        [{_, ScriptOwners}] ->
            all(ScriptOwners, HookName, Args)
    end.

all([Pid|Rest], HookName, Args) ->
    _ = vmq_diversity_script:call_function(Pid, HookName, Args),
    all(Rest, HookName, Args);
all([], _, _) -> next.

unword(T) ->
    iolist_to_binary(vmq_topic:unword(T)).

check_modifiers(Hook, Modifiers)
  when (Hook == auth_on_publish) or (Hook == on_deliver) ->
    case lists:keyfind(topic, 1, Modifiers) of
        {topic, T} when is_binary(T) ->
            case vmq_topic:validate_topic(publish, T) of
                {ok, NewT} ->
                    lists:keyreplace(topic, 1, Modifiers, {topic, NewT});
                {error, R} ->
                    lager:error("can't rewrite topic in ~p due to ~p", [Hook, R]),
                    error
            end;
        false ->
            Modifiers
    end;
check_modifiers(auth_on_subscribe, TopicsModifiers) ->
    lists:foldl(fun (_, error) -> error;
                    ({T, Q}, AccTopics) when is_binary(T) and is_number(Q) ->
                        case vmq_topic:validate_topic(subscribe, T) of
                            {ok, NewTopic} ->
                                [{NewTopic, round(Q)}|AccTopics];
                            {error, R} ->
                                lager:error("can't rewrite topic in auth_on_subscribe due to ~p", [R]),
                                error
                        end;
                    (_, _) ->
                        lager:error("can't rewrite topic in auth_on_subscribe due to wrong format", []),
                        error
                end, [], TopicsModifiers);
check_modifiers(on_unsubscribe, TopicModifiers) ->
    lists:foldl(fun (_, error) -> error;
                    (T, AccTopics) when is_binary(T) ->
                        case vmq_topic:validate_topic(subscribe, T) of
                            {ok, NewTopic} ->
                                [NewTopic|AccTopics];
                            {error, R} ->
                                lager:error("can't rewrite topic in on_unsubscribe due to ~p", [R]),
                                error
                        end;
                    (_, _) ->
                        lager:error("can't rewrite topic in on_unsubscribe due to wrong format", []),
                        error
                end, [], TopicModifiers);
check_modifiers(_, Modifiers) -> Modifiers.


peer({Peer, Port}) when is_tuple(Peer) and is_integer(Port) ->
    case inet:ntoa(Peer) of
        {error, einval} ->
            {undefined, undefined};
        PeerStr ->
            {list_to_binary(PeerStr), Port}
    end.

subscriber_id({"", ClientId}) -> {<<>>, ClientId};
subscriber_id({MP, ClientId}) -> {list_to_binary(MP), ClientId}.
