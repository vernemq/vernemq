%% Copyright 2016 Erlio GmbH Basel Switzerland (http://erl.io)
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
         on_offline_message/5,
         on_client_wakeup/1,
         on_client_offline/1,
         on_client_gone/1]).


%% API functions
-export([start_link/0,
         register_hook/1
        ]).

%% used by vmq_diversity_cache
-export([check_modifiers/2]).

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
    case ets:lookup(?TBL, HookName) of
        [] ->
            enable_hook(HookName),
            ets:insert(?TBL, {HookName, [OwnerPid]});
        [{_, ScriptOwners}] ->
            ets:insert(?TBL, {HookName, [OwnerPid|ScriptOwners]})
    end,
    monitor(process, OwnerPid),
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
                      ets:insert(?TBL, {HookName, lists:delete(Pid, ScriptOwners)}),
                      case ets:lookup(?TBL, HookName) of
                          [{_, []}] ->
                            disable_hook(HookName),
                            ets:delete(?TBL, HookName);
                          _ ->
                              ok
                      end
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
                                   {username, nilify(UserName)},
                                   {password, nilify(Password)},
                                   {clean_session, CleanSession}]).

auth_on_publish(UserName, SubscriberId, QoS, Topic, Payload, IsRetain) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    case vmq_diversity_cache:match_publish_acl(MP, ClientId, QoS, Topic, Payload, IsRetain) of
        true ->
            %% Found a valid cache entry which grants this publish
            ok;
        Modifiers when is_list(Modifiers) ->
            %% Found a valid cache entry containing modifiers
            {ok, Modifiers};
        false ->
            %% Found a valid cache entry which rejects this publish
            error;
        no_cache ->
            all_till_ok(auth_on_publish, [{username, nilify(UserName)},
                                          {mountpoint, MP},
                                          {client_id, ClientId},
                                          {qos, QoS},
                                          {topic, unword(Topic)},
                                          {payload, Payload},
                                          {retain, IsRetain}])
    end.

auth_on_subscribe(UserName, SubscriberId, Topics) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    CacheRet =
    lists:foldl(
      fun
          (_, false) -> false;
          (_, no_cache) -> no_cache;
          ({Topic, QoS}, Acc) ->
              case vmq_diversity_cache:match_subscribe_acl(MP, ClientId, Topic, QoS) of
                  Mods when is_list(Mods) ->
                      Acc ++ Mods;
                  Ret ->
                      Ret
              end
      end, [], Topics),
    case CacheRet of
        true ->
            %% all provided topics match a cache entry which grants this subscribe
            ok;
        Modifiers when is_list(Modifiers) ->
            %% Found a valid cache entry containing modifiers
            {ok, Modifiers};
        false ->
            %% one of the provided topics doesn't match a cache entry which
            %% rejects this subscribe
            error;
        no_cache ->
            all_till_ok(auth_on_subscribe, [{username, nilify(UserName)},
                                            {mountpoint, MP},
                                            {client_id, ClientId},
                                            {topics, [[unword(T), QoS]
                                                      || {T, QoS} <- Topics]}])
    end.

on_register(Peer, SubscriberId, UserName) ->
    {PPeer, Port} = peer(Peer),
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_register, [{addr, PPeer},
                           {port, Port},
                           {mountpoint, MP},
                           {client_id, ClientId},
                           {username, nilify(UserName)}]).

on_publish(UserName, SubscriberId, QoS, Topic, Payload, IsRetain) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_publish, [{username, nilify(UserName)},
                     {mountpoint, MP},
                     {client_id, ClientId},
                     {qos, QoS},
                     {topic, unword(Topic)},
                     {payload, Payload},
                     {retain, IsRetain}]).

on_subscribe(UserName, SubscriberId, Topics) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_subscribe, [{username, nilify(UserName)},
                       {mountpoint, MP},
                       {client_id, ClientId},
                       {topics, [[unword(T), QoS]
                                 || {T, QoS} <- Topics]}]).

on_unsubscribe(UserName, SubscriberId, Topics) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    CacheRet =
    lists:foldl(
      fun
          (_, false) -> false;
          (_, no_cache) -> no_cache;
          (Topic, Acc) ->
              %% We have to rewrite topics
              case vmq_diversity_cache:match_subscribe_acl(MP, ClientId, Topic, 0) of
                  Mods when is_list(Mods) ->
                      Acc ++ [T || {T, _QoS} <- Mods];
                  Ret ->
                      Ret
              end
      end, [], Topics),
    case CacheRet of
        true ->
            ok;
        Modifiers when is_list(Modifiers) ->
            %% Found a valid cache entry containing modifiers
            {ok, Modifiers};
        false ->
            %% one of the provided topics doesn't match a cache entry which
            %% rejects this subscribe
            error;
        no_cache ->
            all_till_ok(on_unsubscribe, [{username, nilify(UserName)},
                                         {mountpoint, MP},
                                         {client_id, ClientId},
                                         {topics, [unword(T)
                                                   || T <- Topics]}])
    end.

on_deliver(UserName, SubscriberId, Topic, Payload) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all_till_ok(on_deliver, [{username, nilify(UserName)},
                             {mountpoint, MP},
                             {client_id, ClientId},
                             {topic, unword(Topic)},
                             {payload, Payload}]).

on_offline_message(SubscriberId, QoS, Topic, Payload, Retain) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_offline_message, [{mountpoint, MP},
                             {client_id, ClientId},
                             {qos, QoS},
                             {topic, unword(Topic)},
                             {payload, Payload},
                             {retain, Retain}]).

on_client_wakeup(SubscriberId) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_client_wakeup, [{mountpoint, MP},
                           {client_id, ClientId}]).

on_client_offline(SubscriberId) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    vmq_diversity_cache:clear_cache(MP, ClientId),
    all(on_client_offline, [{mountpoint, MP},
                            {client_id, ClientId}]).

on_client_gone(SubscriberId) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    vmq_diversity_cache:clear_cache(MP, ClientId),
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
            all_till_ok(lists:reverse(ScriptOwners), HookName, Args)
    end.

all_till_ok([Pid|Rest], HookName, Args) ->
    case vmq_diversity_script:call_function(Pid, HookName, Args) of
        true ->
            ok;
        Modifiers when is_list(Modifiers) ->
            case check_modifiers(HookName, Modifiers) of
                error -> error;
                CheckedModifiers ->
                    {ok, CheckedModifiers}
            end;
        false ->
            error;
        error ->
            error;
        _ ->
            all_till_ok(Rest, HookName, Args)
    end;
all_till_ok([], _, _) -> next.

all(HookName, Args) ->
    case ets:lookup(?TBL, HookName) of
        [] ->
            next;
        [{_, ScriptOwners}] ->
            all(lists:reverse(ScriptOwners), HookName, Args)
    end.

all([Pid|Rest], HookName, Args) ->
    _ = vmq_diversity_script:call_function(Pid, HookName, Args),
    all(Rest, HookName, Args);
all([], _, _) -> next.

unword(T) ->
    iolist_to_binary(vmq_topic:unword(T)).

%% Validators For the Modifiers
val_bool(B) -> is_boolean(B).

val_atom(B) when is_binary(B) -> {ok, binary_to_existing_atom(B, utf8)};
val_atom(_) -> false.

val_binary(B) -> is_binary(B).

val_string(B) when is_binary(B) -> {ok, binary_to_list(B)};
val_string(_) -> false.

val_qos(N) when is_number(N)
                and (N >= 0) and (N =< 2) -> {ok, round(N)};
val_qos(_) -> false.

val_int(I) when is_integer(I) -> true;
val_int(N) when is_number(N) -> {ok, round(N)};
val_int(_) -> false.

val_subscriber_id([{_, _}|_] = SubscriberIdModifier) ->
    case {lists:keyfind(client_id, 1, SubscriberIdModifier),
          lists:keyfind(mountpoint, 1, SubscriberIdModifier)} of
        {{_, ClientId}, {_, Mountpoint}} when is_binary(ClientId) and is_binary(Mountpoint) ->
            {ok, {binary_to_list(Mountpoint), ClientId}};
        _ ->
            false
    end;
val_subscriber_id(_) -> false.

val_pub_topic(B) when is_binary(B) ->
    case vmq_topic:validate_topic(publish, B) of
        {ok, T} -> {ok, T};
        _ -> false
    end;
val_pub_topic(_) -> false.

modifiers(auth_on_register) ->
    [{allow_register, fun val_bool/1},
     {allow_publish, fun val_bool/1},
     {allow_subscribe, fun val_bool/1},
     {allow_unsubscribe, fun val_bool/1},
     {max_message_size, fun val_int/1},
     {subscriber_id, fun val_subscriber_id/1},
     {clean_session, fun val_bool/1},
     {max_message_rate, fun val_int/1},
     {max_inflight_messages, fun val_int/1},
     {shared_subscription_policy, fun val_atom/1},
     {retry_interval, fun val_int/1},
     {upgrade_qos, fun val_bool/1},
     {allow_multiple_sessions, fun val_bool/1},
     {max_online_messages, fun val_int/1},
     {max_offline_messages, fun val_int/1},
     {queue_deliver_mode, fun val_atom/1},
     {queue_type, fun val_atom/1},
     {max_drain_time, fun val_int/1},
     {max_msgs_per_drain_step, fun val_int/1}];
modifiers(auth_on_publish) ->
    [{topic, fun val_pub_topic/1},
     {payload, fun val_binary/1},
     {qos, fun val_qos/1},
     {retain, fun val_bool/1},
     {mountpoint, fun val_string/1}];
modifiers(on_deliver) ->
    [{topic, fun val_pub_topic/1},
     {payload, fun val_binary/1}];
modifiers(_) -> [].

check_modifiers(auth_on_subscribe, Modifiers) ->
    %% auth_on_subscribe take a special form
    lists:foldl(fun (_, error) -> error;
                    ([T, Q], Acc) when is_binary(T) and is_number(Q) ->
                        case vmq_topic:validate_topic(subscribe, T) of
                            {ok, Topic} ->
                                [{Topic, round(Q)}|Acc];
                            {error, Reason} ->
                                lager:error("can't parse topic ~p in auth_on_subscribe", [{T, Q}, Reason]),
                                error
                        end;
                   (T, _) ->
                        lager:error("can't rewrite topic in auth_on_subscribe due to wrong format ~p", [T]),
                        error
                end, [], Modifiers);
check_modifiers(on_unsubscribe, Modifiers) ->
    %% on_unsubscribe take a special form
    lists:foldl(fun (_, error) -> error;
                    (T, Acc) when is_binary(T) ->
                        case vmq_topic:validate_topic(subscribe, T) of
                            {ok, Topic} ->
                                [Topic|Acc];
                            {error, Reason} ->
                                lager:error("can't parse topic ~p in on_unsubscribe", [T, Reason]),
                                error
                        end;
                   (T, _) ->
                        lager:error("can't rewrite topic in on_unsubscribe due to wrong format ~p", [T]),
                        error
                end, [], Modifiers);
check_modifiers(Hook, [{_,_}|_] = Modifiers) ->
    AllowedModifiers = modifiers(Hook),
    lists:foldl(fun (_, error) -> error;
                    ({ModKey, ModVal}, Acc) ->
                        case lists:keyfind(ModKey, 1, AllowedModifiers) of
                            false ->
                                error;
                            {_, ValidatorFun} ->
                                case ValidatorFun(ModVal) of
                                    true ->
                                        [{ModKey, ModVal}|Acc];
                                    false ->
                                        lager:error("can't validate modifier ~p ~p ~p", [Hook, ModKey, ModVal]),
                                        error;
                                    {ok, NewModVal} ->
                                        [{ModKey, NewModVal}|Acc]
                                end
                        end
                end, [], Modifiers);
check_modifiers(Hook, Modifiers) ->
    lager:error("can't check modifiers ~p for hook ~p~n", [Hook, Modifiers]),
    error.

peer({Peer, Port}) when is_tuple(Peer) and is_integer(Port) ->
    case inet:ntoa(Peer) of
        {error, einval} ->
            {undefined, undefined};
        PeerStr ->
            {list_to_binary(PeerStr), Port}
    end.

subscriber_id({"", ClientId}) -> {<<>>, ClientId};
subscriber_id({MP, ClientId}) -> {list_to_binary(MP), ClientId}.

nilify(undefined) ->
    nil;
nilify(Val) ->
    Val.

