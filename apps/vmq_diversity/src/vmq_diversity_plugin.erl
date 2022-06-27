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

-module(vmq_diversity_plugin).

-include_lib("vernemq_dev/include/vernemq_dev.hrl").

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
-behaviour(on_session_expired_hook).

-behaviour(auth_on_register_m5_hook).
-behaviour(on_register_m5_hook).
-behaviour(auth_on_publish_m5_hook).
-behaviour(on_publish_m5_hook).
-behaviour(auth_on_subscribe_m5_hook).
-behaviour(on_subscribe_m5_hook).
-behaviour(on_auth_m5_hook).

-export([
    auth_on_register/5,
    auth_on_publish/6,
    auth_on_subscribe/3,
    on_register/3,
    on_publish/6,
    on_subscribe/3,
    on_unsubscribe/3,
    on_deliver/6,
    on_offline_message/5,
    on_client_wakeup/1,
    on_client_offline/1,
    on_client_gone/1,
    on_session_expired/1,
    auth_on_register_m5/6,
    on_register_m5/4,
    auth_on_publish_m5/7,
    on_publish_m5/7,
    on_deliver_m5/7,
    auth_on_subscribe_m5/4,
    on_subscribe_m5/4,
    on_unsubscribe_m5/4,
    on_auth_m5/3
]).

%% API functions
-export([
    start_link/0,
    register_hook/2
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

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

register_hook(ScriptMgrPid, HookName) ->
    gen_server:call(?MODULE, {register_hook, ScriptMgrPid, HookName}, infinity).

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
            case lists:member(OwnerPid, ScriptOwners) of
                true ->
                    ok;
                false ->
                    ets:insert(?TBL, {HookName, [OwnerPid | ScriptOwners]})
            end
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
    ets:foldl(
        fun({HookName, ScriptOwners}, _) ->
            ets:insert(?TBL, {HookName, lists:delete(Pid, ScriptOwners)}),
            case ets:lookup(?TBL, HookName) of
                [{_, []}] ->
                    disable_hook(HookName),
                    ets:delete(?TBL, HookName);
                _ ->
                    ok
            end
        end,
        ok,
        ?TBL
    ),
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
    Res = all_till_ok(auth_on_register, [
        {addr, PPeer},
        {port, Port},
        {mountpoint, MP},
        {client_id, ClientId},
        {username, nilify(UserName)},
        {password, nilify(Password)},
        {clean_session, CleanSession}
    ]),
    conv_res(auth_on_reg, Res).

auth_on_register_m5(Peer, SubscriberId, UserName, Password, CleanStart, Props) ->
    {PPeer, Port} = peer(Peer),
    {MP, ClientId} = subscriber_id(SubscriberId),
    Res = all_till_ok(auth_on_register_m5, [
        {addr, PPeer},
        {port, Port},
        {mountpoint, MP},
        {client_id, ClientId},
        {username, nilify(UserName)},
        {password, nilify(Password)},
        {clean_start, CleanStart},
        {properties, conv_args_props(Props)}
    ]),
    conv_res(auth_on_reg, Res).

on_register_m5(Peer, SubscriberId, Username, Props) ->
    {PPeer, Port} = peer(Peer),
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_register_m5, [
        {addr, PPeer},
        {port, Port},
        {mountpoint, MP},
        {client_id, ClientId},
        {username, nilify(Username)},
        {properties, conv_args_props(Props)}
    ]).

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
            {error, not_authorized};
        no_cache ->
            Res = all_till_ok(auth_on_publish, [
                {username, nilify(UserName)},
                {mountpoint, MP},
                {client_id, ClientId},
                {qos, QoS},
                {topic, unword(Topic)},
                {payload, Payload},
                {retain, IsRetain}
            ]),
            conv_res(auth_on_pub, Res)
    end.

auth_on_publish_m5(UserName, SubscriberId, QoS, Topic, Payload, IsRetain, Props) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    case vmq_diversity_cache:match_publish_acl(MP, ClientId, QoS, Topic, Payload, IsRetain) of
        true ->
            %% Found a valid cache entry which grants this publish
            ok;
        Modifiers when is_list(Modifiers) ->
            %% Found a valid cache entry containing modifiers
            {ok, modifier_compat(auth_on_publish_m5, Modifiers)};
        false ->
            %% Found a valid cache entry which rejects this publish
            {error, not_authorized};
        no_cache ->
            Res = all_till_ok(auth_on_publish_m5, [
                {username, nilify(UserName)},
                {mountpoint, MP},
                {client_id, ClientId},
                {qos, QoS},
                {topic, unword(Topic)},
                {payload, Payload},
                {retain, IsRetain},
                {properties, conv_args_props(Props)}
            ]),
            conv_res(auth_on_pub, Res)
    end.

on_publish_m5(UserName, SubscriberId, QoS, Topic, Payload, IsRetain, Props) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_publish_m5, [
        {username, nilify(UserName)},
        {mountpoint, MP},
        {client_id, ClientId},
        {qos, QoS},
        {topic, unword(Topic)},
        {payload, Payload},
        {retain, IsRetain},
        {properties, conv_args_props(Props)}
    ]).

on_deliver_m5(UserName, SubscriberId, QoS, Topic, Payload, IsRetain, Props) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all_till_ok(on_deliver_m5, [
        {username, nilify(UserName)},
        {mountpoint, MP},
        {client_id, ClientId},
        {qos, QoS},
        {topic, unword(Topic)},
        {payload, Payload},
        {retain, IsRetain},
        {properties, conv_args_props(Props)}
    ]).

auth_on_subscribe(UserName, SubscriberId, Topics) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    CacheRet =
        lists:foldl(
            fun
                (_, false) ->
                    false;
                (_, no_cache) ->
                    no_cache;
                ({Topic, QoS}, Acc) ->
                    case vmq_diversity_cache:match_subscribe_acl(MP, ClientId, Topic, QoS) of
                        Mods when is_list(Mods) ->
                            Acc ++ Mods;
                        Ret ->
                            Ret
                    end
            end,
            [],
            Topics
        ),
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
            {error, not_authorized};
        no_cache ->
            Res = all_till_ok(auth_on_subscribe, [
                {username, nilify(UserName)},
                {mountpoint, MP},
                {client_id, ClientId},
                {topics, [
                    [unword(T), QoS]
                 || {T, QoS} <- Topics
                ]}
            ]),
            conv_res(auth_on_sub, Res)
    end.

auth_on_subscribe_m5(UserName, SubscriberId, Topics, Props) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    CacheRet =
        lists:foldl(
            fun
                (_, false) ->
                    false;
                (_, no_cache) ->
                    no_cache;
                ({Topic, SubInfo}, Acc) ->
                    QoS = extract_qos(SubInfo),
                    case vmq_diversity_cache:match_subscribe_acl(MP, ClientId, Topic, QoS) of
                        Mods when is_list(Mods) ->
                            Acc ++ Mods;
                        Ret ->
                            Ret
                    end
            end,
            [],
            Topics
        ),
    case CacheRet of
        true ->
            %% all provided topics match a cache entry which grants this subscribe
            ok;
        Modifiers when is_list(Modifiers) ->
            %% Found a valid cache entry containing modifiers
            {ok, modifier_compat(auth_on_subscribe_m5, Modifiers)};
        false ->
            %% one of the provided topics doesn't match a cache entry which
            %% rejects this subscribe
            {error, not_authorized};
        no_cache ->
            Unmap = fun(SubOpts) ->
                vmq_diversity_utils:unmap(SubOpts)
            end,
            Res = all_till_ok(auth_on_subscribe_m5, [
                {username, nilify(UserName)},
                {mountpoint, MP},
                {client_id, ClientId},
                {topics, [
                    [unword(T), [QoS, Unmap(SubOpts)]]
                 || {T, {QoS, SubOpts}} <- Topics
                ]},
                {properties, conv_args_props(Props)}
            ]),
            conv_res(auth_on_sub, Res)
    end.

on_subscribe_m5(UserName, SubscriberId, Topics, Props) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    Unmap = fun(SubOpts) ->
        vmq_diversity_utils:unmap(SubOpts)
    end,
    all(on_subscribe_m5, [
        {username, nilify(UserName)},
        {mountpoint, MP},
        {client_id, ClientId},
        {topics, [
            [unword(T), [QoS, Unmap(SubOpts)]]
         || {T, {QoS, SubOpts}} <- Topics
        ]},
        {properties, conv_args_props(Props)}
    ]).

on_unsubscribe_m5(UserName, SubscriberId, Topics, Props) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    CacheRet =
        lists:foldl(
            fun
                (_, false) ->
                    false;
                (_, no_cache) ->
                    no_cache;
                (Topic, Acc) ->
                    %% We have to rewrite topics
                    case vmq_diversity_cache:match_subscribe_acl(MP, ClientId, Topic, 0) of
                        Mods when is_list(Mods) ->
                            Acc ++ [T || {T, _QoS} <- Mods];
                        Ret ->
                            Ret
                    end
            end,
            [],
            Topics
        ),
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
            all_till_ok(on_unsubscribe_m5, [
                {username, nilify(UserName)},
                {mountpoint, MP},
                {client_id, ClientId},
                {topics, [
                    unword(T)
                 || T <- Topics
                ]},
                {properties, conv_args_props(Props)}
            ])
    end.

on_auth_m5(Username, SubscriberId, Props) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all_till_ok(on_auth_m5, [
        {username, nilify(Username)},
        {mountpoint, MP},
        {client_id, ClientId},
        {properties, conv_args_props(Props)}
    ]).

on_register(Peer, SubscriberId, UserName) ->
    {PPeer, Port} = peer(Peer),
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_register, [
        {addr, PPeer},
        {port, Port},
        {mountpoint, MP},
        {client_id, ClientId},
        {username, nilify(UserName)}
    ]).

on_publish(UserName, SubscriberId, QoS, Topic, Payload, IsRetain) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_publish, [
        {username, nilify(UserName)},
        {mountpoint, MP},
        {client_id, ClientId},
        {qos, QoS},
        {topic, unword(Topic)},
        {payload, Payload},
        {retain, IsRetain}
    ]).

on_subscribe(UserName, SubscriberId, Topics) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_subscribe, [
        {username, nilify(UserName)},
        {mountpoint, MP},
        {client_id, ClientId},
        {topics, [
            [unword(T), QoS]
         || {T, QoS} <- Topics
        ]}
    ]).

on_unsubscribe(UserName, SubscriberId, Topics) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    CacheRet =
        lists:foldl(
            fun
                (_, false) ->
                    false;
                (_, no_cache) ->
                    no_cache;
                (Topic, Acc) ->
                    %% We have to rewrite topics
                    case vmq_diversity_cache:match_subscribe_acl(MP, ClientId, Topic, 0) of
                        Mods when is_list(Mods) ->
                            Acc ++ [T || {T, _QoS} <- Mods];
                        Ret ->
                            Ret
                    end
            end,
            [],
            Topics
        ),
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
            all_till_ok(on_unsubscribe, [
                {username, nilify(UserName)},
                {mountpoint, MP},
                {client_id, ClientId},
                {topics, [
                    unword(T)
                 || T <- Topics
                ]}
            ])
    end.

on_deliver(UserName, SubscriberId, QoS, Topic, Payload, IsRetain) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all_till_ok(on_deliver, [
        {username, nilify(UserName)},
        {mountpoint, MP},
        {client_id, ClientId},
        {qos, QoS},
        {topic, unword(Topic)},
        {payload, Payload},
        {retain, IsRetain}
    ]).

on_offline_message(SubscriberId, QoS, Topic, Payload, Retain) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_offline_message, [
        {mountpoint, MP},
        {client_id, ClientId},
        {qos, QoS},
        {topic, unword(Topic)},
        {payload, Payload},
        {retain, Retain}
    ]).

on_client_wakeup(SubscriberId) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_client_wakeup, [
        {mountpoint, MP},
        {client_id, ClientId}
    ]).

on_client_offline(SubscriberId) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    vmq_diversity_cache:clear_cache(MP, ClientId),
    all(on_client_offline, [
        {mountpoint, MP},
        {client_id, ClientId}
    ]).

on_client_gone(SubscriberId) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    vmq_diversity_cache:clear_cache(MP, ClientId),
    all(on_client_gone, [
        {mountpoint, MP},
        {client_id, ClientId}
    ]).

on_session_expired(SubscriberId) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    vmq_diversity_cache:clear_cache(MP, ClientId),
    all(on_session_expired, [
        {mountpoint, MP},
        {client_id, ClientId}
    ]).

%%%===================================================================
%%% Internal functions
%%%===================================================================
enable_hook(HookName) ->
    check_exported_callback(HookName, ?MODULE:module_info(exports)).

disable_hook(HookName) ->
    uncheck_exported_callback(HookName, ?MODULE:module_info(exports)).

check_exported_callback(HookName, [{HookName, _Arity} | _]) ->
    ok;
check_exported_callback(HookName, [_ | Exports]) ->
    check_exported_callback(HookName, Exports);
check_exported_callback(_, []) ->
    {error, no_matching_callback_found}.

uncheck_exported_callback(HookName, [{HookName, _Arity} | _]) ->
    ok;
uncheck_exported_callback(HookName, [_ | Exports]) ->
    uncheck_exported_callback(HookName, Exports);
uncheck_exported_callback(_, []) ->
    {error, no_matching_callback_found}.

all_till_ok(HookName, Args) ->
    case ets:lookup(?TBL, HookName) of
        [] -> next;
        [{_, ScriptOwners}] -> all_till_ok(lists:reverse(ScriptOwners), HookName, Args)
    end.

all_till_ok([Pid | Rest], HookName, Args) ->
    case vmq_diversity_script:call_function(Pid, HookName, Args) of
        true ->
            ok;
        Mods0 when is_list(Mods0) ->
            Mods1 = convert_modifiers(HookName, Mods0),
            case vmq_plugin_util:check_modifiers(HookName, Mods1) of
                error ->
                    {error, {invalid_modifiers, Mods1}};
                CheckedModifiers ->
                    {ok, CheckedModifiers}
            end;
        false ->
            {error, lua_script_returned_false};
        error ->
            {error, lua_script_error};
        {error, Reason} ->
            {error, Reason};
        _ ->
            all_till_ok(Rest, HookName, Args)
    end;
all_till_ok([], _, _) ->
    next.

all(HookName, Args) ->
    case ets:lookup(?TBL, HookName) of
        [] ->
            next;
        [{_, ScriptOwners}] ->
            all(lists:reverse(ScriptOwners), HookName, Args)
    end.

all([Pid | Rest], HookName, Args) ->
    _ = vmq_diversity_script:call_function(Pid, HookName, Args),
    all(Rest, HookName, Args);
all([], _, _) ->
    next.

unword(T) ->
    iolist_to_binary(vmq_topic:unword(T)).

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

extract_qos({QoS, _SubInfo}) -> QoS;
extract_qos(QoS) when is_integer(QoS) -> QoS.

convert_modifiers(Hook, Mods0) ->
    vmq_diversity_utils:convert_modifiers(Hook, Mods0).

conv_args_props(Props) when is_map(Props) ->
    conv_args_props(maps:to_list(Props));
conv_args_props(Props) when is_list(Props) ->
    lists:map(fun conv_args_prop/1, Props).

conv_args_prop({?P_USER_PROPERTY, UserProps}) ->
    {UPS, _} = lists:foldl(
        fun(P, {Res, Ctr}) ->
            {[{Ctr, [P]} | Res], Ctr + 1}
        end,
        {[], 1},
        UserProps
    ),
    {?P_USER_PROPERTY, lists:reverse(UPS)};
conv_args_prop({?P_RESPONSE_TOPIC, Topic}) ->
    {?P_RESPONSE_TOPIC, unword(Topic)};
conv_args_prop(P) ->
    P.

conv_res(auth_on_reg, {error, lua_script_returned_false}) ->
    {error, invalid_credentials};
conv_res(auth_on_pub, {error, lua_script_returned_false}) ->
    {error, not_authorized};
conv_res(auth_on_sub, {error, lua_script_returned_false}) ->
    {error, not_authorized};
conv_res(_, Other) ->
    Other.

%% @doc convert from the acl format to a form the MQTT 3/4/5 can
%% understand
modifier_compat(auth_on_subscribe_m5, Mods) ->
    #{topics => Mods};
modifier_compat(auth_on_publish_m5, Mods) ->
    maps:from_list(Mods).
