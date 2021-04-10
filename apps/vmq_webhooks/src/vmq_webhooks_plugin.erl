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
-module(vmq_webhooks_plugin).

-include_lib("vernemq_dev/include/vernemq_dev.hrl").
-include_lib("hackney/include/hackney_lib.hrl").

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
-behaviour(auth_on_publish_m5_hook).
-behaviour(auth_on_subscribe_m5_hook).
-behaviour(on_register_m5_hook).
-behaviour(on_publish_m5_hook).
-behaviour(on_subscribe_m5_hook).
-behaviour(on_unsubscribe_m5_hook).
-behaviour(on_deliver_m5_hook).
-behaviour(on_auth_m5_hook).

-export([auth_on_register/5,
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
         auth_on_publish_m5/7,
         auth_on_subscribe_m5/4,
         on_register_m5/4,
         on_publish_m5/7,
         on_subscribe_m5/4,
         on_unsubscribe_m5/4,
         on_deliver_m5/7,
         on_auth_m5/3]).

%% API
-export([start_link/0
        ,register_endpoint/3
        ,deregister_endpoint/2
        ,all_hooks/0
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(SERVER, ?MODULE).

-record(state, {}).
-define(TBL, vmq_webhooks_table).

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
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec register_endpoint(binary(),hook_name(),_) -> any().
register_endpoint(Endpoint, HookName, Opts) when is_binary(Endpoint), is_atom(HookName) ->
    gen_server:call(?MODULE, {register_endpoint, Endpoint, HookName, Opts}).

-spec deregister_endpoint(binary(),hook_name()) -> any().
deregister_endpoint(Endpoint, HookName) when is_binary(Endpoint), is_atom(HookName) ->
    gen_server:call(?MODULE, {deregister_endpoint, Endpoint, HookName}).

-spec all_hooks() -> any().
all_hooks() ->
    ets:foldl(fun({HookName, Endpoints}, Acc) ->
                      [{HookName, Endpoints}|Acc]
              end,
             [],
             ?TBL).

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
    process_flag(trap_exit, true),
    ets:new(?TBL, [public, ordered_set, named_table, {read_concurrency, true}]),
    ok = vmq_webhooks_cache:new(),
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

handle_call({register_endpoint, Endpoint, Hook, Opts}, _From, State) ->
    Reply =
        case ets:lookup(?TBL, Hook) of
            [] ->
                enable_hook(Hook),
                ets:insert(?TBL, {Hook, [{Endpoint, Opts}]}),
                ok;
            [{_, Endpoints}] ->
                case lists:keymember(Endpoint, 1, Endpoints) of
                    false ->
                        %% Hooks/endpoints are invoked in list order
                        %% and oldest should be invoked first.
                        NewEndpoints = Endpoints ++ [{Endpoint, Opts}],
                        ets:insert(?TBL, {Hook, NewEndpoints}),
                        ok;
                    true ->
                        {error, already_registered}
                end
        end,
    maybe_start_pool(Endpoint),
    {reply, Reply, State};
handle_call({deregister_endpoint, Endpoint, Hook}, _From, State) ->
    Reply =
        case ets:lookup(?TBL, Hook) of
            [] ->
                {error, not_found};
            [{_, [{Endpoint, _}]}] ->
                disable_hook(Hook),
                ets:delete(?TBL, Hook),
                ok;
            [{_, Endpoints}] ->
                case lists:keymember(Endpoint, 1, Endpoints) of
                    false ->
                        {error, not_found};
                    true ->
                        ets:insert(?TBL, {Hook, lists:keydelete(Endpoint, 1, Endpoints)}),
                        ok
                end
        end,
    maybe_stop_pool(Endpoint),
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
    {_Hooks, Vals} = lists:unzip(all_hooks()),
    {Endpoints, _Opts} = lists:unzip(lists:flatten(Vals)),
    [ hackney_pool:stop_pool(E) || {E,_} <- lists:usort(Endpoints) ],
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
-spec nullify(_) -> any().
nullify(undefined) ->
    null;
nullify(Val) ->
    Val.

-spec auth_on_register(peer(),subscriber_id(), username(), password(), boolean()) ->
    'next' | 'ok' | {'error',_} | {'ok',[auth_on_register_hook:reg_modifiers()]}.
auth_on_register(Peer, SubscriberId, UserName, Password, CleanSession) ->
    {PPeer, Port} = peer(Peer),
    {MP, ClientId} = subscriber_id(SubscriberId),
    all_till_ok(auth_on_register, [{addr, PPeer},
                                   {port, Port},
                                   {mountpoint, MP},
                                   {client_id, ClientId},
                                   {username, nullify(UserName)},
                                   {password, nullify(Password)},
                                   {clean_session, CleanSession}]).

-spec auth_on_register_m5(peer(), subscriber_id(), username(), password(), boolean(), properties()) ->
    'next' | 'ok' | {'error', #{reason_code => auth_on_register_m5_hook:err_reason_code_name()} | atom()} |
    {'ok', auth_on_register_m5_hook:reg_modifiers()}.
auth_on_register_m5(Peer, SubscriberId, UserName, Password, CleanStart, Props) ->
    {PPeer, Port} = peer(Peer),
    {MP, ClientId} = subscriber_id(SubscriberId),
    all_till_ok(auth_on_register_m5, [{addr, PPeer},
                                      {port, Port},
                                      {mountpoint, MP},
                                      {client_id, ClientId},
                                      {username, nullify(UserName)},
                                      {password, nullify(Password)},
                                      {clean_start, CleanStart},
                                      {properties, Props}]).

-spec auth_on_publish(username(), subscriber_id(), qos(), topic(), payload(), flag()) ->
    'next' | 'ok' | {'error', any() } | {'ok', payload()|[auth_on_publish_hook:msg_modifier()]}.
auth_on_publish(UserName, SubscriberId, QoS, Topic, Payload, IsRetain) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all_till_ok(auth_on_publish, [{username, nullify(UserName)},
                                  {mountpoint, MP},
                                  {client_id, ClientId},
                                  {qos, QoS},
                                  {topic, unword(Topic)},
                                  {payload, Payload},
                                  {retain, IsRetain}]).

-spec auth_on_publish_m5(username(), subscriber_id(), qos(), topic(), payload(), flag(), properties()) ->
    'next' | 'ok' | {'error', auth_on_publish_m5_hook:error_values()} | {'ok', payload()|auth_on_publish_m5_hook:msg_modifier()}.
auth_on_publish_m5(UserName, SubscriberId, QoS, Topic, Payload, IsRetain, Props) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all_till_ok(auth_on_publish_m5, [{username, nullify(UserName)},
                                     {mountpoint, MP},
                                     {client_id, ClientId},
                                     {qos, QoS},
                                     {topic, unword(Topic)},
                                     {payload, Payload},
                                     {retain, IsRetain},
                                     {properties, Props}]).

-spec auth_on_subscribe(username(), subscriber_id(), [topic()]) ->
    'next' | 'ok' | {'error',any()} | {'ok', auth_on_subscribe_hook:sub_modifiers()}.
auth_on_subscribe(UserName, SubscriberId, Topics) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all_till_ok(auth_on_subscribe, [{username, nullify(UserName)},
                                    {mountpoint, MP},
                                    {client_id, ClientId},
                                    {topics, [[unword(T), QoS]
                                              || {T, QoS} <- Topics]}]).

-spec auth_on_subscribe_m5(username(), subscriber_id(), [topic()], properties()) ->
    'next' | 'ok' | {'error',any()} | {'ok',auth_on_subscribe_m5_hook:sub_modifiers()}.
auth_on_subscribe_m5(UserName, SubscriberId, Topics, Props) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all_till_ok(auth_on_subscribe_m5, [{username, nullify(UserName)},
                                       {mountpoint, MP},
                                       {client_id, ClientId},
                                       {topics, [[unword(T), QoS]
                                                 || {T, QoS} <- Topics]},
                                       {properties, Props}]).

-spec on_register(peer(), subscriber_id(), username()) -> 'next'.
on_register(Peer, SubscriberId, UserName) ->
    {PPeer, Port} = peer(Peer),
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_register, [{addr, PPeer},
                           {port, Port},
                           {mountpoint, MP},
                           {client_id, ClientId},
                           {username, nullify(UserName)}]).

-spec on_register_m5(peer(), subscriber_id(), username(), properties()) -> 'next'.
on_register_m5(Peer, SubscriberId, UserName, Props) ->
    {PPeer, Port} = peer(Peer),
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_register_m5, [{addr, PPeer},
                         {port, Port},
                         {mountpoint, MP},
                         {client_id, ClientId},
                         {username, nullify(UserName)},
                         {properties, Props}]).

-spec on_publish(username(), subscriber_id(), qos(), topic(), payload(), flag()) -> 'next'.
on_publish(UserName, SubscriberId, QoS, Topic, Payload, IsRetain) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_publish, [{username, nullify(UserName)},
                     {mountpoint, MP},
                     {client_id, ClientId},
                     {qos, QoS},
                     {topic, unword(Topic)},
                     {payload, Payload},
                     {retain, IsRetain}]).

-spec on_publish_m5(username(), subscriber_id(), qos(), topic(), payload(), flag(), properties()) -> 'next'.
on_publish_m5(UserName, SubscriberId, QoS, Topic, Payload, IsRetain, Props) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_publish_m5, [{username, nullify(UserName)},
                        {mountpoint, MP},
                        {client_id, ClientId},
                        {qos, QoS},
                        {topic, unword(Topic)},
                        {payload, Payload},
                        {retain, IsRetain},
                        {properties, Props}]).

-spec on_subscribe(username(), subscriber_id(), [topic()]) -> 'next'.
on_subscribe(UserName, SubscriberId, Topics) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_subscribe, [{username, nullify(UserName)},
                       {mountpoint, MP},
                       {client_id, ClientId},
                       {topics, [[unword(T), from_internal_qos(QoS)]
                                 || {T, QoS} <- Topics]}]).

-spec on_subscribe_m5(username(), subscriber_id(), [topic()], properties()) ->
    'next'.
on_subscribe_m5(UserName, SubscriberId, Topics, Props) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_subscribe_m5, [{username, nullify(UserName)},
                          {mountpoint, MP},
                          {client_id, ClientId},
                          {topics, [[unword(T), from_internal_qos(QoS)]
                                    || {T, QoS} <- Topics]},
                          {properties, Props}]).

-spec on_unsubscribe(username(), subscriber_id(), [topic()]) ->
    'next' | 'ok' | {'ok',on_unsubscribe_hook:unsub_modifiers()}.
on_unsubscribe(UserName, SubscriberId, Topics) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all_till_ok(on_unsubscribe, [{username, nullify(UserName)},
                                 {mountpoint, MP},
                                 {client_id, ClientId},
                                 {topics, [unword(T)
                                           || T <- Topics]}]).

-spec on_unsubscribe_m5(username(), subscriber_id(), [topic()], properties()) ->
    'next' | 'ok' | {'ok', on_unsubscribe_m5_hook:unsub_modifiers()}.
on_unsubscribe_m5(UserName, SubscriberId, Topics, Props) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all_till_ok(on_unsubscribe_m5, [{username, nullify(UserName)},
                                    {mountpoint, MP},
                                    {client_id, ClientId},
                                    {topics, [unword(T)
                                              || T <- Topics]},
                                    {properties, Props}]).

-spec on_deliver(username(), subscriber_id(), qos(), topic(), payload(), flag()) ->
    'next' | 'ok' | {'ok', payload()|[on_deliver_hook:msg_modifier()]}.
on_deliver(UserName, SubscriberId, QoS, Topic, Payload, IsRetain) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all_till_ok(on_deliver, [{username, nullify(UserName)},
                             {mountpoint, MP},
                             {client_id, ClientId},
                             {qos, QoS},
                             {topic, unword(Topic)},
                             {payload, Payload},
                             {retain, IsRetain}]).

-spec on_deliver_m5(username(), subscriber_id(), qos(), topic(), payload(), flag(), properties()) ->
    'next' | 'ok' | {'ok', on_deliver_m5_hook:msg_modifier()}.
on_deliver_m5(UserName, SubscriberId, QoS, Topic, Payload, IsRetain, Props) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all_till_ok(on_deliver_m5, [{username, nullify(UserName)},
                                {mountpoint, MP},
                                {client_id, ClientId},
                                {qos, QoS},
                                {topic, unword(Topic)},
                                {payload, Payload},
                                {retain, IsRetain},
                                {properties, Props}]).

-spec on_auth_m5(username(), subscriber_id(), properties()) -> 'next' | {'error', any()} | {'ok', on_auth_m5_hook:auth_modifiers()}.
on_auth_m5(UserName, SubscriberId, Props) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all_till_ok(on_auth_m5, [{username, nullify(UserName)},
                             {mountpoint, MP},
                             {client_id, ClientId},
                             {properties, Props}]).

-spec on_offline_message(subscriber_id(), qos(), topic(), payload(), flag()) -> 'next'.
on_offline_message(SubscriberId, QoS, Topic, Payload, IsRetain) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_offline_message, [{mountpoint, MP},
                             {client_id, ClientId},
                             {qos, QoS},
                             {topic, unword(Topic)},
                             {payload, Payload},
                             {retain, IsRetain}]).

-spec on_client_wakeup(subscriber_id()) -> 'next'.
on_client_wakeup(SubscriberId) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_client_wakeup, [{mountpoint, MP},
                           {client_id, ClientId}]).

-spec on_client_offline(subscriber_id()) -> 'next'.
on_client_offline(SubscriberId) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_client_offline, [{mountpoint, MP},
                            {client_id, ClientId}]).

-spec on_client_gone(subscriber_id()) -> 'next'.
on_client_gone(SubscriberId) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_client_gone, [{mountpoint, MP},
                         {client_id, ClientId}]).

-spec on_session_expired(subscriber_id()) -> 'next'.
on_session_expired(SubscriberId) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_session_expired, [{mountpoint, MP},
			     {client_id, ClientId}]).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec maybe_start_pool(_) -> 'ok'.
maybe_start_pool(Endpoint) ->
    {ok, PoolTimeout} = application:get_env(vmq_webhooks, pool_timeout),
    {ok, PoolMaxConn} = application:get_env(vmq_webhooks, pool_max_connections),
    Opts = [{timeout, PoolTimeout}, {max_connections, PoolMaxConn}],
    ok = hackney_pool:start_pool(Endpoint, Opts).

-spec maybe_stop_pool(_) -> 'ok' | {'error','not_found' | 'simple_one_for_one'}.
maybe_stop_pool(Endpoint) ->
    InUse = lists:filter(fun({_, Endpoints}) ->
                                 lists:keymember(Endpoint, 1, Endpoints)
                         end, all_hooks()),
    case InUse of
        [] -> hackney_pool:stop_pool(Endpoint);
        _ -> ok
    end.

-spec enable_hook(hook_name()) -> 'ok' | {'error','no_matching_callback_found'}.
enable_hook(HookName) ->
    check_exported_callback(HookName, ?MODULE:module_info(exports)).

-spec disable_hook(hook_name()) -> 'ok' | {'error','no_matching_callback_found'}.
disable_hook(HookName) ->
    uncheck_exported_callback(HookName, ?MODULE:module_info(exports)).

-spec check_exported_callback(hook_name(),maybe_improper_list()) -> 'ok' | {'error','no_matching_callback_found'}.
check_exported_callback(HookName, [{HookName, _Arity}|_]) ->
    ok;
check_exported_callback(HookName, [_|Exports]) ->
    check_exported_callback(HookName, Exports);
check_exported_callback(_, []) -> {error, no_matching_callback_found}.

-spec uncheck_exported_callback(hook_name(),maybe_improper_list()) -> 'ok' | {'error','no_matching_callback_found'}.
uncheck_exported_callback(HookName, [{HookName, _Arity}|_]) ->
    ok;
uncheck_exported_callback(HookName, [_|Exports]) ->
    uncheck_exported_callback(HookName, Exports);
uncheck_exported_callback(_, []) -> {error, no_matching_callback_found}.

-spec all_till_ok(hook_name(),[{atom(),_},...]) -> 'next' | 'ok' | {'error',_} | {'ok',_}.
all_till_ok(HookName, Args) ->
    case ets:lookup(?TBL, HookName) of
        [] -> next;
        [{_, Endpoints}] ->
            all_till_ok(Endpoints, HookName, Args)
    end.

-spec all_till_ok(list(any()), hook_name(), any()) -> ok | {ok, any()} |
                                                 {error, any()}.
all_till_ok([{Endpoint,EOpts}|Rest], HookName, Args) ->
    case maybe_call_endpoint(Endpoint, EOpts, HookName, Args) of
        [] -> ok;
        #{} = M when map_size(M) =:= 0 -> ok;
        Modifiers when is_list(Modifiers) ->
            NewModifiers = convert_subscriber_id(Modifiers),
            case vmq_plugin_util:check_modifiers(HookName, NewModifiers) of
                error ->
                    error;
                ValidatedModifiers ->
                    {ok, ValidatedModifiers}
            end;
        Modifiers when is_map(Modifiers) ->
            NewModifiers = convert_subscriber_id(maps:to_list(Modifiers)),
            case vmq_plugin_util:check_modifiers(HookName, NewModifiers) of
                error ->
                    error;
                ValidatedModifiers ->
                    {ok, maps:from_list(ValidatedModifiers)}
            end;
        {error, Reason} ->
            {error, Reason};
        _ ->
            all_till_ok(Rest, HookName, Args)
    end;
all_till_ok([], _, _) -> next.

-spec convert_subscriber_id(maybe_improper_list()) -> maybe_improper_list().
%% Collect client_id and mountpoint if they exist and move them in to
%% a subscriber_id tuple. This is the format expected by
%% `vmq_plugins_util:check_modifiers/1`
convert_subscriber_id(Modifiers) ->
    case {lists:keyfind(client_id, 1, Modifiers),
          lists:keyfind(mountpoint, 1, Modifiers)} of
        {{_, ClientId}, {_, Mountpoint}} when is_binary(ClientId) and is_binary(Mountpoint) ->
            SubscriberId = {subscriber_id, [{client_id, ClientId}, {mountpoint, Mountpoint}]},
            [SubscriberId | lists:keydelete(mountpoint, 1, lists:keydelete(client_id, 1, Modifiers))];
        _ ->
            Modifiers
    end.
-spec all(hook_name(),[{'addr' | 'client_id' | 'mountpoint' |
'payload' | 'port' | 'properties' | 'qos' | 'retain' | 'topic' | 'topics' | 'username',_},...]) -> 'next'.
all(HookName, Args) ->
    case ets:lookup(?TBL, HookName) of
        [] ->
            next;
        [{_, Endpoints}] ->
            all(Endpoints, HookName, Args)
    end.

-spec all([_],hook_name(),
[{'addr' | 'client_id' | 'mountpoint' | 'payload' | 'port' | 'properties' | 'qos' | 'retain' | 'topic' | 'topics' | 'username',_},...]) ->
    'next'.
all([{Endpoint,EOpts}|Rest], HookName, Args) ->
    _ = call_endpoint(Endpoint, EOpts, HookName, Args),
    all(Rest, HookName, Args);
all([], _, _) -> next.

-spec unword(topic()) -> binary().
unword(T) ->
    iolist_to_binary(vmq_topic:unword(T)).

-spec peer({inet:ip_address(), integer()}) ->
     {'undefined' | binary(),'undefined' | integer()}.
peer({Peer, Port}) when is_tuple(Peer) and is_integer(Port) ->
    case inet:ntoa(Peer) of
        {error, einval} ->
            {undefined, undefined};
        PeerStr ->
            {list_to_binary(PeerStr), Port}
    end.

-spec subscriber_id(subscriber_id()) -> {binary(), client_id()}.
subscriber_id({"", ClientId}) -> {<<>>, ClientId};
subscriber_id({MP, ClientId}) -> {list_to_binary(MP), ClientId}.

ssl_options(Endpoint) ->
    VerifyOpts = case application:get_env(vmq_webhooks, verify_peer) of
        {ok, false} ->
            [];
        _ ->
            VerifyFun = {
                fun ssl_verify_hostname:verify_fun/3,
                [{check_hostname, Endpoint}]
            },
            [{verify, verify_peer},
             {reuse_sessions, false},
             {verify_fun, VerifyFun},
             {customize_hostname_check, [{match_fun, public_key:pkix_verify_hostname_match_fun(https)}]}]
    end,
    {ok, TlsVersion} =  application:get_env(vmq_webhooks, tls_version),
    TlsVersionOpts = [{versions, [TlsVersion]}],
    CACertFileOpts = case application:get_env(vmq_webhooks, cafile) of
        {ok, ""} -> [];
        {ok, CACF} -> [{cacertfile, CACF}];
        undefined -> []
    end,
    CRLOpts = case application:get_env(vmq_webhooks, use_crls) of
        {ok, false} ->
            [];
        _ ->
            [{crl_check, peer},
             {crl_cache, {ssl_crl_cache, {internal, [{http, 5000}]}}}]
    end,
    {ok, Depth} = application:get_env(vmq_webhooks, depth),
    DepthOpts = [{depth, Depth}],
    CertFileOpts = case application:get_env(vmq_webhooks, certfile) of
        {ok, ""} -> [];
        {ok, CF} -> [{certfile, CF}];
        undefined -> []
    end,
    KeyFileOpts = case application:get_env(vmq_webhooks, keyfile) of
        {ok, ""} -> [];
        {ok, KF} -> [{keyfile, KF}];
        undefined -> []
    end,
    KeyfilePasswordOpts = case application:get_env(vmq_webhooks, keyfile_password) of
        {ok, ""} -> [];
        {ok, KFP} -> [{password, KFP}];
        undefined -> []
    end,
    lists:append([VerifyOpts,
                  TlsVersionOpts,
                  CACertFileOpts,
                  CRLOpts,
                  DepthOpts,
                  CertFileOpts,
                  KeyFileOpts,
                  KeyfilePasswordOpts]).
-spec maybe_ssl_opts(binary()) -> proplists:proplist().
maybe_ssl_opts(Endpoint) ->
    case Endpoint of
        <<"https://", _Rest/binary>> ->
            URL = hackney_url:parse_url(Endpoint),
            [{ssl_options, ssl_options(URL#hackney_url.host)}];
         _ -> []
    end.
-spec maybe_call_endpoint(_,_,hook_name(),[{atom(),_},...]) -> any().
maybe_call_endpoint(Endpoint, EOpts, Hook, Args)
  when Hook =:= auth_on_register;
       Hook =:= auth_on_publish;
       Hook =:= auth_on_subscribe;
       Hook =:= auth_on_register_m5;
       Hook =:= auth_on_publish_m5;
       Hook =:= auth_on_subscribe_m5 ->
    case vmq_webhooks_cache:lookup(Endpoint, Hook, Args) of
        not_found ->
            case call_endpoint(Endpoint, EOpts, Hook, Args) of
                {Modifiers, ExpiryInSecs} when is_list(Modifiers) ->
                    vmq_webhooks_cache:insert(Endpoint, Hook, Args, ExpiryInSecs, Modifiers),
                    Modifiers;
                Res ->
                    Res
            end;
        Modifiers ->
            Modifiers
    end;
maybe_call_endpoint(Endpoint, EOpts, Hook, Args) ->
    call_endpoint(Endpoint, EOpts, Hook, Args).

-spec call_endpoint(binary() | [binary() | maybe_improper_list(any(),binary() | []) | char()] |
{'hackney_url',atom(),atom(),binary(),'undefined' | binary(),'nil' | 'undefined' | binary(),binary(),binary(),string(),'undefined'
| integer(),binary(),binary()},map(),hook_name(),[{atom(),_},...]) -> any().
call_endpoint(Endpoint, EOpts, Hook, Args0) ->
    Method = post,
    Headers = [{<<"Content-Type">>, <<"application/json">>},
               {<<"vernemq-hook">>, atom_to_binary(Hook, utf8)}],
    Opts = [{pool, Endpoint},
            {recv_timeout, maps:get(response_timeout, EOpts)}] ++ maybe_ssl_opts(Endpoint),
    Args1 = filter_args(Args0, Hook, EOpts),
    Payload = encode_payload(Hook, Args1, EOpts),
    Res =
        case hackney:request(Method, Endpoint, Headers, Payload, Opts) of
            {ok, 200, RespHeaders, CRef} ->
                case hackney:body(CRef) of
                    {ok, Body} ->
                        case jsx:is_json(Body) of
                            true ->
                                handle_response(Hook,
                                                parse_headers(RespHeaders),
                                                jsx:decode(Body, [{labels, binary}]),
                                                EOpts);
                            false ->
                                {error, received_payload_not_json}
                        end;
                    {error, _} = E ->
                        E
                end;
            {ok, Code, _, CRef} ->
                hackney:close(CRef),
                {error, {invalid_response_code, Code}};
            {error, _} = E  ->
                E
        end,
    case Res of
        {decoded_error, Reason} ->
            lager:debug("calling endpoint received error due to ~p", [Reason]),
            {error, Reason};
        {error, Reason} ->
            lager:error("calling endpoint failed due to ~p", [Reason]),
            {error, Reason};
        Res ->
            Res
    end.

-spec parse_headers([any()]) -> #{'max_age'=>integer()}.
parse_headers(Headers) ->
    case hackney_headers:parse(<<"cache-control">>, Headers) of
        CC when is_binary(CC) ->
            case parse_max_age(CC) of
                MaxAge when is_integer(MaxAge) -> #{max_age => MaxAge};
                _ -> #{}
            end;
        _ -> #{}
    end.

-spec parse_max_age(binary()) -> 'undefined' | integer() | {'error','badarg'}.
parse_max_age(<<>>) -> undefined;
parse_max_age(<<"max-age=", MaxAgeVal/binary>>) ->
    digits(MaxAgeVal);
parse_max_age(<<_,Rest/binary>>) ->
    parse_max_age(Rest).

-spec digits(binary()) -> integer() | {'error','badarg'}.
digits(<<D, Rest/binary>>) when D>=$0, D=<$9 ->
    digits(Rest, D - $0);
digits(_Data) -> {error, badarg}.

-spec digits(binary(),integer()) -> integer().
digits(<<D, Rest/binary>>, Acc) when D>=$0, D=<$9 ->
    digits(Rest, Acc*10 + (D - $0));
digits(_, Acc) -> Acc.

-spec handle_response(hook_name(),#{'max_age'=>integer()},'false' | 'null' | 'true' | binary() | [any()] | number() | map(),map()) -> any().
handle_response(Hook, #{max_age := MaxAge}, Decoded, EOpts)
  when Hook =:= auth_on_register;
       Hook =:= auth_on_publish;
       Hook =:= auth_on_subscribe ->
    case handle_response(Hook, Decoded, EOpts) of
        Res when is_list(Res) ->
            {Res, MaxAge};
        Res ->
            Res
    end;
handle_response(Hook, _, Decoded, EOpts) ->
    handle_response(Hook, Decoded, EOpts).

-spec handle_response(hook_name(),'false' | 'null' | 'true' | binary() | [any()] | number() | map(),map()) -> any().
handle_response(Hook, Decoded, EOpts)
  when Hook =:= auth_on_register_m5;
       Hook =:= auth_on_publish_m5;
       Hook =:= auth_on_subscribe_m5;
       Hook =:= on_unsubscribe_m5;
       Hook =:= on_deliver_m5;
       Hook =:= on_auth_m5;
       Hook =:= auth_on_register;
       Hook =:= auth_on_publish;
       Hook =:= on_deliver ->
    %% this clause handles all results with modifiers in the return value.
    case proplists:get_value(<<"result">>, Decoded) of
        <<"ok">> ->
            normalize_modifiers(Hook, proplists:get_value(<<"modifiers">>, Decoded, []), EOpts);
        <<"next">> -> next;
        Result when is_list(Result) ->
            {decoded_error, proplists:get_value(<<"error">>, Result, unknown_error)}
    end;
handle_response(Hook, Decoded, EOpts)
  when Hook =:= auth_on_subscribe; Hook =:= on_unsubscribe ->
    %% this clause handles the cases where the results are not
    %% returned as modifiers.
    case proplists:get_value(<<"result">>, Decoded) of
        <<"ok">> ->
            normalize_modifiers(Hook, proplists:get_value(<<"topics">>, Decoded, []), EOpts);
        <<"next">> -> next;
        Result when is_list(Result) ->
            {decoded_error, proplists:get_value(<<"error">>, Result, unknown_error)}
    end;
handle_response(_Hook, _Decoded, _) ->
    next.

-spec atomize_keys([any()]) -> [{atom(),_}].
atomize_keys(Mods) ->
    lists:map(
      fun({K,V}) when is_binary(K) ->
              {binary_to_existing_atom(K,utf8), V};
         ({K,V}) when is_atom(K) ->
              {K,V}
      end, Mods).

-spec normalize_modifiers(hook_name(),_,map()) -> any().
normalize_modifiers(Hook, Mods, Opts)
  when Hook =:= auth_on_register_m5;
       Hook =:= auth_on_unsubscribe_m5;
       Hook =:= on_unsubscribe_m5;
       Hook =:= on_auth_m5 ->
    NMods0 = normalize_properties(atomize_keys(Mods), Opts),
    maps:from_list(NMods0);
normalize_modifiers(Hook, Mods, Opts)
    when Hook =:= auth_on_publish_m5;
         Hook =:= on_deliver_m5 ->
    NMods0 = normalize_properties(atomize_keys(Mods), Opts),
    NMods1 = norm_payload(NMods0, Opts),
    maps:from_list(NMods1);
normalize_modifiers(auth_on_subscribe_m5, Mods, Opts) ->
    NMods0 = normalize_properties(atomize_keys(Mods), Opts),
    NMods1 = normalize_sub_topics(NMods0, Opts),
    maps:from_list(NMods1);
normalize_modifiers(auth_on_register, Mods, _) ->
    atomize_keys(Mods);
normalize_modifiers(auth_on_subscribe, Topics, _) ->
    lists:map(
      fun(PL) ->
              {proplists:get_value(<<"topic">>, PL),
               proplists:get_value(<<"qos">>, PL)}
      end,
      Topics);
normalize_modifiers(auth_on_publish, Mods, EOpts) ->
    norm_payload(atomize_keys(Mods), EOpts);
normalize_modifiers(on_deliver, Mods, EOpts) ->
    norm_payload(atomize_keys(Mods), EOpts);
normalize_modifiers(on_unsubscribe, Mods, _) ->
    Mods.

-spec normalize_properties([{atom(),_}],map()) -> [{atom(),_}].
normalize_properties(Modifiers, Opts) ->
    case lists:keyfind(properties, 1, Modifiers) of
        false ->
            Modifiers;
        {_, Props} ->
            NewProps =
                maps:from_list(lists:map(
                                 fun({K,V}) ->
                                         normalize_property(binary_to_existing_atom(K,utf8), V, Opts)
                                 end, Props)),
            lists:keyreplace(properties, 1, Modifiers, {properties, NewProps})
    end.

-spec normalize_property(atom(),_,map()) -> {atom(),_}.
normalize_property(?P_PAYLOAD_FORMAT_INDICATOR, Val, _Opts) ->
    {?P_PAYLOAD_FORMAT_INDICATOR, binary_to_existing_atom(Val,utf8)};
normalize_property(?P_RESPONSE_TOPIC, Val, _Opts) ->
    {?P_RESPONSE_TOPIC, Val};
normalize_property(?P_USER_PROPERTY, Values, Opts) ->
    NValues = [{maybe_b64decode(K, Opts),
                maybe_b64decode(V, Opts)} || [{_,K},{_,V}] <- Values],
    {?P_USER_PROPERTY, NValues};
normalize_property(?P_AUTHENTICATION_DATA, Val, _Opts) ->
    {?P_AUTHENTICATION_DATA, base64:decode(Val)};
normalize_property(K,V, _Opts) ->
    %% let through unmodified.
    {K,V}.

-spec normalize_sub_topics([{atom(),_}],map()) -> [any()].
normalize_sub_topics(Mods, _Opts) ->
    lists:map(
      fun({topics, Topics}) ->
              {topics,
               lists:map(
                 fun(T) ->
                         Topic = proplists:get_value(<<"topic">>, T),
                         QoS = proplists:get_value(<<"qos">>, T),
                         case lists:member(QoS, [0,1,2]) of
                             true ->
                                 NL = proplists:get_value(<<"no_local">>, T, false),
                                 RAP = proplists:get_value(<<"rap">>, T, false),
                                 RH = retain_handling(
                                        proplists:get_value(<<"retain_handling">>, T, send_retain)),
                                 {Topic, {QoS, #{no_local => NL,
                                                 rap => RAP,
                                                 retain_handling => RH}}};
                             _ ->
                                 {Topic, QoS}
                         end
                 end, Topics)};
         (E) -> E
      end, Mods).

-spec retain_handling(atom() | bitstring()) -> atom().
retain_handling(<<"send_retain">>) -> send_retain;
retain_handling(<<"send_if_new_sub">>) -> send_if_new_sub;
retain_handling(<<"dont_send">>) -> dont_send;
retain_handling(Val) when is_atom(Val) ->
    Val;
retain_handling(Val) ->
    throw({invalid_retain_handling, Val}).

-spec norm_payload([{atom(),_}],map()) -> [any()].
norm_payload(Mods, EOpts) ->
    lists:map(
      fun({payload, Payload}) ->
              {payload, b64decode(Payload, EOpts)};
         (E) -> E
      end, Mods).

-spec filter_args([{atom(),_},...],hook_name(),map()) -> [{atom(),_}].
filter_args(Args, Hook, #{no_payload := true})
            when Hook =:= auth_on_publish;
                 Hook =:= auth_on_publish_m5 ->
    lists:keydelete(payload, 1, Args);
filter_args(Args, _, _) ->
    Args.

-spec encode_payload(hook_name(),[{atom(),_}],map()) -> binary().
encode_payload(Hook, Args, Opts)
  when Hook =:= auth_on_subscribe_m5;
       Hook =:= on_subscribe_m5 ->
    RemappedKeys =
        lists:map(
          fun({topics, Topics}) ->
                  {topics,
                   lists:map(
                             fun([T,{Q, #{no_local := NL,
                                          rap := Rap,
                                          retain_handling := RH}}]) ->
                                     [{topic, T},
                                      {qos, Q},
                                      {no_local, NL},
                                      {rap, Rap},
                                      {retain_handling, RH}];
                                ([T, Q]) ->
                                     [{topic, T},
                                      {qos, Q}]
                             end,
                     Topics)};
             ({client_id, V}) -> {client_id, V};
             ({properties, V}) -> {properties, encode_props(V, Opts)};
             (V) -> V
          end,
          Args),
    jsx:encode(RemappedKeys);
encode_payload(Hook, Args, Opts)
  when Hook =:= auth_on_subscribe; Hook =:= on_subscribe ->
    RemappedKeys =
        lists:map(
          fun({topics, Topics}) ->
                  {topics,
                   lists:map(
                             fun([T,Q]) ->
                                     [{topic, T},
                                      {qos, Q}]
                             end,
                     Topics)};
             ({client_id, V}) -> {client_id, V};
             ({properties, V}) -> {properties, encode_props(V, Opts)};
             (V) -> V
          end,
          Args),
    jsx:encode(RemappedKeys);
encode_payload(_, Args, Opts) ->
    RemappedKeys =
        lists:map(
          fun({addr, V}) -> {peer_addr, V};
             ({port, V}) -> {peer_port, V};
             ({client_id, V}) -> {client_id, V};
             ({properties, V}) -> {properties, encode_props(V, Opts)};
             ({payload, V}) ->
                  {payload, b64encode(V, Opts)};
             (V) -> V
          end,
          Args),
    jsx:encode(RemappedKeys).

-spec encode_props(properties(),map()) -> any().
encode_props(Props, Opts) when is_map(Props) ->
    maps:fold(fun(K,V,Acc) ->
                      {K1, V1} = encode_property(K,V,Opts),
                      maps:put(K1,V1, Acc)
              end, #{}, Props).

-spec encode_property(_,_,map()) -> {_,_}.
encode_property(?P_PAYLOAD_FORMAT_INDICATOR, Val, _Opts) ->
    {?P_PAYLOAD_FORMAT_INDICATOR, erlang:atom_to_binary(Val, utf8)};
encode_property(?P_RESPONSE_TOPIC, Val, _Opts) ->
    {?P_RESPONSE_TOPIC, enc_topic(Val)};
encode_property(?P_USER_PROPERTY, Values, Opts) ->
    Values1 = lists:map(
      fun({K,V}) ->
              #{<<"key">> => maybe_b64encode(K, Opts),
                <<"val">> => maybe_b64encode(V, Opts)}
      end, Values),
    {?P_USER_PROPERTY, Values1};
encode_property(?P_AUTHENTICATION_DATA, Val, _Opts) ->
    {?P_AUTHENTICATION_DATA, base64:encode(Val)};
encode_property(Prop, Val, _) ->
    %% fall-through for properties that need no special handling.
    {Prop, Val}.

-spec maybe_b64encode(_,map()) -> any().
maybe_b64encode(V, #{base64 := false}) -> V;
maybe_b64encode(V, _) -> base64:encode(V).

-spec maybe_b64decode(_,map()) -> any().
maybe_b64decode(V, #{base64 := false}) -> V;
maybe_b64decode(V, _) -> base64:decode(V).

-spec b64encode(_,map()) -> any().
b64encode(V, #{base64_payload := false}) -> V;
b64encode(V, _) -> base64:encode(V).

-spec b64decode(_,map()) -> any().
b64decode(V, #{base64_payload := false}) -> V;
b64decode(V, _) -> base64:decode(V).

-spec from_internal_qos('not_allowed' | integer() | {integer(),map()}) -> integer() | {integer(),map()}.
from_internal_qos(not_allowed) ->
    128;
from_internal_qos(V) when is_integer(V) ->
    V;
from_internal_qos({QoS, Opts}) when is_integer(QoS),
                                    is_map(Opts) ->
    {QoS, Opts}.

-spec enc_topic(topic()) -> binary().
enc_topic(Topic) ->
    iolist_to_binary(Topic).

-ifdef(TEST).
parse_max_age_test() ->
    ?assertEqual(undefined, parse_max_age(<<>>)),
    ?assertEqual({error, badarg}, parse_max_age(<<"max-age=">>)),
    ?assertEqual({error, badarg}, parse_max_age(<<"max-age=x">>)),
    ?assertEqual(45, parse_max_age(<<"  max-age=45,sthelse">>)),
    ?assertEqual(45, parse_max_age(<<"max-age=45">>)).
-endif.
