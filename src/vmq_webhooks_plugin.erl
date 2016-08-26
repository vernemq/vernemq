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

-module(vmq_webhooks_plugin).

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

%% API
-export([start_link/0
        ,register_endpoint/2
        ,deregister_endpoint/2
        ,all_hooks/0
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

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

register_endpoint(Endpoint, HookName) when is_binary(Endpoint), is_atom(HookName) ->
    gen_server:call(?MODULE, {register_endpoint, Endpoint, HookName}).

deregister_endpoint(Endpoint, HookName) when is_binary(Endpoint), is_atom(HookName) ->
    gen_server:call(?MODULE, {deregister_endpoint, Endpoint, HookName}).

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
handle_call({register_endpoint, Endpoint, Hook}, _From, State) ->
    Reply =
        case ets:lookup(?TBL, Hook) of
            [] ->
                enable_hook(Hook),
                ets:insert(?TBL, {Hook, [Endpoint]}),
                ok;
            [{_, Endpoints}] ->
                case lists:member(Endpoint, Endpoints) of
                    false -> 
                        %% Hooks/endpoints are invoked in list order
                        %% and oldest should be invoked first.
                        NewEndpoints = Endpoints ++ [Endpoint],
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
            [{_, [Endpoint]}] ->
                disable_hook(Hook),
                ets:delete(?TBL, Hook),
                ok;
            [{_, Endpoints}] ->
                case lists:member(Endpoint, Endpoints) of
                    false ->
                        {error, not_found};
                    true ->
                        ets:insert(?TBL, {Hook, lists:delete(Endpoint, Endpoints)}),
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
    {_Hooks, Endpoints} = lists:unzip(all_hooks()),
    [ hackney_pool:stop_pool(E) || E <- lists:usort(lists:flatten(Endpoints)) ],
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
                                    {topics, [[unword(T), QoS]
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
                       {topics, [[unword(T), QoS]
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

maybe_start_pool(Endpoint) ->
    ok = hackney_pool:start_pool(Endpoint, [{timeout, 60000}, {max_connections, 100}]).

maybe_stop_pool(Endpoint) ->
    InUse = lists:filter(fun({_, Endpoints}) ->
                                 lists:member(Endpoint, Endpoints)
                         end, all_hooks()),
    case InUse of
        [] -> hackney_pool:stop_pool(Endpoint);
        _ -> ok
    end.

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
        [{_, Endpoints}] ->
            all_till_ok(Endpoints, HookName, Args)
    end.

all_till_ok([Endpoint|Rest], HookName, Args) ->
    case call_endpoint(Endpoint, HookName, Args) of
        true ->
            ok;
        Modifiers when is_list(Modifiers) ->
            case check_modifiers(HookName, Modifiers) of
                error ->
                    error;
                ValidatedModifiers ->
                    {ok, ValidatedModifiers}
            end;
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
        [{_, Endpoints}] ->
            all(Endpoints, HookName, Args)
    end.

all([Endpoint|Rest], HookName, Args) ->
    _ = call_endpoint(Endpoint, HookName, Args),
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
                    ([T, Q], AccTopics) when is_binary(T) and is_number(Q) ->
                        case vmq_topic:validate_topic(subscribe, T) of
                            {ok, NewTopic} ->
                                [{NewTopic, round(Q)}|AccTopics];
                            {error, R} ->
                                lager:error("can't rewrite topic in auth_on_subscribe due to ~p", [R]),
                                error
                        end;
                    (T, _) ->
                        lager:error("can't rewrite topic in auth_on_subscribe due to wrong format ~p", [T]),
                        error
                end, [], TopicsModifiers);
check_modifiers(on_unsubscribe, TopicsModifiers) ->
    lists:foldl(fun (_, error) -> error;
                    (T, AccTopics) when is_binary(T) ->
                        case vmq_topic:validate_topic(subscribe, T) of
                            {ok, NewTopic} ->
                                [NewTopic|AccTopics];
                            {error, R} ->
                                lager:error("can't rewrite topic in on_unsubscribe due to ~p", [R]),
                                error
                        end;
                    (T, _) ->
                        lager:error("can't rewrite topic in on_unsubscribe due to wrong format ~p", [T]),
                        error
                end, [], TopicsModifiers);
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

call_endpoint(Endpoint, Hook, Args) ->
    Method = post,
    Headers = [{<<"vernemq-hook">>, atom_to_binary(Hook, utf8)}],
    Opts = [{pool, Endpoint}],
    Res =
        case hackney:request(Method, Endpoint, Headers, encode_payload(Hook, Args), Opts) of
            {ok, 200, _RespHeaders, CRef} ->
                case hackney:body(CRef) of
                    {ok, Body} ->
                        case jsx:is_json(Body) of
                            true ->
                                handle_response(Hook, jsx:decode(Body, [{labels, atom}]));
                            false ->
                                {error, received_payload_not_json}
                        end;
                    {error, _} = E ->
                        E
                end;
            {ok, Code, _, _} ->
                {error, {invalid_response_code, Code}};
            {error, _} = E  ->
                E
        end,
    case Res of
        {decoded_error, Reason} ->
            lager:debug("calling endpoint received error with reason: ~p", [Reason]),
            error;
        {error, Reason} ->
            lager:error("calling endpoint failed due to: ~p", [Reason]),
            error;
        [] ->
            true;
        Res ->
            Res
    end.

handle_response(Hook, Decoded) 
  when Hook =:= auth_on_register; Hook =:= auth_on_publish;
       Hook =:= on_deliver ->
    case proplists:get_value(result, Decoded) of
        <<"ok">> ->
            normalize_modifiers(Hook, proplists:get_value(modifiers, Decoded, []));
        <<"next">> -> next;
        Result when is_list(Result) ->
            {decoded_error, proplists:get_value(error, Result, unknown_error)}
    end;
handle_response(Hook, Decoded)
  when Hook =:= auth_on_subscribe; Hook =:= on_unsubscribe ->
    case proplists:get_value(result, Decoded) of
        <<"ok">> ->
            normalize_modifiers(Hook, proplists:get_value(topics, Decoded, []));
        <<"next">> -> next;
        Result when is_list(Result) ->
            {decoded_error, proplists:get_value(error, Result, unknown_error)}
    end;
handle_response(_Hook, _Decoded) ->
    next.

normalize_modifiers(auth_on_register, Mods) ->
    lists:map(
      fun({mountpoint, Mountpoint}) ->
              {mountpoint, binary_to_list(Mountpoint)};
         (E) -> E
      end, Mods);
normalize_modifiers(auth_on_subscribe, Topics) ->
    lists:map(
      fun(PL) ->
              [proplists:get_value(topic, PL),
               proplists:get_value(qos, PL)]
      end,
      Topics);
normalize_modifiers(auth_on_publish, Mods) ->
    lists:map(
      fun({mountpoint, Mountpoint}) ->
              {mountpoint, binary_to_list(Mountpoint)};
         (E) -> E
      end, Mods);
normalize_modifiers(on_unsubscribe, Mods) ->
    Mods;
normalize_modifiers(on_deliver, Mods) ->
    Mods.

encode_payload(Hook, Args)
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
             ({client_id, V}) -> {subscriber_id, V};
             (V) -> V
          end,
          Args),
    jsx:encode(RemappedKeys);
encode_payload(_, Args) ->
    RemappedKeys = 
        lists:map(
          fun({addr, V}) -> {peer_addr, V};
             ({port, V}) -> {peer_port, V};
             ({client_id, V}) -> {subscriber_id, V};
             (V) -> V
          end,
          Args),
    jsx:encode(RemappedKeys).
