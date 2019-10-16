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
         on_deliver/4,
         on_offline_message/5,
         on_client_wakeup/1,
         on_client_offline/1,
         on_client_gone/1,

         auth_on_register_m5/6,
         auth_on_publish_m5/7,
         auth_on_subscribe_m5/4,
         on_register_m5/4,
         on_publish_m5/7,
         on_subscribe_m5/4,
         on_unsubscribe_m5/4,
         on_deliver_m5/5,
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

register_endpoint(Endpoint, HookName, Opts) when is_binary(Endpoint), is_atom(HookName) ->
    gen_server:call(?MODULE, {register_endpoint, Endpoint, HookName, Opts}).

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

nullify(undefined) ->
    null;
nullify(Val) ->
    Val.

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

auth_on_publish(UserName, SubscriberId, QoS, Topic, Payload, IsRetain) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all_till_ok(auth_on_publish, [{username, nullify(UserName)},
                                  {mountpoint, MP},
                                  {client_id, ClientId},
                                  {qos, QoS},
                                  {topic, unword(Topic)},
                                  {payload, Payload},
                                  {retain, IsRetain}]).

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

auth_on_subscribe(UserName, SubscriberId, Topics) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all_till_ok(auth_on_subscribe, [{username, nullify(UserName)},
                                    {mountpoint, MP},
                                    {client_id, ClientId},
                                    {topics, [[unword(T), QoS]
                                              || {T, QoS} <- Topics]}]).

auth_on_subscribe_m5(UserName, SubscriberId, Topics, Props) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all_till_ok(auth_on_subscribe_m5, [{username, nullify(UserName)},
                                       {mountpoint, MP},
                                       {client_id, ClientId},
                                       {topics, [[unword(T), QoS]
                                                 || {T, QoS} <- Topics]},
                                       {properties, Props}]).

on_register(Peer, SubscriberId, UserName) ->
    {PPeer, Port} = peer(Peer),
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_register, [{addr, PPeer},
                           {port, Port},
                           {mountpoint, MP},
                           {client_id, ClientId},
                           {username, nullify(UserName)}]).

on_register_m5(Peer, SubscriberId, UserName, Props) ->
    {PPeer, Port} = peer(Peer),
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_register_m5, [{addr, PPeer},
                         {port, Port},
                         {mountpoint, MP},
                         {client_id, ClientId},
                         {username, nullify(UserName)},
                         {properties, Props}]).

on_publish(UserName, SubscriberId, QoS, Topic, Payload, IsRetain) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_publish, [{username, nullify(UserName)},
                     {mountpoint, MP},
                     {client_id, ClientId},
                     {qos, QoS},
                     {topic, unword(Topic)},
                     {payload, Payload},
                     {retain, IsRetain}]).

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

on_subscribe(UserName, SubscriberId, Topics) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_subscribe, [{username, nullify(UserName)},
                       {mountpoint, MP},
                       {client_id, ClientId},
                       {topics, [[unword(T), from_internal_qos(QoS)]
                                 || {T, QoS} <- Topics]}]).

on_subscribe_m5(UserName, SubscriberId, Topics, Props) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_subscribe_m5, [{username, nullify(UserName)},
                          {mountpoint, MP},
                          {client_id, ClientId},
                          {topics, [[unword(T), from_internal_qos(QoS)]
                                    || {T, QoS} <- Topics]},
                          {properties, Props}]).

on_unsubscribe(UserName, SubscriberId, Topics) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all_till_ok(on_unsubscribe, [{username, nullify(UserName)},
                                 {mountpoint, MP},
                                 {client_id, ClientId},
                                 {topics, [unword(T)
                                           || T <- Topics]}]).

on_unsubscribe_m5(UserName, SubscriberId, Topics, Props) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all_till_ok(on_unsubscribe_m5, [{username, nullify(UserName)},
                                    {mountpoint, MP},
                                    {client_id, ClientId},
                                    {topics, [unword(T)
                                              || T <- Topics]},
                                    {properties, Props}]).

on_deliver(UserName, SubscriberId, Topic, Payload) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all_till_ok(on_deliver, [{username, nullify(UserName)},
                             {mountpoint, MP},
                             {client_id, ClientId},
                             {topic, unword(Topic)},
                             {payload, Payload}]).

on_deliver_m5(UserName, SubscriberId, Topic, Payload, Props) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all_till_ok(on_deliver_m5, [{username, nullify(UserName)},
                                {mountpoint, MP},
                                {client_id, ClientId},
                                {topic, unword(Topic)},
                                {payload, Payload},
                                {properties, Props}]).

on_auth_m5(UserName, SubscriberId, Props) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all_till_ok(on_auth_m5, [{username, nullify(UserName)},
                             {mountpoint, MP},
                             {client_id, ClientId},
                             {properties, Props}]).

on_offline_message(SubscriberId, QoS, Topic, Payload, IsRetain) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    all(on_offline_message, [{mountpoint, MP},
                             {client_id, ClientId},
                             {qos, QoS},
                             {topic, unword(Topic)},
                             {payload, Payload},
                             {retain, IsRetain}]).

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
    {ok, PoolTimeout} = application:get_env(vmq_webhooks, pool_timeout),
    {ok, PoolMaxConn} = application:get_env(vmq_webhooks, pool_max_connections),
    ok = hackney_pool:start_pool(Endpoint, [{timeout, PoolTimeout}, {max_connections, PoolMaxConn}]).

maybe_stop_pool(Endpoint) ->
    InUse = lists:filter(fun({_, Endpoints}) ->
                                 lists:keymember(Endpoint, 1, Endpoints)
                         end, all_hooks()),
    case InUse of
        [] -> hackney_pool:stop_pool(Endpoint);
        _ -> ok
    end.

enable_hook(HookName) ->
    check_exported_callback(HookName, ?MODULE:module_info(exports)).

disable_hook(HookName) ->
    uncheck_exported_callback(HookName, ?MODULE:module_info(exports)).

check_exported_callback(HookName, [{HookName, _Arity}|_]) ->
    ok;
check_exported_callback(HookName, [_|Exports]) ->
    check_exported_callback(HookName, Exports);
check_exported_callback(_, []) -> {error, no_matching_callback_found}.

uncheck_exported_callback(HookName, [{HookName, _Arity}|_]) ->
    ok;
uncheck_exported_callback(HookName, [_|Exports]) ->
    uncheck_exported_callback(HookName, Exports);
uncheck_exported_callback(_, []) -> {error, no_matching_callback_found}.

all_till_ok(HookName, Args) ->
    case ets:lookup(?TBL, HookName) of
        [] -> next;
        [{_, Endpoints}] ->
            all_till_ok(Endpoints, HookName, Args)
    end.

-spec all_till_ok(list(any()), atom(), any()) -> ok | {ok, any()} |
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

all(HookName, Args) ->
    case ets:lookup(?TBL, HookName) of
        [] ->
            next;
        [{_, Endpoints}] ->
            all(Endpoints, HookName, Args)
    end.

all([{Endpoint,EOpts}|Rest], HookName, Args) ->
    _ = call_endpoint(Endpoint, EOpts, HookName, Args),
    all(Rest, HookName, Args);
all([], _, _) -> next.

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

call_endpoint(Endpoint, EOpts, Hook, Args0) ->
    Method = post,
    Headers = [{<<"Content-Type">>, <<"application/json">>},
               {<<"vernemq-hook">>, atom_to_binary(Hook, utf8)}],
    Opts = [{pool, Endpoint},
            {recv_timeout, maps:get(response_timeout, EOpts)}],
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

parse_headers(Headers) ->
    case hackney_headers:parse(<<"cache-control">>, Headers) of
        CC when is_binary(CC) ->
            case parse_max_age(CC) of
                MaxAge when is_integer(MaxAge) -> #{max_age => MaxAge};
                _ -> #{}
            end;
        _ -> #{}
    end.

parse_max_age(<<>>) -> undefined;
parse_max_age(<<"max-age=", MaxAgeVal/binary>>) ->
    digits(MaxAgeVal);
parse_max_age(<<_,Rest/binary>>) ->
    parse_max_age(Rest).

digits(<<D, Rest/binary>>) when D>=$0, D=<$9 ->
    digits(Rest, D - $0);
digits(_Data) -> {error, badarg}.

digits(<<D, Rest/binary>>, Acc) when D>=$0, D=<$9 ->
    digits(Rest, Acc*10 + (D - $0));
digits(_, Acc) -> Acc.

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

atomize_keys(Mods) ->
    lists:map(
      fun({K,V}) when is_binary(K) ->
              {binary_to_existing_atom(K,utf8), V};
         ({K,V}) when is_atom(K) ->
              {K,V}
      end, Mods).

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

retain_handling(<<"send_retain">>) -> send_retain;
retain_handling(<<"send_if_new_sub">>) -> send_if_new_sub;
retain_handling(<<"dont_send">>) -> dont_send;
retain_handling(Val) when is_atom(Val) ->
    Val;
retain_handling(Val) ->
    throw({invalid_retain_handling, Val}).

norm_payload(Mods, EOpts) ->
    lists:map(
      fun({payload, Payload}) ->
              {payload, b64decode(Payload, EOpts)};
         (E) -> E
      end, Mods).

filter_args(Args, Hook, #{no_payload := true})
            when Hook =:= auth_on_publish;
                 Hook =:= auth_on_publish_m5 ->
    lists:keydelete(payload, 1, Args);
filter_args(Args, _, _) ->
    Args.

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

encode_props(Props, Opts) when is_map(Props) ->
    maps:fold(fun(K,V,Acc) ->
                      {K1, V1} = encode_property(K,V,Opts),
                      maps:put(K1,V1, Acc)
              end, #{}, Props).

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


maybe_b64encode(V, #{base64 := false}) -> V;
maybe_b64encode(V, _) -> base64:encode(V).

maybe_b64decode(V, #{base64 := false}) -> V;
maybe_b64decode(V, _) -> base64:decode(V).

b64encode(V, #{base64_payload := false}) -> V;
b64encode(V, _) -> base64:encode(V).

b64decode(V, #{base64_payload := false}) -> V;
b64decode(V, _) -> base64:decode(V).

from_internal_qos(not_allowed) ->
    128;
from_internal_qos(V) when is_integer(V) ->
    V;
from_internal_qos({QoS, Opts}) when is_integer(QoS),
                                    is_map(Opts) ->
    {QoS, Opts}.


enc_topic(Topic) when is_list(Topic) ->
    iolist_to_binary(Topic).

-ifdef(TEST).
parse_max_age_test() ->
    ?assertEqual(undefined, parse_max_age(<<>>)),
    ?assertEqual({error, badarg}, parse_max_age(<<"max-age=">>)),
    ?assertEqual({error, badarg}, parse_max_age(<<"max-age=x">>)),
    ?assertEqual(45, parse_max_age(<<"  max-age=45,sthelse">>)),
    ?assertEqual(45, parse_max_age(<<"max-age=45">>)).
-endif.
