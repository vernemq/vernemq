-module(emqttd_msg_store).
-behaviour(gen_server).

-export([start_link/1,
         store/2, store/3,
         retrieve/1,
         deref/1,
         deliver_from_store/2,
         deliver_retained/3,
         clean_session/1,
         retain_action/3,
         defer_deliver/3
         ]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).


-record(state, {store}).
%-record(msg_idx_item, {id, node, qos, msg_ref, idx_ref}).

-define(MSG_ITEM, 0).
-define(INDEX_ITEM, 1).
-define(RETAIN_ITEM, 2).
-define(MSG_INDEX_TABLE, emqttd_msg_index).
-define(MSG_RETAIN_TABLE, emqttd_msg_retain).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% API FUNCTIONS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_link(MsgStoreDir) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [MsgStoreDir], []).

store(RoutingKey, Payload) ->
    store(RoutingKey, Payload, false).

store(RoutingKey, Payload, IsRetain) ->
    MsgId = crypto:hash(md5, term_to_binary(now())),
    store(MsgId, RoutingKey, Payload, IsRetain).

store(undefined, RoutingKey, Payload, IsRetain) ->
    store(RoutingKey, Payload, IsRetain);
store(MsgId, RoutingKey, Payload, IsRetain) ->
    case update_msg_cache(MsgId, {RoutingKey, Payload}) of
        new_cache_item ->
            gen_server:cast(?MODULE, {write, MsgId, IsRetain});
        new_ref_count ->
            ok
    end,
    MsgId.

retrieve(MsgId) ->
    case ets:lookup(emqttd_msg_cache, MsgId) of
        [{_, Msg, _}] -> {ok, Msg};
        [] -> {error, not_found}
    end.

deref(MsgId) ->
    try
        case ets:update_counter(emqttd_msg_cache, MsgId, {3, -1}) of
            0 ->
                ets:delete(emqttd_msg_cache, MsgId),
                gen_server:cast(?MODULE, {delete, MsgId});
            N ->
                {ok, N}
        end
    catch error:badarg -> {error, not_found}
    end.


deliver_from_store(ClientId, ClientPid) ->
    gen_server:call(?MODULE, {deliver, ClientId, ClientPid}).

deliver_retained(ClientPid, Topic, QoS) ->
    Words = emqtt_topic:words(Topic),
    RetainedMessages =
    ets:foldl(fun({RoutingKey, MsgId}, Acc) ->
                      RWords = emqtt_topic:words(RoutingKey),
                      case emqtt_topic:match(RWords, Words) of
                          true ->
                              {ok, {RoutingKey, Payload}} = retrieve(MsgId),
                              [{MsgId, RoutingKey, Payload}|Acc];
                          false ->
                              Acc
                      end
              end, [], ?MSG_RETAIN_TABLE),
    [emqttd_handler_fsm:deliver(ClientPid, RoutingKey, Payload, QoS, true, MsgId)
     || {MsgId, RoutingKey, Payload} <- RetainedMessages],
    ok.

clean_session(ClientId) ->
    gen_server:call(?MODULE, {remove_session, ClientId}),
    ok.

retain_action(_MsgId, RoutingKey, <<>>) ->
    gen_server:call(?MODULE, {reset_retained_msg, RoutingKey});

retain_action(MsgId, RoutingKey, Payload) ->
    NewMsgId = store(MsgId, RoutingKey, Payload, true),
    gen_server:call(?MODULE, {persist_retain_msg, RoutingKey, NewMsgId}).

defer_deliver(ClientId, Qos, MsgId) ->
    ets:insert(?MSG_INDEX_TABLE, {ClientId, Qos, MsgId}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% GEN_SERVER CALLBACKS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init([MsgStoreDir]) ->
    filelib:ensure_dir(MsgStoreDir),
    ets:new(?MSG_INDEX_TABLE, [public, {read_concurrency, true}, named_table, bag]),
    ets:new(?MSG_RETAIN_TABLE, [public, {read_concurrency, true}, named_table]),
    ets:new(emqttd_msg_cache, [public, named_table]),
    MsgStore = bitcask:open(MsgStoreDir, [read_write]),
    ToDelete =
    bitcask:fold(MsgStore,
                 fun
                     (<<?MSG_ITEM, MsgId/binary>> = Key, Val, Acc) ->
                         <<L:16, BRoutingKey:L/binary, Payload/binary>> = Val,
                         RoutingKey = binary_to_list(BRoutingKey),
                         case emqttd_trie:subscriptions(RoutingKey) of
                             [] -> %% weird
                                [Key|Acc];
                             _Subs ->
                                 emqttd_trie:publish(MsgId, RoutingKey, Payload, false),
                                 Acc
                         end;
                     (<<?RETAIN_ITEM, BRoutingKey/binary>> = Key, MsgId, Acc) ->
                         case bitcask:get(MsgStore, <<?MSG_ITEM, MsgId/binary>>) of
                             {ok, <<_:16, BRoutingKey, _/binary>>} ->
                                 RoutingKey = binary_to_list(BRoutingKey),
                                 true = ets:insert(?MSG_RETAIN_TABLE, {RoutingKey, MsgId}),
                                 Acc;
                             E ->
                                 io:format("inconsistency ~p~n", [{MsgId, E}]),
                                 [Key|Acc]
                         end
                 end, []),
    ok = lists:foreach(fun(Key) -> bitcask:delete(MsgStore, Key) end, ToDelete),
    {ok, #state{store=MsgStore}}.





handle_call({persist_retain_msg, RoutingKey, MsgId}, _From, State) ->
    #state{store=MsgStore} = State,
    BRoutingKey = list_to_binary(RoutingKey),
    MsgRef = <<?RETAIN_ITEM, BRoutingKey/binary>>,
    case ets:lookup(?MSG_RETAIN_TABLE, RoutingKey) of
        [{_, MsgId}] ->
            %% same msg already retained
            ok;
        _ ->
            ok = bitcask:put(MsgStore, MsgRef, MsgId),
            true = ets:insert(?MSG_RETAIN_TABLE, {RoutingKey, MsgId})
    end,
    {reply, ok, State};

handle_call({deliver, ClientId, ClientPid}, _From, State) ->
    lists:foreach(fun({_, QoS, MsgId} = Obj) ->
                          {ok, {RoutingKey, Payload}} = retrieve(MsgId),
                          emqttd_handler_fsm:deliver(ClientPid, RoutingKey, Payload, QoS, false, MsgId),
                          ets:delete_object(?MSG_INDEX_TABLE, Obj)
                  end, ets:lookup(?MSG_INDEX_TABLE, ClientId)),
    {reply, ok, State};

handle_call({remove_session, ClientId}, _From, State) ->
    lists:foreach(fun({_, _, MsgId} = Obj) ->
                          true = ets:delete_object(?MSG_INDEX_TABLE, Obj),
                          deref(MsgId)
                  end, ets:lookup(?MSG_INDEX_TABLE, ClientId)),
    {reply, ok, State};

handle_call({reset_retained_msg, RoutingKey}, _From, State) ->
    #state{store=MsgStore} = State,
    case ets:lookup(?MSG_RETAIN_TABLE, RoutingKey) of
        [] ->
            ok;
        [{_, MsgId}] ->
            BRoutingKey = list_to_binary(RoutingKey),
            MsgRef = <<?RETAIN_ITEM, BRoutingKey/binary>>,
            ok = bitcask:delete(MsgStore, MsgRef),
            deref(MsgId),
            true = ets:delete(?MSG_RETAIN_TABLE, RoutingKey)
    end,
    {reply, ok, State}.

handle_cast({write, MsgId}, State) ->
    #state{store=MsgStore} = State,
    [{_, {RoutingKey, Payload}, _}] = ets:lookup(emqttd_msg_cache, MsgId),
    MsgRef = <<?MSG_ITEM, MsgId/binary>>,
    BRoutingKey = list_to_binary(RoutingKey),
    ok = bitcask:put(MsgStore, MsgRef, <<(byte_size(BRoutingKey)):16, BRoutingKey/binary, Payload/binary>>),
    {noreply, State};

handle_cast({delete, MsgId}, State) ->
    #state{store=MsgStore} = State,
    MsgRef = <<?MSG_ITEM, MsgId/binary>>,
    ok = bitcask:delete(MsgStore, MsgRef),
    {noreply, State};




handle_cast(_Req, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    #state{store=MsgStore} = State,
    bitcask:close(MsgStore),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

safe_ets_update_counter(Tab, Key, UpdateOp, SuccessFun, FailThunk) ->
    try
        SuccessFun(ets:update_counter(Tab, Key, UpdateOp))
    catch error:badarg -> FailThunk()
    end.

update_msg_cache(MsgId, Msg) ->
    update_msg_cache(MsgId, Msg, 1).

update_msg_cache(MsgId, Msg, Diff) when Diff >= 1 ->
    case ets:insert_new(emqttd_msg_cache, {MsgId, Msg, Diff}) of
        true -> new_cache_item;
        false ->
            safe_ets_update_counter(
              emqttd_msg_cache, MsgId, {3, +Diff}, fun(_) -> new_ref_count end,
              fun() -> update_msg_cache(MsgId, Msg, Diff) end)
    end.


