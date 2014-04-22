-module(emqttd_msg_store).
-behaviour(gen_server).

-export([start_link/1,
         persist_for_later/3,
         deliver_from_store/2,
         deliver_retained/3,
         get_retained/1,
         clean_session/1,
         remove_session/1,
         persist_retain_msg/2,
         remove_retained_msg/1,
         reset_retained_msg/1
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

persist_for_later([], _, _) -> ok;
persist_for_later(UnroutableClients, RoutingKey, Payload) ->
    gen_server:call(?MODULE, {persist, UnroutableClients, RoutingKey, Payload}).

deliver_from_store(ClientId, ClientPid) ->
    [rpc:call(N, ?MODULE, deliver_from_store, [ClientId, ClientPid]) || N <- nodes()],
    gen_server:call(?MODULE, {deliver, ClientId, ClientPid}).

deliver_retained(ClientPid, Topic, QoS) ->
    RetainedMsgs =
    lists:flatten([get_retained(Topic) |
                   [rpc:call(N, ?MODULE, get_retained, [Topic]) || N <- nodes()]]),
    [emqttd_handler_fsm:deliver(ClientPid, RoutingKey, Payload, QoS, true)
     || {{RoutingKey, _}, Payload} <- remove_older_duplicated(RetainedMsgs)],
    ok.

clean_session(ClientId) ->
    [rpc:call(N, ?MODULE, remove_session, [ClientId]) || N <- nodes()],
    remove_session(1),
    ok.

persist_retain_msg(RoutingKey, Msg) ->
    [rpc:call(Node, ?MODULE, remove_retained_msg, [RoutingKey]) || Node <- nodes()],
    gen_server:call(?MODULE, {persist_retain_msg, RoutingKey, Msg}).

reset_retained_msg(RoutingKey) ->
    [rpc:call(Node, ?MODULE, remove_retained_msg, [RoutingKey]) || Node <- nodes()],
    gen_server:call(?MODULE, {reset_retained_msg, RoutingKey}).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% RPC API FUNCTIONS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_retained(Topic) ->
    %% should only be called through RPC or deliver_retained
    gen_server:call(?MODULE, {get_retained, Topic}).

remove_session(ClientId) ->
    %% should only be called through RPC or clean_session/1
    gen_server:call(?MODULE, {remove_session, ClientId}).

remove_retained_msg(RoutingKey) ->
    %% should only be called through RPC
    gen_server:call(?MODULE, {remove_retained_msg, RoutingKey}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% GEN_SERVER CALLBACKS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init([MsgStoreDir]) ->
    filelib:ensure_dir(MsgStoreDir),
    ets:new(?MSG_INDEX_TABLE, [{read_concurrency, true}, named_table, bag]),
    ets:new(?MSG_RETAIN_TABLE, [{read_concurrency, true}, named_table]),
    MsgStore = bitcask:open(MsgStoreDir, [read_write]),
    bitcask:fold(MsgStore,
                 fun
                     (<<?INDEX_ITEM, IndexRef:16/binary, BClientId/binary>>, <<QoS, MsgRef/binary>>, _) ->
                         true = ets:insert(?MSG_INDEX_TABLE, {{client, binary_to_list(BClientId)}, QoS, MsgRef, IndexRef});
                     (<<?MSG_ITEM, _/binary>>, _, _) ->
                         ok;
                     (<<?RETAIN_ITEM, _/binary>> = MsgRef, V, _) ->
                         {Ts, RoutingKey, Message} = binary_to_term(V),
                         true = ets:insert(?MSG_RETAIN_TABLE, {{retain, RoutingKey}, Ts, MsgRef, crypto:hash(md5, Message)})
                 end, []),
    {ok, #state{store=MsgStore}}.

handle_call({persist, Clients, RoutingKey, Payload}, _From, State) ->
    #state{store=MsgStore} = State,
    MsgRef = <<?MSG_ITEM, (crypto:hash(md5, [RoutingKey, Payload]))/binary>>,
    BRoutingKey = list_to_binary(RoutingKey),
    ok = bitcask:put(MsgStore, MsgRef, <<(byte_size(BRoutingKey)):16, BRoutingKey/binary, Payload/binary>>),
    lists:foreach(fun({ClientId, QoS}) ->
                          BClientId = list_to_binary(ClientId),
                          UniqueId = crypto:hash(md5, [ClientId, term_to_binary(now())]),
                          IndexRef = <<?INDEX_ITEM, UniqueId/binary, BClientId/binary>>,
                          IndexItem = <<QoS, MsgRef/binary>>,
                          ok = bitcask:put(MsgStore, IndexRef, IndexItem),
                          true = ets:insert(?MSG_INDEX_TABLE, {{client, ClientId}, QoS, MsgRef, IndexRef})
                  end, Clients),
    {reply, ok, State};

handle_call({persist_retain_msg, RoutingKey, Message}, _From, State) ->
    #state{store=MsgStore} = State,
    Md5Msg = crypto:hash(md5, Message),
    MsgRef = <<?RETAIN_ITEM, (crypto:hash(md5, RoutingKey))/binary>>,
    case ets:lookup(?MSG_RETAIN_TABLE, {retain, RoutingKey}) of
        [{_, _, MsgRef, Md5Msg}] ->
            %% same msg already retained
            ok;
        _ ->
            Ts = now(),
            ok = bitcask:put(MsgStore, MsgRef, term_to_binary({Ts, RoutingKey, Message})),
            true = ets:insert(?MSG_RETAIN_TABLE, {{retain, RoutingKey}, Ts, MsgRef, Md5Msg})
    end,
    {reply, ok, State};

handle_call({deliver, ClientId, ClientPid}, _From, State) ->
    #state{store=MsgStore} = State,
    lists:foreach(fun({_, QoS, MsgRef, IndexRef} = Obj) ->
                          {ok, <<RLen:16, RoutingKey:RLen/binary,
                                 Payload/binary>>} = bitcask:get(MsgStore, MsgRef),
                          emqttd_handler_fsm:deliver(ClientPid, RoutingKey, Payload, QoS, false),
                          true = ets:delete_object(?MSG_INDEX_TABLE, Obj),
                          ok = bitcask:delete(MsgStore, IndexRef),
                          maybe_delete_from_msg_store(MsgStore, MsgRef)
                  end, ets:lookup(?MSG_INDEX_TABLE, {client, ClientId})),
    {reply, ok, State};

handle_call({get_retained, SubTopic}, _From, State) ->
    #state{store=MsgStore} = State,
    Words = emqtt_topic:words(SubTopic),
    RetainedMessages =
    ets:foldl(fun({{retain, RoutingKey}, _Ts, MsgRef, _Md5Msg}, Acc) ->
                      RWords = emqtt_topic:words(RoutingKey),
                      case emqtt_topic:match(RWords, Words) of
                          true ->
                              {ok, Item} = bitcask:get(MsgStore, MsgRef),
                              {Ts, RoutingKey, Payload} = binary_to_term(Item),
                              [{{RoutingKey, Ts}, Payload}|Acc];
                          false ->
                              Acc
                      end
              end, [], ?MSG_RETAIN_TABLE),
    {reply, RetainedMessages, State};

handle_call({remove_session, ClientId}, _From, State) ->
    #state{store=MsgStore} = State,
    lists:foreach(fun({_, _, MsgRef, IndexRef} = Obj) ->
                          true = ets:delete_object(?MSG_INDEX_TABLE, Obj),
                          ok = bitcask:delete(MsgStore, IndexRef),
                          maybe_delete_from_msg_store(MsgStore, MsgRef)
                  end, ets:lookup(?MSG_INDEX_TABLE, {client, ClientId})),
    {reply, ok, State};

handle_call({reset_retained_msg, RoutingKey}, _From, State) ->
    #state{store=MsgStore} = State,
    case ets:lookup(?MSG_RETAIN_TABLE, {retain, RoutingKey}) of
        [] ->
            ok;
        [{_, MsgRef, _}] ->
            ok = bitcask:delete(MsgStore, MsgRef),
            true = ets:delete(?MSG_RETAIN_TABLE, {retain, RoutingKey})
    end,
    {reply, ok, State};

handle_call({remove_retained_msg, RoutingKey}, _From, State) ->
    #state{store=MsgStore} = State,
    MsgRef = crypto:hash(md5, [RoutingKey]),
    ok = bitcask:delete(MsgStore, MsgRef),
    true = ets:delete(?MSG_RETAIN_TABLE, {retain, RoutingKey}),
    {reply, ok, State}.


handle_cast(_Req, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

maybe_delete_from_msg_store(MsgStore, MsgRef) ->
    case ets:match_object(?MSG_INDEX_TABLE, {'_', '_', MsgRef, '_'}) of
        [] ->
            bitcask:delete(MsgStore, MsgRef);
        _ ->
            ok
    end.

remove_older_duplicated(List) ->
    remove_older_duplicated(lists:reverse(lists:usort(List)), []).
% reversed usorted list
remove_older_duplicated([{{R, T1},_} = Obj, {R, T2} | Rest], Acc) when T1 > T2 ->
    remove_older_duplicated(Rest, [Obj|Acc]);
remove_older_duplicated([Obj|Rest], Acc) ->
    remove_older_duplicated(Rest, [Obj|Acc]);
remove_older_duplicated([], Acc) -> Acc.
