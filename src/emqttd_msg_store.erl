-module(emqttd_msg_store).
-behaviour(gen_server).

-export([start_link/1,
         store/4, store/5,
         in_flight/0,
         retrieve/1,
         deref/1,
         deliver_from_store/2,
         deliver_retained/3,
         clean_session/1,
         retain_action/4,
         defer_deliver/3,
         clean_all/1
         ]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([retain_action_/5]). %% RPC Calls


-record(state, {store}).

-define(MSG_ITEM, 0).
-define(INDEX_ITEM, 1).
-define(RETAIN_ITEM, 2).
-define(MSG_INDEX_TABLE, emqttd_msg_index).
-define(MSG_RETAIN_TABLE, emqttd_msg_retain).
-define(MSG_CACHE_TABLE, emqttd_msg_cache).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% API FUNCTIONS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_link(MsgStoreDir) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [MsgStoreDir], []).

store(User, ClientId, RoutingKey, Payload) ->
    MsgId = crypto:hash(md5, term_to_binary(now())),
    store(User, ClientId, MsgId, RoutingKey, Payload).

store(User, ClientId, undefined, RoutingKey, Payload) ->
    store(User, ClientId, RoutingKey, Payload);
store(User, ClientId, MsgId, RoutingKey, Payload) ->
    ets:update_counter(?MSG_CACHE_TABLE, in_flight, {2, 1}),
    case update_msg_cache(MsgId, {ensure_list(RoutingKey), Payload}) of
        new_cache_item ->
            MsgRef = <<?MSG_ITEM, MsgId/binary>>,
            Val = term_to_binary({User, ClientId, RoutingKey, Payload}),
            gen_server:cast(?MODULE, {write, MsgRef, Val});
        new_ref_count ->
            ok
    end,
    MsgId.

in_flight() ->
    [{in_flight, InFlight}] = ets:lookup(?MSG_CACHE_TABLE, in_flight),
    NrOfDeferedMsgs = ets:info(?MSG_INDEX_TABLE, size),
    InFlight - NrOfDeferedMsgs.

retrieve(MsgId) ->
    case ets:lookup(?MSG_CACHE_TABLE, MsgId) of
        [{_, Msg, _}] -> {ok, Msg};
        [] -> {error, not_found}
    end.

deref(MsgId) ->
    try
        ets:update_counter(?MSG_CACHE_TABLE, in_flight, {2, -1}),
        case ets:update_counter(?MSG_CACHE_TABLE, MsgId, {3, -1}) of
            0 ->
                ets:delete(?MSG_CACHE_TABLE, MsgId),
                MsgRef = <<?MSG_ITEM, MsgId/binary>>,
                gen_server:cast(?MODULE, {delete, MsgRef});
            N ->
                {ok, N}
        end
    catch error:badarg -> {error, not_found}
    end.


deliver_from_store(ClientId, ClientPid) ->
    lists:foreach(fun({_, QoS, MsgId} = Obj) ->
                          case retrieve(MsgId) of
                              {ok, {RoutingKey, Payload}} ->
                                  emqttd_fsm:deliver(ClientPid, RoutingKey,
                                                     Payload, QoS,
                                                     false, MsgId);
                              {error, not_found} ->
                                  %% TODO: this happens,, ??
                                  ok
                          end,
                          ets:delete_object(?MSG_INDEX_TABLE, Obj)
                  end, ets:lookup(?MSG_INDEX_TABLE, ClientId)).

deliver_retained(ClientPid, Topic, QoS) ->
    Words = emqtt_topic:words(Topic),
    ets:foldl(fun({RoutingKey, User, ClientId, Payload, _}, Acc) ->
                      RWords = emqtt_topic:words(RoutingKey),
                      case emqtt_topic:match(RWords, Words) of
                          true ->
                              MsgId = store(User, ClientId,
                                            RoutingKey, Payload),
                              emqttd_fsm:deliver(ClientPid, RoutingKey,
                                                 Payload, QoS, true, MsgId);
                          false ->
                              Acc
                      end
              end, [], ?MSG_RETAIN_TABLE),
    ok.

clean_session(ClientId) ->
    lists:foreach(fun({_, _, MsgId} = Obj) ->
                          true = ets:delete_object(?MSG_INDEX_TABLE, Obj),
                          deref(MsgId)
                  end, ets:lookup(?MSG_INDEX_TABLE, ClientId)).

retain_action(User, ClientId, RoutingKey, Payload) ->
    Nodes = emqttd_cluster:nodes(),
    Now = now(),
    lists:foreach(fun(Node) when Node == node() ->
                          retain_action_(User, ClientId, Now,
                                         RoutingKey, Payload);
                     (Node) ->
                          rpc:call(Node, ?MODULE, retain_action_,
                                   [User, ClientId, Now, RoutingKey, Payload])
                  end, Nodes).

retain_action_(_User, _ClientId, TS, RoutingKey, <<>>) ->
    %% retain-delete action
    case ets:lookup(?MSG_RETAIN_TABLE, RoutingKey) of
        [{_, _, _, _, TSOld}] when TS > TSOld ->
            BRoutingKey = list_to_binary(RoutingKey),
            MsgRef = <<?RETAIN_ITEM, BRoutingKey/binary>>,
            gen_server:cast(?MODULE, {delete, MsgRef}),
            true = ets:delete(?MSG_RETAIN_TABLE, RoutingKey),
            ok;
        _ ->
            %% the retain-delete action is older than
            %% the stored retain message --> ignore
            ok
    end;
retain_action_(User, ClientId, TS, RoutingKey, Payload) ->
    %% retain-insert action
    BRoutingKey = list_to_binary(RoutingKey),
    MsgRef = <<?RETAIN_ITEM, BRoutingKey/binary>>,
    case ets:lookup(?MSG_RETAIN_TABLE, RoutingKey) of
        [] ->
            gen_server:cast(?MODULE, {write, MsgRef,
                                      term_to_binary({User, ClientId,
                                                      Payload, TS})}),
            true = ets:insert(?MSG_RETAIN_TABLE, {RoutingKey, User,
                                                  ClientId, Payload, TS}),
            ok;
        [{_, _, _, _, TSOld}] when TS > TSOld ->
            %% in case of a race between retain-insert actions
            gen_server:cast(?MODULE, {write, MsgRef,
                                      term_to_binary({User, ClientId,
                                                      Payload, TS})}),
            true = ets:insert(?MSG_RETAIN_TABLE, {RoutingKey, User,
                                                  ClientId, Payload, TS}),
            ok;
        _ ->
            ok
    end.

defer_deliver(ClientId, Qos, MsgId) ->
    ets:insert(?MSG_INDEX_TABLE, {ClientId, Qos, MsgId}).


clean_all([]) ->
    %% called using emqttd-admin, mainly for test purposes
    %% you don't want to call this during production
    clean_retain(),
    clean_cache(),
    clean_index().

clean_retain() ->
    clean_retain(ets:last(?MSG_RETAIN_TABLE)).
clean_retain('$end_of_table') -> ok;
clean_retain(RoutingKey) ->
    BRoutingKey = list_to_binary(RoutingKey),
    MsgRef = <<?RETAIN_ITEM, BRoutingKey/binary>>,
    gen_server:cast(?MODULE, {delete, MsgRef}),
    true = ets:delete(?MSG_RETAIN_TABLE, RoutingKey),
    clean_retain(ets:last(?MSG_RETAIN_TABLE)).

clean_cache() ->
    clean_cache(ets:last(?MSG_CACHE_TABLE)).
clean_cache('$end_of_table') ->
    ets:insert(?MSG_CACHE_TABLE, {in_flight, 0}),
    ok;
clean_cache(in_flight) ->
    ets:delete(?MSG_CACHE_TABLE, in_flight),
    clean_cache(ets:last(?MSG_CACHE_TABLE));
clean_cache(MsgId) ->
    MsgRef = <<?MSG_ITEM, MsgId/binary>>,
    gen_server:cast(?MODULE, {delete, MsgRef}),
    true = ets:delete(?MSG_CACHE_TABLE, MsgId),
    clean_cache(ets:last(?MSG_CACHE_TABLE)).

clean_index() ->
    ets:delete_all_objects(?MSG_INDEX_TABLE).




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% GEN_SERVER CALLBACKS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init([MsgStoreDir]) ->
    filelib:ensure_dir(MsgStoreDir),
    TableOpts = [public, named_table,
                 {read_concurrency, true},
                 {write_concurrency, true}],
    ets:new(?MSG_INDEX_TABLE, [bag|TableOpts]),
    ets:new(?MSG_RETAIN_TABLE, TableOpts),
    ets:new(?MSG_CACHE_TABLE, TableOpts),
    ets:insert(?MSG_CACHE_TABLE, {in_flight, 0}),
    MsgStore = bitcask:open(MsgStoreDir, [read_write]),
    ToDelete =
    bitcask:fold(MsgStore,
                 fun
                     (<<?MSG_ITEM, MsgId/binary>> = Key, Val, Acc) ->
                         {User, ClientId,
                          RoutingKey, Payload} = binary_to_term(Val),
                         case emqttd_reg:subscriptions(RoutingKey) of
                             [] -> %% weird
                                [Key|Acc];
                             _Subs ->
                                 emqttd_reg:publish(User, ClientId, MsgId,
                                                    RoutingKey, Payload, false),
                                 Acc
                         end;
                     (<<?RETAIN_ITEM, BRoutingKey/binary>>, Val, Acc) ->
                         {User, ClientId, Payload, TS} = binary_to_term(Val),
                         true = ets:insert(?MSG_RETAIN_TABLE,
                                           {ensure_list(BRoutingKey), User,
                                            ClientId, Payload, TS}),
                         Acc
                 end, []),
    ok = lists:foreach(fun(Key) -> bitcask:delete(MsgStore, Key) end, ToDelete),
    {ok, #state{store=MsgStore}}.

handle_call(_Req, _From, State) ->
    {reply, {error, not_implemented}, State}.

handle_cast({write, Key, Val}, State) ->
    #state{store=MsgStore} = State,
    ok = bitcask:put(MsgStore, Key, Val),
    {noreply, State};

handle_cast({delete, MsgRef}, State) ->
    #state{store=MsgStore} = State,
    ok = bitcask:delete(MsgStore, MsgRef),
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
    case ets:insert_new(?MSG_CACHE_TABLE, {MsgId, Msg, Diff}) of
        true -> new_cache_item;
        false ->
            safe_ets_update_counter(
              ?MSG_CACHE_TABLE, MsgId, {3, +Diff}, fun(_) -> new_ref_count end,
              fun() -> update_msg_cache(MsgId, Msg, Diff) end)
    end.



ensure_list(B) when is_binary(B) -> binary_to_list(B);
ensure_list(L) when is_list(L) -> L.
