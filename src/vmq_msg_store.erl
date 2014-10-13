-module(vmq_msg_store).
-behaviour(gen_server).
-include_lib("emqtt_commons/include/types.hrl").

-export([start_link/0,
         store/4, store/5,
         in_flight/0,
         retained/0,
         stored/0,
         retrieve/1,
         deref/1,
         deliver_from_store/2,
         deliver_retained/3,
         clean_session/1,
         retain_action/4,
         defer_deliver/3,
         defer_deliver/4,
         defer_deliver_uncached/2,
         clean_all/1
         ]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([retain_action_/5]). %% RPC Calls

-export([behaviour_info/1]).

-record(state, {store}).

-define(MSG_ITEM, 0).
-define(INDEX_ITEM, 1).
-define(RETAIN_ITEM, 2).
-define(MSG_INDEX_TABLE, vmq_msg_index).
-define(MSG_RETAIN_TABLE, vmq_msg_retain).
-define(MSG_CACHE_TABLE, vmq_msg_cache).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% API FUNCTIONS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec start_link() -> 'ignore' | {'error',_} | {'ok',pid()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec store(username(),client_id(),routing_key(),payload()) -> msg_ref().
store(User, ClientId, RoutingKey, Payload) ->
    MsgRef = crypto:hash(md5, term_to_binary(os:timestamp())),
    store(User, ClientId, MsgRef, RoutingKey, Payload).

-spec store(username(),client_id(),undefined | msg_ref(),routing_key(),payload()) -> msg_ref().
store(User, ClientId, undefined, RoutingKey, Payload) ->
    store(User, ClientId, RoutingKey, Payload);
store(User, ClientId, MsgRef, RoutingKey, Payload) when is_binary(MsgRef) ->
    ets:update_counter(?MSG_CACHE_TABLE, in_flight, {2, 1}),
    case update_msg_cache(MsgRef, {RoutingKey, Payload}) of
        new_cache_item ->
            MsgRef1 = <<?MSG_ITEM, MsgRef/binary>>,
            Val = term_to_binary({User, ClientId, RoutingKey, Payload}),
            gen_server:call(?MODULE, {write, MsgRef1, Val}, infinity);
        new_ref_count ->
            ok
    end,
    MsgRef.

-spec in_flight() -> non_neg_integer().
in_flight() ->
    [{in_flight, InFlight}] = ets:lookup(?MSG_CACHE_TABLE, in_flight),
    NrOfDeferedMsgs = ets:info(?MSG_INDEX_TABLE, size),
    InFlight - NrOfDeferedMsgs.

-spec retained() -> non_neg_integer().
retained() ->
    ets:info(?MSG_RETAIN_TABLE, size).

-spec stored() -> non_neg_integer().
stored() ->
    ets:info(?MSG_CACHE_TABLE, size) -1.



-spec retrieve(msg_ref()) -> {'error','not_found'} | {'ok', {routing_key(), payload()}}.
retrieve(MsgRef) ->
    case ets:lookup(?MSG_CACHE_TABLE, MsgRef) of
        [{_, Msg, _}] -> {ok, Msg};
        [] -> {error, not_found}
    end.

-spec deref(msg_ref()) -> 'ok' | {'error','not_found'} | {'ok',pos_integer()}.
deref(MsgRef) ->
    try
        ets:update_counter(?MSG_CACHE_TABLE, in_flight, {2, -1}),
        case ets:update_counter(?MSG_CACHE_TABLE, MsgRef, {3, -1}) of
            0 ->
                ets:delete(?MSG_CACHE_TABLE, MsgRef),
                MsgRef1 = <<?MSG_ITEM, MsgRef/binary>>,
                gen_server:cast(?MODULE, {delete, MsgRef1});
            N ->
                {ok, N}
        end
    catch error:badarg -> {error, not_found}
    end.


-spec deliver_from_store(client_id(),pid()) -> 'ok'.
deliver_from_store(ClientId, ClientPid) ->
    lists:foreach(fun ({_, {uncached, Term}} = Obj) ->
                          vmq_session:deliver_bin(ClientPid, Term),
                          ets:delete_object(?MSG_INDEX_TABLE, Obj);
                      ({_, QoS, MsgRef, DeliverAsDup} = Obj) ->
                          case retrieve(MsgRef) of
                              {ok, {RoutingKey, Payload}} ->
                                  vmq_session:deliver(ClientPid, RoutingKey,
                                                     Payload, QoS,
                                                     false, DeliverAsDup, MsgRef);
                              {error, not_found} ->
                                  %% TODO: this happens,, ??
                                  ok
                          end,
                          ets:delete_object(?MSG_INDEX_TABLE, Obj)
                  end, ets:lookup(?MSG_INDEX_TABLE, ClientId)).

-spec deliver_retained(pid(),topic(),qos()) -> 'ok'.
deliver_retained(ClientPid, Topic, QoS) ->
    Words = emqtt_topic:words(Topic),
    ets:foldl(fun({RoutingKey, User, ClientId, Payload, _}, Acc) ->
                      RWords = emqtt_topic:words(RoutingKey),
                      case emqtt_topic:match(RWords, Words) of
                          true ->
                              MsgRef = store(User, ClientId,
                                            RoutingKey, Payload),
                              vmq_session:deliver(ClientPid, RoutingKey,
                                                 Payload, QoS, true, false, MsgRef);
                          false ->
                              Acc
                      end
              end, [], ?MSG_RETAIN_TABLE),
    ok.

-spec clean_session(client_id()) -> 'ok'.
clean_session(ClientId) ->
    lists:foreach(fun ({_, {uncached, _}}) ->
                          ok;
                      ({_, _, MsgRef, _} = Obj) ->
                          true = ets:delete_object(?MSG_INDEX_TABLE, Obj),
                          deref(MsgRef)
                  end, ets:lookup(?MSG_INDEX_TABLE, ClientId)).

-spec retain_action(username(),client_id(),routing_key(),payload()) -> 'ok'.
retain_action(User, ClientId, RoutingKey, Payload) ->
    Nodes = vmq_cluster:nodes(),
    Now = os:timestamp(),
    lists:foreach(fun(Node) when Node == node() ->
                          retain_action_(User, ClientId, Now,
                                         RoutingKey, Payload);
                     (Node) ->
                          rpc:call(Node, ?MODULE, retain_action_,
                                   [User, ClientId, Now, RoutingKey, Payload])
                  end, Nodes).

-spec retain_action_(username(),client_id(),erlang:timestamp(),routing_key(),payload()) -> 'ok'.
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
            gen_server:call(?MODULE, {write, MsgRef,
                                      term_to_binary({User, ClientId,
                                                      Payload, TS})}, infinity),
            true = ets:insert(?MSG_RETAIN_TABLE, {RoutingKey, User,
                                                  ClientId, Payload, TS}),
            ok;
        [{_, _, _, _, TSOld}] when TS > TSOld ->
            %% in case of a race between retain-insert actions
            gen_server:call(?MODULE, {write, MsgRef,
                                      term_to_binary({User, ClientId,
                                                      Payload, TS})}, infinity),
            true = ets:insert(?MSG_RETAIN_TABLE, {RoutingKey, User,
                                                  ClientId, Payload, TS}),
            ok;
        _ ->
            ok
    end.

-spec defer_deliver_uncached(client_id(),any()) -> 'true'.
defer_deliver_uncached(ClientId, Term) ->
    ets:insert(?MSG_INDEX_TABLE, {ClientId, {uncached, Term}}).

-spec defer_deliver(client_id(),qos(),msg_ref()) -> 'true'.
defer_deliver(ClientId, Qos, MsgRef) ->
    defer_deliver(ClientId, Qos, MsgRef, false).

-spec defer_deliver(client_id(),qos(),msg_ref(),boolean()) -> 'true'.
defer_deliver(ClientId, Qos, MsgRef, DeliverAsDup) ->
    ets:insert(?MSG_INDEX_TABLE, {ClientId, Qos, MsgRef, DeliverAsDup}).


-spec clean_all([]) -> 'true'.
clean_all([]) ->
    %% called using vmq-admin, mainly for test purposes
    %% you don't want to call this during production
    clean_retain(),
    clean_cache(),
    clean_index().

-spec clean_retain() -> 'ok'.
clean_retain() ->
    clean_retain(ets:last(?MSG_RETAIN_TABLE)).

-spec clean_retain('$end_of_table' | routing_key()) -> ok.
clean_retain('$end_of_table') -> ok;
clean_retain(RoutingKey) ->
    BRoutingKey = list_to_binary(RoutingKey),
    MsgRef = <<?RETAIN_ITEM, BRoutingKey/binary>>,
    gen_server:call(?MODULE, {delete, MsgRef}, infinity),
    true = ets:delete(?MSG_RETAIN_TABLE, RoutingKey),
    clean_retain(ets:last(?MSG_RETAIN_TABLE)).

-spec clean_cache() -> 'ok'.
clean_cache() ->
    clean_cache(ets:last(?MSG_CACHE_TABLE)).

-spec clean_cache('$end_of_table' | 'in_flight' | msg_ref()) -> 'ok'.
clean_cache('$end_of_table') ->
    ets:insert(?MSG_CACHE_TABLE, {in_flight, 0}),
    ok;
clean_cache(in_flight) ->
    ets:delete(?MSG_CACHE_TABLE, in_flight),
    clean_cache(ets:last(?MSG_CACHE_TABLE));
clean_cache(MsgRef) ->
    MsgRef1 = <<?MSG_ITEM, MsgRef/binary>>,
    gen_server:call(?MODULE, {delete, MsgRef1}, infinity),
    true = ets:delete(?MSG_CACHE_TABLE, MsgRef),
    clean_cache(ets:last(?MSG_CACHE_TABLE)).

-spec clean_index() -> 'true'.
clean_index() ->
    ets:delete_all_objects(?MSG_INDEX_TABLE).


behaviour_info(callbacks) ->
    [{open, 1},
     {fold,3},
     {delete,2},
     {insert,3},
     {close,1}];
behaviour_info(_Other) ->
    undefined.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% GEN_SERVER CALLBACKS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec init([string()]) -> {'ok',#state{store::{atom(), reference()}}}.
init([]) ->
    {MsgStoreImpl, MsgStoreImplArgs} = vmq_config:get_env(msg_store, {vmq_ets_store, []}),
    TableOpts = [public, named_table,
                 {read_concurrency, true},
                 {write_concurrency, true}],
    ets:new(?MSG_INDEX_TABLE, [bag|TableOpts]),
    ets:new(?MSG_RETAIN_TABLE, TableOpts),
    ets:new(?MSG_CACHE_TABLE, TableOpts),
    ets:insert(?MSG_CACHE_TABLE, {in_flight, 0}),
    MsgStore = MsgStoreImpl:open(MsgStoreImplArgs),
    ToDelete =
    MsgStoreImpl:fold(MsgStore,
                 fun
                     (<<?MSG_ITEM, MsgRef/binary>> = Key, Val, Acc) ->
                         {User, ClientId,
                          RoutingKey, Payload} = binary_to_term(Val),
                         case vmq_reg:subscriptions(RoutingKey) of
                             [] -> %% weird
                                [Key|Acc];
                             _Subs ->
                                 vmq_reg:publish(User, ClientId, MsgRef,
                                                    RoutingKey, Payload, false),
                                 Acc
                         end;
                     (<<?RETAIN_ITEM, BRoutingKey/binary>>, Val, Acc) ->
                         {User, ClientId, Payload, TS} = binary_to_term(Val),
                         true = ets:insert(?MSG_RETAIN_TABLE,
                                           {binary_to_list(BRoutingKey), User,
                                            ClientId, Payload, TS}),
                         Acc
                 end, []),
    ok = lists:foreach(fun(Key) -> MsgStoreImpl:delete(MsgStore, Key) end, ToDelete),
    {ok, #state{store={MsgStoreImpl, MsgStore}}}.

-spec handle_call(_,_,_) -> {'reply',ok | {'error','not_implemented'},_}.
handle_call({delete, MsgRef}, _From, State) ->
    %% Synchronized Delete, clean_all (for testing purposes) is currently the only user
    #state{store={MsgStoreImpl, MsgStore}} = State,
    ok = MsgStoreImpl:delete(MsgStore, MsgRef),
    {reply, ok, State};
handle_call({write, Key, Val}, _From, State) ->
    #state{store={MsgStoreImpl, MsgStore}} = State,
    ok = MsgStoreImpl:insert(MsgStore, Key, Val),
    {reply, ok, State};

handle_call(_Req, _From, State) ->
    {reply, {error, not_implemented}, State}.

-spec handle_cast({'delete',msg_ref()} | {'write',binary(),binary()}, #state{}) -> {'noreply',#state{}}.
handle_cast({write, Key, Val}, State) ->
    #state{store={MsgStoreImpl, MsgStore}} = State,
    ok = MsgStoreImpl:insert(MsgStore, Key, Val),
    {noreply, State};

handle_cast({delete, MsgRef}, State) ->
    #state{store={MsgStoreImpl, MsgStore}} = State,
    ok = MsgStoreImpl:delete(MsgStore, MsgRef),
    {noreply, State}.

-spec handle_info(_,_) -> {'noreply',_}.
handle_info(_Info, State) ->
    {noreply, State}.

-spec terminate(_,#state{}) -> 'ok'.
terminate(_Reason, State) ->
    #state{store={MsgStoreImpl, MsgStore}} = State,
    MsgStoreImpl:close(MsgStore),
    ok.

-spec code_change(_,_,_) -> {'ok',_}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-spec safe_ets_update_counter('vmq_msg_cache',_,{3,1},fun((_) -> 'new_ref_count'),fun(() -> 'new_cache_item' | 'new_ref_count')) -> 'new_cache_item' | 'new_ref_count'.
safe_ets_update_counter(Tab, Key, UpdateOp, SuccessFun, FailThunk) ->
    try
        SuccessFun(ets:update_counter(Tab, Key, UpdateOp))
    catch error:badarg -> FailThunk()
    end.

-spec update_msg_cache(msg_ref(),{routing_key(),payload()}) -> 'new_cache_item' | 'new_ref_count'.
update_msg_cache(MsgRef, Msg) ->
    case ets:insert_new(?MSG_CACHE_TABLE, {MsgRef, Msg, 1}) of
        true -> new_cache_item;
        false ->
            safe_ets_update_counter(
              ?MSG_CACHE_TABLE, MsgRef, {3, +1}, fun(_) -> new_ref_count end,
              fun() -> update_msg_cache(MsgRef, Msg) end)
    end.
