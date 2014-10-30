%% Copyright 2014 Erlio GmbH Basel Switzerland (http://erl.io)
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

-module(vmq_msg_store).
-behaviour(gen_server).
-include("vmq_server.hrl").

-export([start_link/0,
         store/3, store/4,
         in_flight/0,
         stored/0,
         retrieve/1,
         deref/1,
         deref_multi/1,
         deliver_from_store/2,
         clean_session/1,
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

-record(state, {store}).
-type state() :: #state{}.

-callback open(Args :: term()) -> {ok, term()} | {error, term()}.
-callback fold(term(), fun((binary(), 
                            binary(), any()) -> any()), 
                          any()) -> any() | {error, any()}.
-callback delete(term(), binary()) -> ok.
-callback insert(term(), binary(), binary()) -> ok | {error, any()}.
-callback close(term()) -> ok.

-define(MSG_ITEM, 0).
-define(INDEX_ITEM, 1).
-define(MSG_INDEX_TABLE, vmq_msg_index).
-define(MSG_CACHE_TABLE, vmq_msg_cache).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% API FUNCTIONS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec start_link() -> 'ignore' | {'error',_} | {'ok',pid()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec store(client_id(), routing_key(), payload()) -> msg_ref().
store(ClientId, RoutingKey, Payload) ->
    MsgRef = crypto:hash(md5, term_to_binary(os:timestamp())),
    store(ClientId, MsgRef, RoutingKey, Payload).

-spec store(client_id(), undefined | msg_ref(),
            routing_key(), payload()) -> msg_ref().
store(ClientId, undefined, RoutingKey, Payload) ->
    store(ClientId, RoutingKey, Payload);
store(ClientId, MsgRef, RoutingKey, Payload) when is_binary(MsgRef) ->
    ets:update_counter(?MSG_CACHE_TABLE, in_flight, {2, 1}),
    case update_msg_cache(MsgRef, {RoutingKey, Payload}) of
        new_cache_item ->
            MsgRef1 = <<?MSG_ITEM, MsgRef/binary>>,
            Val = term_to_binary({ClientId, RoutingKey, Payload}),
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

-spec stored() -> non_neg_integer().
stored() ->
    ets:info(?MSG_CACHE_TABLE, size) -1.



-spec retrieve(msg_ref()) -> 
                      {'error','not_found'} | 
                      {'ok', {routing_key(), payload()}}.
retrieve(MsgRef) ->
    case ets:lookup(?MSG_CACHE_TABLE, MsgRef) of
        [{_, Msg, _}] -> {ok, Msg};
        [] -> {error, not_found}
    end.

-spec deref(msg_ref()) -> 
                   'ok' | {'error','not_found'} | 
                   {'ok',pos_integer()}.
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

-spec deref_multi([msg_ref()]) -> [any()].
deref_multi(MsgRefs) ->
    _ = [deref(MsgRef) || MsgRef <- MsgRefs].


-spec deliver_from_store(client_id(), pid()) -> 'ok'.
deliver_from_store(ClientId, Pid) ->
    NrOfDeliveredMsgs =
    lists:foldl(fun ({_, {uncached, Term}} = Obj, Acc) ->
                          Pid ! {ClientId, {deliver, Term}},
                          ets:delete_object(?MSG_INDEX_TABLE, Obj),
                          Acc + 1;
                      ({_, QoS, MsgRef, DeliverAsDup} = Obj, Acc) ->
                        NewAcc =
                        case retrieve(MsgRef) of
                            {ok, {RoutingKey, Payload}} ->
                                Pid ! {ClientId, 
                                       {deliver, 
                                        RoutingKey, 
                                        Payload, QoS, 
                                        DeliverAsDup, MsgRef}},
                                Acc + 1;
                            {error, not_found} ->
                                %% TODO: this happens,, ??
                                Acc
                        end,
                        ets:delete_object(?MSG_INDEX_TABLE, Obj),
                        NewAcc
                end, 0, ets:lookup(?MSG_INDEX_TABLE, ClientId)),
    Pid ! {all_delivered, NrOfDeliveredMsgs, node(), ClientId},
    ok.

-spec clean_session(client_id()) -> 'ok'.
clean_session(ClientId) ->
    lists:foreach(fun ({_, {uncached, _}}) ->
                          ok;
                      ({_, _, MsgRef, _} = Obj) ->
                          true = ets:delete_object(?MSG_INDEX_TABLE, Obj),
                          deref(MsgRef)
                  end, ets:lookup(?MSG_INDEX_TABLE, ClientId)).

-spec defer_deliver_uncached(client_id(), any()) -> 'true'.
defer_deliver_uncached(ClientId, Term) ->
    ets:insert(?MSG_INDEX_TABLE, {ClientId, {uncached, Term}}).

-spec defer_deliver(client_id(), qos(), msg_ref()) -> 'true'.
defer_deliver(ClientId, Qos, MsgRef) ->
    defer_deliver(ClientId, Qos, MsgRef, false).

-spec defer_deliver(client_id(), qos(), msg_ref(), boolean()) -> 'true'.
defer_deliver(ClientId, Qos, MsgRef, DeliverAsDup) ->
    ets:insert(?MSG_INDEX_TABLE, {ClientId, Qos, MsgRef, DeliverAsDup}).


-spec clean_all([]) -> 'true'.
clean_all([]) ->
    %% called using vmq-admin, mainly for test purposes
    %% you don't want to call this during production
    clean_cache(),
    clean_index().

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



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% GEN_SERVER CALLBACKS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec init([string()]) -> {'ok', state()}.
init([]) ->
    {MsgStoreImpl, 
     MsgStoreImplArgs} = 
        vmq_config:get_env(msg_store, {vmq_null_store, []}),
    TableOpts = [public, named_table,
                 {read_concurrency, true},
                 {write_concurrency, true}],
    ets:new(?MSG_INDEX_TABLE, [bag|TableOpts]),
    ets:new(?MSG_CACHE_TABLE, TableOpts),
    ets:insert(?MSG_CACHE_TABLE, {in_flight, 0}),
    {ok, MsgStore} = MsgStoreImpl:open(MsgStoreImplArgs),
    ToDelete =
    MsgStoreImpl:fold(MsgStore,
                 fun
                     (<<?MSG_ITEM, MsgRef/binary>> = Key, Val, Acc) ->
                         {_, RoutingKey, Payload} = binary_to_term(Val),
                         update_subs_(RoutingKey, MsgRef, Payload, Key, Acc)
                 end, []),
    ok = lists:foreach(fun(Key) -> 
                               MsgStoreImpl:delete(MsgStore, Key) 
                       end, ToDelete),
    {ok, #state{store={MsgStoreImpl, MsgStore}}}.

update_subs_(RoutingKey, MsgRef, Payload, Key, Acc) ->
    case vmq_reg:subscriptions(RoutingKey) of
        [] -> %% weird
            [Key|Acc];
        Subs ->
            lists:foreach(
              fun({ClientId, QoS}) ->
                      %% Increment in_flight counter
                      ets:update_counter(
                        ?MSG_CACHE_TABLE,
                        in_flight, {2, 1}),
                      %% add to cache
                      update_msg_cache(
                        MsgRef, 
                        {RoutingKey, Payload}),
                      %% defer deliver expects the Message
                      %% to be already cached and just adds
                      %% the proper index item
                      defer_deliver(ClientId, QoS, 
                                    MsgRef, true)
              end, Subs),
            Acc
    end.

-spec handle_call(_, _, _) -> {'reply', 
                               ok | {'error', 'not_implemented'}, _}.
handle_call({delete, MsgRef}, _From, State) ->
    %% Synchronized Delete, clean_all 
    %% (for testing purposes) is currently the only user
    #state{store={MsgStoreImpl, MsgStore}} = State,
    ok = MsgStoreImpl:delete(MsgStore, MsgRef),
    {reply, ok, State};
handle_call({write, Key, Val}, _From, State) ->
    #state{store={MsgStoreImpl, MsgStore}} = State,
    ok = MsgStoreImpl:insert(MsgStore, Key, Val),
    {reply, ok, State};

handle_call(_Req, _From, State) ->
    {reply, {error, not_implemented}, State}.

-spec handle_cast({'delete',msg_ref()} | 
                  {'write',binary(),binary()}, 
                  state()) -> {'noreply', state()}.
handle_cast({write, Key, Val}, State) ->
    #state{store={MsgStoreImpl, MsgStore}} = State,
    ok = MsgStoreImpl:insert(MsgStore, Key, Val),
    {noreply, State};

handle_cast({delete, MsgRef}, State) ->
    #state{store={MsgStoreImpl, MsgStore}} = State,
    ok = MsgStoreImpl:delete(MsgStore, MsgRef),
    {noreply, State}.

-spec handle_info(_, _) -> {'noreply', _}.
handle_info(_Info, State) ->
    {noreply, State}.

-spec terminate(_, state()) -> 'ok'.
terminate(_Reason, State) ->
    #state{store={MsgStoreImpl, MsgStore}} = State,
    MsgStoreImpl:close(MsgStore),
    ok.

-spec code_change(_, _, _) -> {'ok', _}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-spec safe_ets_update_counter('vmq_msg_cache', _, {3, 1}, 
                              fun((_) -> 'new_ref_count'), 
                              fun(() -> 'new_cache_item' | 
                                        'new_ref_count')) -> 
                                     'new_cache_item' | 
                                     'new_ref_count'.
safe_ets_update_counter(Tab, Key, UpdateOp, SuccessFun, FailThunk) ->
    try
        SuccessFun(ets:update_counter(Tab, Key, UpdateOp))
    catch error:badarg -> FailThunk()
    end.

-spec update_msg_cache(msg_ref(), {routing_key(), payload()}) -> 
                              'new_cache_item' | 'new_ref_count'.
update_msg_cache(MsgRef, Msg) ->
    case ets:insert_new(?MSG_CACHE_TABLE, {MsgRef, Msg, 1}) of
        true -> new_cache_item;
        false ->
            safe_ets_update_counter(
              ?MSG_CACHE_TABLE, MsgRef, {3, +1}, fun(_) -> new_ref_count end,
              fun() -> update_msg_cache(MsgRef, Msg) end)
    end.
