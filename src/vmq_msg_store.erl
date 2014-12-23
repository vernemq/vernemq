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
         store/2,
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

-export([msg_store_init/1]).

-record(state, {}).
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

-spec store(client_id(), msg()) -> msg().
store(ClientId, #vmq_msg{msg_ref=undefined} = Msg) ->
    store(ClientId, Msg#vmq_msg{msg_ref=msg_ref()});
store(ClientId, Msg) ->
    #vmq_msg{msg_ref=MsgRef, routing_key=RoutingKey,
             payload=Payload} = Msg,
    ets:update_counter(?MSG_CACHE_TABLE, in_flight, {2, 1}),
    case update_msg_cache(MsgRef, {RoutingKey, Payload}) of
        new_cache_item ->
            MsgRef1 = <<?MSG_ITEM, MsgRef/binary>>,
            Val = term_to_binary({ClientId, RoutingKey, Payload}),
            vmq_plugin:only(msg_store_write_sync, [MsgRef1, Val]);
        new_ref_count ->
            ok
    end,
    Msg.

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

-spec deref(msg_ref()|{{pid(), atom()}, msg_ref()}) ->
                   'ok' | {'error','not_found'} |
                   {'ok',pos_integer()}.
deref({{SessionProxy, Node}, MsgRef}) ->
    rpc:call(Node, ?MODULE, deref, [MsgRef]),
    vmq_session_proxy:derefed(SessionProxy, MsgRef);
deref(MsgRef) ->
    try
        ets:update_counter(?MSG_CACHE_TABLE, in_flight, {2, -1}),
        case ets:update_counter(?MSG_CACHE_TABLE, MsgRef, {3, -1}) of
            0 ->
                ets:delete(?MSG_CACHE_TABLE, MsgRef),
                MsgRef1 = <<?MSG_ITEM, MsgRef/binary>>,
                vmq_plugin:only(msg_store_delete_async, [MsgRef1]);
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
    deliver_from_store_(Pid, ets:match_object(
                               ?MSG_INDEX_TABLE, {ClientId, '$1'}, 1)).

deliver_from_store_(Pid, {[{_, {uncached, Term}} = Obj], Cont}) ->
    vmq_session_proxy:deliver(Pid, Term),
    ets:delete_object(?MSG_INDEX_TABLE, Obj),
    deliver_from_store_(Pid, ets:match_object(Cont));
deliver_from_store_(Pid, {[{_, {QoS, MsgRef, Dup}} = Obj], Cont}) ->
    case retrieve(MsgRef) of
        {ok, {RoutingKey, Payload}} ->
            Term = {RoutingKey, Payload, QoS, Dup, MsgRef},
            vmq_session_proxy:deliver(Pid, Term);
        {error, not_found} ->
            %% TODO: this happens,, ??
            ignore
    end,
    ets:delete_object(?MSG_INDEX_TABLE, Obj),
    deliver_from_store_(Pid, ets:match_object(Cont));
deliver_from_store_(_, '$end_of_table') ->
    ok.

-spec clean_session(client_id()) -> 'ok'.
clean_session(ClientId) ->
    lists:foreach(fun ({_, {uncached, _}}) ->
                          ok;
                      ({_, {_, MsgRef, _}} = Obj) ->
                          true = ets:delete_object(?MSG_INDEX_TABLE, Obj),
                          deref(MsgRef)
                  end, ets:lookup(?MSG_INDEX_TABLE, ClientId)).

-spec defer_deliver_uncached(client_id(), any()) -> 'true'.
defer_deliver_uncached(ClientId, Term) ->
    ets:insert(?MSG_INDEX_TABLE, {ClientId, {uncached, Term}}).

-spec defer_deliver(client_id(), qos(),
                    msg_ref() | {atom(), msg_ref()}) -> 'true'.
defer_deliver(ClientId, Qos, MsgRef) ->
    defer_deliver(ClientId, Qos, MsgRef, false).

-spec defer_deliver(client_id(), qos(), msg_ref() | {{pid(), atom()}, msg_ref()}
                    , boolean()) -> 'true'.
defer_deliver(ClientId, Qos, {{_, Node}, MsgRef}, DeliverAsDup) ->
    rpc:call(Node, ?MODULE, defer_deliver,
             [ClientId, Qos, MsgRef, DeliverAsDup]);
defer_deliver(ClientId, Qos, MsgRef, DeliverAsDup) ->
    ets:insert(?MSG_INDEX_TABLE, {ClientId, {Qos, MsgRef, DeliverAsDup}}).


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
    vmq_plugin:only(msg_store_delete_sync, [MsgRef1]),
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
    TableOpts = [public, named_table,
                 {read_concurrency, true},
                 {write_concurrency, true}],
    ets:new(?MSG_INDEX_TABLE, [bag|TableOpts]),
    ets:new(?MSG_CACHE_TABLE, TableOpts),
    ets:insert(?MSG_CACHE_TABLE, {in_flight, 0}),
    {ok, #state{}}.

msg_store_init(PluginName) ->
    %% called by the message store implementation
    case whereis(?MODULE) of
        undefined ->
            %% we are not yet started,
            timer:sleep(100),
            msg_store_init(PluginName);
        _ ->
            gen_server:call(?MODULE, {init_plugin, PluginName})
    end.

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

-spec handle_call(_, _, _) -> {noreply, _} | {'reply', {'error', 'not_implemented'}, _}.
handle_call({init_plugin, HookModule}, From, State) ->
    gen_server:reply(From, ok),
    ok = wait_for_hooks(HookModule),
    case vmq_plugin:only(
           msg_store_fold,
           [fun
                (<<?MSG_ITEM, MsgRef/binary>> = Key, Val, Acc) ->
                    {_, RoutingKey, Payload} = binary_to_term(Val),
                    update_subs_(RoutingKey, MsgRef, Payload, Key, Acc)
            end, []]) of
        {error, Reason} ->
            lager:warning("can't initialize msg cache due to ~p", [Reason]);
        ToDelete ->
            lists:foreach(
              fun(Key) ->
                      vmq_plugin:only(msg_store_delete_sync, [Key])
              end, ToDelete)
    end,
    {noreply, State};
handle_call(_Req, _From, State) ->
    {reply, {error, not_implemented}, State}.

-spec handle_cast(_, _) -> {'noreply', state()}.
handle_cast(_Req, State) ->
    {noreply, State}.

-spec handle_info(_, _) -> {'noreply', _}.
handle_info(_Info, State) ->
    {noreply, State}.

-spec terminate(_, state()) -> 'ok'.
terminate(_Reason, _State) ->
    ok.

-spec code_change(_, _, _) -> {'ok', _}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


wait_for_hooks(HookModule) ->
    PluginInfo = vmq_plugin:info(only),
    MsgStoreHooks = [msg_store_write_sync,
                     msg_store_write_async,
                     msg_store_read,
                     msg_store_fold,
                     msg_store_delete_sync,
                     msg_store_delete_async],
    Hooks = [{Hook, M == HookModule}|| {H, M, _, _} = Hook <- PluginInfo,
                                       lists:member(H, MsgStoreHooks)],
    case length(Hooks) == length(MsgStoreHooks) of
        true ->
            case lists:keyfind(false, 2, Hooks) of
                false -> ok;
                _ ->
                    %% maybe inconsistency with msg store plugin
                    lager:warning("check msg store plugin, not all hooks are
                                  provided by the same plugin", []),
                    ok
            end;
        false ->
            timer:sleep(1000),
            wait_for_hooks(HookModule)
    end.


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

msg_ref() ->
    %% uuid style msg_ref
    R1 = crypto:rand_uniform(1, round(math:pow(2, 48))) - 1,
    R2 = crypto:rand_uniform(1, round(math:pow(2, 12))) - 1,
    R3 = crypto:rand_uniform(1, round(math:pow(2, 32))) - 1,
    R4 = crypto:rand_uniform(1, round(math:pow(2, 30))) - 1,
    <<R1:48, 4:4, R2:12, 2:2, R3:32, R4:30>>.
