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
         deref/2,
         deliver_from_store/2,
         clean_session/1,
         defer_deliver/3,
         defer_deliver/4,
         defer_deliver_uncached/2
         ]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).
-type state() :: #state{}.

-define(MSG_ITEM, 0).
-define(INDEX_ITEM, 1).
-define(MSG_REF, 2).
-define(MSG_CACHE_TABLE, vmq_msg_cache).
-define(MSG_REF_TABLE, vmq_msg_regs).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% API FUNCTIONS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec start_link() -> 'ignore' | {'error',_} | {'ok',pid()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec store(subscriber_id(), msg()) -> msg().
store(SubscriberId, #vmq_msg{msg_ref=undefined} = Msg) ->
    store(SubscriberId, Msg#vmq_msg{msg_ref=msg_ref()});
store(_SubscriberId, #vmq_msg{msg_ref={{_Pid, _Node}, _MsgRef}} = Msg) ->
    %% during message replication, a session will try to store
    %% the a remote message, which we forbid, as long as the
    %% replication process hasn't finished, the message will
    %% remain stored on the remote node, so no need to store it here
    %% TODO: keep track of replicated messages that are not acked yet
    %% --> replication process not yet finished
    Msg;
store(SubscriberId, Msg) ->
    #vmq_msg{msg_ref=MsgRef, routing_key=RoutingKey,
             payload=Payload} = Msg,
    ets:update_counter(?MSG_CACHE_TABLE, in_flight, {2, 1}),
    case update_msg_cache(MsgRef, {RoutingKey, Payload}) of
        new_cache_item ->
            MsgRef1 = <<?MSG_ITEM, MsgRef/binary>>,
            Val = term_to_binary({SubscriberId, RoutingKey, Payload}),
            _ = vmq_plugin:only(msg_store_write_sync, [MsgRef1, Val]),
            ok;
        new_ref_count ->
            ok
    end,
    Msg.

-spec in_flight() -> non_neg_integer().
in_flight() ->
    [{in_flight, InFlight}] = ets:lookup(?MSG_CACHE_TABLE, in_flight),
    NrOfDeferedMsgs = ets:info(?MSG_REF_TABLE, size),
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

-spec deref(subscriber_id(), msg_ref()|{{pid(), atom()}, msg_ref()}) ->
    'ok' | {'error','not_found'}.
deref(SubscriberId, {{SessionProxy, Node}, MsgRef}) ->
    rpc:call(Node, ?MODULE, deref, [SubscriberId, MsgRef]),
    vmq_session_proxy:derefed(SessionProxy, MsgRef);
deref(SubscriberId, MsgRef) ->
    try
        ets:update_counter(?MSG_CACHE_TABLE, in_flight, {2, -1}),
        case ets:update_counter(?MSG_CACHE_TABLE, MsgRef, {3, -1}) of
            0 ->
                ets:delete(?MSG_CACHE_TABLE, MsgRef),
                Key = <<?MSG_ITEM, MsgRef/binary>>,
                vmq_plugin:only(msg_store_delete_async, [Key]);
            _ ->
                ignore
        end,
        SubHash = erlang:phash2(SubscriberId),
        RefKey = <<?MSG_REF, MsgRef/binary, SubHash>>,
        vmq_plugin:only(msg_store_delete_async, [RefKey]),
        ets:match_delete(?MSG_REF_TABLE, {SubscriberId, MsgRef, '_', '_'}),
        ok
    catch error:badarg -> {error, not_found}
    end.

-spec deliver_from_store(subscriber_id(), pid()) -> 'ok'.
deliver_from_store(SubscriberId, Pid) ->
    deliver_from_store_(Pid, ets:lookup(?MSG_REF_TABLE, SubscriberId)).

deliver_from_store_(Pid, [{_, {uncached, Term}} = Obj |Rest]) ->
    %% deliver will last as long as the message is not acked by the client
    %% connected to the remote node
    ok = vmq_session_proxy:deliver(Pid, Term),
    ets:delete_object(?MSG_REF_TABLE, Obj),
    deliver_from_store_(Pid, Rest);
deliver_from_store_(Pid, [{_, MsgRef, QoS, Dup} |Rest]) ->
    case retrieve(MsgRef) of
        {ok, {RoutingKey, Payload}} ->
            Term = {RoutingKey, Payload, QoS, Dup, MsgRef},
            vmq_session_proxy:deliver(Pid, Term);
        {error, not_found} ->
            %% TODO: this happens,, ??
            ignore
    end,
    %%% Message is already properly derefed
    deliver_from_store_(Pid, Rest);
deliver_from_store_(_, []) ->
    ok.

-spec clean_session(subscriber_id()) -> 'ok'.
clean_session(SubscriberId) ->
    lists:foreach(fun ({_, {uncached, _}} = Obj) ->
                          ets:delete_object(?MSG_REF_TABLE, Obj);
                      ({_, MsgRef, _QoS, _Dup} = Obj) ->
                          ets:delete_object(?MSG_REF_TABLE, Obj),
                          deref(SubscriberId, MsgRef)
                  end, ets:lookup(?MSG_REF_TABLE, SubscriberId)).

-spec defer_deliver_uncached(subscriber_id(), any()) -> 'ok'.
defer_deliver_uncached(SubscriberId, Term) ->
    ets:insert(?MSG_REF_TABLE, {SubscriberId, {uncached, Term}}),
    ok.

-spec defer_deliver(subscriber_id(), qos(),
                    msg_ref() | {atom(), msg_ref()}) -> 'ok'.
defer_deliver(SubscriberId, Qos, MsgRef) ->
    defer_deliver(SubscriberId, Qos, MsgRef, false).

-spec defer_deliver(subscriber_id(), qos(), msg_ref()
                    | {{pid(), atom()}, msg_ref()}, boolean()) -> 'ok'.
defer_deliver(SubscriberId, Qos, {{_, Node}, MsgRef}, DeliverAsDup) ->
    %% The MsgRef gets annonated by the vmq_session_proxy when it
    %% enqueues the message the 'local' session queue..
    rpc:call(Node, ?MODULE, defer_deliver,
             [SubscriberId, Qos, MsgRef, DeliverAsDup]);
defer_deliver(SubscriberId, Qos, MsgRef, DeliverAsDup) ->
    SubHash = erlang:phash2(SubscriberId),
    Key = <<?MSG_REF, MsgRef/binary, SubHash>>,
    Val = term_to_binary({SubscriberId, Qos, DeliverAsDup}),
    _ = vmq_plugin:only(msg_store_write_async, [Key, Val]),
    ets:insert(?MSG_REF_TABLE, {SubscriberId, MsgRef, Qos, DeliverAsDup}),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% GEN_SERVER CALLBACKS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec init([]) -> {'ok', state(), non_neg_integer()}.
init([]) ->
    MsgStoreMod = vmq_config:get_env(msg_store_mod),
    ok = vmq_plugin_mgr:enable_module_plugin(MsgStoreMod, msg_store_write_sync, 2),
    ok = vmq_plugin_mgr:enable_module_plugin(MsgStoreMod, msg_store_write_async, 2),
    ok = vmq_plugin_mgr:enable_module_plugin(MsgStoreMod, msg_store_read, 1),
    ok = vmq_plugin_mgr:enable_module_plugin(MsgStoreMod, msg_store_fold, 2),
    ok = vmq_plugin_mgr:enable_module_plugin(MsgStoreMod, msg_store_delete_sync, 1),
    ok = vmq_plugin_mgr:enable_module_plugin(MsgStoreMod, msg_store_delete_async, 1),
    TableOpts = [public, named_table,
                 {read_concurrency, true},
                 {write_concurrency, true}],
    _ = ets:new(?MSG_CACHE_TABLE, TableOpts),
    _ = ets:new(?MSG_REF_TABLE, [bag | TableOpts]),
    ets:insert(?MSG_CACHE_TABLE, {in_flight, 0}),
    spawn_link(fun populate_tables/0),
    {ok, #state{}, 0}.

-spec handle_call(_, _, _) -> {'reply', {'error', 'not_implemented'}, _}.
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

populate_tables() ->
    vmq_plugin:only(
      msg_store_fold,
      [fun
          (<<?MSG_REF, MsgRef:16/binary, _/binary>>, RefData, Acc) ->
              {{_,_} = SubscriberId, QoS, Dup} = binary_to_term(RefData),
              ets:insert(?MSG_REF_TABLE, {SubscriberId, MsgRef, QoS, Dup}),
              Acc;
          (<<?MSG_ITEM, MsgRef/binary>>, Val, Acc) ->
              {_, RoutingKey, Payload} = binary_to_term(Val),
              %% Increment in_flight counter
              ets:update_counter(?MSG_CACHE_TABLE,
                                 in_flight, {2, 1}),
              %% add to cache
              update_msg_cache(MsgRef, {RoutingKey, Payload}),
              Acc
      end, undefined]),
    ets:foldl(
      fun({SubscriberId, MsgRef} = Obj, Acc) ->
              case ets:lookup(?MSG_CACHE_TABLE, MsgRef) of
                  [{_, Msg, _RefCnt}] ->
                      update_msg_cache(MsgRef, Msg),
                      Acc;
                  [] ->
                      %% not found --> clean up
                      SubHash = erlang:phash2(SubscriberId),
                      _ = vmq_plugin:only(msg_store_delete_sync,
                                          [<<?MSG_REF, MsgRef/binary, SubHash>>]),
                      ets:delete_object(?MSG_REF_TABLE, Obj),
                      Acc
              end
      end, undefined, ?MSG_REF_TABLE),
    ok.

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
