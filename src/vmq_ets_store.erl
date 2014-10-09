-module(vmq_ets_store).
-behaviour(vmq_msg_store).
-export([open/1,
         fold/3,
         delete/2,
         insert/3,
         close/1]).

open(_Args) ->
    ets:new(?MODULE, [set, named_table]).

fold(MsgStoreRef, Fun, Acc) ->
    ets:foldl(Fun, Acc, MsgStoreRef).

delete(MsgStoreRef, Key) ->
    true = ets:delete(MsgStoreRef, Key),
    ok.

insert(MsgStoreRef, Key, Val) ->
    true = ets:insert(MsgStoreRef, {Key, Val}),
    ok.

close(MsgStoreRef) ->
    true = ets:delete(MsgStoreRef),
    ok.

