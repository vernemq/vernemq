-module(vmq_bitcask_store).
-behaviour(vmq_msg_store).
-export([open/1,
         fold/3,
         delete/2,
         insert/3,
         close/1]).

open(Args) ->
    MsgStoreDir =
    case Args of
        [] ->
            EMQTTDir = filename:join("EMQTT."++atom_to_list(node()), "store"),
            ok = filelib:ensure_dir(EMQTTDir),
            EMQTTDir;
        [Dir] when is_list(Dir) ->
            true = filelib:is_dir(Dir),
            Dir
    end,
    MsgStore = bitcask:open(MsgStoreDir, [read_write]),
    MsgStore.

fold(MsgStoreRef, Fun, Acc) ->
    bitcask:fold(MsgStoreRef, Fun, Acc).

delete(MsgStoreRef, Key) ->
    bitcask:delete(MsgStoreRef, Key).

insert(MsgStoreRef, Key, Val) ->
    bitcask:put(MsgStoreRef, Key, Val).

close(MsgStoreRef) ->
    bitcask:close(MsgStoreRef).
