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
            EMQTTDir = filename:join("VERNEMQ."++atom_to_list(node()), "store"),
            ok = filelib:ensure_dir(EMQTTDir),
            EMQTTDir;
        [Dir] when is_list(Dir) ->
            case filelib:is_dir(Dir) of
                true -> Dir;
                false ->
                    error_logger:error_msg("Directory ~p is not available!!! We stop here!!!", [Dir]),
                    exit(msg_store_directory_not_available)
            end
    end,
    case bitcask:open(MsgStoreDir, [read_write]) of
        {error, Reason} -> {error, Reason};
        Ref -> {ok, Ref}
    end.

fold(MsgStoreRef, Fun, Acc) ->
    bitcask:fold(MsgStoreRef, Fun, Acc).

delete(MsgStoreRef, Key) ->
    bitcask:delete(MsgStoreRef, Key).

insert(MsgStoreRef, Key, Val) ->
    bitcask:put(MsgStoreRef, Key, Val).

close(MsgStoreRef) ->
    bitcask:close(MsgStoreRef).
