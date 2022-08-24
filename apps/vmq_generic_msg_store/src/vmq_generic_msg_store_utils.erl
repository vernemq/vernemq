%% Copyright 2018 Erlio GmbH Basel Switzerland (http://erl.io)
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

-module(vmq_generic_msg_store_utils).

%% API
-export([
    dump/1,
    dump/2,
    full_table_scan/2
]).

%%%===================================================================
%%% API
%%%===================================================================

%% dumps the message store content to the file.
dump(FileName) ->
    FileWriteOpts = [write],
    dump(FileName, FileWriteOpts).

dump(FileName, FileOpenOpts) ->
    {ok, Fd} = file:open(FileName, FileOpenOpts),
    full_table_scan(fun file_dump_/2, Fd),
    file:close(Fd).

file_dump_({msg, MsgRef, MP, RoutingKey, Payload}, Fd) ->
    file:write(
        Fd,
        io_lib:format(
            "Msg[~s]:\t ref: ~p\t topic: ~s\t data: ~p~n",
            [
                MP,
                erlang:phash2(MsgRef),
                iolist_to_binary(vmq_topic:unword(RoutingKey)),
                Payload
            ]
        )
    ),
    Fd;
file_dump_({ref, MsgRef, MP, ClientId}, Fd) ->
    file:write(
        Fd,
        io_lib:format(
            "Ref[~s]:\t ref: ~p\t client: ~s~n",
            [MP, erlang:phash2(MsgRef), ClientId]
        )
    ),
    Fd;
file_dump_({idx, MsgRef, MP, ClientId, IdxVal}, Fd) ->
    {{MegaS, Sec, MicroS}, Dup, QoS} = IdxVal,
    file:write(
        Fd,
        io_lib:format(
            "Idx[~s]:\t ref: ~p\t client: ~s\t ts: ~p.~p.~p dup: ~p qos: ~p~n",
            [
                MP,
                erlang:phash2(MsgRef),
                ClientId,
                MegaS,
                Sec,
                MicroS,
                Dup,
                QoS
            ]
        )
    ),
    Fd.

full_table_scan(FoldFun, Acc) ->
    full_table_scan_(vmq_generic_msg_store_sup:get_bucket_pids(), {FoldFun, Acc}).

full_table_scan_([Bucket | Rest], Acc) ->
    {Engine, EngineState} = vmq_generic_msg_store:get_engine(Bucket),
    NewAcc = Engine:fold(EngineState, fun full_table_scan__/3, Acc),
    full_table_scan_(Rest, NewAcc);
full_table_scan_([], {_, Acc}) ->
    Acc.

full_table_scan__(Key, Value, {FoldFun, FoldAcc} = Acc) ->
    NewFoldAcc =
        case sext:decode(Key) of
            {msg, MsgRef, {MP, ''}} ->
                {RoutingKey, Payload} = binary_to_term(Value),
                FoldFun({msg, MsgRef, MP, RoutingKey, Payload}, FoldAcc);
            {msg, MsgRef, {MP, ClientId}} ->
                <<>> = Value,
                FoldFun({ref, MsgRef, MP, ClientId}, FoldAcc);
            {idx, {MP, ClientId}, MsgRef} ->
                IdxVal = binary_to_term(Value),
                FoldFun({idx, MsgRef, MP, ClientId, IdxVal}, FoldAcc);
            E ->
                io:format("unknown sext encoded key ~p~n", [E]),
                Acc
        end,
    {FoldFun, NewFoldAcc}.
