%% Copyright 2016 Erlio GmbH Basel Switzerland (http://erl.io)
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

-module(vmq_lvldb_store_utils).
-include("vmq_server.hrl").

%% API
-export([dump/1]).

%%%===================================================================
%%% API
%%%===================================================================
dump(FileName) ->
    dump(FileName, [write]).

dump(FileName, FileOpenOpts) ->
    {ok, Fd} = file:open(FileName, FileOpenOpts),
    dump_(vmq_lvldb_store_sup:get_bucket_pids(), Fd).

dump_([], Fd) -> file:close(Fd);
dump_([Pid|Rest], Fd) ->
    full_table_scan(vmq_lvldb_store:get_ref(Pid), Fd),
    dump_(Rest, Fd).

full_table_scan(Bucket, Fd) ->
    FoldOpts = [{fill_cache, false}],
    eleveldb:fold(Bucket, fun full_table_scan_/2, Fd, FoldOpts).

full_table_scan_({Key, Value}, Fd) ->
    case sext:decode(Key) of
        {msg, MsgRef, {MP, ''}} ->
            {RoutingKey, Payload} = binary_to_term(Value),
            file:write(Fd, io_lib:format("Msg[~s]:\t ref: ~p\t topic: ~s\t data: ~p~n",
                                         [MP, erlang:phash2(MsgRef),
                                          iolist_to_binary(vmq_topic:unword(RoutingKey)), Payload]));
        {msg, MsgRef, {MP, ClientId}} ->
            <<>> = Value,
            file:write(Fd, io_lib:format("Ref[~s]:\t ref: ~p\t client: ~s~n",
                                         [MP, erlang:phash2(MsgRef), ClientId]));
        {idx, {MP, ClientId}, MsgRef} ->
            {{MegaS, Sec, MicroS}, Dup, QoS} = binary_to_term(Value),
            file:write(Fd, io_lib:format("Idx[~s]:\t ref: ~p\t client: ~s\t ts: ~p.~p.~p dup: ~p qos: ~p~n",
                      [MP, erlang:phash2(MsgRef), ClientId, MegaS, Sec, MicroS, Dup, QoS]));
        E ->
            io:format("unknown sext encoded key ~p~n", [E])
    end,
    Fd.
