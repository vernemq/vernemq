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
-module(vmq_ws_transport).

-export([connect/4,
         send/2,
         recv/3,
         close/1
        ]).

connect(Host, Port, Opts, Timeout) ->
    WSProtocols = proplists:get_value(ws_protocols, Opts, ["mqtt"]),
    ProxyInfo = proplists:get_value(proxy_info, Opts),
    Opts1 = proplists:delete(ws_protocols, Opts),
    Opts2 = proplists:delete(proxy_info, Opts1),
    case gen_tcp:connect(Host, Port, Opts2, Timeout) of
        {ok, Socket} ->
            case ProxyInfo of
                undefined -> ignore;
                ProxyInfo -> Header = ranch_proxy_header:header(ProxyInfo),
                             gen_tcp:send(Socket, Header)
            end,
            WSProtocolsStr = string:join(WSProtocols, ","),
            Hello = [
                     "GET /mqtt HTTP/1.1\r\n"
                     "Host: localhost\r\n"
                     "Connection: Upgrade\r\n"
                     "Origin: http://localhost\r\n"
                     "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                     "Sec-WebSocket-Protocol: "++ WSProtocolsStr++"\r\n"
                     "Sec-WebSocket-Version: 13\r\n"
                     "Upgrade: websocket\r\n"
                     "\r\n"],
            ok = gen_tcp:send(Socket, Hello),
            {ok, Handshake} = gen_tcp:recv(Socket, 0, 6000),
            {ok, {http_response, {1, 1}, 101, "Switching Protocols"}, Rest}
		= erlang:decode_packet(http, Handshake, []),
            [Headers, <<>>] = do_decode_headers(
                                erlang:decode_packet(httph, Rest, []), []),
            {'Connection', "Upgrade"} = lists:keyfind('Connection', 1, Headers),
            {'Upgrade', "websocket"} = lists:keyfind('Upgrade', 1, Headers),
            {"sec-websocket-accept", "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="}
		= lists:keyfind("sec-websocket-accept", 1, Headers),
            case lists:keyfind("sec-websocket-protocol", 1, Headers) of
                {"sec-websocket-protocol", "mqtt"} -> {ok, Socket};
                {"sec-websocket-protocol", "mqttv3.1"} -> {ok, Socket};
                false -> {error, unknown_websocket_protocol}
            end;
        E ->
            E
    end.

send(Socket, Data) ->
    Mask = rand:uniform(4294967296) - 1,
    MaskedData = do_mask(Data, Mask, <<>>),
    EncLen = enc_len(byte_size(MaskedData)),
    Frame = <<16#82, EncLen/binary, Mask:32, MaskedData/binary>>,
    gen_tcp:send(Socket, Frame).

recv(Socket, Length, Timeout) when Length > 0 ->
    %% TODO: handle case when Length =:= 0.
    %% get any leftovers from last time we received data.
    Acc = get_and_clear_rest(Socket),
    %% Need to keep receiving until we have Length MQTT data or
    %% Timeout time is elapsed.
    receive_data(Socket, Length, Timeout, 0, Acc).

close(Socket) ->
    gen_tcp:close(Socket).

%%% Internal functions
receive_data(Socket, _Length, Timeout, Elapsed, Acc) when Elapsed > Timeout ->
    store_rest(Socket, Acc),
    {error, timeout};
receive_data(Socket, Length, Timeout, Elapsed, Acc) ->
    case gen_tcp:recv(Socket, 0, 5) of
        {ok, RecvData} ->
            Data = <<Acc/binary, RecvData/binary>>,
            case parse(Data) of
                {close, <<ParsedData:Length/binary>>, _Rest} ->
                    gen_tcp:close(Socket),
                    {ok, ParsedData};
                {close, <<>>, _Rest} ->
                    gen_tcp:close(Socket),
                    {error, closed};
                {ok, <<ParsedData:Length/binary>>,  Rest} ->
                    store_rest(Socket, Rest),
                    {ok, ParsedData};
                more ->
                    receive_data(Socket, Length, Timeout, Elapsed, Data)
            end;
        {error, timeout} ->
            receive_data(Socket, Length, Timeout, Elapsed + 5, Acc);
        {error, _} = E -> E
    end.

parse(Data) ->
    case parse(Data, []) of
        {[], _Rest} -> more;
        {Frames, Rest} ->
            {Action, Data0} = assemble_frames(Frames),
            {Action, Data0, Rest}
    end.

parse(<<136,2, 3, 232, Rest/binary>>, Frames) ->
    parse(Rest, [{close,normal}|Frames]);
parse(<<1:1, 0:3, 2:4, PayloadData/binary>>, Frames) ->
    case get_payload(PayloadData) of
        more ->
            {lists:reverse(Frames), PayloadData};
        {Frame, Rest} ->
            parse(Rest, [{data, Frame}|Frames])
    end;
parse(Rest, Frames) ->
    {lists:reverse(Frames), Rest}.

assemble_frames(Frames) ->
    assemble_frames(Frames, <<>>).

assemble_frames([], Acc) ->
    {ok, Acc};
assemble_frames([{close,normal}|_], Acc) ->
    {close, Acc};
assemble_frames([{data, Data}|Tail], Acc) ->
    assemble_frames(Tail, <<Acc/binary, Data/binary>>).

store_rest(Socket, Rest) ->
    put({Socket, ws_rest_data}, Rest).

get_and_clear_rest(Socket) ->
    case get({Socket, ws_rest_data}) of
        undefined -> <<>>;
        Data ->
            put({Socket, ws_rest_data}, undefined),
            Data
    end.


enc_len(Len) when Len < 126 ->
    <<1:1, Len:7>>;
enc_len(Len) when Len < 65536 ->
    <<1:1, 126:7, Len:16/big>>;
enc_len(Len) when Len < 18446744073709551616 ->
    <<1:1, 127:7, Len:64/big>>.

get_payload(<<0:1, 127:7, Len:64/big, Data:Len/binary, Rest/binary>>) ->
    {Data, Rest};
get_payload(<<0:1, 126:7, Len:16/big, Data:Len/binary, Rest/binary>>) ->
    {Data, Rest};
get_payload(<<0:1, Len:7, Data:Len/binary, Rest/binary>>) ->
    {Data, Rest};
get_payload(_) ->
    more.


%% The `do_decode_headers/2` and `do_mask/3` functions were borrowed
%% from the Cowboy websocket test suite
%% (https://github.com/ninenines/cowboy).
do_decode_headers({ok, http_eoh, Rest}, Acc) ->
	[Acc, Rest];
do_decode_headers({ok, {http_header, _I, Key, _R, Value}, Rest}, Acc) ->
	F = fun(S) when is_atom(S) -> S; (S) -> string:to_lower(S) end,
	do_decode_headers(erlang:decode_packet(httph, Rest, []),
		[{F(Key), Value}|Acc]).

do_mask(<<>>, _, Acc) ->
	Acc;
do_mask(<< O:32, Rest/bits >>, MaskKey, Acc) ->
	T = O bxor MaskKey,
	do_mask(Rest, MaskKey, << Acc/binary, T:32 >>);
do_mask(<< O:24 >>, MaskKey, Acc) ->
	<< MaskKey2:24, _:8 >> = << MaskKey:32 >>,
	T = O bxor MaskKey2,
	<< Acc/binary, T:24 >>;
do_mask(<< O:16 >>, MaskKey, Acc) ->
	<< MaskKey2:16, _:16 >> = << MaskKey:32 >>,
	T = O bxor MaskKey2,
	<< Acc/binary, T:16 >>;
do_mask(<< O:8 >>, MaskKey, Acc) ->
	<< MaskKey2:8, _:24 >> = << MaskKey:32 >>,
	T = O bxor MaskKey2,
	<< Acc/binary, T:8 >>.
