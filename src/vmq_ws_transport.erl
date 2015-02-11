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

-module(vmq_ws_transport).
-include_lib("wsock/include/wsock.hrl").
-behaviour(vmq_tcp_transport).
-export([upgrade_connection/2,
         opts/1,
         decode_bin/3,
         encode_bin/1]).

upgrade_connection(TcpSocket, _) ->
    {ok, {TcpSocket, []}}.

opts(Opts) ->
    vmq_tcp_transport:opts(Opts).

decode_bin(Socket, Data, undefined) ->
    decode_bin(Socket, Data, {{closed, <<>>}, emqtt_frame:initial_state()});
decode_bin(Socket, Data, {{closed, Buffer}, PS}) ->
    NewData = <<Buffer/binary, Data/binary>>,
    case wsock_http:decode(NewData, request) of
        {ok, OpenHTTPMessage} ->
            case wsock_handshake:handle_open(OpenHTTPMessage) of
                {ok, OpenHandshake} ->
                    WSKey = wsock_http:get_header_value(
                        "sec-websocket-key",  OpenHandshake#handshake.message),
                    {ok, Response} = wsock_handshake:response(WSKey),
                    Bin = wsock_http:encode(Response#handshake.message),
                    vmq_tcp_transport:port_cmd(Socket, Bin),
                    {more, {{open, <<>>}, PS}};
                {error, Reason} ->
                    exit({ws_handle_open, Reason})
            end;
        fragmented_http_message ->
            {more, {{closed, NewData}, PS}};
        E ->
            exit({ws_http_decode, E})
    end;
decode_bin(_Socket, Data, {{open, Buffer}, PS}) ->
    NewData = <<Buffer/binary, Data/binary>>,
    case wsock_message:decode(NewData, [masked]) of
        {error, Reason} ->
            exit({ws_msg_decode, Reason});
        List ->
            MQTTBytes =
            lists:foldl(
              fun(#message{type=Type, payload=Payload}, Acc) ->
                      case Type of
                          binary ->
                              [Payload|Acc];
                          _ ->
                              Acc
                      end
              end, [], List),
            {ok, list_to_binary(lists:reverse(MQTTBytes)), {{open, <<>>}, PS}}
    end.

encode_bin(Data) ->
    wsock_message:encode(Data, [mask, binary]).
