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

-module(vmq_wss_transport).
-behaviour(vmq_tcp_transport).
-export([upgrade_connection/2,
         opts/1,
         decode_bin/3,
         encode_bin/1]).

upgrade_connection(TcpSocket, TransportOpts) ->
    vmq_ssl_transport:upgrade_connection(TcpSocket, TransportOpts).

opts(Opts) ->
    vmq_ssl_listener:transport_opts(Opts).

decode_bin(Socket, Data, ParserState) ->
    vmq_ws_transport:process_data(Socket, Data, ParserState).

encode_bin(Bin) ->
    vmq_ws_transport:encode_bin(Bin).
