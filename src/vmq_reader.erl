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

-module(vmq_reader).
-include_lib("wsock/include/wsock.hrl").

-export([start_link/3,
         handover/2]).
-export([init/3]).

-record(st, {socket,
             transport, handler, buffer= <<>>,
             parser_state,
             session, session_mon,
             proto_tag, bytes_recv={os:timestamp(), 0}}).

-spec start_link(_, _, _) -> {'ok', pid()}.
start_link(SessionPid, Handler, Transport) ->
    Pid = proc_lib:spawn_link(?MODULE, init, [SessionPid,
                                              Handler, Transport]),
    {ok, Pid}.

handover(ReaderPid, Socket) ->
    ReaderPid ! {handover, Socket}.

-spec init(_, _, _) -> any().
init(SessionPid, Handler, Transport) ->
    wait_for_socket(#st{transport=Transport,
                        handler=Handler,
                        session=SessionPid,
                        proto_tag=proto_tag(Transport)}).


wait_for_socket(State) ->
    receive
        {handover, Socket} ->
            vmq_systree:incr_socket_count(),
            loop(State#st{socket=Socket});
        M -> exit({unexpected_msg, M})
    after
        5000 ->
            exit(socket_handover_timeout)
    end.


loop(#st{buffer=_Buffer, socket=Socket, transport=Transport,
            handler=Handler,
            session=SessionPid, parser_state=ParserState,
            proto_tag={Proto, ProtoClosed, ProtoError},
            bytes_recv={TS, V}
        } = State) ->
    active_once(Transport, Socket),
    receive
        {Proto, Socket, Data} ->
            NewParserState = process_data(SessionPid, Handler,
                                          Socket, Data, ParserState),
            {M, S, _} = TS,
            NrOfBytes = byte_size(Data),
            NewBytesRecv =
            case os:timestamp() of
                {M, S, _} = NewTS ->
                    {NewTS, V + NrOfBytes};
                NewTS ->
                    vmq_systree:incr_bytes_received(V + NrOfBytes),
                    {NewTS, 0}
            end,
            loop(State#st{parser_state=NewParserState,
                          bytes_recv=NewBytesRecv});
        {ProtoClosed, Socket} ->
            %% we regard a tcp_closed as 'normal'
            teardown(State, normal);
        {ProtoError, Socket, Reason} ->
            teardown(State, Reason);
        M ->
            exit({received_unknown_msg, M})
    end.

teardown(#st{session=SessionPid, transport=Transport, socket=Socket}, Reason) ->
    case Reason of
        normal ->
            lager:debug("[~p] session stopped", [SessionPid]);
        _ ->
            lager:warning("[~p] session stopped
                       abnormally due to ~p", [SessionPid, Reason])
    end,
    fast_close(Transport, Socket).


proto_tag(gen_tcp) -> {tcp, tcp_closed, tcp_error};
proto_tag(ssl) -> {ssl, ssl_closed, ssl_error}.

fast_close(gen_tcp, Socket) ->
    catch port_close(Socket),
    ok;
fast_close(ssl, Socket) ->
    %% from rabbit_net.erl
    %% We cannot simply port_close the underlying tcp socket since the
    %% TLS protocol is quite insistent that a proper closing handshake
    %% should take place (see RFC 5245 s7.2.1). So we call ssl:close
    %% instead, but that can block for a very long time, e.g. when
    %% there is lots of pending output and there is tcp backpressure,
    %% or the ssl_connection process has entered the the
    %% workaround_transport_delivery_problems function during
    %% termination, which, inexplicably, does a gen_tcp:recv(Socket,
    %% 0), which may never return if the client doesn't send a FIN or
    %% that gets swallowed by the network. Since there is no timeout
    %% variant of ssl:close, we construct our own.
    {Pid, MRef} = spawn_monitor(fun () -> ssl:close(Socket) end),
    erlang:send_after(5000, self(), {Pid, ssl_close_timeout}),
    receive
        {Pid, ssl_close_timeout} ->
            erlang:demonitor(MRef, [flush]),
            exit(Pid, kill);
        {'DOWN', MRef, process, Pid, _Reason} ->
            ok
    end,
    catch port_close(Socket),
    ok.

active_once(gen_tcp, Socket) ->
    inet:setopts(Socket, [{active, once}]);
active_once(ssl, Socket) ->
    ssl:setopts(Socket, [{active, once}]).

process_data(SessionPid, vmq_tcp_listener, _, Data, ParserState) ->
    process_bytes(SessionPid, Data, ParserState);
process_data(SessionPid, vmq_ssl_listener, _, Data, ParserState) ->
    process_bytes(SessionPid, Data, ParserState);
process_data(SessionPid, vmq_ws_listener, Socket, Data, ParserState) ->
    process_ws_data(SessionPid, gen_tcp, Socket, Data, ParserState).

process_ws_data(SessionPid, Transport, Socket, Data, undefined) ->
    process_ws_data(SessionPid, Transport, Socket,
                    Data, {{closed, <<>>}, emqtt_frame:initial_state()});
process_ws_data(_, Transport, Socket, Data, {{closed, Buffer}, PS}) ->
    NewData = <<Buffer/binary, Data/binary>>,
    case wsock_http:decode(NewData, request) of
        {ok, OpenHTTPMessage} ->
            case wsock_handshake:handle_open(OpenHTTPMessage) of
                {ok, OpenHandshake} ->
                    WSKey = wsock_http:get_header_value(
                        "sec-websocket-key",  OpenHandshake#handshake.message),
                    {ok, Response} = wsock_handshake:response(WSKey),
                    Bin = wsock_http:encode(Response#handshake.message),
                    Transport:send(Socket, Bin),
                    {{open, <<>>}, PS};
                {error, Reason} ->
                    exit({ws_handle_open, Reason})
            end;
        fragmented_http_message ->
            {{closed, NewData}, PS};
        E ->
            exit({ws_http_decode, E})
    end;
process_ws_data(SessionPid, _, _, Data, {{open, Buffer}, PS}) ->
    NewData = <<Buffer/binary, Data/binary>>,
    case wsock_message:decode(NewData, [masked]) of
        {error, Reason} ->
            exit({ws_msg_decode, Reason});
        List ->
            NewPS =
            lists:foldl(
              fun(#message{type=Type, payload=Payload}, ParserState) ->
                      case Type of
                          binary ->
                              process_bytes(SessionPid, Payload, ParserState);
                          _ ->
                              ParserState
                      end
              end, PS, List),
            {{open, <<>>}, NewPS}
    end.

process_bytes(SessionPid, Bytes, undefined) ->
    process_bytes(SessionPid, Bytes, emqtt_frame:initial_state());
process_bytes(SessionPid, Bytes, ParserState) ->
    case emqtt_frame:parse(Bytes, ParserState) of
        {more, NewParserState} ->
            NewParserState;
        {ok, Frame, Rest} ->
            vmq_session:in(SessionPid, Frame),
            process_bytes(SessionPid, Rest, emqtt_frame:initial_state());
        {error, Reason} ->
            io:format("parse error ~p~n", [Reason]),
            emqtt_frame:initial_state()
    end.
