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

-module(vmq_tcp).
-behaviour(ranch_protocol).
-include_lib("public_key/include/public_key.hrl").
-include("vmq_server.hrl").

-export([start_link/4]).
-export([init/4]).

-record(st, {socket,
             transport, buffer= <<>>,
             parser_state=emqtt_frame:initial_state(),
             session, session_mon,
             proto_tag}).

-spec start_link(_, _, _, _) -> {'ok', pid()}.
start_link(Ref, Socket, Transport, Opts) ->
    Pid = proc_lib:spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
    {ok, Pid}.

-spec init(_, _, atom() | tuple(), maybe_improper_list()) -> any().
init(Ref, Socket, Transport, Opts) ->
    ok = ranch:accept_ack(Ref),
    {ok, Peer} = apply(Transport, peername, [Socket]),
    {ok, WriterPid} = vmq_writer:start_link(Transport, Socket),
    NewOpts =
    case Transport of
        ranch_ssl ->
            case proplists:get_value(use_identity_as_username, Opts, false) of
                true ->
                    [{preauth, socket_to_common_name(Socket)}|Opts];
                false ->
                    Opts
            end;
        _ ->
            Opts
    end,
    {ok, SessionPid} =
    vmq_session:start_link(Peer, fun(Frame) ->
                                         vmq_writer:send(WriterPid, Frame),
                                         ok
                                 end, NewOpts),
    ok = maybe_tune_buffer_size(Transport, Socket),
    ok = apply(Transport, setopts, [Socket, [{nodelay, true},
                                    {packet, raw},
                                    {active, once}]]),
    process_flag(trap_exit, true),
    vmq_systree:incr_socket_count(),
    loop(#st{socket=Socket,
             transport=Transport,
             session=SessionPid,
             proto_tag=proto_tag(Transport)}).

loop(#st{buffer=_Buffer, socket=Socket, transport=Transport,
            session=SessionPid, parser_state=ParserState,
            proto_tag={Proto, ProtoClosed, ProtoError}} = State) ->
    apply(Transport, setopts, [Socket, [{active, once}]]),
    receive
        {inet_reply, _, ok} ->
            loop(State);
        {inet_reply, _, Status} ->
            teardown(State, Status);
        {Proto, Socket, Data} ->
            NewParserState = process_bytes(SessionPid, Data, ParserState),
            vmq_systree:incr_bytes_received(byte_size(Data)),
            loop(State#st{parser_state=NewParserState});
        {ProtoClosed, Socket} ->
            teardown(State, tcp_closed);
        {ProtoError, Socket, Reason} ->
            teardown(State, Reason);
        {'EXIT', _Parent, Reason} ->
            teardown(State, Reason);
        M ->
            exit({received_unknown_msg, M})
    end.

teardown(#st{session=SessionPid, transport=Transport, socket=Socket}, Reason) ->
    case Reason of
        normal ->
            lager:debug("[~p] session stopped", [SessionPid]);
        _ ->
            lager:info("[~p] session stopped
                       abnormally due to ~p", [SessionPid, Reason])
    end,
    fast_close(Transport, Socket).


process_bytes(SessionPid, Bytes, ParserState) ->
    case emqtt_frame:parse(Bytes, ParserState) of
        {more, NewParserState} ->
            NewParserState;
        {ok, #mqtt_frame{} = Frame, Rest} ->
            vmq_systree:incr_messages_received(),
            vmq_session:in(SessionPid, Frame),
            process_bytes(SessionPid, Rest, emqtt_frame:initial_state());
        {error, Reason} ->
            io:format("parse error ~p~n", [Reason]),
            emqtt_frame:initial_state()
    end.

maybe_tune_buffer_size(Transport, Socket) ->
    case vmq_config:get_env(tune_tcp_buffer_size, false) of
        true -> tune_buffer_size(Transport, Socket);
        false -> ok
    end.

tune_buffer_size(ranch_tcp, Sock) ->
    tune_buffer_size_(inet, Sock);
tune_buffer_size(ranch_ssl, Sock) ->
    tune_buffer_size_(ssl, Sock).

tune_buffer_size_(Transport, Sock) ->
    case apply(Transport, getopts, [Sock, [sndbuf, recbuf, buffer]]) of
        {ok, BufSizes} -> BufSz = lists:max([Sz || {_Opt, Sz} <- BufSizes]),
                          apply(Transport, setopts, [Sock, [{buffer, BufSz}]]);
        Error -> Error
    end.

proto_tag(ranch_tcp) -> {tcp, tcp_closed, tcp_error};
proto_tag(ranch_ssl) -> {ssl, ssl_closed, ssl_error}.

fast_close(ranch_tcp, Socket) ->
    catch port_close(Socket),
    ok;
fast_close(ranch_ssl, Socket) ->
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

-spec socket_to_common_name(port()) -> undefined | list().
socket_to_common_name(Socket) ->
    case ssl:peercert(Socket) of
        {error, no_peercert} ->
            undefined;
        {ok, Cert} ->
            OTPCert = public_key:pkix_decode_cert(Cert, otp),
            TBSCert = OTPCert#'OTPCertificate'.tbsCertificate,
            Subject = TBSCert#'OTPTBSCertificate'.subject,
            extract_cn(Subject)
    end.

-spec extract_cn({'rdnSequence', list()}) -> undefined | list().
extract_cn({rdnSequence, List}) ->
    extract_cn2(List).

-spec extract_cn2(list()) -> undefined | list().
extract_cn2([[#'AttributeTypeAndValue'{
                 type=?'id-at-commonName',
                 value={utf8String, CN}}]|_]) ->
    unicode:characters_to_list(CN);
extract_cn2([_|Rest]) ->
    extract_cn2(Rest);
extract_cn2([]) -> undefined.

