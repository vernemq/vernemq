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

-record(st, {socket, writer,
             transport, buffer= <<>>,
             parser_state=emqtt_frame:initial_state(),
             session, session_monitor,
             proto_tag}).

-spec start_link(_,_,_,_) -> {'ok',pid()}.
start_link(Ref, Socket, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
    {ok, Pid}.

-spec init(_,_,atom() | tuple(),maybe_improper_list()) -> any().
init(Ref, Socket, Transport, Opts) ->
    ok = ranch:accept_ack(Ref),
    case lists:keyfind(tune_buffer_size, 1, Opts) of
        {_, true} ->
            ok = tune_buffer_size(Transport, Socket);
        _ ->
            ok
    end,
    ok = Transport:setopts(Socket, [{nodelay, true},
                                    {packet, raw},
                                    {active, once}]),
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

    {ok, WriterPid} = vmq_writer:start_link(Transport, Socket, self()),


    {ok, Peer} = Transport:peername(Socket),
    vmq_systree:incr_socket_count(),
    {ok, SessionPid} = vmq_session:start(self(), Peer,
                                         fun(Frame) ->
                                                 vmq_writer:send(WriterPid, Frame),
                                                 ok
                                         end, NewOpts),
    MRef = monitor(process, SessionPid),
    loop(#st{socket=Socket,
             writer=WriterPid,
             transport=Transport,
             session=SessionPid,
             session_monitor=MRef,
             proto_tag=proto_tag(Transport)}).

loop(#st{buffer=_Buffer, socket=Socket, transport=Transport,
            session=SessionPid, parser_state=ParserState,
            proto_tag={Proto, ProtoClosed, ProtoError}} = State) ->
    Transport:setopts(Socket, [{active, once}]),
    receive
        {inet_reply, _, ok} ->
            loop(State);
        {inet_reply, _, Status} ->
            teardown(State, Status);
        {vmq_writer, exit, Reason} ->
            teardown(State, Reason);
        {Proto, Socket, Data} ->
            NewParserState = process_bytes(SessionPid, Data, ParserState),
            vmq_systree:incr_bytes_received(byte_size(Data)),
            loop(State#st{parser_state=NewParserState});
        {ProtoClosed, Socket} ->
            teardown(State, tcp_closed);
        {ProtoError, Socket, Reason} ->
            teardown(State, Reason);
        {'DOWN', _, process, _Pid, Reason} ->
            %% Session stopped
            teardown(State#st{session_monitor=undefined}, Reason)
    end.

teardown(#st{session=SessionPid, session_monitor=MRef,
             writer=WriterPid, transport=Transport, socket=Socket}, Reason) ->
    case Reason of
        normal -> ok;
        _ ->
            lager:info("[~p] stop session due to ~p", [SessionPid, Reason])
    end,
    case MRef of
        undefined -> ok;
        _ -> demonitor(MRef, [flush])
    end,
    case is_process_alive(SessionPid) of
        true ->
            vmq_session:stop(SessionPid);
        false ->
            ok
    end,
    case is_process_alive(WriterPid) of
        true ->
            ok = vmq_writer:flush_and_die(WriterPid);
        false -> ok
    end,
    Transport:close(Socket).


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

-spec socket_to_common_name({'sslsocket',_,pid() | {port(),_}}) -> 'undefined' | [any()] | {'error',[any()],binary() | maybe_improper_list(binary() | maybe_improper_list(any(),binary() | []) | char(),binary() | [])} | {'incomplete',[any()],binary()}.
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

-spec extract_cn({'rdnSequence',maybe_improper_list()}) -> 'undefined' | [any()] | {'error',[any()],binary() | maybe_improper_list(binary() | maybe_improper_list(any(),binary() | []) | char(),binary() | [])} | {'incomplete',[any()],binary()}.
extract_cn({rdnSequence, List}) ->
    extract_cn2(List).
-spec extract_cn2(maybe_improper_list()) -> 'undefined' | [any()] | {'error',[any()],binary() | maybe_improper_list(binary() | maybe_improper_list(any(),binary() | []) | char(),binary() | [])} | {'incomplete',[any()],binary()}.
extract_cn2([[#'AttributeTypeAndValue'{type=?'id-at-commonName', value={utf8String, CN}}]|_]) ->
    unicode:characters_to_list(CN);
extract_cn2([_|Rest]) ->
    extract_cn2(Rest);
extract_cn2([]) -> undefined.

tune_buffer_size(ranch_tcp, Sock) ->
    tune_buffer_size_(inet, Sock);
tune_buffer_size(ranch_ssl, Sock) ->
    tune_buffer_size_(ssl, Sock).

tune_buffer_size_(T, Sock) ->
    case T:getopts(Sock, [sndbuf, recbuf, buffer]) of
        {ok, BufSizes} -> BufSz = lists:max([Sz || {_Opt, Sz} <- BufSizes]),
                          T:setopts(Sock, [{buffer, BufSz}]);
        Error -> Error
    end.

proto_tag(ranch_tcp) -> {tcp, tcp_closed, tcp_error};
proto_tag(ranch_ssl) -> {ssl, ssl_closed, ssl_error}.
