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

-module(vmq_ranch).
-behaviour(ranch_protocol).

%% API.
-export([start_link/4]).

-export([init/4,
         loop/1]).

-record(st, {socket,
             buffer= <<>>,
             parser_state,
             session,
             proto_tag,
             pending=[],
             throttled=false,
             bytes_recv={os:timestamp(), 0},
             bytes_send={os:timestamp(), 0}}).

%% API.
start_link(Ref, Socket, Transport, Opts) ->
    Pid = proc_lib:spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
    {ok, Pid}.

send(TransportPid, Bin) when is_binary(Bin) ->
    TransportPid ! {send, Bin},
    ok;
send(TransportPid, Frames) when is_list(Frames) ->
    TransportPid ! {send_frames, Frames},
    ok;
send(TransportPid, Frame) when is_tuple(Frame) ->
    TransportPid ! {send_frames, [Frame]},
    ok.

init(Ref, Socket, Transport, Opts) ->
    ok = ranch:accept_ack(Ref),
    Self = self(),
    SendFun = fun(F) -> send(Self, F), ok end,
    NewOpts =
    case Transport of
        ranch_ssl ->
            case proplists:get_value(use_identity_as_username, Opts, false) of
                false ->
                    Opts;
                true ->
                    [{preauth, vmq_ssl:socket_to_common_name(Socket)}|Opts]
            end;
        _ ->
            Opts
    end,

    {ok, Peer} = Transport:peername(Socket),
    {ok, SessionPid} = vmq_session:start_link(Peer, SendFun, NewOpts),

    process_flag(trap_exit, true),
    MaskedSocket = mask_socket(Transport, Socket),
    %% tune buffer sizes
    {ok, BufSizes} = getopts(MaskedSocket, [sndbuf, recbuf, buffer]),
    BufSize = lists:max([Sz || {_, Sz} <- BufSizes]),
    setopts(MaskedSocket, [{buffer, BufSize}]),
    case active_once(MaskedSocket) of
        ok ->
            _ = vmq_exo:incr_socket_count(),
            loop(#st{socket=MaskedSocket,
                     session=SessionPid,
                     proto_tag=proto_tag(Transport)});
        {error, Reason} ->
            exit(Reason)
    end.

mask_socket(ranch_tcp, Socket) -> Socket;
mask_socket(ranch_ssl, Socket) -> {ssl, Socket}.

loop(State) ->
    loop_(State).

loop_(#st{pending=[]} = State) ->
    receive
        M ->
            loop_(handle_message(M, State))
    end;
loop_(#st{} = State) ->
    receive
        M ->
            loop_(handle_message(M, State))
    after
        0 ->
            loop_(internal_flush(State))
    end;
loop_({exit, Reason, State}) ->
    _ = internal_flush(State),
    teardown(State, Reason).

teardown(#st{session=SessionPid, socket=Socket}, Reason) ->
    case Reason of
        normal ->
            lager:debug("[~p] session normally stopped", [SessionPid]);
        shutdown ->
            lager:debug("[~p] session stopped due to shutdown", [SessionPid]);
        _ ->
            lager:warning("[~p] session stopped
                       abnormally due to ~p", [SessionPid, Reason])
    end,
    fast_close(Socket).


proto_tag(ranch_tcp) -> {tcp, tcp_closed, tcp_error};
proto_tag(ranch_ssl) -> {ssl, ssl_closed, ssl_error}.

fast_close({ssl, Socket}) ->
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
    ok;
fast_close(Socket) ->
    catch port_close(Socket),
    ok.

active_once({ssl, Socket}) ->
    ssl:setopts(Socket, [{active, once}]);
active_once(Socket) ->
    inet:setopts(Socket, [{active, once}]).

getopts({ssl, Socket}, Opts) ->
    ssl:getopts(Socket, Opts);
getopts(Socket, Opts) ->
    inet:getopts(Socket, Opts).

setopts({ssl, Socket}, Opts) ->
    ssl:setopts(Socket, Opts);
setopts(Socket, Opts) ->
    inet:setopts(Socket, Opts).

process_bytes(SessionPid, Bytes, undefined) ->
    process_bytes(SessionPid, Bytes, <<>>);
process_bytes(SessionPid, Bytes, ParserState) ->
    NewParserState = <<ParserState/binary, Bytes/binary>>,
    case vmq_parser:parse(NewParserState) of
        more ->
            {ok, NewParserState};
        {error, _} ->
            error;
        {Frame, Rest} ->
            Ret = vmq_session:in(SessionPid, Frame),
            case Ret of
                throttle ->
                    {throttled, Rest};
                _ ->
                    process_bytes(SessionPid, Rest, <<>>)
            end;
        error ->
            error
    end.

handle_message({Proto, _, Data}, #st{proto_tag={Proto, _, _}} = State) ->
    #st{session=SessionPid,
        socket=Socket,
        parser_state=ParserState,
        bytes_recv={TS, V}} = State,
    case process_bytes(SessionPid, Data, ParserState) of
        {ok, NewParserState} ->
            {M, S, _} = TS,
            NrOfBytes = byte_size(Data),
            BytesRecvLastSecond = V + NrOfBytes,
            NewBytesRecv =
            case os:timestamp() of
                {M, S, _} = NewTS ->
                    {NewTS, BytesRecvLastSecond};
                NewTS ->
                    _ = vmq_exo:incr_bytes_received(BytesRecvLastSecond),
                    {NewTS, 0}
            end,
            case active_once(Socket) of
                ok ->
                    State#st{parser_state=NewParserState,
                             bytes_recv=NewBytesRecv};
                {error, Reason} ->
                    {exit, Reason, State}
            end;
        {throttled, HoldBackBuf} ->
            erlang:send_after(1000, self(), restart_work),
            State#st{throttled=true, buffer=HoldBackBuf};
        error ->
            lager:debug("[~p][~p] parse error for data: ~p and  parser state: ~p",
                        [Proto, SessionPid, Data, ParserState]),
            {exit, parse_error, State}
    end;
handle_message({ProtoClosed, _}, #st{proto_tag={_, ProtoClosed, _}} = State) ->
    %% we regard a tcp_closed as 'normal'
    vmq_session:disconnect(State#st.session),
    {exit, normal, State};
handle_message({ProtoErr, _, Error}, #st{proto_tag={_, _, ProtoErr}} = State) ->
    {exit, Error, State};
handle_message({'EXIT', SessionPid, Reason}, #st{session=SessionPid} = State) ->
    {exit, Reason, State};
handle_message({send, Bin}, #st{pending=Pending} = State) ->
    maybe_flush(State#st{pending=[Bin|Pending]});
handle_message({send_frames, Frames}, State) ->
    send_frames(Frames, State);
handle_message({inet_reply, _, ok}, State) ->
    State;
handle_message({inet_reply, _, Status}, State) ->
    {exit, {send_failed, Status}, State};
handle_message(restart_work, #st{throttled=true} = State) ->
    #st{proto_tag={Proto, _, _}, buffer=Data, socket=Socket} = State,
    handle_message({Proto, Socket, Data}, State#st{throttled=false, buffer= <<>>});
handle_message(Msg, State) ->
    {exit, {unknown_message_type, Msg}, State}.

send_frames([Frame|Frames], #st{pending=Pending} = State) ->
    Bin = vmq_parser:serialise(Frame),
    send_frames(Frames, maybe_flush(State#st{pending=[Bin|Pending]}));
send_frames(_, State) -> State.

%% This magic number is the tcp-over-ethernet MSS (1460) minus the 4 byte
%% header of the Publish frame. The idea is that we want to flush just before
%% exceeding the MSS.
-define(FLUSH_THRESHOLD, 1456).
maybe_flush(#st{pending=Pending} = State) ->
    case iolist_size(Pending) >= ?FLUSH_THRESHOLD of
        true ->
            internal_flush(State);
        false ->
            State
    end.

internal_flush(#st{pending=[]} = State) -> State;
internal_flush(#st{pending=Pending, socket=Socket,
                   bytes_send={{M, S, _}, V}} = State) ->
    case port_cmd(Socket, lists:reverse(Pending)) of
        ok ->
            NrOfBytes = iolist_size(Pending),
            NewBytesSend =
            case os:timestamp() of
                {M, S, _} = TS ->
                    {TS, V + NrOfBytes};
                TS ->
                    _ = vmq_exo:incr_bytes_sent(V + NrOfBytes),
                    {TS, 0}
            end,
            State#st{pending=[], bytes_send=NewBytesSend};
        {error, Reason} ->
            {exit, Reason, State}
    end.

%% gen_tcp:send/2 does a selective receive of {inet_reply, Sock,
%% Status} to obtain the result. That is bad when it is called from
%% the writer since it requires scanning of the writers possibly quite
%% large message queue.
%%
%% So instead we lift the code from prim_inet:send/2, which is what
%% gen_tcp:send/2 calls, do the first half here and then just process
%% the result code in handle_message/3 as and when it arrives.
%%
%% This means we may end up happily sending data down a closed/broken
%% socket, but that's ok since a) data in the buffers will be lost in
%% any case (so qualitatively we are no worse off than if we used
%% gen_tcp:send/2), and b) we do detect the changed socket status
%% eventually, i.e. when we get round to handling the result code.
%%
%% Also note that the port has bounded buffers and port_command blocks
%% when these are full. So the fact that we process the result
%% asynchronously does not impact flow control.
port_cmd(Socket, Data) ->
    try
        port_cmd_(Socket, Data),
        ok
    catch
        error:Error ->
            {error, Error}
    end.

port_cmd_({ssl, Socket}, Data) ->
    case ssl:send(Socket, Data) of
        ok ->
            self() ! {inet_reply, Socket, ok},
            true;
        {error, Reason} ->
            erlang:error(Reason)
    end;
port_cmd_(Socket, Data) ->
    erlang:port_command(Socket, Data).


