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

-module(vmq_tcp_transport).

-export([start_link/3,
         handover/2,
         send/2]).
-export([init/3,
         loop/1]).

-export([behaviour_info/1,
         upgrade_connection/2,
         opts/1,
         port_cmd/2]).

-record(st, {socket,
             handler, buffer= <<>>,
             parser_state,
             session, session_mon,
             proto_tag,
             pending=[],
             bytes_recv={os:timestamp(), 0},
             bytes_send={os:timestamp(), 0}}).

-define(HIBERNATE_AFTER, 5000).

-spec start_link(_, _, _) -> {'ok', pid()}.
start_link(Peer, Handler, Opts) ->
    Pid = proc_lib:spawn_link(?MODULE, init, [Peer, Handler, Opts]),
    {ok, Pid}.

%% called from vmq_tcp_listener
upgrade_connection(TcpSocket, _) ->
    {ok, {TcpSocket, []}}.

opts(_) -> [].

behaviour_info(callbacks) ->
    [{opts, 1}, {upgrade_connection, 2},
     {decode_bin, 3}, {encode_bin, 1}];
behaviour_info(_) ->
    undefined.

send(TransportPid, Bin) when is_binary(Bin) ->
    TransportPid ! {send, Bin},
    ok;
send(TransportPid, [F|_] = Frames) when is_tuple(F) ->
    TransportPid ! {send_frames, Frames},
    ok;
send(TransportPid, Frame) when is_tuple(Frame) ->
    TransportPid ! {send_frames, [Frame]},
    ok.

handover(ReaderPid, Socket) ->
    ReaderPid ! {handover, Socket}.

-spec init(_, _, _) -> any().
init(Peer, Handler, Opts) ->
    Self = self(),
    SendFun = fun(F) -> vmq_tcp_transport:send(Self, F), ok end,
    {ok, SessionPid} = vmq_session:start_link(Peer, SendFun, Opts),
    process_flag(trap_exit, true),
    wait_for_socket(#st{handler=Handler,
                        session=SessionPid,
                        proto_tag=proto_tag(Handler)}).

wait_for_socket(State) ->
    receive
        {handover, Socket} ->
            MaybeMaskedSocket =
            case element(1, State#st.proto_tag) of
                ssl -> {ssl, Socket};
                _ -> Socket
            end,
            vmq_plugin:all(incr_socket_count, []),
            active_once(MaybeMaskedSocket),
            loop(State#st{socket=MaybeMaskedSocket});
        M ->
            exit({unexpected_msg, M})
    after
        5000 ->
            exit(socket_handover_timeout)
    end.

loop(State) ->
    loop_(State).

loop_(#st{pending=[]} = State) ->
    receive
        M ->
            loop_(handle_message(M, State))
    after
        ?HIBERNATE_AFTER ->
            erlang:hibernate(?MODULE, loop, [State])
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
    teardown(State, Reason).

teardown(#st{session=SessionPid, socket=Socket}, Reason) ->
    case Reason of
        normal ->
            lager:debug("[~p] session stopped", [SessionPid]);
        _ ->
            lager:warning("[~p] session stopped
                       abnormally due to ~p", [SessionPid, Reason])
    end,
    fast_close(Socket).


proto_tag(vmq_ws_transport) -> proto_tag(?MODULE);
proto_tag(vmq_wss_transport) -> proto_tag(vmq_ssl_transport);
proto_tag(?MODULE) -> {tcp, tcp_closed, tcp_error};
proto_tag(vmq_ssl_transport) -> {ssl, ssl_closed, ssl_error}.

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

process_data(SessionPid, ?MODULE, _, Data, ParserState) ->
    process_bytes(SessionPid, Data, ParserState);
process_data(SessionPid, vmq_ssl_transport, _, Data, ParserState) ->
    process_bytes(SessionPid, Data, ParserState);
process_data(SessionPid, TransportMod, Socket, Data, ParserState) ->
    case apply(TransportMod, decode_bin, [Socket, Data, ParserState]) of
        {ok, Bytes, {WSPs, NewParserState}} ->
            {WSPs, process_bytes(SessionPid, Bytes, NewParserState)};
        {more, NewParserState} ->
            NewParserState
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
        {error, _Reason} ->
            emqtt_frame:initial_state()
    end.

handle_message({reconfigure_session, {CallerPid, CallerRef}, NewConfig}, State) ->
    ok = vmq_session:reconfigure(State#st.session, NewConfig),
    CallerPid ! {CallerRef, ok},
    State;

handle_message({Proto, _, Data}, #st{proto_tag={Proto, _, _}} = State) ->
    #st{session=SessionPid,
        socket=Socket,
        handler=Handler,
        parser_state=ParserState,
        bytes_recv={TS, V}} = State,
    NewParserState = process_data(SessionPid, Handler,
                                  Socket, Data, ParserState),
    {M, S, _} = TS,
    NrOfBytes = byte_size(Data),
    NewBytesRecv =
    case os:timestamp() of
        {M, S, _} = NewTS ->
            {NewTS, V + NrOfBytes};
        NewTS ->
            vmq_plugin:all(incr_bytes_received, [V + NrOfBytes]),
            {NewTS, 0}
    end,
    active_once(Socket),
    State#st{parser_state=NewParserState, bytes_recv=NewBytesRecv};
handle_message({ProtoClosed, _}, #st{proto_tag={_, ProtoClosed, _}} = State) ->
    %% we regard a tcp_closed as 'normal'
    {exit, normal, State};
handle_message({ProtoErr, _, Error}, #st{proto_tag={_, _, ProtoErr}} = State) ->
    {exit, Error, State};
handle_message({'EXIT', _, Reason}, State) ->
    {exit, Reason, State};
handle_message({send, Bin}, #st{pending=Pending, handler=Handler} = State) ->
    maybe_flush(State#st{pending=[reprocess(Handler, Bin)|Pending]});
handle_message({send_frames, Frames}, #st{handler=Handler} = State) ->
    lists:foldl(fun(Frame, #st{pending=AccPending} = AccSt) ->
                        Bin = emqtt_frame:serialise(Frame),
                        maybe_flush(AccSt#st{
                                      pending=[reprocess(Handler, Bin)
                                               |AccPending]})
                end, State, Frames);
handle_message({inet_reply, _, ok}, State) ->
    State;
handle_message({inet_reply, _, Status}, _) ->
    exit({vmq_tcp_transport, send_failed, Status});
handle_message(Msg, _) ->
    exit({vmq_tcp_transport, unknown_message_type, Msg}).

reprocess(vmq_tcp_transport, Bin) -> Bin;
reprocess(TransportMod, Bin) ->
    apply(TransportMod, encode_bin, [Bin]).

%% This magic number is the tcp-over-ethernet MSS (1460) minus the
%% minimum size of a AMQP basic.deliver method frame (24) plus basic
%% content header (22). The idea is that we want to flush just before
%% exceeding the MSS.
-define(FLUSH_THRESHOLD, 1414).
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
    ok = port_cmd(Socket, lists:reverse(Pending)),
    NrOfBytes = iolist_size(Pending),
    NewBytesSend =
    case os:timestamp() of
        {M, S, _} = TS ->
            {TS, V + NrOfBytes};
        TS ->
            vmq_plugin:all(incr_bytes_sent, [V + NrOfBytes]),
            {TS, 0}
    end,
    State#st{pending=[], bytes_send=NewBytesSend}.

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
    true =
    try port_cmd_(Socket, Data)
    catch error:Error ->
              exit({writer, send_failed, Error})
    end,
    ok.

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

