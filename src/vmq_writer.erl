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

-module(vmq_writer).
-export([start_link/3,
         send/2,
         main_loop/1,
         recv_loop/1]).

-record(state, {socket,
                handler,
                pending=[],
                bytes_send={os:timestamp(), 0}}).

-define(HIBERNATE_AFTER, 5000).

start_link(Transport, Handler, Socket) ->
    MaybeMaskedSocket =
    case Transport of
        ssl -> {ssl, Socket};
        _ -> Socket
    end,
    {ok, proc_lib:spawn_link(?MODULE, main_loop,
                             [#state{handler=Handler,
                                     socket=MaybeMaskedSocket}])}.

send(WriterPid, Bin) when is_binary(Bin) ->
    WriterPid ! {send, Bin},
    ok;
send(WriterPid, [F|_] = Frames) when is_tuple(F) ->
    WriterPid ! {send_frames, Frames},
    ok;
send(WriterPid, Frame) when is_tuple(Frame) ->
    WriterPid ! {send_frames, [Frame]},
    ok.

main_loop(State) ->
    try
        recv_loop(State)
    catch
        exit:_Reason ->
            internal_flush(State),
            exit(normal)
    end.

recv_loop(#state{pending=[]} = State) ->
    receive
        Message ->
            NewState = handle_message(Message, State),
            ?MODULE:recv_loop(NewState)
    after
        ?HIBERNATE_AFTER ->
            erlang:hibernate(?MODULE, main_loop, [State])
    end;
recv_loop(State) ->
    receive
        Message ->
            NewState = handle_message(Message, State),
            ?MODULE:recv_loop(NewState)
    after
        0 ->
            NewState = internal_flush(State),
            ?MODULE:recv_loop(NewState)
    end.


handle_message({send, Bin}, #state{pending=Pending, handler=Handler} = State) ->
    maybe_flush(State#state{pending=[reprocess(Handler, Bin)|Pending]});
handle_message({send_frames, Frames}, #state{handler=Handler} = State) ->
    lists:foldl(fun(Frame, #state{pending=AccPending} = AccSt) ->
                        Bin = emqtt_frame:serialise(Frame),
                        maybe_flush(AccSt#state{
                                      pending=[reprocess(Handler, Bin)
                                               |AccPending]})
                end, State, Frames);
handle_message({inet_reply, _, ok}, State) ->
    State;
handle_message({inet_reply, _, Status}, _) ->
    exit({writer, send_failed, Status});
handle_message(Msg, _) ->
    exit({writer, unknown_message_type, Msg}).

reprocess(vmq_tcp_listener, Bin) -> Bin;
reprocess(vmq_ssl_listener, Bin) -> Bin;
reprocess(vmq_ws_listener, Bin) ->
    wsock_message:encode(Bin, [mask, binary]);
reprocess(vmq_wss_listener, Bin) ->
    wsock_message:encode(Bin, [mask, binary]).

%% This magic number is the tcp-over-ethernet MSS (1460) minus the
%% minimum size of a AMQP basic.deliver method frame (24) plus basic
%% content header (22). The idea is that we want to flush just before
%% exceeding the MSS.
-define(FLUSH_THRESHOLD, 1414).
maybe_flush(#state{pending=Pending} = State) ->
    case iolist_size(Pending) >= ?FLUSH_THRESHOLD of
        true ->
            internal_flush(State);
        false ->
            State
    end.

internal_flush(#state{pending=[]} = State) -> State;
internal_flush(#state{pending=Pending, socket=Socket,
                      bytes_send={{M, S, _}, V}} = State) ->
    ok = port_cmd(Socket, lists:reverse(Pending)),
    NrOfBytes = iolist_size(Pending),
    NewBytesSend =
    case os:timestamp() of
        {M, S, _} = TS ->
            {TS, V + NrOfBytes};
        TS ->
            vmq_systree:incr_bytes_sent(V + NrOfBytes),
            {TS, 0}
    end,
    State#state{pending=[], bytes_send=NewBytesSend}.

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

