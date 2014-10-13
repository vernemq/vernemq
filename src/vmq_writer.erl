-module(vmq_writer).
-export([start_link/3,
         send/2,
         flush/1,
         main_loop/3,
         recv_loop/3]).

-define(HIBERNATE_AFTER, 5000).

start_link(Transport, Socket, Reader) ->
    MaybeMaskedSocket =
    case Transport of
        ranch_ssl -> {ssl, Socket};
        _ -> Socket
    end,
    {ok, proc_lib:spawn_link(?MODULE, main_loop, [MaybeMaskedSocket, Reader, []])}.

send(WriterPid, Bin) when is_binary(Bin) ->
    WriterPid ! {send, Bin};
send(WriterPid, Frame) when is_tuple(Frame) ->
    WriterPid ! {send_frame, Frame}.

flush(WriterPid) ->
    {ok, ok} = gen:call(WriterPid, '$gen_call', flush, infinity),
    ok.

main_loop(Socket, Reader, Pending) ->
    try
        recv_loop(Socket, Reader, Pending)
    catch
        exit:Error ->
            Reader ! {?MODULE, exit, Error}
    end.

recv_loop(Socket, Reader, []) ->
    receive
        Message ->
            ?MODULE:recv_loop(Socket, Reader,
                         handle_message(Message, Socket, []))
    after
        ?HIBERNATE_AFTER ->
            erlang:hibernate(?MODULE, main_loop, [Socket, Reader, []])
    end;
recv_loop(Socket, Reader, Pending) ->
    receive
        Message ->
            ?MODULE:recv_loop(Socket, Reader,
                         handle_message(Message, Socket, Pending))
    after
        0 ->
            ?MODULE:recv_loop(Socket, Reader, internal_flush(Socket, Pending))
    end.

handle_message({send, Bin}, Socket, Pending) ->
    maybe_flush(Socket, [Bin|Pending]);
handle_message({send_frame, Frame}, Socket, Pending) ->
    Bin = emqtt_frame:serialise(Frame),
    maybe_flush(Socket, [Bin|Pending]);
handle_message({inet_reply, _, ok}, _Socket, Pending) ->
    Pending;
handle_message({inet_reply, _, Status}, _, _) ->
    exit({writer, send_failed, Status});
handle_message({'$gen_call', From, flush}, Socket, Pending) ->
    NewPending = internal_flush(Socket, Pending),
    gen_server:reply(From, ok),
    NewPending;
handle_message(Msg, _, _) ->
    exit({writer, unknown_message_type, Msg}).



%% This magic number is the tcp-over-ethernet MSS (1460) minus the
%% minimum size of a AMQP basic.deliver method frame (24) plus basic
%% content header (22). The idea is that we want to flush just before
%% exceeding the MSS.
-define(FLUSH_THRESHOLD, 1414).
maybe_flush(Socket, Pending) ->
    case iolist_size(Pending) >= ?FLUSH_THRESHOLD of
        true ->
            internal_flush(Socket, Pending);
        false ->
            Pending
    end.

internal_flush(_Socket, Pending = []) -> Pending;
internal_flush(Socket, Pending) ->
    ok = port_cmd(Socket, lists:reverse(Pending)),
    [].

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
    vmq_systree:incr_bytes_sent(iolist_size(Data)),
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

