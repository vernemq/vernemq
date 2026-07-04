%% Copyright 2018 Erlio GmbH Basel Switzerland (http://erl.io)
%% Copyright 2018-2024 Octavo Labs/VerneMQ (https://vernemq.com/)
%% and Individual Contributors.
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
%%
-module(vmq_cluster_node).
-include("vmq_server.hrl").
-include_lib("kernel/include/logger.hrl").

%% API
-export([
    start_link/1,
    publish/2,
    enqueue/4,
    enqueue_async/3,
    connect_params/1,
    status/1
]).

%% gen_server callbacks
-export([
    init/1,
    loop/1
]).

-export([system_continue/3]).
-export([system_terminate/4]).
-export([system_code_change/4]).

-record(state, {
    parent,
    node,
    socket,
    transport,
    reachable = false,
    handshake = none,
    handshake_tref,
    handshake_buffer = <<>>,
    delayed_publish_replies = [],
    pending = [],
    pending_bytes = 0,
    max_queue_size,
    flush_threshold,
    reconnect_tref,
    async_connect_pid,
    bytes_dropped = {os:timestamp(), 0},
    bytes_send = {os:timestamp(), 0}
}).

-define(RECONNECT, 1000).
-define(CONNECT_ACK, <<"vmq-connect-ack">>).

%%%===================================================================
%%% API
%%%===================================================================

start_link(RemoteNode) ->
    proc_lib:start_link(?MODULE, init, [[self(), RemoteNode]]).

publish(Pid, Msg) ->
    Ref = make_ref(),
    MRef = monitor(process, Pid),
    Pid ! {msg, self(), Ref, Msg},
    receive
        {Ref, Reply} ->
            demonitor(MRef, [flush]),
            Reply;
        {'DOWN', MRef, process, Pid, Reason} ->
            {error, Reason}
    end.

enqueue(Pid, Term, BufferIfUnreachable, Timeout) ->
    Ref = make_ref(),
    MRef = monitor(process, Pid),
    Pid ! {enq, self(), Ref, Term, BufferIfUnreachable},
    case Timeout of
        infinity ->
            receive
                {Ref, Reply} ->
                    demonitor(MRef, [flush]),
                    Reply;
                {'DOWN', MRef, process, Pid, Reason} ->
                    {error, Reason}
            end;
        _ ->
            receive
                {Ref, Reply} ->
                    demonitor(MRef, [flush]),
                    Reply;
                {'DOWN', MRef, process, Pid, Reason} ->
                    {error, Reason}
            after Timeout ->
                demonitor(MRef, [flush]),
                {error, timeout}
            end
    end.

enqueue_async(Pid, Term, BufferIfUnreachable) ->
    Ref = make_ref(),
    MRef = monitor(process, Pid),
    Pid ! {enq, self(), Ref, Term, BufferIfUnreachable},
    {MRef, Ref}.

status(Pid) ->
    Ref = make_ref(),
    MRef = monitor(process, Pid),
    Pid ! {status, self(), Ref},
    receive
        {Ref, Reply} ->
            demonitor(MRef, [flush]),
            Reply;
        {'DOWN', MRef, process, Pid, Reason} ->
            {error, Reason}
    end.

init([Parent, RemoteNode]) ->
    MaxQueueSize = vmq_config:get_env(outgoing_clustering_buffer_size),
    FlushThreshold = vmq_config:get_env(outgoing_clustering_flush_threshold, 65536),
    proc_lib:init_ack(Parent, {ok, self()}),
    % Delay the initial connect attempt, this is useful when automating
    % cluster node setup, where multiple nodes are concurrently setup.
    % Without a delay a node may try to connect to a cluster node that
    % hasn't finished setting up the vmq cluster listener.
    erlang:send_after(1000, self(), reconnect),
    loop(#state{
        parent = Parent,
        node = RemoteNode,
        max_queue_size = MaxQueueSize,
        flush_threshold = FlushThreshold
    }).

loop(#state{pending = Pending, reachable = Reachable} = State) when
    Pending == [];
    Reachable == false
->
    receive
        M ->
            loop(handle_message(M, State))
    end;
loop(#state{} = State) ->
    receive
        M ->
            loop(handle_message(M, State))
    after 0 ->
        loop(internal_flush(State))
    end.

buffer_message(
    BinMsg,
    #state{
        pending = Pending,
        pending_bytes = PendingBytes,
        max_queue_size = Max,
        reachable = Reachable,
        bytes_dropped = {{M, S, _}, V}
    } = State
) ->
    {NewPending, Dropped} =
        case Reachable of
            true ->
                {[BinMsg | Pending], 0};
            false ->
                case PendingBytes < Max of
                    true ->
                        {[BinMsg | Pending], 0};
                    false ->
                        {Pending, byte_size(BinMsg)}
                end
        end,
    NewPendingBytes = PendingBytes + byte_size(BinMsg) - Dropped,
    NewBytesDropped =
        case os:timestamp() of
            {M, S, _} = TS ->
                {TS, V + Dropped};
            TS ->
                _ = vmq_metrics:incr_cluster_bytes_dropped(V + Dropped),
                {TS, 0}
        end,
    {Dropped, maybe_flush(State#state{
        pending = NewPending,
        pending_bytes = NewPendingBytes,
        bytes_dropped = NewBytesDropped
    })}.

handle_message(
    {enq, CallerPid, Ref, _, BufferIfUnreachable},
    #state{reachable = false} = State
) when
    BufferIfUnreachable =:= false
->
    CallerPid ! {Ref, {error, not_reachable}},
    State;
handle_message({enq, CallerPid, Ref, Term, _}, State) ->
    Bin = term_to_binary({CallerPid, Ref, Term}),
    L = byte_size(Bin),
    BinMsg = <<"enq", L:32, Bin/binary>>,
    %% buffering is allowed, but will only happen if the remote node
    %% is unreachable
    {Dropped, NewState} = buffer_message(BinMsg, State),
    case Dropped > 0 of
        true ->
            CallerPid ! {Ref, {error, msg_dropped}};
        false ->
            %% reply directly from other node
            ignore
    end,
    NewState;
handle_message({msg, CallerPid, Ref, Msg}, State) ->
    Bin = term_to_binary(Msg),
    L = byte_size(Bin),
    BinMsg = <<"msg", L:32, Bin/binary>>,
    {Dropped, NewState} = buffer_message(BinMsg, State),
    case Dropped > 0 of
        true ->
            CallerPid ! {Ref, {error, msg_dropped}},
            NewState;
        false when NewState#state.handshake =:= pending ->
            maybe_delay_publish_reply(CallerPid, Ref, NewState);
        false ->
            CallerPid ! {Ref, ok},
            NewState
    end;
handle_message(
    {connect_async_done, AsyncPid, {ok, {Transport, Socket}}},
    #state{async_connect_pid = AsyncPid, node = RemoteNode} = State
) ->
    NodeName = term_to_binary(node()),
    L = byte_size(NodeName),
    Msg = [<<"vmq-connect">>, <<L:32, NodeName/binary>>],
    case send(Transport, Socket, Msg) of
        ok ->
            ?LOG_INFO("successfully connected to cluster node ~p", [RemoteNode]),
            HandshakeTRef = handshake_timer(),
            State#state{
                socket = Socket,
                transport = Transport,
                %% Wait for a new-peer ack, or fall back to legacy mode after a short timeout.
                async_connect_pid = undefined,
                reachable = false,
                handshake = pending,
                handshake_tref = HandshakeTRef,
                handshake_buffer = <<>>
            };
        {error, Reason} ->
            ?LOG_WARNING("can't initiate connect to cluster node ~p due to ~p", [
                RemoteNode, Reason
            ]),
            close_reconnect(State)
    end;
handle_message({connect_async_done, AsyncPid, error}, #state{async_connect_pid = AsyncPid} = State) ->
    % connect_async already logged the error details
    close_reconnect(State);
handle_message(handshake_timeout, #state{handshake = pending, node = RemoteNode} = State) ->
    ?LOG_DEBUG("cluster node ~p did not send connect ack, assuming legacy peer", [RemoteNode]),
    handshake_complete(legacy, State#state{handshake_tref = undefined});
handle_message(handshake_timeout, State) ->
    State;
handle_message({tcp, Socket, Data}, #state{socket = Socket} = State) ->
    handle_socket_data(Data, State);
handle_message({ssl, Socket, Data}, #state{socket = {ssl, Socket}} = State) ->
    handle_socket_data(Data, State);
handle_message(reconnect, #state{reachable = false} = State) ->
    connect(State#state{reconnect_tref = undefined});
handle_message({status, CallerPid, Ref}, #state{socket = Socket, reachable = Reachable} = State) ->
    Status =
        case Reachable of
            true ->
                up;
            false when Socket == undefined ->
                init;
            false ->
                down
        end,
    CallerPid ! {Ref, Status},
    State;
handle_message({system, From, Request}, #state{parent = Parent} = State) ->
    sys:handle_system_msg(Request, From, Parent, ?MODULE, [], State);
handle_message({NetEvClosed, Socket}, #state{node = RemoteNode, socket = Socket} = State) when
    NetEvClosed == tcp_closed;
    NetEvClosed == ssl_closed
->
    ?LOG_WARNING(
        "connection to node ~p has been closed, reconnect in ~pms",
        [RemoteNode, ?RECONNECT]
    ),
    close_reconnect(State);
handle_message(
    {NetEvError, Socket, Reason}, #state{node = RemoteNode, socket = Socket} = State
) when
    NetEvError == tcp_error;
    NetEvError == ssl_error
->
    ?LOG_WARNING(
        "connection to node ~p has been closed due to error ~p, reconnect in ~pms",
        [RemoteNode, Reason, ?RECONNECT]
    ),
    close_reconnect(State);
handle_message(Msg, #state{node = Node, reachable = Reachable} = State) ->
    ?LOG_WARNING(
        "got unknown message ~p for node ~p (reachable ~p)",
        [Msg, Node, Reachable]
    ),
    State.

maybe_flush(#state{pending_bytes = PendingBytes, flush_threshold = FlushThreshold} = State) ->
    case PendingBytes >= FlushThreshold of
        true ->
            internal_flush(State);
        false ->
            State
    end.

internal_flush(#state{reachable = false} = State) ->
    State;
internal_flush(#state{pending = []} = State) ->
    State;
internal_flush(
    #state{
        pending = Pending,
        pending_bytes = PendingBytes,
        node = Node,
        transport = Transport,
        socket = Socket,
        bytes_send = {{M, S, _}, V}
    } = State
) ->
    L = PendingBytes,
    Msg = [<<"vmq-send", L:32>> | lists:reverse(Pending)],
    case send(Transport, Socket, Msg) of
        ok ->
            NewBytesSend =
                case os:timestamp() of
                    {M, S, _} = TS ->
                        {TS, V + L};
                    TS ->
                        _ = vmq_metrics:incr_cluster_bytes_sent(V + L),
                        {TS, 0}
                end,
            State#state{pending = [], pending_bytes = 0, bytes_send = NewBytesSend};
        {error, Reason} ->
            ?LOG_WARNING(
                "can't send ~p bytes to ~p due to ~p, reconnect!",
                [iolist_size(Pending), Node, Reason]
            ),
            close_reconnect(State)
    end.

connect(#state{node = RemoteNode, reachable = false} = State) ->
    Self = self(),
    ConnectAsyncPid = spawn_link(fun() -> connect_async(Self, RemoteNode) end),
    State#state{async_connect_pid = ConnectAsyncPid}.

connect_async(ParentPid, RemoteNode) ->
    ConnectOpts = vmq_config:get_env(outgoing_connect_options),
    % the outgoing_connect_params_module must implement the connect_params/1 function
    ConnectParamsMod = vmq_config:get_env(outgoing_connect_params_module),
    ConnectTimeout = vmq_config:get_env(outgoing_connect_timeout),
    Reply =
        case rpc:call(RemoteNode, ConnectParamsMod, connect_params, [node()]) of
            {Transport, Host, Port} ->
                case
                    connect(
                        Transport,
                        Host,
                        Port,
                        lists:usort([
                            binary,
                            {active, true}
                            | ConnectOpts
                        ]),
                        ConnectTimeout
                    )
                of
                    {ok, Socket} ->
                        % at least tune 'buffer'
                        MaskedSocket = mask_socket(Transport, Socket),
                        {ok, BufSizes} = getopts(MaskedSocket, [sndbuf, recbuf, buffer]),
                        BufSize = lists:max([Sz || {_, Sz} <- BufSizes]),
                        setopts(MaskedSocket, [{buffer, BufSize}]),
                        case controlling_process(Transport, MaskedSocket, ParentPid) of
                            ok ->
                                {ok, {Transport, MaskedSocket}};
                            {error, Reason} ->
                                ?LOG_DEBUG("can't assign socket ownership to ~p due to ~p", [
                                    ParentPid, Reason
                                ]),
                                error
                        end;
                    {error, Reason} ->
                        ?LOG_WARNING("can't connect to cluster node ~p due to ~p", [
                            RemoteNode, Reason
                        ]),
                        error
                end;
            {badrpc, nodedown} ->
                %% we don't scream.. vmq_cluster_mon screams
                error;
            E ->
                ?LOG_WARNING("can't connect to cluster node ~p due to ~p", [RemoteNode, E]),
                error
        end,
    ParentPid ! {connect_async_done, self(), Reply}.

close_reconnect(#state{transport = Transport, socket = Socket} = State) ->
    cancel_timer(State#state.handshake_tref),
    close(Transport, Socket),
    State#state{
        async_connect_pid = undefined,
        reachable = false,
        socket = undefined,
        handshake = none,
        handshake_tref = undefined,
        handshake_buffer = <<>>,
        reconnect_tref = reconnect_timer()
    }.

reconnect_timer() ->
    erlang:send_after(?RECONNECT, self(), reconnect).

handshake_timer() ->
    Timeout = vmq_config:get_env(outgoing_cluster_handshake_ack_timeout, 250),
    erlang:send_after(Timeout, self(), handshake_timeout).

cancel_timer(undefined) ->
    ok;
cancel_timer(TRef) ->
    _ = erlang:cancel_timer(TRef),
    ok.

maybe_delay_publish_reply(CallerPid, Ref, #state{handshake = pending} = State) ->
    State#state{delayed_publish_replies = [{CallerPid, Ref} | State#state.delayed_publish_replies]};
maybe_delay_publish_reply(_, _, State) ->
    State.

handshake_complete(Handshake, #state{} = State) ->
    cancel_timer(State#state.handshake_tref),
    lists:foreach(
        fun({CallerPid, Ref}) -> CallerPid ! {Ref, ok} end,
        lists:reverse(State#state.delayed_publish_replies)
    ),
    maybe_flush(State#state{
        reachable = true,
        handshake = Handshake,
        handshake_tref = undefined,
        handshake_buffer = <<>>,
        delayed_publish_replies = []
    }).

handle_socket_data(Data, #state{handshake = pending} = State) ->
    Ack = ?CONNECT_ACK,
    AckSize = byte_size(Ack),
    Buffer = <<(State#state.handshake_buffer)/binary, Data/binary>>,
    case Buffer of
        <<Ack:AckSize/binary, _/binary>> ->
            handshake_complete(acked, State);
        _ when byte_size(Buffer) < AckSize ->
            State#state{handshake_buffer = Buffer};
        _ ->
            ?LOG_DEBUG("ignoring unexpected cluster handshake data ~p", [Buffer]),
            State#state{handshake_buffer = <<>>}
    end;
handle_socket_data(Data, State) ->
    ?LOG_DEBUG("ignoring unexpected data on outgoing cluster socket: ~p", [Data]),
    State.

%% connect_params is called by a RPC
connect_params(_Node) ->
    case whereis(vmq_server_sup) of
        undefined ->
            %% vmq_server app not ready
            {error, not_ready};
        _ ->
            Listeners = vmq_config:get_env(listeners),
            MaybeSSLConfig = proplists:get_value(vmqs, Listeners, []),
            case connect_params(ssl, MaybeSSLConfig) of
                no_config ->
                    case proplists:get_value(vmq, Listeners) of
                        undefined ->
                            exit("can't connect to cluster node");
                        Config ->
                            connect_params(tcp, Config)
                    end;
                Config ->
                    Config
            end
    end.
mask_socket(gen_tcp, Socket) -> Socket;
mask_socket(ssl, Socket) -> {ssl, Socket}.
getopts({ssl, Socket}, Opts) ->
    ssl:getopts(Socket, Opts);
getopts(Socket, Opts) ->
    inet:getopts(Socket, Opts).

setopts({ssl, Socket}, Opts) ->
    ssl:setopts(Socket, Opts);
setopts(Socket, Opts) ->
    inet:setopts(Socket, Opts).

connect_params(tcp, [{{Addr, Port}, _} | _]) ->
    {gen_tcp, Addr, Port};
connect_params(ssl, [{{Addr, Port}, _} | _]) ->
    {ssl, Addr, Port};
connect_params(_, []) ->
    no_config.

send(gen_tcp, Socket, Msg) ->
    gen_tcp:send(Socket, Msg);
send(ssl, {'ssl', Socket}, Msg) ->
    ssl:send(Socket, Msg).

close(_, undefined) -> ok;
close(gen_tcp, Socket) -> gen_tcp:close(Socket);
close(ssl, {'ssl', Socket}) -> ssl:close(Socket).

connect(gen_tcp, Host, Port, Opts, Timeout) ->
    gen_tcp:connect(Host, Port, Opts, Timeout);
connect(ssl, Host, Port, Opts, Timeout) ->
    ssl:connect(Host, Port, Opts, Timeout).

controlling_process(gen_tcp, Socket, Pid) ->
    gen_tcp:controlling_process(Socket, Pid);
controlling_process(ssl, {'ssl', Socket}, Pid) ->
    ssl:controlling_process(Socket, Pid).

teardown(#state{socket = Socket, transport = Transport, async_connect_pid = AsyncPid}, Reason) ->
    case AsyncPid of
        undefined -> ignore;
        Pid -> exit(Pid, normal)
    end,
    case Reason of
        normal ->
            ?LOG_DEBUG("normally stopped", []);
        shutdown ->
            ?LOG_DEBUG("stopped due to shutdown", []);
        _ ->
            ?LOG_WARNING("stopped abnormally due to '~p'", [Reason])
    end,
    close(Transport, Socket),
    ok.

system_continue(_, _, State) ->
    loop(State).

-spec system_terminate(any(), _, _, _) -> no_return().
system_terminate(Reason, _, _, State) -> teardown(State, Reason).

system_code_change(Misc, _, _, _) ->
    {ok, Misc}.
