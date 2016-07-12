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
%%
-module(vmq_cluster_node).
-include("vmq_server.hrl").

%% API
-export([start_link/1,
         publish/2,
         enqueue/2,
         connect_params/1]).

%% gen_server callbacks
-export([init/1]).

-record(state, {node,
                socket,
                transport,
                reachable=false,
                pending = [],
                max_queue_size,
                reconnect_tref,
                bytes_dropped={os:timestamp(), 0},
                bytes_send={os:timestamp(), 0}}).

-define(RECONNECT, 1000).

%%%===================================================================
%%% API
%%%===================================================================

start_link(RemoteNode) ->
    proc_lib:start_link(?MODULE, init, [[self(), RemoteNode]]).

publish(Pid, Msg) ->
    Pid ! {msg, Msg},
    ok.

enqueue(Pid, Term) ->
    Ref = make_ref(),
    MRef = monitor(process, Pid),
    Pid ! {enq, self(), Ref, Term},
    receive
        {Ref, Reply} ->
            demonitor(MRef, [flush]),
            Reply;
        {'DOWN', MRef, process, Pid, Reason} ->
            {error, Reason}
    end.


init([Parent, RemoteNode]) ->
    MaxQueueSize = vmq_config:get_env(outgoing_clustering_buffer_size),
    proc_lib:init_ack(Parent, {ok, self()}),
    self() ! reconnect, %% initial connect
    loop(#state{node=RemoteNode, max_queue_size=MaxQueueSize}).

loop(#state{pending=[]} = State) ->
    receive
        M ->
            loop(handle_message(M, State))
    end;
loop(#state{} = State) ->
    receive
        M ->
            loop(handle_message(M, State))
    after
        0 ->
            loop(internal_flush(State))
    end.

buffer_message(BinMsg, #state{pending=Pending, max_queue_size=Max,
                              reachable=Reachable,
                              bytes_dropped={{M, S, _}, V}} = State) ->
    {NewPending, Dropped} =
    case Reachable of
        true ->
            {[BinMsg|Pending], 0};
        false ->
            case iolist_size(Pending) < Max of
                true ->
                    {[BinMsg|Pending], 0};
                false ->
                    {Pending, byte_size(BinMsg)}
            end
    end,
    NewBytesDropped =
    case os:timestamp() of
        {M, S, _} = TS ->
            {TS, V + Dropped};
        TS ->
            _ = vmq_metrics:incr_cluster_bytes_dropped(V + Dropped),
            {TS, 0}
    end,
    {Dropped, maybe_flush(State#state{pending=NewPending, bytes_dropped=NewBytesDropped})}.


handle_message({enq, CallerPid, Ref, Term}, State) ->
    Bin = term_to_binary({CallerPid, Ref, Term}),
    L = byte_size(Bin),
    BinMsg = <<"enq", L:32, Bin/binary>>,
    {Dropped, NewState} = buffer_message(BinMsg, State),
    case Dropped > 0 of
        true ->
            CallerPid ! {Ref, {error, msg_dropped}};
        false ->
            %% reply directly from other node
            ignore
    end,
    NewState;
handle_message({msg, Msg}, State) ->
    Bin = term_to_binary(Msg),
    L = byte_size(Bin),
    BinMsg = <<"msg", L:32, Bin/binary>>,
    {_, NewState} = buffer_message(BinMsg, State),
    NewState;
handle_message({NetEv, _}, #state{reconnect_tref=TRef} = State)
  when
      NetEv == tcp_closed;
      NetEv == tcp_error;
      NetEv == ssl_closed;
      NetEv == ssl_error ->
    NewTRef =
    case TRef of
        undefined ->
            reconnect_timer();
        _ ->
            %% we're already reconnecting
            TRef
    end,
    State#state{reachable=false, reconnect_tref=NewTRef};
handle_message(reconnect, #state{reachable=false} = State) ->
    connect(State#state{reconnect_tref=undefined});
handle_message(Msg, #state{node=Node, reachable=Reachable} = State) ->
    lager:warning("got unknown message ~p for node ~p (reachable ~p)",
                  [Msg, Node, Reachable]),
    State.

-define(FLUSH_THRESHOLD, 1460). % tcp-over-ethernet MSS 1460
maybe_flush(#state{pending=Pending} = State) ->
    case iolist_size(Pending) >= ?FLUSH_THRESHOLD of
        true ->
            internal_flush(State);
        false ->
            State
    end.

internal_flush(#state{reachable=false} = State) -> State;
internal_flush(#state{pending=[]} = State) -> State;
internal_flush(#state{pending=Pending, node=Node, transport=Transport,
                      socket=Socket, bytes_send={{M, S, _}, V}} = State) ->
    L = iolist_size(Pending),
    Msg = [<<"vmq-send", L:32>>|lists:reverse(Pending)],
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
            State#state{pending=[], bytes_send=NewBytesSend};
        {error, Reason} ->
            lager:warning("can't send ~p bytes to ~p due to ~p, reconnect!",
                          [iolist_size(Pending), Node, Reason]),
            State#state{reachable=false, reconnect_tref=reconnect_timer()}
    end.

connect(#state{node=RemoteNode} = State) ->
    ConnectOpts = vmq_config:get_env(outgoing_connect_opts),
    case rpc:call(RemoteNode, ?MODULE, connect_params, [node()]) of
        {Transport, Host, Port} ->
            case connect(Transport, Host, Port,
                                   lists:usort([binary, {active, true}|ConnectOpts])) of
                {ok, Socket} ->
                    NodeName = term_to_binary(node()),
                    L = byte_size(NodeName),
                    Msg = [<<"vmq-connect">>, <<L:32, NodeName/binary>>],
                    case send(Transport, Socket, Msg) of
                        ok ->
                            State#state{socket=Socket, transport=Transport,
                                        %% !!! remote node is reachable
                                        reachable=true};
                        {error, Reason} ->
                            lager:warning("can't initiate connect to cluster node ~p due to ~p", [RemoteNode, Reason]),
                            State#state{reachable=false, reconnect_tref=reconnect_timer()}
                    end;
                {error, Reason} ->
                    lager:warning("can't connect to cluster node ~p due to ~p", [RemoteNode, Reason]),
                    State#state{reachable=false, reconnect_tref=reconnect_timer()}
            end;
        {badrpc, nodedown} ->
            %% we don't scream.. vmq_cluster_mon screams
            State#state{reachable=false, reconnect_tref=reconnect_timer()};
        E ->
            lager:warning("can't connect to cluster node ~p due to ~p", [RemoteNode, E]),
            State#state{reachable=false, reconnect_tref=reconnect_timer()}
    end.

reconnect_timer() ->
    erlang:send_after(?RECONNECT, self(), reconnect).


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

connect_params(tcp, [{{Addr, Port}, _}|_]) ->
    {gen_tcp, Addr, Port};
connect_params(ssl, [{{Addr, Port}, _}|_]) ->
    {ssl, Addr, Port};
connect_params(_, []) -> no_config.

send(gen_tcp, Socket, Msg) ->
    gen_tcp:send(Socket, Msg);
send(ssl, Socket, Msg) ->
    ssl:send(Socket, Msg).

connect(gen_tcp, Host, Port, Opts) ->
    gen_tcp:connect(Host, Port, Opts);
connect(ssl, Host, Port, Opts) ->
    gen_tcp:connect(Host, Port, Opts).
