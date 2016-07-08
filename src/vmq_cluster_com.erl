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

-module(vmq_cluster_com).
-include("vmq_server.hrl").
-behaviour(ranch_protocol).

%% API.
-export([start_link/4]).

-export([init/4,
         loop/1]).

-record(st, {socket,
             buffer= <<>>,
             parser_state,
             proto_tag,
             pending=[],
             throttled=false,
             bytes_recv={os:timestamp(), 0}}).

%% API.
start_link(Ref, Socket, Transport, Opts) ->
    Pid = proc_lib:spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
    {ok, Pid}.

init(Ref, Socket, Transport, _Opts) ->
    ok = ranch:accept_ack(Ref),

    process_flag(trap_exit, true),
    MaskedSocket = mask_socket(Transport, Socket),
    %% tune buffer sizes
    {ok, BufSizes} = getopts(MaskedSocket, [sndbuf, recbuf, buffer]),
    BufSize = lists:max([Sz || {_, Sz} <- BufSizes]),
    setopts(MaskedSocket, [{buffer, BufSize}]),
    case active_once(MaskedSocket) of
        ok ->
            loop(#st{socket=MaskedSocket, proto_tag=proto_tag(Transport)});
        {error, Reason} ->
            exit(Reason)
    end.

proto_tag(ranch_tcp) -> {tcp, tcp_closed, tcp_error};
proto_tag(ranch_ssl) -> {ssl, ssl_closed, ssl_error}.

mask_socket(ranch_tcp, Socket) -> Socket;
mask_socket(ranch_ssl, Socket) -> {ssl, Socket}.

loop(#st{} = State) ->
    receive
        M ->
            loop(handle_message(M, State))
    end;
loop({exit, Reason, _State}) ->
    case Reason of
        shutdown -> ok;
        normal -> ok;
        _ ->
            lager:warning("terminate due to ~p", [Reason])
    end.

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

handle_message({Proto, _, Data}, #st{socket=Socket,
                                     parser_state=ParserState,
                                     proto_tag={Proto, _, _},
                                     bytes_recv={{M, S, _}, V}} = State) ->
    case process_bytes(Data, ParserState) of
        {ok, NewParserState} ->
            case active_once(Socket) of
                ok ->
                    L = byte_size(Data),
                    NewBytesRecv =
                    case os:timestamp() of
                        {M, S, _} = TS ->
                            {TS, V + L};
                        TS ->
                            _ = vmq_metrics:incr_cluster_bytes_received(V + L),
                            {TS, 0}
                    end,
                    State#st{parser_state=NewParserState, bytes_recv=NewBytesRecv};
                {error, _InetError} ->
                    %% Socket has a problem (most possibly closed)
                    %% ther's not much we can do right now.
                    %% let's go down, and let the remote node
                    %% reconnect!
                    {exit, normal, State}
            end;
        {error, Reason} ->
            {exit, Reason, State}
    end;
handle_message({ProtoClosed, _}, #st{proto_tag={_, ProtoClosed, _}} = State) ->
    %% we regard a tcp_closed as 'normal'
    {exit, normal, State};
handle_message({ProtoErr, _, Error}, #st{proto_tag={_, _, ProtoErr}} = State) ->
    {exit, Error, State};
handle_message({'DOWN', _, process, _ClusterNodePid, Reason}, State) ->
    {exit, Reason, State}.

process_bytes(<<"vmq-connect", L:32, BNodeName:L/binary, Rest/binary>>, undefined) ->
    NodeName = binary_to_term(BNodeName),
    case vmq_cluster_node_sup:get_cluster_node(NodeName) of
        {ok, ClusterNodePid} ->
            monitor(process, ClusterNodePid),
            process_bytes(Rest, <<>>);
        {error, not_found} ->
            lager:debug("got connect request from unknown cluster node ~p", [NodeName]),
            {error, remote_node_not_available}
    end;
process_bytes(Bytes, Buffer) ->
    NewBuffer = <<Buffer/binary, Bytes/binary>>,
    case NewBuffer of
        <<"vmq-send", L:32, BFrames:L/binary, Rest/binary>> ->
            process(BFrames),
            process_bytes(Rest, <<>>);
        _ ->
            {ok, NewBuffer}
    end.


process(<<"msg", L:32, Bin:L/binary, Rest/binary>>) ->
    #vmq_msg{mountpoint=MP,
             routing_key=Topic,
             reg_view=RegView} = Msg = binary_to_term(Bin),
    _ = vmq_reg_view:fold(RegView, MP, Topic, fun publish/2, Msg),
    process(Rest);
process(<<"enq", L:32, Bin:L/binary, Rest/binary>>) ->
    {CallerPid, Ref, {enqueue, QueuePid, Msgs}} = binary_to_term(Bin),
    %% enqueue in own process context
    %% to ensure that this won't block
    %% the cluster communication.
    spawn(fun() ->
                  try
                      Reply = vmq_queue:enqueue_many(QueuePid, Msgs),
                      CallerPid ! {Ref, Reply}
                  catch
                      _:_ ->
                          CallerPid ! {Ref, {error, cant_remote_enqueue}}
                  end
          end),
    process(Rest);
process(<<>>) -> ok.

publish({_, _} = SubscriberIdAndQoS, Msg) ->
    vmq_reg:publish(SubscriberIdAndQoS, Msg);
publish(_Node, Msg) ->
    %% we ignore remote subscriptions, they are already covered
    %% by original publisher
    Msg.
