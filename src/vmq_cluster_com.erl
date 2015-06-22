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
             cluster_node,
             proto_tag,
             pending=[],
             throttled=false,
             bytes_recv={os:timestamp(), 0},
             bytes_send={os:timestamp(), 0}}).

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
loop({exit, Reason, State}) ->
    teardown(State, Reason).

teardown(#st{cluster_node=ClusterNodePid}, Reason) ->
    case Reason of
        normal ->
            lager:debug("[~p] cluster node connection normally stopped", [ClusterNodePid]);
        shutdown ->
            lager:debug("[~p] cluster node connection stopped due to shutdown", [ClusterNodePid]);
        _ ->
            lager:warning("[~p] cluster node connection stopped abnormally due to ~p", [ClusterNodePid, Reason])
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

handle_message({Proto, _, Data}, #st{proto_tag={Proto, _, _}} = State) ->
    #st{cluster_node=ClusterNodePid,
        socket=Socket,
        parser_state=ParserState} = State,
    case process_bytes(ClusterNodePid, Data, ParserState) of
        {ok, NewParserState} ->
            case active_once(Socket) of
                ok ->
                    State#st{parser_state=NewParserState};
                {error, Reason} ->
                    {exit, Reason, State}
            end;
        error ->
            {exit, error, State}
    end;
handle_message({ProtoClosed, _}, #st{proto_tag={_, ProtoClosed, _}} = State) ->
    %% we regard a tcp_closed as 'normal'
    {exit, normal, State};
handle_message({ProtoErr, _, Error}, #st{proto_tag={_, _, ProtoErr}} = State) ->
    {exit, Error, State};
handle_message({'EXIT', ClusterNodePid, Reason}, #st{cluster_node=ClusterNodePid} = State) ->
    {exit, Reason, State}.

process_bytes(_,
              <<"vmq-connect", L:32, BNodeName:L/binary, Rest/binary>>, undefined) ->
    NodeName = binary_to_term(BNodeName),
    case vmq_cluster_node_sup:get_cluster_node(NodeName) of
        {ok, ClusterNodePid} ->
            monitor(process, ClusterNodePid),
            process_bytes(ClusterNodePid, Rest, <<>>);
        {error, not_found} ->
            lager:warning("got connect request from unknown cluster node ~p", [NodeName]),
            error
    end;
process_bytes(ClusterNodePid, Bytes, Buffer) ->
    NewBuffer = <<Buffer/binary, Bytes/binary>>,
    case NewBuffer of
        <<"vmq-send", L:32, BFrames:L/binary, Rest/binary>> ->
            publish(BFrames),
            process_bytes(ClusterNodePid, Rest, <<>>);
        _ ->
            {ok, NewBuffer}
    end.


publish(<<L:32, Bin:L/binary, Rest/binary>>) ->
    #vmq_msg{mountpoint=MP,
             routing_key=Topic,
             reg_view=RegView} = Msg = binary_to_term(Bin),
    _ = RegView:fold(MP, Topic, fun publish/2, Msg),
    publish(Rest);
publish(<<>>) -> ok.

publish({_,_} = SubscriberIdAndQoS, Msg) ->
    vmq_reg:publish(SubscriberIdAndQoS, Msg);
publish(_Node, Msg) ->
    %% we ignore remote subscriptions, they are already covered
    %% by original publisher
    Msg.
