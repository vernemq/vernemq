%% Copyright 2018 Octavo Labs AG Zurich Switzerland (https://octavolabs.com)
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

-module(vmq_swc_proxy_diameter_transport).

-export([init/1,
         start_listener/3,
         start_connection/4,
         stop_connection/2,
         listener_address_port/1,
         rpc/5,
         rpc_cast/5]).

-export([listen/2,
         accept/1,
         connect/3,
         sockname/1,
         setopts/2,
         send/2,
         close/1]).

-export([partition_cluster/2,
         heal_partitioned_cluster/2,
         proxy_command/2]).


-define(PROXY_PORT(DiaPort), DiaPort - 100).


init(SwcConfig) ->
    ets:new(?MODULE, [public, bag, named_table]),
    vmq_swc_diameter_transport:init(SwcConfig).

start_listener(SwcConfig, Port, TCPConfig) ->
    Ret = vmq_swc_diameter_transport:start_listener(SwcConfig, Port, [{module, ?MODULE}|TCPConfig]),
    spawn_link(
      fun() ->
              proxy_start(?PROXY_PORT(Port), Port)
      end),
    Ret.

listener_address_port(SwcConfig) ->
    % this one is called by plumtree group_membership strategy to form the cluster
    {ok, {Ip, Port}} = vmq_swc_diameter_transport:listener_address_port(SwcConfig),
    {ok, {Ip, ?PROXY_PORT(Port)}}.

start_connection(SwcConfig, DestinationIp, DestinationPort, TCPConfig) ->
    vmq_swc_diameter_transport:start_connection(SwcConfig, DestinationIp, DestinationPort, [{module, ?MODULE}|TCPConfig]).

stop_connection(SwcConfig, TRef) ->
    vmq_swc_diameter_transport:stop_connection(SwcConfig, TRef).

rpc(SwcConfig, Peer, Module, Function, Args) ->
    vmq_swc_diameter_transport:rpc(SwcConfig, Peer, Module, Function, Args).

rpc_cast(SwcConfig, Peer, Module, Function, Args) ->
    vmq_swc_diameter_transport:rpc_cast(SwcConfig, Peer, Module, Function, Args).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% gen_tcp - like ....
listen(Port, Opts) ->
    gen_tcp:listen(Port, Opts).

accept(LSock) ->
    gen_tcp:accept(LSock).

sockname(Sock) ->
    inet:sockname(Sock).

setopts(Sock, Opts) ->
    inet:setopts(Sock, Opts).

send(Sock, Packet) ->
    gen_tcp:send(Sock, Packet).

close(Sock) ->
    gen_tcp:close(Sock).

connect(Host, Port, Opts) ->
    case gen_tcp:connect(Host, Port, Opts) of
        {ok, Socket} ->
            BNode = term_to_binary(node()),
            FirstPacket = <<(byte_size(BNode)):32, BNode/binary>>,
            ok = gen_tcp:send(Socket, FirstPacket),
            {ok, Socket};
        E ->
            E
    end.

proxy_start(ProxyPort, DestinationPort) ->
    {ok, LSock} = gen_tcp:listen(ProxyPort, [binary, {reuseaddr, true}, {active, false}]),
    proxy_accept_loop(LSock, DestinationPort).

proxy_accept_loop(LSock, DestinationPort) ->
    {ok, Sock} = gen_tcp:accept(LSock),
    {ok, <<Length:32>>} = gen_tcp:recv(Sock, 4),
    {ok, BNode} = gen_tcp:recv(Sock, Length),
    SourceNode = binary_to_term(BNode),

    Pid = spawn_link(fun() -> proxy_loop_init(Sock, DestinationPort) end),
    ets:insert(?MODULE, [{SourceNode, Pid}, {Pid, SourceNode}]),
    _ = gen_tcp:controlling_process(Sock, Pid),
    proxy_accept_loop(LSock, DestinationPort).

proxy_loop_init(InSock, DestinationPort) ->
    {ok, OutSock} = gen_tcp:connect({127,0,0,1}, DestinationPort, [binary]),
    ok = inet:setopts(InSock, [{active, true}]),
    ok = inet:setopts(OutSock, [{active, true}]),
    proxy_loop(InSock, OutSock).


proxy_loop(InSock, OutSock) ->
    receive
        pause ->
            inet:setopts(InSock, [{active, false}]),
            inet:setopts(OutSock, [{active, false}]),
            proxy_loop(InSock, OutSock);
        resume ->
            inet:setopts(InSock, [{active, true}]),
            inet:setopts(OutSock, [{active, true}]),
            proxy_loop(InSock, OutSock);
        {tcp, InSock, Data} ->
            ok = gen_tcp:send(OutSock, Data),
            proxy_loop(InSock, OutSock);
        {tcp, OutSock, Data} ->
            ok = gen_tcp:send(InSock, Data),
            proxy_loop(InSock, OutSock);
        {tcp_closed, InSock} ->
            [{_, SourceNode}] = ets:lookup(?MODULE, self()),
            ets:delete_object(?MODULE, {self(), SourceNode}),
            ets:delete_object(?MODULE, {SourceNode, self()}),
            gen_tcp:close(OutSock);
        {tcp_closed, OutSock} ->
            [{_, SourceNode}] = ets:lookup(?MODULE, self()),
            ets:delete_object(?MODULE, {self(), SourceNode}),
            ets:delete_object(?MODULE, {SourceNode, self()}),
            gen_tcp:close(InSock)
    end.

partition_cluster(PartitionA, PartitionB) ->
    cluster_partition_command(PartitionA, PartitionB, pause).

heal_partitioned_cluster(PartitionA, PartitionB) ->
    cluster_partition_command(PartitionA, PartitionB, resume).

cluster_partition_command(PartitionA, PartitionB, Command) ->
    lists:foreach(
      fun(NodeA) ->
              lists:foreach(
                fun(NodeB) ->
                        rpc:call(NodeA, ?MODULE, proxy_command, [NodeB, Command]),
                        rpc:call(NodeB, ?MODULE, proxy_command, [NodeA, Command])
                end, PartitionB)
      end, PartitionA).

proxy_command(Node, Command) ->
    lists:foreach(
      fun({N, Pid}) ->
              Pid ! Command
      end, ets:lookup(?MODULE, Node)).
