-module(vmq_cluster_com_SUITE).
-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(Config) ->
    S = vmq_test_utils:get_suite_rand_seed(),
    %% this might help, might not...
    os:cmd(os:find_executable("epmd")++" -daemon"),
    {ok, Hostname} = inet:gethostname(),
    case net_kernel:start([list_to_atom("runner@"++Hostname), shortnames]) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,
    ct:log("node name ~p", [node()]),
    Node = vmq_cluster_test_utils:start_node(test_com1, Config, default_case),
    ct:pal("This is the default NODE : ~p~n", [Node]),
    {ok, _} = ct_cover:add_nodes([Node]),
    vmq_cluster_test_utils:wait_until_ready([Node]),
    [{node, Node}, S|Config].

end_per_suite(_Config) ->
    ct_slave:stop(test_com1),
    _Config.

init_per_testcase(_Case, Config) ->
    vmq_test_utils:seed_rand(Config),
    ClusterNodePid = setup_mock_vmq_cluster_node(Config),
    [{cluster_node_pid, ClusterNodePid}|Config].

end_per_testcase(_Case, Config) ->
    terminate_mock_vmq_cluster_node(Config),
    ok.

all() ->
    [connect_success_test,
     connect_success_send_error,
     connect_success_send_error_timeout
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
connect_params(_RemoteNode) ->
    {gen_tcp, {127,0,0,1}, 12345}.

connect_success_test(Config) ->
    ClusterNodePid = cluster_node_pid(Config),
    {ok, ListenSocket} = gen_tcp:listen(12345, [binary, {reuseaddr, true}, {active, false}]),
    {ok, Socket} = gen_tcp:accept(ListenSocket, 30000),
    recv_connect(Socket, Config),

    % send test message
    ok = send_message(ClusterNodePid, hello_world),
    % recv this message
    recv_message(Socket, hello_world).

connect_success_send_error(Config) ->
    % check that message isn't lost
    ClusterNodePid = cluster_node_pid(Config),
    {ok, ListenSocket} = gen_tcp:listen(12345, [binary, {reuseaddr, true}, {active, false}]),
    {ok, Socket1} = gen_tcp:accept(ListenSocket, 30000),
    recv_connect(Socket1, Config),
    % close this socket
    gen_tcp:close(Socket1),
    % send test message, will be buffered and delivered on next successful reconnect
    ok = send_message(ClusterNodePid, hello_world),

    {ok, Socket2} = gen_tcp:accept(ListenSocket, 30000),
    recv_connect(Socket2, Config),
    % recv this message
    recv_message(Socket2, hello_world).

connect_success_send_error_timeout(Config) ->
    ct:timetrap({minutes, 10}),
    % check that message isn't lost
    ClusterNodePid = cluster_node_pid(Config),
    {ok, ListenSocket} = gen_tcp:listen(12345, [binary, {reuseaddr, true}, {active, false}]),
    {ok, Socket1} = gen_tcp:accept(ListenSocket, 30000),
    recv_connect(Socket1, Config),

    N = send_until_tcp_buffer_full(ClusterNodePid),
    % once the tcp buffer is full, we get disconnected
    recv_until_tcp_buffer_empty(Socket1, N),
    % we should have a TCP_CLOSE now
    {error, closed} = gen_tcp:recv(Socket1, 0),

    % the cluster node should do the reconnect
    {ok, Socket2} = gen_tcp:accept(ListenSocket, 30000),
    recv_connect(Socket2, Config),

    % the last buffered message is repeated as the cluster node doesn't
    % actually know if we have received it or not.
    recv_message(Socket2, <<1:10000, N:32>>),
    {error, timeout} = gen_tcp:recv(Socket2, 0, 1000).


send_until_tcp_buffer_full(ClusterNodePid) ->
   send_until_tcp_buffer_full(ClusterNodePid, 0).
send_until_tcp_buffer_full(ClusterNodePid, MsgsAcc) ->
    % the only way we detect that the buffer is full is that vmq_cluster_node will close
    % the connection and will reconnect
    case send_message(ClusterNodePid, <<1:10000, MsgsAcc:32>>) of
        ok  ->
            send_until_tcp_buffer_full(ClusterNodePid, MsgsAcc + 1);
        {error, msg_dropped} ->
            MsgsAcc - 1
    end.

recv_until_tcp_buffer_empty(Socket, N) ->
    recv_until_tcp_buffer_empty(Socket, 0, N).

recv_until_tcp_buffer_empty(Socket, I, N) when I =< N ->
    recv_message(Socket, <<1:10000, I:32>>),
    recv_until_tcp_buffer_empty(Socket, I + 1, N);
recv_until_tcp_buffer_empty(_, _, _) -> ok.


setup_mock_vmq_cluster_node(Config) ->
    setup_mock_vmq_cluster_node(Config, []).

setup_mock_vmq_cluster_node(Config, Opts) ->
    Node = proplists:get_value(node, Config),
    % make the test_com1 node connect to myself
    ok = rpc:block_call(Node, vmq_config, set_env, [outgoing_connect_opts, Opts, false]),
    ok = rpc:block_call(Node, vmq_config, set_env, [outgoing_connect_params_module, ?MODULE, false]),
    ok = rpc:block_call(Node, vmq_config, set_env, [outgoing_connect_timeout, 1000, false]),
    ok = rpc:block_call(Node, vmq_config, set_env, [outgoing_clustering_buffer_size, 1000, false]),
    {ok, ClusterNodePid} = rpc:block_call(Node, vmq_cluster_node, start_link, [node()]),
    ClusterNodePid.

terminate_mock_vmq_cluster_node(Config) ->
    Node = proplists:get_value(node, Config),
    ClusterNodePid = cluster_node_pid(Config),
    rpc:block_call(Node, erlang, exit, [ClusterNodePid, kill]).

cluster_node_pid(Config) ->
    proplists:get_value(cluster_node_pid, Config).

recv_connect(Socket, Config) ->
    Node = proplists:get_value(node, Config),
    NodeName = term_to_binary(Node),
    L1 = byte_size(NodeName),
    HandshakeMsg = <<"vmq-connect", L1:32, NodeName/binary>>,
    {ok, HandshakeMsg} = gen_tcp:recv(Socket, byte_size(HandshakeMsg)),
    ok.


send_message(ClusterNodePid, Msg) ->
    rpc:call(node(ClusterNodePid), vmq_cluster_node, publish, [ClusterNodePid, Msg]).

recv_message(Socket, Term) ->
    TermBin = term_to_binary(Term),
    L = byte_size(TermBin),
    Msg = <<"msg", L:32, TermBin/binary>>,
    BatchMsg = <<"vmq-send", (byte_size(Msg)):32, Msg/binary>>,
    case gen_tcp:recv(Socket, byte_size(BatchMsg)) of
        {ok, BatchMsg} -> ok;
        E ->
            io:format(user, "got ~p instead of ~p~n", [E, {ok, BatchMsg}]),
            E
    end.
