-module(vmq_cluster_com_SUITE).
-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(ssl),
    S = vmq_test_utils:get_suite_rand_seed(),
    Config0 = vmq_cluster_test_utils:init_distribution(Config),
    ct:log("node name ~p", [node()]),
    {ok, Peer, Node} = vmq_cluster_test_utils:start_node(test_com1, Config, default_case),
    ct:pal("This is the default NODE : ~p~n", [Node]),
    {ok, _} = ct_cover:add_nodes([Node]),
    vmq_cluster_test_utils:wait_until_ready([Node]),
    [{peer, Peer}, {node, Node}, S| Config0].

end_per_suite(Config) ->
    {_, Peer} = lists:keyfind(peer, 1, Config),
    {_, Node} = lists:keyfind(node, 1, Config),
    ok = vmq_cluster_test_utils:stop_peer(Peer, Node),
    Config.

init_per_testcase(Case, Config) when
    Case == connect_success_ssl_test;
    Case == invalid_initial_cluster_frame_test;
    Case == partial_initial_cluster_frame_test;
    Case == oversized_initial_cluster_frame_test;
    Case == oversized_send_cluster_frame_test;
    Case == invalid_connect_term_test;
    Case == invalid_msg_term_test;
    Case == invalid_enq_term_test;
    Case == invalid_inner_cluster_frame_test
->
    persistent_term:erase({?MODULE, transport}),
    persistent_term:erase({?MODULE, cluster_node_pid}),
    vmq_test_utils:seed_rand(Config),
    Config;
init_per_testcase(_Case, Config) ->
    persistent_term:erase({?MODULE, transport}),
    persistent_term:erase({?MODULE, cluster_node_pid}),
    vmq_test_utils:seed_rand(Config),
    ClusterNodePid = setup_mock_vmq_cluster_node(Config),
    [{cluster_node_pid, ClusterNodePid}|Config].

end_per_testcase(_Case, Config) ->
    case persistent_term:get({?MODULE, cluster_node_pid}, cluster_node_pid(Config)) of
        undefined -> ok;
        ClusterNodePid -> terminate_cluster_node_pid(Config, ClusterNodePid)
    end,
    persistent_term:erase({?MODULE, transport}),
    persistent_term:erase({?MODULE, cluster_node_pid}),
    ok.

all() ->
    [connect_success_test,
     connect_success_ssl_test,
     connect_success_delays_publish_until_ack,
     connect_success_legacy_fallback,
     connect_success_send_error,
     connect_success_send_error_timeout,
     invalid_initial_cluster_frame_test,
     partial_initial_cluster_frame_test,
     oversized_initial_cluster_frame_test,
     oversized_send_cluster_frame_test,
     invalid_connect_term_test,
     invalid_msg_term_test,
     invalid_enq_term_test,
     invalid_inner_cluster_frame_test
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
connect_params(_RemoteNode) ->
    case persistent_term:get({?MODULE, transport}, gen_tcp) of
        ssl -> {ssl, {127,0,0,1}, 12346};
        gen_tcp -> {gen_tcp, {127,0,0,1}, 12345}
    end.

connect_success_test(Config) ->
    ClusterNodePid = cluster_node_pid(Config),
    {ok, ListenSocket} = gen_tcp:listen(12345, [binary, {reuseaddr, true}, {active, false}]),
    {ok, Socket} = gen_tcp:accept(ListenSocket, 30000),
    recv_connect_ack(Socket, Config),

    % send test message
    ok = send_message(ClusterNodePid, hello_world),
    % recv this message
    recv_message(Socket, hello_world).

connect_success_ssl_test(Config) ->
    ClusterNodePid = setup_mock_ssl_vmq_cluster_node(Config),
    {ok, ListenSocket} = ssl:listen(12346, [
        binary,
        {reuseaddr, true},
        {active, false},
        {certfile, ssl_path("server.crt")},
        {keyfile, ssl_path("server.key")}
    ]),
    {ok, Socket} = accept_ssl(ListenSocket),
    recv_ssl_connect_ack(Socket, Config),

    ok = send_message(ClusterNodePid, hello_world),
    ok = recv_ssl_message(Socket, hello_world),
    ok.

connect_success_delays_publish_until_ack(Config) ->
    ClusterNodePid = cluster_node_pid(Config),
    ok = rpc:block_call(node(ClusterNodePid), vmq_config, set_env, [outgoing_cluster_handshake_ack_timeout, 5000, false]),
    {ok, ListenSocket} = gen_tcp:listen(12345, [binary, {reuseaddr, true}, {active, false}]),
    {ok, Socket} = gen_tcp:accept(ListenSocket, 30000),
    recv_connect(Socket, Config),

    Caller = self(),
    SendPid = spawn(fun() -> Caller ! {publish_result, send_message(ClusterNodePid, hello_world)} end),
    receive
        {publish_result, Result} ->
            exit({publish_returned_before_ack, Result})
    after 100 ->
        ok
    end,

    ok = gen_tcp:send(Socket, <<"vmq-connect-ack">>),
    receive
        {publish_result, ok} -> ok
    after 30000 ->
        exit({publish_did_not_return, SendPid})
    end,
    recv_message(Socket, hello_world).

connect_success_legacy_fallback(Config) ->
    ClusterNodePid = cluster_node_pid(Config),
    ok = rpc:block_call(node(ClusterNodePid), vmq_config, set_env, [outgoing_cluster_handshake_ack_timeout, 100, false]),
    {ok, ListenSocket} = gen_tcp:listen(12345, [binary, {reuseaddr, true}, {active, false}]),
    {ok, Socket} = gen_tcp:accept(ListenSocket, 30000),
    recv_connect(Socket, Config),

    ok = send_message(ClusterNodePid, hello_world),
    recv_message(Socket, hello_world).

connect_success_send_error(Config) ->
    % check that message isn't lost
    ClusterNodePid = cluster_node_pid(Config),
    {ok, ListenSocket} = gen_tcp:listen(12345, [binary, {reuseaddr, true}, {active, false}]),
    {ok, Socket1} = gen_tcp:accept(ListenSocket, 30000),
    recv_connect_ack(Socket1, Config),
    % close this socket with RST so the peer sees a send error reliably
    ok = inet:setopts(Socket1, [{linger, {true, 0}}]),
    gen_tcp:close(Socket1),
    % send test message, will be buffered and delivered on next successful reconnect
    ok = send_message(ClusterNodePid, hello_world),

    {ok, Socket2} = gen_tcp:accept(ListenSocket, 30000),
    recv_connect_ack(Socket2, Config),
    % recv this message
    recv_message(Socket2, hello_world).

connect_success_send_error_timeout(Config) ->
    ct:timetrap({minutes, 10}),
    % check that message isn't lost
    ClusterNodePid = cluster_node_pid(Config),
    {ok, ListenSocket} = gen_tcp:listen(12345, [binary, {reuseaddr, true}, {active, false}]),
    {ok, Socket1} = gen_tcp:accept(ListenSocket, 30000),
    recv_connect_ack(Socket1, Config),

    N = send_until_tcp_buffer_full(ClusterNodePid),
    % once the tcp buffer is full, we get disconnected
    recv_until_tcp_buffer_empty(Socket1, N),
    % we should have a TCP_CLOSE now
    {error, closed} = gen_tcp:recv(Socket1, 0),

    % the cluster node should do the reconnect
    {ok, Socket2} = gen_tcp:accept(ListenSocket, 30000),
    recv_connect_ack(Socket2, Config),

    % the last buffered message is repeated as the cluster node doesn't
    % actually know if we have received it or not.
    recv_message(Socket2, <<1:10000, N:32>>),
    {error, timeout} = gen_tcp:recv(Socket2, 0, 1000).

invalid_initial_cluster_frame_test(_) ->
    {error, invalid_cluster_frame} = vmq_cluster_com:test_process_bytes(<<"nope">>, undefined, 64).

partial_initial_cluster_frame_test(_) ->
    {ok, {connect, <<"vmq-con">>}} = vmq_cluster_com:test_process_bytes(<<"vmq-con">>, undefined, 64).

oversized_initial_cluster_frame_test(_) ->
    Bytes = <<"vmq-connect", 64:32, 1:64>>,
    {error, cluster_buffer_too_large} = vmq_cluster_com:test_process_bytes(Bytes, undefined, 16).

oversized_send_cluster_frame_test(_) ->
    {error, cluster_frame_too_large} = vmq_cluster_com:test_process_bytes(<<"vmq-send", 17:32>>, <<>>, 16).

invalid_connect_term_test(_) ->
    BadTerm = <<"not-an-external-term">>,
    Bytes = <<"vmq-connect", (byte_size(BadTerm)):32, BadTerm/binary>>,
    {error, invalid_cluster_frame} = vmq_cluster_com:test_process_bytes(Bytes, undefined, 64).

invalid_msg_term_test(_) ->
    BadTerm = <<"not-an-external-term">>,
    Msg = <<"msg", (byte_size(BadTerm)):32, BadTerm/binary>>,
    Bytes = <<"vmq-send", (byte_size(Msg)):32, Msg/binary>>,
    {error, invalid_cluster_frame} = vmq_cluster_com:test_process_bytes(Bytes, <<>>, 64).

invalid_enq_term_test(_) ->
    BadTerm = <<"not-an-external-term">>,
    Msg = <<"enq", (byte_size(BadTerm)):32, BadTerm/binary>>,
    Bytes = <<"vmq-send", (byte_size(Msg)):32, Msg/binary>>,
    {error, invalid_cluster_frame} = vmq_cluster_com:test_process_bytes(Bytes, <<>>, 64).

invalid_inner_cluster_frame_test(_) ->
    Bytes = <<"vmq-send", 2:32, "ms">>,
    {error, invalid_cluster_frame} = vmq_cluster_com:test_process_bytes(Bytes, <<>>, 64).


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
    persistent_term:put({?MODULE, transport}, gen_tcp),
    ok = rpc:block_call(Node, vmq_config, set_env, [outgoing_connect_options, lists:flatten([[{keepalive, true}, {send_timeout, 0}] | Opts]), false]),
    ok = rpc:block_call(Node, vmq_config, set_env, [outgoing_connect_params_module, ?MODULE, false]),
    ok = rpc:block_call(Node, vmq_config, set_env, [outgoing_connect_timeout, 1000, false]),
    ok = rpc:block_call(Node, vmq_config, set_env, [outgoing_clustering_buffer_size, 1000, false]),
    ok = rpc:block_call(Node, vmq_config, set_env, [outgoing_clustering_flush_threshold, 1460, false]),
    ok = rpc:block_call(Node, vmq_config, set_env, [outgoing_cluster_handshake_ack_timeout, 250, false]),
    {ok, ClusterNodePid} = rpc:block_call(Node, vmq_cluster_node, start_link, [node()]),
    ClusterNodePid.

setup_mock_ssl_vmq_cluster_node(Config) ->
    Node = proplists:get_value(node, Config),
    persistent_term:put({?MODULE, transport}, ssl),
    {ok, _} = rpc:block_call(Node, application, ensure_all_started, [ssl]),
    ok = rpc:block_call(Node, vmq_config, set_env, [outgoing_connect_options, [{verify, verify_none}, {send_timeout, 0}], false]),
    ok = rpc:block_call(Node, vmq_config, set_env, [outgoing_connect_params_module, ?MODULE, false]),
    ok = rpc:block_call(Node, vmq_config, set_env, [outgoing_connect_timeout, 1000, false]),
    ok = rpc:block_call(Node, vmq_config, set_env, [outgoing_clustering_buffer_size, 1000, false]),
    ok = rpc:block_call(Node, vmq_config, set_env, [outgoing_clustering_flush_threshold, 1460, false]),
    ok = rpc:block_call(Node, vmq_config, set_env, [outgoing_cluster_handshake_ack_timeout, 250, false]),
    {ok, ClusterNodePid} = rpc:block_call(Node, vmq_cluster_node, start_link, [node()]),
    persistent_term:put({?MODULE, cluster_node_pid}, ClusterNodePid),
    ClusterNodePid.

terminate_mock_vmq_cluster_node(Config) ->
    terminate_cluster_node_pid(Config, cluster_node_pid(Config)).

terminate_cluster_node_pid(Config, ClusterNodePid) ->
    Node = proplists:get_value(node, Config),
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

recv_connect_ack(Socket, Config) ->
    ok = recv_connect(Socket, Config),
    ok = gen_tcp:send(Socket, <<"vmq-connect-ack">>).

accept_ssl(ListenSocket) ->
    {ok, TransportSocket} = ssl:transport_accept(ListenSocket, 30000),
    case ssl:handshake(TransportSocket) of
        ok -> {ok, TransportSocket};
        {ok, Socket} -> {ok, Socket}
    end.

recv_ssl_connect(Socket, Config) ->
    Node = proplists:get_value(node, Config),
    NodeName = term_to_binary(Node),
    L1 = byte_size(NodeName),
    HandshakeMsg = <<"vmq-connect", L1:32, NodeName/binary>>,
    {ok, HandshakeMsg} = ssl:recv(Socket, byte_size(HandshakeMsg)),
    ok.

recv_ssl_connect_ack(Socket, Config) ->
    ok = recv_ssl_connect(Socket, Config),
    ok = ssl:send(Socket, <<"vmq-connect-ack">>).


send_message(ClusterNodePid, Msg) ->
    rpc:call(node(ClusterNodePid), vmq_cluster_node, publish, [ClusterNodePid, Msg]).

recv_message(Socket, Term) ->
    TermBin = term_to_binary(Term),
    L = byte_size(TermBin),
    Msg = <<"msg", L:32, TermBin/binary>>,
    BatchMsg = <<"vmq-send", (byte_size(Msg)):32, Msg/binary>>,
    case gen_tcp:recv(Socket, byte_size(BatchMsg), 30000) of
        {ok, BatchMsg} -> ok;
        E ->
            io:format(user, "got ~p instead of ~p~n", [E, {ok, BatchMsg}]),
            E
    end.

recv_ssl_message(Socket, Term) ->
    TermBin = term_to_binary(Term),
    L = byte_size(TermBin),
    Msg = <<"msg", L:32, TermBin/binary>>,
    BatchMsg = <<"vmq-send", (byte_size(Msg)):32, Msg/binary>>,
    case ssl:recv(Socket, byte_size(BatchMsg)) of
        {ok, BatchMsg} -> ok;
        E ->
            io:format(user, "got ~p instead of ~p~n", [E, {ok, BatchMsg}]),
            E
    end.

ssl_path(File) ->
    Path = filename:dirname(proplists:get_value(source, ?MODULE:module_info(compile))),
    filename:join([Path, "ssl", File]).
