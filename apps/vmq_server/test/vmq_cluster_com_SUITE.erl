-module(vmq_cluster_com_SUITE).
-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(Config) ->
    S = vmq_test_utils:get_suite_rand_seed(),
    %lager:start(),
    %% this might help, might not...
    os:cmd(os:find_executable("epmd")++" -daemon"),
    {ok, Hostname} = inet:gethostname(),
    case net_kernel:start([list_to_atom("runner@"++Hostname), shortnames]) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,
    lager:info("node name ~p", [node()]),
    [S|Config].

end_per_suite(_Config) ->
    application:stop(lager),
    _Config.

init_per_testcase(Case, Config) ->
    vmq_test_utils:seed_rand(Config),
    Node = vmq_cluster_test_utils:start_node(test1, Config, Case),
    {ok, _} = ct_cover:add_nodes([Node]),
    vmq_cluster_test_utils:wait_until_ready([Node]),
    [{node, Node}|Config].

end_per_testcase(_Case, _Config) ->
    ct_slave:stop(test1),
    ok.

all() ->
    [connect_success_test,
     connect_success_send_error
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
connect_params(_RemoteNode) ->
    {gen_tcp, {127,0,0,1}, 12345}.

connect_success_test(Config) ->
    ClusterNodePid = setup_mock_vmq_cluster_node(Config),
    {ok, ListenSocket} = gen_tcp:listen(12345, [binary, {reuseaddr, true}, {active, false}]),
    {ok, Socket} = gen_tcp:accept(ListenSocket, 20000),
    recv_connect(Socket, Config),

    % send test message
    send_message(ClusterNodePid, hello_world),
    % recv this message
    recv_message(Socket, hello_world).

connect_success_send_error(Config) ->
    % check that message isn't lost
    ClusterNodePid = setup_mock_vmq_cluster_node(Config),
    {ok, ListenSocket} = gen_tcp:listen(12345, [binary, {reuseaddr, true}, {active, false}]),
    {ok, Socket1} = gen_tcp:accept(ListenSocket, 20000),
    recv_connect(Socket1, Config),
    % close this socket
    erlang:port_close(Socket1),
    % send test message, will be buffered and delivered on next successful reconnect
    send_message(ClusterNodePid, hello_world),

    {ok, Socket2} = gen_tcp:accept(ListenSocket, 20000),
    recv_connect(Socket2, Config),
    % recv this message
    recv_message(Socket2, hello_world).

setup_mock_vmq_cluster_node(Config) ->
    setup_mock_vmq_cluster_node(Config, []).

setup_mock_vmq_cluster_node(Config, Opts) ->
    Node = proplists:get_value(node, Config),
    % make the test1 node connect to myself
    ok = rpc:call(Node, vmq_config, set_env, [outgoing_connect_opts, Opts, false]),
    ok = rpc:call(Node, vmq_config, set_env, [outgoing_connect_params_module, ?MODULE, false]),
    ok = rpc:call(Node, vmq_config, set_env, [outgoing_connect_timeout, 1000, false]),
    {ok, ClusterNodePid} = rpc:call(Node, vmq_cluster_node, start_link, [node()]),
    ClusterNodePid.

recv_connect(Socket, Config) ->
    Node = proplists:get_value(node, Config),
    NodeName = term_to_binary(Node),
    L1 = byte_size(NodeName),
    HandshakeMsg = <<"vmq-connect", L1:32, NodeName/binary>>,
    {ok, HandshakeMsg} = gen_tcp:recv(Socket, byte_size(HandshakeMsg)),
    ok.


send_message(ClusterNodePid, Msg) ->
    ok = rpc:call(node(ClusterNodePid), vmq_cluster_node, publish, [ClusterNodePid, Msg]).

recv_message(Socket, Term) ->
    TermBin = term_to_binary(Term),
    L = byte_size(TermBin),
    Msg = <<"msg", L:32, TermBin/binary>>,
    BatchMsg = <<"vmq-send", (byte_size(Msg)):32, Msg/binary>>,
    {ok, BatchMsg} = gen_tcp:recv(Socket, byte_size(BatchMsg)).
