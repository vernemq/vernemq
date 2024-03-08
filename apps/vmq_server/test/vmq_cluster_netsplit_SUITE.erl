-module(vmq_cluster_netsplit_SUITE).
-include_lib("kernel/include/logger.hrl").

-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([publish_qos0_test/1,
         register_consistency_test/1,
         register_consistency_multiple_sessions_test/1,
         register_not_ready_test/1,
         remote_enqueue_cant_block_the_publisher/1
        ]).

-include_lib("eunit/include/eunit.hrl").

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(Config) ->
    S = vmq_test_utils:get_suite_rand_seed(),
    Config0 = vmq_cluster_test_utils:init_distribution(Config),
    ?LOG_INFO("node name ~p", [node()]),
    [S | Config0].

end_per_suite(_Config) ->
    _Config.

init_per_testcase(Case, Config) ->
    vmq_test_utils:seed_rand(Config),
    set_config(metadata_plugin, vmq_swc),

    NodeWithPorts =
        [vmq_cluster_test_utils:random_node_with_port(Case) || _I <- lists:seq(1, 3)],
    Nodes =
        vmq_cluster_test_utils:pmap(fun({N, Port}) ->
                                       {ok, Peer, Node} =
                                           vmq_cluster_test_utils:start_node(N, Config, Case),
                                       {ok, _} =
                                           rpc:call(Node,
                                                    vmq_server_cmd,
                                                    listener_start,
                                                    [Port, []]),
                                       %% allow all
                                       ok = rpc:call(Node, vmq_auth, register_hooks, []),
                                       {Peer, Node, Port}
                                    end,
                                    NodeWithPorts),
    {_, CoverNodes, _} = lists:unzip3(Nodes),
    {ok, _} = ct_cover:add_nodes(CoverNodes),
    [{nodes, Nodes} | Config].

end_per_testcase(_, Config) ->
    {_, NodeList} = lists:keyfind(nodes, 1, Config),
    {Peers, Nodes, _} = lists:unzip3(NodeList),
    vmq_cluster_test_utils:pmap(fun({Peer, Node}) ->
                                   ok = vmq_cluster_test_utils:stop_peer(Peer, Node)
                                end,
                                lists:zip(Peers, Nodes)),
    ok.

all() ->
    [publish_qos0_test,
     register_consistency_test,
     register_consistency_multiple_sessions_test,
     register_not_ready_test,
     remote_enqueue_cant_block_the_publisher].


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
set_config(Key, Val) ->
    rpc:multicall(vmq_server_cmd, set_config, [Key, Val]),
    ok.

register_consistency_test(Config) ->
    ok = ensure_cluster(Config),
    set_config(allow_register_during_netsplit, false),
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    {Island1, Island2} = lists:split(length(Nodes) div 2, Nodes),

    %% Create Partitions
    {_, Island1Names, _} = lists:unzip3(Island1),
    {_, Island2Names, _} = lists:unzip3(Island2),
    vmq_cluster_test_utils:partition_cluster(Island1Names, Island2Names),

    ok = wait_until_converged(Nodes,
                         fun(N) ->
                                 {rpc:call(N, vmq_cluster, netsplit_statistics, []),
                                  rpc:call(N, vmq_cluster, is_ready, [])}
                         end, {{1, 0}, false}),

    {_, _, Island1Port} = random_node(Island1),
    {_, _, Island2Port} = random_node(Island2),
    ct:sleep(10000),
    Connect = packet:gen_connect("test-client", [{clean_session, true},
                                                 {keepalive, 10}]),
    %% Island 1 should return us the proper CONNACK(3)
    {ok, _} = packet:do_client_connect(Connect, packet:gen_connack(3),
                                       [{port, Island1Port}]),
    %% Island 2 should return us the proper CONACK(3)
    {ok, _} = packet:do_client_connect(Connect, packet:gen_connack(3),
                                       [{port, Island2Port}]),
    vmq_cluster_test_utils:heal_cluster(Island1Names, Island2Names),

    ok = wait_until_converged(Nodes,
                         fun(N) ->
                                 {rpc:call(N, vmq_cluster, netsplit_statistics, []),
                                  rpc:call(N, vmq_cluster, is_ready, [])}
                         end, {{1, 1}, true}),
    ok.

register_consistency_multiple_sessions_test(Config) ->
    ok = ensure_cluster(Config),
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    {Island1, Island2} = lists:split(length(Nodes) div 2, Nodes),

    %% we configure the nodes to trade consistency for availability
    set_config(allow_register_during_netsplit, true),

    %% Create Partitions
    {_, Island1Names, _} = lists:unzip3(Island1),
    {_, Island2Names, _} = lists:unzip3(Island2),
    vmq_cluster_test_utils:partition_cluster(Island1Names, Island2Names),

    {_, _, Island1Port} = random_node(Island1),
    {_, _, Island2Port} = random_node(Island2),

    Connect = packet:gen_connect("test-client-multiple", [{clean_session, true},
                                                 {keepalive, 10}]),
    Connack = packet:gen_connack(0),
    {ok, Socket1} = packet:do_client_connect(Connect, Connack,
                                             [{port, Island1Port}]),

    {ok, Socket2} = packet:do_client_connect(Connect, Connack,
                                               [{port, Island2Port}]),
    vmq_cluster_test_utils:heal_cluster(Island1Names, Island2Names),
    gen_tcp:close(Socket1),
    gen_tcp:close(Socket2),
    ok.

register_not_ready_test(Config) ->
    ok = ensure_cluster(Config),
    set_config(allow_register_during_netsplit, false),
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    {Island1, Island2} = lists:split(length(Nodes) div 2, Nodes),

    %% Connect a test-client
    Connect = packet:gen_connect("test-client-not-ready", [{clean_session, true},
                                                 {keepalive, 10}]),
    Connack = packet:gen_connack(0),
    {_, _, Port} = random_node(Nodes),
    {ok, _Socket} = packet:do_client_connect(Connect, Connack,
                                             [{port, Port}]),

    %% Create Partitions
    {_, Island1Names, _} = lists:unzip3(Island1),
    {_, Island2Names, _} = lists:unzip3(Island2),
    vmq_cluster_test_utils:partition_cluster(Island1Names, Island2Names),

    ok = wait_until_converged(Nodes,
                         fun(N) ->
                                 rpc:call(N, vmq_cluster, is_ready, [])
                         end, false),

    %% we are now on a partitioned network and SHOULD NOT allow new connections
    ConnNack = packet:gen_connack(3), %% server unavailable
    [begin
         {ok, S} = packet:do_client_connect(Connect, ConnNack, [{port, P}]),
         gen_tcp:close(S)
     end || {_, _, P} <- Nodes],

    %% fix cables
    vmq_cluster_test_utils:heal_cluster(Island1Names, Island2Names),

    ok = wait_until_converged(Nodes,
                         fun(N) ->
                                 rpc:call(N, vmq_cluster, is_ready, [])
                         end, true),

    %% connect MUST go through now.
    [begin
         {ok, S} = packet:do_client_connect(Connect, Connack, [{port, P}]),
         gen_tcp:close(S)
     end || {_, _, P} <- Nodes],
    ok.

publish_qos0_test(Config) ->
    ok = ensure_cluster(Config),
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    {Island1, Island2} = lists:split(length(Nodes) div 2, Nodes),
    Connect = packet:gen_connect("test-netsplit-client", [{clean_session, false},
                                                          {keepalive, 60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(53, "netsplit/0/test", 0),
    Suback = packet:gen_suback(53, 0),
    {_, _, Island1Port} = random_node(Island1),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, [{port,
                                                                Island1Port}]),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = wait_until_converged(Nodes,
                         fun(N) ->
                                 rpc:call(N, vmq_reg, total_subscriptions, [])
                         end, [{total, 1}]),

    %% Create Partitions
    {_, Island1Names, _} = lists:unzip3(Island1),
    {_, Island2Names, _} = lists:unzip3(Island2),
    vmq_cluster_test_utils:partition_cluster(Island1Names, Island2Names),

    ok = wait_until_converged(Nodes,
                         fun(N) ->
                                 rpc:call(N, vmq_cluster, is_ready, [])
                         end, false),

    {_, _, Island2Port} = random_node(Island2),
    set_config(allow_register_during_netsplit, true),
    set_config(allow_publish_during_netsplit, true),
    Publish = packet:gen_publish("netsplit/0/test", 0, <<"message">>,
                                 [{mid, 1}]),
    helper_pub_qos1("test-netsplit-sender", Publish, Island2Port),

    %% fix the network
    vmq_cluster_test_utils:heal_cluster(Island1Names, Island2Names),

    %% the publish is expected once the netsplit is fixed
    ok = packet:expect_packet(Socket, "publish", Publish).

remote_enqueue_cant_block_the_publisher(Config) ->
    ok = ensure_cluster(Config),

    set_config(allow_publish_during_netsplit, true),
    set_config(remote_enqueue_timeout, 500),

    %% Partition the nodenames into two sets
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    {Island1, Island2} = lists:split(length(Nodes) div 2, Nodes),

    %% Connect shared subscriber to the other side of the cluster
    %% (publishes to a shared subscriber are delivered using
    %% the enqueue_remote function in the vmq_cluster mod).
    ConnectSub = packet:gen_connect("netsplit-shared-sub", [{clean_session, true},
                                                            {keepalive, 120}]),
    Connack = packet:gen_connack(0),
    {_, _, PortSub} = random_node(Island1),
    {ok, SubSocket} = packet:do_client_connect(ConnectSub, Connack,
                                             [{port, PortSub}]),
    Subscribe = packet:gen_subscribe(53, "$share/group1/topic", 1),
    Suback = packet:gen_suback(53, 1),
    ok = gen_tcp:send(SubSocket, Subscribe),
    ok = packet:expect_packet(SubSocket, "suback", Suback),

    %% Connect publisher to one side of the cluster
    ConnectPub = packet:gen_connect("netsplit-publisher", [{clean_session, true},
                                                           {keepalive, 120}]),
    {_, _, PortPub} = random_node(Island2),
    {ok, PubSocket} = packet:do_client_connect(ConnectPub, Connack,
                                               [{port, PortPub}]),
    %% Let the cluster metadata converge.
    ok = wait_until_converged(Nodes,
                              fun(N) ->
                                      rpc:call(N, vmq_reg, total_subscriptions, [])
                              end, [{total, 1}]),

    %% Start the actual partition of the nodes.
    {_, Island1Names, _} = lists:unzip3(Island1),
    {_, Island2Names, _} = lists:unzip3(Island2),
    vmq_cluster_test_utils:partition_cluster(Island1Names, Island2Names),
    ok = wait_until_converged(Nodes,
                              fun(N) ->
                                      rpc:call(N, vmq_cluster, is_ready, [])
                              end, false),

    %% Publish
    Publish = packet:gen_publish("topic", 1, <<"message">>,
                                 [{mid, 1}]),
    Puback = packet:gen_puback(1),
    ok = gen_tcp:send(PubSocket, Publish),
    %% The ack should arrive fast as we'll at most blocked up to
    %% `remote_enqueue_timeout` trying to enqueue the msg one the
    %% remote node.
    ok = packet:expect_packet(gen_tcp, PubSocket, "puback", Puback, 5000),
    gen_tcp:close(PubSocket),
    ok.

helper_pub_qos1(ClientId, Publish, Port) ->
    Connect = packet:gen_connect(ClientId, [{keepalive, 60}]),
    Connack = packet:gen_connack(0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, [{port, Port}]),
    ok = gen_tcp:send(Socket, Publish),
    gen_tcp:close(Socket).

ensure_cluster(Config) ->
    vmq_cluster_test_utils:ensure_cluster(Config).

wait_until_converged(Nodes, Fun, ExpectedReturn) ->
    {_, NodeNames, _} = lists:unzip3(Nodes),
    vmq_cluster_test_utils:wait_until(
      fun() ->
              lists:all(fun(X) -> X == true end,
                        vmq_cluster_test_utils:pmap(
                          fun(Node) ->
                                  ExpectedReturn == Fun(Node)
                          end, NodeNames))
      end, 100*2, 500).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
random_node(Nodes) ->
    lists:nth(rand:uniform(length(Nodes)), Nodes).
