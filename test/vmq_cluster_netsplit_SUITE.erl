-module(vmq_cluster_netsplit_SUITE).
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
         register_not_ready_test/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("kernel/include/inet.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("vmq_commons/include/vmq_types.hrl").

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(_Config) ->
    lager:start(),
    %% this might help, might not...
    os:cmd(os:find_executable("epmd")++" -daemon"),
    {ok, Hostname} = inet:gethostname(),
    case net_kernel:start([list_to_atom("runner@"++Hostname), shortnames]) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,
    lager:info("node name ~p", [node()]),
    _Config.

end_per_suite(_Config) ->
    application:stop(lager),
    _Config.

init_per_testcase(Case, Config) ->
    Nodes = vmq_cluster_test_utils:pmap(
              fun({N, P}) ->
                      Node = vmq_cluster_test_utils:start_node(N, Config, Case),
                      {ok, _} = rpc:call(Node, vmq_server_cmd, listener_start,
                                         [P, []]),
                      %% allow all
                      ok = rpc:call(Node, vmq_auth, register_hooks, []),
                      {Node, P}
              end, [{test1, 18883},
                    {test2, 18884},
                    {test3, 18885},
                    {test4, 18886},
                    {test5, 18887}]),
    {CoverNodes, _} = lists:unzip(Nodes),
    {ok, _} = ct_cover:add_nodes(CoverNodes),
    [{nodes, Nodes}|Config].

end_per_testcase(_, _Config) ->
    vmq_cluster_test_utils:pmap(fun(Node) -> ct_slave:stop(Node) end,
                                [test1, test2, test3, test4, test5]),
    ok.

all() ->
    [publish_qos0_test,
     register_consistency_test,
     register_consistency_multiple_sessions_test,
     register_not_ready_test].


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
set_config(Key, Val) ->
    rpc:multicall(vmq_server_cmd, set_config, [Key, Val]),
    ok.

register_consistency_test(Config) ->
    ok = ensure_cluster(Config),
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    {Island1, Island2} = lists:split(length(Nodes) div 2, Nodes),

    %% Create Partitions
    {Island1Names, _} = lists:unzip(Island1),
    {Island2Names, _} = lists:unzip(Island2),
    vmq_cluster_test_utils:partition_cluster(Island1Names, Island2Names),

    ok = wait_until_converged(Nodes,
                         fun(N) ->
                                 rpc:call(N, vmq_cluster, is_ready, [])
                         end, false),

    {_, Island1Port} = random_node(Island1),
    {_, Island2Port} = random_node(Island2),

    Connect = packet:gen_connect("test-client", [{clean_session, true},
                                                 {keepalive, 10}]),
    %% Island 1 should return us the proper CONNACK(3)
    {ok, _} = packet:do_client_connect(Connect, packet:gen_connack(3),
                                       [{port, Island1Port}]),
    %% Island 2 should return us the proper CONACK(3)
    {ok, _} = packet:do_client_connect(Connect, packet:gen_connack(3),
                                       [{port, Island2Port}]),
    vmq_cluster_test_utils:heal_cluster(Island1Names, Island2Names),
    ok.

register_consistency_multiple_sessions_test(Config) ->
    ok = ensure_cluster(Config),
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    {Island1, Island2} = lists:split(length(Nodes) div 2, Nodes),

    %% we configure the nodes to trade consistency for availability
    set_config(trade_consistency, true),
    set_config(allow_multiple_sessions, true),

    %% Create Partitions
    {Island1Names, _} = lists:unzip(Island1),
    {Island2Names, _} = lists:unzip(Island2),
    vmq_cluster_test_utils:partition_cluster(Island1Names, Island2Names),

    {_, Island1Port} = random_node(Island1),
    {_, Island2Port} = random_node(Island2),

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
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    {Island1, Island2} = lists:split(length(Nodes) div 2, Nodes),

    %% Connect a test-client
    Connect = packet:gen_connect("test-client-not-ready", [{clean_session, true},
                                                 {keepalive, 10}]),
    Connack = packet:gen_connack(0),
    {_, Port} = random_node(Nodes),
    {ok, _Socket} = packet:do_client_connect(Connect, Connack,
                                             [{port, Port}]),

    %% Create Partitions
    {Island1Names, _} = lists:unzip(Island1),
    {Island2Names, _} = lists:unzip(Island2),
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
     end || {_, P} <- Nodes],

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
     end || {_, P} <- Nodes],
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
    {_, Island1Port} = random_node(Island1),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, [{port,
                                                                Island1Port}]),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = wait_until_converged(Nodes,
                         fun(N) ->
                                 rpc:call(N, vmq_reg, total_subscriptions, [])
                         end, [{total, 1}]),

    %% Create Partitions
    {Island1Names, _} = lists:unzip(Island1),
    {Island2Names, _} = lists:unzip(Island2),
    vmq_cluster_test_utils:partition_cluster(Island1Names, Island2Names),

    ok = wait_until_converged(Nodes,
                         fun(N) ->
                                 rpc:call(N, vmq_cluster, is_ready, [])
                         end, false),

    {_, Island2Port} = random_node(Island2),
    set_config(trade_consistency, true),
    set_config(allow_multiple_sessions, true),
    Publish = packet:gen_publish("netsplit/0/test", 0, <<"message">>,
                                 [{mid, 1}]),
    helper_pub_qos1("test-netsplit-sender", Publish, Island2Port),

    %% fix the network
    vmq_cluster_test_utils:heal_cluster(Island1Names, Island2Names),

    %% the publish is expected once the netsplit is fixed
    ok = packet:expect_packet(Socket, "publish", Publish).


helper_pub_qos1(ClientId, Publish, Port) ->
    Connect = packet:gen_connect(ClientId, [{keepalive, 60}]),
    Connack = packet:gen_connack(0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, [{port, Port}]),
    ok = gen_tcp:send(Socket, Publish),
    gen_tcp:close(Socket).

ensure_cluster(Config) ->
    [{Node1, _}|OtherNodes] = Nodes = proplists:get_value(nodes, Config),
    [begin
         {ok, _} = rpc:call(Node, vmq_server_cmd, node_join, [Node1])
     end || {Node, _} <- OtherNodes],
    {NodeNames, _} = lists:unzip(Nodes),
    Expected = lists:sort(NodeNames),
    ok = vmq_cluster_test_utils:wait_until_joined(NodeNames, Expected),
    [?assertEqual({Node, Expected}, {Node,
                                     lists:sort(vmq_cluster_test_utils:get_cluster_members(Node))})
     || Node <- NodeNames],
    ok.

wait_until_converged(Nodes, Fun, ExpectedReturn) ->
    {NodeNames, _} = lists:unzip(Nodes),
    vmq_cluster_test_utils:wait_until(
      fun() ->
              lists:all(fun(X) -> X == true end,
                        vmq_cluster_test_utils:pmap(
                          fun(Node) ->
                                  ExpectedReturn == Fun(Node)
                          end, NodeNames))
      end, 60*2, 500).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
random_node(Nodes) ->
    lists:nth(random:uniform(length(Nodes)), Nodes).
