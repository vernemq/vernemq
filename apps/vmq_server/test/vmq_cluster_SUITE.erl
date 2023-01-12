-module(vmq_cluster_SUITE).
-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([multiple_connect_test/1,
         multiple_connect_unclean_test/1,
         distributed_subscribe_test/1,
         racing_connect_test/1,
         racing_subscriber_test/1,
         aborted_queue_migration_test/1,
         cluster_leave_test/1,
         cluster_leave_myself_test/1,
         cluster_leave_dead_node_test/1,
         shared_subs_random_policy_test/1,
         shared_subs_random_policy_online_first_test/1,
         shared_subs_prefer_local_policy_test/1,
         shared_subs_local_only_policy_test/1,
         cross_node_publish_subscribe/1,
         routing_table_survives_node_restart/1,
         convert_new_msgs_to_old_format/1]).

-export([hook_uname_password_success/5,
         hook_auth_on_publish/6,
         hook_auth_on_subscribe/3]).

-compile(nowarn_deprecated_function).

-include_lib("common_test/include/ct.hrl").
-include_lib("kernel/include/inet.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("vmq_commons/include/vmq_types.hrl").
-include("src/vmq_server.hrl").


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

init_per_testcase(convert_new_msgs_to_old_format, Config) ->
    %% no setup necessary,
    Config;
init_per_testcase(Case, Config) ->
    vmq_test_utils:seed_rand(Config),
    Nodes = vmq_cluster_test_utils:pmap(
              fun({N, P}) ->
                      Node = vmq_cluster_test_utils:start_node(N, Config, Case),
                      {ok, _} = rpc:call(Node, vmq_server_cmd, listener_start,
                                         [P, []]),
                      %% allow all
                      ok = rpc:call(Node, vmq_auth, register_hooks, []),
                      rpc:call(Node, vmq_subscriber_db, flushall, []),
                      {Node, P}
              end, [{test1, 18883},
                    {test2, 18884},
                    {test3, 18885}]),
    {CoverNodes, _} = lists:unzip(Nodes),
    {ok, _} = ct_cover:add_nodes(CoverNodes),
    [{nodes, Nodes},{nodenames, [test1, test2, test3]}|Config].

end_per_testcase(convert_new_msgs_to_old_format, Config) ->
    %% no teardown necessary,
    Config;
end_per_testcase(_, _Config) ->
    vmq_cluster_test_utils:pmap(fun(Node) -> ct_slave:stop(Node) end,
                                [test1, test2, test3]),
    ok.

all() ->
    [
     multiple_connect_test
    ,multiple_connect_unclean_test
    ,distributed_subscribe_test
    ,racing_connect_test
    ,racing_subscriber_test
    ,aborted_queue_migration_test
    ,cluster_leave_test
    ,cluster_leave_myself_test
    ,cluster_leave_dead_node_test
    ,shared_subs_random_policy_test
    ,shared_subs_random_policy_online_first_test
    ,shared_subs_prefer_local_policy_test
    ,shared_subs_local_only_policy_test
    ,cross_node_publish_subscribe
    ,routing_table_survives_node_restart
    ,convert_new_msgs_to_old_format
    ].


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
multiple_connect_test(Config) ->
    ok = ensure_cluster(Config),
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    NrOfConnects = 250,
    NrOfProcesses = NrOfConnects div 50, %rand:uniform(NrOfConnects),
    NrOfMsgsPerProcess = NrOfConnects div NrOfProcesses,
    publish(Nodes, NrOfProcesses, NrOfMsgsPerProcess),
    done = receive_times(done, NrOfProcesses),
    true = check_unique_client("connect-multiple", Nodes),
    Config.

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

multiple_connect_unclean_test(Config) ->
    %% This test makes sure that a cs false subscriber can receive QoS
    %% 1 messages, one message at a time only acknowleding one message
    %% per connection.
    ok = ensure_cluster(Config),
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    Topic = "qos1/multiple/test",
    Connect = packet:gen_connect("connect-unclean", [{clean_session, false},
                                                      {keepalive, 60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(123, Topic, 1),
    Suback = packet:gen_suback(123, 1),
    Disconnect = packet:gen_disconnect(),
    {RandomNode, RandomPort} = random_node(Nodes),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, [{port,
                                                                RandomPort}]),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = gen_tcp:send(Socket, Disconnect),
    gen_tcp:close(Socket),
    [PublishNode|_] = Nodes,
    Payloads = publish_random([PublishNode], 20, Topic),
    ok = vmq_cluster_test_utils:wait_until(
           fun() ->
                   StoredMsgs = rpc:call(RandomNode, vmq_reg, stored,
                       [{"", <<"connect-unclean">>}]),
                   io:format(user, "~nStored Messages: ~p~n", [StoredMsgs]),
                   20 == StoredMsgs
           end, 60, 500),
    ok = receive_publishes(Nodes, Topic, Payloads).

distributed_subscribe_test(Config) ->
    ok = ensure_cluster(Config),
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    Topic = "qos1/distributed/test",
    Sockets =
    [begin
         Connect = packet:gen_connect("connect-" ++ integer_to_list(Port),
                                      [{clean_session, true},
                                       {keepalive, 60}]),
         Connack = packet:gen_connack(0),
         Subscribe = packet:gen_subscribe(123, Topic, 1),
         Suback = packet:gen_suback(123, 1),
         {ok, Socket} = packet:do_client_connect(Connect, Connack, [{port, Port}]),
         ok = gen_tcp:send(Socket, Subscribe),
         ok = packet:expect_packet(Socket, "suback", Suback),
         Socket
     end || {_, Port} <- Nodes],
    [PubSocket|Rest] = Sockets,
    Publish = packet:gen_publish(Topic, 1, <<"test-message">>, [{mid, 1}]),
    Puback = packet:gen_puback(1),
    ok = gen_tcp:send(PubSocket, Publish),
    ok = packet:expect_packet(PubSocket, "puback", Puback),
    _ = [begin
             ok = packet:expect_packet(Socket, "publish", Publish),
             ok = gen_tcp:send(Socket, Puback),
             gen_tcp:close(Socket)
         end || Socket <- Rest],
    Config.

racing_connect_test(Config) ->
    ok = ensure_cluster(Config),
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    Connect = packet:gen_connect("connect-racer",
                                 [{clean_session, false},
                                  {keepalive, 60}]),
    Pids =
    [begin
         Connack =
         case I of
             1 ->
                 %% no session present
                 packet:gen_connack(false, 0);
             2 ->
                 %% second iteration, wait for all nodes to catch up
                 %% this is required to create proper connack
                 ok = wait_until_converged(Nodes,
                                           fun(N) ->
                                                   case rpc:call(N, vmq_subscriber_db, read, [{"", <<"connect-racer">>}, undefined]) of
                                                       undefined -> false;
                                                       [{_, false, []}] -> true
                                                   end
                                           end, true),
                 packet:gen_connack(true, 0);
             _ ->

                 packet:gen_connack(true, 0)
         end,
         spawn_link(
           fun() ->
                   {_RandomNode, RandomPort} = random_node(Nodes),
                   {ok, Socket} = packet:do_client_connect(Connect, Connack, [{port,
                                                                               RandomPort}]),
                   inet:setopts(Socket, [{active, true}]),
                   receive
                       {tcp_closed, Socket} ->
                           %% we should be kicked out by the subsequent client
                           ok;
                       {lastman, test_over} ->
                           ok;
                       M ->
                           exit({unknown_message, M})
                   end
           end)
     end || I <- lists:seq(1, 25)],

    LastManStanding = fun(F) ->
                              case [Pid || Pid <- Pids, is_process_alive(Pid)] of
                                  [LastMan] -> LastMan;
                                  [] ->
                                      exit({no_session_left});
                                  _ ->
                                      timer:sleep(10),
                                      F(F)
                              end
                      end,
    LastMan = LastManStanding(LastManStanding),
    %% Tell the last process the test is over and wait for it to
    %% terminate before ending the test and tearing down the test
    %% nodes.
    LastManRef = monitor(process, LastMan),
    LastMan ! {lastman, test_over},
    receive
        {'DOWN', LastManRef, process, _, normal} ->
            ok
    after
        3000 ->
            throw("no DOWN msg received from LastMan")
    end,
    Config.

aborted_queue_migration_test(Config) ->
    ok = ensure_cluster(Config),
    {_, [{Node, Port}|RestNodes] = _Nodes} = lists:keyfind(nodes, 1, Config),
    Connect = packet:gen_connect("connect-aborter",
                                 [{clean_session, false},
                                  {keepalive, 60}]),
    Topic = "migration/abort/test",
    Subscribe = packet:gen_subscribe(123, Topic, 1),
    Suback = packet:gen_suback(123, 1),

    %% no session present
    Connack1 = packet:gen_connack(false, 0),
    {ok, Socket1} = packet:do_client_connect(Connect, Connack1, [{port, Port}]),
    ok = gen_tcp:send(Socket1, Subscribe),
    ok = packet:expect_packet(Socket1, "suback", Suback),
    gen_tcp:close(Socket1),
    %% publish 10 messages
    [PublishNode|_] = RestNodes,
    _Payloads = publish_random([PublishNode], 10, Topic),

    %% wait until the queue has all 10 messages stored
    ok = vmq_cluster_test_utils:wait_until(
           fun() ->
                   {0, 0, 0, 1, 10} == rpc:call(Node, vmq_queue_sup_sup, summary, [])
           end, 60, 500),

    %% connect and disconnect/exit right away
    {RandomNode, RandomPort} = random_node(RestNodes),
    {ok, Socket2} = gen_tcp:connect("localhost",  RandomPort, [binary, {reuseaddr, true}, {active, false}, {packet, raw}]),
    gen_tcp:send(Socket2, Connect),
    gen_tcp:close(Socket2),

    %% although the connect flow didn't finish properly (because we closed the
    %% connection right away) the messages MUST be migrated to the 'RandomNode'
    ok = vmq_cluster_test_utils:wait_until(
           fun() ->
                   {0, 0, 0, 1, 10} == rpc:call(RandomNode, vmq_queue_sup_sup, summary, [])
           end, 60, 500).

racing_subscriber_test(Config) ->
    ok = ensure_cluster(Config),
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    Connect = packet:gen_connect("connect-racer",
                                 [{clean_session, false},
                                  {keepalive, 60}]),
    Topic = "racing/subscriber/test",
    Subscribe = packet:gen_subscribe(123, Topic, 1),
    Suback = packet:gen_suback(123, 1),
    Pids =
    [begin
         Connack =
         case I of
             1 ->
                 %% no session present
                 packet:gen_connack(false, 0);
             2 ->
                 %% second iteration, wait for all nodes to catch up
                 %% this is required to create proper connack
                 ok = wait_until_converged(Nodes,
                                           fun(N) ->
                                                   case rpc:call(N, vmq_subscriber_db, read, [{"", "connect-racer"}, []]) of
                                                       [] -> false;
                                                       [{_, false, _}] -> true
                                                   end
                                           end, true),
                 packet:gen_connack(true, 0);
             _ ->
                 packet:gen_connack(true, 0)
         end,
         spawn_link(
           fun() ->
                   {_RandomNode, RandomPort} = random_node(Nodes),
                   case packet:do_client_connect(Connect, Connack, [{port, RandomPort}]) of
                       {ok, Socket} ->
                           case gen_tcp:send(Socket, Subscribe) of
                               ok ->
                                   case packet:expect_packet(Socket, "suback", Suback) of
                                       ok ->
                                           inet:setopts(Socket, [{active, true}]),
                                           receive
                                               {tcp_closed, Socket} ->
                                                   %% we should be kicked out by the subsequent client
                                                   ok;
                                               {lastman, test_over} ->
                                                   ok;
                                               M ->
                                                   exit({unknown_message, M})
                                           end;
                                       {error, closed} ->
                                           ok
                                   end;
                               {error, closed} ->
                                   %% it's possible that we can't even subscribe due to
                                   %% a racing subscriber
                                   ok
                           end;
                       {error, closed} ->
                           ok
                   end
           end)
     end || I <- lists:seq(1, 25)],

    LastManStanding = fun(F) ->
                              case [Pid || Pid <- Pids, is_process_alive(Pid)] of
                                  [LastMan] -> LastMan;
                                  [] ->
                                      exit({no_session_left});
                                  _ ->
                                      timer:sleep(10),
                                      F(F)
                              end
                      end,
    LastMan = LastManStanding(LastManStanding),
    %% Tell the last process the test is over and wait for it to
    %% terminate before ending the test and tearing down the test
    %% nodes.
    LastManRef = monitor(process, LastMan),
    LastMan ! {lastman, test_over},
    receive
        {'DOWN', LastManRef, process, _, normal} ->
            ok
    after
        3000 ->
            throw("no DOWN msg received from LastMan")
    end,
    Config.

cluster_leave_test(Config) ->
    ok = ensure_cluster(Config),
    {_, [{Node, Port}|RestNodes] = _Nodes} = lists:keyfind(nodes, 1, Config),
    Topic = "cluster/leave/topic",
    ToMigrate = 8,
    %% create ToMigrate sessions
    [PubSocket|_] = _Sockets =
    [begin
         Connect = packet:gen_connect("connect-" ++ integer_to_list(I),
                                      [{clean_session, false},
                                       {keepalive, 60}]),
         Connack = packet:gen_connack(0),
         Subscribe = packet:gen_subscribe(123, Topic, 1),
         Suback = packet:gen_suback(123, 1),
         {ok, Socket} = packet:do_client_connect(Connect, Connack, [{port, Port}]),
         ok = gen_tcp:send(Socket, Subscribe),
         ok = packet:expect_packet(Socket, "suback", Suback),
         Socket
     end || I <- lists:seq(1,ToMigrate)],
    %% publish a message for every session
    Publish = packet:gen_publish(Topic, 1, <<"test-message">>, [{mid, 1}]),
    Puback = packet:gen_puback(1),
    ok = gen_tcp:send(PubSocket, Publish),
    ok = packet:expect_packet(PubSocket, "puback", Puback),
    ok = vmq_cluster_test_utils:wait_until(
           fun() ->
                   {ToMigrate, 0, 0, 0, 0} == rpc:call(Node, vmq_queue_sup_sup, summary, [])
           end, 60, 500),
    %% Pick a control node for initiating the cluster leave
    [{CtrlNode, CtrlPort}|_] = RestNodes,
    {ok, _} = rpc:call(CtrlNode, vmq_server_cmd, node_leave, [Node]),
    %% Leaving Node will disconnect all sessions and inflight messages will be stored in offline store
    %% On reconnect the stored offline messages will be replayed as if session is migrated
    [begin
         Connect = packet:gen_connect("connect-" ++ integer_to_list(I),
             [{clean_session, false},
                 {keepalive, 60}]),
         Connack = packet:gen_connack(1, 0),
         {ok, Socket} = packet:do_client_connect(Connect, Connack, [{port, CtrlPort}]),
         timer:sleep(500),
         %% The above interval will ensure msg is passed to connection process before closing the socket
         %% Otherwise, the msg from online queue might get lost
         gen_tcp:close(Socket),
         Socket
     end || I <- lists:seq(1,ToMigrate)],
    ok = vmq_cluster_test_utils:wait_until(
        fun() ->
            Res = rpc:call(CtrlNode, vmq_queue_sup_sup, summary, []),
            io:format(user, "~nQueueSupSup Summary: ~p~n", [Res]),
            {0, 0, 0, ToMigrate, ToMigrate} == Res
        end, 20, 500).

cluster_leave_myself_test(Config) ->
    ok = ensure_cluster(Config),
    {_, [{Node, _Port}|RestNodesWithPorts]} = lists:keyfind(nodes, 1, Config),
    {RestNodes, _} = lists:unzip(RestNodesWithPorts),
    {ok, _} = rpc:call(Node, vmq_server_cmd, node_leave, [Node]),

    %% check that the leave was propagated to the rest
    ok = wait_until_converged(RestNodesWithPorts,
                              fun(N) ->
                                      NSStats = rpc:call(N, vmq_cluster, netsplit_statistics, []),
                                      Nodes = lists:usort(rpc:call(N, vmq_cluster, nodes, [])),
                                      {NSStats, Nodes}
                              end, {{0,0}, lists:usort(RestNodes)}).

cluster_leave_dead_node_test(Config) ->
    ok = ensure_cluster(Config),
    {_, [{Node, Port}|RestNodes] = Nodes} = lists:keyfind(nodes, 1, Config),
    {_, [Nodename|_]} = lists:keyfind(nodenames, 1, Config),

    {NodeNames, _} = lists:unzip(Nodes),
    [ rpc:call(N, lager, set_loglevel, [lager_console_backend, debug]) || N <- NodeNames ],
    Topic = "cluster/leave/dead/topic",
    %% create 10 sessions on first Node
    _ =
    [begin
         Connect = packet:gen_connect("connect-d-" ++ integer_to_list(I),
                                      [{clean_session, false},
                                       {keepalive, 60}]),
         Connack = packet:gen_connack(0),
         Subscribe = packet:gen_subscribe(123, Topic, 1),
         Suback = packet:gen_suback(123, 1),
         {ok, Socket} = packet:do_client_connect(Connect, Connack, [{port, Port}]),
         ok = gen_tcp:send(Socket, Subscribe),
         ok = packet:expect_packet(Socket, "suback", Suback),
         gen_tcp:send(Socket, packet:gen_disconnect()),
         gen_tcp:close(Socket)
     end || I <- lists:seq(1,10)],
    %% stop first node
    {ok, Node} = ct_slave:stop(Nodename),

    %% Pick a control node for initiating the cluster leave
    %% let first node leave the cluster, this should migrate the sessions,
    %% but not the messages
    [{CtrlNode, _}|_] = RestNodes,
    %% Node_leave might return before migration has finished
    {ok, _} = rpc:call(CtrlNode, vmq_server_cmd, node_leave, [Node]),
    %% The disconnected sessions are lazily migrated to the rest of the nodes on reconnection
    ok = wait_until_converged(RestNodes,
                              fun(N) ->
                                      rpc:call(N, vmq_queue_sup_sup, summary, [])
                              end, {0, 0, 0, 0, 0}).

shared_subs_prefer_local_policy_test(Config) ->
    ensure_cluster(Config),
    [LocalNode|OtherNodes] = _Nodes = nodes_(Config),
    set_shared_subs_policy(prefer_local, nodenames(Config)),

    LocalSubscriberSockets = connect_subscribers(<<"$share/share/sharedtopic">>, 5, [LocalNode]),
    RemoteSubscriberSockets = connect_subscribers(<<"$share/share/sharedtopic">>, 5, OtherNodes),

    %% publish to shared topic on local node
    {_, LocalPort} = LocalNode,
    Connect = packet:gen_connect("ss-publisher",
                                 [{keepalive, 60}, {clean_session, true}]),
    Connack = packet:gen_connack(0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, [{port, LocalPort}]),
    Payloads = publish_to_topic(Socket, <<"sharedtopic">>, 10),
    Disconnect = packet:gen_disconnect(),
    ok = gen_tcp:send(Socket, Disconnect),
    ok = gen_tcp:close(Socket),

    %% receive on subscriber sockets.
    spawn_receivers(LocalSubscriberSockets),
    receive_msgs(Payloads),
    receive_nothing(200),
    spawn_receivers(RemoteSubscriberSockets),
    receive_nothing(200),

    %% cleanup
    [ ok = gen_tcp:close(S) || S <- LocalSubscriberSockets ++ RemoteSubscriberSockets ],
    ok.

shared_subs_local_only_policy_test(Config) ->
    ensure_cluster(Config),
    [LocalNode|OtherNodes] = _Nodes = nodes_(Config),
    set_shared_subs_policy(local_only, nodenames(Config)),

    LocalSubscriberSockets = connect_subscribers(<<"$share/share/sharedtopic">>, 5, [LocalNode]),
    RemoteSubscriberSockets = connect_subscribers(<<"$share/share/sharedtopic">>, 5, OtherNodes),

    %% publish to shared topic on local node
    {_, LocalPort} = LocalNode,
    Connect = packet:gen_connect("ss-publisher",
                                 [{keepalive, 60}, {clean_session, true}]),
    Connack = packet:gen_connack(0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, [{port, LocalPort}]),
    Payloads = publish_to_topic(Socket, <<"sharedtopic">>, 10),

    %% receive on subscriber sockets.
    spawn_receivers(LocalSubscriberSockets),
    receive_msgs(Payloads),
    receive_nothing(200),
    spawn_receivers(RemoteSubscriberSockets),
    receive_nothing(200),

    %% disconnect all locals
    Disconnect = packet:gen_disconnect(),
    [begin
         ok = gen_tcp:send(S, Disconnect),
         ok = gen_tcp:close(S)
     end || S <- LocalSubscriberSockets],

    _ = publish_to_topic(Socket, <<"sharedtopic">>, 11, 20),
    ok = gen_tcp:send(Socket, Disconnect),
    ok = gen_tcp:close(Socket),

    %% receive nothing as policy was local_only
    receive_nothing(200),

    %% cleanup
    [ ok = gen_tcp:close(S) || S <- RemoteSubscriberSockets ],
    ok.

shared_subs_random_policy_test(Config) ->
    ensure_cluster(Config),
    Nodes = nodes_(Config),
    set_shared_subs_policy(random, nodenames(Config)),

    SubscriberSockets = connect_subscribers(<<"$share/share/sharedtopic">>, 10, Nodes),

    %% publish to shared topic on random node
    {_, Port} = random_node(Nodes),
    Connect = packet:gen_connect("ss-publisher",
                                 [{keepalive, 60}, {clean_session, true}]),
    Connack = packet:gen_connack(0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, [{port, Port}]),
    Payloads = publish_to_topic(Socket, <<"sharedtopic">>, 10),
    Disconnect = packet:gen_disconnect(),
    ok = gen_tcp:send(Socket, Disconnect),
    ok = gen_tcp:close(Socket),

    %% receive on subscriber sockets.
    spawn_receivers(SubscriberSockets),
    receive_msgs(Payloads),
    receive_nothing(200),

    %% cleanup
    [ ok = gen_tcp:close(S) || S <- SubscriberSockets ],
    ok.

shared_subs_random_policy_online_first_test(Config) ->
    ensure_cluster(Config),
    Nodes = nodes_(Config),
    set_shared_subs_policy(random, nodenames(Config)),

    [OnlineSubNode | RestNodes] = Nodes,
    ok = create_offline_subscribers(<<"$share/share/sharedtopic">>, 10, RestNodes),
    SubscriberSocketsOnline = connect_subscribers(<<"$share/share/sharedtopic">>, 1, [OnlineSubNode]),

    %% publish to shared topic on random node
    {_, Port} = random_node(RestNodes),
    Connect = packet:gen_connect("ss-publisher",
        [{keepalive, 60}, {clean_session, true}]),
    Connack = packet:gen_connack(0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, [{port, Port}]),
    Payloads = publish_to_topic(Socket, <<"sharedtopic">>, 10),
    Disconnect = packet:gen_disconnect(),
    ok = gen_tcp:send(Socket, Disconnect),
    ok = gen_tcp:close(Socket),

    %% receive on subscriber sockets.
    spawn_receivers(SubscriberSocketsOnline),
    receive_msgs(Payloads),
    receive_nothing(200),

    %% cleanup
    [ ok = gen_tcp:close(S) || S <- SubscriberSocketsOnline ],
    ok.

routing_table_survives_node_restart(Config) ->
    %% Ensure that we subscribers can still receive publishes from a
    %% node that has been restarted. I.e., this ensures that the
    %% routing table of the restarted node is intact.
    ok = ensure_cluster(Config),
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    [{RestartNodeName, RestartNodePort},
     {_, OtherNodePort}|_] = Nodes,

    %% Connect and subscribe on a node with cs true
    Topic = <<"topic/sub">>,
    SharedTopic = <<"$share/group/sharedtopic">>,
    ClientId = "restart-node-test-subscriber",
    SubSocket = connect(OtherNodePort, ClientId, [{keepalive, 60}, {clean_session, true}]),
    subscribe(SubSocket, Topic, 1),
    subscribe(SubSocket, SharedTopic, 1),


    %% Restart the node.
    _ = ct_slave:stop(RestartNodeName),
    RestartNodeName = vmq_cluster_test_utils:start_node(nodename(RestartNodeName), Config,
                                                        routing_table_survives_node_restart),
    %% Make sure cluster is ready
    ok = vmq_cluster_test_utils:wait_until_ready([N || {N, _} <- Nodes]),
    {ok, _} = rpc:call(RestartNodeName, vmq_server_cmd, listener_start, [RestartNodePort, []]),
    ok = rpc:call(RestartNodeName, vmq_auth, register_hooks, []),

    %% Publish to the subscribed topics
    Connect = packet:gen_connect("restart-node-test-publisher",
                                 [{keepalive, 60}, {clean_session, true}]),
    Connack = packet:gen_connack(0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, [{port, RestartNodePort}]),
    Payloads = publish_to_topic(Socket, Topic, 1, 5),
    PayloadsShared = publish_to_topic(Socket, <<"sharedtopic">>, 6, 10),
    Disconnect = packet:gen_disconnect(),
    ok = gen_tcp:send(Socket, Disconnect),
    ok = gen_tcp:close(Socket),

    %% Make sure everything arrives successfully
    spawn_receivers([SubSocket]),
    receive_msgs(Payloads ++ PayloadsShared),
    receive_nothing(200).

cross_node_publish_subscribe(Config) ->
    %% Make sure all subscribers on a cross-node publish receive the
    %% published messages.
    ok = ensure_cluster(Config),
    {_, Nodes} = lists:keyfind(nodes, 1, Config),

    Topic = <<"cross-node-topic">>,

    %% 1. Connect two or more subscribers to node1.
    [LocalNode|OtherNodes] = Nodes = nodes_(Config),
    LocalSubscriberSockets = connect_subscribers(Topic, 5, [LocalNode]),

    %% 2. Connect and publish on another node.
    {_, OtherPort} = random_node(OtherNodes),
    Connect = packet:gen_connect("publisher",
                                 [{keepalive, 60}, {clean_session, true}]),
    Connack = packet:gen_connack(0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, [{port, OtherPort}]),
    Payloads = publish_to_topic(Socket, Topic, 10),
    Disconnect = packet:gen_disconnect(),
    ok = gen_tcp:send(Socket, Disconnect),
    ok = gen_tcp:close(Socket),

    %% 3. Check our subscribers received all messages.
    %% receive on subscriber sockets.
    spawn_receivers(LocalSubscriberSockets),

    %% the payloads will be received 5 times as all subscribers will
    %% get a copy.
    receive_msgs(Payloads
                 ++ Payloads
                 ++ Payloads
                 ++ Payloads
                 ++ Payloads),
    receive_nothing(200).

convert_new_msgs_to_old_format(_Config) ->
    %% create a #vmq_msg{} as a raw tuple.
    Orig = {
      %% record name,
      vmq_msg,

      %% field values
      "msg_ref",
      "routing_key",
      "payload",
      "retain",
      "dup",
      "qos",
      "mountpoint",
      "persisted",
      "sg_policy",
      "properties",
      "expiry_ts"
     },

    %% fail if new items were added to the #vmq_msg{} record:
    #vmq_msg{} = Orig,

    %% test identity
    Orig = vmq_cluster_com:to_vmq_msg(Orig),

    %% test we can strip away extra tuple elements an the result is
    %% the original record.
    Extended = list_to_tuple(tuple_to_list(Orig) ++ [a,b,c]),
    Orig = vmq_cluster_com:to_vmq_msg(Extended).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
create_offline_subscribers(Topic, Number, Nodes) ->
    lists:foreach(fun(I) ->
                    {_, Port} = random_node(Nodes),
                    ClientId = "subscriber-" ++ integer_to_list(I) ++ "-node-" ++
                        integer_to_list(Port),
                    Connect = packet:gen_connect(ClientId,
                        [{keepalive, 60}, {clean_session, false}]),
                    Connack = packet:gen_connack(0),
                    Subscribe = packet:gen_subscribe(1, [Topic], 1),
                    Suback = packet:gen_suback(1, 1),
                    %% TODO: make it connect to random node instead
                    {ok, Socket} = packet:do_client_connect(Connect, Connack, [{port, Port}]),
                    ok = gen_tcp:send(Socket, Subscribe),
                    ok = packet:expect_packet(Socket, "suback", Suback),
                    Disconnect = packet:gen_disconnect(),
                    ok = gen_tcp:send(Socket, Disconnect),
                    ok = gen_tcp:close(Socket),
                    %% wait for the client to be offline
                    timer:sleep(500)
                  end, lists:seq(1,Number)).

connect_subscribers(Topic, Number, Nodes) ->
    [begin

         {_, Port} = random_node(Nodes),
         Connect = packet:gen_connect("subscriber-" ++ integer_to_list(I) ++ "-node-" ++
                                          integer_to_list(Port),
                                      [{keepalive, 60}, {clean_session, true}]),
         Connack = packet:gen_connack(0),
         Subscribe = packet:gen_subscribe(1, [Topic], 1),
         Suback = packet:gen_suback(1, 1),
         %% TODO: make it connect to random node instead
         {ok, Socket} = packet:do_client_connect(Connect, Connack, [{port, Port}]),
         ok = gen_tcp:send(Socket, Subscribe),
         ok = packet:expect_packet(Socket, "suback", Suback),
         Socket
     end || I <- lists:seq(1,Number)].

publish_to_topic(Socket, Topic, Number) when Number > 1 ->
    publish_to_topic(Socket, Topic, 1, Number).

publish_to_topic(Socket, Topic, Begin, End) when Begin < End ->
    [begin
         Payload = vmq_test_utils:rand_bytes(5),
         Publish = packet:gen_publish(Topic, 1, Payload, [{mid, I}]),
         Puback = packet:gen_puback(I),
         ok = gen_tcp:send(Socket, Publish),
         ok = packet:expect_packet(Socket, "puback", Puback),
         Payload
     end || I <- lists:seq(Begin,End)].

set_shared_subs_policy(Policy, Nodes) ->
    lists:foreach(
      fun(N) ->
              {ok, []} = rpc:call(N, vmq_server_cmd, set_config, [shared_subscription_policy, Policy])
      end,
      Nodes).

nodenames(Config) ->
    {NodeNames, _} = lists:unzip(nodes_(Config)),
    NodeNames.

nodes_(Config) ->
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    Nodes.

spawn_receivers(ReceiverSockets) ->
    Master = self(),
    lists:map(
      fun(S) ->
              spawn_link(fun() -> recv_and_forward_msg(S, Master, <<>>) end)
      end,
      ReceiverSockets).


receive_msgs(Payloads) ->
    receive_msgs(Payloads, 5000).

receive_msgs([], _Wait) ->
    ok;
receive_msgs(Payloads, Wait) ->
    receive
        #mqtt_publish{payload=Payload} ->
            true = lists:member(Payload, Payloads),
            receive_msgs(Payloads -- [Payload])
    after
        Wait -> throw({wait_for_messages_timeout, Payloads, erlang:get_stacktrace()})
    end.

receive_nothing(Wait) ->
    receive
        X -> throw({received_unexpected_msgs, X, erlang:get_stacktrace()})
    after
        Wait -> ok
    end.

recv_and_forward_msg(Socket, Dest, Rest) ->
    case recv_all(Socket, Rest) of
        {ok, Frames, Rest} ->
            lists:foreach(
              fun(#mqtt_publish{message_id = MsgId} = Msg) ->
                      ok = gen_tcp:send(Socket, packet:gen_puback(MsgId)),
                      Dest ! Msg
              end,
              Frames),
            recv_and_forward_msg(Socket, Dest, Rest);
        {error, _Reason} ->
            ok
    end.

publish(Nodes, NrOfProcesses, NrOfMsgsPerProcess) ->
    publish(self(), Nodes, NrOfProcesses, NrOfMsgsPerProcess, []).

publish(_, _, 0, _, Pids) -> Pids;
publish(Self, [Node|Rest] = Nodes, NrOfProcesses, NrOfMsgsPerProcess, Pids) ->
    Pid = spawn_link(fun() -> publish_(Self, {Node, Nodes}, NrOfMsgsPerProcess) end),
    publish(Self, Rest ++ [Node], NrOfProcesses -1, NrOfMsgsPerProcess, [Pid|Pids]).

publish_(Self, Node, NrOfMsgsPerProcess) ->
    publish__(Self, Node, NrOfMsgsPerProcess).
publish__(Self, _, 0) ->
    Self ! done;
publish__(Self, {{_, Port}, Nodes} = Conf, NrOfMsgsPerProcess) ->
    Connect = packet:gen_connect("connect-multiple", [{keepalive, 10}]),
    Connack = packet:gen_connack(0),
    case packet:do_client_connect(Connect, Connack, [{port, Port}]) of
        {ok, Socket} ->
            check_unique_client("connect-multiple", Nodes),
            gen_tcp:close(Socket),
            timer:sleep(rand:uniform(100)),
            publish__(Self, Conf, NrOfMsgsPerProcess - 1);
        {error, closed} ->
            %% this happens if at the same time the same client id
            %% connects to the cluster
            timer:sleep(rand:uniform(100)),
            publish__(Self, Conf, NrOfMsgsPerProcess)
    end.

publish_random(Nodes, N, Topic) ->
    publish_random(Nodes, N, Topic, []).

publish_random(_, 0, _, Acc) -> Acc;
publish_random(Nodes, N, Topic, Acc) ->
    Connect = packet:gen_connect("connect-unclean-pub", [{clean_session, true},
                                                         {keepalive, 10}]),
    Connack = packet:gen_connack(0),
    Payload = vmq_test_utils:rand_bytes(rand:uniform(50)),
    Publish = packet:gen_publish(Topic, 1, Payload, [{mid, N}]),
    Puback = packet:gen_puback(N),
    Disconnect = packet:gen_disconnect(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, opts(Nodes)),
    ok = gen_tcp:send(Socket, Publish),
    ok = packet:expect_packet(Socket, "puback", Puback),
    ok = gen_tcp:send(Socket, Disconnect),
    gen_tcp:close(Socket),
    publish_random(Nodes, N - 1, Topic, [Payload|Acc]).

receive_publishes(_, _, []) -> ok;
receive_publishes([{_,Port}=N|Nodes], Topic, Payloads) ->
    Connect = packet:gen_connect("connect-unclean", [{clean_session, false},
                                                     {keepalive, 10}]),
    Connack = packet:gen_connack(true, 0),
    Opts = [{port, Port}],
    {ok, Socket} = packet:do_client_connect(Connect, Connack, Opts),
    case recv(Socket, <<>>) of
        {ok, #mqtt_publish{message_id=MsgId, payload=Payload}} ->
            ok = gen_tcp:send(Socket, packet:gen_puback(MsgId)),
            receive_publishes(Nodes ++ [N], Topic, Payloads -- [Payload]);
        {error, _} ->
            receive_publishes(Nodes ++ [N], Topic, Payloads)
    end.

recv(Socket, Buf) ->
    case recv_all(Socket, Buf) of
        {ok, [], Rest} ->
            recv_all(Socket, Rest);
        {ok, [Frame|_], _Rest} ->
            {ok, Frame};
        E -> E
    end.

recv_all(Socket, Buf) ->
    case gen_tcp:recv(Socket, 0) of
        {ok, Data} ->
            NewData = <<Buf/binary, Data/binary>>,
            parse_all(NewData);
        {error, Reason} -> {error, Reason}
    end.

parse_all(Data) ->
    parse_all(Data, []).

parse_all(Data, Frames) ->
    case vmq_parser:parse(Data) of
        more ->
            {ok, lists:reverse(Frames), Data};
        {error, _Rest} ->
            {error, parse_error};
        error ->
            {error, parse_error};
        {Frame, Rest} ->
            parse_all(Rest, [Frame|Frames])
    end.

connect(Port, ClientId, Opts) ->
    Connect = packet:gen_connect(ClientId, Opts),
    Connack = packet:gen_connack(0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, [{port, Port}]),
    Socket.

subscribe(Socket, Topic, QoS) ->
    Subscribe = packet:gen_subscribe(1, [Topic], QoS),
    Suback = packet:gen_suback(1, QoS),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback).

ensure_cluster(Config) ->
    vmq_cluster_test_utils:ensure_cluster(Config).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
hook_uname_password_success(_, _, _, _, _) -> {ok, [{max_inflight_messages, 1}]}.
hook_auth_on_publish(_, _, _, _, _, _) -> ok.
hook_auth_on_subscribe(_, _, _) -> ok.

random_node(Nodes) ->
    lists:nth(rand:uniform(length(Nodes)), Nodes).

opts(Nodes) ->
    {_, Port} = lists:nth(rand:uniform(length(Nodes)), Nodes),
    [{port, Port}].

check_unique_client(ClientId, Nodes) ->
    Res =
    lists:foldl(
             fun({Node, _Port}, Acc) ->
                     case rpc:call(Node, vmq_reg, get_session_pids, [ClientId]) of
                         {ok, [Pid]} ->
                             [{Node, Pid}|Acc];
                         {error, not_found} ->
                             Acc
                     end
             end, [], Nodes),
    L = length(Res),
    case L > 1 of
        true ->
            io:format(user, "multiple registered ~p~n", [Res]);
        false ->
            ok
    end,
    length(Res) =< 1.

receive_times(Msg, 0) -> Msg;
receive_times(Msg, N) ->
    receive
        Msg ->
            receive_times(Msg, N-1)
    end.

%% Get the nodename part of nodename@host. This is a hack due to the
%% fact that PRE erlang 20 ct_slave:start/2, the hostname would be
%% unconditionally appended to the nodename forming invalid names such
%% as nodename@host@host.
nodename(Node) when is_atom(Node) ->
    list_to_atom(lists:takewhile(fun(C) -> C =/= $@ end, atom_to_list(Node))).
