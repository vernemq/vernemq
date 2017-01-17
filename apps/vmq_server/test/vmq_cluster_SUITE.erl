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
         cluster_leave_test/1,
         cluster_leave_dead_node_test/1,
         shared_subs_random_policy_test/1,
         shared_subs_prefer_local_policy_test/1]).

-export([hook_uname_password_success/5,
         hook_auth_on_publish/6,
         hook_auth_on_subscribe/3]).

-include_lib("common_test/include/ct.hrl").
-include_lib("kernel/include/inet.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("vmq_commons/include/vmq_types.hrl").

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
                    {test3, 18885}]),
    {CoverNodes, _} = lists:unzip(Nodes),
    {ok, _} = ct_cover:add_nodes(CoverNodes),
    [{nodes, Nodes},{nodenames, [test1, test2, test3]}|Config].

end_per_testcase(_, _Config) ->
    vmq_cluster_test_utils:pmap(fun(Node) -> ct_slave:stop(Node) end,
                                [test1, test2, test3]),
    ok.

all() ->
    [multiple_connect_test
    ,multiple_connect_unclean_test
    ,distributed_subscribe_test
    ,racing_connect_test
    ,racing_subscriber_test
    ,cluster_leave_test
    ,cluster_leave_dead_node_test
    ,shared_subs_random_policy_test
    ,shared_subs_prefer_local_policy_test
    ].


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
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

multiple_connect_test(Config) ->
    ok = ensure_cluster(Config),
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    NrOfConnects = 250,
    NrOfProcesses = NrOfConnects div 50, %random:uniform(NrOfConnects),
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
    ok = wait_until_converged(Nodes,
                         fun(N) ->
                                 rpc:call(N, vmq_reg, total_subscriptions, [])
                         end, [{total, 1}]),
    [PublishNode|_] = Nodes,
    Payloads = publish_random([PublishNode], 20, Topic),
    ok = vmq_cluster_test_utils:wait_until(
           fun() ->
                   20 == rpc:call(RandomNode, vmq_reg, stored,
                                   [{"", <<"connect-unclean">>}])
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
    ok = wait_until_converged(Nodes,
                         fun(N) ->
                                 rpc:call(N, vmq_reg, total_subscriptions, [])
                         end, [{total, length(Sockets)}]),
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
                                                   rpc:call(N, vmq_reg, total_subscriptions, [])
                                           end, [{total, 1}]),
                 packet:gen_connack(true, 0);
             _ ->
                 packet:gen_connack(true, 0)
         end,
         spawn_link(
           fun() ->
                   {_RandomNode, RandomPort} = random_node(Nodes),
                   {ok, Socket} = packet:do_client_connect(Connect, Connack, [{port,
                                                                               RandomPort}]),
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
    {_, [{Node, Port}|RestNodes] = Nodes} = lists:keyfind(nodes, 1, Config),
    Topic = "cluster/leave/topic",
    %% create 8 sessions
    [PubSocket|_] = Sockets =
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
     end || I <- lists:seq(1,8)],
    ok = wait_until_converged(Nodes,
                         fun(N) ->
                                 rpc:call(N, vmq_reg, total_subscriptions, [])
                         end, [{total, length(Sockets)}]),
    %% publish a message for every session
    Publish = packet:gen_publish(Topic, 1, <<"test-message">>, [{mid, 1}]),
    Puback = packet:gen_puback(1),
    ok = gen_tcp:send(PubSocket, Publish),
    ok = packet:expect_packet(PubSocket, "puback", Puback),
    ok = vmq_cluster_test_utils:wait_until(
           fun() ->
                   {8, 0, 0, 0, 0} == rpc:call(Node, vmq_queue_sup_sup, summary, [])
           end, 60, 500),
    %% Pick a control node for initiating the cluster leave
    [{CtrlNode, _}|_] = RestNodes,
    {ok, _} = rpc:call(CtrlNode, vmq_server_cmd, node_leave, [Node]),
    %% Leaving Node will disconnect all sessions and give away all messages
    %% The disconnected sessions are equally migrated to the rest of the nodes
    %% As the clients don't reconnect (in this test), their sessions are offline
    ok = wait_until_converged(RestNodes,
                              fun(N) ->
                                      rpc:call(N, vmq_queue_sup_sup, summary, [])
                              end, {0, 0, 0, 4, 4}).

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
    ok = wait_until_converged(Nodes,
                         fun(N) ->
                                 rpc:call(N, vmq_reg, total_subscriptions, [])
                         end, [{total, 10}]),
    %% stop first node
    {ok, Node} = ct_slave:stop(Nodename),

    %% Pick a control node for initiating the cluster leave
    %% let first node leave the cluster, this should migrate the sessions,
    %% but not the messages
    [{CtrlNode, _}|_] = RestNodes,
    %% Node_leave might return before migration has finished
    {ok, _} = rpc:call(CtrlNode, vmq_server_cmd, node_leave, [Node]),
    %% The disconnected sessions are equally migrated to the rest of the nodes
    ok = wait_until_converged(RestNodes,
                              fun(N) ->
                                      rpc:call(N, vmq_queue_sup_sup, summary, [])
                              end, {0, 0, 0, 5, 0}).

shared_subs_prefer_local_policy_test(Config) ->
    ensure_cluster(Config),
    [LocalNode|OtherNodes] = Nodes = nodes_(Config),
    set_shared_subs_policy(prefer_local, nodenames(Config)),

    LocalSubscriberSockets = connect_shared_subscribers(<<"$share/share/sharedtopic">>, 5, [LocalNode]),
    RemoteSubscriberSockets = connect_shared_subscribers(<<"$share/share/sharedtopic">>, 5, OtherNodes),
    
    %% Make sure subscriptions have propagated to all nodes
    ok = wait_until_converged(Nodes,
                              fun(N) ->
                                      rpc:call(N, vmq_reg, total_subscriptions, [])
                              end, [{total, 10}]),

    %% publish to shared topic on local node
    {_, LocalPort} = LocalNode,
    Connect = packet:gen_connect("ss-publisher",
                                 [{keepalive, 60}, {clean_session, true}]),
    Connack = packet:gen_connack(0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, [{port, LocalPort}]),
    Payloads = publish_to_shared_topic(Socket, <<"sharedtopic">>, 10),
    Disconnect = packet:gen_disconnect(),
    ok = gen_tcp:send(Socket, Disconnect),
    ok = gen_tcp:close(Socket),

    %% receive on subscriber sockets.
    receive_shared_sub_messages(LocalSubscriberSockets, Payloads),
    receive_nothing(200),
    receive_shared_sub_messages(RemoteSubscriberSockets, []),
    receive_nothing(200),

    %% cleanup
    [ ok = gen_tcp:close(S) || S <- LocalSubscriberSockets ++ RemoteSubscriberSockets ],
    ok.

shared_subs_random_policy_test(Config) ->
    ensure_cluster(Config),
    Nodes = nodes_(Config),
    set_shared_subs_policy(random, nodenames(Config)),

    SubscriberSockets = connect_shared_subscribers(<<"$share/share/sharedtopic">>, 10, Nodes),

    %% Make sure subscriptions have propagated to all nodes
    ok = wait_until_converged(Nodes,
                              fun(N) ->
                                      rpc:call(N, vmq_reg, total_subscriptions, [])
                              end, [{total, 10}]),

    %% publish to shared topic on random node
    {_, Port} = random_node(Nodes),
    Connect = packet:gen_connect("ss-publisher",
                                 [{keepalive, 60}, {clean_session, true}]),
    Connack = packet:gen_connack(0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, [{port, Port}]),
    Payloads = publish_to_shared_topic(Socket, <<"sharedtopic">>, 10),
    Disconnect = packet:gen_disconnect(),
    ok = gen_tcp:send(Socket, Disconnect),
    ok = gen_tcp:close(Socket),

    %% receive on subscriber sockets.
    receive_shared_sub_messages(SubscriberSockets, Payloads),
    receive_nothing(200),
    
    %% cleanup
    [ ok = gen_tcp:close(S) || S <- SubscriberSockets ],
    ok.

connect_shared_subscribers(Topic, Number, Nodes) ->
    [begin
         
         {_, Port} = random_node(Nodes),
         Connect = packet:gen_connect("ss-subscriber-" ++ integer_to_list(I) ++ "-node-" ++ 
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

publish_to_shared_topic(Socket, Topic, Number) ->
    [begin
         Payload = vmq_test_utils:rand_bytes(5),
         Publish = packet:gen_publish(Topic, 1, Payload, [{mid, I}]),
         Puback = packet:gen_puback(I),
         ok = gen_tcp:send(Socket, Publish),
         ok = packet:expect_packet(Socket, "puback", Puback),
         Payload
     end || I <- lists:seq(1,Number)].

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
    
receive_shared_sub_messages(ReceiverSockets, Payloads) ->
    Master = self(),
    _RecProcs =
        lists:foreach(
          fun(S) ->
                  spawn_link(fun() -> recv_and_forward_msg(S, Master, <<>>) end)
          end,
          ReceiverSockets),
    receive_shared_sub_messages(Payloads).

receive_shared_sub_messages([]) ->
    ok;
receive_shared_sub_messages(Payloads) ->
    receive
        #mqtt_publish{payload=Payload} ->
            true = lists:member(Payload, Payloads),
            receive_shared_sub_messages(Payloads -- [Payload])
    end.

receive_nothing(Wait) ->
    receive
        X -> throw({received_unexpected_msgs, X})
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
            timer:sleep(rnd:uniform(100)),
            publish__(Self, Conf, NrOfMsgsPerProcess - 1);
        {error, closed} ->
            %% this happens if at the same time the same client id
            %% connects to the cluster
            timer:sleep(rnd:uniform(100)),
            publish__(Self, Conf, NrOfMsgsPerProcess)
    end.

publish_random(Nodes, N, Topic) ->
    publish_random(Nodes, N, Topic, []).

publish_random(_, 0, _, Acc) -> Acc;
publish_random(Nodes, N, Topic, Acc) ->
    Connect = packet:gen_connect("connect-unclean-pub", [{clean_session, true},
                                                         {keepalive, 10}]),
    Connack = packet:gen_connack(0),
    Payload = vmq_test_utils:rand_bytes(rnd:uniform(50)),
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



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
hook_uname_password_success(_, _, _, _, _) -> {ok, [{max_inflight_messages, 1}]}.
hook_auth_on_publish(_, _, _, _, _, _) -> ok.
hook_auth_on_subscribe(_, _, _) -> ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
random_node(Nodes) ->
    lists:nth(rnd:uniform(length(Nodes)), Nodes).


opts(Nodes) ->
    {_, Port} = lists:nth(rnd:uniform(length(Nodes)), Nodes),
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

