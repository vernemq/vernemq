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
         cluster_leave_test/1,
         cluster_leave_dead_node_test/1]).

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
init_per_suite(_Config) ->
    %lager:start(),
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
                    {test3, 18885}]),
    {CoverNodes, _} = lists:unzip(Nodes),
    {ok, _} = ct_cover:add_nodes(CoverNodes),
    [{nodes, Nodes}|Config].

end_per_testcase(_, _Config) ->
    vmq_cluster_test_utils:pmap(fun(Node) -> ct_slave:stop(Node) end,
                                [test1, test2, test3]),
    ok.

all() ->
    [multiple_connect_test
     ,multiple_connect_unclean_test
     , distributed_subscribe_test
     , cluster_leave_test
     , cluster_leave_dead_node_test].


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
    Payloads = publish_random(Nodes, 20, Topic),
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
                   {8, 0, 0, 0, 0} == rpc:call(Node, vmq_queue_sup, summary, [])
           end, 60, 500),
    %% Pick a control node for initiating the cluster leave
    [{CtrlNode, _}|_] = RestNodes,
    {ok, _} = rpc:call(CtrlNode, vmq_server_cmd, node_leave, [Node]),
    %% Leaving Node will disconnect all sessions and give away all messages
    %% The disconnected sessions are equally migrated to the rest of the nodes
    %% As the clients don't reconnect (in this test), their sessions are offline
    ok = wait_until_converged(RestNodes,
                              fun(N) ->
                                      rpc:call(N, vmq_queue_sup, summary, [])
                              end, {0, 0, 0, 4, 4}).

cluster_leave_dead_node_test(Config) ->
    ok = ensure_cluster(Config),
    {_, [{Node, Port}|RestNodes] = Nodes} = lists:keyfind(nodes, 1, Config),
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
    ct_slave:stop(Node),

    %% Pick a control node for initiating the cluster leave
    %% let first node leave the cluster, this should migrate the sessions,
    %% but not the messages
    [{CtrlNode, _}|_] = RestNodes,
    %% Node_leave might return before migration has finished
    {ok, _} = rpc:call(CtrlNode, vmq_server_cmd, node_leave, [Node]),
    %% The disconnected sessions are equally migrated to the rest of the nodes
    ok = wait_until_converged(RestNodes,
                              fun(N) ->
                                      rpc:call(N, vmq_queue_sup, summary, [])
                              end, {0, 0, 0, 5, 0}).

publish(Nodes, NrOfProcesses, NrOfMsgsPerProcess) ->
    publish(self(), Nodes, NrOfProcesses, NrOfMsgsPerProcess, []).

publish(_, _, 0, _, Pids) -> Pids;
publish(Self, [Node|Rest] = Nodes, NrOfProcesses, NrOfMsgsPerProcess, Pids) ->
    Pid = spawn_link(fun() -> publish_(Self, {Node, Nodes}, NrOfMsgsPerProcess) end),
    publish(Self, Rest ++ [Node], NrOfProcesses -1, NrOfMsgsPerProcess, [Pid|Pids]).

publish_(Self, Node, NrOfMsgsPerProcess) ->
    {A, B, C} =  os:timestamp(),
    random:seed(A, B, C),
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
            timer:sleep(random:uniform(100)),
            publish__(Self, Conf, NrOfMsgsPerProcess - 1);
        {error, closed} ->
            %% this happens if at the same time the same client id
            %% connects to the cluster
            timer:sleep(random:uniform(100)),
            publish__(Self, Conf, NrOfMsgsPerProcess)
    end.

publish_random(Nodes, N, Topic) ->
    publish_random(Nodes, N, Topic, []).

publish_random(_, 0, _, Acc) -> Acc;
publish_random(Nodes, N, Topic, Acc) ->
    Connect = packet:gen_connect("connect-unclean-pub", [{clean_session, true},
                                                         {keepalive, 10}]),
    Connack = packet:gen_connack(0),
    Payload = crypto:rand_bytes(random:uniform(50)),
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
    case gen_tcp:recv(Socket, 0) of
        {ok, Data} ->
            NewData = <<Buf/binary, Data/binary>>,
            case vmq_parser:parse(NewData) of
                more ->
                    recv(Socket, NewData);
                {error, _Rest} ->
                    {error, parse_error};
                error ->
                    {error, parse_error};
                {Frame, _Rest} ->
                    {ok, Frame}
            end;
        {error, Reason} -> {error, Reason}
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
    lists:nth(random:uniform(length(Nodes)), Nodes).


opts(Nodes) ->
    {_, Port} = lists:nth(random:uniform(length(Nodes)), Nodes),
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

