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
         distributed_subscribe_test/1]).

-export([hook_uname_password_success/5,
         hook_auth_on_publish/6,
         hook_auth_on_subscribe/3]).

-include_lib("common_test/include/ct.hrl").
-include_lib("kernel/include/inet.hrl").
-include_lib("vmq_commons/include/vmq_types.hrl").

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(_Config) ->
    %% this might help, might not...
    os:cmd(os:find_executable("epmd")++" -daemon"),
    case net_kernel:start([test_master, shortnames]) of
        {ok, _} -> ok;
        {error, _} -> ok
    end,
    cover:start(),
    _Config.

end_per_suite(_Config) ->
    _Config.

init_per_testcase(_Case, Config) ->
    {_MasterNode, Nodes} = start_cluster(5),
    CoverNodes = [begin
                      wait_til_ready(Node),
                      Node
                  end || {Node, _} <- Nodes],
    {ok, _} = ct_cover:add_nodes(CoverNodes),
    [{nodes, Nodes}|Config].

end_per_testcase(_, Config) ->
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    stop_cluster(Nodes),
    Config.

all() ->
    [multiple_connect_test
     ,multiple_connect_unclean_test
     , distributed_subscribe_test].


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
multiple_connect_test(Config) ->
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    NrOfConnects = 250,
    NrOfProcesses = NrOfConnects div 50, %random:uniform(NrOfConnects),
    NrOfMsgsPerProcess = NrOfConnects div NrOfProcesses,
    publish(Nodes, NrOfProcesses, NrOfMsgsPerProcess),
    done = receive_times(done, NrOfProcesses),
    true = check_unique_client("connect-multiple", Nodes),
    Config.

multiple_connect_unclean_test(Config) ->
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    Topic = "qos1/multiple/test",
    Connect = packet:gen_connect("connect-unclean", [{clean_session, false},
                                                      {keepalive, 10}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(123, Topic, 1),
    Suback = packet:gen_suback(123, 1),
    Disconnect = packet:gen_disconnect(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, opts(Nodes)),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = gen_tcp:send(Socket, Disconnect),
    timer:sleep(500),
    Subs = fun() -> rpc:multicall([N || {N, _} <-Nodes],
                                      vmq_reg, total_subscriptions, [])
               end,
    io:format(user, "!!!!!!!!!!!!!!!!!!! Subs before send ~p~n", [Subs()]),
    %% publish random content to the topic
    Strd = fun() -> rpc:multicall([N || {N, _} <-Nodes],
                                  vmq_msg_store, stored, [])
           end,
    io:format(user, "!!!!!!!!!!!!!!!!!!! stored msgs before send ~p~n", [Strd()]),
    Payloads = publish_random(Nodes, 1000, Topic),
    Ports = fun() -> rpc:multicall([N || {N, _} <-Nodes],
                                  erlang, system_info, [port_count])
           end,
    Procs = fun() -> rpc:multicall([N || {N, _} <-Nodes],
                                  erlang, system_info, [process_count])
           end,
    io:format(user, "!!!!!!!!!!!!!!!!!!! stored msgs after send ~p~n", [Strd()]),
    io:format(user, "!!!!!!!!!!!!!!!!!!! port_count ~p~n", [Ports()]),
    io:format(user, "!!!!!!!!!!!!!!!!!!! process_count ~p~n", [Procs()]),
    timer:sleep(2000),
    ok = receive_publishes(Nodes, Topic, Payloads),
    timer:sleep(2000),
    io:format(user, "!!!!!!!!!!!!!!!!!!! stored msgs after deliver~p~n", [Strd()]),
    io:format(user, "!!!!!!!!!!!!!!!!!!! port_count ~p~n", [Ports()]),
    io:format(user, "!!!!!!!!!!!!!!!!!!! process_count ~p~n", [Procs()]),
    Config.

distributed_subscribe_test(Config) ->
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    Topic = "qos1/distributed/test",
    Sockets =
    [begin
         Connect = packet:gen_connect("connect-" ++ integer_to_list(Port),
                                      [{clean_session, true},
                                       {keepalive, 10}]),
         Connack = packet:gen_connack(0),
         Subscribe = packet:gen_subscribe(123, Topic, 1),
         Suback = packet:gen_suback(123, 1),
         {ok, Socket} = packet:do_client_connect(Connect, Connack, [{port, Port}]),
         ok = gen_tcp:send(Socket, Subscribe),
         ok = packet:expect_packet(Socket, "suback", Suback),
         Socket
     end || {_, Port} <- Nodes],
    timer:sleep(100),
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

publish(Nodes, NrOfProcesses, NrOfMsgsPerProcess) ->
    publish(self(), Nodes, NrOfProcesses, NrOfMsgsPerProcess, []).

publish(_, _, 0, _, Pids) -> Pids;
publish(Self, [Node|Rest] = Nodes, NrOfProcesses, NrOfMsgsPerProcess, Pids) ->
    Pid = spawn_link(fun() -> publish_(Self, {Node, Nodes}, NrOfMsgsPerProcess) end),
    publish(Self, Rest ++ [Node], NrOfProcesses -1, NrOfMsgsPerProcess, [Pid|Pids]).

publish_(Self, Node, NrOfMsgsPerProcess) ->
    {A, B, C} =  now(),
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
    Payload = crypto:rand_bytes(random:uniform(10000)),
    Publish = packet:gen_publish(Topic, 1, Payload, [{mid, N}]),
    Puback = packet:gen_puback(N),
    Disconnect = packet:gen_disconnect(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, opts(Nodes)),
    ok = gen_tcp:send(Socket, Publish),
    ok = packet:expect_packet(Socket, "puback", Puback),
    ok = gen_tcp:send(Socket, Disconnect),
    publish_random(Nodes, N - 1, Topic, [Payload|Acc]).

receive_publishes(_, _, []) -> ok;
receive_publishes(Nodes, Topic, Payloads) ->
    Connect = packet:gen_connect("connect-unclean", [{clean_session, false},
                                                           {keepalive, 10}]),
    Connack = packet:gen_connack(0),
    Disconnect = packet:gen_disconnect(),
    Opts = opts(Nodes),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, Opts),
    case recv(Socket, <<>>) of
        {ok, #mqtt_publish{message_id=MsgId, payload=Payload}} ->
            ok = gen_tcp:send(Socket, packet:gen_puback(MsgId)),
            ok = gen_tcp:send(Socket, Disconnect),
            gen_tcp:close(Socket),
            io:format(user, "+", []),
            receive_publishes(Nodes, Topic, Payloads -- [Payload]);
        {error, _} ->
            receive_publishes(Nodes, Topic, Payloads)
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
hook_uname_password_success(_, _, _, _, _) -> ok.
hook_auth_on_publish(_, _, _, _, _, _) -> ok.
hook_auth_on_subscribe(_, _, _) -> ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_cluster(NrOfNodes) ->
    Self = self(),
    {MasterNode, Nodes} = State =
    lists:foldl(
      fun(I, {MasterNode, Acc}) ->
              Name = list_to_atom("test_"++integer_to_list(I)),
              Port = 18880 + I,
              {NewMasterNode, Node} =
              case MasterNode of
                  undefined ->
                      N = start_node(Name, Port, []),
                      {N, N};
                  _ ->
                      N = start_node(Name, Port, [MasterNode]),
                      {MasterNode, N}
              end,
              Self ! done,
              {NewMasterNode, [{Node, Port}|Acc]}
      end , {undefined, []}, lists:seq(1, NrOfNodes)),
    receive_times(done, NrOfNodes),
    wait_til_ready(MasterNode),
    Readies = fun() ->
                      RPCNodes = [N || {N, _} <- Nodes],
                      rpc:multicall(RPCNodes, vmq_cluster, recheck, []),
                      timer:sleep(500),
                      rpc:multicall(RPCNodes, vmq_cluster, is_ready, [])
              end,
    io:format(user, "cluster state ~p~n", [Readies()]),
    State.

start_node(Name, Port, DiscoveryNodes) ->
    CodePath = lists:filter(fun filelib:is_dir/1, code:get_path()),
    NodeConfig = [{monitor_master, true},
                  {startup_functions,
                   [{code, set_path, [CodePath]}]}],
    case ct_slave:start(Name, NodeConfig) of
        {ok, Node} ->
            ok = rpc:call(Node, vmq_test_utils, setup, []),
            case DiscoveryNodes of
                [] ->
                    ignore;
                _ ->
                    {ok, _} = rpc:call(Node, vmq_server_cmd, node_join, DiscoveryNodes)
            end,
            wait_til_ready(Node),
            {ok, _} = rpc:call(Node, vmq_server_cmd, set_config, [allow_anonymous, false]),
            {ok, _} = rpc:call(Node, vmq_server_cmd, listener_start, [Port, []]),

            ok = rpc:call(Node, vmq_plugin_mgr, enable_module_plugin,
                          [auth_on_register, ?MODULE, hook_uname_password_success, 5]),
            ok = rpc:call(Node, vmq_plugin_mgr, enable_module_plugin,
                          [auth_on_publish, ?MODULE, hook_auth_on_publish, 6]),
            ok = rpc:call(Node, vmq_plugin_mgr, enable_module_plugin,
                          [auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3]),
            Node;
        {error, already_started, Node} ->
            ct_slave:stop(Node),
            timer:sleep(1000),
            start_node(Name, Port, DiscoveryNodes)
    end.


stop_cluster(Nodes) ->
    error_logger:info_msg("stop cluster nodes ~p", [Nodes]),
    lists:foreach(
      fun({Node, _Port}) ->
              [NodeNameStr|_] = re:split(atom_to_list(Node), "@", [{return, list}]),
              ok = rpc:call(Node, vmq_test_utils, teardown, []),
              ShortNodeName = list_to_existing_atom(NodeNameStr),
              {ok, _} = ct_slave:stop(ShortNodeName)
      end, lists:reverse(Nodes)).

wait_til_ready(Node) ->
    wait_til_ready(Node, rpc:call(Node, vmq_cluster, is_ready, []), 100).
wait_til_ready(_, true, _) -> ok;
wait_til_ready(Node, false, I) when I > 0 ->
    timer:sleep(100),
    wait_til_ready(Node, rpc:call(Node, vmq_cluster, is_ready, []), I - 1);
wait_til_ready(N, _, _) ->
    exit({not_ready, N, rpc:call(N, erlang, whereis, [vmq_cluster])}).

opts(Nodes) ->
    {_, Port} = lists:nth(random:uniform(length(Nodes)), Nodes),
    [{port, Port}].

check_unique_client(ClientId, Nodes) ->
    Res =
    lists:foldl(
             fun({Node, _Port}, Acc) ->
                     case rpc:call(Node, vmq_reg, get_subscriber_pids, [ClientId]) of
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

