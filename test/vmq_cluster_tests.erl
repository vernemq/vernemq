-module(vmq_cluster_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqtt_commons/include/emqtt_frame.hrl").

-define(SETUP(F), {setup, fun setup/0, fun teardown/1,
                   fun(State) -> {timeout, 600, F(State)} end}).

-export([hook_uname_password_success/5,
         hook_auth_on_publish/6,
         hook_auth_on_subscribe/3]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Tests Descriptions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
cluster_test_() ->
    [
     ?SETUP(fun multiple_connect/1),
     ?SETUP(fun multiple_connect_unclean/1),
     ?SETUP(fun distributed_subscribe/1)
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Setup Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
setup() ->
    {A, B, C} = now(),
    random:seed(A, B, C),
    net_kernel:start([test_master, shortnames]),
    {_MasterNode, Nodes} = start_cluster(5),
    [wait_til_ready(Node) || {Node, _} <- Nodes],
    Nodes.

teardown(Nodes) ->
    [vmq_plugin_mgr:disable_plugin(P) || P <- vmq_plugin:info(all)],
    ok = stop_cluster(Nodes).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
multiple_connect(Nodes) ->
    NrOfConnects = 250,
    NrOfProcesses = NrOfConnects div 50, %random:uniform(NrOfConnects),
    NrOfMsgsPerProcess = NrOfConnects div NrOfProcesses,
    publish(Nodes, NrOfProcesses, NrOfMsgsPerProcess),
    done = receive_times(done, NrOfProcesses),
    ?_assertEqual(true, check_unique_client("connect-multiple", Nodes)).

multiple_connect_unclean(Nodes) ->
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
    [{RpcNode, _}|_] = Nodes,
    io:format(user, "subs ~p~n", [rpc:call(RpcNode, mnesia, dirty_read,
                                           [vmq_subscriber, Topic])]),

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
    io:format(user, "subs ~p~n", [rpc:call(RpcNode, mnesia, dirty_read,
                                           [vmq_subscriber, Topic])]),
    timer:sleep(2000),
    ok = receive_publishes(Nodes, Topic, Payloads),
    timer:sleep(2000),
    io:format(user, "!!!!!!!!!!!!!!!!!!! stored msgs after deliver~p~n", [Strd()]),
    io:format(user, "!!!!!!!!!!!!!!!!!!! port_count ~p~n", [Ports()]),
    io:format(user, "!!!!!!!!!!!!!!!!!!! process_count ~p~n", [Procs()]),
    ?_assertEqual(true, true).

distributed_subscribe(Nodes) ->
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
    ?_assertEqual(true, true).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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
    {ok, Socket} = packet:do_client_connect(Connect, Connack, [{port, Port}]),
    check_unique_client("connect-multiple", Nodes),
    gen_tcp:close(Socket),
    timer:sleep(random:uniform(100)),
    publish__(Self, Conf, NrOfMsgsPerProcess - 1).

publish_random(Nodes, N, Topic) ->
    publish_random(Nodes, N, Topic, []).

publish_random(_, 0, _, Acc) -> Acc;
publish_random(Nodes, N, Topic, Acc) ->
    Connect = packet:gen_connect("connect-unclean-pub", [{clean_session, true},
                                                           {keepalive, 10}]),
    Connack = packet:gen_connack(0),
    Payload = crypto:rand_bytes(random:uniform(10000)),
    Publish = packet:gen_publish(Topic, 1, Payload, [{mid, 1}]),
    Puback = packet:gen_puback(1),
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
    {ok, Socket} = packet:do_client_connect(Connect, Connack, opts(Nodes)),
    case recv(Socket, emqtt_frame:initial_state()) of
        {ok, #mqtt_frame{variable= #mqtt_frame_publish{
                                      message_id=MsgId
                                     },
                         payload=Payload}} ->
            ok = gen_tcp:send(Socket, packet:gen_puback(MsgId)),
            ok = gen_tcp:send(Socket, Disconnect),
            io:format(user, "+", []),
            receive_publishes(Nodes, Topic, Payloads -- [Payload]);
        E ->
            io:format(user, "got other ~p~n", [E]),
            gen_tcp:send(Socket, Disconnect),
            gen_tcp:close(Socket),
            receive_publishes(Nodes, Topic, Payloads)
    end.

recv(Socket, ParserState) ->
    case gen_tcp:recv(Socket, 0) of
        {ok, Data} ->
            case emqtt_frame:parse(Data, ParserState) of
                {ok, F, _} -> {ok, F};
                {more, NewParserState} ->
                    recv(Socket, NewParserState);
                {error, Reason} -> {error, Reason}
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
    {ok, HostName} = inet:gethostname(),
    AHostName = list_to_atom(HostName),
    Self = self(),
    State =
    lists:foldl(
      fun(I, {Node, Acc}) ->
              Slave = list_to_atom("test_"++integer_to_list(I)),
              {ok, SlaveNode} = slave:start(AHostName, Slave),
              Master =
              case Node of
                  undefined -> SlaveNode;
                  _ -> Node
              end,
              Port = 18880 + I,
              case I of
                  1 ->
                      start_node(Master, Port, []);
                  _ ->
                      RandomTime = random:uniform(2000),
                      timer:sleep(RandomTime),
                      start_node(SlaveNode, Port, [Master])
              end,
              Self ! done,
              {Master, [{SlaveNode, Port}|Acc]}
      end , {undefined, []}, lists:seq(1, NrOfNodes)),
    receive_times(done, NrOfNodes),
    State.
start_node(Node, Port, DiscoveryNodes) ->
    rpc:call(Node, code, add_paths, [code:get_path()]),
    ok = rpc:call(Node, vmq_test_utils, setup, []),
    case DiscoveryNodes of
        [] ->
            ignore;
        _ ->
            {ok, _} = rpc:call(Node, vmq_server_cmd, node_stop, []),
            {ok, _} = rpc:call(Node, vmq_server_cmd, node_join, DiscoveryNodes),
            {ok, _} = rpc:call(Node, vmq_server_cmd, node_start, [])
    end,
    wait_til_ready(Node),
    {ok, _} = rpc:call(Node, vmq_server_cmd, set_config, [allow_anonymous, false]),
    {ok, _} = rpc:call(Node, vmq_server_cmd, listener_start, [Port, []]),

    ok = rpc:call(Node, vmq_plugin_mgr, enable_module_plugin,
                  [auth_on_register, ?MODULE, hook_uname_password_success, 5]),
    ok = rpc:call(Node, vmq_plugin_mgr, enable_module_plugin,
                  [auth_on_publish, ?MODULE, hook_auth_on_publish, 6]),
    ok = rpc:call(Node, vmq_plugin_mgr, enable_module_plugin,
                  [auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3]).



    %% rpc:call(Node, application, load, [vmq_server]),
    %% rpc:call(Node, application, set_env, [vmq_server, allow_anonymous, false]),
    %% rpc:call(Node, application, set_env, [vmq_server, listeners,
    %%                                       [{mqtt, [?LISTENER(Port)]}]]),
    %% rpc:call(Node, application, set_env, [vmq_server, msg_store,
    %%                                       {vmq_null_store, []}]),
    %% rpc:call(Node, vmq_server, start_no_auth, DiscoveryNodes),


stop_cluster(Nodes) ->
    lists:foreach(
      fun({Node, _Port}) ->
              ok = rpc:call(Node, vmq_test_utils, teardown, []),
              ok = slave:stop(Node)
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

