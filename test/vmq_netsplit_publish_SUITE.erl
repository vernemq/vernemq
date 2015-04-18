-module(vmq_netsplit_publish_SUITE).
-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([publish_qos0_test/1]).

-define(NET_TICK_TIME, 5).

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(_Config) ->
    cover:start(),
    _Config.

end_per_suite(_Config) ->
    _Config.

init_per_testcase(_Case, Config) ->
    Nodes = vmq_netsplit_utils:setup(?NET_TICK_TIME),
    [{nodes, Nodes}|Config].

end_per_testcase(_, Config) ->
    vmq_netsplit_utils:teardown(proplists:get_value(nodes, Config, [])),
    Config.

all() ->
    [publish_qos0_test].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
publish_qos0_test(Config) ->
    Nodes = proplists:get_value(nodes, Config, []),
    ok = vmq_netsplit_utils:check_connected(Nodes),
    Connect = packet:gen_connect("test-netsplit-client", [{clean_session, false},
                                                          {keepalive, 60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(53, "netsplit/0/test", 0),
    Suback = packet:gen_suback(53, 0),
    Port = vmq_netsplit_utils:get_port(Nodes),
    {ok, Socket} = packet:do_client_connect(Connect, Connack,
                                            [{port, Port}]),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),

    % ensures the subscription is replicated
    timer:sleep(100),
    %% Create Partitions
    {Island1, Island2} = vmq_netsplit_utils:partition_network(Nodes),

    %%================================%%
    %%     Window of Uncertanity      %%
    %%                                %%
    %% Erlang thinks everything is ok %%
    %%          ..outch..             %%
    %%================================%%

    %% in order the test sender can connect and register itself
    %% we enable allow_multiple_session
    ok = vmq_netsplit_utils:configure_trade_consistency(Nodes),
    Publish = packet:gen_publish("netsplit/0/test", 0, <<"message">>,
                                 [{mid, 1}]),
    Island2Port = vmq_netsplit_utils:get_port(Island2),
    helper_pub_qos1("test-netsplit-sender", Publish, Island2Port),

    %% fix the network
    vmq_netsplit_utils:fix_network(Island1, Island2),

    %% the publish is expected once the netsplit is fixed
    ok = packet:expect_packet(Socket, "publish", Publish).


helper_pub_qos1(ClientId, Publish, Port) ->
    Connect = packet:gen_connect(ClientId, [{keepalive, 60}]),
    Connack = packet:gen_connack(0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, [{port, Port}]),
    ok = gen_tcp:send(Socket, Publish),
    gen_tcp:close(Socket).

