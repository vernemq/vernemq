-module(vmq_netsplit_publish_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqtt_commons/include/emqtt_frame.hrl").
-define(NET_TICK_TIME, 10).

run_test_() ->
    NetTickTime = ?NET_TICK_TIME,
    vmq_netsplit_utils:test(NetTickTime, NetTickTime * 10,
                            fun(Nodes) ->
                                    {timeout, NetTickTime * 5,
                                     [?_test(publish_qos0(Nodes))]}
                            end).

publish_qos0(Nodes) ->
    vmq_netsplit_utils:reset_tables(Nodes),
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
