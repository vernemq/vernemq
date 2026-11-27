-module(vmq_node_restart_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("vmq_commons/include/vmq_types.hrl").

-define(MAX_ONLINE_MESSAGES, 10).
-define(INFLIGHT_WINDOW, 2).
-define(SESSION_EXPIRY_INTERVAL, 300).
-define(RECEIVE_TIMEOUT, 5000).
-define(TOPIC, <<"node-restart/online-queue">>).

init_per_suite(Config) ->
    S = vmq_test_utils:get_suite_rand_seed(),
    [S | vmq_cluster_test_utils:init_distribution(Config)].

end_per_suite(Config) ->
    Config.

init_per_testcase(Case, Config) ->
    vmq_test_utils:seed_rand(Config),
    {Name, Port} = vmq_cluster_test_utils:random_node_with_port(Case),
    {ok, Peer, Node} = start_broker(Name, Port, Config, Case),
    {ok, _} = ct_cover:add_nodes([Node]),
    ok = vmq_cluster_test_utils:refresh_plugin_hooks([{Peer, Node, Port}]),
    [{node, {Peer, Node, Port}} | Config].

end_per_testcase(_Case, Config) ->
    {Peer, Node, _Port} = proplists:get_value(node, Config),
    ok = vmq_cluster_test_utils:stop_peer(Peer, Node),
    case net_adm:ping(Node) of
        pong ->
            rpc:cast(Node, erlang, halt, []),
            ok = vmq_cluster_test_utils:wait_until_offline(Node);
        pang ->
            ok
    end.

all() ->
    [online_queue_survives_node_restart].

online_queue_survives_node_restart() ->
    [
        {doc,
            "Unacked and queued messages of a persistent MQTT v5 session survive "
            "a graceful single-node restart."}
    ].

online_queue_survives_node_restart(Config) ->
    {_Peer, Node, Port} = proplists:get_value(node, Config),
    Expected = [payload(I) || I <- lists:seq(0, ?MAX_ONLINE_MESSAGES - 1)],

    {SubSocket, 0, <<>>} = connect_persistent(Port, "client-a"),
    ok = subscribe(SubSocket, ?TOPIC),

    PubSocket = connect_clean(Port, "publisher"),
    ok = publish_all(PubSocket, Expected),
    ok = disconnect(PubSocket),
    {Unacked, _} = receive_publishes(SubSocket, ?INFLIGHT_WINDOW, <<>>),
    ?INFLIGHT_WINDOW = length(Unacked),

    ok = graceful_restart(Node, Port, Config),
    ok = gen_tcp:close(SubSocket),

    {SubSocket1, SessionPresent, Buf} = connect_persistent(Port, "client-a"),
    1 = SessionPresent,

    {Received, Rest} = receive_and_ack(SubSocket1, ?MAX_ONLINE_MESSAGES, Buf),

    PubSocket1 = connect_clean(Port, "publisher"),
    ok = publish_all(PubSocket1, [<<"sentinel">>]),
    ok = disconnect(PubSocket1),
    {[Next], _} = receive_and_ack(SubSocket1, 1, Rest),
    ok = disconnect(SubSocket1),

    <<"sentinel">> = Next,
    Expected = lists:sort(Received),
    ok.

start_broker(Name, Port, Config, Case) ->
    {ok, Peer, Node} = vmq_cluster_test_utils:start_node(Name, Config, Case),
    {ok, _} =
        rpc:call(Node, vmq_server_cmd, listener_start, [
            Port, [{allowed_protocol_versions, "3,4,5"}]
        ]),
    ok = rpc:call(Node, vmq_auth, register_hooks, []),
    ok = enable_mqtt5_auth_hooks(Node),
    ok = configure_broker(Node),
    ok = vmq_cluster_test_utils:wait_until_ready([Node]),
    {ok, Peer, Node}.

enable_mqtt5_auth_hooks(Node) ->
    lists:foreach(
        fun({Hook, Arity, CompatHook, CompatArity}) ->
            ok = rpc:call(Node, vmq_plugin_mgr, enable_module_plugin, [
                Hook,
                vmq_auth,
                Hook,
                Arity,
                [{compat, {CompatHook, vmq_plugin_compat_m5, convert, CompatArity}}]
            ])
        end,
        [
            {auth_on_subscribe, 3, auth_on_subscribe_m5, 4},
            {auth_on_publish, 6, auth_on_publish_m5, 7}
        ]
    ).

configure_broker(Node) ->
    lists:foreach(
        fun({Key, Val}) ->
            {ok, _} = rpc:call(Node, vmq_server_cmd, set_config, [Key, Val])
        end,
        [
            {allow_anonymous, true},
            {max_online_messages, ?MAX_ONLINE_MESSAGES},
            {max_offline_messages, -1},
            {receive_max_client, ?INFLIGHT_WINDOW}
        ]
    ).

graceful_restart(Node, Port, Config) ->
    ok = rpc:call(Node, init, stop, []),
    ok = vmq_cluster_test_utils:wait_until_offline(Node),
    [Name | _] = string:split(atom_to_list(Node), "@"),
    {ok, _Peer, Node} =
        start_broker(list_to_atom(Name), Port, Config, online_queue_survives_node_restart),
    ok.

connect_persistent(Port, ClientId) ->
    Connect = packetv5:gen_connect(ClientId, [
        {keepalive, 60},
        {clean_start, false},
        {properties, #{p_session_expiry_interval => ?SESSION_EXPIRY_INTERVAL}}
    ]),
    {ok, Socket, Connack, Rest} = packetv5:do_client_connect(Connect, [{port, Port}]),
    #mqtt5_connack{reason_code = ?M5_CONNACK_ACCEPT, session_present = SessionPresent} = Connack,
    {Socket, SessionPresent, Rest}.

connect_clean(Port, ClientId) ->
    Connect = packetv5:gen_connect(ClientId, [{keepalive, 60}, {clean_start, true}]),
    {ok, Socket, Connack, <<>>} = packetv5:do_client_connect(Connect, [{port, Port}]),
    #mqtt5_connack{reason_code = ?M5_CONNACK_ACCEPT} = Connack,
    Socket.

subscribe(Socket, Topic) ->
    Subscribe = packetv5:gen_subscribe(1, [packetv5:gen_subtopic(Topic, 1)], #{}),
    ok = gen_tcp:send(Socket, Subscribe),
    packetv5:expect_frame(Socket, packetv5:gen_suback(1, [1], #{})).

publish_all(Socket, Payloads) ->
    lists:foldl(
        fun(Payload, Mid) ->
            ok = gen_tcp:send(Socket, packetv5:gen_publish(?TOPIC, 1, Payload, [{mid, Mid}])),
            ok = packetv5:expect_frame(Socket, packetv5:gen_puback(Mid)),
            Mid + 1
        end,
        1,
        Payloads
    ),
    ok.

disconnect(Socket) ->
    ok = gen_tcp:send(Socket, packetv5:gen_disconnect()),
    gen_tcp:close(Socket).

receive_publishes(Socket, N, Buf) ->
    receive_publishes(Socket, N, Buf, fun(_) -> ok end, []).

receive_and_ack(Socket, N, Buf) ->
    Ack = fun(#mqtt5_publish{message_id = Mid}) ->
        ok = gen_tcp:send(Socket, packetv5:gen_puback(Mid))
    end,
    receive_publishes(Socket, N, Buf, Ack, []).

receive_publishes(_Socket, 0, Buf, _OnPublish, Acc) ->
    {lists:reverse(Acc), Buf};
receive_publishes(Socket, N, Buf, OnPublish, Acc) ->
    case packetv5:receive_frame(gen_tcp, Socket, ?RECEIVE_TIMEOUT, Buf) of
        {ok, #mqtt5_publish{payload = Payload} = Publish, Rest} ->
            ok = OnPublish(Publish),
            receive_publishes(Socket, N - 1, Rest, OnPublish, [Payload | Acc]);
        {ok, Frame, Rest} ->
            ct:log("ignoring unexpected frame ~p", [Frame]),
            receive_publishes(Socket, N, Rest, OnPublish, Acc);
        {error, Reason} ->
            Received = lists:reverse(Acc),
            ct:fail(
                "expected ~p more message(s), receive failed with ~p after ~p message(s): ~p",
                [N, Reason, length(Received), Received]
            )
    end.

payload(I) ->
    list_to_binary("queued-" ++ integer_to_list(I)).
