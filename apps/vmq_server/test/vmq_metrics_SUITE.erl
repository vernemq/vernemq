-module(vmq_metrics_SUITE).
-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([simple_systree_test/1,
         simple_graphite_test/1,
         simple_prometheus_test/1,
         simple_cli_test/1]).

-export([hook_auth_on_subscribe/3]).

init_per_suite(_Config) ->
    cover:start(),
    _Config.

end_per_suite(_Config) ->
    _Config.

init_per_testcase(_Case, Config) ->
    vmq_test_utils:setup(),
    enable_on_subscribe(),
    vmq_server_cmd:set_config(allow_anonymous, true),
    vmq_server_cmd:set_config(retry_interval, 10),
    vmq_server_cmd:listener_start(1888, []),
    vmq_metrics:reset_counters(),
    Config.

end_per_testcase(_, Config) ->
    disable_on_subscribe(),
    vmq_test_utils:teardown(),
    Config.

all() ->
    [simple_systree_test,
     simple_graphite_test,
     simple_prometheus_test,
     simple_cli_test].

simple_systree_test(_) ->
    Socket = sample_subscribe(),
    SysTopic = "$SYS/"++ atom_to_list(node()) ++ "/mqtt/subscribe/received",
    Publish = packet:gen_publish(SysTopic, 0, <<"1">>, []),
    ok = packet:expect_packet(Socket, "publish", Publish),
    gen_tcp:close(Socket).

simple_graphite_test(_) ->
    vmq_server_cmd:set_config(graphite_enabled, true),
    vmq_server_cmd:set_config(graphite_host, "localhost"),
    vmq_server_cmd:set_config(graphite_interval, 1000),
    SubSocket = sample_subscribe(),
    {ok, LSocket} = gen_tcp:listen(2003, [binary, {packet, raw},
                                          {active, false},
                                          {reuseaddr, true}]),
    {ok, GraphiteSocket} = gen_tcp:accept(LSocket), %% vmq_graphite connects
    true = recv_data(GraphiteSocket),
    gen_tcp:close(SubSocket),
    gen_tcp:close(GraphiteSocket),
    gen_tcp:close(LSocket).

recv_data(Socket) ->
    case gen_tcp:recv(Socket, 0) of
        {ok, Data} ->
            Ret =
            lists:foldl(fun(<<"mqtt.subscribe.received 1", _/binary>>, _) ->
                                true;
                           (_, Acc) ->
                                Acc
                        end, false, re:split(Data, "\n")),
            case Ret of
                true -> true;
                false ->
                    recv_data(Socket)
            end
    end.

simple_prometheus_test(_) ->
    %% we have to setup the listener here, because vmq_test_utils is overriding
    %% the default set in vmq_server.app.src
    vmq_server_cmd:listener_start(8888, [{http, true},
                                         {config_mod, vmq_metrics_http},
                                         {config_fun, routes}]),
    application:ensure_all_started(inets),
    SubSocket = sample_subscribe(),
    {ok, {_Status, _Headers, Body}} = httpc:request("http://localhost:8888/metrics"),
    Lines = re:split(Body, "\n"),
    Node = atom_to_list(node()),
    Line = list_to_binary("mqtt_subscribe_received{node=\"" ++ Node ++ "\"} 1"),
    true = lists:foldl(fun(L, _) when L == Line ->
                               true;
                          (_, Acc) -> Acc
                       end, false, Lines),
    gen_tcp:close(SubSocket).

simple_cli_test(_) ->
    SubSocket = sample_subscribe(),
    {ok, Ret} = vmq_server_cmd:metrics(),
    true =
    lists:foldl(fun({text, "counter.mqtt_subscribe_received = 1"}, _) -> true;
                   (_, Acc) -> Acc
                end, false, Ret),
    gen_tcp:close(SubSocket).


enable_on_subscribe() ->
    vmq_plugin_mgr:enable_module_plugin(
      auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3).
disable_on_subscribe() ->
    vmq_plugin_mgr:disable_module_plugin(
      auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3).

hook_auth_on_subscribe(_, _, _) -> ok.

sample_subscribe() ->
    %% let the metrics system do some increments
    Connect = packet:gen_connect("metrics-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(53, "$SYS/+/mqtt/subscribe/received", 0),
    Suback = packet:gen_suback(53, 0),
    {ok, SubSocket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(SubSocket, Subscribe),
    ok = packet:expect_packet(SubSocket, "suback", Suback),
    SubSocket.
