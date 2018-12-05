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
         simple_cli_test/1,
         pluggable_metrics_test/1]).

-export([hook_auth_on_subscribe/3]).
-export([plugin_metrics/0]).

init_per_suite(_Config) ->
    cover:start(),
    _Config.

end_per_suite(_Config) ->
    _Config.

init_per_testcase(_Case, Config) ->
    vmq_test_utils:setup(),
    enable_on_subscribe(),
    vmq_server_cmd:set_config(allow_anonymous, true),
    vmq_server_cmd:set_config(graphite_interval, 100),
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
     simple_cli_test,
     pluggable_metrics_test].

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
    vmq_server_cmd:set_config(graphite_include_labels, false),
    SubSocket = sample_subscribe(),
    {ok, LSocket} = gen_tcp:listen(2003, [binary, {packet, raw},
                                          {active, false},
                                          {reuseaddr, true}]),
    {ok, GraphiteSocket1} = gen_tcp:accept(LSocket), %% vmq_graphite connects
    Want = [<<"mqtt.subscribe.received 1">>],
    true = recv_data(GraphiteSocket1, Want),
    gen_tcp:close(GraphiteSocket1),

    vmq_server_cmd:set_config(graphite_include_labels, true),
    {ok, GraphiteSocket2} = gen_tcp:accept(LSocket), %% vmq_graphite connects
    WantLabels = [<<"mqtt.subscribe.received 1">>,
                  <<"mqtt.subscribe.received.mqtt_version_4 1">>],
    true = recv_data(GraphiteSocket2, WantLabels),

    gen_tcp:close(SubSocket),
    gen_tcp:close(GraphiteSocket2),
    gen_tcp:close(LSocket).

recv_data(Socket, Want0) ->
    case gen_tcp:recv(Socket, 0, 5000) of
        {ok, Data0} ->
            %% split in lines and remove trailing timestamps
            Data1 = lists:map(
                      fun(<<>>) -> <<>>;
                         (Line) ->
                              case re:split(Line, " ") of
                                  [Key, Val, _Timestamp] ->
                                      <<Key/binary, " ", Val/binary>>;
                                  _ ->
                                      %% probably received a partial line,
                                      %% ignore it and hope we get it next time
                                      <<>>
                              end
                      end,
                      re:split(Data0, "\n")),
            Want1 =
            lists:foldl(
              fun(Line, WantAcc) ->
                      case lists:member(Line, WantAcc) of
                          true -> WantAcc -- [Line];
                          false -> WantAcc
                      end
              end, Want0, Data1),
            case Want1 of
                [] -> true;
                _ ->
                    recv_data(Socket, Want1)
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
    Line = list_to_binary(
             "mqtt_subscribe_received{node=\"" ++ Node ++ "\",mqtt_version=\"4\"} 1"),
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

pluggable_metrics_test(_) ->
    application:set_env(vmq_server, vmq_metrics_mfa, {?MODULE, plugin_metrics, []}),
    {ok, Ret0} = vmq_server_cmd:metrics(),
    true =
    lists:foldl(fun({text, "counter.plugin_metrics = 123"}, _) -> true;
                   (_, Acc) -> Acc
                end, false, Ret0),
    application:unset_env(vmq_server, vmq_metrics_mfa),
    {ok, Ret1} = vmq_server_cmd:metrics(),
    true =
    lists:foldl(fun({text, "counter.plugin_metrics = 123"}, _) -> false;
                   (_, Acc) -> Acc
                end, true, Ret1).

plugin_metrics() ->
    [{counter, [], plugin_metrics, plugin_metrics, <<"Simple Plugin Metric">>, 123}].

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
