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
         histogram_systree_test/1,
         simple_graphite_test/1,
         simple_prometheus_test/1,
         simple_cli_test/1]).

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
    vmq_server_cmd:set_config(systree_interval, 100),
    vmq_server_cmd:set_config(retry_interval, 10),
    application:set_env(vmq_server, vmq_metrics_mfa, {?MODULE, plugin_metrics, []}),
    vmq_server_cmd:listener_start(1888, []),
    vmq_metrics:reset_counters(),
    Config.

end_per_testcase(_, Config) ->
    application:unset_env(vmq_server, vmq_metrics_mfa),
    {ok, [{text, Text}]} = vmq_server_cmd:metrics(),
    Lines = re:split(Text, "\n", [{return, list}]),
    false = lists:member("counter.plugin_metrics = 123", Lines),
    disable_on_subscribe(),
    vmq_test_utils:teardown(),
    Config.

all() ->
    [simple_systree_test,
     histogram_systree_test,
     simple_graphite_test,
     simple_prometheus_test,
     simple_cli_test].

simple_systree_test(_) ->
    Socket = sample_subscribe(),
    SysTopic = "$SYS/"++ atom_to_list(node()) ++ "/mqtt/subscribe/received",
    Publish = packet:gen_publish(SysTopic, 0, <<"1">>, []),
    ok = packet:expect_packet(gen_tcp, Socket, "publish", Publish, 15000),
    gen_tcp:close(Socket).

histogram_systree_test(_) ->
    histogram_systree_test("/count", 10),
    histogram_systree_test("/sum", 100),
    histogram_systree_test("/bucket/10", 4),
    histogram_systree_test("/bucket/100", 6),
    histogram_systree_test("/bucket/1000", 8),
    histogram_systree_test("/bucket/inf", 10).

simple_graphite_test(_) ->
    vmq_server_cmd:set_config(graphite_enabled, true),
    vmq_server_cmd:set_config(graphite_host, "localhost"),
    vmq_server_cmd:set_config(graphite_interval, 1000),
    vmq_server_cmd:set_config(graphite_include_labels, false),
    % ensure we have a subscription that we can count later on
    SubSocket = sample_subscribe(),
    {ok, LSocket} = gen_tcp:listen(2003, [binary, {packet, raw},
                                          {active, false},
                                          {reuseaddr, true}]),
    {ok, GraphiteSocket1} = gen_tcp:accept(LSocket), %% vmq_graphite connects
    Want = [<<"mqtt.subscribe.received 1">>
            ,<<"plugin.histogram.count 10">>
            ,<<"plugin.histogram.sum 100">>
            ,<<"plugin.histogram.bucket.10 4">>
            ,<<"plugin.histogram.bucket.100 6">>
            ,<<"plugin.histogram.bucket.1000 8">>
            ,<<"plugin.histogram.bucket.inf 10">>
           ],
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
              fun(WItem, WantAcc) ->
                      case lists:member(WItem, Data1) of
                          true -> WantAcc -- [WItem];
                          false -> WantAcc
                      end
              end, Want0, Want0),
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
    true = lists:member(
             list_to_binary(
               "mqtt_subscribe_received{node=\"" ++ Node ++ "\",mqtt_version=\"4\"} 1"), Lines),

    true = lists:member(
             list_to_binary(
               "plugin_histogram_count{node=\"" ++ Node ++ "\"} 10"), Lines),
    true = lists:member(
             list_to_binary(
               "plugin_histogram_sum{node=\"" ++ Node ++ "\"} 100"), Lines),
    true = lists:member(
             list_to_binary(
               "plugin_histogram_bucket{node=\"" ++ Node ++ "\",le=\"10\"} 4"), Lines),
    true = lists:member(
             list_to_binary(
               "plugin_histogram_bucket{node=\"" ++ Node ++ "\",le=\"100\"} 6"), Lines),
    true = lists:member(
             list_to_binary(
               "plugin_histogram_bucket{node=\"" ++ Node ++ "\",le=\"1000\"} 8"), Lines),
    true = lists:member(
             list_to_binary(
               "plugin_histogram_bucket{node=\"" ++ Node ++ "\",le=\"+Inf\"} 10"), Lines),

    gen_tcp:close(SubSocket).

simple_cli_test(_) ->
    SubSocket = sample_subscribe(),
    {ok, [{text, Text}]} = vmq_server_cmd:metrics(),
    Lines = re:split(Text, "\n", [{return, list}]),
    true = lists:member("counter.mqtt_subscribe_received = 1", Lines),
    true = lists:member("counter.plugin_metrics = 123", Lines),
    true = lists:member("histogram.plugin_histogram_count = 10", Lines),
    true = lists:member("histogram.plugin_histogram_sum = 100", Lines),
    true = lists:member("histogram.plugin_histogram_bucket_10 = 4", Lines),
    true = lists:member("histogram.plugin_histogram_bucket_100 = 6", Lines),
    true = lists:member("histogram.plugin_histogram_bucket_1000 = 8", Lines),
    true = lists:member("histogram.plugin_histogram_bucket_infinity = 10", Lines),
    gen_tcp:close(SubSocket).

plugin_metrics() ->
    [{counter, [], plugin_metrics, plugin_metrics, <<"Simple Plugin Metric">>, 123},
     {histogram, [], plugin_histogram, plugin_histogram, <<"Simple Plugin Histogram">>,
      {10, 100, #{10 => 4,  100 => 6, 1000 => 8, infinity => 10}}}].

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

histogram_systree_test(Suffix, Val) ->
    %% let the metrics system do some increments
    Connect = packet:gen_connect("hist-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Topic =  "$SYS/+/plugin/histogram" ++ Suffix,
    Subscribe = packet:gen_subscribe(53, Topic, 0),
    Suback = packet:gen_suback(53, 0),
    {ok, SubSocket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(SubSocket, Subscribe),
    ok = packet:expect_packet(SubSocket, "suback", Suback),
    PublishTopic = "$SYS/" ++ atom_to_list(node()) ++ "/plugin/histogram" ++ Suffix,
    Publish = packet:gen_publish(PublishTopic, 0, integer_to_binary(Val), []),
    ok = packet:expect_packet(SubSocket, "publish", Publish),
    gen_tcp:send(SubSocket, packet:gen_disconnect()),
    gen_tcp:close(SubSocket).
