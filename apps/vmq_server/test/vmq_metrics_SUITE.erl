-module(vmq_metrics_SUITE).
-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0,
         groups/0
        ]).

-export([simple_systree_test/1,
         histogram_systree_test/1,
         simple_graphite_test/1,
         simple_prometheus_test/1,
         simple_cli_test/1]).

-export([hook_auth_on_subscribe/3]).
-export([plugin_metrics/0]).

init_per_suite(Config) ->
    cover:start(),
    [{ct_hooks, vmq_cth} | Config].

end_per_suite(_Config) ->
    _Config.

init_per_testcase(_Case, Config) ->
    case proplists:get_value(vmq_md, Config) of
        #{group := vmq_reg_redis_trie, tc := _} ->
            vmq_test_utils:setup(vmq_reg_redis_trie),
            eredis_cluster:flushdb();
        _ -> vmq_test_utils:setup(vmq_reg_trie)
    end,
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
    [
        {group, vmq_reg_redis_trie},
        {group, vmq_reg_trie}
    ].

groups() ->
    Tests =
    [simple_systree_test,
     histogram_systree_test,
     simple_graphite_test,
     simple_prometheus_test,
     simple_cli_test],
    [
        {vmq_reg_redis_trie, [], Tests},
        {vmq_reg_trie, [], Tests}
    ].

simple_systree_test(Cfg) ->
    Socket = sample_subscribe(Cfg),
    SysTopic = "$SYS/"++ atom_to_list(node()) ++ "/mqtt/subscribe/received",
    Publish = packet:gen_publish(SysTopic, 0, <<"1">>, []),
    ok = packet:expect_packet(Socket, "publish", Publish),
    gen_tcp:close(Socket).

histogram_systree_test(Cfg) ->
    histogram_systree_test(Cfg, "/count", 10),
    histogram_systree_test(Cfg, "/sum", 100),
    histogram_systree_test(Cfg, "/bucket/10", 4),
    histogram_systree_test(Cfg, "/bucket/100", 6),
    histogram_systree_test(Cfg, "/bucket/1000", 8),
    histogram_systree_test(Cfg, "/bucket/inf", 10).

simple_graphite_test(Cfg) ->
    vmq_server_cmd:set_config(graphite_enabled, true),
    vmq_server_cmd:set_config(graphite_host, "localhost"),
    vmq_server_cmd:set_config(graphite_interval, 1000),
    vmq_server_cmd:set_config(graphite_include_labels, false),
    % ensure we have a subscription that we can count later on
    SubSocket = sample_subscribe(Cfg),
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

simple_prometheus_test(Cfg) ->
    %% we have to setup the listener here, because vmq_test_utils is overriding
    %% the default set in vmq_server.app.src
    vmq_server_cmd:listener_start(8888, [{http, true},
                                         {config_mod, vmq_metrics_http},
                                         {config_fun, routes}]),
    application:ensure_all_started(inets),
    SubSocket = sample_subscribe(Cfg),
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

simple_cli_test(Cfg) ->
    SubSocket = sample_subscribe(Cfg),
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

sample_subscribe(Cfg) ->
    %% let the metrics system do some increments
    SubTopic = "$SYS/+/mqtt/subscribe/received",
    Connect = packet:gen_connect("metrics-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(53, SubTopic, 0),
    Suback = packet:gen_suback(53, 0),
    {ok, CT} = vmq_topic:validate_topic(subscribe, list_to_binary(SubTopic)),
    case proplists:get_value(vmq_md, Cfg) of
        #{group := vmq_reg_redis_trie, tc := _} ->
            vmq_reg_redis_trie:add_complex_topics([CT]);
        _ -> ok
    end,
    {ok, SubSocket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(SubSocket, Subscribe),
    ok = packet:expect_packet(SubSocket, "suback", Suback),
    SubSocket.

histogram_systree_test(Cfg, Suffix, Val) ->
    %% let the metrics system do some increments
    Connect = packet:gen_connect("hist-test", [{keepalive,60}]),
    Connack = packet:gen_connack(0),
    Topic =  "$SYS/+/plugin/histogram" ++ Suffix,
    Subscribe = packet:gen_subscribe(53, Topic, 0),
    Suback = packet:gen_suback(53, 0),
    {ok, CT} = vmq_topic:validate_topic(subscribe, list_to_binary(Topic)),
    case proplists:get_value(vmq_md, Cfg) of
        #{group := vmq_reg_redis_trie, tc := _} ->
            vmq_reg_redis_trie:add_complex_topics([CT]);
        _ -> ok
    end,
    {ok, SubSocket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(SubSocket, Subscribe),
    ok = packet:expect_packet(SubSocket, "suback", Suback),
    PublishTopic = "$SYS/" ++ atom_to_list(node()) ++ "/plugin/histogram" ++ Suffix,
    Publish = packet:gen_publish(PublishTopic, 0, integer_to_binary(Val), []),
    ok = packet:expect_packet(SubSocket, "publish", Publish),
    gen_tcp:send(SubSocket, packet:gen_disconnect()),
    gen_tcp:close(SubSocket).
