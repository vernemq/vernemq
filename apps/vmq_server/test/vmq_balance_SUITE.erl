%% Copyright 2024 Octavo Labs/VerneMQ (https://vernemq.com/)
%% and Individual Contributors.
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(vmq_balance_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

%% ===================================================================
%% common_test callbacks
%% ===================================================================

init_per_suite(Config) ->
    cover:start(),
    Config.

end_per_suite(_Config) ->
    _Config.

init_per_testcase(_Case, Config) ->
    vmq_test_utils:setup(),
    vmq_server_cmd:set_config(allow_anonymous, true),
    Config.

end_per_testcase(_, Config) ->
    vmq_test_utils:teardown(),
    Config.

all() ->
    [
     %% vmq_balance_srv tests
     balance_srv_starts_test,
     balance_srv_disabled_always_accepts_test,
     balance_srv_enabled_accepts_below_threshold_test,
     balance_srv_stats_returns_tuple_test,

     %% HTTP endpoint tests
     balance_http_returns_200_when_disabled_test,
     balance_http_returns_200_when_accepting_test,
     balance_http_json_format_test,

     %% Metrics tests
     balance_metrics_exposed_in_prometheus_test,
     balance_metrics_default_values_test,

     %% Integration: single-node balance behavior
     balance_single_node_always_accepts_test,
     balance_enabled_single_node_always_accepts_test
    ].

%% ===================================================================
%% vmq_balance_srv tests
%% ===================================================================

balance_srv_starts_test(_Config) ->
    %% vmq_balance_srv should be running as part of the supervision tree
    Pid = erlang:whereis(vmq_balance_srv),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)).

balance_srv_disabled_always_accepts_test(_Config) ->
    %% By default, balance is disabled — should always accept
    ?assert(vmq_balance_srv:is_accepting()).

balance_srv_enabled_accepts_below_threshold_test(_Config) ->
    %% Enable balance and verify that with 0 connections (below min_connections),
    %% the node still accepts
    application:set_env(vmq_server, balance_enabled, true),
    application:set_env(vmq_server, balance_min_connections, 100),
    %% Restart the server to pick up new config
    restart_balance_srv(),
    %% Wait for at least one check cycle
    timer:sleep(200),
    %% Single node with 0 connections should always accept
    ?assert(vmq_balance_srv:is_accepting()).

balance_srv_stats_returns_tuple_test(_Config) ->
    {IsAccepting, LocalConns, ClusterAvg, IsEnabled} = vmq_balance_srv:balance_stats(),
    ?assert(is_integer(IsAccepting)),
    ?assert(IsAccepting =:= 0 orelse IsAccepting =:= 1),
    ?assert(is_integer(LocalConns)),
    ?assert(LocalConns >= 0),
    ?assert(is_integer(ClusterAvg)),
    ?assert(is_integer(IsEnabled)),
    ?assert(IsEnabled =:= 0 orelse IsEnabled =:= 1).

%% ===================================================================
%% HTTP endpoint tests
%% ===================================================================

balance_http_returns_200_when_disabled_test(_Config) ->
    %% Balance is disabled by default — endpoint should return 200
    Port = start_http_listener(),
    application:ensure_all_started(inets),
    Url = "http://localhost:" ++ integer_to_list(Port) ++ "/api/balance-health",
    {ok, {{_, 200, _}, _Headers, _Body}} = httpc:request(Url),
    stop_http_listener(Port).

balance_http_returns_200_when_accepting_test(_Config) ->
    %% Enable balance, but single node should still accept
    application:set_env(vmq_server, balance_enabled, true),
    restart_balance_srv(),
    timer:sleep(200),
    Port = start_http_listener(),
    application:ensure_all_started(inets),
    Url = "http://localhost:" ++ integer_to_list(Port) ++ "/api/balance-health",
    {ok, {{_, 200, _}, _Headers, _Body}} = httpc:request(Url),
    stop_http_listener(Port).

balance_http_json_format_test(_Config) ->
    Port = start_http_listener(),
    application:ensure_all_started(inets),
    Url = "http://localhost:" ++ integer_to_list(Port) ++ "/api/balance-health",
    {ok, {_Status, _Headers, Body}} = httpc:request(Url),
    Json = vmq_json:decode(list_to_binary(Body), [return_maps, {labels, binary}]),
    %% Verify JSON structure
    ?assert(maps:is_key(<<"status">>, Json)),
    ?assert(maps:is_key(<<"connections">>, Json)),
    ?assert(maps:is_key(<<"cluster_avg">>, Json)),
    %% When disabled/accepting, status should be "accepting"
    ?assertEqual(<<"accepting">>, maps:get(<<"status">>, Json)),
    %% Connections should be an integer
    Connections = maps:get(<<"connections">>, Json),
    ?assert(is_integer(Connections)),
    stop_http_listener(Port).

%% ===================================================================
%% Metrics tests
%% ===================================================================

balance_metrics_exposed_in_prometheus_test(_Config) ->
    Port = start_metrics_listener(),
    application:ensure_all_started(inets),
    Url = "http://localhost:" ++ integer_to_list(Port) ++ "/metrics",
    {ok, {_Status, _Headers, Body}} = httpc:request(Url),
    Lines = re:split(Body, "\n"),
    Node = atom_to_list(node()),
    %% Check that balance metrics are present in prometheus output
    ?assert(has_metric_line(Lines, "balance_is_accepting", Node)),
    ?assert(has_metric_line(Lines, "balance_local_connections", Node)),
    ?assert(has_metric_line(Lines, "balance_cluster_avg", Node)),
    ?assert(has_metric_line(Lines, "balance_is_enabled", Node)),
    stop_http_listener(Port).

balance_metrics_default_values_test(_Config) ->
    %% When disabled, balance_is_accepting should be 1, balance_is_enabled should be 0
    {IsAccepting, _LocalConns, _ClusterAvg, IsEnabled} = vmq_balance_srv:balance_stats(),
    ?assertEqual(1, IsAccepting),
    ?assertEqual(0, IsEnabled).

%% ===================================================================
%% Integration tests
%% ===================================================================

balance_single_node_always_accepts_test(_Config) ->
    %% A single-node cluster should always accept regardless of connection count,
    %% because there's nowhere else to send traffic
    ?assert(vmq_balance_srv:is_accepting()),
    %% Even after a check cycle
    timer:sleep(100),
    ?assert(vmq_balance_srv:is_accepting()).

balance_enabled_single_node_always_accepts_test(_Config) ->
    %% Enable balance, set a very low threshold
    application:set_env(vmq_server, balance_enabled, true),
    application:set_env(vmq_server, balance_threshold, "1.0"),
    application:set_env(vmq_server, balance_min_connections, 0),
    restart_balance_srv(),
    timer:sleep(200),
    %% Even with balance enabled and threshold=1.0, single node should accept
    %% because you can't redirect traffic when there's only one node
    ?assert(vmq_balance_srv:is_accepting()),
    %% Start an MQTT listener and make some connections
    vmq_server_cmd:listener_start(1888, []),
    Connect = packet:gen_connect("balance-test-client", [{keepalive, 60}]),
    Connack = packet:gen_connack(0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    %% Wait for a balance check cycle
    timer:sleep(200),
    %% Still accepting — single node
    ?assert(vmq_balance_srv:is_accepting()),
    gen_tcp:close(Socket),
    vmq_server_cmd:listener_stop(1888, "127.0.0.1", false).

%% ===================================================================
%% Cluster tests
%% ===================================================================

%% Note: Multi-node cluster tests for balance behavior belong in
%% vmq_balance_cluster_SUITE.erl which uses the cluster test utilities
%% to start multiple peer nodes. See vmq_cluster_SUITE.erl for the pattern.

%% ===================================================================
%% Helpers
%% ===================================================================

start_http_listener() ->
    Port = vmq_test_utils:get_free_port(),
    vmq_server_cmd:listener_start(Port, [{http, true},
                                         {config_mod, vmq_balance_http},
                                         {config_fun, routes}]),
    Port.

start_metrics_listener() ->
    Port = vmq_test_utils:get_free_port(),
    application:set_env(vmq_server, http_modules_auth, #{vmq_metrics_http => "noauth"}),
    vmq_server_cmd:listener_start(Port, [{http, true},
                                         {config_mod, vmq_metrics_http},
                                         {config_fun, routes}]),
    Port.

stop_http_listener(Port) ->
    vmq_server_cmd:listener_stop(Port, "127.0.0.1", false).

restart_balance_srv() ->
    case erlang:whereis(vmq_balance_srv) of
        undefined -> ok;
        Pid ->
            supervisor:terminate_child(vmq_server_sup, vmq_balance_srv),
            supervisor:restart_child(vmq_server_sup, vmq_balance_srv),
            %% Wait for the new process to be up
            wait_for_balance_srv(Pid, 50)
    end.

wait_for_balance_srv(_OldPid, 0) ->
    error(balance_srv_restart_timeout);
wait_for_balance_srv(OldPid, Retries) ->
    case erlang:whereis(vmq_balance_srv) of
        undefined ->
            timer:sleep(50),
            wait_for_balance_srv(OldPid, Retries - 1);
        OldPid ->
            timer:sleep(50),
            wait_for_balance_srv(OldPid, Retries - 1);
        _NewPid ->
            ok
    end.

has_metric_line(Lines, MetricName, Node) ->
    Prefix = list_to_binary(MetricName ++ "{node=\"" ++ Node ++ "\""),
    lists:any(fun(Line) ->
        case binary:match(Line, Prefix) of
            {0, _} -> true;
            _ -> false
        end
    end, Lines).
