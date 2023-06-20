%% Copyright 2018 Erlio GmbH Basel Switzerland (http://erl.io)
%%
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

-module(vmq_status_http).
-behaviour(vmq_http_config).
-include("vmq_metrics.hrl").

-export([routes/0, is_authorized/2]).
-export([node_status/0]).
-export([
    init/2,
    allowed_methods/2,
    content_types_provided/2,
    reply/2,
    terminate/3
]).

routes() ->
    [
        {"/status.json", ?MODULE, []},
        {"/status", cowboy_static, {priv_file, vmq_server, "static/index.html"}},
        {"/status/[...]", cowboy_static, {priv_dir, vmq_server, "static"}}
    ].

init(Req, Opts) ->
    {cowboy_rest, Req, Opts}.

allowed_methods(Req, State) ->
    {[<<"GET">>], Req, State}.

content_types_provided(Req, State) ->
    {
        [
            {{<<"application">>, <<"json">>, '*'}, reply}
        ],
        Req,
        State
    }.

is_authorized(Req, State) ->
    AuthMode = vmq_http_config:auth_mode(Req, vmq_status_http),
    case AuthMode of
        "apikey" -> vmq_auth_apikey:is_authorized(Req, State, "status");
        "noauth" -> {true, Req, State};
        _ -> {error, invalid_authentication_scheme}
    end.

terminate(_Reason, _Req, _State) ->
    ok.

reply(Req, State) ->
    Output = cluster_status(),
    {Output, Req, State}.

cluster_status() ->
    Nodes0 = nodes(),
    {Result0, _BadNodes} = rpc:multicall(Nodes0, ?MODULE, node_status, []),
    Result1 = [{R, N} || {{ok, R}, N} <- lists:zip(Result0, Nodes0)],
    {Result2, Nodes1} = lists:unzip(Result1),
    {ok, MyStatus} = node_status(),
    Data = [
        {atom_to_binary(Node, utf8), NodeResult}
     || {Node, NodeResult} <- lists:zip([node() | Nodes1], [MyStatus | Result2])
    ],
    vmq_json:encode([Data]).

node_status() ->
    % Total Connections
    TotalActiveMqttConnections = lists:sum(
        tuple_to_list(vmq_ranch_sup:active_mqtt_connections())
    ),
    % Total Online Queues
    TotalQueues = vmq_queue_sup_sup:nr_of_queues(),
    TotalOfflineQueues = TotalQueues - TotalActiveMqttConnections,
    TotalPublishIn =
        counter_val(?MQTT4_PUBLISH_RECEIVED) +
            counter_val(?MQTT5_PUBLISH_RECEIVED),
    TotalPublishOut =
        counter_val(?MQTT4_PUBLISH_SENT) +
            counter_val(?MQTT5_PUBLISH_SENT),
    TotalQueueIn = counter_val(?METRIC_QUEUE_MESSAGE_IN),
    TotalQueueOut = counter_val(?METRIC_QUEUE_MESSAGE_OUT),
    TotalQueueDrop = counter_val(?METRIC_QUEUE_MESSAGE_DROP),
    TotalQueueUnhandled = counter_val(?METRIC_QUEUE_MESSAGE_UNHANDLED),
    TotalMatchesLocal = counter_val(?METRIC_ROUTER_MATCHES_LOCAL),
    TotalMatchesRemote = counter_val(?METRIC_ROUTER_MATCHES_REMOTE),
    RegView = vmq_config:get_env(default_reg_view, vmq_reg_trie),
    {NrOfSubs, _SMemory} = vmq_metrics:fetch_external_metric(RegView, stats, {0, 0}),
    {NrOfRetain, _RMemory} = vmq_retain_srv:stats(),
    {ok, [
        {<<"num_online">>, TotalActiveMqttConnections},
        {<<"num_offline">>, TotalOfflineQueues},
        {<<"msg_in">>, TotalPublishIn},
        {<<"msg_out">>, TotalPublishOut},
        {<<"queue_in">>, TotalQueueIn},
        {<<"queue_out">>, TotalQueueOut},
        {<<"queue_drop">>, TotalQueueDrop},
        {<<"queue_unhandled">>, TotalQueueUnhandled},
        {<<"num_subscriptions">>, NrOfSubs},
        {<<"num_retained">>, NrOfRetain},
        {<<"matches_local">>, TotalMatchesLocal},
        {<<"matches_remote">>, TotalMatchesRemote},
        {<<"mystatus">>, [
            [{atom_to_binary(Node, utf8), Status} || {Node, Status} <- vmq_cluster:status()]
        ]},
        {<<"listeners">>, listeners()},
        {<<"version">>, version()}
    ]}.

counter_val(C) ->
    try vmq_metrics:counter_val(C) of
        Value -> Value
    catch
        _:_ -> 0
    end.

listeners() ->
    lists:foldl(
        fun({Type, Ip, Port, Status, MP, MaxConns, _, _}, Acc) ->
            Ip1 =
                case Ip of
                    {local, FS} -> list_to_binary(FS);
                    Any -> list_to_binary(Any)
                end,
            [
                [
                    {type, Type},
                    {status, Status},
                    {ip, Ip1},
                    {port, list_to_integer(Port)},
                    {mountpoint, MP},
                    {max_conns, MaxConns}
                ]
                | Acc
            ]
        end,
        [],
        vmq_ranch_config:listeners()
    ).

version() ->
    case release_handler:which_releases(current) of
        [{"vernemq", Version, _, current} | _] ->
            list_to_binary(Version);
        [] ->
            [{"vernemq", Version, _, permanent} | _] = release_handler:which_releases(permanent),
            list_to_binary(Version)
    end.
