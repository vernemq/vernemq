%% Copyright 2018 Erlio GmbH Basel Switzerland (http://erl.io)
%% Copyright 2018-2024 Octavo Labs/VerneMQ (https://vernemq.com/)
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

-module(vmq_metrics_http).
-behaviour(vmq_http_config).

-include("vmq_metrics.hrl").

-export([routes/0, is_authorized/2]).
-export([
    init/2,
    content_types_provided/2,
    reply_to_text/2
]).

routes() ->
    [{"/metrics", ?MODULE, []}].

is_authorized(Req, State) ->
    AuthMode = vmq_http_config:auth_mode(Req, vmq_metrics_http),
    case AuthMode of
        "apikey" -> vmq_auth_apikey:is_authorized(Req, State, "metrics");
        "noauth" -> {true, Req, State};
        _ -> {error, invalid_authentication_scheme}
    end.

init(Req, Opts) ->
    {cowboy_rest, Req, Opts}.

content_types_provided(Req, State) ->
    {
        [
            {{<<"text">>, <<"plain">>, '*'}, reply_to_text}
        ],
        Req,
        State
    }.

reply_to_text(Req, State) ->
    %% Prometheus output
    Metrics = vmq_metrics:metrics(#{aggregate => false}),
    Namespace = vmq_config:get_env(prometheus_namespace, ""),
    Output = prometheus_output(Metrics, {#{}, []}, Namespace),
    {Output, Req, State}.

prometheus_output(
    [
        {
            #metric_def{
                type = histogram = Type, name = Metric, description = Descr, labels = Labels
            },
            Val
        }
        | Metrics
    ],
    {EmittedAcc, OutAcc},
    Namespace
) ->
    BinMetric0 = atom_to_binary(Metric, utf8),
    BinMetric = erlang:iolist_to_binary([Namespace, BinMetric0]),
    Node = atom_to_binary(node(), utf8),
    {Count, Sum, Buckets} = Val,
    CountLine = line(<<BinMetric/binary, "_count">>, Node, Labels, integer_to_binary(Count)),
    SumLine = line(<<BinMetric/binary, "_sum">>, Node, Labels, integer_to_binary(Sum)),
    Lines =
        maps:fold(
            fun(Bucket, BucketVal, BAcc) ->
                [
                    line(
                        <<BinMetric/binary, "_bucket">>,
                        Node,
                        [
                            {<<"le">>,
                                case Bucket of
                                    infinity -> <<"+Inf">>;
                                    _ -> integer_to_binary(Bucket)
                                end}
                            | Labels
                        ],
                        integer_to_binary(BucketVal)
                    )
                    | BAcc
                ]
            end,
            [CountLine, SumLine],
            Buckets
        ),
    case EmittedAcc of
        #{Metric := _} ->
            prometheus_output(Metrics, {EmittedAcc, [Lines | OutAcc]}, Namespace);
        _ ->
            HelpLine = [<<"# HELP ">>, BinMetric, <<" ", Descr/binary, "\n">>],
            TypeLine = [<<"# TYPE ">>, BinMetric, type(Type)],
            prometheus_output(
                Metrics,
                {EmittedAcc#{Metric => true}, [[HelpLine, TypeLine, Lines] | OutAcc]},
                Namespace
            )
    end;
prometheus_output(
    [
        {#metric_def{type = Type, name = Metric, description = Descr, labels = Labels}, Val}
        | Metrics
    ],
    {EmittedAcc, OutAcc},
    Namespace
) ->
    BinMetric0 = erlang:atom_to_binary(Metric, utf8),
    BinMetric = erlang:iolist_to_binary([Namespace, BinMetric0]),
    BinVal = integer_to_binary(Val),
    Node = atom_to_binary(node(), utf8),
    Line = line(BinMetric, Node, Labels, BinVal),
    case EmittedAcc of
        #{Metric := _} ->
            prometheus_output(Metrics, {EmittedAcc, [Line | OutAcc]}, Namespace);
        _ ->
            HelpLine = [<<"# HELP ">>, BinMetric, <<" ", Descr/binary, "\n">>],
            TypeLine = [<<"# TYPE ">>, BinMetric, type(Type)],
            prometheus_output(
                Metrics,
                {EmittedAcc#{Metric => true}, [[HelpLine, TypeLine, Line] | OutAcc]},
                Namespace
            )
    end;
prometheus_output([], {_, OutAcc}, _) ->
    %% Make sure the metrics with HELP and TYPE annotations are
    %% emitted first.
    lists:reverse(OutAcc).

line(BinMetric, Node, Labels, BinVal) ->
    [
        BinMetric,
        <<"{">>,
        labels([{<<"node">>, Node} | Labels]),
        <<"} ">>,
        BinVal,
        <<"\n">>
    ].

labels(Labels) ->
    lists:join(
        $,,
        lists:map(
            fun({Key, Val}) ->
                label(Key, Val)
            end,
            Labels
        )
    ).

label(Key, Val) ->
    [ensure_bin(Key), <<"=\"">>, ensure_bin(Val), <<"\"">>].

ensure_bin(E) when is_atom(E) ->
    atom_to_binary(E, utf8);
ensure_bin(E) when is_list(E) ->
    list_to_binary(E);
ensure_bin(E) when is_binary(E) ->
    E.

type(gauge) ->
    <<" gauge\n">>;
type(counter) ->
    <<" counter\n">>;
type(histogram) ->
    <<" histogram\n">>.
