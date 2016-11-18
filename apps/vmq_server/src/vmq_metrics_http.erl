%% Copyright 2016 Erlio GmbH Basel Switzerland (http://erl.io)
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

-module(vmq_metrics_http).

-export([routes/0]).
-export([init/3,
         handle/2,
         terminate/3]).

routes() ->
    [{"/metrics", ?MODULE, []}].

init(_Type, Req, _Opts) ->
    {ok, Req, undefined}.

handle(Req, State) ->
    {ContentType, Req2} = cowboy_req:header(<<"content-type">>, Req,
                                            <<"text/plain">>),
    {ok, reply(Req2, ContentType), State}.

terminate(_Reason, _Req, _State) ->
    ok.

reply(Req, <<"text/plain">>) ->
    %% Prometheus output
    Metrics = vmq_metrics:metrics(),
    Output = prometheus_output(Metrics, []),
    {ok, Req2} = cowboy_req:reply(200, [{<<"content-type">>, <<"text/plain">>}],
                                  Output, Req),
    Req2.



prometheus_output([{counter, Metric, Val}|Metrics], Acc) ->
    BinMetric = atom_to_binary(Metric, utf8),
    BinVal = integer_to_binary(Val),
    Node = atom_to_binary(node(), utf8),
    TypeLine = [<<"# TYPE ">>, BinMetric, <<" counter\n">>],
    CounterLine = [BinMetric, <<"{node=\"", Node/binary, "\"} ", BinVal/binary, "\n">>],
    prometheus_output(Metrics, [TypeLine, CounterLine|Acc]);
prometheus_output([{gauge, Metric, Val}|Metrics], Acc) ->
    BinMetric = atom_to_binary(Metric, utf8),
    BinVal = integer_to_binary(Val),
    Node = atom_to_binary(node(), utf8),
    TypeLine = [<<"# TYPE ">>, BinMetric, <<" gauge\n">>],
    CounterLine = [BinMetric, <<"{node=\"", Node/binary, "\"} ", BinVal/binary, "\n">>],
    prometheus_output(Metrics, [TypeLine, CounterLine|Acc]);
prometheus_output([], Acc) -> Acc.





