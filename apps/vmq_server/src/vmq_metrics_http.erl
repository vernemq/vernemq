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

-module(vmq_metrics_http).
-behaviour(vmq_http_config).

-include("vmq_metrics.hrl").

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
    Metrics = vmq_metrics:metrics(#{aggregate => false}),
    Output = prometheus_output(Metrics, {#{}, []}),
    {ok, Req2} = cowboy_req:reply(200, [{<<"content-type">>, <<"text/plain">>}],
                                  Output, Req),
    Req2.

prometheus_output([{#metric_def{type=Type, name=Metric, description=Descr, labels=Labels}, Val}|Metrics],
                  {EmittedAcc, OutAcc}) ->
    BinMetric = atom_to_binary(Metric, utf8),
    BinVal = integer_to_binary(Val),
    Node = atom_to_binary(node(), utf8),
    Line = line(BinMetric, Node, Labels, BinVal),
    case EmittedAcc of
        #{Metric := _ } ->
            prometheus_output(Metrics, {EmittedAcc, [Line|OutAcc]});
        _ ->
            HelpLine = [<<"# HELP ">>, BinMetric, <<" ", Descr/binary, "\n">>],
            TypeLine = [<<"# TYPE ">>, BinMetric, type(Type)],
            prometheus_output(Metrics, {EmittedAcc#{Metric => true}, [[HelpLine, TypeLine, Line]|OutAcc]})
    end;
prometheus_output([], {_, OutAcc}) ->
    %% Make sure the metrics with HELP and TYPE annotations are
    %% emitted first.
    lists:reverse(OutAcc).

line(BinMetric, Node, Labels, BinVal) ->
    [BinMetric,
     <<"{">>,
     labels([{<<"node">>, Node}|Labels]),
     <<"} ">>, BinVal, <<"\n">>].

labels(Labels) ->
    join($,,
         lists:map(fun({Key, Val}) ->
                           label(Key, Val)
                   end, Labels)).

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
    <<" counter\n">>.

%% backported to support OTP18. TODO: replace with lists:join/2 when
%% dropping OTP18.
join(Sep, [H|T]) -> [H|join_prepend(Sep, T)].

join_prepend(_Sep, []) -> [];
join_prepend(Sep, [H|T]) -> [Sep,H|join_prepend(Sep,T)].
