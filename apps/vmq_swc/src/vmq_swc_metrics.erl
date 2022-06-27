%% Copyright 2018 Octavo Labs AG Zurich Switzerland (https://octavolabs.com)
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

-module(vmq_swc_metrics).

-behaviour(gen_server).

%% API
-export([
    start_link/0,
    register_gauge/2
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-export([
    metrics/0,
    timed_measurement/4,
    incr_counter/1,
    incr_counter/2
]).

-define(SERVER, ?MODULE).
-define(TIMER_TABLE, vmq_swc_metrics_timers).
-define(COUNTER_TABLE, vmq_swc_metrics_counters).
-define(NR_OF_SAMPLES, 1000).

-record(state, {gauges = #{}}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

register_gauge(MetricName, Fun) ->
    gen_server:call(?SERVER, {register_gauge, self(), MetricName, Fun}, infinity).

metric_name({Metric, SubMetric}) ->
    LMetric = atom_to_list(Metric),
    LSubMetric = atom_to_list(SubMetric),
    Name = list_to_atom(LMetric ++ "_" ++ LSubMetric ++ "_microseconds"),
    Description = list_to_binary(
        "A histogram of the " ++ LMetric ++ " " ++ LSubMetric ++ " latency."
    ),
    {Name, Description}.

metrics() ->
    Histograms =
        ets:foldl(
            fun({Metric, TotalCount, LE10, LE100, LE1K, LE10K, LE100K, LE1M, INF, TotalSum}, Acc) ->
                {MetricName, Description} = metric_name(Metric),
                Buckets =
                    #{
                        10 => LE10,
                        100 => LE100,
                        1000 => LE1K,
                        10000 => LE10K,
                        100000 => LE100K,
                        1000000 => LE1M,
                        infinity => INF
                    },
                [
                    {histogram, [], MetricName, MetricName, Description,
                        {TotalCount, TotalSum, Buckets}}
                    | Acc
                ]
            end,
            [],
            ?TIMER_TABLE
        ),

    Gauges = gen_server:call(?SERVER, get_gauges, infinity),
    maps:fold(
        fun({MetricName, Group}, {GaugeFun, _OwnerRef}, Acc) ->
            try
                UniqueId = list_to_atom(atom_to_list(MetricName) ++ "_" ++ atom_to_list(Group)),
                {Labels, Description, Value} = GaugeFun(),
                [{gauge, Labels, UniqueId, MetricName, Description, Value} | Acc]
            catch
                _:_ ->
                    Acc
            end
        end,
        Histograms,
        Gauges
    ).

incr_bucket_ops(V) when V =< 10 ->
    [{2, 1}, {3, 1}, {4, 1}, {5, 1}, {6, 1}, {7, 1}, {8, 1}, {9, 1}, {10, V}];
incr_bucket_ops(V) when V =< 100 ->
    [{2, 1}, {4, 1}, {5, 1}, {6, 1}, {7, 1}, {8, 1}, {9, 1}, {10, V}];
incr_bucket_ops(V) when V =< 1000 ->
    [{2, 1}, {5, 1}, {6, 1}, {7, 1}, {8, 1}, {9, 1}, {10, V}];
incr_bucket_ops(V) when V =< 10000 ->
    [{2, 1}, {6, 1}, {7, 1}, {8, 1}, {9, 1}, {10, V}];
incr_bucket_ops(V) when V =< 100000 ->
    [{2, 1}, {7, 1}, {8, 1}, {9, 1}, {10, V}];
incr_bucket_ops(V) when V =< 1000000 ->
    [{2, 1}, {8, 1}, {9, 1}, {10, V}];
incr_bucket_ops(V) ->
    [{2, 1}, {9, 1}, {10, V}].

timed_measurement({_, _} = Metric, Module, Function, Args) ->
    Ts1 = ts(),
    Ret = apply(Module, Function, Args),
    Ts2 = ts(),
    Val = Ts2 - Ts1,
    BucketOps = incr_bucket_ops(Val),
    incr_histogram_buckets(Metric, BucketOps),
    Ret.

incr_histogram_buckets(Metric, BucketOps) ->
    try
        ets:update_counter(?TIMER_TABLE, Metric, BucketOps)
    catch
        _:_ ->
            ets:insert_new(?TIMER_TABLE, {Metric, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
            incr_histogram_buckets(Metric, BucketOps)
    end.

incr_counter({_, _} = Metric) ->
    incr_counter(Metric, 1).

incr_counter({_, _} = Metric, N) ->
    try
        ets:update_counter(?COUNTER_TABLE, Metric, {2, N})
    catch
        _:_ ->
            ets:insert_new(?COUNTER_TABLE, {Metric, 0}),
            incr_counter(Metric, N)
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    ets:new(?COUNTER_TABLE, [named_table, public, {write_concurrency, true}]),
    ets:new(?TIMER_TABLE, [named_table, public, {write_concurrency, true}]),
    {ok, #state{}}.

handle_call({register_gauge, OwnerPid, MetricName, Fun}, _From, #state{gauges = Gauges} = State) ->
    MRef = monitor(process, OwnerPid),
    Reply = ok,
    {reply, Reply, State#state{gauges = maps:put(MetricName, {Fun, {OwnerPid, MRef}}, Gauges)}};
handle_call(get_gauges, _From, #state{gauges = Gauges} = State) ->
    {reply, Gauges, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', MRef, process, OwnerPid, _Info}, #state{gauges = Gauges0} = State) ->
    Gauges1 =
        maps:filter(
            fun
                (_MetricName, {_, MetricRef}) when MetricRef == {OwnerPid, MRef} -> false;
                (_, _) -> true
            end,
            Gauges0
        ),
    {noreply, State#state{gauges = Gauges1}}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
ts() ->
    {Mega, Sec, Micro} = os:timestamp(),
    (Mega * 1000000 + Sec) * 1000000 + Micro.
