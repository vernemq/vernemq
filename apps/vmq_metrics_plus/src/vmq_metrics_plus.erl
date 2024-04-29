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

-module(vmq_metrics_plus).

-behaviour(gen_server).

%% API
-export([
    start_link/0
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
    incr_matched_topic/3
]).

-define(SERVER, ?MODULE).
-define(TOPIC_LABEL_TABLE, topic_labels).

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

metrics() -> topic_metrics().

topic_metrics() ->
    ets:foldl(
        fun({Metric, TotalCount}, Acc) ->
            {UniqueId, MetricName, Description, Labels} = topic_metric_name(Metric),
            [{counter, Labels, UniqueId, MetricName, Description, TotalCount} | Acc]
        end,
        [],
        ?TOPIC_LABEL_TABLE
    ).

-type metric_type() :: subscribe | publish | deliver | delivery_complete | message_drop.

-spec incr_topic_counter(
    Metric :: {topic_matches, metric_type(), Labels :: [{atom(), atom() | list() | binary()}]}
) -> ok.
incr_topic_counter(Metric) ->
    try
        ets:update_counter(?TOPIC_LABEL_TABLE, Metric, 1)
    catch
        _:_ ->
            try
                ets:insert_new(?TOPIC_LABEL_TABLE, {Metric, 0}),
                incr_topic_counter(Metric)
            catch
                _:_ ->
                    lager:warning("couldn't initialize tables", [])
            end
    end.

-spec incr_matched_topic(binary() | undefined, metric_type(), integer()) -> ok.
incr_matched_topic(<<>>, _Type, _Qos) ->
    ok;
incr_matched_topic(undefined, _Type, _Qos) ->
    ok;
incr_matched_topic(Name, Type, Qos) ->
    incr_topic_counter({topic_matches, Type, [{acl_matched, Name}, {qos, integer_to_list(Qos)}]}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    ets:new(?TOPIC_LABEL_TABLE, [named_table, public, {write_concurrency, true}]),
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================

topic_metric_name({Metric, SubMetric, Labels}) ->
    LMetric = atom_to_list(Metric),
    LSubMetric = atom_to_list(SubMetric),
    MetricName = list_to_atom(LSubMetric ++ "_" ++ LMetric),
    Description = list_to_binary(
        "The number of " ++ LSubMetric ++ " packets on ACL matched topics."
    ),
    {[MetricName | Labels], MetricName, Description, Labels}.
