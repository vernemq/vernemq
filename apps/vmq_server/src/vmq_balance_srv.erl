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

-module(vmq_balance_srv).
-include_lib("kernel/include/logger.hrl").

-behaviour(gen_server).

%% API
-export([
    start_link/0,
    is_accepting/0,
    balance_stats/0,
    rpc_failures_total/0,
    info/0
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

-define(SERVER, ?MODULE).
-define(DEFAULT_CHECK_INTERVAL, 5000).
-define(MIN_CHECK_INTERVAL, 100).
-define(DEFAULT_THRESHOLD, 1.2).
-define(MIN_THRESHOLD, 1.0).
-define(DEFAULT_HYSTERESIS, 0.1).
-define(DEFAULT_MIN_CONNECTIONS, 100).
-define(DEFAULT_MIN_VISIBLE_NODES, 1).
-define(RPC_TIMEOUT, 2000).
%% Jitter as a percentage of the configured check interval, applied
%% as +/- in each direction. e.g. 10 -> ticks land in
%% [Interval * 0.9, Interval * 1.1] uniformly.
-define(JITTER_PERCENT, 10).

-record(state, {
    local_count = 0 :: non_neg_integer(),
    cluster_avg = 0.0 :: float(),
    node_counts = #{} :: #{node() => non_neg_integer()},
    accepting = true :: boolean(),
    enabled = false :: boolean(),
    threshold = ?DEFAULT_THRESHOLD :: float(),
    hysteresis = ?DEFAULT_HYSTERESIS :: float(),
    min_connections = ?DEFAULT_MIN_CONNECTIONS :: non_neg_integer(),
    min_visible_nodes = ?DEFAULT_MIN_VISIBLE_NODES :: pos_integer(),
    check_interval = ?DEFAULT_CHECK_INTERVAL :: pos_integer(),
    tref :: reference() | undefined,
    last_tick_ms :: integer() | undefined,
    rpc_failures_total = 0 :: non_neg_integer()
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec is_accepting() -> boolean().
is_accepting() ->
    try
        gen_server:call(?SERVER, is_accepting, 1000)
    catch
        exit:{timeout, _} -> true;
        exit:{noproc, _} -> true;
        _:_ -> true
    end.

-spec balance_stats() ->
    {
        IsAccepting :: 0 | 1,
        LocalConnections :: non_neg_integer(),
        ClusterAvg :: float(),
        IsEnabled :: 0 | 1
    }.
balance_stats() ->
    try
        gen_server:call(?SERVER, balance_stats, 1000)
    catch
        exit:{timeout, _} -> {1, 0, 0.0, 0};
        exit:{noproc, _} -> {1, 0, 0.0, 0};
        _:_ -> {1, 0, 0.0, 0}
    end.

-spec rpc_failures_total() -> non_neg_integer().
rpc_failures_total() ->
    try
        gen_server:call(?SERVER, rpc_failures_total, 1000)
    catch
        exit:{timeout, _} -> 0;
        exit:{noproc, _} -> 0;
        _:_ -> 0
    end.

-spec info() -> map().
info() ->
    try
        gen_server:call(?SERVER, info, 1000)
    catch
        exit:{timeout, _} -> #{error => timeout};
        exit:{noproc, _} -> #{error => not_running};
        _:_ -> #{error => unavailable}
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    Enabled = vmq_config:get_env(balance_enabled, false),
    Threshold = load_threshold(),
    Hysteresis = load_hysteresis(),
    MinConnections = load_min_connections(),
    MinVisibleNodes = load_min_visible_nodes(),
    CheckInterval = load_check_interval(),
    TRef = schedule_check(CheckInterval),
    ?LOG_INFO(
        "vmq_balance_srv starting: enabled=~p, threshold=~p, hysteresis=~p, "
        "min_connections=~p, min_visible_nodes=~p, check_interval=~pms",
        [Enabled, Threshold, Hysteresis, MinConnections, MinVisibleNodes, CheckInterval]
    ),
    {ok, #state{
        enabled = Enabled,
        threshold = Threshold,
        hysteresis = Hysteresis,
        min_connections = MinConnections,
        min_visible_nodes = MinVisibleNodes,
        check_interval = CheckInterval,
        tref = TRef
    }}.

handle_call(is_accepting, _From, #state{accepting = Accepting, enabled = Enabled} = State) ->
    %% When disabled, always accept
    Result = (not Enabled) orelse Accepting,
    {reply, Result, State};
handle_call(balance_stats, _From, #state{} = State) ->
    #state{
        accepting = Accepting,
        local_count = LocalCount,
        cluster_avg = ClusterAvg,
        enabled = Enabled
    } = State,
    EffectiveAccepting = (not Enabled) orelse Accepting,
    Reply = {
        bool_to_int(EffectiveAccepting),
        LocalCount,
        ClusterAvg,
        bool_to_int(Enabled)
    },
    {reply, Reply, State};
handle_call(rpc_failures_total, _From, #state{rpc_failures_total = N} = State) ->
    {reply, N, State};
handle_call(info, _From, State) ->
    #state{
        local_count = LocalCount,
        cluster_avg = ClusterAvg,
        node_counts = NodeCounts,
        accepting = Accepting,
        enabled = Enabled,
        threshold = Threshold,
        hysteresis = Hysteresis,
        min_connections = MinConnections,
        min_visible_nodes = MinVisibleNodes,
        check_interval = CheckInterval,
        last_tick_ms = LastTickMs,
        rpc_failures_total = RpcFailuresTotal
    } = State,
    Reply = #{
        enabled => Enabled,
        accepting => (not Enabled) orelse Accepting,
        local_count => LocalCount,
        cluster_avg => ClusterAvg,
        node_counts => NodeCounts,
        num_visible_nodes => map_size(NodeCounts),
        threshold => Threshold,
        hysteresis => Hysteresis,
        min_connections => MinConnections,
        min_visible_nodes => MinVisibleNodes,
        check_interval => CheckInterval,
        last_tick_ms => LastTickMs,
        rpc_failures_total => RpcFailuresTotal
    },
    {reply, Reply, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(check_balance, State) ->
    Enabled = vmq_config:get_env(balance_enabled, false),
    Threshold = load_threshold(),
    Hysteresis = load_hysteresis(),
    MinConnections = load_min_connections(),
    MinVisibleNodes = load_min_visible_nodes(),
    Interval = load_check_interval(),
    Reloaded = State#state{
        enabled = Enabled,
        threshold = Threshold,
        hysteresis = Hysteresis,
        min_connections = MinConnections,
        min_visible_nodes = MinVisibleNodes,
        check_interval = Interval,
        last_tick_ms = erlang:system_time(millisecond)
    },
    NewState =
        case Enabled of
            true -> do_balance_check(Reloaded);
            false -> Reloaded
        end,
    TRef = schedule_check(Interval),
    {noreply, NewState#state{tref = TRef}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{tref = TRef}) ->
    cancel_timer(TRef),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_balance_check(State) ->
    #state{
        threshold = Threshold,
        hysteresis = Hysteresis,
        min_connections = MinConnections,
        min_visible_nodes = MinVisibleNodes,
        accepting = WasAccepting,
        rpc_failures_total = PrevFailures
    } = State,

    LocalCount = get_local_connection_count(),
    {NodeCounts, ClusterAvg, TotalConnections, NumNodes, TickFailures} =
        gather_cluster_counts(LocalCount),

    NewAccepting = compute_accepting(
        WasAccepting,
        LocalCount,
        ClusterAvg,
        TotalConnections,
        Threshold,
        Hysteresis,
        MinConnections,
        MinVisibleNodes,
        NumNodes
    ),

    case WasAccepting =/= NewAccepting of
        true ->
            ?LOG_NOTICE(
                "vmq_balance_srv: accepting changed ~p -> ~p "
                "(local=~p, avg=~.1f, total=~p, nodes=~p)",
                [
                    WasAccepting,
                    NewAccepting,
                    LocalCount,
                    ClusterAvg,
                    TotalConnections,
                    NumNodes
                ]
            );
        false ->
            ok
    end,

    State#state{
        local_count = LocalCount,
        cluster_avg = ClusterAvg,
        node_counts = NodeCounts,
        accepting = NewAccepting,
        rpc_failures_total = PrevFailures + TickFailures
    }.

-spec get_local_connection_count() -> non_neg_integer().
get_local_connection_count() ->
    try
        {MQTTCount, WSCount} = vmq_ranch_sup:active_mqtt_connections(),
        MQTTCount + WSCount
    catch
        _:_ -> 0
    end.

-spec gather_cluster_counts(non_neg_integer()) ->
    {
        #{node() => non_neg_integer()},
        float(),
        non_neg_integer(),
        pos_integer(),
        non_neg_integer()
    }.
gather_cluster_counts(LocalCount) ->
    OtherNodes = [N || N <- vmq_cluster_nodes(), N =/= node()],
    {RemoteCounts, TickFailures} =
        case OtherNodes of
            [] ->
                {#{}, 0};
            _ ->
                {Results, BadNodes} = rpc:multicall(
                    OtherNodes,
                    vmq_ranch_sup,
                    active_mqtt_connections,
                    [],
                    ?RPC_TIMEOUT
                ),
                {Counts, BadrpcCount} = collect_remote_counts(OtherNodes, Results),
                %% BadNodes (nodes the local kernel couldn't reach at all) are
                %% distinct from {badrpc, _} replies (node reachable but call failed).
                %% Both count as visibility failures for this tick.
                {Counts, BadrpcCount + length(BadNodes)}
        end,
    AllCounts = RemoteCounts#{node() => LocalCount},
    TotalConnections = maps:fold(fun(_N, C, Acc) -> Acc + C end, 0, AllCounts),
    NumNodes = map_size(AllCounts),
    ClusterAvg =
        case NumNodes of
            0 -> 0.0;
            _ -> TotalConnections / NumNodes
        end,
    {AllCounts, ClusterAvg, TotalConnections, NumNodes, TickFailures}.

collect_remote_counts(Nodes, Results) ->
    collect_remote_counts(Nodes, Results, #{}, 0).

collect_remote_counts([], _, Acc, Failures) ->
    {Acc, Failures};
collect_remote_counts([_Node | RestNodes], [], Acc, Failures) ->
    %% Fewer results than nodes (shouldn't happen with multicall, but be safe)
    collect_remote_counts(RestNodes, [], Acc, Failures);
collect_remote_counts([Node | RestNodes], [{badrpc, Reason} | RestResults], Acc, Failures) ->
    ?LOG_DEBUG("vmq_balance_srv: RPC to ~p failed: ~p", [Node, Reason]),
    collect_remote_counts(RestNodes, RestResults, Acc, Failures + 1);
collect_remote_counts([Node | RestNodes], [{MQTTCount, WSCount} | RestResults], Acc, Failures) when
    is_integer(MQTTCount), is_integer(WSCount)
->
    collect_remote_counts(
        RestNodes,
        RestResults,
        Acc#{Node => MQTTCount + WSCount},
        Failures
    );
collect_remote_counts([Node | RestNodes], [_Other | RestResults], Acc, Failures) ->
    ?LOG_DEBUG("vmq_balance_srv: unexpected RPC result from ~p", [Node]),
    collect_remote_counts(RestNodes, RestResults, Acc, Failures + 1).

-spec compute_accepting(
    boolean(),
    non_neg_integer(),
    float(),
    non_neg_integer(),
    float(),
    float(),
    non_neg_integer(),
    pos_integer(),
    pos_integer()
) -> boolean().
compute_accepting(
    _WasAccepting,
    0 = _LocalCount,
    _ClusterAvg,
    _TotalConnections,
    _Threshold,
    _Hysteresis,
    _MinConnections,
    _MinVisibleNodes,
    _NumNodes
) ->
    %% An empty node is never overloaded. This also closes the degenerate
    %% case where ClusterAvg=0 (all peers idle) would otherwise leave a
    %% previously-rejecting node stuck rejecting via `0 < 0 * (T-H)`.
    true;
compute_accepting(
    _WasAccepting,
    _LocalCount,
    _ClusterAvg,
    _TotalConnections,
    _Threshold,
    _Hysteresis,
    _MinConnections,
    MinVisibleNodes,
    NumNodes
) when NumNodes < MinVisibleNodes ->
    %% Not enough peers visible this tick to make a confident decision -
    %% fail-open. Default min_visible_nodes=1 makes this clause equivalent
    %% to the pre-existing "single node always accepts" short-circuit;
    %% operators may raise it for tighter tuning in flaky-network setups.
    true;
compute_accepting(
    _WasAccepting,
    _LocalCount,
    _ClusterAvg,
    _TotalConnections,
    _Threshold,
    _Hysteresis,
    _MinConnections,
    _MinVisibleNodes,
    NumNodes
) when NumNodes =< 1 ->
    %% Single node or no nodes - always accept
    true;
compute_accepting(
    _WasAccepting,
    _LocalCount,
    _ClusterAvg,
    TotalConnections,
    _Threshold,
    _Hysteresis,
    MinConnections,
    _MinVisibleNodes,
    _NumNodes
) when TotalConnections < MinConnections ->
    %% Below minimum - always accept
    true;
compute_accepting(
    true = _WasAccepting,
    LocalCount,
    ClusterAvg,
    _TotalConnections,
    Threshold,
    _Hysteresis,
    _MinConnections,
    _MinVisibleNodes,
    _NumNodes
) ->
    %% Currently accepting: stop accepting when above threshold
    LocalCount =< ClusterAvg * Threshold;
compute_accepting(
    false = _WasAccepting,
    LocalCount,
    ClusterAvg,
    _TotalConnections,
    Threshold,
    Hysteresis,
    _MinConnections,
    _MinVisibleNodes,
    _NumNodes
) ->
    %% Currently rejecting: resume accepting when below (threshold - hysteresis)
    LocalCount < ClusterAvg * (Threshold - Hysteresis).

vmq_cluster_nodes() ->
    try
        case vmq_cluster:status() of
            Status when is_list(Status) ->
                [Node || {Node, true} <- Status];
            _ ->
                [node()]
        end
    catch
        _:_ -> [node()]
    end.

schedule_check(Interval) ->
    %% Apply +/- JITTER_PERCENT to prevent ticks across a freshly-restarted
    %% cluster from aligning and producing synchronized RPC fan-outs.
    erlang:send_after(jitter(Interval), self(), check_balance).

-spec jitter(pos_integer()) -> pos_integer().
jitter(Interval) ->
    Range = max(1, (Interval * ?JITTER_PERCENT) div 100),
    %% rand:uniform(2*Range+1) gives 1..2R+1; subtract R+1 -> -R..R offset.
    Offset = rand:uniform(2 * Range + 1) - (Range + 1),
    max(1, Interval + Offset).

cancel_timer(undefined) -> ok;
cancel_timer(TRef) -> erlang:cancel_timer(TRef).

bool_to_int(true) -> 1;
bool_to_int(false) -> 0.

load_threshold() ->
    %% Clamp to MIN_THRESHOLD: a threshold < 1.0 would reject as soon as a
    %% node is at or below the cluster average, which is nonsensical.
    erlang:max(?MIN_THRESHOLD, parse_float_env(balance_threshold, ?DEFAULT_THRESHOLD)).

load_hysteresis() ->
    erlang:max(0.0, parse_float_env(balance_hysteresis, ?DEFAULT_HYSTERESIS)).

load_min_connections() ->
    case vmq_config:get_env(balance_min_connections, ?DEFAULT_MIN_CONNECTIONS) of
        N when is_integer(N), N >= 0 -> N;
        _ -> ?DEFAULT_MIN_CONNECTIONS
    end.

load_min_visible_nodes() ->
    case vmq_config:get_env(balance_min_visible_nodes, ?DEFAULT_MIN_VISIBLE_NODES) of
        N when is_integer(N), N >= 1 -> N;
        _ -> ?DEFAULT_MIN_VISIBLE_NODES
    end.

load_check_interval() ->
    case vmq_config:get_env(balance_check_interval, ?DEFAULT_CHECK_INTERVAL) of
        N when is_integer(N) -> erlang:max(?MIN_CHECK_INTERVAL, N);
        _ -> ?DEFAULT_CHECK_INTERVAL
    end.

parse_float_env(Key, Default) ->
    case vmq_config:get_env(Key, undefined) of
        undefined ->
            Default;
        Val when is_float(Val) ->
            Val;
        Val when is_integer(Val) ->
            float(Val);
        Val when is_list(Val) ->
            try
                list_to_float(Val)
            catch
                _:_ ->
                    try
                        float(list_to_integer(Val))
                    catch
                        _:_ -> Default
                    end
            end;
        Val when is_binary(Val) ->
            parse_float_env_bin(Val, Default);
        _ ->
            Default
    end.

parse_float_env_bin(Bin, Default) ->
    try
        binary_to_float(Bin)
    catch
        _:_ ->
            try
                float(binary_to_integer(Bin))
            catch
                _:_ -> Default
            end
    end.
