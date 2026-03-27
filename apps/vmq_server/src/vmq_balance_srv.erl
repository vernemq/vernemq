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
    balance_stats/0
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
-define(DEFAULT_THRESHOLD, 1.2).
-define(DEFAULT_HYSTERESIS, 0.1).
-define(DEFAULT_MIN_CONNECTIONS, 100).
-define(RPC_TIMEOUT, 2000).

-record(state, {
    local_count = 0 :: non_neg_integer(),
    cluster_avg = 0.0 :: float(),
    node_counts = #{} :: #{node() => non_neg_integer()},
    accepting = true :: boolean(),
    enabled = false :: boolean(),
    threshold = ?DEFAULT_THRESHOLD :: float(),
    hysteresis = ?DEFAULT_HYSTERESIS :: float(),
    min_connections = ?DEFAULT_MIN_CONNECTIONS :: non_neg_integer(),
    check_interval = ?DEFAULT_CHECK_INTERVAL :: pos_integer(),
    tref :: reference() | undefined
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
        IsAccepting :: non_neg_integer(),
        LocalConnections :: non_neg_integer(),
        ClusterAvg :: non_neg_integer(),
        IsEnabled :: non_neg_integer()
    }.
balance_stats() ->
    try
        gen_server:call(?SERVER, balance_stats, 1000)
    catch
        exit:{timeout, _} -> {1, 0, 0, 0};
        exit:{noproc, _} -> {1, 0, 0, 0};
        _:_ -> {1, 0, 0, 0}
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    Enabled = vmq_config:get_env(balance_enabled, false),
    Threshold = parse_float_env(balance_threshold, ?DEFAULT_THRESHOLD),
    Hysteresis = parse_float_env(balance_hysteresis, ?DEFAULT_HYSTERESIS),
    MinConnections = vmq_config:get_env(balance_min_connections, ?DEFAULT_MIN_CONNECTIONS),
    CheckInterval = vmq_config:get_env(balance_check_interval, ?DEFAULT_CHECK_INTERVAL),
    TRef = schedule_check(CheckInterval),
    ?LOG_INFO(
        "vmq_balance_srv starting: enabled=~p, threshold=~p, hysteresis=~p, "
        "min_connections=~p, check_interval=~pms",
        [Enabled, Threshold, Hysteresis, MinConnections, CheckInterval]
    ),
    {ok, #state{
        enabled = Enabled,
        threshold = Threshold,
        hysteresis = Hysteresis,
        min_connections = MinConnections,
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
        round(ClusterAvg),
        bool_to_int(Enabled)
    },
    {reply, Reply, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(check_balance, State) ->
    Enabled = vmq_config:get_env(balance_enabled, false),
    Interval = vmq_config:get_env(balance_check_interval, ?DEFAULT_CHECK_INTERVAL),
    NewState =
        case Enabled of
            true -> do_balance_check(State#state{enabled = true});
            false -> State#state{enabled = false}
        end,
    TRef = schedule_check(Interval),
    {noreply, NewState#state{check_interval = Interval, tref = TRef}};
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
        accepting = WasAccepting
    } = State,

    LocalCount = get_local_connection_count(),
    {NodeCounts, ClusterAvg, TotalConnections, NumNodes} = gather_cluster_counts(LocalCount),

    NewAccepting = compute_accepting(
        WasAccepting,
        LocalCount,
        ClusterAvg,
        TotalConnections,
        Threshold,
        Hysteresis,
        MinConnections,
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
        accepting = NewAccepting
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
    {#{node() => non_neg_integer()}, float(), non_neg_integer(), pos_integer()}.
gather_cluster_counts(LocalCount) ->
    OtherNodes = [N || N <- vmq_cluster_nodes(), N =/= node()],
    RemoteCounts =
        case OtherNodes of
            [] ->
                #{};
            _ ->
                {Results, _BadNodes} = rpc:multicall(
                    OtherNodes,
                    vmq_ranch_sup,
                    active_mqtt_connections,
                    [],
                    ?RPC_TIMEOUT
                ),
                collect_remote_counts(OtherNodes, Results)
        end,
    AllCounts = RemoteCounts#{node() => LocalCount},
    TotalConnections = maps:fold(fun(_N, C, Acc) -> Acc + C end, 0, AllCounts),
    NumNodes = map_size(AllCounts),
    ClusterAvg =
        case NumNodes of
            0 -> 0.0;
            _ -> TotalConnections / NumNodes
        end,
    {AllCounts, ClusterAvg, TotalConnections, NumNodes}.

collect_remote_counts(Nodes, Results) ->
    collect_remote_counts(Nodes, Results, #{}).

collect_remote_counts([], _, Acc) ->
    Acc;
collect_remote_counts([_Node | RestNodes], [], Acc) ->
    %% Fewer results than nodes (shouldn't happen with multicall, but be safe)
    collect_remote_counts(RestNodes, [], Acc);
collect_remote_counts([Node | RestNodes], [{badrpc, Reason} | RestResults], Acc) ->
    ?LOG_DEBUG("vmq_balance_srv: RPC to ~p failed: ~p", [Node, Reason]),
    collect_remote_counts(RestNodes, RestResults, Acc);
collect_remote_counts([Node | RestNodes], [{MQTTCount, WSCount} | RestResults], Acc) when
    is_integer(MQTTCount), is_integer(WSCount)
->
    collect_remote_counts(RestNodes, RestResults, Acc#{Node => MQTTCount + WSCount});
collect_remote_counts([Node | RestNodes], [_Other | RestResults], Acc) ->
    ?LOG_DEBUG("vmq_balance_srv: unexpected RPC result from ~p", [Node]),
    collect_remote_counts(RestNodes, RestResults, Acc).

-spec compute_accepting(
    boolean(),
    non_neg_integer(),
    float(),
    non_neg_integer(),
    float(),
    float(),
    non_neg_integer(),
    pos_integer()
) -> boolean().
compute_accepting(
    _WasAccepting,
    _LocalCount,
    _ClusterAvg,
    _TotalConnections,
    _Threshold,
    _Hysteresis,
    _MinConnections,
    NumNodes
) when NumNodes =< 1 ->
    %% Single node or no nodes — always accept
    true;
compute_accepting(
    _WasAccepting,
    _LocalCount,
    _ClusterAvg,
    TotalConnections,
    _Threshold,
    _Hysteresis,
    MinConnections,
    _NumNodes
) when TotalConnections < MinConnections ->
    %% Below minimum — always accept
    true;
compute_accepting(
    true = _WasAccepting,
    LocalCount,
    ClusterAvg,
    _TotalConnections,
    Threshold,
    _Hysteresis,
    _MinConnections,
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
    erlang:send_after(Interval, self(), check_balance).

cancel_timer(undefined) -> ok;
cancel_timer(TRef) -> erlang:cancel_timer(TRef).

bool_to_int(true) -> 1;
bool_to_int(false) -> 0.

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
