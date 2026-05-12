-module(vmq_enhanced_auth_rate_limiter).

-behaviour(gen_server).

-include("vmq_enhanced_auth.hrl").

-export([
    start_link/0,
    check_publish_rate/2,
    set_rate/2,
    delete_rate/1,
    list_rates/0
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER, ?MODULE).

-record(state, {}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec set_rate(AclName :: binary(), MaxRate :: pos_integer()) -> ok.
set_rate(AclName, MaxRate) when is_binary(AclName), is_integer(MaxRate), MaxRate > 0 ->
    gen_server:call(?SERVER, {set_rate, AclName, MaxRate}).

-spec delete_rate(AclName :: binary()) -> ok | {error, not_found}.
delete_rate(AclName) when is_binary(AclName) ->
    gen_server:call(?SERVER, {delete_rate, AclName}).

-spec list_rates() -> [{binary(), pos_integer()}].
list_rates() ->
    gen_server:call(?SERVER, list_rates).

-spec check_publish_rate(AclName :: undefined | binary(), QoS :: non_neg_integer()) -> allow | drop.
check_publish_rate(undefined, _QoS) ->
    allow;
check_publish_rate(AclName, QoS) when is_binary(AclName) ->
    case ets:lookup(?RATE_CONFIG_TBL, AclName) of
        [] ->
            allow;
        [{AclName, MaxRate}] ->
            Count = ets:update_counter(?RATE_COUNTER_TBL, AclName, 1, {AclName, 0}),
            case Count > MaxRate of
                true ->
                    vmq_enhanced_auth_metrics:incr_publish_drop_metric(AclName, QoS),
                    drop;
                false ->
                    allow
            end
    end.

init([]) ->
    ets:new(?RATE_CONFIG_TBL, [public, named_table, {read_concurrency, true}]),
    ets:new(?RATE_COUNTER_TBL, [public, named_table, {write_concurrency, true}]),
    ets:new(?RATE_LIMIT_METRICS_TBL, [public, named_table, {write_concurrency, true}]),
    {ok, TRef} = timer:send_interval(1000, reset_counters),
    put(reset_timer, TRef),
    load_config(),
    {ok, #state{}}.

handle_call({set_rate, AclName, MaxRate}, _From, State) ->
    ets:insert(?RATE_CONFIG_TBL, {AclName, MaxRate}),
    {reply, ok, State};
handle_call({delete_rate, AclName}, _From, State) ->
    case ets:lookup(?RATE_CONFIG_TBL, AclName) of
        [] ->
            {reply, {error, not_found}, State};
        _ ->
            ets:delete(?RATE_CONFIG_TBL, AclName),
            ets:match_delete(?RATE_LIMIT_METRICS_TBL, {{AclName, '_'}, '_'}),
            {reply, ok, State}
    end;
handle_call(list_rates, _From, State) ->
    Rates = ets:tab2list(?RATE_CONFIG_TBL),
    {reply, Rates, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(reset_counters, State) ->
    ets:delete_all_objects(?RATE_COUNTER_TBL),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    case get(reset_timer) of
        undefined -> ok;
        TRef -> timer:cancel(TRef)
    end,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

load_config() ->
    RateLimits = application:get_env(vmq_enhanced_auth, publish_rate_limit, []),
    lists:foreach(
        fun
            ({AclNameStr, MaxRate}) when is_integer(MaxRate), MaxRate > 0 ->
                AclName = list_to_binary(AclNameStr),
                ets:insert(?RATE_CONFIG_TBL, {AclName, MaxRate});
            (_) ->
                ok
        end,
        RateLimits
    ).
