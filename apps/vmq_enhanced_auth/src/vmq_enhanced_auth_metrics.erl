%% Copyright Gojek

-module(vmq_enhanced_auth_metrics).

-behaviour(gen_server).

%% API
-export([
    start_link/0,
    metrics/0,
    incr/1,
    incr/2
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

-include_lib("vmq_enhanced_auth/src/vmq_enhanced_auth.hrl").

-record(state, {}).
-record(metric_def, {
    type :: atom(),
    labels :: [metric_label()],
    id :: metric_id(),
    name :: atom(),
    description :: undefined | binary()
}).

-type metric_def() :: #metric_def{}.
-type metric_val() :: {Id :: metric_id(), Val :: any()}.
-type metric_label() :: {atom(), string()}.
-type metric_id() ::
    atom()
    | {atom(), non_neg_integer() | atom()}
    | {atom(), atom(), atom()}
    | [{atom(), [{atom(), any()}]}].

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link() -> 'ignore' | {'error', _} | {'ok', pid()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec metrics() -> [{metric_def(), non_neg_integer()}].
metrics() ->
    MetricDefs = metrics_defs(),
    MetricValues = metric_values(),

    %% Create id->metric def map
    IdDef = lists:foldl(
        fun(#metric_def{id = Id} = MD, Acc) ->
            maps:put(Id, MD, Acc)
        end,
        #{},
        MetricDefs
    ),

    %% Merge metrics definitions with values and filter based on labels.
    Metrics = lists:filtermap(
        fun({Id, Val}) ->
            case maps:find(Id, IdDef) of
                {ok, #metric_def{
                    type = Type, id = Id, name = Name, labels = GotLabels, description = Description
                }} ->
                    {true, {Type, GotLabels, Id, Name, Description, Val}};
                error ->
                    %% this could happen if metric definitions does
                    %% not correspond to the ids returned with the
                    %% metrics values.
                    lager:warning("unknown metrics id: ~p", [Id]),
                    false
            end
        end,
        MetricValues
    ),
    Metrics.

-spec incr(any()) -> 'ok'.
incr(Entry) ->
    incr_item(Entry, 1).

-spec incr(any(), non_neg_integer()) -> 'ok'.
incr(Entry, N) ->
    incr_item(Entry, N).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init(_) -> {ok, #state{}}.
init([]) ->
    AllEntries = [Id || #metric_def{id = Id} <- metrics_defs()],
    NumEntries = length(AllEntries),
    %% Sanity check where it is checked that there is a one-to-one
    %% mapping between atomics indexes and metrics identifiers by 1)
    %% checking that all metric identifiers have an atomics index and
    %% 2) that there are as many indexes as there are metrics and 3)
    %% that there are no index duplicates.
    Idxs = lists:map(fun(Id) -> met2idx(Id) end, AllEntries),
    NumEntries = length(lists:sort(Idxs)),
    NumEntries = length(lists:usort(Idxs)),

    %% only alloc a new atomics array if one doesn't already exist!
    case catch persistent_term:get(?MODULE) of
        {'EXIT', {badarg, _}} ->
            %% allocate twice the number of entries to make it possible to add
            %% new metrics during a hot code upgrade.
            ARef = atomics:new(2 * NumEntries, [{signed, false}]),
            persistent_term:put(?MODULE, ARef);
        _ExistingRef ->
            ok
    end,
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
%%% Internal functions
%%%===================================================================

%% don't do the update
incr_item(_, 0) ->
    ok;
incr_item(Entry, Val) when Val > 0 ->
    ARef =
        case get(vmq_enhanced_auth_atomics_ref) of
            undefined ->
                Ref = persistent_term:get(?MODULE),
                put(vmq_enhanced_auth_atomics_ref, Ref),
                Ref;
            Ref ->
                Ref
        end,
    atomics:add(ARef, met2idx(Entry), Val).

-spec metric_values() -> [metric_val()].
metric_values() ->
    lists:map(
        fun(#metric_def{id = Id}) ->
            try counter_val(Id) of
                Value -> {Id, Value}
            catch
                _:_ -> {Id, 0}
            end
        end,
        metrics_defs()
    ).

-spec metrics_defs() -> [metric_def()].
metrics_defs() ->
    [
        m(
            counter,
            [
                {reason, atom_to_list(?INVALID_SIGNATURE)}
            ],
            {?REGISTER_AUTH_ERROR, ?INVALID_SIGNATURE},
            ?REGISTER_AUTH_ERROR,
            <<"The number of times the auth_on_register hook returned error due to invalid_signature.">>
        ),
        m(
            counter,
            [
                {reason, atom_to_list(?MISSING_RID)}
            ],
            {?REGISTER_AUTH_ERROR, ?MISSING_RID},
            ?REGISTER_AUTH_ERROR,
            <<"The number of times the auth_on_register hook returned error due to missing_rid.">>
        ),
        m(
            counter,
            [
                {reason, atom_to_list(?USERNAME_RID_MISMATCH)}
            ],
            {?REGISTER_AUTH_ERROR, ?USERNAME_RID_MISMATCH},
            ?REGISTER_AUTH_ERROR,
            <<"The number of times the auth_on_register hook returned error due to username_rid_mismatch.">>
        )
    ].

-spec m(atom(), [metric_label()], metric_id(), atom(), 'undefined' | binary()) -> metric_def().
m(Type, Labels, UniqueId, Name, Description) ->
    #metric_def{
        type = Type,
        labels = Labels,
        id = UniqueId,
        name = Name,
        description = Description
    }.

counter_val(Entry) ->
    ARef = persistent_term:get(?MODULE),
    atomics:get(ARef, met2idx(Entry)).

met2idx({?REGISTER_AUTH_ERROR, ?INVALID_SIGNATURE}) -> 1;
met2idx({?REGISTER_AUTH_ERROR, ?USERNAME_RID_MISMATCH}) -> 2;
met2idx({?REGISTER_AUTH_ERROR, ?MISSING_RID}) -> 3.
