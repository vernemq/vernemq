%%%-------------------------------------------------------------------
%% @doc vmq_http_pub plugin top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(vmq_http_pub_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, metrics/0, incr_metric/2]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    init_metrics(),
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    {ok, {{one_for_one, 0, 1}, []}}.

%%====================================================================
%% Internal functions
%%====================================================================

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% METRICS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init_metrics() ->
    case catch persistent_term:get(?MODULE) of
        {'EXIT', {badarg, _}} ->
            %% allocate twice the number of entries to make it possible to add
            %% new metrics during a hot code upgrade.
            ARef = atomics:new(2 * 4, [{signed, false}]),
            persistent_term:put(?MODULE, ARef);
        _ExistingRef ->
            ok
    end.

incr_metric(Entry, Val) when Val > 0 ->
    ARef =
        case get(atomics_mqtt_metric_ref) of
            undefined ->
                Ref = persistent_term:get(?MODULE),
                put(atomics_mqtt_metric_ref, Ref),
                Ref;
            Ref ->
                Ref
        end,
    atomics:add_get(ARef, met2idx(Entry), Val).

get_item(Entry) ->
    ARef =
        case get(atomics_mqtt_metric_ref) of
            undefined ->
                Ref = persistent_term:get(?MODULE),
                put(atomics_mqtt_metric_ref, Ref),
                Ref;
            Ref ->
                Ref
        end,
    atomics:get(ARef, met2idx(Entry)).

get_mqtt_publish_sent() ->
    get_item(published).

get_mqtt_error() ->
    get_item(api_error).

get_mqtt_auth_error() ->
    get_item(auth_error).

metrics() ->
    Name = "http_pub",
    [
        {counter, [], {published, 0}, label(Name, published),
            <<"The number of messages sent through the REST Publish API">>,
            get_mqtt_publish_sent()},
        {counter, [], {api_error, 0}, label(Name, api_error),
            <<"Number of errors reported by the REST Publish API">>, get_mqtt_error()},
        {counter, [], {auth_error, 0}, label(Name, auth_error),
            <<"Number of Auth errors reported by the REST Publish API">>, get_mqtt_auth_error()}
    ].

label(Name, Metric) ->
    % this will create 6 labels (atoms) per bridge for the metrics above. atoms will be the same in every call after that.
    list_to_atom(lists:concat([Name, "_", Metric])).

met2idx(published) -> 1;
met2idx(api_error) -> 2;
met2idx(auth_error) -> 3.
