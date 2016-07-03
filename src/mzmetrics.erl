-module(mzmetrics).
-export([alloc_resource/3, incr_resource_counter/2, num_counters/1]).
-export([decr_resource_counter/2, get_resource_counter/2]).
-export([reset_resource_counter/2, update_resource_counter/3, get_all_resources/2]).
-export([report_all_counters/3]).

-include("mzmetrics_header.hrl").

-on_load(init/0).
-define(NOT_LOADED, not_loaded(?LINE)).
not_loaded(Line) ->
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, Line}]}).

-define(CACHELINE_BYTES, 64).
-define(COUNTER_SIZE, 8).
align_cntr(X) ->  ((X * ?COUNTER_SIZE + ?CACHELINE_BYTES - 1) band bnot(?CACHELINE_BYTES - 1)) div ?COUNTER_SIZE.

-spec num_counters(Family::non_neg_integer()) -> NumCounters::non_neg_integer().
num_counters(Family) ->
    case Family of
        ?METRICS_FAMILY_RABBITS ->
            align_cntr(?MAX_CNTRS_PER_RABBIT);
        ?METRICS_FAMILY_CARS ->
            align_cntr(?MAX_CNTRS_PER_CAR);
        _ ->
            align_cntr(?MAX_CNTRS_PER_UNKNOWN_FAMILY)
    end.

init() ->
    PrivDir = case code:priv_dir(?MODULE) of
                  {error, _} ->
                      EbinDir = filename:dirname(code:which(?MODULE)),
                      AppPath = filename:dirname(EbinDir),
                      filename:join(AppPath, "priv");
                  Path ->
                      Path
              end,
    CountersTuple = list_to_tuple([num_counters(Family) || Family <- lists:seq(0, ?MAX_FAMILIES - 1)]),
    erlang:load_nif(filename:join(PrivDir, "mzmetrics"),
                    {ncpus(), ?MAX_FAMILIES, CountersTuple}).

ncpus() ->
    case erlang:system_info(logical_processors_available) of
        unknown ->    % Happens on Mac OS X.
            erlang:system_info(schedulers);
        N -> N
    end.

-spec alloc_resource(Family::non_neg_integer(),
                     ResourceName::resource_name(),
                     NumCounters::non_neg_integer()) ->
                                            Res::binary() | no_return().
alloc_resource(_X, _Y, _Z) ->
    ?NOT_LOADED.

-spec update_resource_counter(ResourceName::resource_name(), Offset::non_neg_integer(), Val::integer()) ->
                                        Val::non_neg_integer() | no_return().
update_resource_counter(_X, _Y, _Z) ->
    ?NOT_LOADED.

-spec incr_resource_counter(ResourceName::resource_name(), Offset::non_neg_integer()) ->
                                        Val::non_neg_integer() | no_return().
incr_resource_counter(_X, _Y) ->
    ?NOT_LOADED.

-spec decr_resource_counter(ResourceName::resource_name(), Offset::non_neg_integer()) ->
                                    Val::non_neg_integer() | no_return().
decr_resource_counter(_X, _Y) ->
    ?NOT_LOADED.

-spec get_resource_counter(ResourceName::resource_name(), Offset::non_neg_integer()) ->
                                        Val::non_neg_integer() | no_return().
get_resource_counter(_X, _Y) ->
    ?NOT_LOADED.

-spec reset_resource_counter(ResourceName::resource_name(), Offset::non_neg_integer()) ->
                                        Val::non_neg_integer() | no_return().
reset_resource_counter(_X, _Y) ->
    ?NOT_LOADED.

-spec get_all_resources(Family::non_neg_integer(), Options::non_neg_integer()) ->
                                            binary() | no_return().
get_all_resources(_X, _Y) ->
    ?NOT_LOADED.

-define(CounterDataSize, 64).
-define(ResourceNameSize, 1000).
-define(BinDataSizePerResource, 1064).

json_ensure_binary(Bin) when is_binary(Bin) ->
    Bin;
json_ensure_binary(Bin) when is_list(Bin) ->
    iolist_to_binary(Bin).

-spec report_all_counters(Family::family_name(), [ReportFun::fun()], JsonFun::fun()) -> ok.
report_all_counters(Family, ReportFuns, JsonFun) ->
    C = mzmetrics:get_all_resources(Family, ?CNTR_DONT_RESET),
    NumResources = byte_size(C) div ?BinDataSizePerResource,
    extract_resource_data(ReportFuns, JsonFun, NumResources, C).

extract_counter_values(BinData) ->
    <<C1:64/little-unsigned-integer>> = binary:part(BinData, 0, 8),
    <<C2:64/little-unsigned-integer>> = binary:part(BinData, 8, 8),
    <<C3:64/little-unsigned-integer>> = binary:part(BinData, 16, 8),
    {C1, C2, C3}.

extract_resource_name(BinData) ->
    [ResourceName|_] = binary:split(BinData, [<<0>>]),
    ResourceName.

extract_resource_data(_, _, 0, _) ->
    ok;
extract_resource_data(ReportFuns, JsonFun, N, BinData) ->
    {C1, C2, C3} = extract_counter_values(binary:part(BinData, ?ResourceNameSize, ?CounterDataSize)),
    ResourceName = extract_resource_name(BinData),
    JSON = json_ensure_binary(JsonFun(ResourceName, C1, C2, C3)),
    [ ok = ReportFun(JSON) || ReportFun <- ReportFuns ],
    extract_resource_data(ReportFuns, JsonFun, N-1, binary:part(BinData, ?BinDataSizePerResource, byte_size(BinData) - ?BinDataSizePerResource)).
