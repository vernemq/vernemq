%%%-------------------------------------------------------------------
%%% @copyright (C) 2022, Gojek
%%% @doc
%%%
%%% @end
%%% Created : 07. Jun 2022 12:18 PM
%%%-------------------------------------------------------------------
-module(vmq_util).

%% API
-export([
    ts/0,
    timed_measurement/4,
    set_interval/2,
    extract_version/1
]).

ts() ->
    {Mega, Sec, Micro} = os:timestamp(),
    (Mega * 1000000 + Sec) * 1000000 + Micro.

timed_measurement({_, _} = Metric, Module, Function, Args) ->
    Ts1 = vmq_util:ts(),
    Ret = apply(Module, Function, Args),
    Ts2 = vmq_util:ts(),
    Val = Ts2 - Ts1,
    vmq_metrics:pretimed_measurement(Metric, Val),
    Ret.

-spec set_interval(Interval :: integer(), Pid :: pid()) ->
    {integer(), reference()} | {0, undefined}.
set_interval(Interval, Pid) ->
    case Interval of
        0 ->
            {0, undefined};
        I ->
            IinMs = abs(I * 1000),
            NTRef = erlang:send_after(IinMs, Pid, reload),
            {IinMs, NTRef}
    end.

-spec extract_version(BinaryData :: binary()) -> string() | nomatch | {error, any()}.
extract_version(File) ->
    case file:read_file(File) of
        {ok, BinaryData} when byte_size(BinaryData) > 0 ->
            Line = hd(string:tokens(binary_to_list(BinaryData), "\n")),
            case re:run(Line, "#\\s*(v\\d+\\.\\d+\\.\\d+)", [{capture, [1], list}]) of
                {match, [Version]} -> Version;
                nomatch -> nomatch
            end;
        {ok, _} ->
            nomatch;
        {error, Reason} ->
            {error, Reason}
    end.
