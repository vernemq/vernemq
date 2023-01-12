%%%-------------------------------------------------------------------
%%% @copyright (C) 2022, Gojek
%%% @doc
%%%
%%% @end
%%% Created : 07. Jun 2022 12:18 PM
%%%-------------------------------------------------------------------
-module(vmq_util).

%% API
-export([ts/0, timed_measurement/4]).

ts() ->
    {Mega, Sec, Micro} = os:timestamp(),
    (Mega * 1000000 + Sec) * 1000000 + Micro.

timed_measurement({_,_} = Metric, Module, Function, Args) ->
    Ts1 = vmq_util:ts(),
    Ret = apply(Module, Function, Args),
    Ts2 = vmq_util:ts(),
    Val = Ts2 - Ts1,
    vmq_metrics:pretimed_measurement(Metric, Val),
    Ret.
