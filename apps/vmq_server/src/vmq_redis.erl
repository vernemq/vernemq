-module(vmq_redis).
-author("dhruvjain").

-include("vmq_server.hrl").

%% API
-export([query/4, query/5, pipelined_query/3]).

-define(TIMEOUT, 5000).

-type return_value() :: undefined | binary() | [binary() | nonempty_list() | undefined].

-spec query(atom(), [any()], atom(), atom()) ->
    {ok, return_value()} | {error, Reason :: any()}.
query(Client, QueryCmd, Cmd, Operation) ->
    query(Client, QueryCmd, Cmd, Operation, ?TIMEOUT).

-spec query(atom(), [any()], atom(), atom(), non_neg_integer()) ->
    {ok, return_value()} | {error, Reason :: any()}.
query(Client, QueryCmd, Cmd, Operation, Timeout) ->
    vmq_metrics:incr_redis_cmd({Cmd, Operation}),
    V1 = vmq_util:ts(),
    Pid =
        case Client of
            C when is_pid(Client) -> C;
            Named -> whereis(Named)
        end,
    Result =
        try eredis:q(Pid, QueryCmd, Timeout) of
            {error, <<"ERR stale_request">>} = Res when Cmd == ?FCALL ->
                vmq_metrics:incr_redis_stale_cmd({Cmd, Operation}),
                lager:error("Cannot ~p:~p due to staleness", [Cmd, Operation]),
                Res;
            {error, <<"ERR unauthorized">>} = Res when Cmd == ?FCALL ->
                vmq_metrics:incr_unauth_redis_cmd({Cmd, Operation}),
                lager:error("Cannot ~p:~p as client is connected on different node", [
                    Cmd, Operation
                ]),
                Res;
            {error, no_connection} ->
                vmq_metrics:incr_redis_cmd_err({Cmd, Operation}),
                lager:debug("Cannot ~p:~p due to ~p", [Cmd, Operation, no_connection]),
                {error, no_connection};
            {error, Reason} ->
                vmq_metrics:incr_redis_cmd_err({Cmd, Operation}),
                lager:error("Cannot ~p:~p due to ~p", [Cmd, Operation, Reason]),
                {error, Reason};
            {ok, undefined} ->
                vmq_metrics:incr_redis_cmd_miss({Cmd, Operation}),
                {ok, undefined};
            {ok, []} ->
                vmq_metrics:incr_redis_cmd_miss({Cmd, Operation}),
                {ok, []};
            Res ->
                Res
        catch
            Type:Exception ->
                vmq_metrics:incr_redis_cmd_err({Cmd, Operation}),
                lager:error("Cannot ~p:~p due to ~p:~p", [Cmd, Operation, Type, Exception]),
                {error, Exception}
        end,
    vmq_metrics:pretimed_measurement(
        {redis_cmd, run, [
            {cmd, Cmd},
            {operation, Operation}
        ]},
        vmq_util:ts() - V1
    ),
    Result.

pipelined_query(Client, QueryList, Operation) ->
    [_ | PipelinedCmd] = lists:foldl(
        fun([Cmd | _], Acc) -> "|" ++ atom_to_list(Cmd) ++ Acc end, "", QueryList
    ),

    vmq_metrics:incr_redis_cmd({?PIPELINE, Operation}),
    V1 = vmq_util:ts(),

    Pid =
        case Client of
            C when is_pid(Client) -> C;
            Named -> whereis(Named)
        end,
    Result =
        try eredis:qp(Pid, QueryList) of
            {error, no_connection} ->
                vmq_metrics:incr_redis_cmd_err({?PIPELINE, Operation}),
                lager:debug("No connection with Redis"),
                {error, no_connection};
            Res ->
                IsErrPresent = lists:foldl(
                    fun
                        ({ok, _}, Acc) -> Acc;
                        ({error, _Reason}, _Acc) -> true
                    end,
                    false,
                    Res
                ),
                if
                    IsErrPresent -> vmq_metrics:incr_redis_cmd_err({?PIPELINE, Operation});
                    true -> ok
                end,
                Res
        catch
            Type:Exception ->
                vmq_metrics:incr_redis_cmd_err({?PIPELINE, Operation}),
                lager:error("Cannot ~p:~p due to ~p:~p", [?PIPELINE, Operation, Type, Exception]),
                {error, Exception}
        end,

    vmq_metrics:pretimed_measurement(
        {redis_cmd, run, [{cmd, PipelinedCmd}, {operation, Operation}]}, vmq_util:ts() - V1
    ),
    Result.
