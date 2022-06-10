-module(vmq_schema_util).

-export([file_exists/1, file_is_readable/1, parse_list/1]).

-include_lib("kernel/include/file.hrl").

file_exists(Filename) ->
    case file:read_file_info(Filename) of
        {error, enonent} ->
            false;
        _ -> true
    end.

file_is_readable(Filename) ->
    case file:read_file_info(Filename) of
        {ok, #file_info{access=Access}} when Access =:= read; Access =:= read_write ->
            %% we can read the file
            true;
        _ ->
            false
    end.

-spec parse_list(string()) -> list().
parse_list(S) ->
    {ok, Ts, _} = erl_scan:string(S),
    {ok, Result} = erl_parse:parse_term(Ts ++ [{dot,1} || element(1, lists:last(Ts)) =/= dot]),
    Result.
