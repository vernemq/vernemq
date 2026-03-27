#!/usr/bin/env escript
%%! -noshell

main([RetrySpec]) ->
    case file:consult(RetrySpec) of
        {ok, Terms} ->
            Suites = lists:usort(extract_suites(Terms, [])),
            io:format("~s", [string:join(Suites, ",")]),
            halt(0);
        {error, Reason} ->
            io:format(standard_error, "failed to read ~s: ~s~n", [RetrySpec, file:format_error(Reason)]),
            halt(1)
    end;
main(_) ->
    io:format(standard_error, "usage: retry_suites.escript <retry.spec>~n", []),
    halt(2).

extract_suites([H | T], Acc) ->
    extract_suites(T, extract_suites(H, Acc));
extract_suites([], Acc) ->
    Acc;
extract_suites({cases, Path, CaseList, _}, Acc) ->
    prepend_suites(Path, CaseList, Acc);
extract_suites({cases, Path, CaseList}, Acc) ->
    prepend_suites(Path, CaseList, Acc);
extract_suites(Term, Acc) when is_tuple(Term) ->
    extract_suites(tuple_to_list(Term), Acc);
extract_suites(_, Acc) ->
    Acc.

prepend_suites(Path, CaseList, Acc) when is_list(CaseList) ->
    PathStr = to_list(Path),
    lists:foldl(
        fun(Case, A0) ->
            case to_case_name(Case) of
                undefined ->
                    A0;
                Name ->
                    [filename:join(PathStr, Name) | A0]
            end
        end,
        Acc,
        CaseList
    );
prepend_suites(_, _, Acc) ->
    Acc.

to_case_name(Case) when is_atom(Case) ->
    atom_to_list(Case);
to_case_name(Case) when is_binary(Case) ->
    strip_erl_extension(binary_to_list(Case));
to_case_name(Case) when is_list(Case) ->
    strip_erl_extension(Case);
to_case_name(_) ->
    undefined.

to_list(Value) when is_binary(Value) ->
    binary_to_list(Value);
to_list(Value) when is_list(Value) ->
    Value;
to_list(Value) when is_atom(Value) ->
    atom_to_list(Value);
to_list(_) ->
    "".

strip_erl_extension(Name) ->
    filename:rootname(Name, ".erl").
