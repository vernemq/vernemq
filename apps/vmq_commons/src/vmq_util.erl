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
            Lines = string:tokens(binary_to_list(BinaryData), "\n"),
            case Lines of
                % If Lines is empty, return nomatch
                [] ->
                    nomatch;
                [FirstLine | _Rest] ->
                    case re:run(FirstLine, "#\\s*(v\\d+\\.\\d+\\.\\d+)", [{capture, [1], list}]) of
                        {match, [Version]} ->
                            Version;
                        nomatch ->
                            nomatch
                    end
            end;
        {ok, _} ->
            nomatch;
        {error, Reason} ->
            {error, Reason}
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(setup(F), {setup, fun setup/0, fun teardown/1, F}).

extract_version_test_() ->
    [
        {"Extract config version", ?setup(fun extract_version_test/1)}
    ].

setup() ->
    %% Create a test file with some content
    {ok, File} = file:open("test_file.acl", [write]),
    file:write(File, "# v1.2.3\nSome text\n"),
    file:close(File),
    {ok, NoVersionFile} = file:open("no_version_file.acl", [write]),
    file:write(NoVersionFile, "Some text\nRandom text\n"),
    file:close(NoVersionFile),
    {ok, EmptyFile} = file:open("empty_test_file.acl", [write]),
    file:close(EmptyFile),
    {ok, NewLineFile} = file:open("new_line_test_file.acl", [write]),
    file:write(NewLineFile, "\n"),
    file:close(NewLineFile).

teardown(_) ->
    file:delete("test_file.acl"),
    file:delete("no_version_file.acl"),
    file:delete("empty_test_file.acl"),
    file:delete("new_line_test_file.acl").

extract_version_test(_) ->
    [
        ?_assertEqual("v1.2.3", extract_version("test_file.acl")),
        ?_assertEqual(nomatch, extract_version("no_version_file.acl")),
        ?_assertEqual(nomatch, extract_version("empty_test_file.acl")),
        ?_assertEqual(nomatch, extract_version("new_line_test_file.acl"))
    ].
-endif.
