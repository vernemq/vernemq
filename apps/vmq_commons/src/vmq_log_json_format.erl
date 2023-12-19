%%
%% Based on rabbit_logger_json_fmt and rabbit_logger_fmt_helpers.erl
%%
%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(vmq_log_json_format).

-export([format/2]).
-compile(nowarn_unused_function).
-compile(nowarn_unused_vars).

format(
    #{
        msg := Msg,
        level := Level,
        meta := Meta
    },
    Config
) ->
    FormattedLevel = unicode:characters_to_binary(atom_to_list(Level)),
    FormattedMeta = format_meta(Meta, Config),
    %% We need to call `unicode:characters_to_binary()' here and several other
    %% places because JSON encoder will format a string as a list of
    %% integers (we don't blame it for that, it makes sense).
    FormattedMsg = unicode:characters_to_binary(format_msg(Msg, Meta, Config)),
    InitialDoc0 = FormattedMeta#{
        level => FormattedLevel,
        msg => FormattedMsg
    },
    InitialDoc =
        case level_to_verbosity(Level, Config) of
            undefined -> InitialDoc0;
            Verbosity -> InitialDoc0#{verbosity => Verbosity}
        end,
    DocAfterMapping = apply_mapping_and_ordering(InitialDoc, Config),
    Json = vmq_json:encode(DocAfterMapping),
    [Json, $\n].

level_to_verbosity(Level, #{verbosity_map := Mapping}) ->
    case maps:is_key(Level, Mapping) of
        true -> maps:get(Level, Mapping);
        false -> undefined
    end;
level_to_verbosity(_, _) ->
    undefined.

format_meta(Meta, _Config) ->
    maps:fold(
        fun
            (time, Timestamp, Acc) ->
                FormattedTime0 = Timestamp,
                FormattedTime1 =
                    case is_number(FormattedTime0) of
                        true ->
                            FormattedTime0;
                        false ->
                            unicode:characters_to_binary(
                                FormattedTime0
                            )
                    end,
                Acc#{time => FormattedTime1};
            (domain = Key, Components, Acc) ->
                Term = unicode:characters_to_binary(
                    string:join(
                        [atom_to_list(Cmp) || Cmp <- Components],
                        "."
                    )
                ),
                Acc#{Key => Term};
            (Key, Value, Acc) ->
                case convert_to_types_accepted_by_json_encoder(Value) of
                    false -> Acc;
                    Term -> Acc#{Key => Term}
                end
        end,
        #{},
        Meta
    ).

convert_to_types_accepted_by_json_encoder(Term) when is_map(Term) ->
    maps:map(
        fun(_, Value) -> convert_to_types_accepted_by_json_encoder(Value) end,
        Term
    );
convert_to_types_accepted_by_json_encoder(Term) when is_list(Term) ->
    case io_lib:deep_char_list(Term) of
        true ->
            unicode:characters_to_binary(Term);
        false ->
            [convert_to_types_accepted_by_json_encoder(E) || E <- Term]
    end;
convert_to_types_accepted_by_json_encoder(Term) when is_tuple(Term) ->
    convert_to_types_accepted_by_json_encoder(erlang:tuple_to_list(Term));
convert_to_types_accepted_by_json_encoder(Term) when is_function(Term) ->
    String = erlang:fun_to_list(Term),
    unicode:characters_to_binary(String);
convert_to_types_accepted_by_json_encoder(Term) when is_pid(Term) ->
    String = erlang:pid_to_list(Term),
    unicode:characters_to_binary(String);
convert_to_types_accepted_by_json_encoder(Term) when is_port(Term) ->
    String = erlang:port_to_list(Term),
    unicode:characters_to_binary(String);
convert_to_types_accepted_by_json_encoder(Term) when is_reference(Term) ->
    String = erlang:ref_to_list(Term),
    unicode:characters_to_binary(String);
convert_to_types_accepted_by_json_encoder(Term) ->
    Term.

apply_mapping_and_ordering(Doc, #{field_map := Mapping}) ->
    apply_mapping_and_ordering(Mapping, Doc, []);
apply_mapping_and_ordering(Doc, _) ->
    maps:to_list(Doc).

apply_mapping_and_ordering([{'$REST', false} | Rest], _, Result) ->
    apply_mapping_and_ordering(Rest, #{}, Result);
apply_mapping_and_ordering([{Old, false} | Rest], Doc, Result) when
    is_atom(Old)
->
    Doc1 = maps:remove(Old, Doc),
    apply_mapping_and_ordering(Rest, Doc1, Result);
apply_mapping_and_ordering([{Old, New} | Rest], Doc, Result) when
    is_atom(Old) andalso is_atom(New)
->
    case maps:is_key(Old, Doc) of
        true ->
            Value = maps:get(Old, Doc),
            Doc1 = maps:remove(Old, Doc),
            Result1 = [{New, Value} | Result],
            apply_mapping_and_ordering(Rest, Doc1, Result1);
        false ->
            apply_mapping_and_ordering(Rest, Doc, Result)
    end;
apply_mapping_and_ordering([], Doc, Result) ->
    lists:reverse(Result) ++ maps:to_list(Doc).

format_msg(Msg, Meta, #{single_line := true} = Config) ->
    FormattedMsg = format_msg1(Msg, Meta, Config),
    %% The following behavior is the same as the one in the official
    %% `logger_formatter'; the code is taken from:
    %% https://github.com/erlang/otp/blob/c5ed910098e9c2787e2c3f9f462c84322064e00d/lib/kernel/src/logger_formatter.erl
    FormattedMsg1 = string:strip(FormattedMsg, both),
    re:replace(
        FormattedMsg1,
        ",?\r?\n\s*",
        ", ",
        [{return, list}, global, unicode]
    );
format_msg(Msg, Meta, Config) ->
    format_msg1(Msg, Meta, Config).

format_msg1({string, Chardata}, Meta, Config) ->
    format_msg1({"~ts", [Chardata]}, Meta, Config);
format_msg1({report, Report}, Meta, Config) ->
    FormattedReport = format_report(Report, Meta, Config),
    format_msg1(FormattedReport, Meta, Config);
format_msg1({Format, Args}, _, _) ->
    io_lib:format(Format, Args).

format_report(
    #{label := {application_controller, _}} = Report, Meta, Config
) ->
    format_application_progress(Report, Meta, Config);
format_report(
    #{label := {supervisor, progress}} = Report, Meta, Config
) ->
    format_supervisor_progress(Report, Meta, Config);
format_report(
    Report, #{report_cb := Cb} = Meta, Config
) ->
    try
        case erlang:fun_info(Cb, arity) of
            {arity, 1} -> Cb(Report);
            {arity, 2} -> {"~ts", [Cb(Report, #{})]}
        end
    catch
        _:_:_ ->
            format_report(Report, maps:remove(report_cb, Meta), Config)
    end;
format_report(Report, _, _) ->
    logger:format_report(Report).

format_application_progress(
    #{
        label := {_, progress},
        report := InternalReport
    },
    _,
    _
) ->
    Application = proplists:get_value(application, InternalReport),
    StartedAt = proplists:get_value(started_at, InternalReport),
    {"Application ~w started on ~0p", [Application, StartedAt]};
format_application_progress(
    #{
        label := {_, exit},
        report := InternalReport
    },
    _,
    _
) ->
    Application = proplists:get_value(application, InternalReport),
    Exited = proplists:get_value(exited, InternalReport),
    {"Application ~w exited with reason: ~0p", [Application, Exited]}.

format_supervisor_progress(#{report := InternalReport}, _, _) ->
    Supervisor = proplists:get_value(supervisor, InternalReport),
    Started = proplists:get_value(started, InternalReport),
    Id = proplists:get_value(id, Started),
    Pid = proplists:get_value(pid, Started),
    Mfa = proplists:get_value(mfargs, Started),
    {"Supervisor ~w: child ~w started (~w): ~0p", [Supervisor, Id, Pid, Mfa]}.
