%% Copyright 2018-2024 Octavo Labs/VerneMQ (https://vernemq.com/)
%% and Individual Contributors.
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
%% decode_cert and related based on code by Marc Worrell, Maas-Maarten Zeeman
%% https://github.com/zotonic/zotonic_ssl/

-module(vmq_ssl_cli).
-include_lib("public_key/include/public_key.hrl").
-export([
    register_cli/0,
    process_cert_files/1
]).

register_cli() ->
    clique:register_usage(["vmq-admin", "tls"], tls_usage()),
    clique:register_usage(["vmq-admin", "tls", "show"], tls_show_usage()),
    clique:register_usage(["vmq-admin", "tls", "check"], tls_check_usage()),
    clique:register_usage(["vmq-admin", "tls", "clear-pem-cache"], tls_clear_pem_cache_usage()),

    vmq_mgmt_tls_show_cmd(),
    vmq_mgmt_tls_clear_pem_cache_cmd(),
    vmq_mgmt_tls_check_cmd().

tls_usage() ->
    [
        "vmq-admin tls <sub-command>\n\n",
        "  Interact with the TLS subsystem.\n\n",
        "  Sub-commands:\n",
        "    show                 Show TLS certificates.\n"
        "    check                Check certificates validity.\n"
        "    clear-pem-cache      Cleares cached certifactes and forces reload.\n"
    ].

tls_clear_pem_cache_usage() ->
    [
        "vmq-admin tls clear-pem-cache\n\n",
        "  Cleares cached certifactes and forces reload.\n\n"
    ].

tls_show_usage() ->
    [
        "vmq-admin tls show\n\n",
        "  Show information about TLS (PEM) certificates.\n\n"
    ].

tls_check_usage() ->
    [
        "vmq-admin tls check\n\n"
        "  Flags: \n"
        "    --validity=Seconds    Check if certificates expire before n Seconds (default 2 days)\n"
        "  Show information about expired or not yet valid certificates.\n\n"
    ].

extract_cert_file([], Acc) ->
    Acc;
extract_cert_file([{_, _, _, _, _, _, _, _, _, [], _, _, _, _, _, _} | Rest], Acc) ->
    extract_cert_file(Rest, Acc);
extract_cert_file([{_, _, _, _, _, _, _, _, _, Element, _, _, _, _, _, _} | Rest], Acc) ->
    extract_cert_file(Rest, [Element | Acc]).

process_cert_files([]) ->
    [];
process_cert_files(FileNames) ->
    lists:map(
        fun(FileName) ->
            {ok, CertData} = decode_cert(FileName),
            transform_cert_data(FileName, CertData)
        end,
        FileNames
    ).

process_cert_files_and_check(FileNames, SecondsIn) ->
    lists:map(
        fun(FileName) ->
            {ok, CertData} = decode_cert(FileName),
            A1 = transform_cert_data_exp(FileName, CertData),
            V1 = proplists:get_value('SecExp', A1),
            VDT =
                (V1 < erlang:list_to_integer(SecondsIn)) or
                    (proplists:get_value('SecToVal', A1) > 0),
            case VDT of
                true -> A1;
                _ -> []
            end
        end,
        FileNames
    ).

transform_cert_data_exp(FileName, CertData) ->
    [
        {'File', list_to_binary(FileName)},
        {'Not_Before', to_date(proplists:get_value(not_before, CertData))},
        {'Not_After', to_date(proplists:get_value(not_after, CertData))},
        {'SecExp', sec_to_exp(proplists:get_value(not_after, CertData))},
        {'SecToVal', sec_to_exp(proplists:get_value(not_before, CertData))}
    ].

transform_cert_data(FileName, CertData) ->
    [
        {'File', list_to_binary(FileName)},
        {'Not_Before', to_date(proplists:get_value(not_before, CertData))},
        {'Not_After', to_date(proplists:get_value(not_after, CertData))},
        {'CN', proplists:get_value(common_name, CertData)},
        {'Issuer', proplists:get_value(issuer, CertData)},
        {'SAN', proplists:get_value(subject_alt_names, CertData)}
    ].

sec_to_exp(Date) ->
    DT1 = calendar:datetime_to_gregorian_seconds(calendar:now_to_datetime(erlang:timestamp())),
    DT2 = calendar:datetime_to_gregorian_seconds(Date),
    DT2 - DT1.

to_date({{Year, Month, Day}, {Hour, Minute, Second}}) ->
    io_lib:format(
        "~4w-~2..0w-~2..0w~s~2..0w:~2..0w:~2..0w",
        [Year, Month, Day, ' ', Hour, Minute, Second]
    ).

vmq_mgmt_tls_show_cmd() ->
    Cmd = ["vmq-admin", "tls", "show"],
    KeySpecs = [],
    FlagSpecs = [],
    Callback = fun(_, _, _) ->
        NodeTable = process_cert_files(extract_cert_file(vmq_ranch_config:listeners(with_tls), [])),
        [clique_status:table(NodeTable)]
    end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

vmq_mgmt_tls_check_cmd() ->
    Cmd = ["vmq-admin", "tls", "check"],
    KeySpecs = [],
    FlagSpecs = [
        {secondsValid, [
            {shortname, "v"},
            {longname, "validity"},
            {typecast, fun(VS) -> VS end}
        ]}
    ],
    Callback = fun(_, _, Flags) ->
        VS = proplists:get_value(secondsValid, Flags, "172800"),
        NodeTable = process_cert_files_and_check(
            extract_cert_file(vmq_ranch_config:listeners(with_tls), []), VS
        ),
        case NodeTable of
            [[]] -> [clique_status:text("All fine")];
            _ -> [clique_status:table(NodeTable)]
        end
    end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

vmq_mgmt_tls_clear_pem_cache_cmd() ->
    Cmd = ["vmq-admin", "tls", "clear-pem-cache"],
    KeySpecs = [],
    FlagSpecs = [],
    Callback = fun(_, _, _) ->
        ok = ssl:clear_pem_cache()
    end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

decode_cert(CertFile) ->
    decode_cert_data(file:read_file(CertFile)).

decode_cert_data({ok, CertData}) ->
    PemEntries = public_key:pem_decode(CertData),
    case public_key:pem_entry_decode(hd(PemEntries)) of
        {'Certificate', #'TBSCertificate'{} = TBS, _, _} ->
            #'Validity'{notAfter = NotAfter} = TBS#'TBSCertificate'.validity,
            #'Validity'{notBefore = NotBefore} = TBS#'TBSCertificate'.validity,
            Subject = decode_rdnSequence(TBS#'TBSCertificate'.subject),
            Issuer = decode_rdnSequence(TBS#'TBSCertificate'.issuer),
            SANs = decode_sans(TBS#'TBSCertificate'.extensions),
            {ok, [
                {not_after, decode_time(NotAfter)},
                {not_before, decode_time(NotBefore)},
                {issuer, maps:get(cn, Issuer, undefined)},
                {common_name, maps:get(cn, Subject, undefined)},
                {subject_alt_names, SANs}
            ]};
        _ ->
            {error, not_a_certificate}
    end;
decode_cert_data({error, _} = Error) ->
    Error.
decode_time({utcTime, [Y1, Y2, _M1, _M2, _D1, _D2, _H1, _H2, _M3, _M4, _S1, _S2, $Z] = T}) ->
    case list_to_integer([Y1, Y2]) of
        N when N >= 50 ->
            decode_time({generalTime, [$1, $9 | T]});
        _ ->
            decode_time({generalTime, [$2, $0 | T]})
    end;
decode_time({_, [Y1, Y2, Y3, Y4, M1, M2, D1, D2, H1, H2, M3, M4, S1, S2, $Z]}) ->
    Year = list_to_integer([Y1, Y2, Y3, Y4]),
    Month = list_to_integer([M1, M2]),
    Day = list_to_integer([D1, D2]),
    Hour = list_to_integer([H1, H2]),
    Min = list_to_integer([M3, M4]),
    Sec = list_to_integer([S1, S2]),
    {{Year, Month, Day}, {Hour, Min, Sec}}.

decode_rdnSequence({rdnSequence, _} = R) ->
    {rdnSequence, List} = pubkey_cert_records:transform(R, decode),
    lists:foldl(
        fun
            (#'AttributeTypeAndValue'{type = ?'id-at-commonName', value = CN}, Acc) ->
                Acc#{cn => decode_value(CN)};
            (_, Acc) ->
                Acc
        end,
        #{},
        lists:flatten(List)
    ).

decode_sans(asn1_NOVALUE) ->
    [];
decode_sans([]) ->
    [];
decode_sans([#'Extension'{extnID = ?'id-ce-subjectAltName', extnValue = V} | _]) ->
    case 'OTP-PUB-KEY':decode('SubjectAltName', iolist_to_binary(V)) of
        {ok, Vs} -> lists:map(fun decode_value/1, Vs);
        _ -> []
    end;
decode_sans([_ | Exts]) ->
    decode_sans(Exts).

decode_value({dNSName, Name}) -> iolist_to_binary(Name);
decode_value({printableString, P}) -> iolist_to_binary(P);
decode_value({utf8String, B}) -> B.
