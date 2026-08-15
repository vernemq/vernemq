-module(vmq_global_schema_SUITE).

-compile([nowarn_export_all, export_all]).
-compile(nowarn_deprecated_function).

-include_lib("common_test/include/ct.hrl").

suite() ->
    [{timetrap, {seconds, 30}}].

init_per_suite(Config) ->
    application:ensure_all_started(cuttlefish),
    Config.

end_per_suite(_Config) ->
    ok.

all() ->
    [syslog_remote_test].

syslog_remote_test(Config) ->
    CAFile = dummy_file(Config, "syslog-ca.crt"),
    rfc3164 = expect(
        [{["log", "syslog"], "on"}],
        [syslog, protocol]),
    BaseConf = [
        {["log", "syslog"], "on"},
        {["log", "syslog", "remote", "host"], "syslog.example.com"},
        {["log", "syslog", "remote", "port"], "6514"},
        {["log", "syslog", "facility"], "local0"}
    ],
    {rfc3164, udp} = expect(
        [{["log", "syslog", "remote", "protocol"], "udp"} | BaseConf],
        [syslog, protocol]),
    {rfc5424, udp} = expect(
        [{["log", "syslog", "format"], "rfc5424"} | BaseConf],
        [syslog, protocol]),
    {rfc5424, tcp} = expect(
        [{["log", "syslog", "format"], "rfc5424"},
         {["log", "syslog", "remote", "protocol"], "tcp"} | BaseConf],
        [syslog, protocol]),
    {rfc5424, tls, [{verify, verify_peer}, {cacertfile, CAFile}]} = expect(
        [{["log", "syslog", "format"], "rfc5424"},
         {["log", "syslog", "remote", "protocol"], "tls"},
         {["log", "syslog", "remote", "tls", "cafile"], CAFile} | BaseConf],
        [syslog, protocol]),
    case catch expect(
        [{["log", "syslog", "remote", "protocol"], "tls"},
         {["log", "syslog", "remote", "tls", "cafile"], CAFile} | BaseConf],
        [syslog, protocol]) of
        {{error, apply_translations, _}, _} -> ok;
        _ -> ct:fail("Expected TLS SysLog to require rfc5424 format")
    end,
    "vernemq" = expect(
        [{["log", "syslog", "app_name"], "vernemq"} | BaseConf],
        [syslog, app_name]),
    "syslog.example.com" = expect(BaseConf, [syslog, dest_host]),
    6514 = expect(BaseConf, [syslog, dest_port]),
    local0 = expect(BaseConf, [syslog, facility]).

dummy_file(Config, Name) ->
    DataFolder = ?config(data_dir, Config),
    filename:join([DataFolder, Name]).

-define(stacktrace, try throw(foo) catch _:foo:Stacktrace -> Stacktrace end).

expect(Conf, Setting) ->
    Schema = cuttlefish_schema:files([root_schema_file()]),
    case cuttlefish_generator:map(Schema, Conf ++ [{["log", "console"], "off"}]) of
        {error, _, _} = Error ->
            StackTrace = ?stacktrace,
            throw({Error, StackTrace});
        Generated ->
            deep_find(Generated, Setting)
    end.

root_schema_file() ->
    filename:join([code:lib_dir(vmq_server), "..", "..", "..", "..", "files", "vmq.schema"]).

deep_find(Value, []) ->
    Value;
deep_find(Conf, [Property | Rest]) ->
    case lists:keyfind(Property, 1, Conf) of
        false ->
            StackTrace = ?stacktrace,
            throw({could_not_find, Property, in, Conf, StackTrace});
        {Property, Value} ->
            deep_find(Value, Rest)
    end.