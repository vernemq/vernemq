-module(vmq_schema_SUITE).

-compile([nowarn_export_all,export_all]).
-compile(nowarn_deprecated_function).

-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% @spec suite() -> Info
%% Info = [tuple()]
%% @end
%%--------------------------------------------------------------------
suite() ->
    [{timetrap,{seconds,30}}].

init_per_suite(Config) ->
    application:ensure_all_started(cuttlefish),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_GroupName, Config) ->
    Config.

end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

groups() ->
    Tests =
        [proxy_protocol_inheritance_test,
         proxy_protocol_override_test,
         ssl_certs_opts_inheritance_test,
         ssl_certs_opts_override_test,
         allowed_protocol_versions_inheritance_test,
         allowed_protocol_versions_override_test,
         allowed_eccs_test,
         default_eccs_test,
         invalid_eccs_test
        ],
    [{schema, [parallel], Tests}].

all() ->
    [{group, schema}].

global_substitutions() ->
    [{["metadata_plugin"], "vmq_swc"},
     {["listener", "max_connections"], "10000"},
     {["listener", "nr_of_acceptors"], "100"}].


dummy_file(Config, Name) ->
    DataFolder = ?config(data_dir, Config),
    filename:join([DataFolder, Name]).

ssl_certs_opts_inheritance_test(Config) ->
    DummyCert = dummy_file(Config, "dummy.crt"),
    ConfFun =
        fun(LType) ->
                Base =
                [
                 {["listener", LType, "certfile"], DummyCert},
                 {["listener", LType, "cafile"], DummyCert},
                 {["listener", LType, "keyfile"], DummyCert},
                 {["listener", LType, "depth"], 10},
                 {["listener", LType, "ciphers"], "ciphers"},
                 {["listener", LType, "crlfile"], DummyCert},
                 {["listener", LType, "require_certificate"], "on"},

                 {["listener", LType, "tls_version"], "tlsv1.1"},
                 {["listener", LType, "default"], "127.0.0.1:1234"}
                 | global_substitutions()
                ],
                case LType of
                    "https" ->
                        Base;
                    _ ->
                        [{["listener", LType, "mountpoint"], "mpval"} | Base]
                end
        end,
    TestFun =
        fun(Conf, IntName) ->
                DummyCert = expect(Conf, [vmq_server, listeners, IntName,  {{127,0,0,1}, 1234}, certfile]),
                DummyCert = expect(Conf, [vmq_server, listeners, IntName,  {{127,0,0,1}, 1234}, cafile]),
                DummyCert = expect(Conf, [vmq_server, listeners, IntName,  {{127,0,0,1}, 1234}, keyfile]),
                10        = expect(Conf, [vmq_server, listeners, IntName,  {{127,0,0,1}, 1234}, depth]),
                case IntName of
                    https -> skip;
                    _ ->
                        "mpval"   = expect(Conf, [vmq_server, listeners, IntName,  {{127,0,0,1}, 1234}, mountpoint])
                end,
                "ciphers" = expect(Conf, [vmq_server, listeners, IntName,  {{127,0,0,1}, 1234}, ciphers]),
                DummyCert = expect(Conf, [vmq_server, listeners, IntName,  {{127,0,0,1}, 1234}, crlfile]),
                true      = expect(Conf, [vmq_server, listeners, IntName,  {{127,0,0,1}, 1234}, require_certificate]),
                'tlsv1.1' = expect(Conf, [vmq_server, listeners, IntName,  {{127,0,0,1}, 1234}, tls_version])
        end,

    lists:foreach(
      fun({ConfName, IntName} = L) ->
              try
                  TestFun(ConfFun(ConfName), IntName)
              catch C:E:ST ->
                      ct:pal("Exception while running: ~p~n~p", [L, {C,E, ST}]),
                      throw(E)
              end
      end,
      [
       {"vmqs", vmqs},
       {"ssl", mqtts},
       {"wss", mqttwss},
       {"https", https}
      ]).

ssl_certs_opts_override_test(Config) ->
    DummyCert = dummy_file(Config, "dummy.crt"),
    DummyCertOverride = dummy_file(Config, "dummy_override.crt"),
    ConfFun =
        fun(LType) ->
                Base = [
                 %% protocol defaults
                 {["listener", LType, "certfile"], DummyCert},
                 {["listener", LType, "cafile"], DummyCert},
                 {["listener", LType, "keyfile"], DummyCert},
                 {["listener", LType, "depth"], 10},
                 {["listener", LType, "ciphers"], "ciphers"},
                 {["listener", LType, "crlfile"], DummyCert},
                 {["listener", LType, "require_certificate"], "on"},
                 {["listener", LType, "tls_version"], "tlsv1.1"},

                 %% listener overrides
                 {["listener", LType, "mylistener", "certfile"], DummyCertOverride},
                 {["listener", LType, "mylistener", "cafile"], DummyCertOverride},
                 {["listener", LType, "mylistener", "keyfile"], DummyCertOverride},
                 {["listener", LType, "mylistener", "depth"], 20},
                 {["listener", LType, "mylistener", "ciphers"], "overridden"},
                 {["listener", LType, "mylistener", "crlfile"], DummyCertOverride},
                 {["listener", LType, "mylistener", "require_certificate"], "off"},
                 {["listener", LType, "mylistener", "tls_version"], "tlsv1.2"},

                 {["listener", LType, "mylistener"], "127.0.0.1:1234"}
                 | global_substitutions()
                ],
                case LType of
                    "https" ->
                        Base;
                    _ ->
                        [{["listener", LType, "mountpoint"], "mpval"},
                         {["listener", LType, "mylistener", "mountpoint"], "overriden"}
                         | Base]
                end

        end,
    TestFun =
        fun(Conf, IntName) ->
                DummyCertOverride = expect(Conf, [vmq_server, listeners, IntName,  {{127,0,0,1}, 1234}, certfile]),
                DummyCertOverride = expect(Conf, [vmq_server, listeners, IntName,  {{127,0,0,1}, 1234}, cafile]),
                DummyCertOverride = expect(Conf, [vmq_server, listeners, IntName,  {{127,0,0,1}, 1234}, keyfile]),
                20                = expect(Conf, [vmq_server, listeners, IntName,  {{127,0,0,1}, 1234}, depth]),
                "overridden"      = expect(Conf, [vmq_server, listeners, IntName,  {{127,0,0,1}, 1234}, ciphers]),
                DummyCertOverride = expect(Conf, [vmq_server, listeners, IntName,  {{127,0,0,1}, 1234}, crlfile]),
                false             = expect(Conf, [vmq_server, listeners, IntName,  {{127,0,0,1}, 1234}, require_certificate]),
                'tlsv1.2'         = expect(Conf, [vmq_server, listeners, IntName,  {{127,0,0,1}, 1234}, tls_version]),
                case IntName of
                    https -> skip;
                    _ ->
                        "overriden"   = expect(Conf, [vmq_server, listeners, IntName,  {{127,0,0,1}, 1234}, mountpoint])
                end
        end,

    lists:foreach(
      fun({ConfName, IntName} = L) ->
              try
                  TestFun(ConfFun(ConfName), IntName)
              catch C:E ->
                      ct:pal("Exception while running: ~p~n~p", [L, {C,E, erlang:get_stacktrace()}]),
                      throw(E)
              end
      end,
      [
       {"vmqs", vmqs},
       {"ssl", mqtts},
       {"wss", mqttwss},
       {"https", https}
      ]).


proxy_protocol_inheritance_test(_Config) ->
    Conf = [
            %% tcp/mqtt
            {["listener","tcp","proxy_protocol"], "on"},
            {["listener","tcp","default"],"127.0.0.1:1884"},
            %% http
            {["listener","http","proxy_protocol"], "on"},
            {["listener","http","default"],"127.0.0.1:8888"},
            %% websocket
            {["listener","ws","proxy_protocol"], "on"},
            {["listener","ws","default"],"127.0.0.1:800"}
            | global_substitutions()
           ],
    true = expect(Conf, [vmq_server, listeners, mqtt,  {{127,0,0,1}, 1884},proxy_protocol]),
    true = expect(Conf, [vmq_server, listeners, http,  {{127,0,0,1}, 8888},proxy_protocol]),
    true = expect(Conf, [vmq_server, listeners, mqttws,{{127,0,0,1}, 800}, proxy_protocol]).

proxy_protocol_override_test(_Config) ->
    Conf = [
            %% tcp/mqtt
            {["listener","tcp","proxy_protocol"], "off"},
            {["listener","tcp","default"],"127.0.0.1:1884"},
            {["listener","tcp","default","proxy_protocol"], "on"},
            %% http
            {["listener","http","proxy_protocol"], "off"},
            {["listener","http","default"],"127.0.0.1:8888"},
            {["listener","http","default","proxy_protocol"], "on"},
            %% websocket
            {["listener","ws","proxy_protocol"], "off"},
            {["listener","ws","default"],"127.0.0.1:800"},
            {["listener","ws","default","proxy_protocol"], "on"}
            | global_substitutions()
           ],
    true = expect(Conf, [vmq_server, listeners, mqtt,  {{127,0,0,1}, 1884},proxy_protocol]),
    true = expect(Conf, [vmq_server, listeners, http,  {{127,0,0,1}, 8888},proxy_protocol]),
    true = expect(Conf, [vmq_server, listeners, mqttws,{{127,0,0,1}, 800}, proxy_protocol]).

allowed_protocol_versions_inheritance_test(_Config) ->
    Conf = [
            %% tcp/mqtt
            {["listener","tcp","allowed_protocol_versions"], "3,4,5"},
            {["listener","tcp","default"],"127.0.0.1:1884"},
            %% tcp/ssl/mqtt
            {["listener","ssl","allowed_protocol_versions"], "3,4,5"},
            {["listener","ssl","default"],"127.0.0.1:8884"},
            %% websocket
            {["listener","ws","allowed_protocol_versions"], "3,4,5"},
            {["listener","ws","default"],"127.0.0.1:800"},
            %% websocket/ssl
            {["listener","wss","allowed_protocol_versions"], "3,4,5"},
            {["listener","wss","default"],"127.0.0.1:900"}
            | global_substitutions()
           ],
    [3,4,5] = expect(Conf, [vmq_server, listeners, mqtt,  {{127,0,0,1}, 1884},allowed_protocol_versions]),
    [3,4,5] = expect(Conf, [vmq_server, listeners, mqtts,  {{127,0,0,1}, 8884},allowed_protocol_versions]),
    [3,4,5] = expect(Conf, [vmq_server, listeners, mqttws,{{127,0,0,1}, 800}, allowed_protocol_versions]),
    [3,4,5] = expect(Conf, [vmq_server, listeners, mqttwss,{{127,0,0,1}, 900}, allowed_protocol_versions]).

allowed_eccs_test(_Config) ->
    Conf = [
            {["listener","ssl","default", "eccs"], "sect163r1,sect163r2,secp160k1,secp160r1"},
            {["listener","ssl","default"],"127.0.0.1:8884"}
            | global_substitutions()
           ],
    ExpectedECCs = lists:usort([sect163r1,sect163r2,secp160k1,secp160r1]),
    ExpectedECCs = expect(Conf, [vmq_server, listeners, mqtts, {{127,0,0,1}, 8884}, eccs]).

default_eccs_test(_Config) ->
    Conf = [
            %% tcp/ssl/mqtt
            {["listener","ssl","default"],"127.0.0.1:8884"}
            | global_substitutions()
           ],
    KnownECCs = ssl:eccs(),
    KnownECCs = expect(Conf, [vmq_server, listeners, mqtts, {{127,0,0,1}, 8884}, eccs]).

invalid_eccs_test(_Config) ->
    Conf = [
            %% tcp/ssl/mqtt
            {["listener","ssl","default","eccs"], "[sect163r1,sect163r2,wrong,secp160r1]"},
            {["listener","ssl","default"],"127.0.0.1:8884"}
            | global_substitutions()
           ],
    case catch expect(Conf, [vmq_server, listeners, mqtts, {{127,0,0,1}, 8884}, eccs]) of
        {{error,apply_translations,{errorlist,[{error,{translation_invalid_configuration,{"vmq_server.listeners","Unknown ECC named curves: wrong"}}}]}},_} -> ok;
        _ -> ct:fail("Did not receive exception for invalid named curve")
    end.

allowed_protocol_versions_override_test(_Config) ->
    Conf = [
            %% tcp/mqtt
            {["listener","tcp","allowed_protocol_versions"], "3,4"},
            {["listener","tcp","default"],"127.0.0.1:1884"},
            {["listener","tcp","default","allowed_protocol_versions"], "4"},
            %% tcp/ssl/mqtt
            {["listener","ssl","allowed_protocol_versions"], "3,4"},
            {["listener","ssl","default"],"127.0.0.1:8884"},
            {["listener","ssl","default","allowed_protocol_versions"], "4"},
            %% websocket
            {["listener","ws","allowed_protocol_versions"], "3,4"},
            {["listener","ws","default"],"127.0.0.1:800"},
            {["listener","ws","default","allowed_protocol_versions"], "4"},
            %% websocket/ssl
            {["listener","wss","allowed_protocol_versions"], "3,4"},
            {["listener","wss","default"],"127.0.0.1:900"},
            {["listener","wss","default","allowed_protocol_versions"], "4"}
            | global_substitutions()
           ],
    [4] = expect(Conf, [vmq_server, listeners, mqtt, {{127,0,0,1}, 1884}, allowed_protocol_versions]),
    [4] = expect(Conf, [vmq_server, listeners, mqttws, {{127,0,0,1}, 800}, allowed_protocol_versions]),
    [4] = expect(Conf, [vmq_server, listeners, mqttws,{{127,0,0,1}, 800}, allowed_protocol_versions]),
    [4] = expect(Conf, [vmq_server, listeners, mqttwss,{{127,0,0,1}, 900}, allowed_protocol_versions]).


-define(stacktrace, try throw(foo) catch foo -> erlang:get_stacktrace() end).

expect(Conf, Setting) ->
    Schema = cuttlefish_schema:files([code:priv_dir(vmq_server) ++ "/vmq_server.schema"]),
    case cuttlefish_generator:map(Schema,Conf) of
        {error, _, _} = E ->
            StackTrace = ?stacktrace,
            throw({E, StackTrace});
        Gen ->
            deep_find(Gen, Setting)
    end.

deep_find(Value, []) ->
    Value;
deep_find(Conf, [Prop|T]) ->
    case lists:keyfind(Prop, 1, Conf) of
        false ->
            StackTrace = ?stacktrace,
            throw({could_not_find, Prop, in, Conf, StackTrace});
        {Prop, Value} ->
            deep_find(Value, T)
    end.
