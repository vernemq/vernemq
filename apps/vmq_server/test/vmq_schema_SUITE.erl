-module(vmq_schema_SUITE).

-compile([nowarn_export_all,export_all]).

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
    [].

all() ->
    [proxy_protocol_inheritance_test,
     proxy_protocol_override_test].

proxy_protocol_inheritance_test(_Config) ->
    Conf = [%% required settings the listener translator.
            {["listener","max_connections"], "10000"},
            {["listener","nr_of_acceptors"], "100"},
            %% tcp/mqtt
            {["listener","tcp","proxy_protocol"], "on"},
            {["listener","tcp","default"],"127.0.0.1:1884"},
            %% http
            {["listener","http","proxy_protocol"], "on"},
            {["listener","http","default"],"127.0.0.1:8888"},
            %% websocket
            {["listener","ws","proxy_protocol"], "on"},
            {["listener","ws","default"],"127.0.0.1:800"}],
    true = expect(Conf, [vmq_server, listeners, mqtt,  {{127,0,0,1}, 1884},proxy_protocol]),
    true = expect(Conf, [vmq_server, listeners, http,  {{127,0,0,1}, 8888},proxy_protocol]),
    true = expect(Conf, [vmq_server, listeners, mqttws,{{127,0,0,1}, 800}, proxy_protocol]).

proxy_protocol_override_test(_Config) ->
    Conf = [%% required settings the listener translator.
            {["listener","max_connections"], "10000"},
            {["listener","nr_of_acceptors"], "100"},
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
            {["listener","ws","default","proxy_protocol"], "on"}],
    true = expect(Conf, [vmq_server, listeners, mqtt,  {{127,0,0,1}, 1884},proxy_protocol]),
    true = expect(Conf, [vmq_server, listeners, http,  {{127,0,0,1}, 8888},proxy_protocol]),
    true = expect(Conf, [vmq_server, listeners, mqttws,{{127,0,0,1}, 800}, proxy_protocol]).

expect(Conf, Setting) ->
    Schema = cuttlefish_schema:files([code:priv_dir(vmq_server) ++ "/vmq_server.schema"]),
    Gen = cuttlefish_generator:map(Schema,Conf),
    deep_find(Gen, Setting).

deep_find(Value, []) ->
    Value;
deep_find(Conf, [Prop|T]) ->
    case lists:keyfind(Prop, 1, Conf) of
        false ->
            throw({could_not_find, Prop, in, Conf});
        {Prop, Value} ->
            deep_find(Value, T)
    end.
