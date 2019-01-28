-module(vmq_dev_api_SUITE).

-compile([nowarn_export_all,export_all]).

-include_lib("vmq_commons/include/vmq_types.hrl").
-include_lib("common_test/include/ct.hrl").

suite() ->
    [{timetrap,{seconds,30}}].

init_per_suite(Config) ->
    cover:start(),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_GroupName, Config) ->
    Config.

end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    ets:new(?MODULE, [named_table, public]),
    vmq_test_utils:setup(),
    enable_on_subscribe(),
    enable_on_publish(),
    vmq_server_cmd:set_config(allow_anonymous, true),
    vmq_server_cmd:set_config(retry_interval, 10),
    vmq_server_cmd:listener_start(1888, [{allowed_protocol_versions, "3,4,5"}]),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ets:delete(?MODULE),
    vmq_test_utils:teardown(),
    ok.

groups() ->
    [].

all() ->
    [reauthorize_works,
     reauthorize_works_m5].

reauthorize_works_m5(_Config) ->
    Connect = packetv5:gen_connect("vmq-reauth-client", [{keepalive,60},{clean_start, false}]),
    Connack = packetv5:gen_connack(?M5_CONNACK_ACCEPT),
    SubTopic1 = packetv5:gen_subtopic(<<"some/reauth/1">>, 0),
    SubTopic2 = packetv5:gen_subtopic(<<"some/reauth/2">>, 0),
    Subscribe = packetv5:gen_subscribe(1, [SubTopic1, SubTopic2], #{}),
    Suback = packetv5:gen_suback(1, [0, 0], #{}),
    {ok, Socket} = packetv5:do_client_connect(Connect, Connack, []),
    ets:insert(?MODULE, {[<<"some">>,<<"reauth">>, <<"1">>]}),
    ets:insert(?MODULE, {[<<"some">>,<<"reauth">>, <<"2">>]}),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packetv5:expect_frame(Socket, Suback),
    Publish = packetv5:gen_publish("some/reauth/1", 0, <<"thepayload">>, [{mid, 2}]),
    ok = gen_tcp:send(Socket, Publish),
    ok = packetv5:expect_frame(Socket, Publish),
    % disabling the subscribe hook, and reauthorizing the client will reject the publish
    ets:delete(?MODULE, [<<"some">>,<<"reauth">>, <<"1">>]),
    Node = node(),
    Changes0 = vernemq_dev_api:reauthorize_subscriptions(undefined, {"", <<"vmq-reauth-client">>}, []),
    {[{Node, [{[<<"some">>, <<"reauth">>, <<"1">>], {0, _SubInfo}}]}], []} = Changes0,
    ok = gen_tcp:send(Socket, Publish),
    {error, timeout} = gen_tcp:recv(Socket, 0, 100),
    gen_tcp:close(Socket).

reauthorize_works(_Config) ->
    Connect = packet:gen_connect("vmq-reauth-client", [{keepalive,60},{clean_session, false}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(1, [{"some/reauth/1", 0}, {"some/reauth/2", 0}]),
    Suback = packet:gen_suback(1, [0, 0]),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ets:insert(?MODULE, {[<<"some">>,<<"reauth">>, <<"1">>]}),
    ets:insert(?MODULE, {[<<"some">>,<<"reauth">>, <<"2">>]}),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    Publish = packet:gen_publish("some/reauth/1", 0, <<"thepayload">>, [{mid, 2}]),
    ok = gen_tcp:send(Socket, Publish),
    ok = packet:expect_packet(Socket, "publish", Publish),
    % disabling the subscribe hook, and reauthorizing the client will reject the publish
    ets:delete(?MODULE, [<<"some">>,<<"reauth">>, <<"1">>]),
    Node = node(),
    Changes0 = vernemq_dev_api:reauthorize_subscriptions(undefined, {"", <<"vmq-reauth-client">>}, []),
    {[{Node, [{[<<"some">>, <<"reauth">>, <<"1">>], 0}]}], []} = Changes0,
    ok = gen_tcp:send(Socket, Publish),
    {error, timeout} = gen_tcp:recv(Socket, 0, 100),
    gen_tcp:close(Socket).

enable_on_subscribe() ->
    ok = vmq_plugin_mgr:enable_module_plugin(
           auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3),
    ok = vmq_plugin_mgr:enable_module_plugin(
           auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3,
           [{compat, {auth_on_subscribe_m5, vmq_plugin_compat_m5,
                      convert, 4}}]).

enable_on_publish() ->
    ok = vmq_plugin_mgr:enable_module_plugin(
           auth_on_publish, ?MODULE, hook_auth_on_publish, 6),
    ok = vmq_plugin_mgr:enable_module_plugin(
           auth_on_publish, ?MODULE, hook_auth_on_publish, 6,
           [{compat, {auth_on_publish_m5, vmq_plugin_compat_m5,
                      convert, 7}}]).

hook_auth_on_subscribe(_, _, Topics) ->
    Verdict =
    lists:foldl(fun(_, false) ->
                        false;
                   ({T, _}, true) ->
                        case ets:lookup(?MODULE, T) of
                            [] ->
                                false;
                            _ ->
                                true
                        end
                end, true, Topics),
    case Verdict of
        true -> ok;
        false -> next
    end.
hook_auth_on_publish(_,_,_,_,_,_) ->
    ok.
