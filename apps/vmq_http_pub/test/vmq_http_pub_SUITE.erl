-module(vmq_http_pub_SUITE).

-export([
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2,
    all/0
]).

-export([
    test_payload_push/1,
    test_header_push/1,
    test_header_qos/1,
    test_header_topic/1,
    test_header_retain/1,
    test_header_user_props/1,
    test_uee_push/1,
    test_uee_qos/1,
    test_mp_overload/1,
    test_payload_push_endoding/1,
    test_auth_conf_1/1,
    test_no_auth/1,
    test_payload_push_obo/1,
    test_payload_change_mp_reg/1,
    test_payload_change_mp_pub/1,
    test_payload_change_qos_pub/1,
    test_payload_change_topic_pub/1,
    test_max_message_size/1,
    test_payload_change_mp_predef_pub/1
]).

init_per_suite(Config) ->
    {ok, StartedApps} = application:ensure_all_started(vmq_server),
    application:set_env(vmq_server, ignore_db_config, true),
    {ok, StartedApps2} = application:ensure_all_started(cowboy),

    application:load(vmq_plugin),
    application:load(vmq_http_pub),
    {ok, StartedApps3} = application:ensure_all_started(vmq_plugin),
    ok = vmq_plugin_mgr:enable_plugin(vmq_diversity),
    ok = vmq_plugin_mgr:enable_plugin(vmq_http_pub),

    application:set_env(vmq_server, http_modules_auth, #{vmq_http_pub => "noauth"}),
    application:set_env(vmq_http_pub, config, [{mqttauth, "testMode"}]),

    vmq_server_cmd:listener_start(38908, [{http, true},
        {config_mod, vmq_http_pub},
        {config_fun, routes}]),
    {ok, StartedApps4}  = application:ensure_all_started(inets),
    {ok, _} = vmq_diversity:load_script(code:lib_dir(vmq_http_pub) ++ "/test/test_auth.lua"),
    cover:start(),
    [{started_apps, StartedApps ++ StartedApps2 ++ StartedApps3 ++ StartedApps4}] ++ Config.

end_per_suite(Config) ->
    vmq_server_cmd:listener_stop(38908, "127.0.0.1", false),
    ok = vmq_plugin_mgr:disable_plugin(vmq_diversity),
    ok = vmq_plugin_mgr:disable_plugin(vmq_http_pub),
    vmq_server:stop(no_wait),
    application:unload(vmq_server),

    [ application:stop(App) || App <- proplists:get_value(started_apps, Config, []) ],
     Config.

init_per_testcase(_Case, Config) ->
    vmq_config:set_env(allow_anonymous, true, false),
    vmq_config:configure_node(),
    Config.

end_per_testcase(_, Config) ->
    Config.

all() ->
    [test_payload_push, test_header_push, test_header_qos, test_header_topic, test_header_retain, test_header_user_props,
        test_uee_push,test_uee_qos, test_payload_push_endoding, test_mp_overload, test_auth_conf_1, test_no_auth, test_payload_push_obo,
        test_payload_change_mp_reg, test_payload_change_mp_pub, test_payload_change_qos_pub, test_payload_change_topic_pub,
        test_payload_change_mp_predef_pub, test_max_message_size] .

% The test suite is currenty solely relying on the HTTP response. It would be nice to test if the
% payload is actualy deliverd by registering a listener.

json_body() ->
  <<"{\"topic\": \"T1\", \"qos\": 1, \"retain\": false, \"payload\": \"asddsadsadas22dasasdsad\", \"user_properties\": [{\"a\":\"b\"}]}">>.

json_body_diversity(User, ClientId) ->
    P1 = <<"{\"client_id\": \"">>,
    P2 = <<"\", \"user\": \"">>,
    P3 = <<"\", \"password\":\"test-password\", \"topic\": \"a/b/c\", \"qos\": 1, \"retain\": false, \"payload\": \"asddsadsadas22dasasdsad\", \"user_properties\": [{\"a\":\"b\"}]}">>,
   <<P1/binary,ClientId/binary,P2/binary,User/binary,P3/binary>>.

json_body_base64() ->
    <<"{\"topic\": \"T1\", \"qos\": 1, \"retain\": false, \"payload\": \"aGFsbG8gd2VsdA==\", \"user_properties\": [{\"a\":\"b\"}]}">>.

binary_body() ->
    <<"sdjfjghdzuizzztretr2223312sdlkfdsajdfljdsflj">>.

url() ->
    "http://localhost:38908/restmqtt/api/v1/publish".

mock_request_http() ->
    #{bindings => #{},body_length => 105,cert => undefined,
        charset => undefined,has_body => true,
        host => <<"localhost">>,host_info => undefined,
        method => <<"PUT">>,path => <<"/restmqtt/api/v1/publish">>,
        path_info => undefined,
        peer => {{127,0,0,1},53264},
        port => 38908,qs => <<"encoding=base64">>,
        ref => {{127,0,0,1},38908},
        scheme => <<"http">>,
        sock => {{127,0,0,1},38908},
        streamid => 1,version => 'HTTP/1.1'}.

allow_anon(Anon) ->
    vmq_config:set_env(allow_anonymous, Anon, false),
    vmq_config:configure_node().


test_payload_push(_) ->
    %% we have to setup the listener here, because vmq_test_utils is overriding
    %% the default set in vmq_server.app.src
    {ok, {_Status1, _, _}} = httpc:request(post, {url(), [], "application/json", json_body()}, [], []),
    {_, 200, _} = _Status1,
    {ok, {_Status2, _, _}} = httpc:request(put, {url(), [], "application/json", json_body()}, [], []),
    {_, 200, _} = _Status2.

test_payload_push_endoding(_) ->
    {ok, {_Status1, _, _}} = httpc:request(post, {url() ++ "?encoding=plain", [], "application/json", json_body()}, [], []),
    {_, 200, _} = _Status1,
    {ok, {_Status2, _, _}} = httpc:request(put, {url() ++ "?encoding=base64", [], "application/json", json_body()}, [], []),
    {_, 500, _} = _Status2,
    {ok, {_Status3, _, _}} = httpc:request(put, {url() ++ "?encoding=base64", [], "application/json", json_body_base64()}, [], []),
    {_, 200, _} = _Status3.

test_header_push(_) ->
    Headers = [{"topic", "T1"}, {"qos", "2"}],
    {ok, {_Status, _Headers, _Body}} = httpc:request(put, {url(), Headers, "application/octet-stream", binary_body()}, [], []),
    {_, 200, _} = _Status.

test_header_qos(_) ->
    Headers = [{"topic", "T1"}, {"qos", "5"}],
    {ok, {_Status, _Headers, _Body}} = httpc:request(put, {url(), Headers, "application/octet-stream", binary_body()}, [], []),
    {_, 400, _} = _Status,

    Headers2 = [{"topic", "T1"}, {"qos", "-1"}],
    {ok, {_Status2, _, _}} = httpc:request(put, {url(), Headers2, "application/octet-stream", binary_body()}, [], []),
    {_, 400, _} = _Status2,

    Headers3 = [{"topic", "T1"}, {"qos", "a"}],
    {ok, {_Status3, _, _}} = httpc:request(put, {url(), Headers3, "application/octet-stream", binary_body()}, [], []),
    {_, 400, _} = _Status3,

    Headers4 = [{"topic", "T1"}],
    {ok, {_Status4, _, _}} = httpc:request(put, {url(), Headers4, "application/octet-stream", binary_body()}, [], []),
    {_, 200, _} = _Status4.

test_header_topic(_) ->
    Headers = [{"topic", ""}, {"qos", "2"}],
    {ok, {_Status, _, _}} = httpc:request(put, {url(), Headers, "application/octet-stream", binary_body()}, [], []),
    {_, 400, _} = _Status,

    Headers2 = [{"topic", "$SYS/123"}, {"qos", "2"}],
    {ok, {_Status2, _, _}} = httpc:request(put, {url(), Headers2, "application/octet-stream", binary_body()}, [], []),
    {_, 400, _} = _Status2,

    Headers3 = [{"topic", "bla/#/"}, {"qos", "2"}],
    {ok, {_Status3, _, _}} = httpc:request(put, {url(), Headers3, "application/octet-stream", binary_body()}, [], []),
    {_, 400, _} = _Status3.

test_header_retain(_) ->
    Headers = [{"topic", "T1/T2/T3"}, {"qos", "2"}, {"retain", "false"}],
    {ok, {_Status, _, _}} = httpc:request(put, {url(), Headers, "application/octet-stream", binary_body()}, [], []),
    {_, 200, _} = _Status,

    Headers2 = [{"topic", "T2"}, {"qos", "2"}, {"retain", "true"}],
    {ok, {_Status2, _, _}} = httpc:request(put, {url(), Headers2, "application/octet-stream", binary_body()}, [], []),
    {_, 200, _} = _Status2,

    Headers3 = [{"topic", "T2"}, {"qos", "2"}, {"retain", "something"}],
    {ok, {_Status3, _, _}} = httpc:request(put, {url(), Headers3, "application/octet-stream", binary_body()}, [], []),
    {_, 400, _} = _Status3.

test_header_user_props(_) ->
    Headers = [{"topic", "T1"}, {"qos", "2"}, {"retain", "false"}, {"user_properties", "[{\"a\":\"b2\"}]"}],
    {ok, {_Status, _, _}} = httpc:request(put, {url(), Headers, "application/octet-stream", binary_body()}, [], []),
    {_, 200, _} = _Status,
    Headers2 = [{"topic", "T1"}, {"qos", "2"}, {"retain", "false"}, {"user_properties", "[{\"a\":\"b2\"}"}],
    {ok, {_Status2, _, _}} = httpc:request(put, {url(), Headers2, "application/octet-stream", binary_body()}, [], []),
    {_, 400, _} = _Status2.

test_uee_push(_) ->
    Headers = [{"topic", "T1"}, {"qos", "2"}],
    {ok, {_Status, _Headers, _Body}} = httpc:request(put, {url()++"?"++uri_string:compose_query(Headers), [], "application/octet-stream", binary_body()}, [], []),
    {_, 200, _} = _Status.

test_uee_qos(_) ->
    Headers = [{"topic", "T1"}, {"qos", "5"}],
    {ok, {_Status, _Headers, _Body}} = httpc:request(put, {url()++"?"++uri_string:compose_query(Headers), [], "application/octet-stream", binary_body()}, [], []),
    {_, 400, _} = _Status,

    Headers2 = [{"topic", "T1"}, {"qos", "-1"}],
    {ok, {_Status2, _, _}} = httpc:request(put, {url()++"?"++uri_string:compose_query(Headers2), [],  "application/octet-stream", binary_body()}, [], []),
    {_, 400, _} = _Status2,

    Headers3 = [{"topic", "T1"}, {"qos", "a"}],
    {ok, {_Status3, _, _}} = httpc:request(put, {url()++"?"++uri_string:compose_query(Headers3), [], "application/octet-stream", binary_body()}, [], []),
    {_, 400, _} = _Status3,

    Headers4 = [{"topic", "T1"}],
    {ok, {_Status4, _, _}} = httpc:request(put, {url()++"?"++uri_string:compose_query(Headers4), [],  "application/octet-stream", binary_body()}, [], []),
    {_, 200, _} = _Status4.


test_mp_overload(_) ->
    application:set_env(vmq_http_pub, config, [{mqttauth, "testMode"}, {mqttmountpoint, "mpt1"}]),
    {ok, {_Status1, _, Body}} = httpc:request(post, {url(), [], "application/json", json_body()}, [], []),
    {_, 200, _} = _Status1,
    "Published[{topic,[<<\"T1\">>]},{qos,1},{retain,false},{mp,\"mpt1\"}]" = Body.

test_auth_conf_1(_) ->
    application:set_env(vmq_http_pub, config, [{mqttauth, "on-behalf-of"}]),
    {ok, "on-behalf-of", _} = vmq_http_pub:auth_conf(mock_request_http()),
    application:set_env(vmq_http_pub, config, [{mqttauth, "predefined"}]),
    {ok, "predefined", _} = vmq_http_pub:auth_conf(mock_request_http()).

test_no_auth(_) ->
    allow_anon(false),
    {ok, {_Status1, _, _}} = httpc:request(post, {url(), [], "application/json", json_body_diversity(<<"WrongUser">>, <<"test-client">>)}, [], []),
    {_, 401, _} = _Status1,
    allow_anon(true),
    {ok, {_Status2, _, _}} = httpc:request(post, {url(), [], "application/json", json_body_diversity(<<"WrongUser">>, <<"test-client">>)}, [], []),
    {_, 200, _} = _Status2.


test_payload_push_obo(_) ->
    allow_anon(false),
    application:set_env(vmq_http_pub, config, [{mqttauth, "on-behalf-of"}]),
    {ok, {_Status1, _, _}} = httpc:request(post, {url(), [], "application/json", json_body_diversity(<<"WrongUser">>, <<"test-client">>)}, [], []),
    {_, 401, _} = _Status1,
    {ok, {_Status2, _, _}} = httpc:request(post, {url(), [], "application/json", json_body_diversity(<<"test-user">>, <<"test-client">>)}, [], []),
    {_, 200, _} = _Status2.

test_payload_change_mp_reg(_) ->
    allow_anon(false),
    application:set_env(vmq_http_pub, config, [{mqttauth, "on-behalf-of"}]),
    {ok, {_Status1, _, _}} = httpc:request(post, {url(), [], "application/json", json_body_diversity(<<"test-user">>, <<"test-change-mountpoint">>)}, [], []),
    {_, 403, _} = _Status1,
    {ok, {_Status2, _, _}} = httpc:request(post, {url(), [], "application/json", json_body_diversity(<<"test-user">>, <<"test-change-mountpoint-pass">>)}, [], []),
    {_, 200, _} = _Status2,
    application:set_env(vmq_http_pub, config, [{mqttauth, "on-behalf-of"}, {mqttmountpoint, "mpt1"}]),
    {ok, {_Status3, _, _}} = httpc:request(post, {url(), [], "application/json", json_body_diversity(<<"test-user">>, <<"test-change-mountpoint-pass">>)}, [], []),
    {_, 401, _} = _Status3,
    application:set_env(vmq_http_pub, config, [{mqttauth, "on-behalf-of"}, {mqttmountpoint, "mpt22"}]),
    {ok, {_Status4, _, Body}} = httpc:request(post, {url(), [], "application/json", json_body_diversity(<<"test-user">>, <<"test-change-mountpoint-pass-ovr">>)}, [], []),
    {_, 200, _} = _Status4,
    "Published[{topic,[<<\"a\">>,<<\"b\">>,<<\"c\">>]},\n {qos,1},\n {retain,false},\n {mp,\"override-mountpoint\"}]" = Body.


test_payload_change_mp_pub(_) ->
    allow_anon(false),
    application:set_env(vmq_http_pub, config, [{mqttauth, "on-behalf-of"}]),
    {ok, {_Status1, _, Body}} = httpc:request(post, {url(), [], "application/json", json_body_diversity(<<"test-user">>, <<"test-change-mountpoint-pub">>)}, [], []),
    "Published[{topic,[<<\"a\">>,<<\"b\">>,<<\"c\">>]},\n {qos,1},\n {retain,false},\n {mp,\"override-mountpoint-pub\"}]" = Body,
    {_, 200, _} = _Status1.

test_payload_change_qos_pub(_) ->
    allow_anon(false),
    application:set_env(vmq_http_pub, config, [{mqttauth, "on-behalf-of"}]),
    {ok, {_Status1, _, Body}} = httpc:request(post, {url(), [], "application/json", json_body_diversity(<<"test-user">>, <<"test-change-qos-pub">>)}, [], []),
    "Published[{topic,[<<\"a\">>,<<\"b\">>,<<\"c\">>]},{qos,2},{retain,false},{mp,[]}]" = Body,
    {_, 200, _} = _Status1.

test_payload_change_topic_pub(_) ->
    allow_anon(false),
    {ok, {_Status1, _, Body}} = httpc:request(post, {url(), [], "application/json", json_body_diversity(<<"test-user">>, <<"test-change-topic-pub">>)}, [], []),
    "Published[{topic,[<<\"d\">>,<<\"e\">>,<<\"f\">>]},{qos,1},{retain,false},{mp,[]}]" = Body,
    {_, 200, _} = _Status1.

test_max_message_size(_) ->
    allow_anon(false),
    application:set_env(vmq_http_pub, config, [{mqttauth, "on-behalf-of"}]),
    {ok, {_Status1, _, _Body}} = httpc:request(post, {url(), [], "application/json", json_body_diversity(<<"test-user">>, <<"test-change-max-message-size-ovr">>)}, [], []),
    {_, 413, _} = _Status1.

test_payload_change_mp_predef_pub(_) ->
    allow_anon(false),
    application:set_env(vmq_http_pub, config,[{mqttauth, "predefined"}]),
    {ok, {_Status1, _, _}} = httpc:request(post, {url(), [], "application/json", json_body_diversity(<<"test-user">>, <<"test-change-mountpoint-pub">>)}, [], []),
    {_, 401, _} = _Status1,
    application:set_env(vmq_http_pub, config,[{mqttauth, "predefined"}, {mqttauthuser, "test-user"}, {mqttauthpassword, "test-password"}, {mqttauthclientid, "test-change-mountpoint-pub"}]),
    {ok, {_Status2, _, Body2}} = httpc:request(post, {url(), [], "application/json", json_body_diversity(<<"test-user">>, <<"test-change-mountpoint-pub">>)}, [], []),
    "Published[{topic,[<<\"a\">>,<<\"b\">>,<<\"c\">>]},\n {qos,1},\n {retain,false},\n {mp,\"override-mountpoint-pub\"}]" = Body2,
    {_, 200, _} = _Status2.

