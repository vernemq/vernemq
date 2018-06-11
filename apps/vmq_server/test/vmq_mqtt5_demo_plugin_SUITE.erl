-module(vmq_mqtt5_demo_plugin_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("vmq_commons/include/vmq_types_mqtt5.hrl").

init_per_suite(Config) ->
    vmq_test_utils:setup(),
    vmq_server_cmd:listener_start(1888, [{allowed_protocol_versions, "5"}]),
    ok = vmq_plugin_mgr:enable_plugin(vmq_mqtt5_demo_plugin),
    cover:start(),
    [{ct_hooks, vmq_cth}|Config].

end_per_suite(_Config) ->
    ok = vmq_plugin_mgr:disable_plugin(vmq_mqtt5_demo_plugin),
    vmq_test_utils:teardown(),
    ok.

init_per_testcase(_TestCase, Config) ->
    vmq_server_cmd:set_config(allow_anonymous, false),
    vmq_server_cmd:set_config(max_client_id_size, 500),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

all() ->
    [
     {group, mqtt}
    ].

groups() ->
    ConnectTests =
    [
     connack_error_with_reason_string,
     puback_error_with_reason_string,
     pubrec_error_with_reason_string,
     suback_with_properties,
     unsuback_with_properties,
     auth_with_properties
    ],
    [
     {mqtt, [shuffle, parallel], ConnectTests}
    ].

connack_error_with_reason_string(Cfg) ->
    ClientId = vmq_cth:ustr(Cfg),
    Connect = packetv5:gen_connect(ClientId, [{username, <<"quota_exceeded">>}]),
    {ok, Socket, Connack, <<>>} = packetv5:do_client_connect(Connect, []),
    #mqtt5_connack{session_present = 0,
                   reason_code = ?M5_QUOTA_EXCEEDED,
                   properties = #{?P_REASON_STRING :=
                                      <<"You have exceeded your quota">>}}
        = Connack,
    ok = gen_tcp:close(Socket).

puback_error_with_reason_string(Cfg) ->
    ClientId = vmq_cth:ustr(Cfg),
    Connect = packetv5:gen_connect(ClientId, []),
    Connack = packetv5:gen_connack(),
    {ok, Socket} = packetv5:do_client_connect(Connect, Connack, []),

    Publish = packetv5:gen_publish("invalid/topic", 1, <<"message">>, [{mid, 1}]),
    Puback = packetv5:gen_puback(1, ?M5_TOPIC_NAME_INVALID, #{?P_REASON_STRING => "Invalid topic name"}),
    ok = gen_tcp:send(Socket, Publish),
    ok = packetv5:expect_frame(Socket, Puback),
    ok = gen_tcp:close(Socket).

pubrec_error_with_reason_string(Cfg) ->
    ClientId = vmq_cth:ustr(Cfg),
    Connect = packetv5:gen_connect(ClientId, []),
    Connack = packetv5:gen_connack(),
    {ok, Socket} = packetv5:do_client_connect(Connect, Connack, []),

    Publish = packetv5:gen_publish("invalid/topic", 2, <<"message">>, [{mid, 1}]),
    Pubrec = packetv5:gen_pubrec(1, ?M5_TOPIC_NAME_INVALID, #{?P_REASON_STRING => "Invalid topic name"}),
    ok = gen_tcp:send(Socket, Publish),
    ok = packetv5:expect_frame(Socket, Pubrec),
    ok = gen_tcp:close(Socket).

suback_with_properties(Cfg) ->
    ClientId = vmq_cth:ustr(Cfg),
    Connect = packetv5:gen_connect(ClientId, []),
    Connack = packetv5:gen_connack(),
    {ok, Socket} = packetv5:do_client_connect(Connect, Connack, []),

    SubTopic = packetv5:gen_subtopic("suback/withprops", 1),
    Subscribe = packetv5:gen_subscribe(7, [SubTopic], #{}),
    Suback = packetv5:gen_suback(7, [1], #{?P_USER_PROPERTY => [{<<"key">>, <<"val">>}],
                                           ?P_REASON_STRING => <<"successful subscribe">>}),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packetv5:expect_frame(Socket, Suback),
    ok = gen_tcp:close(Socket).


unsuback_with_properties(Cfg) ->
    ClientId = vmq_cth:ustr(Cfg),
    Connect = packetv5:gen_connect(ClientId, []),
    Connack = packetv5:gen_connack(),
    {ok, Socket} = packetv5:do_client_connect(Connect, Connack, []),

    Unsubscribe = packetv5:gen_unsubscribe(7, ["unsuback/withprops"], #{}),
    Unsuback = packetv5:gen_unsuback(7, [?M5_SUCCESS],
                                     #{?P_USER_PROPERTY => [{<<"key">>, <<"val">>}],
                                       ?P_REASON_STRING => <<"Unsubscribe worked">>}),
    ok = gen_tcp:send(Socket, Unsubscribe),
    ok = packetv5:expect_frame(Socket, Unsuback),
    ok = gen_tcp:close(Socket).

auth_with_properties(Cfg) ->
    ClientId = vmq_cth:ustr(Cfg),
    Connect = packetv5:gen_connect(ClientId, [{properties,
                                              #{?P_AUTHENTICATION_METHOD => <<"method1">>,
                                                ?P_AUTHENTICATION_DATA => <<"client1">>}}]),
    AuthIn1 = packetv5:gen_auth(?M5_CONTINUE_AUTHENTICATION,
                                #{?P_AUTHENTICATION_METHOD => <<"method1">>,
                                  ?P_AUTHENTICATION_DATA => <<"server1">>}),
    AuthOut1 = packetv5:gen_auth(?M5_CONTINUE_AUTHENTICATION,
                                #{?P_AUTHENTICATION_METHOD => <<"method1">>,
                                  ?P_AUTHENTICATION_DATA => <<"client2">>}),
    Connack = packetv5:gen_connack(0, ?M5_SUCCESS, #{?P_AUTHENTICATION_METHOD => <<"method1">>,
                                                  ?P_AUTHENTICATION_DATA => <<"server2">>}),
    {ok, Socket} = packetv5:do_client_connect(Connect, AuthIn1, []),
    ok = gen_tcp:send(Socket, AuthOut1),
    ok = packetv5:expect_frame(Socket, Connack),


    %% re-do the auth... because it's allowed:
    AuthOut0 = packetv5:gen_auth(?M5_CONTINUE_AUTHENTICATION,
                                 #{?P_AUTHENTICATION_METHOD => <<"method1">>,
                                   ?P_AUTHENTICATION_DATA => <<"client1">>}),
    AuthIn2 = packetv5:gen_auth(?M5_SUCCESS,
                                #{?P_AUTHENTICATION_METHOD => <<"method1">>,
                                  ?P_AUTHENTICATION_DATA => <<"server2">>}),


    ok = gen_tcp:send(Socket, AuthOut0),
    ok = packetv5:expect_frame(Socket, AuthIn1),
    ok = gen_tcp:send(Socket, AuthOut1),
    ok = packetv5:expect_frame(Socket, AuthIn2),

    %% let's try to authentication with 'bad' data
    AuthOutWrong = packetv5:gen_auth(?M5_CONTINUE_AUTHENTICATION,
                                     #{?P_AUTHENTICATION_METHOD => <<"method1">>,
                                       ?P_AUTHENTICATION_DATA => <<"baddata">>}),
    DisconnectWrongAuth =
        packetv5:gen_disconnect(?M5_NOT_AUTHORIZED,
                                #{?P_REASON_STRING =>
                                      <<"Bad authentication data: baddata">>}),
    ok = gen_tcp:send(Socket, AuthOutWrong),
    ok = packetv5:expect_frame(Socket, DisconnectWrongAuth),
    {error, closed} = gen_tcp:recv(Socket, 0, 100).
