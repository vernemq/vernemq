-module(vmq_mqtt5_demo_plugin_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("vmq_commons/include/vmq_types.hrl").

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

init_per_testcase(TestCase, Config) ->
    case lists:member(TestCase, [publish_modify_props, publish_remove_props]) of
     true -> {skip, travis};
    _ ->
        vmq_server_cmd:set_config(allow_anonymous, false),
        vmq_server_cmd:set_config(max_client_id_size, 500),
        Config
    end.

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
     connack_error_use_another_server,
     connack_error_server_moved,
     connack_broker_capabilities,
     publish_modify_props,
     publish_remove_props,
     puback_error_with_reason_string,
     pubrec_error_with_reason_string,
     suback_with_properties,
     unsuback_with_properties,
     auth_with_properties,
     remove_props_on_deliver_m5,
     modify_props_on_deliver_m5
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

connack_error_use_another_server(Cfg) ->
    ClientId = vmq_cth:ustr(Cfg),
    Connect = packetv5:gen_connect(ClientId, [{username, <<"use_another_server">>}]),
    {ok, Socket, Connack, <<>>} = packetv5:do_client_connect(Connect, []),
    #mqtt5_connack{session_present = 0,
                   reason_code = ?M5_USE_ANOTHER_SERVER,
                   properties = #{?P_SERVER_REF :=
                                      <<"server_ref">>}}
        = Connack,
    ok = gen_tcp:close(Socket).

connack_error_server_moved(Cfg) ->
    ClientId = vmq_cth:ustr(Cfg),
    Connect = packetv5:gen_connect(ClientId, [{username, <<"server_moved">>}]),
    {ok, Socket, Connack, <<>>} = packetv5:do_client_connect(Connect, []),
    #mqtt5_connack{session_present = 0,
                   reason_code = ?M5_SERVER_MOVED,
                   properties = #{?P_SERVER_REF :=
                                      <<"server_ref">>}}
        = Connack,
    ok = gen_tcp:close(Socket).

connack_broker_capabilities(Cfg) ->
    ClientId = vmq_cth:ustr(Cfg),
    Connect = packetv5:gen_connect(ClientId, [{username, <<"broker_capabilities">>}]),
    {ok, Socket, Connack, <<>>} = packetv5:do_client_connect(Connect, []),
    #mqtt5_connack{reason_code = ?M5_SUCCESS,
                   properties = #{?P_MAX_QOS := 0,
                                  ?P_RETAIN_AVAILABLE := false,
                                  ?P_WILDCARD_SUBS_AVAILABLE := false,
                                  ?P_SUB_IDS_AVAILABLE := false,
                                  ?P_SHARED_SUBS_AVAILABLE := false}}
        = Connack,
    ok = gen_tcp:close(Socket).

publish_modify_props(Cfg) ->
    PubClientId = vmq_cth:ustr(Cfg) ++ "-pub",
    SubClientId = vmq_cth:ustr(Cfg) ++ "-sub",
    PubConnect = packetv5:gen_connect(PubClientId, [{username, <<"modify_props">>}]),
    SubConnect = packetv5:gen_connect(SubClientId, []),
    Connack = packetv5:gen_connack(),
    Topic = vmq_cth:ustr(Cfg),
    Publish = packetv5:gen_publish(Topic, 0, <<"message">>,
                                   [{properties,
                                     #{p_user_property =>
                                           [{<<"k1">>, <<"v1">>},
                                            {<<"k2">>, <<"v2">>}],
                                       p_correlation_data => <<"correlation_data">>,
                                       p_response_topic => [<<"response">>,<<"topic">>]}}]),
    Subscribe = packetv5:gen_subscribe(6, [packetv5:gen_subtopic(Topic, 0)], #{}),
    Suback = packetv5:gen_suback(6, [0], #{}),

    {ok, PubSocket} = packetv5:do_client_connect(PubConnect, Connack, []),
    {ok, SubSocket} = packetv5:do_client_connect(SubConnect, Connack, []),

    ok = gen_tcp:send(SubSocket, Subscribe),
    ok = packetv5:expect_frame(SubSocket, Suback),

    ok = gen_tcp:send(PubSocket, Publish),
    ok = gen_tcp:close(PubSocket),

    {ok, RecvPub, <<>>} = packetv5:receive_frame(SubSocket),
    #mqtt5_publish{topic = _,
                   qos = 0,
                   properties = #{p_user_property := GotUserProps,
                                  p_correlation_data := <<"modified_correlation_data">>,
                                  p_response_topic := <<"modified_response/topic">>},
                   payload = <<"message">>} = RecvPub,

    ExpUserProps = #{<<"k1">> => <<"v1">>,
                     <<"k2">> => <<"v2">>,
                     <<"added">> => <<"user_property">>},
    ExpUserProps = maps:from_list(GotUserProps).

publish_remove_props(Cfg) ->
    PubClientId = vmq_cth:ustr(Cfg) ++ "-pub",
    SubClientId = vmq_cth:ustr(Cfg) ++ "-sub",
    PubConnect = packetv5:gen_connect(PubClientId, [{username, <<"remove_props">>}]),
    SubConnect = packetv5:gen_connect(SubClientId, []),
    Connack = packetv5:gen_connack(),
    Topic = vmq_cth:ustr(Cfg),
    Publish = packetv5:gen_publish(Topic, 0, <<"message">>,
                                   [{properties,
                                     #{p_user_property =>
                                           [{<<"k1">>, <<"v1">>},
                                            {<<"k2">>, <<"v2">>}],
                                       p_correlation_data => <<"correlation_data">>,
                                       p_response_topic => [<<"response">>,<<"topic">>]}}]),
    Subscribe = packetv5:gen_subscribe(6, [packetv5:gen_subtopic(Topic, 0)], #{}),
    Suback = packetv5:gen_suback(6, [0], #{}),

    {ok, PubSocket} = packetv5:do_client_connect(PubConnect, Connack, []),
    {ok, SubSocket} = packetv5:do_client_connect(SubConnect, Connack, []),

    ok = gen_tcp:send(SubSocket, Subscribe),
    ok = packetv5:expect_frame(SubSocket, Suback),

    ok = gen_tcp:send(PubSocket, Publish),
    ok = gen_tcp:close(PubSocket),

    {ok, RecvPub, <<>>} = packetv5:receive_frame(SubSocket),
    #mqtt5_publish{topic = _,
                   qos = 0,
                   properties = Props,
                   payload = <<"message">>} = RecvPub,

    case map_size(Props) of
        0 ->
            ok;
        _ ->
            throw({expected_no_properties, Props})
    end.

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
                                #{?P_REASON_STRING => <<"Bad authentication data: baddata">>}),
    ok = gen_tcp:send(Socket, AuthOutWrong),
    ok = packetv5:expect_frame(Socket, DisconnectWrongAuth),
    {error, closed} = gen_tcp:recv(Socket, 0, 100).


modify_props_on_deliver_m5(Cfg) ->
    Username = <<"modify_props_on_deliver_m5">>,
    PubClientId = vmq_cth:ustr(Cfg) ++ "-pub",
    SubClientId = vmq_cth:ustr(Cfg) ++ "-sub",
    PubConnect = packetv5:gen_connect(PubClientId, []),
    SubConnect = packetv5:gen_connect(SubClientId, [{username, Username}]),
    Connack = packetv5:gen_connack(),
    Topic = vmq_cth:ustr(Cfg),
    Publish = packetv5:gen_publish(Topic, 0, <<"message">>,
                                   [{properties,
                                     #{
                                       ?P_PAYLOAD_FORMAT_INDICATOR => unspecified,
                                       ?P_CONTENT_TYPE => <<"type1">>,
                                       p_user_property => [{<<"k1">>, <<"v1">>}],
                                       p_correlation_data => <<"correlation_data">>,
                                       p_response_topic => [<<"response">>,<<"topic">>]}}]),
    Subscribe = packetv5:gen_subscribe(6, [packetv5:gen_subtopic(Topic, 0)], #{}),
    Suback = packetv5:gen_suback(6, [0], #{}),

    {ok, PubSocket} = packetv5:do_client_connect(PubConnect, Connack, []),
    {ok, SubSocket} = packetv5:do_client_connect(SubConnect, Connack, []),

    ok = gen_tcp:send(SubSocket, Subscribe),
    ok = packetv5:expect_frame(SubSocket, Suback),

    ok = gen_tcp:send(PubSocket, Publish),
    ok = gen_tcp:close(PubSocket),

    {ok, RecvPub, <<>>} = packetv5:receive_frame(SubSocket),
    #mqtt5_publish{topic = _,
                   qos = 0,
                   properties = #{?P_PAYLOAD_FORMAT_INDICATOR := utf8,
                                  ?P_CONTENT_TYPE := <<"type2">>,
                                  p_user_property := GotUserProps,
                                  p_correlation_data := <<"modified_correlation_data">>,
                                  p_response_topic := <<"modified_response/topic">>},
                   payload = <<"message">>} = RecvPub,

    ExpUserProps = #{<<"k1">> => <<"v1">>,
                     <<"added">> => <<"user_property">>},
    ExpUserProps = maps:from_list(GotUserProps).

remove_props_on_deliver_m5(Cfg) ->
    Username = <<"remove_props_on_deliver_m5">>,
    PubClientId = vmq_cth:ustr(Cfg) ++ "-pub",
    SubClientId = vmq_cth:ustr(Cfg) ++ "-sub",
    PubConnect = packetv5:gen_connect(PubClientId, []),
    SubConnect = packetv5:gen_connect(SubClientId, [{username, Username}]),
    Connack = packetv5:gen_connack(),
    Topic = vmq_cth:ustr(Cfg),
    Publish = packetv5:gen_publish(Topic, 0, <<"message">>,
                                   [{properties,
                                     #{?P_PAYLOAD_FORMAT_INDICATOR => unspecified,
                                       ?P_CONTENT_TYPE => <<"sometype">>,
                                       p_user_property => [{<<"k1">>, <<"v1">>}],
                                       p_correlation_data => <<"correlation_data">>,
                                       p_response_topic => [<<"response">>,<<"topic">>]}}]),
    Subscribe = packetv5:gen_subscribe(6, [packetv5:gen_subtopic(Topic, 0)], #{}),
    Suback = packetv5:gen_suback(6, [0], #{}),

    {ok, PubSocket} = packetv5:do_client_connect(PubConnect, Connack, []),
    {ok, SubSocket} = packetv5:do_client_connect(SubConnect, Connack, []),

    ok = gen_tcp:send(SubSocket, Subscribe),
    ok = packetv5:expect_frame(SubSocket, Suback),

    ok = gen_tcp:send(PubSocket, Publish),
    ok = gen_tcp:close(PubSocket),

    {ok, RecvPub, <<>>} = packetv5:receive_frame(gen_tcp, SubSocket, 15000),
    #mqtt5_publish{topic = _,
                   qos = 0,
                   properties = Props,
                   payload = <<"message">>} = RecvPub,

    case map_size(Props) of
        0 ->
            ok;
        _ ->
            throw({expected_no_properties, Props})
    end.
