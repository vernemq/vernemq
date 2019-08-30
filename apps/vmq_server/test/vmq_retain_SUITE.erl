-module(vmq_retain_SUITE).

-include_lib("vmq_commons/include/vmq_types.hrl").

-compile(export_all).
-compile(nowarn_export_all).

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(Config) ->
    vmq_test_utils:setup(),
    vmq_server_cmd:set_config(allow_anonymous, true),
    vmq_server_cmd:set_config(max_client_id_size, 1000),
    vmq_server_cmd:set_config(retry_interval, 10),
    vmq_server_cmd:listener_start(1888, [{allowed_protocol_versions, "3,4,5"}]),
    cover:start(),
    enable_on_publish(),
    enable_on_subscribe(),
    enable_on_message_drop(),
    [{ct_hooks, vmq_cth} | Config].

end_per_suite(_Config) ->
    disable_on_publish(),
    disable_on_subscribe(),
    disable_on_message_drop(),
    vmq_test_utils:teardown(),
    _Config.

init_per_group(mqttv4, Config) ->
    [{protover, 4}|Config];
init_per_group(mqttv5, Config) ->
    [{protover, 5}|Config].

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_, Config) ->
    Config.

all() ->
    [
     {group, mqttv4},
     {group, mqttv5}
    ].

groups() ->
    Tests =
        [retain_qos0_test,
         retain_qos0_repeated_test,
         retain_qos0_fresh_test,
         retain_qos0_clear_test,
         retain_qos1_qos0_test,
         retain_wildcard_test,
         publish_empty_retained_msg_test,
         retain_compat_pre_test],
    [
     {mqttv4, [shuffle, parallel], Tests},
     {mqttv5, [shuffle, parallel],
      [
       retain_with_properties,
       retain_with_message_expiry,
       subscribe_retain_as_published_test,
       subscribe_retain_handling_flags_test,
       subscribe_retain_subid_test
       |Tests]}
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

retain_qos0_test(Cfg) ->
    Topic = vmq_cth:ustr(Cfg) ++ "retain/qos0/test",
    Connect = mqtt5_v4compat:gen_connect(vmq_cth:ustr(Cfg) ++ "retain-qos0-test", [{keepalive,60}], Cfg),
    Connack = mqtt5_v4compat:gen_connack(success, Cfg),
    Publish = mqtt5_v4compat:gen_publish(Topic, 0, <<"retained message">>, [{retain, true}], Cfg),
    Subscribe = mqtt5_v4compat:gen_subscribe(16, Topic, 0, Cfg),
    Suback = mqtt5_v4compat:gen_suback(16, 0, Cfg),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Cfg),
    %% Send retained message
    ok = gen_tcp:send(Socket, Publish),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "suback", Suback, Cfg),
    ok = mqtt5_v4compat:expect_packet(Socket, "publish", Publish, Cfg),
    ok = gen_tcp:close(Socket).

retain_qos0_repeated_test(Cfg) ->
    Topic = vmq_cth:ustr(Cfg) ++ "retain/qos0/reptest",
    Connect = mqtt5_v4compat:gen_connect(vmq_cth:ustr(Cfg)
                                 ++ "retain-qos0-rep-test", [{keepalive,60}], Cfg),
    Connack = mqtt5_v4compat:gen_connack(success, Cfg),
    Publish = mqtt5_v4compat:gen_publish(Topic, 0, <<"retained message">>, [{retain, true}], Cfg),
    Subscribe = mqtt5_v4compat:gen_subscribe(16, Topic, 0, Cfg),
    Suback = mqtt5_v4compat:gen_suback(16, 0, Cfg),
    Unsubscribe = mqtt5_v4compat:gen_unsubscribe(13, Topic, Cfg),
    Unsuback = mqtt5_v4compat:gen_unsuback(13, Cfg),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Cfg),
    %% Send retained message
    ok = gen_tcp:send(Socket, Publish),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "suback", Suback, Cfg),
    ok = mqtt5_v4compat:expect_packet(Socket, "publish", Publish, Cfg),
    ok = gen_tcp:send(Socket, Unsubscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "unsuback", Unsuback, Cfg),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "suback", Suback, Cfg),
    ok = mqtt5_v4compat:expect_packet(Socket, "publish", Publish, Cfg),
    ok = gen_tcp:close(Socket).

retain_qos0_fresh_test(Cfg) ->
    Topic = vmq_cth:ustr(Cfg) ++ "retain/qos0/freshtest",
    Connect = mqtt5_v4compat:gen_connect(vmq_cth:ustr(Cfg)
                                 ++ "retain-qos0-fresh-test", [{keepalive,60}], Cfg),
    Connack = mqtt5_v4compat:gen_connack(success, Cfg),
    Publish = mqtt5_v4compat:gen_publish(Topic, 0, <<"retained message">>, [{retain, true}], Cfg),
    PublishFresh = mqtt5_v4compat:gen_publish(Topic, 0, <<"retained message">>, [], Cfg),
    Subscribe = mqtt5_v4compat:gen_subscribe(16, Topic, 0, Cfg),
    Suback = mqtt5_v4compat:gen_suback(16, 0, Cfg),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Cfg),
    %% Send retained message
    ok = gen_tcp:send(Socket, Subscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "suback", Suback, Cfg),
    ok = gen_tcp:send(Socket, Publish),
    ok = mqtt5_v4compat:expect_packet(Socket, "publish", PublishFresh, Cfg),
    ok = gen_tcp:close(Socket).

retain_qos0_clear_test(Cfg) ->
    Topic = vmq_cth:ustr(Cfg) ++ "retain/clear/test",
    Connect = mqtt5_v4compat:gen_connect(vmq_cth:ustr(Cfg)
                                 ++ "retain-clear-test", [{keepalive,60}], Cfg),
    Connack = mqtt5_v4compat:gen_connack(success, Cfg),
    Publish = mqtt5_v4compat:gen_publish(Topic, 0, <<"retained message">>, [{retain, true}], Cfg),
    RetainClear = mqtt5_v4compat:gen_publish(Topic, 0, <<>>, [{retain, true}], Cfg),
    Subscribe = mqtt5_v4compat:gen_subscribe(592, Topic, 0, Cfg),
    Suback = mqtt5_v4compat:gen_suback(592, 0, Cfg),
    Unsubscribe = mqtt5_v4compat:gen_unsubscribe(593, Topic, Cfg),
    Unsuback = mqtt5_v4compat:gen_unsuback(593, Cfg),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Cfg),
    %% Send retained message
    ok = gen_tcp:send(Socket, Publish),
    %% Subscribe to topic, we should get the retained message back.
    ok = gen_tcp:send(Socket, Subscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "suback", Suback, Cfg),
    ok = mqtt5_v4compat:expect_packet(Socket, "publish", Publish, Cfg),
    %% Now unsubscribe from the topic before we clear the retained mesage
    ok = gen_tcp:send(Socket, Unsubscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "unsuback", Unsuback, Cfg),
    %% Now clear the retained message
    ok = gen_tcp:send(Socket, RetainClear),
    %% Subscribe to topic, we shouldn't get anything back apart from the SUBACK
    ok = gen_tcp:send(Socket, Subscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "suback", Suback, Cfg),
    {error, timeout} = gen_tcp:recv(Socket, 256, 1000),
    ok = gen_tcp:close(Socket).

retain_qos1_qos0_test(Cfg) ->
    Topic = vmq_cth:ustr(Cfg) ++ "retain/qos1/test",
    Connect = mqtt5_v4compat:gen_connect(vmq_cth:ustr(Cfg)
                                 ++ "retain-qos1-test", [{keepalive,60}], Cfg),
    Connack = mqtt5_v4compat:gen_connack(success, Cfg),
    Publish = mqtt5_v4compat:gen_publish(Topic, 1, <<"retained message">>, [{mid, 6}, {retain, true}], Cfg),
    Puback = mqtt5_v4compat:gen_puback(6, Cfg),
    Subscribe = mqtt5_v4compat:gen_subscribe(18, Topic, 0, Cfg),
    Suback = mqtt5_v4compat:gen_suback(18, 0, Cfg),
    Publish0 = mqtt5_v4compat:gen_publish(Topic, 0, <<"retained message">>, [{retain, true}], Cfg),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Cfg),
    %% Send retained message
    ok = gen_tcp:send(Socket, Publish),
    ok = mqtt5_v4compat:expect_packet(Socket, "puback", Puback, Cfg),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "suback", Suback, Cfg),
    ok = mqtt5_v4compat:expect_packet(Socket, "publish0", Publish0, Cfg),
    ok = gen_tcp:close(Socket).

publish_empty_retained_msg_test(Cfg) ->
    Topic = vmq_cth:ustr(Cfg) ++ "retain/clear/emptytest",
    Connect = mqtt5_v4compat:gen_connect(vmq_cth:ustr(Cfg)
                                 ++ "retain-clear-empty-test", [{keepalive,60}], Cfg),
    Connack = mqtt5_v4compat:gen_connack(success, Cfg),
    Publish = mqtt5_v4compat:gen_publish(Topic, 0, <<"retained message">>, [{retain, true}], Cfg),
    RetainClearPub = mqtt5_v4compat:gen_publish(Topic, 0, <<>>, [{retain, true}], Cfg),
    RetainClearSub = mqtt5_v4compat:gen_publish(Topic, 0, <<>>, [{retain, false}], Cfg),
    Subscribe = mqtt5_v4compat:gen_subscribe(592, Topic, 0, Cfg),
    Suback = mqtt5_v4compat:gen_suback(592, 0, Cfg),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Cfg),

    %% Send retained message
    ok = gen_tcp:send(Socket, Publish),
    %% Subscribe to topic, we should get the retained message back.
    ok = gen_tcp:send(Socket, Subscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "suback", Suback, Cfg),
    ok = mqtt5_v4compat:expect_packet(Socket, "publish", Publish, Cfg),

    %% Now clear the retained message
    ok = gen_tcp:send(Socket, RetainClearPub),
    %% Receive the empty payload msg as normal publish.
    ok = mqtt5_v4compat:expect_packet(Socket, "publish", RetainClearSub, Cfg),
    {error, timeout} = gen_tcp:recv(Socket, 256, 1000),
    ok = gen_tcp:close(Socket).

retain_wildcard_test(Cfg) ->
    Prefix = vmq_cth:ustr(Cfg),
    Topic = Prefix ++ "retainwildcard/wildcard/test",
    Connect = mqtt5_v4compat:gen_connect(vmq_cth:ustr(Cfg)
                                 ++ "retain-wildcard-test", [{keepalive,60}], Cfg),
    Connack = mqtt5_v4compat:gen_connack(success, Cfg),
    Publish = mqtt5_v4compat:gen_publish(Topic, 0, <<"retained message">>, [{retain, true}], Cfg),
    Subscribe = mqtt5_v4compat:gen_subscribe(16, Prefix ++ "retainwildcard/+/#", 0, Cfg),
    Suback = mqtt5_v4compat:gen_suback(16, 0, Cfg),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Cfg),
    %% Send retained message
    ok = gen_tcp:send(Socket, Publish),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = mqtt5_v4compat:expect_packet(Socket, "suback", Suback, Cfg),
    ok = mqtt5_v4compat:expect_packet(Socket, "publish", Publish, Cfg),
    ok = gen_tcp:close(Socket).

retain_with_properties(Cfg) ->
    Topic = vmq_cth:utopic(Cfg),

    %% setup publisher
    Properties =
        #{p_payload_format_indicator => utf8,
          p_response_topic => <<"response topic">>,
          p_correlation_data => <<"correlation data">>,
          p_user_property => [{<<"k1">>, <<"v1">>},
                              {<<"k2">>, <<"v2">>},
                              {<<"k3">>, <<"v3">>},
                              {<<"k2">>, <<"v4">>}],
          p_content_type => <<"content type">>},

    PubConnect = packetv5:gen_connect(vmq_cth:ustr(Cfg) ++ "-pub", [{keepalive, 60}]),
    PubConnack = packetv5:gen_connack(0, ?M5_CONNACK_ACCEPT, #{}),
    {ok, PubSocket} = packetv5:do_client_connect(PubConnect, PubConnack, []),

    Pub = packetv5:gen_publish(Topic, 0, <<"retained message">>,
                               [{retain, true},
                                {properties, Properties}]),
    ok = gen_tcp:send(PubSocket, Pub),

    %% setup subscriber
    SubConnect = packetv5:gen_connect(vmq_cth:ustr(Cfg) ++ "-sub", [{keepalive, 60}]),
    SubConnack = packetv5:gen_connack(0, ?M5_CONNACK_ACCEPT, #{}),
    {ok, SubSocket} = packetv5:do_client_connect(SubConnect, SubConnack, []),
    Subscribe = packetv5:gen_subscribe(77, [packetv5:gen_subtopic(Topic,0)], #{}),
    ok = gen_tcp:send(SubSocket, Subscribe),
    SubAck = packetv5:gen_suback(77, [0], #{}),
    ok = packetv5:expect_frame(SubSocket, SubAck),
    ok = packetv5:expect_frame(SubSocket, Pub).

retain_with_message_expiry(Cfg) ->
    %% set up publisher
    PubConnect = packetv5:gen_connect(vmq_cth:ustr(Cfg) ++ "-pub", [{keepalive, 60}]),
    Connack = packetv5:gen_connack(?M5_CONNACK_ACCEPT),
    {ok, PubSocket} = packetv5:do_client_connect(PubConnect, Connack, []),

    %% Publish some messages
    Expiry60s = #{p_message_expiry_interval => 60},
    PE60s = packetv5:gen_publish(<<"message/expiry/60s">>, 1, <<"e60s">>,
                                 [{properties, Expiry60s}, {mid, 0},
                                  {retain, true}]),
    ok = gen_tcp:send(PubSocket, PE60s),
    Puback0 = packetv5:gen_puback(0),
    ok = packetv5:expect_frame(PubSocket, Puback0),

    Expiry1s = #{p_message_expiry_interval => 1},
    PE1s = packetv5:gen_publish(<<"message/expiry/1s">>, 1, <<"e1s">>,
                                [{properties, Expiry1s}, {mid, 1},
                                 {retain, true}]),
    ok = gen_tcp:send(PubSocket, PE1s),
    Puback1 = packetv5:gen_puback(1),
    ok = packetv5:expect_frame(PubSocket, Puback1),
    ok = gen_tcp:close(PubSocket),

    %% Wait a bit to ensure the messages will have been held long
    %% enough in the retained table
    timer:sleep(1100),

    %% set up subscriber
    SubConnect = packetv5:gen_connect(vmq_cth:ustr(Cfg) ++ "-sub",
                                      [{keepalive, 60},
                                       {clean_start,true}]),
    {ok, SubSocket} = packetv5:do_client_connect(SubConnect, Connack, []),
    SubTopic60s = packetv5:gen_subtopic(<<"message/expiry/60s">>, 0),
    SubTopic1s = packetv5:gen_subtopic(<<"message/expiry/1s">>, 0),
    SubscribeAll = packetv5:gen_subscribe(10, [SubTopic60s, SubTopic1s], #{}),
    ok = gen_tcp:send(SubSocket, SubscribeAll),
    SubAck = packetv5:gen_suback(10, [0,0], #{}),
    ok = packetv5:expect_frame(SubSocket, SubAck),

    %% receive the message with a long expiry interval
    {ok, RPE60s, <<>>} = packetv5:receive_frame(SubSocket),
    #mqtt5_publish{topic = [<<"message">>, <<"expiry">>, <<"60s">>],
                   qos = 0,
                   retain = 1,
                   properties = #{p_message_expiry_interval := Remaining}} = RPE60s,
    true = Remaining < 60,

    %% The 1s message shouldn't arrive, but let's just block a bit to
    %% make sure.
    {error, timeout} = gen_tcp:recv(SubSocket, 0, 500),

    %% check that a message was really expired:
    1 = vmq_metrics:counter_val(queue_message_expired),

    ok = gen_tcp:close(SubSocket).

subscribe_retain_as_published_test(Cfg) ->
    %% Bit 3 of the Subscription Options represents the Retain As
    %% Published option. If 1, Application Messages forwarded using
    %% this subscription keep the RETAIN flag they were published
    %% with. If 0, Application Messages forwarded using this
    %% subscription have the RETAIN flag set to 0. Retained messages
    %% sent when the subscription is established have the RETAIN flag
    %% set to 1.

    %% [MQTT-3.3.1-12]   If  the   value   of   Retain  As   Published
    %% subscription option is set to 0, the Server MUST set the RETAIN
    %% flag to 0 when forwarding  an Application Message regardless of
    %% how the RETAIN flag was set in the received PUBLISH packet.


    %% [MQTT-3.3.1-13] If the value of Retain As Published
    %% subscription option is set to 1, the Server MUST set the RETAIN
    %% flag equal to the RETAIN flag in the received PUBLISH packet.

    ClientId = vmq_cth:ustr(Cfg),
    RH = send_retain, NL = false,

    TopicRapTrue = vmq_cth:utopic(Cfg) ++ "/retaspubtrue",
    SubTopicRapTrue = packetv5:gen_subtopic(TopicRapTrue, 0,  NL, true, RH),
    SubscribeRapTrue = packetv5:gen_subscribe(77, [SubTopicRapTrue], #{}),
    SubAck77 = packetv5:gen_suback(77, [0], #{}),

    TopicRapFalse = vmq_cth:utopic(Cfg) ++ "/retaspubfalse",
    SubTopicRapFalse = packetv5:gen_subtopic(TopicRapFalse, 0,  NL, false, RH),
    SubscribeRapFalse = packetv5:gen_subscribe(78, [SubTopicRapFalse], #{}),
    SubAck78 = packetv5:gen_suback(78, [0], #{}),

    Connect = packetv5:gen_connect(ClientId, [{clean_start, true}]),
    Connack = packetv5:gen_connack(),
    {ok, Socket} = packetv5:do_client_connect(Connect, Connack, []),

    ok = gen_tcp:send(Socket, SubscribeRapTrue),
    ok = packetv5:expect_frame(Socket, SubAck77),
    ok = gen_tcp:send(Socket, SubscribeRapFalse),
    ok = packetv5:expect_frame(Socket, SubAck78),

    %% Test subscription with RAP true
    PublishRetainedRapTrue = packetv5:gen_publish(TopicRapTrue, 0, <<"msg1">>, [{retain, true}]),
    PublishRapTrue = packetv5:gen_publish(TopicRapTrue, 0, <<"msg2">>, []),
    ok = gen_tcp:send(Socket, PublishRetainedRapTrue),
    ok = gen_tcp:send(Socket, PublishRapTrue),

    %% Rap True, published messages keep original retain flag
    {ok, #mqtt5_publish{
            retain = 1,
            payload = <<"msg1">>
           }, Rest0} = packetv5:receive_frame(Socket),
    {ok, #mqtt5_publish{
            retain = 0,
            payload = <<"msg2">>
           }, <<>>} = packetv5:receive_frame(gen_tcp, Socket, 5000, Rest0),

    %% Test subscription with RAP false
    PublishRetainedRapFalse = packetv5:gen_publish(TopicRapFalse, 0, <<"msg3">>, [{retain, true}]),
    PublishRapFalse = packetv5:gen_publish(TopicRapFalse, 0, <<"msg4">>, []),
    ok = gen_tcp:send(Socket, PublishRetainedRapFalse),
    ok = gen_tcp:send(Socket, PublishRapFalse),

    %% RAP false, published retained messages lose the retain flag
    {ok, #mqtt5_publish{
            retain = 0,
            payload = <<"msg3">>
           }, Rest1} = packetv5:receive_frame(Socket),
    {ok, #mqtt5_publish{
            retain = 0,
            payload = <<"msg4">>
           }, <<>>} = packetv5:receive_frame(gen_tcp, Socket, 5000, Rest1),

    Disconnect = packetv5:gen_disconnect(),
    ok = gen_tcp:send(Socket, Disconnect),
    ok = gen_tcp:close(Socket).

subscribe_retain_handling_flags_test(Cfg) ->
    %% Bits 4 and 5 of the Subscription Options represent the Retain
    %% Handling option. This option specifies whether retained
    %% messages are sent when the subscription is established. This
    %% does not affect the sending of retained messages at any point
    %% after the subscribe. If there are no retained messages matching
    %% the Topic Filter, all of these values act the same. The values
    %% are:
    %%
    %% 0 = Send retained messages at the time of the subscribe
    %%
    %% 1 = Send retained messages at subscribe only if the
    %% subscription does not currently exist
    %%
    %% 2 = Do not send retained messages at the time of the subscribe
    %%
    %% It is a Protocol Error to send a Retain Handling value of 3.

    %% [MQTT-3.3.1-9] If Retain Handling is set to 0 the Server MUST
    %% send the retained messages matching the Topic Filter of the
    %% subscription to the Client.

    %% [MQTT-3.3.1-10] If Retain Handling is set to 1 then if the
    %% subscription did not already exist, the Server MUST send all
    %% retained message matching the Topic Filter of the subscription
    %% to the Client, and if the subscription did exist the Server
    %% MUST NOT send the retained messages.

    %% [MQTT-3.3.1-11] If Retain Handling is set to 2, the Server MUST
    %% NOT send the retained message.

    Topic = vmq_cth:utopic(Cfg),
    ClientId = vmq_cth:ustr(Cfg),
    Connect = packetv5:gen_connect(ClientId, [{clean_start, true}]),
    Connack = packetv5:gen_connack(),
    Disconnect = packetv5:gen_disconnect(),
    PublishRetained = packetv5:gen_publish(Topic, 0, <<"a retained message">>, [{retain, true}]),

    SubFun =
        fun(Socket, T, Mid, RH) ->
                NL = false,
                Rap = false,
                SubTopic = packetv5:gen_subtopic(T, 0,  NL, Rap, RH),
                Subscribe = packetv5:gen_subscribe(Mid, [SubTopic], #{}),
                SubAck = packetv5:gen_suback(Mid, [0], #{}),
                ok = gen_tcp:send(Socket, Subscribe),
                ok = packetv5:expect_frame(Socket, SubAck),
                ok
        end,

    %% publish a retained msg
    {ok, Socket0} = packetv5:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket0, PublishRetained),

    %% send_retain
    %% Subscribe to topic and receive message
    ok = SubFun(Socket0, Topic, 1, send_retain),
    ok = packetv5:expect_frame(Socket0, PublishRetained),
    ok = gen_tcp:send(Socket0, Disconnect),
    ok = gen_tcp:close(Socket0),

    %% dont_send
    {ok, Socket3} = packetv5:do_client_connect(Connect, Connack, []),
    ok = SubFun(Socket3, Topic, 0, dont_send),
    {error, timeout} = gen_tcp:recv(Socket3, 0, 100),

    %% send_if_new_sub
    %% subscribe twice and don't get the message the second time
    {ok, Socket1} = packetv5:do_client_connect(Connect, Connack, []),

    ok = SubFun(Socket1, Topic, 1, send_if_new_sub),
    ok = packetv5:expect_frame(Socket1, PublishRetained),
    ok = SubFun(Socket1, Topic, 2, send_if_new_sub),
    {error, timeout} = gen_tcp:recv(Socket1, 0, 100),
    ok = SubFun(Socket1, Topic, 3, send_if_new_sub),
    {error, timeout} = gen_tcp:recv(Socket1, 0, 100),
    ok = gen_tcp:send(Socket1, Disconnect).

subscribe_retain_subid_test(Cfg) ->
    %% test that retained messages delivered on subscription contain
    %% the correct subscription identifiers.
    Topic = vmq_cth:utopic(Cfg),
    ClientId = vmq_cth:ustr(Cfg),

    %% publish a retained msgs
    Connect = packetv5:gen_connect(ClientId, [{clean_start, true}]),
    Connack = packetv5:gen_connack(),
    Disconnect = packetv5:gen_disconnect(),
    PublishRetained = packetv5:gen_publish(Topic, 1, <<"a retained message">>, [{retain, true},
                                                                                {mid, 7}]),
    Puback = packetv5:gen_puback(7),
    {ok, Socket0} = packetv5:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(Socket0, PublishRetained),
    ok = packetv5:expect_frame(Socket0, Puback),

    %% subscribe to retained msg topic
    SubTopic = packetv5:gen_subtopic(Topic, 0,  false, false, send_retain),
    Subscribe = packetv5:gen_subscribe(8, [SubTopic], #{p_subscription_id => [5]}),
    SubAck = packetv5:gen_suback(8, [0], #{}),
    ok = gen_tcp:send(Socket0, Subscribe),
    ok = packetv5:expect_frame(Socket0, SubAck),

    %% see that we get the retained messages with the correct subid.
    PubRet = packetv5:gen_publish(Topic, 0, <<"a retained message">>, [{retain, true},
                                                                       {properties,
                                                                        #{p_subscription_id => [5]}}]),
    ok = packetv5:expect_frame(Socket0, PubRet),

    ok = gen_tcp:send(Socket0, Disconnect),
    ok = gen_tcp:close(Socket0).

retain_compat_pre_test(_Cfg) ->
    Pre = <<"msg">>,
    Pre = vmq_reg:retain_pre(Pre),

    Future = {retain_msg, 1, <<"future_msg">>, something, else},
    <<"future_msg">> = vmq_reg:retain_pre(Future).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
hook_auth_on_subscribe(_,_,_) -> ok.
hook_auth_on_publish(_, _, _, _, _, _) -> ok.
hook_on_message_drop(_, _, expired) -> ok.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Helper
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
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
enable_on_message_drop() ->
    ok = vmq_plugin_mgr:enable_module_plugin(
           on_message_drop, ?MODULE, hook_on_message_drop, 3).

disable_on_subscribe() ->
    ok = vmq_plugin_mgr:disable_module_plugin(
           auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3),
    ok = vmq_plugin_mgr:disable_module_plugin(
           auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3,
           [{compat, {auth_on_subscribe_m5, vmq_plugin_compat_m5,
                      convert, 4}}]).
disable_on_publish() ->
    ok = vmq_plugin_mgr:disable_module_plugin(
           auth_on_publish, ?MODULE, hook_auth_on_publish, 6),
    ok = vmq_plugin_mgr:disable_module_plugin(
           auth_on_publish, ?MODULE, hook_auth_on_publish, 6,
           [{compat, {auth_on_publish_m5, vmq_plugin_compat_m5,
                      convert, 7}}]).
disable_on_message_drop() ->
    ok = vmq_plugin_mgr:disable_module_plugin(
           on_message_drop, ?MODULE, hook_on_message_drop, 3).

