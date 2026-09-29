-module(vmq_queue_hooks_SUITE).

-include_lib("vmq_commons/src/vmq_types_mqtt5.hrl").

-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([queue_hooks_lifecycle_test1/1,
         queue_hooks_lifecycle_test2/1,
         queue_hooks_lifecycle_test3/1,
         queue_hooks_lifecycle_test4/1,
         queue_hooks_lifecycle_test5/1,
         queue_hooks_lifecycle_test6/1,
         disconnect_reason_v4_client_disconnect_test/1,
         disconnect_reason_v5_client_disconnect_test/1,
         disconnect_reason_v5_client_reason_code_test/1,
         disconnect_reason_v5_tcp_closed_test/1,
         disconnect_reason_v5_keepalive_test/1,
         disconnect_reason_v5_session_taken_over_test/1]).

-export([hook_auth_on_subscribe/4,
         hook_auth_on_publish/7,
         hook_on_client_gone/4,
         hook_on_client_offline/4,
         hook_on_client_wakeup/2,
         hook_on_session_expired/2,
         hook_on_offline_message/6,
         hook_on_topic_unsubscribed/2]).

-ifdef(nowarn_gen_fsm).
-compile([{nowarn_deprecated_function,
           [
                {gen_fsm,send_event,2},
                {gen_fsm,sync_send_all_state_event,2}
            ]}]).
-endif.

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(_Config) ->
    cover:start(),
    _Config.

end_per_suite(_Config) ->
    _Config.

init_per_testcase(_Case, Config) ->
    vmq_test_utils:setup(),
    vmq_server_cmd:set_config(allow_anonymous, true),
    vmq_server_cmd:set_config(retry_interval, 10),
    vmq_server_cmd:listener_start(1888, [{allowed_protocol_versions, "3,4,5"}]),
    ets:new(?MODULE, [public, named_table]),
    enable_on_publish(),
    enable_on_subscribe(),
    enable_queue_hooks(),
    Config.

end_per_testcase(_, Config) ->
    disable_queue_hooks(),
    disable_on_subscribe(),
    disable_on_publish(),
    vmq_test_utils:teardown(),
    ets:delete(?MODULE),
    Config.

all() ->
    [queue_hooks_lifecycle_test1,
     queue_hooks_lifecycle_test2,
     queue_hooks_lifecycle_test3,
     queue_hooks_lifecycle_test4,
     queue_hooks_lifecycle_test5,
     queue_hooks_lifecycle_test6,
     disconnect_reason_v4_client_disconnect_test,
     disconnect_reason_v5_client_disconnect_test,
     disconnect_reason_v5_client_reason_code_test,
     disconnect_reason_v5_tcp_closed_test,
     disconnect_reason_v5_keepalive_test,
     disconnect_reason_v5_session_taken_over_test].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

queue_hooks_lifecycle_test1(_) ->
    Connect = packet:gen_connect("queue-client", [{keepalive, 60}]),
    Connack = packet:gen_connack(0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),

    ok = hook_called(on_client_wakeup),
    ExpectedSessionId = get_queue_session_id(),

    gen_tcp:close(Socket),
    ok = hook_called(on_topic_unsubscribed),
    ok = hook_called(on_client_gone),
    [{session_id_gone, ExpectedSessionId}] = ets:lookup(?MODULE, session_id_gone).

queue_hooks_lifecycle_test2(_) ->
    Connect = packet:gen_connect("queue-client", [{keepalive, 60}, {clean_session, false}]),
    Connack = packet:gen_connack(0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),

    ok = hook_called(on_client_wakeup),
    ExpectedSessionId = get_queue_session_id(),

    gen_tcp:close(Socket),
    ok = hook_called(on_client_offline),
    [{session_id_offline, ExpectedSessionId}] = ets:lookup(?MODULE, session_id_offline).

queue_hooks_lifecycle_test3(_) ->
    Connect = packet:gen_connect("queue-client", [{keepalive, 60}, {clean_session, false}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(3265, "queue/hook/test", 1),
    Suback = packet:gen_suback(3265, 1),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),

    ok = hook_called(on_client_wakeup),
    ExpectedSessionId = get_queue_session_id(),

    gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),

    gen_tcp:close(Socket),
    ok = hook_called(on_client_offline),
    [{session_id_offline, ExpectedSessionId}] = ets:lookup(?MODULE, session_id_offline),

    %% publish an offline message
    Connect1 = packet:gen_connect("queue-pub-client", [{keepalive, 60}]),
    Connack1 = packet:gen_connack(0),
    {ok, Socket1} = packet:do_client_connect(Connect1, Connack1, []),
    Publish = packet:gen_publish("queue/hook/test", 1, <<"message">>, [{mid, 19}]),
    Puback = packet:gen_puback(19),

    gen_tcp:send(Socket1, Publish),
    ok = packet:expect_packet(Socket1, "puback", Puback),
    gen_tcp:close(Socket1),
    ok = hook_called(on_offline_message),
    [{session_id_offline_msg, ExpectedSessionId}] = ets:lookup(?MODULE, session_id_offline_msg).

queue_hooks_lifecycle_test4(_) ->
    Connect = packet:gen_connect("queue-client",
                                 [{keepalive, 60}, {clean_session, false}]),
    Connack = packet:gen_connack(0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = hook_called(on_client_wakeup),

    ExpectedSessionId = get_queue_session_id(),
    gen_tcp:close(Socket),
    ok = hook_called(on_client_offline),
    [{session_id_offline, ExpectedSessionId}] = ets:lookup(?MODULE, session_id_offline),
    QPid = vmq_queue_sup_sup:get_queue_pid({"" , <<"queue-client">>}),
    ok = gen_fsm:send_event(QPid, expire_session),
    ok = hook_called(on_topic_unsubscribed),
    ok = hook_called(on_session_expired),
    [{session_id_expired, ExpectedSessionId}] = ets:lookup(?MODULE, session_id_expired).

queue_hooks_lifecycle_test5(_) ->
    Connect = packet:gen_connect("queue-client",
                                 [{keepalive, 60}, {clean_session, false}]),
    Connack = packet:gen_connack(0),
    {ok, _Socket} = packet:do_client_connect(Connect, Connack, []),
    QPid = vmq_queue_sup_sup:get_queue_pid({"", <<"queue-client">>}),
    ok = gen_fsm:sync_send_all_state_event(QPid, {force_disconnect, test, true}),
    ok = hook_called(on_topic_unsubscribed).

queue_hooks_lifecycle_test6(_) ->
    Connect = packet:gen_connect("queue-client",
                                 [{keepalive, 60}, {clean_session, true}]),
    Connack = packet:gen_connack(0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    Connect1 = packet:gen_connect("queue-client-2",
                                 [{keepalive, 60}, {clean_session, false}]),
    Connack1 = packet:gen_connack(0),
    {ok, _} = packet:do_client_connect(Connect1, Connack1, []),
    QPid = vmq_queue_sup_sup:get_queue_pid({"", <<"queue-client">>}),
    OtherQPid = vmq_queue_sup_sup:get_queue_pid({"", <<"queue-client-2">>}),
    ok = vmq_queue:migrate(QPid, OtherQPid),
    gen_tcp:close(Socket),
    ok = hook_called(on_topic_unsubscribed).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Disconnect reasons reported to on_client_gone
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% pins the existing v3.1.1 behaviour, so that the shared reason table
%% can be extended for MQTT 5 without moving it
disconnect_reason_v4_client_disconnect_test(_) ->
    Connect = packet:gen_connect("queue-client", [{keepalive, 60}]),
    Connack = packet:gen_connack(0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = hook_called(on_client_wakeup),
    ok = gen_tcp:send(Socket, packet:gen_disconnect()),
    ok = gen_tcp:close(Socket),
    'REASON_MQTT_CLIENT_DISCONNECT' = disconnect_reason(reason_gone).

%% a plain MQTT 5 DISCONNECT says no more than v3.1.1 does, and is
%% reported the same way
disconnect_reason_v5_client_disconnect_test(_) ->
    {ok, Socket} = connect_v5("queue-client"),
    ok = gen_tcp:send(Socket, packetv5:gen_disconnect()),
    ok = gen_tcp:close(Socket),
    'REASON_MQTT_CLIENT_DISCONNECT' = disconnect_reason(reason_gone).

%% ...but when the client says why, that is what gets reported
disconnect_reason_v5_client_reason_code_test(_) ->
    {ok, Socket} = connect_v5("queue-client"),
    ok = gen_tcp:send(Socket, packetv5:gen_disconnect(?M5_PACKET_TOO_LARGE, #{})),
    ok = gen_tcp:close(Socket),
    'REASON_PACKET_TOO_LARGE' = disconnect_reason(reason_gone).

disconnect_reason_v5_tcp_closed_test(_) ->
    {ok, Socket} = connect_v5("queue-client"),
    ok = gen_tcp:close(Socket),
    'REASON_TCP_CLOSED' = disconnect_reason(reason_gone).

disconnect_reason_v5_keepalive_test(_) ->
    Connect = packetv5:gen_connect("queue-client", [{keepalive, 1}]),
    {ok, Socket} = packetv5:do_client_connect(Connect, packetv5:gen_connack(), []),
    ok = hook_called(on_client_wakeup),
    %% say nothing until the broker gives up on us
    'REASON_DISCONNECT_KEEP_ALIVE' = disconnect_reason(reason_gone),
    ok = gen_tcp:close(Socket).

disconnect_reason_v5_session_taken_over_test(_) ->
    {ok, Socket} = connect_v5("queue-client"),
    {ok, NewSocket} = connect_v5("queue-client"),
    'REASON_SESSION_TAKEN_OVER' = disconnect_reason(reason_gone),
    ok = gen_tcp:close(Socket),
    ok = gen_tcp:close(NewSocket).

connect_v5(ClientId) ->
    Connect = packetv5:gen_connect(ClientId, [{keepalive, 60}]),
    {ok, Socket} = packetv5:do_client_connect(Connect, packetv5:gen_connack(), []),
    ok = hook_called(on_client_wakeup),
    {ok, Socket}.

%% the hooks fire asynchronously once the queue notices the session is
%% down, so wait for the reason rather than reading it straight away
disconnect_reason(Key) ->
    case ets:lookup(?MODULE, Key) of
        [] ->
            timer:sleep(50),
            disconnect_reason(Key);
        [{Key, Reason}] ->
            Reason
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks (as explicit as possible)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
hook_called(Hook) ->
    case ets:lookup(?MODULE, Hook) of
        [] ->
            timer:sleep(50),
            hook_called(Hook);
        [{Hook, true}] -> ok
    end.

hook_auth_on_subscribe(_, _, _, _) -> ok.

hook_auth_on_publish(_, _, _, _, _, _, _) -> ok.

hook_on_client_wakeup({"" , <<"queue-client">>}, SessionId) ->
    ets:insert(?MODULE, {on_client_wakeup, true}),
    ets:insert(?MODULE, {session_id, SessionId});
hook_on_client_wakeup(_, _) ->
    ok.

hook_on_client_gone({"" , <<"queue-client">>}, Reason, _, SessionId) ->
    ets:insert(?MODULE, {on_client_gone, true}),
    ets:insert(?MODULE, {reason_gone, Reason}),
    ets:insert(?MODULE, {session_id_gone, SessionId});
hook_on_client_gone(_, _, _, _) ->
    ok.

hook_on_client_offline({"" , <<"queue-client">>}, Reason, _, SessionId) ->
    ets:insert(?MODULE, {on_client_offline, true}),
    ets:insert(?MODULE, {reason_offline, Reason}),
    ets:insert(?MODULE, {session_id_offline, SessionId});
hook_on_client_offline(_, _, _, _) ->
    ok.

hook_on_session_expired({"" , <<"queue-client">>}, SessionId) ->
    ets:insert(?MODULE, {on_session_expired, true}),
    ets:insert(?MODULE, {session_id_expired, SessionId});
hook_on_session_expired(_, _) ->
    ok.

hook_on_offline_message({"", <<"queue-client">>}, 1,
                        [<<"queue">>, <<"hook">>, <<"test">>], <<"message">>, false, SessionId) ->
    ets:insert(?MODULE, {on_offline_message, true}),
    ets:insert(?MODULE, {session_id_offline_msg, SessionId});
hook_on_offline_message(_, _, _, _, _, _) ->
    ok.

hook_on_topic_unsubscribed({"", <<"queue-client">>}, _) ->
    ets:insert(?MODULE, {on_topic_unsubscribed, true});
hook_on_topic_unsubscribed(_, _) ->
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Helper
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
enable_on_subscribe() ->
    vmq_plugin_mgr:enable_module_plugin(
      auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 4).
enable_on_publish() ->
    vmq_plugin_mgr:enable_module_plugin(
      auth_on_publish, ?MODULE, hook_auth_on_publish, 7).
disable_on_subscribe() ->
    vmq_plugin_mgr:disable_module_plugin(
      auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 4).
disable_on_publish() ->
    vmq_plugin_mgr:disable_module_plugin(
      auth_on_publish, ?MODULE, hook_auth_on_publish, 7).

enable_queue_hooks() ->
    vmq_plugin_mgr:enable_module_plugin(
      on_client_gone, ?MODULE, hook_on_client_gone, 4),
    vmq_plugin_mgr:enable_module_plugin(
      on_client_offline, ?MODULE, hook_on_client_offline, 4),
    vmq_plugin_mgr:enable_module_plugin(
      on_client_wakeup, ?MODULE, hook_on_client_wakeup, 2),
    vmq_plugin_mgr:enable_module_plugin(
      on_offline_message, ?MODULE, hook_on_offline_message, 6),
    vmq_plugin_mgr:enable_module_plugin(
      on_session_expired, ?MODULE, hook_on_session_expired, 2),
    vmq_plugin_mgr:enable_module_plugin(
        on_topic_unsubscribed, ?MODULE, hook_on_topic_unsubscribed, 2).

disable_queue_hooks() ->
    vmq_plugin_mgr:disable_module_plugin(
      on_client_gone, ?MODULE, hook_on_client_gone, 4),
    vmq_plugin_mgr:disable_module_plugin(
      on_client_offline, ?MODULE, hook_on_client_offline, 4),
    vmq_plugin_mgr:disable_module_plugin(
      on_client_wakeup, ?MODULE, hook_on_client_wakeup, 2),
    vmq_plugin_mgr:disable_module_plugin(
      on_offline_message, ?MODULE, hook_on_offline_message, 6),
    vmq_plugin_mgr:disable_module_plugin(
      on_session_expired, ?MODULE, hook_on_session_expired, 2),
    vmq_plugin_mgr:disable_module_plugin(
      on_topic_unsubscribed, ?MODULE, hook_on_topic_unsubscribed, 2).

get_queue_session_id() ->
    [{session_id, SessionId}] = ets:lookup(?MODULE, session_id),
    SessionId.