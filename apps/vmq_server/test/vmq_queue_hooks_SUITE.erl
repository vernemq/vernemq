-module(vmq_queue_hooks_SUITE).
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
         queue_hooks_lifecycle_test6/1]).

-export([hook_auth_on_subscribe/3,
         hook_auth_on_publish/6,
         hook_on_client_gone/1,
         hook_on_client_offline/1,
         hook_on_client_wakeup/1,
         hook_on_session_expired/1,
         hook_on_offline_message/5,
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
     queue_hooks_lifecycle_test6].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

queue_hooks_lifecycle_test1(_) ->
    Connect = packet:gen_connect("queue-client", [{keepalive, 60}]),
    Connack = packet:gen_connack(0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),

    ok = hook_called(on_client_wakeup),

    gen_tcp:close(Socket),
    ok = hook_called(on_topic_unsubscribed),
    ok = hook_called(on_client_gone).

queue_hooks_lifecycle_test2(_) ->
    Connect = packet:gen_connect("queue-client", [{keepalive, 60}, {clean_session, false}]),
    Connack = packet:gen_connack(0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),

    ok = hook_called(on_client_wakeup),

    gen_tcp:close(Socket),
    ok = hook_called(on_client_offline).

queue_hooks_lifecycle_test3(_) ->
    Connect = packet:gen_connect("queue-client", [{keepalive, 60}, {clean_session, false}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(3265, "queue/hook/test", 1),
    Suback = packet:gen_suback(3265, 1),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),

    ok = hook_called(on_client_wakeup),

    gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),

    gen_tcp:close(Socket),
    ok = hook_called(on_client_offline),

    %% publish an offline message
    Connect1 = packet:gen_connect("queue-pub-client", [{keepalive, 60}]),
    Connack1 = packet:gen_connack(0),
    {ok, Socket1} = packet:do_client_connect(Connect1, Connack1, []),
    Publish = packet:gen_publish("queue/hook/test", 1, <<"message">>, [{mid, 19}]),
    Puback = packet:gen_puback(19),

    gen_tcp:send(Socket1, Publish),
    ok = packet:expect_packet(Socket1, "puback", Puback),
    gen_tcp:close(Socket1),
    ok = hook_called(on_offline_message).

queue_hooks_lifecycle_test4(_) ->
    Connect = packet:gen_connect("queue-client",
                                 [{keepalive, 60}, {clean_session, false}]),
    Connack = packet:gen_connack(0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = hook_called(on_client_wakeup),
    gen_tcp:close(Socket),
    ok = hook_called(on_client_offline),
    QPid = vmq_queue_sup_sup:get_queue_pid({"", <<"queue-client">>}),
    ok = gen_fsm:send_event(QPid, expire_session),
    ok = hook_called(on_topic_unsubscribed),
    ok = hook_called(on_session_expired).

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
%%% Hooks (as explicit as possible)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
hook_called(Hook) ->
    case ets:lookup(?MODULE, Hook) of
        [] ->
            timer:sleep(50),
            hook_called(Hook);
        [{Hook, true}] -> ok
    end.

hook_auth_on_subscribe(_, _, _) -> ok.

hook_auth_on_publish(_, _, _, _, _, _) -> ok.

hook_on_client_wakeup({"", <<"queue-client">>}) ->
    ets:insert(?MODULE, {on_client_wakeup, true});
hook_on_client_wakeup(_) ->
    ok.

hook_on_client_gone({"", <<"queue-client">>}) ->
    ets:insert(?MODULE, {on_client_gone, true});
hook_on_client_gone(_) ->
    ok.

hook_on_client_offline({"", <<"queue-client">>}) ->
    ets:insert(?MODULE, {on_client_offline, true});
hook_on_client_offline(_) ->
    ok.

hook_on_session_expired({"", <<"queue-client">>}) ->
    ets:insert(?MODULE, {on_session_expired, true});
hook_on_session_expired(_) ->
    ok.

hook_on_offline_message({"", <<"queue-client">>}, 1,
                        [<<"queue">>, <<"hook">>, <<"test">>], <<"message">>, false) ->
    ets:insert(?MODULE, {on_offline_message, true});
hook_on_offline_message(_, _, _, _, _) ->
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
      auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3).
enable_on_publish() ->
    vmq_plugin_mgr:enable_module_plugin(
      auth_on_publish, ?MODULE, hook_auth_on_publish, 6).
disable_on_subscribe() ->
    vmq_plugin_mgr:disable_module_plugin(
      auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3).
disable_on_publish() ->
    vmq_plugin_mgr:disable_module_plugin(
      auth_on_publish, ?MODULE, hook_auth_on_publish, 6).

enable_queue_hooks() ->
    vmq_plugin_mgr:enable_module_plugin(
      on_client_gone, ?MODULE, hook_on_client_gone, 1),
    vmq_plugin_mgr:enable_module_plugin(
      on_client_offline, ?MODULE, hook_on_client_offline, 1),
    vmq_plugin_mgr:enable_module_plugin(
      on_client_wakeup, ?MODULE, hook_on_client_wakeup, 1),
    vmq_plugin_mgr:enable_module_plugin(
      on_offline_message, ?MODULE, hook_on_offline_message, 5),
    vmq_plugin_mgr:enable_module_plugin(
      on_session_expired, ?MODULE, hook_on_session_expired, 1),
    vmq_plugin_mgr:enable_module_plugin(
        on_topic_unsubscribed, ?MODULE, hook_on_topic_unsubscribed, 2).

disable_queue_hooks() ->
    vmq_plugin_mgr:disable_module_plugin(
      on_client_gone, ?MODULE, hook_on_client_gone, 1),
    vmq_plugin_mgr:disable_module_plugin(
      on_client_offline, ?MODULE, hook_on_client_offline, 1),
    vmq_plugin_mgr:disable_module_plugin(
      on_client_wakeup, ?MODULE, hook_on_client_wakeup, 1),
    vmq_plugin_mgr:disable_module_plugin(
      on_offline_message, ?MODULE, hook_on_offline_message, 5),
    vmq_plugin_mgr:disable_module_plugin(
      on_session_expired, ?MODULE, hook_on_session_expired, 1),
    vmq_plugin_mgr:disable_module_plugin(
      on_topic_unsubscribed, ?MODULE, hook_on_topic_unsubscribed, 2).
