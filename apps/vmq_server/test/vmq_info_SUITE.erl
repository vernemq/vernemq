-module(vmq_info_SUITE).

-compile([nowarn_export_all,export_all]).

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
    vmq_test_utils:setup(),
    enable_on_subscribe(),
    enable_on_publish(),
    vmq_server_cmd:set_config(allow_anonymous, true),
    vmq_server_cmd:set_config(retry_interval, 10),
    vmq_server_cmd:listener_start(1888, []),
    Config.

end_per_testcase(_TestCase, _Config) ->
    vmq_test_utils:teardown(),
    ok.

groups() ->
    [].

all() ->
    [filtering_works].

filtering_works(_Config) ->
    Connect = packet:gen_connect("vmq-info-client", [{keepalive,60},{clean_session, false}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(1, [{"some/topic/1", 0},
                                         {"with/wildcard/#",1},
                                         {"with/+/wildcard",2}]),
    Suback = packet:gen_suback(1, [0,1,2]),

    {ok, SubSocket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:send(SubSocket, Subscribe),
    ok = packet:expect_packet(SubSocket, "suback", Suback),

    [#{session_pid := Pid}] =
        execute(["vmq-admin", "session", "show", "--session_pid"]),
    [#{session_pid := Pid}] =
        execute(["vmq-admin", "session", "show",
                 "--session_pid=" ++ pid_to_list(Pid), "--session_pid"]),

    [#{client_id := <<"vmq-info-client">>, is_offline := false}] =
        execute(["vmq-admin", "session", "show",
                 "--is_offline=false", "--client_id", "--is_offline"]),

    [#{client_id := <<"vmq-info-client">>, is_online := true}] =
        execute(["vmq-admin", "session", "show",
                 "--is_online=true", "--client_id", "--is_online"]),

    [#{client_id := <<"vmq-info-client">>, statename := online}] =
        execute(["vmq-admin", "session", "show", "--statename=online",
                "--statename", "--client_id"]),

    [#{client_id := <<"vmq-info-client">>}] =
        execute(["vmq-admin", "session", "show", "--deliver_mode=fanout",
                "--deliver_mode", "--client_id"]),

    [#{client_id := <<"vmq-info-client">>, offline_messages := 0}] =
        execute(["vmq-admin", "session", "show", "--offline_messages=0",
                "--offline_messages", "--client_id"]),

    [#{client_id := <<"vmq-info-client">>, online_messages := 0}] =
        execute(["vmq-admin", "session", "show", "--online_messages=0",
                 "--online_messages", "--client_id"]),

    [#{client_id := <<"vmq-info-client">>, num_sessions := 1}] =
        execute(["vmq-admin", "session", "show", "--num_sessions=1",
                 "--num_sessions", "--client_id"]),

    [#{client_id := <<"vmq-info-client">>, clean_session := false}] =
        execute(["vmq-admin", "session", "show", "--clean_session=false",
                 "--clean_session", "--client_id"]),

    [#{client_id := <<"vmq-info-client">>, is_plugin := false}] =
        execute(["vmq-admin", "session", "show", "--is_plugin=false",
                 "--is_plugin", "--client_id"]),

    [#{client_id := <<"vmq-info-client">>, user := undefined}] =
        execute(["vmq-admin", "session", "show", "--user=undefined",
                 "--user", "--client_id"]),

    [#{client_id := <<"vmq-info-client">>, peer_host := PeerHost}] =
        execute(["vmq-admin", "session", "show", "--client_id", "--peer_host"]),
    [#{client_id := <<"vmq-info-client">>, peer_host := PeerHost}] =
        execute(["vmq-admin", "session", "show", "--peer_host=" ++ binary_to_list(PeerHost),
                 "--peer_host", "--client_id"]),

    [#{client_id := <<"vmq-info-client">>, peer_port := PeerPort}] =
        execute(["vmq-admin", "session", "show", "--client_id", "--peer_port"]),
    [#{client_id := <<"vmq-info-client">>, peer_port := PeerPort}] =
        execute(["vmq-admin", "session", "show", "--peer_port=" ++ integer_to_list(PeerPort),
                 "--peer_port", "--client_id"]),

    [#{client_id := <<"vmq-info-client">>, protocol := 3}] =
        execute(["vmq-admin", "session", "show", "--protocol=3",
                 "--protocol", "--client_id"]),

    [#{client_id := <<"vmq-info-client">>, waiting_acks := 0}] =
        execute(["vmq-admin", "session", "show", "--waiting_acks=0",
                 "--waiting_acks", "--client_id"]),

    [#{topic := <<"some/topic/1">>, qos := 0}] =
        execute(["vmq-admin", "session", "show", "--topic=some/topic/1",
                 "--topic", "--qos"]),
    [#{topic := <<"some/topic/1">>, qos := 0}] =
        execute(["vmq-admin", "session", "show", "--qos=0",
                 "--topic", "--qos"]),

    [#{topic := <<"with/wildcard/#">>, qos := 1}] =
        execute(["vmq-admin", "session", "show", "--topic=with/wildcard/#",
                 "--topic", "--qos"]),
    [#{topic := <<"with/wildcard/#">>, qos := 1}] =
        execute(["vmq-admin", "session", "show", "--qos=1",
                 "--topic", "--qos"]),

    [#{topic := <<"with/+/wildcard">>, qos := 2}] =
        execute(["vmq-admin", "session", "show", "--topic=with/+/wildcard",
                 "--topic", "--qos"]),
    [#{topic := <<"with/+/wildcard">>, qos := 2}] =
        execute(["vmq-admin", "session", "show", "--qos=2",
                 "--topic", "--qos"]),

    Disconnect = packet:gen_disconnect(),
    ok = gen_tcp:send(SubSocket, Disconnect),
    ok = gen_tcp:close(SubSocket),

    %% Publish a message for the offline client
    ConPut = packet:gen_connect("vmq-info-client-pub", [{keepalive, 60}]),
    Publish = packet:gen_publish("with/wildcard/qos1", 1, <<"thepayload">>, [{mid, 1}]),
    Puback = packet:gen_puback(1),
    {ok, PubSocket} = packet:do_client_connect(ConPut, Connack, []),
    ok = gen_tcp:send(PubSocket, Publish),
    ok = packet:expect_packet(PubSocket, "puback", Puback),

    %% offline msg related tests
    [#{client_id := <<"vmq-info-client">>, msg_ref := _MsgRef}] =
        execute(["vmq-admin", "session", "show", "--client_id", "--msg_ref"]),

    %% Currently filtering by msgref is broken as (apparently) the
    %% `==` b64 encoding does not work with clique and is stripped away...
    %% [#{client_id := <<"vmq-info-client">>, msg_ref := MsgRef}] =
    %%     execute(["vmq-admin", "session", "show", "--client_id", "--msg_ref=" ++ binary_to_list(MsgRef)]),

    [#{client_id := <<"vmq-info-client">>, msg_qos := 1}] =
        execute(["vmq-admin", "session", "show", "--msg_qos=1",
                 "--client_id", "--msg_qos"]),

    [#{client_id := <<"vmq-info-client">>, routing_key := <<"with/wildcard/qos1">>}] =
        execute(["vmq-admin", "session", "show", "--routing_key=with/wildcard/qos1",
                 "--client_id", "--routing_key"]),

    [#{client_id := <<"vmq-info-client">>, dup := false}] =
        execute(["vmq-admin", "session", "show", "--dup=false",
                 "--client_id", "--dup"]),

    [#{client_id := <<"vmq-info-client">>, payload := <<"thepayload">>}] =
        execute(["vmq-admin", "session", "show", "--payload=thepayload",
                 "--client_id", "--payload"]),
    ok = gen_tcp:send(PubSocket, Disconnect),
    ok = gen_tcp:close(PubSocket).

execute(Cmd) ->
    M0 = clique_command:match(Cmd),
    M1 = clique_parser:parse(M0),
    M2 = clique_parser:extract_global_flags(M1),
    M3 = clique_parser:validate(M2),
    {[{table, Res}],_,_} = clique_command:run(M3),
    [maps:from_list(Res1) || Res1 <- Res].

enable_on_subscribe() ->
    vmq_plugin_mgr:enable_module_plugin(
      auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3).
disable_on_subscribe() ->
    vmq_plugin_mgr:disable_module_plugin(
      auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3).

enable_on_publish() ->
    vmq_plugin_mgr:enable_module_plugin(
      auth_on_publish, ?MODULE, hook_auth_on_publish, 6).
disable_on_publish() ->
    vmq_plugin_mgr:disable_module_plugin(
      auth_on_publish, ?MODULE, hook_auth_on_publish, 6).

hook_auth_on_subscribe(_, _, _) -> ok.
hook_auth_on_publish(_,_,_,_,_,_) ->
    ok.
