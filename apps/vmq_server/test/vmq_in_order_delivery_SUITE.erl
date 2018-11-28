-module(vmq_in_order_delivery_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("vmq_commons/include/vmq_types.hrl").

-define(RETRY_INTERVAL, 2).
-define(NR_OF_MSGS, 400).

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(Config) ->
    cover:start(),
    vmq_test_utils:setup(),
    vmq_server_cmd:listener_start(1888, [{allowed_protocol_versions, "3,4,5"}]),

    enable_on_subscribe(),
    enable_on_publish(),
    [{ct_hooks, vmq_cth} | Config].

end_per_suite(_Config) ->
    disable_on_subscribe(),
    disable_on_publish(),
    vmq_test_utils:teardown(),
    _Config.

init_per_group(mqttv4, Config) ->
    [{protover, 4}|Config];
init_per_group(mqttv5, Config) ->
    [{protover, 5}|Config].

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    vmq_server_cmd:set_config(allow_anonymous, true),
    vmq_server_cmd:set_config(retry_interval, ?RETRY_INTERVAL),
    vmq_server_cmd:set_config(max_client_id_size, 100),
    Config.

end_per_testcase(_, Config) ->
    Config.

all() ->
    [
     {group, mqttv4}, % flow control in MQTT 3 & 4 is identical
     {group, mqttv5}
    ].

groups() ->
    Tests =
        [qos1_online,
         qos2_online,
         qos1_offline,
         qos2_offline],
    [
     {mqttv4, [shuffle], Tests},
     {mqttv5, [shuffle], [receive_max_broker | Tests]}
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

qos1_online(Config) ->
    lists:foreach(fun(I) -> qos1_online_test(I, Config) end, lists:seq(1, 30)).

qos2_online(Config) ->
    lists:foreach(fun(I) -> qos2_online_test(I, Config) end, lists:seq(1, 30)).

qos1_offline(Config) ->
    lists:foreach(fun(I) -> qos1_offline_test(I, Config) end, lists:seq(1, 30)).

qos2_offline(Config) ->
    lists:foreach(fun(I) -> qos2_offline_test(I, Config) end, lists:seq(1, 30)).

receive_max_broker(Config) ->
    ReceiveMax = ?NR_OF_MSGS div 2,
    set_flow_control_config(ReceiveMax, Config),
    %% enable client-to-broker flow control
    {ok, _} = vmq_server_cmd:set_config(receive_max_broker, ReceiveMax),
    Topic = vmq_cth:utopic(Config) ++ "/receive/max",
    %% this will publish ?NR_OF_MSGS
    Connack = packetv5:gen_connack(0, 0, #{p_receive_max => ReceiveMax}),
    %% Publish exactly `ReceiveMax` Msgs
    PubSocket = setup_pub(Topic, fun setup_pub_qos2_dont_ack/4, Connack, Config, ReceiveMax),
    %% Send another Publish, which won't make it through due to the receive_max
    Pub = mqtt5_v4compat:gen_publish(Topic, 2, <<"not going to happen">>, [{mid, ReceiveMax + 1}], Config),
    ok = gen_tcp:send(PubSocket, Pub),
    ok = mqtt5_v4compat:expect_packet(PubSocket, "disconnect", packetv5:gen_disconnect(?M5_RECEIVE_MAX_EXCEEDED, #{}), Config).

qos1_online_test(MaxInflightMsgs, Config) ->
    set_flow_control_config(MaxInflightMsgs, Config),
    Topic = vmq_cth:utopic(Config) ++ "/online/qos1",
    SubSocket = setup_con(Topic, 1, Config),
    PubSocket = setup_pub(Topic, fun setup_pub_qos1/4, Config),
    ok = recv_qos1(SubSocket, Topic, MaxInflightMsgs, Config),
    ok = gen_tcp:close(SubSocket),
    ok = gen_tcp:close(PubSocket),
    teardown_con(Config).

qos2_online_test(MaxInflightMsgs, Config) ->
    set_flow_control_config(MaxInflightMsgs, Config),
    Topic = vmq_cth:utopic(Config) ++ "/online/qos2",
    SubSocket = setup_con(Topic, 2, Config),
    PubSocket = setup_pub(Topic, fun setup_pub_qos2/4, Config),
    ok = recv_qos2(SubSocket, Topic, MaxInflightMsgs, Config),
    ok = gen_tcp:close(SubSocket),
    ok = gen_tcp:close(PubSocket),
    teardown_con(Config).

qos1_offline_test(MaxInflightMsgs, Config) ->
    set_flow_control_config(MaxInflightMsgs, Config),
    Topic = vmq_cth:utopic(Config) ++ "/off/qos1",
    SubSocket1 = setup_con(Topic, 1, Config),
    PubSocket = setup_pub(Topic, fun setup_pub_qos1/4, Config),
    gen_tcp:close(SubSocket1),
    SubSocket2 = setup_con(Topic, 1, true, Config), % session present
    ok = recv_qos1(SubSocket2, Topic, MaxInflightMsgs, true, Config), %% dup=true
    ok = gen_tcp:close(SubSocket2),
    ok = gen_tcp:close(PubSocket),
    teardown_con(Config).

qos2_offline_test(MaxInflightMsgs, Config) ->
    set_flow_control_config(MaxInflightMsgs, Config),
    Topic = vmq_cth:utopic(Config) ++ "/off/qos2",
    SubSocket1 = setup_con(Topic, 2, Config),
    PubSocket = setup_pub(Topic, fun setup_pub_qos2/4, Config),
    gen_tcp:close(SubSocket1),
    SubSocket2 = setup_con(Topic, 2, true, Config), % session present
    ok = recv_qos2(SubSocket2, Topic, MaxInflightMsgs, true, Config), %% dup=true
    ok = gen_tcp:close(SubSocket2),
    ok = gen_tcp:close(PubSocket),
    teardown_con(Config).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

set_flow_control_config(MaxInflightMsgs, Config) ->
    case mqtt5_v4compat:protover(Config) of
        4 ->
            {ok, _} = vmq_server_cmd:set_config(max_inflight_messages, MaxInflightMsgs);
        5 ->
            {ok, _} = vmq_server_cmd:set_config(receive_max_client, MaxInflightMsgs),
            %% Reset default value
            {ok, _} = vmq_server_cmd:set_config(receive_max_broker, 16#FFFF)
    end.

setup_pub(Topic, SetupFun, Config) ->
    Connack = mqtt5_v4compat:gen_connack(success, Config),
    setup_pub(Topic, SetupFun, Connack, Config, ?NR_OF_MSGS).
setup_pub(Topic, SetupFun, Connack, Config, NrOfMsgs) ->
    ClientId = vmq_cth:ustr(Config) ++ "in-order-pub",
    Connect = mqtt5_v4compat:gen_connect(ClientId, [{keepalive,60},
                                                    {clean_session, true}], Config),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Config),
    lists:foreach(fun(I) -> SetupFun(Socket, Topic, I, Config) end,
                  lists:seq(1, NrOfMsgs)),
    Socket.

setup_pub_qos1(Socket, Topic, I, Config) ->
    Payload = list_to_binary("msg" ++ integer_to_list(I)),
    Pub = mqtt5_v4compat:gen_publish(Topic, 1, Payload, [{mid, I}], Config),
    gen_tcp:send(Socket, Pub),
    ok = mqtt5_v4compat:expect_packet(Socket, "puback", mqtt5_v4compat:gen_puback(I, Config), Config).

setup_pub_qos2_dont_ack(Socket, Topic, I, Config) ->
    Payload = list_to_binary("msg" ++ integer_to_list(I)),
    Pub = mqtt5_v4compat:gen_publish(Topic, 2, Payload, [{mid, I}], Config),
    ok = gen_tcp:send(Socket, Pub),
    % consume the pubrec, but don't send the pubrel
    ok = mqtt5_v4compat:expect_packet(Socket, "pubrec", mqtt5_v4compat:gen_pubrec(I, Config), Config).

setup_pub_qos2(Socket, Topic, I, Config) ->
    Payload = list_to_binary("msg" ++ integer_to_list(I)),
    Pub = mqtt5_v4compat:gen_publish(Topic, 2, Payload, [{mid, I}], Config),
    gen_tcp:send(Socket, Pub),
    ok = mqtt5_v4compat:expect_packet(Socket, "pubrec", mqtt5_v4compat:gen_pubrec(I, Config), Config),
    ok = gen_tcp:send(Socket, packet:gen_pubrel(I)),
    ok = mqtt5_v4compat:expect_packet(Socket, "pubcomp", mqtt5_v4compat:gen_pubcomp(I, Config), Config).

setup_con(Topic, Qos, Config) ->
    setup_con(Topic, Qos, false, Config).
setup_con(Topic, Qos, SessionPresent, Config) ->
    ClientId = vmq_cth:ustr(Config) ++ "in-order-sub",
    Connect = mqtt5_v4compat:gen_connect(ClientId, [{keepalive,60},
                                                    {clean_session, false}], Config),
    Connack = mqtt5_v4compat:gen_connack(SessionPresent, success, Config),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Config),
    case SessionPresent of
        false ->
            Subscribe = mqtt5_v4compat:gen_subscribe(109, Topic, Qos, Config),
            Suback = mqtt5_v4compat:gen_suback(109, Qos, Config),
            ok = gen_tcp:send(Socket, Subscribe),
            ok = mqtt5_v4compat:expect_packet(Socket, "suback", Suback, Config);
        true ->
            ignore % no subscription necessary
    end,
    Socket.

teardown_con(Config) ->
    ClientId = vmq_cth:ustr(Config) ++ "in-order-sub",
    Connect = mqtt5_v4compat:gen_connect(ClientId, [{keepalive,60},
                                                    {clean_session, true}], Config),
    Connack = mqtt5_v4compat:gen_connack(success, Config),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Config),
    gen_tcp:close(Socket).


recv_qos1(Socket, Topic, MaxInflightMsgs, Config) ->
    recv_qos1(Socket, Topic, MaxInflightMsgs, false, Config).

recv_qos1(Socket, Topic, MaxInflightMsgs, Dup, Config) ->
    recv(Socket, Topic, 1, MaxInflightMsgs, Dup, Config,
         [fun recv_pub_qos1/5, fun send_puback/5]).

recv_qos2(Socket, Topic, MaxInflightMsgs, Config) ->
    recv_qos2(Socket, Topic, MaxInflightMsgs, false, Config).
recv_qos2(Socket, Topic, MaxInflightMsgs, Dup, Config) ->
    recv(Socket, Topic, 1, MaxInflightMsgs, Dup, Config,
         [fun recv_pub_qos2/5, fun send_pubrec/5,
          fun recv_pubrel_send_pubcomp/5]).

recv_pub_qos1(Socket, Topic, Id, Dup, Config) ->
    Payload = list_to_binary("msg" ++ integer_to_list(Id)),
    Pub = mqtt5_v4compat:gen_publish(Topic, 1, Payload, [{mid, Id}, {dup, Dup}], Config),
    ok = mqtt5_v4compat:expect_packet(Socket, "publish", Pub, Config).

recv_pub_qos2(Socket, Topic, Id, Dup, Config) ->
    Payload = list_to_binary("msg" ++ integer_to_list(Id)),
    Pub = mqtt5_v4compat:gen_publish(Topic, 2, Payload, [{mid, Id}, {dup, Dup}], Config),
    ok = mqtt5_v4compat:expect_packet(Socket, "publish", Pub, Config).

send_puback(Socket, _, Id, _, Config) ->
    ok = gen_tcp:send(Socket, mqtt5_v4compat:gen_puback(Id, Config)).

send_pubrec(Socket, _, Id, _, Config) ->
    ok = gen_tcp:send(Socket, mqtt5_v4compat:gen_pubrec(Id, Config)).

recv_pubrel_send_pubcomp(Socket, _, Id, _, Config) ->
    ok = mqtt5_v4compat:expect_packet(Socket, "pubrel", packet:gen_pubrel(Id), Config),
    ok = gen_tcp:send(Socket, mqtt5_v4compat:gen_pubcomp(Id, Config)).

recv(_, _, Id, _, _, _, _) when Id > ?NR_OF_MSGS -> ok;
recv(Socket, Topic, Id, MaxInflightMsgs, Dup, Config, Funs0) ->
    Funs1 = lists:foldr(fun(F, Acc) ->
                               [fun(I) -> F(Socket, Topic, I, is_dup(I =< MaxInflightMsgs, Dup), Config) end|Acc]
                        end, [], Funs0),
    multi_fold_range(Funs1, Id, Id + MaxInflightMsgs),
    recv(Socket, Topic, Id + MaxInflightMsgs, MaxInflightMsgs, Dup, Config, Funs0).

is_dup(true = _IsFirstWindow, Dup) -> Dup; % in first batch, Dup is whatever we have provided
is_dup(false = _IsFirstWindow, _Dup) -> false.

multi_fold_range(Funs, Start, Stop) ->
    lists:foreach(fun(F) -> fold_range(F, Start, Stop) end, Funs).

fold_range(Fun, Start, Stop) when (Start < Stop) and (Start =< ?NR_OF_MSGS) ->
    Fun(Start),
    fold_range(Fun, Start + 1, Stop);
fold_range(_Fun, _Start, _Stop) -> ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks (as explicit as possible)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
hook_auth_on_subscribe(_,_,_) -> ok.
hook_auth_on_publish(_, _, _,_, _,_) -> ok.
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
