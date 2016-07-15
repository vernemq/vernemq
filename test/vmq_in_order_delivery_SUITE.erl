-module(vmq_in_order_delivery_SUITE).
-include_lib("vmq_commons/include/vmq_types.hrl").
-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([qos1_online/1,
         qos2_online/1,
         qos1_offline/1,
         qos2_offline/1]).

-export([hook_auth_on_subscribe/3,
         hook_auth_on_publish/6]).

-define(RETRY_INTERVAL, 10).
-define(NR_OF_MSGS, 1000).

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
    vmq_server_cmd:set_config(retry_interval, ?RETRY_INTERVAL),
    vmq_server_cmd:listener_start(1888, []),

    enable_on_subscribe(),
    enable_on_publish(),

    Config.

end_per_testcase(_, Config) ->
    disable_on_subscribe(),
    disable_on_publish(),
    vmq_test_utils:teardown(),
    Config.

all() ->
    [qos1_online,
     qos2_online,
     qos1_offline,
     qos2_offline].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

qos1_online(_) ->
    lists:foreach(fun qos1_online_test/1, lists:seq(1, 30)).

qos2_online(_) ->
    lists:foreach(fun qos2_online_test/1, lists:seq(1, 30)).

qos1_offline(_) ->
    lists:foreach(fun qos1_offline_test/1, lists:seq(1, 30)).

qos2_offline(_) ->
    lists:foreach(fun qos2_offline_test/1, lists:seq(1, 30)).


qos1_online_test(MaxInflightMsgs) ->
    {ok, _} = vmq_server_cmd:set_config(max_inflight_messages, MaxInflightMsgs),
    Topic = "in/order/qos/1",
    SubSocket = setup_con(Topic, 1),
    PubSocket = setup_pub(Topic, fun setup_pub_qos1/3),
    ok = recv_qos1(SubSocket, Topic, MaxInflightMsgs),
    ok = gen_tcp:close(SubSocket),
    ok = gen_tcp:close(PubSocket),
    teardown_con().

qos2_online_test(MaxInflightMsgs) ->
    {ok, _} = vmq_server_cmd:set_config(max_inflight_messages, MaxInflightMsgs),
    Topic = "in/order/qos/2",
    SubSocket = setup_con(Topic, 2),
    PubSocket = setup_pub(Topic, fun setup_pub_qos2/3),
    ok = recv_qos2(SubSocket, Topic, MaxInflightMsgs),
    ok = gen_tcp:close(SubSocket),
    ok = gen_tcp:close(PubSocket),
    teardown_con().

qos1_offline_test(MaxInflightMsgs) ->
    {ok, _} = vmq_server_cmd:set_config(max_inflight_messages, MaxInflightMsgs),
    Topic = "in/order/qos/1",
    SubSocket1 = setup_con(Topic, 1),
    PubSocket = setup_pub(Topic, fun setup_pub_qos1/3),
    gen_tcp:close(SubSocket1),
    SubSocket2 = setup_con(Topic, 1, true), % session present
    ok = recv_qos1(SubSocket2, Topic, MaxInflightMsgs, true), %% dup=true
    ok = gen_tcp:close(SubSocket2),
    ok = gen_tcp:close(PubSocket),
    teardown_con().

qos2_offline_test(MaxInflightMsgs) ->
    {ok, _} = vmq_server_cmd:set_config(max_inflight_messages, MaxInflightMsgs),
    Topic = "in/order/qos/2",
    SubSocket1 = setup_con(Topic, 2),
    PubSocket = setup_pub(Topic, fun setup_pub_qos2/3),
    gen_tcp:close(SubSocket1),
    SubSocket2 = setup_con(Topic, 2, true), % session present
    ok = recv_qos2(SubSocket2, Topic, MaxInflightMsgs, true), %% dup=true
    ok = gen_tcp:close(SubSocket2),
    ok = gen_tcp:close(PubSocket),
    teardown_con().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

setup_pub(Topic, SetupFun) ->
    Connect = packet:gen_connect("in-order-pub", [{keepalive,60},
                                                  {clean_session, true}]),
    Connack = packet:gen_connack(0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    lists:foreach(fun(I) -> SetupFun(Socket, Topic, I) end,
                  lists:seq(1, ?NR_OF_MSGS)),
    Socket.

setup_pub_qos1(Socket, Topic, I) ->
    Payload = list_to_binary("msg" ++ integer_to_list(I)),
    Pub = packet:gen_publish(Topic, 1, Payload, [{mid, I}]),
    gen_tcp:send(Socket, Pub),
    ok = packet:expect_packet(Socket, "puback", packet:gen_puback(I)).

setup_pub_qos2(Socket, Topic, I) ->
    Payload = list_to_binary("msg" ++ integer_to_list(I)),
    Pub = packet:gen_publish(Topic, 2, Payload, [{mid, I}]),
    gen_tcp:send(Socket, Pub),
    ok = packet:expect_packet(Socket, "pubrec", packet:gen_pubrec(I)),
    ok = gen_tcp:send(Socket, packet:gen_pubrel(I)),
    ok = packet:expect_packet(Socket, "pubcomp", packet:gen_pubcomp(I)).

setup_con(Topic, Qos) ->
    setup_con(Topic, Qos, false).
setup_con(Topic, Qos, SessionPresent) ->
    Connect = packet:gen_connect("in-order-sub", [{keepalive,60},
                                                  {clean_session, false}]),
    Connack = packet:gen_connack(SessionPresent, 0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    case SessionPresent of
        false ->
            Subscribe = packet:gen_subscribe(109, Topic, Qos),
            Suback = packet:gen_suback(109, Qos),
            ok = gen_tcp:send(Socket, Subscribe),
            ok = packet:expect_packet(Socket, "suback", Suback);
        true ->
            ignore % no subscription necessary
    end,
    Socket.

teardown_con() ->
    Connect = packet:gen_connect("in-order-sub", [{keepalive,60},
                                                  {clean_session, true}]),
    Connack = packet:gen_connack(0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    gen_tcp:close(Socket).


recv_qos1(Socket, Topic, MaxInflightMsgs) ->
    recv_qos1(Socket, Topic, MaxInflightMsgs, false).

recv_qos1(Socket, Topic, MaxInflightMsgs, Dup) ->
    recv(Socket, Topic, 1, MaxInflightMsgs, Dup,
         [fun recv_pub_qos1/4, fun send_puback/4]).

recv_qos2(Socket, Topic, MaxInflightMsgs) ->
    recv_qos2(Socket, Topic, MaxInflightMsgs, false).
recv_qos2(Socket, Topic, MaxInflightMsgs, Dup) ->
    recv(Socket, Topic, 1, MaxInflightMsgs, Dup,
         [fun recv_pub_qos2/4, fun send_pubrec/4,
          fun recv_pubrel_send_pubcomp/4]).

recv_pub_qos1(Socket, Topic, Id, Dup) ->
    Payload = list_to_binary("msg" ++ integer_to_list(Id)),
    Pub = packet:gen_publish(Topic, 1, Payload, [{mid, Id}, {dup, Dup}]),
    ok = packet:expect_packet(Socket, "publish", Pub).

recv_pub_qos2(Socket, Topic, Id, Dup) ->
    Payload = list_to_binary("msg" ++ integer_to_list(Id)),
    Pub = packet:gen_publish(Topic, 2, Payload, [{mid, Id}, {dup, Dup}]),
    ok = packet:expect_packet(Socket, "publish", Pub).

send_puback(Socket, _, Id, _) ->
    ok = gen_tcp:send(Socket, packet:gen_puback(Id)).

send_pubrec(Socket, _, Id, _) ->
    ok = gen_tcp:send(Socket, packet:gen_pubrec(Id)).

recv_pubrel_send_pubcomp(Socket, _, Id, _) ->
    ok = packet:expect_packet(Socket, "pubrel", packet:gen_pubrel(Id)),
    ok = gen_tcp:send(Socket, packet:gen_pubcomp(Id)).


recv(_, _, Id, _, _, _) when Id > ?NR_OF_MSGS -> ok;
recv(Socket, Topic, Id, MaxInflightMsgs, Dup, Funs) ->
    NextId = recv(Socket, Topic, Id, MaxInflightMsgs, MaxInflightMsgs, Dup, Funs),
    %% dup<might-be-true> only for the first batch of max-inflights,
    %% after that the messages haven't been sent.
    recv(Socket, Topic, NextId, MaxInflightMsgs, false, Funs).

recv(Socket, Topic, Id, 0, MaxInflightMsgs, Dup, [_|Funs])
  when Id =< ?NR_OF_MSGS ->
    recv(Socket, Topic, Id - MaxInflightMsgs, MaxInflightMsgs, MaxInflightMsgs,
         Dup, Funs);
recv(Socket, Topic, Id, T, MaxInflightMsgs, Dup, [Fun|_] = Funs)
  when Id =< ?NR_OF_MSGS ->
    ok = Fun(Socket, Topic, Id, Dup),
    recv(Socket, Topic, Id + 1, T - 1, MaxInflightMsgs, Dup, Funs);
recv(_, _, Id, _, MaxInflightMsgs, _, _) -> Id + MaxInflightMsgs.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks (as explicit as possible)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
hook_auth_on_subscribe(_,_,_) -> ok.
hook_auth_on_publish(_, _, _,_, _,_) -> ok.
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
