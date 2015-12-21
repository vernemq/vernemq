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

-export([in_order_offline_qos1_test/1,
         in_order_offline_qos2_test/1,
         in_order_offline_retry_qos1_test/1,
         in_order_offline_retry_qos2_test/1,
         in_order_online_qos1_test/1,
         in_order_online_qos2_test/1]).

-export([hook_auth_on_subscribe/3,
         hook_auth_on_publish/6]).

-define(RETRY_INTERVAL, 10).

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
    [in_order_offline_qos1_test,
     in_order_offline_qos2_test,
     in_order_offline_retry_qos1_test,
     in_order_offline_retry_qos2_test,
     in_order_online_qos1_test,
     in_order_online_qos2_test].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
in_order_offline_qos1_test(_) ->
    lists:foreach(fun(MaxInflightMessages) ->
                          in_order_routine(MaxInflightMessages, 1,
                                           MaxInflightMessages * 10, true, false)
                  end, lists:seq(1, 100)).

in_order_offline_qos2_test(_) ->
    in_order_routine(1, 2, 100, true, false).

in_order_offline_retry_qos1_test(_) ->
    in_order_routine(20, 1, 100, true, true).

in_order_offline_retry_qos2_test(_) ->
    in_order_routine(20, 2, 100, true, true).

in_order_online_qos1_test(_) ->
    lists:foreach(fun(MaxInflightMessages) ->
                          in_order_routine(MaxInflightMessages, 1,
                                           MaxInflightMessages * 10, false, false)
                  end, lists:seq(1, 100)).

in_order_online_qos2_test(_) ->
    in_order_routine(1, 2, 100, false, false).

in_order_routine(MaxInflightMessages, QoS, LoadLevel, GoOffline, LetRetry) ->
    {ok, _} = vmq_server_cmd:set_config(max_inflight_messages, MaxInflightMessages),
    SubConnect = packet:gen_connect("inorder-sub", [{keepalive,60}, {clean_session, false}]),
    PubConnect = packet:gen_connect("inorder-pub", [{keepalive,60}, {clean_session, true}]),
    Connack1 = packet:gen_connack(0),
    Connack2 = packet:gen_connack(true, 0),
    Subscribe = packet:gen_subscribe(109, "inorder/test", QoS),
    Suback = packet:gen_suback(109, QoS),
    {ok, SubSocket0} = packet:do_client_connect(SubConnect, Connack1, []),
    ok = gen_tcp:send(SubSocket0, Subscribe),
    ok = packet:expect_packet(SubSocket0, "suback", Suback),

    case GoOffline of
        true ->
            ok = gen_tcp:close(SubSocket0);
        false ->
            ok
    end,

    %% load the queue

    {ok, PubSocket} = packet:do_client_connect(PubConnect, Connack1, []),

    Pubs =
    lists:foldl(
      fun(I, Acc) ->
              Payload = list_to_binary("msg" ++ integer_to_list(I)),
              Pub = packet:gen_publish("inorder/test", QoS, Payload, [{mid, I}]),
              gen_tcp:send(PubSocket, Pub),
              case QoS of
                  1 ->
                      ok = packet:expect_packet(PubSocket, "puback",
                                                packet:gen_puback(I));
                  2 ->
                      ok = packet:expect_packet(PubSocket, "pubrec",
                                               packet:gen_pubrec(I)),
                      ok = gen_tcp:send(PubSocket, packet:gen_pubrel(I)),
                      ok = packet:expect_packet(PubSocket, "pubcomp",
                                               packet:gen_pubcomp(I))
              end,
              [{I, Pub}|Acc]
      end, [], lists:seq(1, LoadLevel)),
    gen_tcp:close(PubSocket),

    {ok, SubSocket1} =
    case GoOffline of
        true ->
            packet:do_client_connect(SubConnect, Connack2, []);
        false ->
            {ok, SubSocket0}
    end,

    case LetRetry of
        false ->
              handle_inflight_messages(QoS, SubSocket1, MaxInflightMessages,
                                       lists:reverse(Pubs));
        true ->
            %% we receive the messages of the max_inflight_window but dont ack them
            {PubsInFlight, PubsRest} = lists:split(MaxInflightMessages,
                                            lists:reverse(Pubs)),
            PubsInFlightDup =
            lists:map(
              fun({I, Pub}) ->
                      %% we don't ack the publish
                      ok = packet:expect_packet(SubSocket1, "publish", Pub),
                      {#mqtt_publish{} = F, _} = vmq_parser:parse(Pub),
                      {I, iolist_to_binary(
                         vmq_parser:serialise(F#mqtt_publish{dup=true}))}
              end, PubsInFlight),

              timer:sleep(?RETRY_INTERVAL),

              handle_inflight_messages(QoS, SubSocket1, MaxInflightMessages,
                                       PubsInFlightDup ++ PubsRest)
    end,
    gen_tcp:close(SubSocket1),
    %% clean session
    SubConnectClean = packet:gen_connect("inorder-sub", [{clean_session, true}]),
    {ok, SubSocket2} = packet:do_client_connect(SubConnectClean, Connack1, []),
    gen_tcp:close(SubSocket2),
    ok.

handle_inflight_messages(1, Socket, _MaxInflightMessages, Pubs) ->
    lists:foreach(
      fun({I, Pub}) ->
              ok = packet:expect_packet(Socket, "publish", Pub),
              ok = gen_tcp:send(Socket, packet:gen_puback(I))
      end, Pubs);
handle_inflight_messages(2, _, _, []) ->
    ok;
handle_inflight_messages(2, Socket, MaxInflightMessages, Pubs) ->
    {PubsInFlight, PubsRest} =
    case length(Pubs) > MaxInflightMessages of
        true ->
            lists:split(MaxInflightMessages, Pubs);
        false ->
            {Pubs, []}
    end,
    %% right after connection is established we get
    %% 'max_inflight_messages'-Nr offline messages delivered.
    %% we must make sure that we send the final pubcomp once
    %% all intermediate steps are done, otherwise 'expect_packet'
    %% is receiving subsequent messages..
    %%
    %% Step 1. PUBREC
    lists:foreach(
      fun({I, Pub}) ->
              ok = packet:expect_packet(Socket, "publish", Pub),
              ok = gen_tcp:send(Socket, packet:gen_pubrec(I))
      end, PubsInFlight),
    %% Step 2. Receive PUBREL
    lists:foreach(
      fun({I, _}) ->
              ok = packet:expect_packet(Socket, "pubrel", packet:gen_pubrel(I)),
              ok = gen_tcp:send(Socket, packet:gen_pubcomp(I))
      end, PubsInFlight),
    handle_inflight_messages(2, Socket, MaxInflightMessages, PubsRest).




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
