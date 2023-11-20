-module(events_sidecar_handler).
-include_lib("vernemq_dev/include/vernemq_dev.hrl").
-include("vmq_events_sidecar_test.hrl").
-include_lib("vmq_proto/include/on_register_pb.hrl").
-include_lib("vmq_proto/include/on_publish_pb.hrl").
-include_lib("vmq_proto/include/on_subscribe_pb.hrl").
-include_lib("vmq_proto/include/on_unsubscribe_pb.hrl").
-include_lib("vmq_proto/include/on_deliver_pb.hrl").
-include_lib("vmq_proto/include/on_delivery_complete_pb.hrl").
-include_lib("vmq_proto/include/on_offline_message_pb.hrl").
-include_lib("vmq_proto/include/on_client_offline_pb.hrl").
-include_lib("vmq_proto/include/on_client_gone_pb.hrl").
-include_lib("vmq_proto/include/on_client_wakeup_pb.hrl").
-include_lib("vmq_proto/include/on_session_expired_pb.hrl").
-include_lib("vmq_proto/include/any_pb.hrl").

-export([start_tcp_server/0,
         stop_tcp_server/1]).

-define(DEBUG, false).
-define(LISTEN_SOCKET, listen_socket).

start_tcp_server() ->
    Pid = self(),
    spawn_link(fun() ->
          case gen_tcp:listen(8890,[{active, false},binary, {backlog, 200}]) of
            {ok, ListenSock} ->
              ?DEBUG andalso io:format(user, "tcp server started: ~p~n",[ListenSock]),
              Pid ! ListenSock,
              spawn(fun() -> acceptor(ListenSock) end),
              timer:sleep(infinity);
            {error,Reason} ->
              ?DEBUG andalso io:format(user, "Could not start tcp server: ~p~n",[Reason]),
              {error,Reason}
          end
    end),
    get_socket().

acceptor(ListenSocket) ->
  case gen_tcp:accept(ListenSocket) of
    {ok, Socket} ->
      ?DEBUG andalso io:format(user, "ACCEPTED : ~p~n",[Socket]),
      spawn(fun() -> acceptor(ListenSocket) end),
      handle(Socket);
    Other ->
      ?DEBUG andalso io:format(user, "accept returned ~w - goodbye!~n",[Other])
  end.

handle(S) ->
    inet:setopts(S,[{active,once}]),
    receive
      {tcp,S,<<Size:32, Data/binary>>} ->
        ?DEBUG andalso io:format(user, "Size: ~p~n", [Size]),
        ?DEBUG andalso io:format(user, "DATA: ~p~n", [Data]),
        ?DEBUG andalso io:format(user, "Decoded Msg: ~p~n", [any_pb:decode_msg(Data, 'Any')]),
        ?DEBUG andalso io:format(user, "Decoded Event: ~p~n", [decode(any_pb:decode_msg(Data, 'Any'))]),
        process_hook(decode(any_pb:decode_msg(Data, 'Any'))),
        handle(S);
      {tcp_closed,S} ->
        gen_tcp:close(S),
        ?DEBUG andalso io:format(user, "Socket ~w closed [~w]~n",[S,self()]),
        ok
    end.

stop_tcp_server(S) -> ?DEBUG andalso io:format(user, "Socket: ~p~n", [S]), ok = gen_tcp:close(S).

decode({_, "type.googleapis.com/eventssidecar.v1.OnDeliveryComplete", Value}) ->
    on_delivery_complete_pb:decode_msg(Value, 'eventssidecar.v1.OnDeliveryComplete');
decode({_, "type.googleapis.com/eventssidecar.v1.OnDeliver", Value}) ->
    on_deliver_pb:decode_msg(Value, 'eventssidecar.v1.OnDeliver');
decode({_, "type.googleapis.com/eventssidecar.v1.OnRegister", Value}) ->
    on_register_pb:decode_msg(Value, 'eventssidecar.v1.OnRegister');
decode({_, "type.googleapis.com/eventssidecar.v1.OnSubscribe", Value}) ->
    on_subscribe_pb:decode_msg(Value, 'eventssidecar.v1.OnSubscribe');
decode({_, "type.googleapis.com/eventssidecar.v1.OnUnsubscribe", Value}) ->
    on_unsubscribe_pb:decode_msg(Value, 'eventssidecar.v1.OnUnsubscribe');
decode({_, "type.googleapis.com/eventssidecar.v1.OnClientGone", Value}) ->
    on_client_gone_pb:decode_msg(Value, 'eventssidecar.v1.OnClientGone');
decode({_, "type.googleapis.com/eventssidecar.v1.OnOfflineMessage", Value}) ->
    on_offline_message_pb:decode_msg(Value, 'eventssidecar.v1.OnOfflineMessage');
decode({_, "type.googleapis.com/eventssidecar.v1.OnClientOffline", Value}) ->
    on_client_offline_pb:decode_msg(Value, 'eventssidecar.v1.OnClientOffline');
decode({_, "type.googleapis.com/eventssidecar.v1.OnPublish", Value}) ->
    on_publish_pb:decode_msg(Value, 'eventssidecar.v1.OnPublish');
decode({_, "type.googleapis.com/eventssidecar.v1.OnSessionExpired", Value}) ->
    on_session_expired_pb:decode_msg(Value, 'eventssidecar.v1.OnSessionExpired');
decode({_, "type.googleapis.com/eventssidecar.v1.OnClientWakeUp", Value}) ->
    on_client_wakeup_pb:decode_msg(Value, 'eventssidecar.v1.OnClientWakeUp').

%% callbacks for each hook
on_register(#'eventssidecar.v1.OnRegister'{peer_addr = ?PEER_BIN,
              peer_port = ?PEERPORT,
              username = BinPid,
              mountpoint = ?MOUNTPOINT_BIN,
              client_id = ?ALLOWED_CLIENT_ID,
              user_properties =  [{<<"k3">>,<<"v3">>},{<<"k2">>,<<"v2">>},{<<"k1">>,<<"v1">>}]}) ->
    Pid = list_to_pid(binary_to_list(BinPid)),
    Pid ! on_register_ok;

on_register(#'eventssidecar.v1.OnRegister'{peer_addr = ?PEER_BIN,
  peer_port = ?PEERPORT,
  username = BinPid,
  mountpoint = ?MOUNTPOINT_BIN,
  client_id = ?ALLOWED_CLIENT_ID}) ->
  Pid = list_to_pid(binary_to_list(BinPid)),
  Pid ! on_register_ok.
    

on_publish(#'eventssidecar.v1.OnPublish'{username = BinPid,
             mountpoint = ?MOUNTPOINT_BIN,
             client_id = ?ALLOWED_CLIENT_ID,
             topic = ?TOPIC,
             qos = 1,
             payload = ?PAYLOAD,
             retain = false}) ->
    Pid = list_to_pid(binary_to_list(BinPid)),
    Pid ! on_publish_ok.
    

on_subscribe(#'eventssidecar.v1.OnSubscribe'{username = BinPid,
               mountpoint = ?MOUNTPOINT_BIN,
               client_id = ?ALLOWED_CLIENT_ID,
               topics = [#'eventssidecar.v1.TopicInfo'{topic = ?TOPIC, qos = 1},
                          #'eventssidecar.v1.TopicInfo'{topic = ?TOPIC, qos = 128}]
              }) ->
    Pid = list_to_pid(binary_to_list(BinPid)),
    Pid ! on_subscribe_ok.
    

on_unsubscribe(#'eventssidecar.v1.OnUnsubscribe'{username = BinPid, client_id = ?ALLOWED_CLIENT_ID}) ->
    Pid = list_to_pid(binary_to_list(BinPid)),
    Pid ! on_unsubscribe_ok.

on_deliver(#'eventssidecar.v1.OnDeliver'{username = BinPid,
             mountpoint = ?MOUNTPOINT_BIN,
             client_id = ?ALLOWED_CLIENT_ID,
             qos = 1,
             topic = ?TOPIC,
             payload = ?PAYLOAD,
             is_retain = false}) ->
    Pid = list_to_pid(binary_to_list(BinPid)),
    Pid ! on_deliver_ok.

on_delivery_complete(#'eventssidecar.v1.OnDeliveryComplete'{username = BinPid,
                       mountpoint = ?MOUNTPOINT_BIN,
                       client_id = ?ALLOWED_CLIENT_ID,
                       qos = 1,
                       topic = ?TOPIC,
                       payload = ?PAYLOAD,
                       is_retain = false}) ->
    Pid = list_to_pid(binary_to_list(BinPid)),
    Pid ! on_delivery_complete_ok.

on_offline_message(#'eventssidecar.v1.OnOfflineMessage'{mountpoint = ?MOUNTPOINT_BIN,
                     client_id = BinPid}) ->
    Pid = list_to_pid(binary_to_list(BinPid)),
    Pid ! on_offline_message_ok.
    

on_client_wakeup(#'eventssidecar.v1.OnClientWakeUp'{mountpoint = ?MOUNTPOINT_BIN,
                   client_id = BinPid}) ->
    Pid = list_to_pid(binary_to_list(BinPid)),
    Pid ! on_client_wakeup_ok.
    

on_client_offline(#'eventssidecar.v1.OnClientOffline'{mountpoint = ?MOUNTPOINT_BIN,
                   client_id = BinPid, reason = ?REASON}) ->
    Pid = list_to_pid(binary_to_list(BinPid)),
    Pid ! on_client_offline_ok.
    

on_client_gone(#'eventssidecar.v1.OnClientGone'{mountpoint = ?MOUNTPOINT_BIN,
                 client_id = BinPid, reason = ?REASON}) ->
    Pid = list_to_pid(binary_to_list(BinPid)),
    Pid ! on_client_gone_ok.
    

on_session_expired(#'eventssidecar.v1.OnSessionExpired'{mountpoint = ?MOUNTPOINT_BIN,
                     client_id = BinPid}) ->
    Pid = list_to_pid(binary_to_list(BinPid)),
    Pid ! on_session_expired_ok.

process_hook(Event) when is_record(Event, 'eventssidecar.v1.OnRegister') ->
    on_register(Event);
process_hook(Event) when is_record(Event, 'eventssidecar.v1.OnSubscribe') ->
    on_subscribe(Event);
process_hook(Event) when is_record(Event, 'eventssidecar.v1.OnUnsubscribe') ->
    on_unsubscribe(Event);
process_hook(Event) when is_record(Event, 'eventssidecar.v1.OnPublish') ->
    on_publish(Event);
process_hook(Event) when is_record(Event, 'eventssidecar.v1.OnClientWakeUp') ->
    on_client_wakeup(Event);
process_hook(Event) when is_record(Event, 'eventssidecar.v1.OnOfflineMessage') ->
    on_offline_message(Event);
process_hook(Event) when is_record(Event, 'eventssidecar.v1.OnClientGone') ->
    on_client_gone(Event);
process_hook(Event) when is_record(Event, 'eventssidecar.v1.OnClientOffline') ->
    on_client_offline(Event);
process_hook(Event) when is_record(Event, 'eventssidecar.v1.OnDeliveryComplete') ->
    on_delivery_complete(Event);
process_hook(Event) when is_record(Event, 'eventssidecar.v1.OnSessionExpired') ->
    on_session_expired(Event);
process_hook(Event) when is_record(Event, 'eventssidecar.v1.OnDeliver') ->
    on_deliver(Event).


get_socket() ->
  receive
    Socket -> Socket
  after
    1000 -> ok
  end.