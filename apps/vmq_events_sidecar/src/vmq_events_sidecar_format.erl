-module(vmq_events_sidecar_format).
-include("../include/vmq_events_sidecar.hrl").
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

%% API
-export([encode/1]).

-spec encode(event()) -> iodata().
encode({on_register, {MP, ClientId, PPeer, Port, UserName}}) ->
  encode_envelope("OnRegister", on_register_pb:encode_msg(#'OnRegister'{peer_addr = PPeer,
                                                                         peer_port = Port,
                                                                         username = UserName,
                                                                         mountpoint = MP,
                                                                         client_id = ClientId}));
encode({on_publish, {MP, ClientId, UserName, QoS, Topic, Payload, IsRetain}}) ->
  encode_envelope("OnPublish", on_publish_pb:encode_msg(#'OnPublish'{username = UserName,
                                                                      client_id = ClientId,
                                                                      mountpoint = MP,
                                                                      qos = QoS,
                                                                      topic = Topic,
                                                                      payload = Payload,
                                                                      retain = IsRetain}));
encode({on_subscribe, {MP, ClientId, UserName, Topics}}) ->
  encode_envelope("OnSubscribe", on_subscribe_pb:encode_msg(#'OnSubscribe'{client_id = ClientId,
                                                                            mountpoint = MP,
                                                                            username = UserName,
                                                                            topics = [#'TopicInfo'{topic = T, qos = QoS} || [T, QoS] <- Topics]}));
encode({on_unsubscribe, {MP, ClientId, UserName, Topics}}) ->
  encode_envelope("OnUnsubscribe", on_unsubscribe_pb:encode_msg(#'OnUnsubscribe'{client_id = ClientId,
                                                                                  mountpoint = MP,
                                                                                  username = UserName,
                                                                                  topics = Topics}));
encode({on_deliver, {MP, ClientId, UserName, QoS, Topic, Payload, IsRetain}}) ->
  encode_envelope("OnDeliver", on_deliver_pb:encode_msg(#'OnDeliver'{client_id = ClientId,
                                                                      mountpoint = MP,
                                                                      username = UserName,
                                                                      topic = Topic,
                                                                      qos = QoS,
                                                                      is_retain = IsRetain,
                                                                      payload = Payload}));
encode({on_delivery_complete, {MP, ClientId, UserName, QoS, Topic, Payload, IsRetain}}) ->
  encode_envelope("OnDeliveryComplete", on_delivery_complete_pb:encode_msg(#'OnDeliveryComplete'{client_id = ClientId,
                                                                      mountpoint = MP,
                                                                      username = UserName,
                                                                      topic = Topic,
                                                                      qos = QoS,
                                                                      is_retain = IsRetain,
                                                                      payload = Payload}));
encode({on_offline_message, {MP, ClientId, QoS, Topic, Payload, IsRetain}}) ->
  encode_envelope("OnOfflineMessage", on_offline_message_pb:encode_msg(#'OnOfflineMessage'{client_id = ClientId,
                                                                      mountpoint = MP,
                                                                      qos = QoS,
                                                                      topic = Topic,
                                                                      payload = Payload,
                                                                      retain = IsRetain}));
encode({on_client_wakeup, {MP, ClientId}}) ->
  encode_envelope("OnClientWakeUp", on_client_wakeup_pb:encode_msg(#'OnClientWakeUp'{client_id = ClientId,
                                                                                       mountpoint = MP}));
encode({on_client_offline, {MP, ClientId}}) ->
  encode_envelope("OnClientOffline", on_client_offline_pb:encode_msg(#'OnClientOffline'{client_id = ClientId,
                                                                                          mountpoint = MP}));
encode({on_client_gone, {MP, ClientId}}) ->
  encode_envelope("OnClientGone", on_client_gone_pb:encode_msg(#'OnClientGone'{client_id = ClientId,
                                                                                 mountpoint = MP}));
encode({on_session_expired, {MP, ClientId}}) ->
  encode_envelope("OnSessionExpired", on_session_expired_pb:encode_msg(#'OnSessionExpired'{client_id = ClientId,
                                                                                          mountpoint = MP}));
encode(_) ->
  <<>>.

-spec encode_envelope(string(), iodata()) -> iodata().
encode_envelope(Name, Value) ->
  any_pb:encode_msg(#'Any'{type_url = "type.googleapis.com/eventssidecar.v1." ++ Name, value = Value}).
