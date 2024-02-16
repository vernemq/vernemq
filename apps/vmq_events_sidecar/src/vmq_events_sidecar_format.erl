-module(vmq_events_sidecar_format).
-include("../include/vmq_events_sidecar.hrl").
-include_lib("vernemq_dev/include/vernemq_dev.hrl").
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
-include_lib("vmq_commons/include/vmq_types.hrl").

%% API
-export([encode/1]).

-spec encode(event()) -> iodata().
encode(
    {on_register, Timestamp,
        {MP, ClientId, PPeer, Port, UserName, #{?P_USER_PROPERTY := Properties}}}
) ->
    encode_envelope(
        "OnRegister",
        on_register_pb:encode_msg(#'eventssidecar.v1.OnRegister'{
            peer_addr = PPeer,
            peer_port = Port,
            username = UserName,
            mountpoint = MP,
            client_id = ClientId,
            timestamp = convert_timestamp(Timestamp),
            user_properties = Properties
        })
    );
encode({on_register, Timestamp, {MP, ClientId, PPeer, Port, UserName, #{}}}) ->
    encode_envelope(
        "OnRegister",
        on_register_pb:encode_msg(#'eventssidecar.v1.OnRegister'{
            peer_addr = PPeer,
            peer_port = Port,
            username = UserName,
            mountpoint = MP,
            client_id = ClientId,
            timestamp = convert_timestamp(Timestamp)
        })
    );
encode(
    {on_publish, Timestamp,
        {MP, ClientId, UserName, QoS, Topic, Payload, IsRetain, #matched_acl{
            name = Name, pattern = Pattern
        }}}
) ->
    encode_envelope(
        "OnPublish",
        on_publish_pb:encode_msg(#'eventssidecar.v1.OnPublish'{
            username = UserName,
            client_id = ClientId,
            mountpoint = MP,
            qos = QoS,
            topic = Topic,
            payload = Payload,
            retain = IsRetain,
            timestamp = convert_timestamp(Timestamp),
            matched_acl = #'eventssidecar.v1.MatchedACL'{
                name = Name, pattern = Pattern
            }
        })
    );
encode(
    {on_subscribe, Timestamp, {MP, ClientId, UserName, Topics}}
) ->
    encode_envelope(
        "OnSubscribe",
        on_subscribe_pb:encode_msg(#'eventssidecar.v1.OnSubscribe'{
            client_id = ClientId,
            mountpoint = MP,
            username = UserName,
            topics = [
                #'eventssidecar.v1.TopicInfo'{
                    topic = T,
                    qos = QoS,
                    matched_acl = #'eventssidecar.v1.MatchedACL'{name = Name, pattern = Pattern}
                }
             || [T, QoS, #matched_acl{name = Name, pattern = Pattern}] <- Topics
            ],
            timestamp = convert_timestamp(Timestamp)
        })
    );
encode({on_unsubscribe, Timestamp, {MP, ClientId, UserName, Topics}}) ->
    encode_envelope(
        "OnUnsubscribe",
        on_unsubscribe_pb:encode_msg(#'eventssidecar.v1.OnUnsubscribe'{
            client_id = ClientId,
            mountpoint = MP,
            username = UserName,
            topics = Topics,
            timestamp = convert_timestamp(Timestamp)
        })
    );
encode(
    {on_deliver, Timestamp,
        {MP, ClientId, UserName, QoS, Topic, Payload, IsRetain, #matched_acl{
            name = Name, pattern = Pattern
        }}}
) ->
    encode_envelope(
        "OnDeliver",
        on_deliver_pb:encode_msg(#'eventssidecar.v1.OnDeliver'{
            client_id = ClientId,
            mountpoint = MP,
            username = UserName,
            topic = Topic,
            qos = QoS,
            is_retain = IsRetain,
            payload = Payload,
            timestamp = convert_timestamp(Timestamp),
            matched_acl = #'eventssidecar.v1.MatchedACL'{
                name = Name, pattern = Pattern
            }
        })
    );
encode(
    {on_delivery_complete, Timestamp,
        {MP, ClientId, UserName, QoS, Topic, Payload, IsRetain, #matched_acl{
            name = Name, pattern = Pattern
        }}}
) ->
    encode_envelope(
        "OnDeliveryComplete",
        on_delivery_complete_pb:encode_msg(#'eventssidecar.v1.OnDeliveryComplete'{
            client_id = ClientId,
            mountpoint = MP,
            username = UserName,
            topic = Topic,
            qos = QoS,
            is_retain = IsRetain,
            payload = Payload,
            timestamp = convert_timestamp(Timestamp),
            matched_acl = #'eventssidecar.v1.MatchedACL'{
                name = Name, pattern = Pattern
            }
        })
    );
encode({on_offline_message, Timestamp, {MP, ClientId, QoS, Topic, Payload, IsRetain}}) ->
    encode_envelope(
        "OnOfflineMessage",
        on_offline_message_pb:encode_msg(#'eventssidecar.v1.OnOfflineMessage'{
            client_id = ClientId,
            mountpoint = MP,
            qos = QoS,
            topic = Topic,
            payload = Payload,
            retain = IsRetain,
            timestamp = convert_timestamp(Timestamp)
        })
    );
encode({on_client_wakeup, Timestamp, {MP, ClientId}}) ->
    encode_envelope(
        "OnClientWakeUp",
        on_client_wakeup_pb:encode_msg(#'eventssidecar.v1.OnClientWakeUp'{
            client_id = ClientId,
            mountpoint = MP,
            timestamp = convert_timestamp(Timestamp)
        })
    );
encode({on_client_offline, Timestamp, {MP, ClientId, Reason}}) ->
    encode_envelope(
        "OnClientOffline",
        on_client_offline_pb:encode_msg(#'eventssidecar.v1.OnClientOffline'{
            client_id = ClientId,
            mountpoint = MP,
            timestamp = convert_timestamp(Timestamp),
            reason = Reason
        })
    );
encode({on_client_gone, Timestamp, {MP, ClientId, Reason}}) ->
    encode_envelope(
        "OnClientGone",
        on_client_gone_pb:encode_msg(#'eventssidecar.v1.OnClientGone'{
            client_id = ClientId,
            mountpoint = MP,
            timestamp = convert_timestamp(Timestamp),
            reason = Reason
        })
    );
encode({on_session_expired, Timestamp, {MP, ClientId}}) ->
    encode_envelope(
        "OnSessionExpired",
        on_session_expired_pb:encode_msg(#'eventssidecar.v1.OnSessionExpired'{
            client_id = ClientId,
            mountpoint = MP,
            timestamp = convert_timestamp(Timestamp)
        })
    );
encode(_) ->
    <<>>.

-spec encode_envelope(string(), iodata()) -> iodata().
encode_envelope(Name, Value) ->
    any_pb:encode_msg(#'Any'{
        type_url = "type.googleapis.com/eventssidecar.v1." ++ Name, value = Value
    }).

convert_timestamp(Now) ->
    #'google.protobuf.Timestamp'{seconds = Now div 1000000000, nanos = Now rem 1000000000}.
