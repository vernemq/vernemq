
-type metric_label() :: {atom(), string()}.

-type metric_id() :: atom() | {atom(), non_neg_integer() | atom()}.

-record(metric_def,
        {type        :: atom(),
         labels      :: [metric_label()],
         id          :: metric_id(),
         name        :: atom(),
         description :: undefined | binary()}).
-type metric_def() :: #metric_def{}.

-type metric_val() :: {Def :: metric_def(), Val :: any()}.

%% metrics identifiers
-define(MQTT5_CONNECT_RECEIVED, mqtt5_connect_received).
-define(MQTT5_CONNACK_SENT, mqtt5_connack_sent).
-define(MQTT5_DISCONNECT_RECEIVED, mqtt5_disconnect_received).
-define(MQTT5_DISCONNECT_SENT, mqtt5_disconnect_sent).
-define(MQTT5_PUBLISH_AUTH_ERROR, mqtt5_error_auth_publish).
-define(MQTT5_SUBSCRIBE_AUTH_ERROR, mqtt5_error_auth_subscribe).
-define(MQTT5_INVALID_MSG_SIZE_ERROR, mqtt5_error_invalid_msg_size).
-define(MQTT5_PUBACK_INVALID_ERROR, mqtt5_error_invalid_puback).
-define(MQTT5_PUBCOMP_INVALID_ERROR, mqtt5_error_invalid_pubcomp).
-define(MQTT5_PUBLISH_ERROR, mqtt5_error_publish).
-define(MQTT5_SUBSCRIBE_ERROR, mqtt5_error_subscribe).
-define(MQTT5_UNSUBSCRIBE_ERROR, mqtt5_error_unsubscribe).
-define(MQTT5_PINGREQ_RECEIVED, mqtt5_pingreq_received).
-define(MQTT5_PINGRESP_SENT, mqtt5_pingresp_sent).
-define(MQTT5_PUBACK_RECEIVED, mqtt5_puback_received).
-define(MQTT5_PUBACK_SENT, mqtt5_puback_sent).
-define(MQTT5_PUBCOMP_RECEIVED, mqtt5_pubcomp_received).
-define(MQTT5_PUBCOMP_SENT, mqtt5_pubcomp_sent).
-define(MQTT5_PUBLISH_RECEIVED, mqtt5_publish_received).
-define(MQTT5_PUBLISH_SENT, mqtt5_publish_sent).
-define(MQTT5_PUBREC_RECEIVED, mqtt5_pubrec_received).
-define(MQTT5_PUBREC_SENT, mqtt5_pubrec_sent).
-define(MQTT5_PUBREL_RECEIVED, mqtt5_pubrel_received).
-define(MQTT5_PUBREL_SENT, mqtt5_pubrel_sent).
-define(MQTT5_SUBACK_SENT, mqtt5_suback_sent).
-define(MQTT5_SUBSCRIBE_RECEIVED, mqtt5_subscribe_received).
-define(MQTT5_UNSUBACK_SENT, mqtt5_unsuback_sent).
-define(MQTT5_UNSUBSCRIBE_RECEIVED, mqtt5_unsubscribe_received).
-define(MQTT5_AUTH_SENT, mqtt5_auth_sent).
-define(MQTT5_AUTH_RECEIVED, mqtt5_auth_received).
-define(MQTT5_CLIENT_KEEPALIVE_EXPIRED, mqtt5_client_keepalive_expired).


-define(MQTT4_CONNACK_SENT, mqtt_connack_sent).
-define(MQTT4_CONNECT_RECEIVED, mqtt_connect_received).
-define(MQTT4_PUBLISH_RECEIVED, mqtt_publish_received).
-define(MQTT4_PUBACK_RECEIVED, mqtt_puback_received).
-define(MQTT4_PUBREC_RECEIVED, mqtt_pubrec_received).
-define(MQTT4_PUBREL_RECEIVED, mqtt_pubrel_received).
-define(MQTT4_PUBCOMP_RECEIVED, mqtt_pubcomp_received).
-define(MQTT4_SUBSCRIBE_RECEIVED, mqtt_subscribe_received).
-define(MQTT4_UNSUBSCRIBE_RECEIVED, mqtt_unsubscribe_received).
-define(MQTT4_PINGREQ_RECEIVED, mqtt_pingreq_received).
-define(MQTT4_DISCONNECT_RECEIVED, mqtt_disconnect_received).
-define(MQTT4_PUBLISH_SENT, mqtt_publish_sent).
-define(MQTT4_PUBACK_SENT, mqtt_puback_sent).
-define(MQTT4_PUBREC_SENT, mqtt_pubrec_sent).
-define(MQTT4_PUBREL_SENT, mqtt_pubrel_sent).
-define(MQTT4_PUBCOMP_SENT, mqtt_pubcomp_sent).
-define(MQTT4_SUBACK_SENT, mqtt_suback_sent).
-define(MQTT4_UNSUBACK_SENT, mqtt_unsuback_sent).
-define(MQTT4_PINGRESP_SENT, mqtt_pingresp_sent).
-define(MQTT4_PUBLISH_AUTH_ERROR, mqtt_publish_auth_error).
-define(MQTT4_SUBSCRIBE_AUTH_ERROR, mqtt_subscribe_auth_error).
-define(MQTT4_INVALID_MSG_SIZE_ERROR, mqtt_invalid_msg_size_error).
-define(MQTT4_PUBACK_INVALID_ERROR, mqtt_puback_invalid_error).
-define(MQTT4_PUBREC_INVALID_ERROR, mqtt_pubrec_invalid_error).
-define(MQTT4_PUBCOMP_INVALID_ERROR, mqtt_pubcomp_invalid_error).
-define(MQTT4_PUBLISH_ERROR, mqtt_publish_error).
-define(MQTT4_SUBSCRIBE_ERROR, mqtt_subscribe_error).
-define(MQTT4_UNSUBSCRIBE_ERROR, mqtt_unsubscribe_error).
-define(MQTT4_CLIENT_KEEPALIVE_EXPIRED, mqtt4_client_keepalive_expired).
-define(METRIC_QUEUE_SETUP, queue_setup).
-define(METRIC_QUEUE_INITIALIZED_FROM_STORAGE, queue_initialized_from_storage).
-define(METRIC_QUEUE_TEARDOWN, queue_teardown).
-define(METRIC_QUEUE_MESSAGE_DROP, queue_message_drop).
-define(METRIC_QUEUE_MESSAGE_EXPIRED, queue_message_expired).
-define(METRIC_QUEUE_MESSAGE_UNHANDLED, queue_message_unhandled).
-define(METRIC_QUEUE_MESSAGE_IN, queue_message_in).
-define(METRIC_QUEUE_MESSAGE_OUT, queue_message_out).
-define(METRIC_CLIENT_EXPIRED, client_expired). %% unused/deprecated
-define(METRIC_CLUSTER_BYTES_RECEIVED, cluster_bytes_received).
-define(METRIC_CLUSTER_BYTES_SENT, cluster_bytes_sent).
-define(METRIC_CLUSTER_BYTES_DROPPED, cluster_bytes_dropped).
-define(METRIC_SOCKET_OPEN, socket_open).
-define(METRIC_SOCKET_CLOSE, socket_close).
-define(METRIC_SOCKET_CLOSE_TIMEOUT, socket_close_timeout).
-define(METRIC_SOCKET_ERROR, socket_error).
-define(METRIC_BYTES_RECEIVED, bytes_received).
-define(METRIC_BYTES_SENT, bytes_sent).
-define(METRIC_MSG_IN_RATE, msg_in_rate).
-define(METRIC_MSG_OUT_RATE, msg_out_rate).
-define(METRIC_BYTE_IN_RATE, byte_in_rate).
-define(METRIC_BYTE_OUT_RATE, byte_out_rate).
-define(METRIC_ROUTER_MATCHES_LOCAL, router_matches_local).
-define(METRIC_ROUTER_MATCHES_REMOTE, router_matches_remote).
