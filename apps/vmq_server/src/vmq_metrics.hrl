
-type metric_label() :: {atom(), string()}.

-type metric_id() :: atom().

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
-define(MQTT5_DISCONNECT_RECEIVED, mqtt5_disconnect_received).
-define(MQTT5_PUBLISH_AUTH_ERROR, mqtt5_error_auth_publish).
-define(MQTT5_SUBSCRIBE_AUTH_ERROR, mqtt5_error_auth_subscribe).
-define(MQTT5_INVALID_MSG_SIZE_ERROR, mqtt5_error_invalid_msg_size).
-define(MQTT5_PUBACK_INVALID_ERROR, mqtt5_error_invalid_puback).
-define(MQTT5_PUBCOMP_INVALID_ERROR, mqtt5_error_invalid_pubcomp).
-define(MQTT5_PUBREC_INVALID_ERROR, mqtt5_error_invalid_pubrec).
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
