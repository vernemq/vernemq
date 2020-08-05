-include("vmq_types.hrl").

%% MQTT5 property identifiers.
-define(M5P_PAYLOAD_FORMAT_INDICATOR, 1).
-define(M5P_MESSAGE_EXPIRY_INTERVAL,  2).
-define(M5P_CONTENT_TYPE,             3).
-define(M5P_RESPONSE_TOPIC,           8).
-define(M5P_CORRELATION_DATA,         9).
-define(M5P_SUBSCRIPTION_ID,          11).
-define(M5P_SESSION_EXPIRY_INTERVAL,  17).
-define(M5P_ASSIGNED_CLIENT_ID,       18).
-define(M5P_SERVER_KEEP_ALIVE,        19).
-define(M5P_AUTHENTICATION_METHOD,    21).
-define(M5P_AUTHENTICATION_DATA,      22).
-define(M5P_REQUEST_PROBLEM_INFO,     23).
-define(M5P_WILL_DELAY_INTERVAL,      24).
-define(M5P_REQUEST_RESPONSE_INFO,    25).
-define(M5P_RESPONSE_INFO,            26).
-define(M5P_SERVER_REF,               28).
-define(M5P_REASON_STRING,            31).
-define(M5P_RECEIVE_MAX,              33).
-define(M5P_TOPIC_ALIAS_MAX,          34).
-define(M5P_TOPIC_ALIAS,              35).
-define(M5P_MAXIMUM_QOS,              36).
-define(M5P_RETAIN_AVAILABLE,         37).
-define(M5P_USER_PROPERTY,            38).
-define(M5P_MAX_PACKET_SIZE,          39).
-define(M5P_WILDCARD_SUBS_AVAILABLE,  40).
-define(M5P_SUB_IDS_AVAILABLE,        41).
-define(M5P_SHARED_SUBS_AVAILABLE,    42).



%% frame types
-define(CONNECT,      1).
-define(CONNACK,      2).
-define(PUBLISH,      3).
-define(PUBACK,       4).
-define(PUBREC,       5).
-define(PUBREL,       6).
-define(PUBCOMP,      7).
-define(SUBSCRIBE,    8).
-define(SUBACK,       9).
-define(UNSUBSCRIBE, 10).
-define(UNSUBACK,    11).
-define(PINGREQ,     12).
-define(PINGRESP,    13).
-define(DISCONNECT,  14).
-define(AUTH,        15).

-define(RESERVED, 0).
-define(PROTOCOL_MAGIC_31, <<"MQIsdp">>).
-define(PROTOCOL_MAGIC_311, <<"MQTT">>).
-define(PROTOCOL_MAGIC_5,   <<"MQTT">>).
-define(PROTOCOL_5, 5).
-define(MAX_LEN, 16#fffffff).
-define(HIGHBIT, 2#10000000).
-define(LOWBITS, 2#01111111).

-define(MAX_PACKET_SIZE, 268435455).

-define(allowedSubackRCs,
        [?M5_GRANTED_QOS0, 
         ?M5_GRANTED_QOS1, 
         ?M5_GRANTED_QOS2, 
         ?M5_UNSPECIFIED_ERROR, 
         ?M5_IMPL_SPECIFIC_ERROR, 
         ?M5_NOT_AUTHORIZED, 
         ?M5_TOPIC_FILTER_INVALID, 
         ?M5_PACKET_ID_IN_USE, 
         ?M5_QUOTA_EXCEEDED, 
         ?M5_SHARED_SUBS_NOT_SUPPORTED, 
         ?M5_SUBSCRIPTION_IDS_NOT_SUPPORTED,
         ?M5_WILDCARD_SUBS_NOT_SUPPORTED]).

-define(allowedUnsubackRCs,
        [?M5_SUCCESS,
         ?M5_NO_SUBSCRIPTION_EXISTED,
         ?M5_UNSPECIFIED_ERROR, 
         ?M5_IMPL_SPECIFIC_ERROR, 
         ?M5_NOT_AUTHORIZED, 
         ?M5_TOPIC_FILTER_INVALID, 
         ?M5_PACKET_ID_IN_USE, 
         ?M5_QUOTA_EXCEEDED]).

