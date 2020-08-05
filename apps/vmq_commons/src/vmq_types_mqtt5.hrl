-include_lib("vernemq_dev/include/vernemq_dev.hrl").
-include("vmq_types_common.hrl").

%% Reason codes
-define(M5_SUCCESS,                        16#00).
-define(M5_NORMAL_DISCONNECT,              16#00).
-define(M5_CONNACK_ACCEPT,                 16#00).
-define(M5_GRANTED_QOS0,                   16#00).
-define(M5_GRANTED_QOS1,                   16#01).
-define(M5_GRANTED_QOS2,                   16#02).
-define(M5_DISCONNECT_WITH_WILL_MSG,       16#04).
-define(M5_NO_MATCHING_SUBSCRIBERS,        16#10).
-define(M5_NO_SUBSCRIPTION_EXISTED,        16#11).
-define(M5_CONTINUE_AUTHENTICATION,        16#18).
-define(M5_REAUTHENTICATE,                 16#19).
-define(M5_UNSPECIFIED_ERROR,              16#80).
-define(M5_MALFORMED_PACKET,               16#81).
-define(M5_PROTOCOL_ERROR,                 16#82).
-define(M5_IMPL_SPECIFIC_ERROR,            16#83).
-define(M5_UNSUPPORTED_PROTOCOL_VERSION,   16#84).
-define(M5_CLIENT_IDENTIFIER_NOT_VALID,    16#85).
-define(M5_BAD_USERNAME_OR_PASSWORD,       16#86).
-define(M5_NOT_AUTHORIZED,                 16#87).
-define(M5_SERVER_UNAVAILABLE,             16#88).
-define(M5_SERVER_BUSY,                    16#89).
-define(M5_BANNED,                         16#8A).
-define(M5_SERVER_SHUTTING_DOWN,           16#8B).
-define(M5_BAD_AUTHENTICATION_METHOD,      16#8C).
-define(M5_KEEP_ALIVE_TIMEOUT,             16#8D).
-define(M5_SESSION_TAKEN_OVER,             16#8E).
-define(M5_TOPIC_FILTER_INVALID,           16#8F).
-define(M5_TOPIC_NAME_INVALID,             16#90).
-define(M5_PACKET_ID_IN_USE,               16#91).
-define(M5_PACKET_ID_NOT_FOUND,            16#92).
-define(M5_RECEIVE_MAX_EXCEEDED,           16#93).
-define(M5_TOPIC_ALIAS_INVALID,            16#94).
-define(M5_PACKET_TOO_LARGE,               16#95).
-define(M5_MESSAGE_RATE_TOO_HIGH,          16#96).
-define(M5_QUOTA_EXCEEDED,                 16#97).
-define(M5_ADMINISTRATIVE_ACTION,          16#98).
-define(M5_PAYLOAD_FORMAT_INVALID,         16#99).
-define(M5_RETAIN_NOT_SUPPORTED,           16#9A).
-define(M5_QOS_NOT_SUPPORTED,              16#9B).
-define(M5_USE_ANOTHER_SERVER,             16#9C).
-define(M5_SERVER_MOVED,                   16#9D).
-define(M5_SHARED_SUBS_NOT_SUPPORTED,      16#9E).
-define(M5_CONNECTION_RATE_EXCEEDED,       16#9F).
-define(M5_MAX_CONNECT_TIME,               16#A0).
-define(M5_SUBSCRIPTION_IDS_NOT_SUPPORTED, 16#A1).
-define(M5_WILDCARD_SUBS_NOT_SUPPORTED,    16#A2).


-type mqtt5_properties() :: map().

-record(mqtt5_lwt, {
          will_properties=#{} :: mqtt5_properties(),
          will_retain       :: flag(),
          will_qos          :: qos(),
          will_topic        :: topic(),
          will_msg          :: payload()
         }).

-type mqtt5_lwt() :: #mqtt5_lwt{}.


-define(P_PAYLOAD_FORMAT_INDICATOR_ASSOC, p_payload_format_indicator => unspecified | utf8).
-define(P_MESSAGE_EXPIRY_INTERVAL_ASSOC, p_message_expiry_interval => seconds()).
-define(P_CONTENT_TYPE_ASSOC, p_content_type => utf8string()).
-define(P_RESPONSE_TOPIC_ASSOC, p_response_topic => topic()).
-define(P_CORRELATION_DATA_ASSOC, p_correlation_data => binary()).
-define(P_SUBSCRIPTION_ID_ASSOC, p_subscription_id => [subscription_id()]).
-define(P_SESSION_EXPIRY_INTERVAL_ASSOC, p_session_expiry_interval => seconds()).
-define(P_ASSIGNED_CLIENT_ID_ASSOC, p_assigned_client_id => utf8string()).
-define(P_SERVER_KEEP_ALIVE_ASSOC, p_server_keep_alive => seconds()).
-define(P_AUTHENTICATION_METHOD_ASSOC, p_authentication_method => utf8string()).
-define(P_AUTHENTICATION_DATA_ASSOC, p_authentication_data => binary()).
-define(P_REQUEST_PROBLEM_INFO_ASSOC, p_request_problem_info => boolean()).
-define(P_WILL_DELAY_INTERVAL_ASSOC, p_will_delay_interval => seconds()).
-define(P_REQUEST_RESPONSE_INFO_ASSOC, p_request_response_info => boolean()).
-define(P_RESPONSE_INFO_ASSOC, p_response_info => utf8string()).
-define(P_SERVER_REF_ASSOC, p_server_ref => utf8string()).
-define(P_REASON_STRING_ASSOC, p_reason_string => utf8string()).
-define(P_RECEIVE_MAX_ASSOC, p_receive_max => 1..65535).
-define(P_TOPIC_ALIAS_MAX_ASSOC, p_topic_alias_max => 1..65535).
-define(P_TOPIC_ALIAS_ASSOC, p_topic_alias => 1..65535).
-define(P_MAX_QOS_ASSOC, p_max_qos => 0|1).
-define(P_RETAIN_AVAILABLE_ASSOC, p_retain_available => boolean()).
-define(P_USER_PROPERTY_ASSOC, p_user_property => [user_property()]).
-define(P_MAX_PACKET_SIZE_ASSOC, p_max_packet_size => 1..4294967296).
-define(P_WILDCARD_SUBS_AVAILABLE_ASSOC, p_wildcard_subs_available => boolean()).
-define(P_SUB_IDS_AVAILABLE_ASSOC, p_sub_ids_available => boolean()).
-define(P_SHARED_SUBS_AVAILABLE_ASSOC, p_shared_subs_available => boolean()).

-record(mqtt5_connect, {
          proto_ver         :: 5,
          username          :: username(),
          password          :: password(),
          clean_start       :: flag(),
          keep_alive        :: non_neg_integer(),
          client_id         :: client_id(),
          lwt               :: mqtt5_lwt() | undefined,
          properties=#{}    :: mqtt5_properties()
         }).
-type mqtt5_connect()        :: #mqtt5_connect{}.
-type mqtt5_connect_props() :: #{?P_SESSION_EXPIRY_INTERVAL_ASSOC,
                                 ?P_AUTHENTICATION_METHOD_ASSOC,
                                 ?P_AUTHENTICATION_DATA_ASSOC,
                                 ?P_REQUEST_PROBLEM_INFO_ASSOC,
                                 ?P_RECEIVE_MAX_ASSOC,
                                 ?P_TOPIC_ALIAS_MAX_ASSOC,
                                 ?P_USER_PROPERTY_ASSOC,
                                 ?P_MAX_PACKET_SIZE_ASSOC}.
-type mqtt5_will_property() :: #{?P_WILL_DELAY_INTERVAL_ASSOC,
                                 ?P_PAYLOAD_FORMAT_INDICATOR_ASSOC,
                                 ?P_MESSAGE_EXPIRY_INTERVAL_ASSOC,
                                 ?P_CONTENT_TYPE_ASSOC,
                                 ?P_RESPONSE_TOPIC_ASSOC,
                                 ?P_CORRELATION_DATA_ASSOC,
                                 ?P_USER_PROPERTY_ASSOC}.

-record(mqtt5_connack, {
          session_present   :: flag(),
          reason_code       :: reason_code(),
          properties=#{}    :: mqtt5_connack_props()
         }).
-type mqtt5_connack()       :: #mqtt5_connack{}.
-type mqtt5_connack_props() :: #{?P_SESSION_EXPIRY_INTERVAL_ASSOC,
                                 ?P_ASSIGNED_CLIENT_ID_ASSOC,
                                 ?P_SERVER_KEEP_ALIVE_ASSOC,
                                 ?P_AUTHENTICATION_DATA_ASSOC,
                                 ?P_AUTHENTICATION_METHOD_ASSOC,
                                 ?P_RESPONSE_INFO_ASSOC,
                                 ?P_SERVER_REF_ASSOC,
                                 ?P_REASON_STRING_ASSOC,
                                 ?P_RECEIVE_MAX_ASSOC,
                                 ?P_TOPIC_ALIAS_MAX_ASSOC,
                                 ?P_MAX_QOS_ASSOC,
                                 ?P_MAX_PACKET_SIZE_ASSOC,
                                 ?P_WILDCARD_SUBS_AVAILABLE_ASSOC,
                                 ?P_SUB_IDS_AVAILABLE_ASSOC,
                                 ?P_SHARED_SUBS_AVAILABLE_ASSOC}.

-record(mqtt5_publish, {
          message_id        :: msg_id(),
          topic             :: topic(),
          qos               :: qos(),
          retain            :: flag(),
          dup               :: flag(),
          properties=#{}    :: mqtt5_publish_props(),
          payload           :: payload()
        }).
-type mqtt5_publish()       :: #mqtt5_publish{}.
-type mqtt5_publish_props() :: #{?P_PAYLOAD_FORMAT_INDICATOR_ASSOC,
                                 ?P_MESSAGE_EXPIRY_INTERVAL_ASSOC,
                                 ?P_CONTENT_TYPE_ASSOC,
                                 ?P_RESPONSE_TOPIC_ASSOC,
                                 ?P_CORRELATION_DATA_ASSOC,
                                 ?P_SUBSCRIPTION_ID_ASSOC,
                                 ?P_TOPIC_ALIAS_ASSOC,
                                 ?P_USER_PROPERTY_ASSOC}.

-record(mqtt5_puback, {
          message_id        :: msg_id(),
          reason_code       :: reason_code(),
          properties=#{}    :: mqtt5_puback_props()
         }).
-type mqtt5_puback()        :: #mqtt5_puback{}.
-type mqtt5_puback_props()  :: #{?P_REASON_STRING_ASSOC,
                                 ?P_USER_PROPERTY_ASSOC}.

-record(mqtt5_pubrec, {
          message_id        :: msg_id(),
          reason_code       :: reason_code(),
          properties=#{}    :: mqtt5_pubrec_props()
         }).
-type mqtt5_pubrec()        :: #mqtt5_pubrec{}.
-type mqtt5_pubrec_props()  :: #{?P_REASON_STRING_ASSOC,
                                 ?P_USER_PROPERTY_ASSOC}.

-record(mqtt5_pubrel, {
          message_id        :: msg_id(),
          reason_code       :: reason_code(),
          properties=#{}    :: mqtt5_pubrel_props()
         }).
-type mqtt5_pubrel()        :: #mqtt5_pubrel{}.
-type mqtt5_pubrel_props()  :: #{?P_REASON_STRING_ASSOC,
                                 ?P_USER_PROPERTY_ASSOC}.
-record(mqtt5_pubcomp, {
          message_id        :: msg_id(),
          reason_code       :: reason_code(),
          properties=#{}    :: mqtt5_pubcomp_props()
         }).
-type mqtt5_pubcomp()       :: #mqtt5_pubcomp{}.
-type mqtt5_pubcomp_props() :: #{?P_REASON_STRING_ASSOC,
                                 ?P_USER_PROPERTY_ASSOC}.

-type no_local() :: flag().
-type rap() :: flag().
-type retain_handling() :: send_retain | send_if_new_sub | dont_send.

-record(mqtt5_subscribe_topic, {
          topic             :: topic(),
          qos               :: qos(),
          no_local          :: no_local(),
          rap               :: rap(),
          retain_handling   :: retain_handling()
         }).
-type mqtt5_subscribe_topic() :: #mqtt5_subscribe_topic{}.

-record(mqtt5_subscribe, {
          message_id        :: msg_id(),
          topics=[]         :: [mqtt5_subscribe_topic()],
          properties=#{}    :: mqtt5_sub_props()
         }).
-type mqtt5_subscribe()     :: #mqtt5_subscribe{}.
-type mqtt5_sub_props()     :: #{?P_SUBSCRIPTION_ID_ASSOC,
                                 ?P_USER_PROPERTY_ASSOC}.

-record(mqtt5_suback, {
          message_id        :: msg_id(),
          reason_codes=[]   :: [?M5_GRANTED_QOS0 |
                                ?M5_GRANTED_QOS1 |
                                ?M5_GRANTED_QOS2 |
                                ?M5_UNSPECIFIED_ERROR |
                                ?M5_IMPL_SPECIFIC_ERROR |
                                ?M5_NOT_AUTHORIZED |
                                ?M5_TOPIC_FILTER_INVALID |
                                ?M5_PACKET_ID_IN_USE |
                                ?M5_QUOTA_EXCEEDED |
                                ?M5_SHARED_SUBS_NOT_SUPPORTED |
                                ?M5_SUBSCRIPTION_IDS_NOT_SUPPORTED |
                                ?M5_WILDCARD_SUBS_NOT_SUPPORTED],
          properties=#{}     :: mqtt5_suback_props()
         }).
-type mqtt5_suback()        :: #mqtt5_suback{}.
-type mqtt5_suback_props( ) :: #{?P_REASON_STRING_ASSOC,
                                 ?P_USER_PROPERTY_ASSOC}.

-record(mqtt5_unsubscribe, {
          message_id        :: msg_id(),
          topics=[]         :: nonempty_list(topic()),
          properties=#{}    :: mqtt5_unsub_props()
         }).
-type mqtt5_unsubscribe()   :: #mqtt5_unsubscribe{}.
-type mqtt5_unsub_props()   :: #{?P_USER_PROPERTY_ASSOC}.

-record(mqtt5_unsuback, {
          message_id        :: msg_id(),
          reason_codes=[]   :: [?M5_SUCCESS |
                                ?M5_NO_SUBSCRIPTION_EXISTED |
                                ?M5_UNSPECIFIED_ERROR |
                                ?M5_IMPL_SPECIFIC_ERROR |
                                ?M5_NOT_AUTHORIZED |
                                ?M5_TOPIC_FILTER_INVALID |
                                ?M5_PACKET_ID_IN_USE |
                                ?M5_QUOTA_EXCEEDED],
          properties=#{}     :: mqtt5_unsuback_props()
         }).
-type mqtt5_unsuback()      :: #mqtt5_unsuback{}.
-type mqtt5_unsuback_props( ) :: #{?P_REASON_STRING_ASSOC,
                                   ?P_USER_PROPERTY_ASSOC}.

-record(mqtt5_pingreq, {}).
-type mqtt5_pingreq()       :: #mqtt5_pingreq{}.

-record(mqtt5_pingresp, {}).
-type mqtt5_pingresp()      :: #mqtt5_pingresp{}.

-record(mqtt5_disconnect, {
          reason_code       :: ?M5_NORMAL_DISCONNECT |
                               ?M5_DISCONNECT_WITH_WILL_MSG |
                               ?M5_UNSPECIFIED_ERROR |
                               ?M5_MALFORMED_PACKET |
                               ?M5_PROTOCOL_ERROR |
                               ?M5_IMPL_SPECIFIC_ERROR |
                               ?M5_NOT_AUTHORIZED |
                               ?M5_NOT_AUTHORIZED |
                               ?M5_SERVER_BUSY |
                               ?M5_SERVER_SHUTTING_DOWN |
                               ?M5_KEEP_ALIVE_TIMEOUT |
                               ?M5_SESSION_TAKEN_OVER |
                               ?M5_TOPIC_FILTER_INVALID |
                               ?M5_TOPIC_NAME_INVALID |
                               ?M5_RECEIVE_MAX_EXCEEDED |
                               ?M5_TOPIC_ALIAS_INVALID |
                               ?M5_PACKET_TOO_LARGE |
                               ?M5_MESSAGE_RATE_TOO_HIGH |
                               ?M5_QUOTA_EXCEEDED |
                               ?M5_ADMINISTRATIVE_ACTION |
                               ?M5_PAYLOAD_FORMAT_INVALID |
                               ?M5_RETAIN_NOT_SUPPORTED |
                               ?M5_QOS_NOT_SUPPORTED |
                               ?M5_USE_ANOTHER_SERVER |
                               ?M5_SERVER_MOVED |
                               ?M5_SHARED_SUBS_NOT_SUPPORTED |
                               ?M5_CONNECTION_RATE_EXCEEDED |
                               ?M5_MAX_CONNECT_TIME |
                               ?M5_SUBSCRIPTION_IDS_NOT_SUPPORTED |
                               ?M5_WILDCARD_SUBS_NOT_SUPPORTED,
          properties=#{}     :: mqtt5_disconnect_props()
         }).
-type mqtt5_disconnect()    :: #mqtt5_disconnect{}.
-type mqtt5_disconnect_props() :: #{?P_SESSION_EXPIRY_INTERVAL_ASSOC,
                                    ?P_SERVER_REF_ASSOC,
                                    ?P_REASON_STRING_ASSOC,
                                    ?P_USER_PROPERTY_ASSOC}.

-record(mqtt5_auth, {
         reason_code        :: ?M5_SUCCESS |
                               ?M5_CONTINUE_AUTHENTICATION |
                               ?M5_REAUTHENTICATE,
         properties=#{}     :: mqtt5_auth_props()
         }).
-type mqtt5_auth()          :: #mqtt5_auth{}.
-type mqtt5_auth_props()    :: #{?P_AUTHENTICATION_METHOD_ASSOC,
                                 ?P_AUTHENTICATION_DATA_ASSOC,
                                 ?P_REASON_STRING_ASSOC,
                                 ?P_USER_PROPERTY_ASSOC}.

-type mqtt5_frame()         :: mqtt5_connect()
                             | mqtt5_connack()
                             | mqtt5_publish()
                             | mqtt5_puback()
                             | mqtt5_pubrec()
                             | mqtt5_pubrel()
                             | mqtt5_pubcomp()
                             | mqtt5_subscribe()
                             | mqtt5_suback()
                             | mqtt5_unsubscribe()
                             | mqtt5_unsuback()
                             | mqtt5_pingreq()
                             | mqtt5_pingresp()
                             | mqtt5_disconnect()
                             | mqtt5_auth().

%% TODO: these are only preliminary types so we had something to use
%% below. Should probable be replaced with something else.
-type utf8string() :: binary().
-type user_property() :: {utf8string(), utf8string()}.
-type seconds() :: non_neg_integer().

%% MQTT5 property records and types
-record(p_payload_format_indicator, {
          value :: unspecified | utf8
         }).
-record(p_message_expiry_interval, {value :: seconds()}).
-record(p_content_type, {value :: utf8string()}).
-record(p_response_topic, {value :: topic()}).
-record(p_correlation_data, {value :: binary()}).
%% As there can be more than one subscription id in an outgoing
%% publish frame (only one in a subscribe frame) and we represent the
%% properties as a map we store multiple values in the record.
-type subscription_id() :: 1..268435455.
-record(p_subscription_id, {value :: [subscription_id()]}).
-record(p_session_expiry_interval, {value :: seconds()}).
-record(p_assigned_client_id, {value :: utf8string()}).
-record(p_server_keep_alive, {value :: seconds()}).
-record(p_authentication_method, {value :: utf8string()}).
-record(p_authentication_data, {value :: binary()}).
-record(p_request_problem_info, {value :: boolean()}).
-record(p_will_delay_interval, {value :: seconds()}).
-record(p_request_response_info, {value :: boolean()}).
-record(p_response_info, {value :: utf8string()}).
-record(p_server_ref, {value :: utf8string()}).
-record(p_reason_string, {value :: utf8string()}).
-record(p_receive_max, {value :: 1..65535}).
-record(p_topic_alias_max, {value :: 0..65535}).
-record(p_topic_alias, {value :: 1..65535}).
%% Spec: It is a Protocol Error to include Maximum QoS more than once,
%% or to have a value other than 0 or 1. If the Maximum QoS is absent,
%% the Client uses a Maximum QoS of 2.
%%
%% TODO: Maybe we want to explicity model qos2 as well as that might
%% be easier to handle in the code - in that case we should extend
%% this type to include 2.q
-record(p_max_qos, {value :: 0 | 1}).
-record(p_retain_available, {value :: boolean()}).
%% as there can be more than one user property id in a single frame
%% and we represent the properties as a map we store multiple values
%% in the record.
-record(p_user_property, {value :: [user_property()]}).
-record(p_max_packet_size, {value :: 1..4294967296}).
-record(p_wildcard_subs_available, {value :: boolean()}).
-record(p_sub_ids_available, {value :: boolean()}).
-record(p_shared_subs_available, {value :: boolean()}).

-type reason_code()         :: ?M5_SUCCESS
                             | ?M5_GRANTED_QOS0
                             | ?M5_GRANTED_QOS1
                             | ?M5_GRANTED_QOS2
                             | ?M5_DISCONNECT_WITH_WILL_MSG
                             | ?M5_NO_MATCHING_SUBSCRIBERS
                             | ?M5_NO_SUBSCRIPTION_EXISTED
                             | ?M5_CONTINUE_AUTHENTICATION
                             | ?M5_REAUTHENTICATE
                             | ?M5_UNSPECIFIED_ERROR
                             | ?M5_MALFORMED_PACKET
                             | ?M5_PROTOCOL_ERROR
                             | ?M5_IMPL_SPECIFIC_ERROR
                             | ?M5_UNSUPPORTED_PROTOCOL_VERSION
                             | ?M5_CLIENT_IDENTIFIER_NOT_VALID
                             | ?M5_BAD_USERNAME_OR_PASSWORD
                             | ?M5_NOT_AUTHORIZED
                             | ?M5_SERVER_UNAVAILABLE
                             | ?M5_SERVER_BUSY
                             | ?M5_BANNED
                             | ?M5_SERVER_SHUTTING_DOWN
                             | ?M5_BAD_AUTHENTICATION_METHOD
                             | ?M5_KEEP_ALIVE_TIMEOUT
                             | ?M5_SESSION_TAKEN_OVER
                             | ?M5_TOPIC_FILTER_INVALID
                             | ?M5_TOPIC_NAME_INVALID
                             | ?M5_PACKET_ID_IN_USE
                             | ?M5_PACKET_ID_NOT_FOUND
                             | ?M5_RECEIVE_MAX_EXCEEDED
                             | ?M5_TOPIC_ALIAS_INVALID
                             | ?M5_PACKET_TOO_LARGE
                             | ?M5_MESSAGE_RATE_TOO_HIGH
                             | ?M5_QUOTA_EXCEEDED
                             | ?M5_ADMINISTRATIVE_ACTION
                             | ?M5_PAYLOAD_FORMAT_INVALID
                             | ?M5_RETAIN_NOT_SUPPORTED
                             | ?M5_QOS_NOT_SUPPORTED
                             | ?M5_USE_ANOTHER_SERVER
                             | ?M5_SERVER_MOVED
                             | ?M5_SHARED_SUBS_NOT_SUPPORTED
                             | ?M5_CONNECTION_RATE_EXCEEDED
                             | ?M5_MAX_CONNECT_TIME
                             | ?M5_SUBSCRIPTION_IDS_NOT_SUPPORTED
                             | ?M5_WILDCARD_SUBS_NOT_SUPPORTED.
