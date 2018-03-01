-include_lib("vernemq_dev/include/vernemq_dev.hrl").

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


-type msg_id()          :: undefined | 0..65535.

-record(mqtt5_lwt, {
          will_properties=#{} :: map(),
          will_retain       :: flag(),
          will_qos          :: qos(),
          will_topic        :: topic(),
          will_msg          :: payload()
         }).

-type mqtt5_lwt() :: #mqtt5_lwt{}.


-record(mqtt5_connect, {
          proto_ver         :: 5,
          username          :: username(),
          password          :: password(),
          clean_start       :: flag(),
          keep_alive        :: non_neg_integer(),
          client_id         :: client_id(),
          lwt               :: mqtt5_lwt() | undefined,
          properties=#{}    :: map()
         }).
-type mqtt5_connect()        :: #mqtt5_connect{}.

-type mqtt5_will_property() :: p_will_delay_interval()
                             | p_payload_format_indicator()
                             | p_message_expiry_interval()
                             | p_content_type()
                             | p_response_topic()
                             | p_correlation_data()
                             | user_property().

-record(mqtt5_connack, {
          session_present   :: flag(),
          reason_code       :: reason_code(),
          properties=#{}     :: [mqtt5_property]
         }).
-type mqtt5_connack()       :: #mqtt5_connack{}.

-record(mqtt5_publish, {
          message_id        :: msg_id(),
          topic             :: topic(),
          qos               :: qos(),
          retain            :: flag(),
          dup               :: flag(),
          properties=#{}    :: map(),
          payload           :: payload()
        }).
-type mqtt5_publish()       :: #mqtt5_publish{}.

-record(mqtt5_puback, {
          message_id        :: msg_id(),
          reason_code       :: reason_code(),
          properties=#{}    :: map()
         }).
-type mqtt5_puback()        :: #mqtt5_puback{}.

-record(mqtt5_pubrec, {
          message_id        :: msg_id(),
          reason_code       :: reason_code(),
          properties=#{}    :: map()
         }).
-type mqtt5_pubrec()        :: #mqtt5_pubrec{}.

-record(mqtt5_pubrel, {
          message_id        :: msg_id(),
          reason_code       :: reason_code(),
          properties=#{}    :: map()
         }).
-type mqtt5_pubrel()        :: #mqtt5_pubrel{}.

-record(mqtt5_pubcomp, {
          message_id        :: msg_id(),
          reason_code       :: reason_code(),
          properties=#{}    :: map()
         }).
-type mqtt5_pubcomp()       :: #mqtt5_pubcomp{}.

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
          properties=#{}     :: map()
         }).
-type mqtt5_subscribe()     :: #mqtt5_subscribe{}.

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
          properties=#{}     :: map()
         }).
-type mqtt5_suback()        :: #mqtt5_suback{}.

-record(mqtt5_unsubscribe, {
          message_id        :: msg_id(),
          topics=[]         :: [topic()],
          properties=#{}     :: [mqtt5_property()]
         }).
-type mqtt5_unsubscribe()   :: #mqtt5_unsubscribe{}.

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
          properties=#{}     :: map()
         }).
-type mqtt5_unsuback()      :: #mqtt5_unsuback{}.

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
          properties=#{}     :: map()
         }).
-type mqtt5_disconnect()    :: #mqtt5_disconnect{}.

-record(mqtt5_auth, {
         reason_code        :: ?M5_SUCCESS |
                               ?M5_CONTINUE_AUTHENTICATION |
                               ?M5_REAUTHENTICATE,
         properties=#{}      :: map()
         }).
-type mqtt5_auth()          :: #mqtt5_auth{}.

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
-type p_payload_format_indicator() :: #p_payload_format_indicator{}.

-record(p_message_expiry_interval, {value :: seconds()}).
-type p_message_expiry_interval() :: #p_message_expiry_interval{}.

-record(p_content_type, {value :: utf8string()}).
-type p_content_type() :: #p_content_type{}.

-record(p_response_topic, {value :: topic()}).
-type p_response_topic() :: #p_content_type{}.

-record(p_correlation_data, {value :: binary()}).
-type p_correlation_data() :: #p_correlation_data{}.

-record(p_subscription_id, {value :: 1..268435455}).
-type p_subscription_id() :: #p_subscription_id{}.

-record(p_session_expiry_interval, {value :: seconds()}).
-type p_session_expiry_interval() :: #p_session_expiry_interval{}.

-record(p_assigned_client_id, {value :: utf8string()}).
-type p_assigned_client_id() :: #p_assigned_client_id{}.

-record(p_server_keep_alive, {value :: seconds()}).
-type p_server_keep_alive() :: #p_server_keep_alive{}.

-record(p_authentication_method, {value :: utf8string()}).
-type p_authentication_method() :: #p_authentication_method{}.

-record(p_authentication_data, {value :: binary()}).
-type p_authentication_data() :: #p_authentication_data{}.

-record(p_request_problem_info, {value :: boolean()}).
-type p_request_problem_info() :: #p_request_problem_info{}.

-record(p_will_delay_interval, {value :: seconds()}).
-type p_will_delay_interval() :: #p_will_delay_interval{}.

-record(p_request_response_info, {value :: boolean()}).
-type p_request_response_info() :: #p_request_response_info{}.

-record(p_response_info, {value :: utf8string()}).
-type p_response_info() :: #p_response_info{}.

-record(p_server_ref, {value :: utf8string()}).
-type p_server_ref() :: #p_server_ref{}.

-record(p_reason_string, {value :: utf8string()}).
-type p_reason_string() :: #p_reason_string{}.

-record(p_receive_max, {value :: 1..65535}).
-type p_receive_max() :: #p_receive_max{}.

-record(p_topic_alias_max, {value :: 0..65535}).
-type p_topic_alias_max() :: #p_topic_alias_max{}.

-record(p_topic_alias, {value :: 1..65535}).
-type p_topic_alias() :: #p_topic_alias{}.

%% Spec: It is a Protocol Error to include Maximum QoS more than once,
%% or to have a value other than 0 or 1. If the Maximum QoS is absent,
%% the Client uses a Maximum QoS of 2.
%%
%% TODO: Maybe we want to explicity model qos2 as well as that might
%% be easier to handle in the code - in that case we should extend
%% this type to include 2.q
-record(p_max_qos, {value :: 0 | 1}).
-type p_max_qos() :: #p_max_qos{}.

-record(p_retain_available, {value :: boolean()}).
-type p_retain_available() :: #p_retain_available{}.

-record(p_user_property, {value :: user_property()}).
-type p_user_property() :: #p_user_property{}.

-record(p_max_packet_size, {value :: 1..4294967296}).
-type p_max_packet_size() :: #p_max_packet_size{}.

-record(p_wildcard_subs_available, {value :: boolean()}).
-type p_wildcard_subs_available() :: #p_wildcard_subs_available{}.

-record(p_sub_ids_available, {value :: boolean()}).
-type p_sub_ids_available() :: #p_sub_ids_available{}.

-record(p_shared_subs_available, {value :: boolean()}).
-type p_shared_subs_available() :: #p_shared_subs_available{}.

-type mqtt5_property() :: p_payload_format_indicator()
                        | p_message_expiry_interval()
                        | p_content_type()
                        | p_response_topic()
                        | p_correlation_data()
                        | p_subscription_id()
                        | p_session_expiry_interval()
                        | p_assigned_client_id()
                        | p_server_keep_alive()
                        | p_authentication_method()
                        | p_authentication_data()
                        | p_request_problem_info()
                        | p_will_delay_interval()
                        | p_request_response_info()
                        | p_response_info()
                        | p_server_ref()
                        | p_reason_string()
                        | p_receive_max()
                        | p_topic_alias_max()
                        | p_topic_alias()
                        | p_max_qos()
                        | p_retain_available()
                        | p_user_property()
                        | p_max_packet_size()
                        | p_wildcard_subs_available()
                        | p_sub_ids_available()
                        | p_shared_subs_available().

-type reason_code()         :: ?M5_GRANTED_QOS0
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
