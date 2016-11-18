%% connect return codes
-define(CONNACK_ACCEPT,      0).
-define(CONNACK_PROTO_VER,   1). %% unacceptable protocol version
-define(CONNACK_INVALID_ID,  2). %% identifier rejected
-define(CONNACK_SERVER,      3). %% server unavailable
-define(CONNACK_CREDENTIALS, 4). %% bad user name or password
-define(CONNACK_AUTH,        5). %% not authorized
-define(true, 1).
-define(false, 0).

-type peer()                :: {inet:ip_address(), inet:port_number()}.
-type username()            :: binary() | undefined.
-type password()            :: binary() | undefined.
-type client_id()           :: binary().
-type mountpoint()          :: string().
-type subscriber_id()       :: {mountpoint(), client_id()}.
-type reg_view()            :: atom().
-type topic()               :: [binary()] | binary().
-type qos()                 :: 0 | 1 | 2.
-type routing_key()         :: [binary()].
-type payload()             :: binary().
-type flag()                :: ?true | ?false | boolean() | empty. % empty for test purposes
-type msg_ref()             :: binary().
-type proto_version()       :: 3 | 4 | 131.
-type return_code()         :: ?CONNACK_ACCEPT
                            | ?CONNACK_PROTO_VER
                            | ?CONNACK_INVALID_ID
                            | ?CONNACK_SERVER
                            | ?CONNACK_CREDENTIALS
                            | ?CONNACK_AUTH.
-type message_id()          :: undefined | 0..65535.

-record(mqtt_connect, {
          proto_ver         :: proto_version(),
          username          :: username(),
          password          :: password(),
          clean_session     :: flag(),
          keep_alive        :: non_neg_integer(),
          client_id         :: client_id(),
          will_retain       :: flag(),
          will_qos          :: qos(),
          will_topic        :: topic(),
          will_msg          :: payload()
         }).
-type mqtt_connect()        :: #mqtt_connect{}.

-record(mqtt_connack, {
          session_present   :: flag(),
          return_code       :: return_code()
         }).
-type mqtt_connack()        :: #mqtt_connack{}.

-record(mqtt_publish, {
          message_id        :: message_id(),
          topic             :: topic(),
          qos               :: qos(),
          retain            :: flag(),
          dup               :: flag(),
          payload           :: payload()
        }).
-type mqtt_publish()        :: #mqtt_publish{}.

-record(mqtt_puback, {
          message_id        :: message_id()
         }).
-type mqtt_puback()        :: #mqtt_puback{}.

-record(mqtt_pubrec, {
          message_id        :: message_id()
         }).
-type mqtt_pubrec()        :: #mqtt_pubrec{}.

-record(mqtt_pubrel, {
          message_id        :: message_id()
         }).
-type mqtt_pubrel()        :: #mqtt_pubrel{}.

-record(mqtt_pubcomp, {
          message_id        :: message_id()
         }).
-type mqtt_pubcomp()        :: #mqtt_pubcomp{}.

-record(mqtt_subscribe, {
          message_id        :: message_id(),
          topics=[]         :: [{topic(), qos()}]
         }).
-type mqtt_subscribe()      :: #mqtt_subscribe{}.

-record(mqtt_unsubscribe, {
          message_id        :: message_id(),
          topics=[]         :: [topic()]
         }).
-type mqtt_unsubscribe()    :: #mqtt_unsubscribe{}.

-record(mqtt_suback, {
          message_id        :: message_id(),
          qos_table=[]      :: [qos()]
         }).
-type mqtt_suback()         :: #mqtt_suback{}.

-record(mqtt_unsuback, {
          message_id        :: message_id()
         }).
-type mqtt_unsuback()       :: #mqtt_unsuback{}.

-record(mqtt_pingreq, {}).
-type mqtt_pingreq()        :: #mqtt_pingreq{}.

-record(mqtt_pingresp, {}).
-type mqtt_pingresp()       :: #mqtt_pingresp{}.

-record(mqtt_disconnect, {}).
-type mqtt_disconnect()     :: #mqtt_disconnect{}.

-type mqtt_frame()          :: mqtt_connect()
                             | mqtt_connack()
                             | mqtt_publish()
                             | mqtt_puback()
                             | mqtt_pubrec()
                             | mqtt_pubrel()
                             | mqtt_pubcomp()
                             | mqtt_subscribe()
                             | mqtt_suback()
                             | mqtt_unsubscribe()
                             | mqtt_unsuback()
                             | mqtt_pingreq()
                             | mqtt_pingresp()
                             | mqtt_disconnect().
