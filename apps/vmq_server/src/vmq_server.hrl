-include_lib("vernemq_dev/include/vernemq_dev.hrl").
-type routing_key()         :: [binary()].
-type msg_ref()             :: binary().

-type plugin_id()       :: {plugin, atom(), pid()}.

-type sg_policy() :: prefer_local | local_only | random.
-record(vmq_msg, {
          msg_ref               :: msg_ref() | 'undefined', % OTP-12719
          routing_key           :: routing_key() | 'undefined',
          payload               :: payload() | 'undefined',
          retain=false          :: flag(),
          dup=false             :: flag(),
          qos                   :: qos(),
          mountpoint            :: mountpoint(),
          persisted=false       :: flag(),
          sg_policy=prefer_local:: sg_policy(),
          %% TODOv5: need to import the mqtt5 property typespec?
          properties=#{}        :: map(),
          expiry_ts             :: undefined
                                 | {expire_after, non_neg_integer()}
                                 | {non_neg_integer(), non_neg_integer()}
         }).
-type msg()             :: #vmq_msg{}.

%% TODO: these definitions should probably be moved somewhere else.
-define(SESSION_TAKEN_OVER,       session_taken_over).
-define(NORMAL_DISCONNECT,        normal_disconnect).
-define(ADMINISTRATIVE_ACTION,    administrative_action).
-define(DISCONNECT_KEEP_ALIVE,    disconnect_keep_alive).
-define(DISCONNECT_MIGRATION,     disconnect_migration).
-define(BAD_AUTH_METHOD,          bad_auth_method).
-define(PROTOCOL_ERROR,           protocol_error).


-type disconnect_reasons() ::
        ?NORMAL_DISCONNECT |
        ?SESSION_TAKEN_OVER |
        ?ADMINISTRATIVE_ACTION |
        ?DISCONNECT_KEEP_ALIVE |
        ?DISCONNECT_MIGRATION |
        ?BAD_AUTH_METHOD |
        ?PROTOCOL_ERROR.
