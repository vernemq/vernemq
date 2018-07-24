-include_lib("vmq_commons/include/vmq_types.hrl").
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
          sg_policy=prefer_local:: sg_policy()
         }).
-type msg()             :: #vmq_msg{}.

%% TODO: these definitions should probably be moved somewhere else.
-define(SESSION_TAKEN_OVER,       session_taken_over).
-define(NORMAL_DISCONNECT,        normal_disconnect).
-define(ADMINISTRATIVE_ACTION,    administrative_action).
-define(DISCONNECT_KEEP_ALIVE,    disconnect_keep_alive).
-define(DISCONNECT_MIGRATION,     disconnect_migration).
-define(CLIENT_DISCONNECT,        mqtt_client_disconnect).

-type disconnect_reasons() ::
        ?NORMAL_DISCONNECT |
        ?SESSION_TAKEN_OVER |
        ?ADMINISTRATIVE_ACTION |
        ?DISCONNECT_KEEP_ALIVE |
        ?DISCONNECT_MIGRATION |
        ?CLIENT_DISCONNECT.
