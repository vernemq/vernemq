-ifndef(VMQ_SERVER_HRL).
-define(VMQ_SERVER_HRL, true).
-include_lib("vmq_commons/include/vmq_types.hrl").

-type plugin_id()       :: {plugin, atom(), pid()}.

-record(retain_msg,
        {
          payload    :: binary(),
          properties :: any(),
          expiry_ts  :: undefined
                      | msg_expiry_ts()
        }).
-type retain_msg() :: #retain_msg{}.

-record(deliver,
        {
         qos        :: qos(),
         %% an undefined msg_id means this message has never been sent
         %% to the client or that it is a qos0 message.

         %% TODO use `msg_id()` type instead, but currently no in scope.
         msg_id     :: undefined | non_neg_integer(),
         msg        :: msg()
        }).

-type deliver() :: #deliver{}.

-type subscription() :: {topic(), subinfo()}.
-define(INTERNAL_CLIENT_ID, '$vmq_internal_client_id').

%% These reason codes are used internally within vernemq and are not
%% *real* MQTT reason codes.
-define(DISCONNECT_KEEP_ALIVE,    disconnect_keep_alive).
-define(DISCONNECT_MIGRATION,     disconnect_migration).
-define(CLIENT_DISCONNECT,        mqtt_client_disconnect).

-type disconnect_reasons() ::
        ?NOT_AUTHORIZED |
        ?NORMAL_DISCONNECT |
        ?SESSION_TAKEN_OVER |
        ?ADMINISTRATIVE_ACTION |
        ?DISCONNECT_KEEP_ALIVE |
        ?DISCONNECT_MIGRATION |
        ?BAD_AUTHENTICATION_METHOD |
        ?PROTOCOL_ERROR |
        ?RECEIVE_MAX_EXCEEDED |
        ?CLIENT_DISCONNECT.

-type duration_ms() :: non_neg_integer().
-type session_ctrl() :: #{throttle => duration_ms()}.
-type aop_success_fun() :: fun((msg(), list(), session_ctrl()) ->
                                      {ok, msg()} |
                                      {ok, msg(), session_ctrl()} |
                                      {error, atom()}).

-type reg_view_fold_fun() :: fun((node() | {subscriber_id(), qos(), client_id() | any()}, any()) -> any()).
-endif.
