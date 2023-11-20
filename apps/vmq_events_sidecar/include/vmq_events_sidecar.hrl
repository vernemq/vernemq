-ifndef(VERNEMQ_EVENTS_SIDECAR_HRL).
-define(VERNEMQ_EVENTS_SIDECAR_HRL, true).
-define(APP, vmq_events_sidecar).
-define(CLIENT, vmq_events_sidecar_client).

%% types
-type event() :: {atom(), integer(), tuple()}.
-type pool_size() :: pos_integer().
-type reason() :: atom().
-endif.
