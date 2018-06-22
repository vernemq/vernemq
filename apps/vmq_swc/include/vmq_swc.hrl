-type type() :: dcc | log | default.
-type deleted() :: '$deleted'.
-type key()     :: binary().
-type value()   :: binary().
-type kv() :: {type(), key(), value() | deleted()}.
-type opts() :: any().
-type backend() :: {backend, module()}.
-type db_name() :: atom().
-type itr_name() :: atom().

-define(DEFAULT_BACKEND, {backend, vmq_swc_db_rocksdb}).
-define(DELETED, '$deleted').

-record(swc_config, {
          peer          :: binary(),
          group         :: atom(),
          db            :: db_name(),
          store         :: atom(),
          itr           :: itr_name(),
          batcher       :: atom(),
          membership    :: atom(),
          transport     :: atom()
         }).
-type config() :: #swc_config{}.
