-include_lib("swc/include/swc.hrl").

-define(DB_OBJ, obj).
-define(DB_DKM, dkm).
-define(DB_DEFAULT, default).

-type key_prefix() :: atom().
-type key_suffix() :: term().
-type key() :: {key_prefix(), key_suffix()}.

-type type() :: ?DB_OBJ | ?DB_DKM | ?DB_DEFAULT.
-type deleted() :: '$deleted'.
-type db_key()     :: binary().
-type db_value()   :: binary().
-type foldfun() :: fun((db_key(), db_value(), any()) -> any()).
-type db_op() :: {type(), db_key(), db_value() | deleted()}.
-type opts() :: any().
-type db_name() :: atom().

-type peer() :: atom().
-type actor() :: binary().
-type swc_id() :: {peer(), actor()}.
-type watermark() :: vv_matrix().
-type nodeclock() :: bvv().
-type object() :: dcc().
-type context() :: vv().
-type dotkeymap() :: vmq_swc_dkm:dkm().

-define(DELETED, '$deleted').

-record(swc_config, {
          peer          :: swc_id(),
          group         :: atom(),
          db            :: db_name(),
          db_backend    :: atom(),
          store         :: atom(),
          r_o_w_cache   :: atom(),
          batcher       :: atom(),
          membership    :: atom(),
          transport     :: atom()
         }).
-type config() :: #swc_config{}.
