-ifndef(VMQ_TYPE_COMMON_HRL).
-define(VMQ_TYPE_COMMON_HRL, true).
-include_lib("vernemq_dev/include/vernemq_dev.hrl").

-type msg_id()          :: undefined | 0..65535.

-type routing_key()         :: [binary()].
-type msg_ref()             :: binary().

-type msg_expiry_ts() :: {expire_after, non_neg_integer()}
                       | {non_neg_integer(), non_neg_integer()}.

-record(vmq_msg, {
          msg_ref               :: msg_ref() | 'undefined', % OTP-12719
          routing_key           :: routing_key() | 'undefined',
          payload               :: payload() | 'undefined',
          retain=false          :: flag(),
          dup=false             :: flag(),
          qos                   :: qos(),
          mountpoint            :: mountpoint(),
          persisted=false       :: flag(),
          sg_policy=prefer_local:: shared_sub_policy(),
          properties=#{}        :: properties(),
          expiry_ts             :: undefined
                                 | msg_expiry_ts()
         }).
-type msg()             :: #vmq_msg{}.
-endif.
