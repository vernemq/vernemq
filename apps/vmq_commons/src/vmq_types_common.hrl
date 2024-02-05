-ifndef(VMQ_TYPE_COMMON_HRL).
-define(VMQ_TYPE_COMMON_HRL, true).
-include_lib("vernemq_dev/include/vernemq_dev.hrl").

-type msg_id() :: undefined | 0..65535.

-type routing_key() :: [binary()].
-type msg_ref() :: binary().

-type msg_expiry_ts() ::
    {expire_after, non_neg_integer()}
    | {non_neg_integer(), non_neg_integer()}.

-record(deliver, {
    qos :: qos(),
    %% an undefined msg_id means this message has never been sent
    %% to the client or that it is a qos0 message.

    %% TODO use `msg_id()` type instead, but currently no in scope.
    msg_id :: undefined | non_neg_integer(),
    msg :: msg()
}).

-record(vmq_msg, {
    % OTP-12719
    msg_ref :: msg_ref() | 'undefined',
    routing_key :: routing_key() | 'undefined',
    payload :: payload() | 'undefined',
    retain = false :: flag(),
    dup = false :: flag(),
    qos :: qos(),
    mountpoint :: mountpoint(),
    persisted = false :: flag(),
    sg_policy = prefer_local :: shared_sub_policy(),
    properties = #{} :: properties(),
    expiry_ts ::
        undefined
        | msg_expiry_ts(),
    non_retry = false :: flag(),
    non_persistence = false :: flag()
}).
-type msg() :: #vmq_msg{}.
-record(matched_acl, {
    name = undefined :: binary() | undefined, pattern = undefined :: binary() | undefined
}).
-endif.
