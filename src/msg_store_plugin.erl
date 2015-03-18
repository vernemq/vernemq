-module(msg_store_plugin).
-include("vmq_types.hrl").

%% all hooks are called as an 'only'-hook, return value is ignored
-callback msg_store_write_sync(MsgRef :: msg_ref(),
                               Value  :: binary()) -> ok | {error, Reason :: any()}.

-callback msg_store_write_async(MsgRef :: msg_ref(),
                                Value  :: binary()) -> ok.

-callback msg_store_read(MsgRef :: msg_ref()) -> {ok, Value :: binary()} | notfound.

-callback msg_store_fold(fun((MsgRef :: msg_ref(),
                              Value :: binary(),
                              Acc :: any()) -> Acc :: any()), Acc :: any()) -> Acc :: any() | {error, Reason :: any()}.

-callback msg_store_delete_sync(MsgRef :: msg_ref()) -> ok | {error, Reason :: any()}.
-callback msg_store_delete_async(MsgRef :: msg_ref()) -> ok.

