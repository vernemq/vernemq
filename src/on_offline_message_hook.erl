-module(on_offline_message_hook).
-include("vmq_types.hrl").

%% called as an 'all'-hook, return value is ignored
-callback on_offline_message(SubscriberId  :: subscriber_id()) -> any().
