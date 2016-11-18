-module(on_client_wakeup_hook).
-include("vmq_types.hrl").

%% called as an 'all'-hook, return value is ignored
-callback on_client_wakeup(SubscriberId  :: subscriber_id()) -> any().


