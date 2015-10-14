-module(on_client_gone_hook).
-include("vmq_types.hrl").

%% called as an 'all'-hook, return value is ignored
-callback on_client_gone(SubscriberId  :: subscriber_id()) -> any().

