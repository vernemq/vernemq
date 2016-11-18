-module(on_unsubscribe_hook).
-include("vmq_types.hrl").

%% called as an 'all'-hook, return value is ignored
-callback on_unsubscribe(UserName      :: username(),
                         SubscriberId  :: subscriber_id(),
                         Topics        :: [Topic :: topic()]) -> ok
                                                                 | {ok, [Topic :: topic()]}
                                                                 | next.
