-module(auth_on_register_hook).
-include("vmq_types.hrl").

-type reg_modifiers()   :: {mountpoint, mountpoint()}
                         | {regview, reg_view()}
                         | {clean_session, flag()}.

%% called as an all_till_ok hook
-callback auth_on_register(Peer          :: peer(),
                           SubscriberId  :: subscriber_id(),
                           UserName      :: username(),
                           Password      :: password(),
                           CleanSession  :: flag()) -> ok
                                                           | {ok, [reg_modifiers()]}
                                                           | {error, invalid_credentials | any()}
                                                           | next.
