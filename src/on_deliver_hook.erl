-module(on_deliver_hook).
-include("vmq_types.hrl").
-type msg_modifier() :: {topic, topic()}
                      | {payload, payload()}.

-callback on_deliver(UserName      :: username(),
                     SubscriberId  :: subscriber_id(),
                     Topic         :: topic(),
                     Payload       :: payload()) -> ok
                                                    | {ok, Payload    :: payload()}
                                                    | {ok, Modifiers  :: [msg_modifier()]}
                                                    | next.

-export_type([msg_modifier/0]).
