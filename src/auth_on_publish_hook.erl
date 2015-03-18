-module(auth_on_publish_hook).
-include("vmq_types.hrl").
-type msg_modifier() :: {topic, topic()}
                      | {payload, payload()}
                      | {reg_view, reg_view()}
                      | {qos, qos()}
                      | {retain, flag()}
                      | {mountpoint, mountpoint()}.

-callback auth_on_publish(UserName      :: username(),
                          SubscriberId  :: subscriber_id(),
                          QoS           :: qos(),
                          Topic         :: topic(),
                          Payload       :: payload(),
                          IsRetain      :: flag()) -> ok
                                                      | {ok, Payload    :: payload()}
                                                      | {ok, Modifiers  :: [msg_modifier()]}
                                                      | {error, Reason  :: any()}
                                                      | next.
