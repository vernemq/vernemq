-module(vmq_mqtt5_demo_plugin).

-include_lib("vernemq_dev/include/vernemq_dev.hrl").

-behaviour(auth_on_register_m5_hook).
-behaviour(on_register_m5_hook).
-behaviour(auth_on_publish_m5_hook).
-behaviour(on_publish_m5_hook).
-behaviour(auth_on_subscribe_m5_hook).
-behaviour(on_subscribe_m5_hook).
-behaviour(on_unsubscribe_m5_hook).
-behaviour(on_auth_m5_hook).
-behaviour(on_deliver_m5_hook).

%% API exports
-export([
    auth_on_register_m5/6,
    on_register_m5/4,
    auth_on_publish_m5/7,
    on_publish_m5/7,
    auth_on_subscribe_m5/4,
    on_subscribe_m5/4,
    on_unsubscribe_m5/4,
    on_auth_m5/3,
    on_deliver_m5/7
]).

%%====================================================================
%% API functions
%%====================================================================

-define(LOG(Args0), begin
    [Hook | Args1] = Args0,
    Str = atom_to_list(Hook) ++ "(" ++ string:join(["~p" || _ <- Args1], " ") ++ ")~n",
    lager:info(Str, Args1)
end).

%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%% Register hooks %%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%
auth_on_register_m5(Peer, SubscriberId, Username, Password, CleanStart, Properties) ->
    ?LOG([auth_on_register_m5, Peer, SubscriberId, Username, Password, CleanStart, Properties]),
    auth_on_register_m5_(Peer, SubscriberId, Username, Password, CleanStart, Properties).

auth_on_register_m5_(
    _Peer, _SubscriberId, <<"quota_exceeded">>, _Password, _CleanStart, _Properties
) ->
    {error, #{
        reason_code => ?QUOTA_EXCEEDED,
        properties => #{?P_REASON_STRING => <<"You have exceeded your quota">>}
    }};
auth_on_register_m5_(
    _Peer, _SubscriberId, <<"use_another_server">>, _Password, _CleanStart, _Properties
) ->
    {error, #{
        reason_code => ?USE_ANOTHER_SERVER,
        properties => #{?P_SERVER_REF => <<"server_ref">>}
    }};
auth_on_register_m5_(_Peer, _SubscriberId, <<"server_moved">>, _Password, _CleanStart, _Properties) ->
    {error, #{
        reason_code => ?SERVER_MOVED,
        properties => #{?P_SERVER_REF => <<"server_ref">>}
    }};
auth_on_register_m5_(
    _Peer, _SubscriberId, <<"broker_capabilities">>, _Password, _CleanStart, _Properties
) ->
    Props =
        #{
            ?P_MAX_QOS => 0,
            ?P_RETAIN_AVAILABLE => false,
            ?P_WILDCARD_SUBS_AVAILABLE => false,
            ?P_SUB_IDS_AVAILABLE => false,
            ?P_SHARED_SUBS_AVAILABLE => false
            %% TODO: verify if the properties below can be
            %% controlled from plugins.
            %%
            %%topic_alias_max => 100,
            %%receive_max => 100,
            %%server_keep_alive => 4000,
            %%session_expiry_interval => 3600
        },
    {ok, #{
        reason_code => ?SUCCESS,
        %% TODO: See vmq_mqtt5_fsm:auth_on_register/4
        %%max_packet_size => 1024
        properties => Props
    }};
auth_on_register_m5_(_Peer, _SubscriberId, _Username, _Password, _CleanStart, _Properties) ->
    ok.

on_register_m5(Peer, SubscriberId, Username, Properties) ->
    ?LOG([on_register_m5, Peer, SubscriberId, Username, Properties]),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%% Publish hooks %%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%
auth_on_publish_m5(Username, SubscriberId, QoS, Topic, Payload, IsRetain, Properties) ->
    ?LOG([auth_on_publish_m5, Username, SubscriberId, QoS, Topic, Payload, IsRetain, Properties]),
    auth_on_publish_m5_(Username, SubscriberId, QoS, Topic, Payload, IsRetain, Properties).

auth_on_publish_m5_(
    <<"remove_props">>,
    _SubscriberId,
    _QoS,
    _Topic,
    _Payload,
    _IsRetain,
    #{
        ?P_USER_PROPERTY := _,
        ?P_CORRELATION_DATA := _,
        ?P_RESPONSE_TOPIC := _
    }
) ->
    NewProps = #{},
    {ok, #{properties => NewProps}};
auth_on_publish_m5_(
    <<"modify_props">>,
    _SubscriberId,
    _QoS,
    _Topic,
    _Payload,
    _IsRetain,
    #{
        ?P_USER_PROPERTY := UserProperties,
        ?P_CORRELATION_DATA := CorrelationData,
        ?P_RESPONSE_TOPIC := <<"response/topic">> = RT
    }
) ->
    NewProps = #{
        ?P_USER_PROPERTY => [
            {<<"added">>, <<"user_property">>}
            | UserProperties
        ],
        ?P_CORRELATION_DATA => <<"modified_", CorrelationData/binary>>,
        ?P_RESPONSE_TOPIC => <<"modified_", RT/binary>>
    },
    {ok, #{properties => NewProps}};
auth_on_publish_m5_(
    _Username, _SubscriberId, _QoS, [<<"invalid">>, <<"topic">>], _Payload, _IsRetain, _Properties
) ->
    {error, #{
        reason_code => ?TOPIC_NAME_INVALID,
        reason_string => <<"Invalid topic name">>
    }};
auth_on_publish_m5_(_Username, _SubscriberId, _QoS, _Topic, _Payload, _IsRetain, _Properties) ->
    ok.

on_publish_m5(Username, SubscriberId, QoS, Topic, Payload, IsRetain, Properties) ->
    ?LOG([on_publish_m5, Username, SubscriberId, QoS, Topic, Payload, IsRetain, Properties]),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%% Subscribe hooks %%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%
auth_on_subscribe_m5(Username, SubscriberId, Topics, Properties) ->
    ?LOG([auth_on_subscribe_m5, Username, SubscriberId, Topics, Properties]),
    auth_on_subscribe_m5_(Username, SubscriberId, Topics, Properties).

auth_on_subscribe_m5_(
    _Username,
    _SubscriberId,
    [{[<<"suback">>, <<"withprops">>], _}] = Topics,
    _Properties
) ->
    Props = #{
        ?P_USER_PROPERTY => [{<<"key">>, <<"val">>}],
        ?P_REASON_STRING => <<"successful subscribe">>
    },
    {ok, #{
        topics => Topics,
        properties => Props
    }};
auth_on_subscribe_m5_(_Username, _SubscriberId, _Topics, _Properties) ->
    ok.

on_subscribe_m5(Username, SubscriberId, Topics, Props) ->
    ?LOG([on_subscribe_m5, Username, SubscriberId, Topics, Props]),
    ok.

on_unsubscribe_m5(Username, SubscriberId, Topics, Properties) ->
    ?LOG([on_unsubscribe_m5, Username, SubscriberId, Topics, Properties]),
    on_unsubscribe_m5_(Username, SubscriberId, Topics, Properties).

on_unsubscribe_m5_(
    _Username,
    _SubscriberId,
    [[<<"unsuback">>, <<"withprops">>]] = Topics,
    _Properties
) ->
    {ok, #{
        topics => Topics,
        properties =>
            #{
                ?P_USER_PROPERTY => [{<<"key">>, <<"val">>}],
                ?P_REASON_STRING => <<"Unsubscribe worked">>
            }
    }};
on_unsubscribe_m5_(_Username, _SubscriberId, _Topics, _Properties) ->
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%% Enh. Auth hooks %%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%
on_auth_m5(Username, SubscriberId, Properties) ->
    ?LOG([on_auth_m5, Username, SubscriberId, Properties]),
    on_auth_m5_(Username, SubscriberId, Properties).

on_auth_m5_(
    _Username,
    _SubscriberId,
    #{
        ?P_AUTHENTICATION_METHOD := <<"method1">>,
        ?P_AUTHENTICATION_DATA := <<"client1">>
    }
) ->
    Props = #{
        ?P_AUTHENTICATION_METHOD => <<"method1">>,
        ?P_AUTHENTICATION_DATA => <<"server1">>
    },
    {ok, #{
        reason_code => ?CONTINUE_AUTHENTICATION,
        properties => Props
    }};
on_auth_m5_(
    _Username,
    _SubscriberId,
    #{
        ?P_AUTHENTICATION_METHOD := <<"method1">>,
        ?P_AUTHENTICATION_DATA := <<"client2">>
    }
) ->
    Props = #{
        ?P_AUTHENTICATION_METHOD => <<"method1">>,
        ?P_AUTHENTICATION_DATA => <<"server2">>
    },
    {ok, #{
        reason_code => ?SUCCESS,
        properties => Props
    }};
on_auth_m5_(
    _Username,
    _SubscriberId,
    #{
        ?P_AUTHENTICATION_METHOD := <<"method1">>,
        ?P_AUTHENTICATION_DATA := <<"baddata">>
    }
) ->
    %% any other auth method we just reject.
    {error, #{
        reason_code => ?NOT_AUTHORIZED,
        properties => #{?P_REASON_STRING => <<"Bad authentication data: baddata">>}
    }};
on_auth_m5_(_Username, _SubscriberId, _Props) ->
    {error, unexpected_authentication_attempt}.

%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%% Delivery hooks %%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%
on_deliver_m5(Username, SubscriberId, QoS, Topic, Payload, IsRetain, Properties) ->
    ?LOG([on_deliver_m5, Username, SubscriberId, QoS, Topic, Payload, IsRetain, Properties]),
    on_deliver_m5_(Username, SubscriberId, QoS, Topic, Payload, IsRetain, Properties).

on_deliver_m5_(
    <<"remove_props_on_deliver_m5">>,
    _SubscriberId,
    _QoS,
    _Topic,
    _Payload,
    _IsRetain,
    #{
        ?P_PAYLOAD_FORMAT_INDICATOR := _,
        ?P_CONTENT_TYPE := _,
        ?P_USER_PROPERTY := _,
        ?P_CORRELATION_DATA := _,
        ?P_RESPONSE_TOPIC := _
    }
) ->
    NewProps = #{},
    {ok, #{properties => NewProps}};
on_deliver_m5_(
    <<"modify_props_on_deliver_m5">>,
    _SubscriberId,
    _QoS,
    _Topic,
    _Payload,
    _IsRetain,
    #{
        ?P_PAYLOAD_FORMAT_INDICATOR := unspecified,
        ?P_CONTENT_TYPE := <<"type1">>,
        ?P_USER_PROPERTY := UserProperties,
        ?P_CORRELATION_DATA := CorrelationData,
        ?P_RESPONSE_TOPIC := <<"response/topic">> = RT
    }
) ->
    NewProps = #{
        ?P_PAYLOAD_FORMAT_INDICATOR => utf8,
        ?P_CONTENT_TYPE => <<"type2">>,
        ?P_USER_PROPERTY => [
            {<<"added">>, <<"user_property">>}
            | UserProperties
        ],
        ?P_CORRELATION_DATA => <<"modified_", CorrelationData/binary>>,
        ?P_RESPONSE_TOPIC => <<"modified_", RT/binary>>
    },
    {ok, #{properties => NewProps}};
on_deliver_m5_(_Username, _SubscriberId, _QoS, _Topic, _Payload, _IsRetain, _Properties) ->
    ok.
