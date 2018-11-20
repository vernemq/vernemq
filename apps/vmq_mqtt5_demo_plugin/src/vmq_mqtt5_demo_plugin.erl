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
         on_deliver_m5/5]).

%%====================================================================
%% API functions
%%====================================================================

-define(LOG(Args0),
        begin
            [Hook|Args1] = Args0,
            Str = atom_to_list(Hook) ++ "(" ++ string:join(["~p" || _ <- Args1], " ") ++ ")~n",
            lager:info(Str, Args1)
        end).

%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%% Register hooks %%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%
auth_on_register_m5(Peer,SubscriberId,Username,Password,CleanStart,Properties) ->
    ?LOG([auth_on_register_m5,Peer,SubscriberId,Username,Password,CleanStart,Properties]),
    auth_on_register_m5_(Peer,SubscriberId,Username,Password,CleanStart,Properties).

auth_on_register_m5_(_Peer,_SubscriberId,<<"quota_exceeded">>,_Password,_CleanStart,_Properties) ->
    {error, #{reason_code => ?QUOTA_EXCEEDED,
              reason_string => <<"You have exceeded your quota">>}};
auth_on_register_m5_(_Peer,_SubscriberId,<<"use_another_server">>,_Password,_CleanStart,_Properties) ->
    {error, #{reason_code => ?USE_ANOTHER_SERVER,
              server_ref => <<"server_ref">>}};
auth_on_register_m5_(_Peer,_SubscriberId,<<"server_moved">>,_Password,_CleanStart,_Properties) ->
    {error, #{reason_code => ?SERVER_MOVED,
              server_ref => <<"server_ref">>}};
auth_on_register_m5_(_Peer,_SubscriberId,<<"broker_capabilities">>,_Password,_CleanStart,_Properties) ->
    {ok, #{reason_code => ?SUCCESS,
           max_qos => 0,
           retain_available => false,
           wildcard_subscriptions_available => false,
           subscription_identifiers_available => false,
           shared_subscriptions_available => false
           %% TODO: See vmq_mqtt5_fsm:auth_on_register/4
           %%max_packet_size => 1024

           %% TODO: verify if the properties below can be
           %% controlled from plugins.
           %%
           %%topic_alias_max => 100,
           %%receive_max => 100,
           %%server_keep_alive => 4000,
           %%session_expiry_interval => 3600
          }};
auth_on_register_m5_(_Peer,_SubscriberId,_Username,_Password,_CleanStart,_Properties) ->
    ok.

on_register_m5(Peer,SubscriberId,Username,Properties) ->
    ?LOG([on_register_m5,Peer,SubscriberId,Username,Properties]),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%% Publish hooks %%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%
auth_on_publish_m5(Username,SubscriberId,QoS,Topic,Payload,IsRetain,Properties) ->
    ?LOG([auth_on_publish_m5,Username,SubscriberId,QoS,Topic,Payload,IsRetain,Properties]),
    auth_on_publish_m5_(Username,SubscriberId,QoS,Topic,Payload,IsRetain,Properties).

auth_on_publish_m5_(<<"modify_props">>,_SubscriberId,_QoS,_Topic,_Payload,_IsRetain,#{p_user_property := UserProperties}) ->
    {ok, #{user_property => [{<<"added">>, <<"user_property">>}|
                             UserProperties]}};
auth_on_publish_m5_(_Username,_SubscriberId,_QoS,[<<"invalid">>, <<"topic">>],_Payload,_IsRetain,_Properties) ->
    {error, #{reason_code => ?TOPIC_NAME_INVALID,
              reason_string => <<"Invalid topic name">>}};
auth_on_publish_m5_(_Username,_SubscriberId,_QoS,_Topic,_Payload,_IsRetain,_Properties) ->
    ok.

on_publish_m5(Username,SubscriberId,QoS,Topic,Payload,IsRetain,Properties)->
    ?LOG([on_publish_m5,Username,SubscriberId,QoS,Topic,Payload,IsRetain,Properties]),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%% Subscribe hooks %%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%
auth_on_subscribe_m5(Username,SubscriberId,Topics,Properties) ->
    ?LOG([auth_on_subscribe_m5,Username,SubscriberId,Topics,Properties]),
    auth_on_subscribe_m5_(Username,SubscriberId,Topics,Properties).

auth_on_subscribe_m5_(_Username,_SubscriberId,
                      [{[<<"suback">>,<<"withprops">>], _}] = Topics,
                      _Properties) ->
    {ok, #{topics => Topics,
           user_property => [{<<"key">>, <<"val">>}],
           reason_string => <<"successful subscribe">>}};
auth_on_subscribe_m5_(_Username,_SubscriberId,_Topics,_Properties) ->
    ok.

on_subscribe_m5(Username,SubscriberId,Topics,Props)->
    ?LOG([on_subscribe_m5,Username,SubscriberId,Topics,Props]),
    ok.

on_unsubscribe_m5(Username,SubscriberId,Topics,Properties) ->
    ?LOG([on_unsubscribe_m5,Username,SubscriberId,Topics,Properties]),
    on_unsubscribe_m5_(Username,SubscriberId,Topics,Properties).

on_unsubscribe_m5_(_Username,_SubscriberId,
                   [[<<"unsuback">>,<<"withprops">>]] = Topics,
                   _Properties) ->
    {ok, #{topics => Topics,
           user_property => [{<<"key">>, <<"val">>}],
           reason_string => <<"Unsubscribe worked">>}};
on_unsubscribe_m5_(_Username,_SubscriberId,_Topics,_Properties) ->
    ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%% Enh. Auth hooks %%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%
on_auth_m5(Username, SubscriberId, Properties) ->
    ?LOG([on_auth_m5, Username, SubscriberId, Properties]),
    on_auth_m5_(Username, SubscriberId, Properties).

on_auth_m5_(_Username, _SubscriberId,
            #{?P_AUTHENTICATION_METHOD := <<"method1">>,
              ?P_AUTHENTICATION_DATA := <<"client1">>}) ->
    {ok, #{reason_code => ?CONTINUE_AUTHENTICATION,
           auth_method => <<"method1">>,
           auth_data => <<"server1">>}};
on_auth_m5_(_Username, _SubscriberId,
            #{?P_AUTHENTICATION_METHOD := <<"method1">>,
              ?P_AUTHENTICATION_DATA := <<"client2">>}) ->
    {ok, #{reason_code => ?SUCCESS,
           auth_method => <<"method1">>,
           auth_data => <<"server2">>}};
on_auth_m5_(_Username, _SubscriberId,
            #{?P_AUTHENTICATION_METHOD := <<"method1">>,
              ?P_AUTHENTICATION_DATA := <<"baddata">>}) ->
    %% any other auth method we just reject.
    {error, #{reason_code => ?NOT_AUTHORIZED,
              reason_string => <<"Bad authentication data: baddata">>}};
on_auth_m5_(_Username, _SubscriberId, _Props) ->
    {error, unexpected_authentication_attempt}.



%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%% Delivery hooks %%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%
on_deliver_m5(Username,SubscriberId,Topic,Properties,Payload) ->
    ?LOG([on_deliver_m5,Username,SubscriberId,Topic,Properties,Payload]),
    ok.
