-module(vmq_mqtt5_demo_plugin).

-include_lib("vernemq_dev/include/vernemq_dev.hrl").

-behaviour(auth_on_register_v1_hook).
-behaviour(on_register_v1_hook).
-behaviour(auth_on_publish_v1_hook).
-behaviour(on_publish_v1_hook).
-behaviour(auth_on_subscribe_v1_hook).
-behaviour(on_subscribe_v1_hook).
-behaviour(on_unsubscribe_v1_hook).
-behaviour(on_auth_v1_hook).
-behaviour(on_deliver_v1_hook).

%% API exports
-export([
         auth_on_register_v1/6,
         on_register_v1/4,
         auth_on_publish_v1/7,
         on_publish_v1/7,
         auth_on_subscribe_v1/4,
         on_subscribe_v1/3,
         on_unsubscribe_v1/4,
         on_auth_v1/1,
         on_deliver_v1/5]).

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
auth_on_register_v1(Peer,SubscriberId,Username,Password,CleanStart,Properties) ->
    ?LOG([auth_on_register_v1,Peer,SubscriberId,Username,Password,CleanStart,Properties]),
    auth_on_register_v1_(Peer,SubscriberId,Username,Password,CleanStart,Properties).

auth_on_register_v1_(_Peer,_SubscriberId,<<"quota_exceeded">>,_Password,_CleanStart,_Properties) ->
    {error, #{reason_code => ?QUOTA_EXCEEDED,
              properties => #{?P_REASON_STRING => <<"You have exceeded your quota">>}}};
auth_on_register_v1_(_Peer,_SubscriberId,_Username,_Password,_CleanStart,_Properties) ->
    ok.

on_register_v1(Peer,SubscriberId,Username,Properties) ->
    ?LOG([on_register_v1,Peer,SubscriberId,Username,Properties]),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%% Publish hooks %%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%
auth_on_publish_v1(Username,SubscriberId,QoS,Topic,Payload,IsRetain,Properties) ->
    ?LOG([auth_on_publish_v1,Username,SubscriberId,QoS,Topic,Payload,IsRetain,Properties]),
    auth_on_publish_v1_(Username,SubscriberId,QoS,Topic,Payload,IsRetain,Properties).

auth_on_publish_v1_(_Username,_SubscriberId,_QoS,[<<"invalid">>, <<"topic">>],_Payload,_IsRetain,_Properties) ->
    {error, #{reason_code => ?TOPIC_NAME_INVALID,
              properties => #{?P_REASON_STRING => <<"Invalid topic name">>}}};
auth_on_publish_v1_(_Username,_SubscriberId,_QoS,_Topic,_Payload,_IsRetain,_Properties) ->
    ok.

on_publish_v1(Username,SubscriberId,QoS,Topic,Payload,IsRetain,Properties)->
    ?LOG([on_publish_v1,Username,SubscriberId,QoS,Topic,Payload,IsRetain,Properties]),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%% Subscribe hooks %%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%
auth_on_subscribe_v1(Username,SubscriberId,Topics,Properties) ->
    ?LOG([auth_on_subscribe_v1,Username,SubscriberId,Topics,Properties]),
    auth_on_subscribe_v1_(Username,SubscriberId,Topics,Properties).

auth_on_subscribe_v1_(_Username,_SubscriberId,
                      [{[<<"suback">>,<<"withprops">>], _}] = Topics,
                      _Properties) ->
    {ok, #{topics => Topics,
           properties => #{?P_USER_PROPERTY => [{<<"key">>, <<"val">>}],
                           ?P_REASON_STRING => <<"successful subscribe">>}}};
auth_on_subscribe_v1_(_Username,_SubscriberId,_Topics,_Properties) ->
    ok.

on_subscribe_v1(Username,SubscriberId,Topics)->
    ?LOG([on_subscribe_v1,Username,SubscriberId,Topics]),
    ok.

on_unsubscribe_v1(Username,SubscriberId,Topics,Properties) ->
    ?LOG([on_unsubscribe_v1,Username,SubscriberId,Topics,Properties]),
    on_unsubscribe_v1_(Username,SubscriberId,Topics,Properties).

on_unsubscribe_v1_(_Username,_SubscriberId,
                   [[<<"unsuback">>,<<"withprops">>]] = Topics,
                   _Properties) ->
    {ok, #{topics => Topics,
           properties => #{?P_USER_PROPERTY => [{<<"key">>, <<"val">>}],
                           ?P_REASON_STRING => <<"Unsubscribe worked">>}}};
on_unsubscribe_v1_(_Username,_SubscriberId,_Topics,_Properties) ->
    ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%% Enh. Auth hooks %%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%
on_auth_v1(Properties) ->
    ?LOG([on_auth_v1, Properties]),
    on_auth_v1_(Properties).

on_auth_v1_(#{?P_AUTHENTICATION_METHOD := <<"method1">>,
              ?P_AUTHENTICATION_DATA := <<"client1">>}) ->
    {ok, #{reason_code => ?CONTINUE_AUTHENTICATION,
           properties => #{?P_AUTHENTICATION_METHOD => <<"method1">>,
                           ?P_AUTHENTICATION_DATA => <<"server1">>}}};
on_auth_v1_(#{?P_AUTHENTICATION_METHOD := <<"method1">>,
              ?P_AUTHENTICATION_DATA := <<"client2">>}) ->
    {ok, #{reason_code => ?SUCCESS,
           properties => #{?P_AUTHENTICATION_METHOD => <<"method1">>,
                           ?P_AUTHENTICATION_DATA => <<"server2">>}}};
on_auth_v1_(#{?P_AUTHENTICATION_METHOD := <<"method1">>,
              ?P_AUTHENTICATION_DATA := <<"baddata">>}) ->
    %% any other auth method we just reject.
    {error, #{reason_code => ?NOT_AUTHORIZED,
              properties => #{?P_REASON_STRING => <<"Bad authentication data: baddata">>}}};
on_auth_v1_(_) ->
    {error, unexpected_authentication_attempt}.



%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%% Delivery hooks %%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%
on_deliver_v1(Username,SubscriberId,Topic,Properties,Payload) ->
    ?LOG([on_deliver_v1,Username,SubscriberId,Topic,Properties,Payload]),
    ok.
