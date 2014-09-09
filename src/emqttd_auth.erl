-module(emqttd_auth).
-export([register_hooks/0]).
-export([auth_on_register/4,
         auth_on_subscribe/3,
         auth_on_publish/6]).


register_hooks() ->
%%  -register_hook({auth_on_register, {?MODULE, auth_on_register, 4}}).
%%  -register_hook({auth_on_subscribe, {?MODULE, auth_on_subscribe, 3}}).
%%  -register_hook({auth_on_publish, {?MODULE, auth_on_publish, 6}}).
    emqttd_hook:add(auth_on_register, 100, {?MODULE, auth_on_register, 4}),
    emqttd_hook:add(auth_on_subscribe, 101, {?MODULE, auth_on_subscribe, 3}),
    emqttd_hook:add(auth_on_publish, 102, {?MODULE, auth_on_publish, 6}).


auth_on_register(SrcIp, ClientId, User, Password) ->
    io:format("[~p] auth client ~p from ~p with username ~p and password ~p~n", [self(), ClientId, SrcIp, User, Password]),
    %% return {error, not_authorized} --> error msg is sent to client
    %% return {error, invalid_credentials} --> error msg is sent to client
    %% return next --> next auth handler is tried
    ok.

auth_on_subscribe(User, ClientId, Topics) ->
    io:format("[~p] auth client subscriptions ~p from ~p with username ~p~n", [self(), Topics, ClientId, User]),
    ok.

auth_on_publish(User, ClientId, MsgRef, Topic, _Payload, _IsRetain) ->
   io:format("[~p] auth client publish ~p with topic ~p from ~p with username ~p~n", [self(), MsgRef, Topic, ClientId, User]),
  ok.
