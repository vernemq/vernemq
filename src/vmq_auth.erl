%% Copyright 2014 Erlio GmbH Basel Switzerland (http://erl.io)
%% 
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% 
%%     http://www.apache.org/licenses/LICENSE-2.0
%% 
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(vmq_auth).
-export([register_hooks/0]).
-export([auth_on_register/4,
         auth_on_subscribe/3,
         auth_on_publish/6]).


-spec register_hooks() -> 'ok'.
register_hooks() ->
%%  -register_hook({auth_on_register, {?MODULE, auth_on_register, 4}}).
%%  -register_hook({auth_on_subscribe, {?MODULE, auth_on_subscribe, 3}}).
%%  -register_hook({auth_on_publish, {?MODULE, auth_on_publish, 6}}).
    vmq_hook:add(auth_on_register, {?MODULE, auth_on_register, 4}),
    vmq_hook:add(auth_on_subscribe, {?MODULE, auth_on_subscribe, 3}),
    vmq_hook:add(auth_on_publish, {?MODULE, auth_on_publish, 6}).


-spec auth_on_register(_,_,_,_) -> 'ok'.
auth_on_register(SrcIp, ClientId, User, Password) ->
    io:format("[~p] auth client ~p from ~p
              with username ~p and password ~p~n",
              [self(), ClientId, SrcIp, User, Password]),
    %% return {error, not_authorized} --> error msg is sent to client
    %% return {error, invalid_credentials} --> error msg is sent to client
    %% return next --> next auth handler is tried
    ok.

-spec auth_on_subscribe(_,_,_) -> 'ok'.
auth_on_subscribe(User, ClientId, Topics) ->
    io:format("[~p] auth client subscriptions ~p
              from ~p with username ~p~n",
              [self(), Topics, ClientId, User]),
    ok.

-spec auth_on_publish(_,_,_,_,_,_) -> 'ok'.
auth_on_publish(User, ClientId, MsgRef, Topic, _Payload, _IsRetain) ->
   io:format("[~p] auth client publish ~p with
             topic ~p from ~p with username ~p~n",
             [self(), MsgRef, Topic, ClientId, User]),
  ok.
