%% Copyright 2018 Erlio GmbH Basel Switzerland (http://erl.io)
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
-behaviour(auth_on_register_hook).
-behaviour(auth_on_publish_hook).
-behaviour(auth_on_subscribe_hook).

-export([register_hooks/0]).
-export([auth_on_register/5,
         auth_on_subscribe/3,
         auth_on_publish/6]).


-spec register_hooks() -> 'ok'.
register_hooks() ->
    ok = vmq_plugin_mgr:enable_module_plugin(
           ?MODULE, auth_on_register, 5),
    ok = vmq_plugin_mgr:enable_module_plugin(
           ?MODULE, auth_on_subscribe, 3),
    ok = vmq_plugin_mgr:enable_module_plugin(
           ?MODULE, auth_on_publish, 6).


-spec auth_on_register(_, _, _, _, _) -> 'ok'.
auth_on_register(SrcIp, SubscriberId, User, Password, CleanSession) ->
    lager:info("auth subscriber ~p from ~p
              with username ~p and password ~p, cleansession: ~p",
              [SubscriberId, SrcIp, User, Password, CleanSession]),
    ok.

-spec auth_on_subscribe(_, _, _) -> 'ok'.
auth_on_subscribe(User, SubscriberId, Topics) ->
    lager:info("auth subscriber subscriptions ~p
              from ~p with username ~p",
              [Topics, SubscriberId, User]),
    ok.

-spec auth_on_publish(_, _, _, _, _, _) -> 'ok'.
auth_on_publish(User, SubscriberId, MsgRef, Topic, _Payload, _IsRetain) ->
   lager:debug("auth subscriber publish ~p with
             topic ~p from ~p with username ~p",
             [MsgRef, Topic, SubscriberId, User]),
  ok.
