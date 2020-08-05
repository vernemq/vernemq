%% Copyright 2019 Octavo Labs AG Zurich Switzerland (https://octavolabs.com)
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

%% @doc module wrapping the cowboy_websocket module so we can add the
%% raw socket to the websocket handler state. This works together with
%% the stream handler vmq_cowboy_websocket_h which ensures that the
%% takeover/7 function is called when switching protocols.
-module(vmq_cowboy_websocket).
-behaviour(cowboy_sub_protocol).

-export([is_upgrade_request/1]).
-export([upgrade/4]).
-export([upgrade/5]).
-export([takeover/7]).
-export([loop/3]).

-export([system_continue/3]).
-export([system_terminate/4]).
-export([system_code_change/4]).

-spec is_upgrade_request(cowboy_req:req()) -> boolean().
is_upgrade_request(Req) ->
    cowboy_websocket:is_upgrade_request(Req).

-spec upgrade(Req, Env, module(), any())
	-> {ok, Req, Env}
	when Req::cowboy_req:req(), Env::cowboy_middleware:env().
upgrade(Req, Env, Handler, HandlerState) ->
    cowboy_websocket:upgrade(Req, Env, Handler, HandlerState).

-spec upgrade(Req, Env, module(), any(), any())
	-> {ok, Req, Env}
	when Req::cowboy_req:req(), Env::cowboy_middleware:env().
upgrade(Req, Env, Handler, HandlerState, Opts) ->
    cowboy_websocket:upgrade(Req, Env, Handler, HandlerState, Opts).

-spec takeover(pid(), ranch:ref(), inet:socket() | {pid(), cowboy_stream:streamid()},
	module() | undefined, any(), binary(),
	any()) -> no_return().
takeover(Parent, Ref, Socket, Transport, Opts, Buffer, {State, HandlerState0}) ->
    HandlerState1 = vmq_websocket:add_socket(Socket, HandlerState0),
    cowboy_websocket:takeover(Parent, Ref, Socket, Transport, Opts, Buffer, {State, HandlerState1}).

-spec loop(any(), any(), any()) -> no_return().
loop(State, HandlerState, ParseState) ->
    cowboy_websocket:loop(State, HandlerState, ParseState).

-spec system_continue(_, _, any()) -> no_return().
system_continue(Parent, Debug, Misc) ->
    cowboy_websocket:system_continue(Parent, Debug, Misc).

-spec system_terminate(any(), _, _, any()) -> no_return().
system_terminate(Reason, Parent, Debug, Misc) ->
    cowboy_websocket:system_terminate(Reason, Parent, Debug, Misc).

-spec system_code_change(_, _, _, _)
	-> {ok, any()}.
system_code_change(Misc, Module, OldVsn, Extra) ->
    cowboy_websocket:system_code_change(Misc, Module, OldVsn, Extra).
