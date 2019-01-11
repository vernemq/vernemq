%% Copyright 2018 Octavo Labs AG Basel Switzerland (http://erl.io)
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
-module(vmq_websocket_middleware).
-behavior(cowboy_middleware).

-export([execute/2]).

execute(Req, Env) ->
    %% This middleware allows to access the socket from each request.
    Socket = maps:get(socket, Env),
    {ok, Req#{socket => Socket}, Env}.
