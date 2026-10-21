%% Copyright 2018-2024 Octavo Labs/VerneMQ (https://vernemq.com/)
%% and Individual Contributors.
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

%% Publishes the replicated wills (vmq_lwt_db) of this node's previous
%% incarnation, `lwt_fire_delay` seconds after it started (default 45 s, the
%% time an MQTT client with a 30 s keepalive gets before the broker publishes
%% its will). A node that crashed and was restarted under the same name looks
%% alive again to its peers, so only the node itself can tell that the
%% sessions it held before are gone. Clients that reconnected in the meantime
%% are skipped.
-module(vmq_lwt_watch).
-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(INTERVAL, 1000).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    erlang:send_after(?INTERVAL, self(), check),
    {ok, #{started => erlang:monotonic_time(millisecond), stale => true}}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(check, State0) ->
    State = maybe_fire_stale(erlang:monotonic_time(millisecond), State0),
    _ =
        case State of
            #{stale := true} -> erlang:send_after(?INTERVAL, self(), check);
            _ -> ok
        end,
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

maybe_fire_stale(Now, #{stale := true, started := Started} = State) ->
    case vmq_config:get_env(replicate_lwt, false) andalso Now - Started >= delay() of
        true ->
            _ = vmq_lwt_db:fire_stale(),
            State#{stale := false};
        false ->
            State
    end;
maybe_fire_stale(_Now, State) ->
    State.

delay() ->
    1000 * vmq_config:get_env(lwt_fire_delay, 45).
