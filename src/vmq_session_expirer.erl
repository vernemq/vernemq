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

-module(vmq_session_expirer).

-behaviour(gen_server).

%% API
-export([start_link/0,
         change_duration/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {duration=0}).
-type state() :: #state{}.
%%%===================================================================
%%% API
%%%===================================================================
-spec start_link() -> {ok, pid()} | ignore | {error, atom()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

change_duration(NewDuration) ->
    gen_server:cast(?MODULE, {new_duration, NewDuration}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
-spec init([any()]) -> {ok, state()}.
init([]) ->
    Duration = vmq_config:get_env(persistent_client_expiration, 0),
    erlang:send_after(5000, self(), expire_clients),
    {ok, #state{duration=Duration}}.

-spec handle_call(_, _, state()) -> {reply, ok, state()}.
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

-spec handle_cast(_, state()) -> {noreply, state()}.
handle_cast({new_duration, Duration}, State) ->
    {noreply, State#state{duration=Duration}}.

-spec handle_info(_, state()) -> {noreply, state()}.
handle_info(expire_clients, State) ->
    case State#state.duration of
        0 ->
            ok;
        Duration ->
            vmq_reg:remove_expired_clients(Duration)
    end,
    erlang:send_after(5000, self(), expire_clients),
    {noreply, State}.

-spec terminate(_, state()) -> ok.
terminate(_Reason, _State) ->
    ok.

-spec code_change(_, state(), _) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
