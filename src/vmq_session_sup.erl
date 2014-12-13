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

-module(vmq_session_sup).
-behaviour(supervisor).

%% API
-export([start_link/0,
         start_session/3,
         reconfigure_sessions/1,
         stop_session/1,
         active_clients/0]).

%% Supervisor callbacks
-export([init/1]).

-define(CHILD(Id, Mod, Type, Args), {Id, {Mod, start_link, Args},
                                     permanent, 5000, Type, [Mod]}).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_session(Socket, Handler, Opts) ->
    Transport = t(Handler),
    {ok, Peer} = (i(Transport)):peername(Socket),
    {ok, TransportPid} = supervisor:start_child(?MODULE, [Peer, Handler,
                                                         Transport, Opts]),
    apply(Transport, controlling_process, [Socket, TransportPid]),
    vmq_tcp_transport:handover(TransportPid, Socket),
    {ok, TransportPid}.

stop_session(TransportPid) ->
    supervisor:terminate_child(?MODULE, TransportPid).

active_clients() ->
    Counts = supervisor:count_children(?MODULE),
    {_, N} = lists:keyfind(active, 1, Counts),
    N.

reconfigure_sessions(NewConfig) ->
    reconfigure_sessions(NewConfig, supervisor:which_children(?MODULE)).

reconfigure_sessions(NewConfig, [{_, Pid, _, _}|Rest]) when is_pid(Pid) ->
    MRef = monitor(process, Pid),
    Ref = make_ref(),
    Caller = {self(), Ref},
    Pid ! {reconfigure_session, Caller, NewConfig},
    receive
        {Ref, ok} ->
            demonitor(MRef),
            reconfigure_sessions(NewConfig, Rest);
        {'DOWN', MRef, process, Pid, normal} ->
            reconfigure_sessions(NewConfig, Rest);
        {'DOWN', MRef, process, Pid, Reason} ->
            {error, Reason}
    end;
reconfigure_sessions(NewConfig, [_|Rest]) ->
    reconfigure_sessions(NewConfig, Rest);
reconfigure_sessions(_, []) -> ok.


%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @spec init(Args) -> {ok, {SupFlags, [ChildSpec]}} |
%%                     ignore |
%%                     {error, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    {ok, {{simple_one_for_one, 5, 10},
          [{vmq_tcp_transport,
            {vmq_tcp_transport, start_link, []},
            temporary, 5000, worker, [vmq_tcp_transport]}]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
t(vmq_tcp_listener) -> gen_tcp;
t(vmq_ssl_listener) -> ssl;
t(vmq_ws_listener) -> gen_tcp;
t(vmq_wss_listener) -> ssl.
i(gen_tcp) -> inet;
i(ssl) -> ssl.
