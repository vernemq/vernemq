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

-module(vmq_tcp_listener_sup_sup).

-behaviour(supervisor).

%% API
-export([start_link/2,
         status/1,
         setopts/5,
         accept/1,
         stop_listener/2,
         restart_listener/1]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================
start_link(Addr, Port) ->
    supervisor:start_link(?MODULE, [Addr, Port]).

status(SupPid) ->
    case listener(SupPid) of
        {ok, _} -> running;
        {error, Reason} -> Reason
    end.



setopts(SupPid, Type, MaxConns, MountPoint, TransportOpts) ->
    case listener(SupPid) of
        {ok, ListenerPid} ->
            vmq_tcp_listener:setopts(ListenerPid, Type, MaxConns,
                                     MountPoint, TransportOpts);
        {error, Reason} ->
            {error, Reason}
    end.

accept(SupPid) ->
    case listener(SupPid) of
        {ok, ListenerPid} ->
            {_, ConSupPid, supervisor, _} =
            lists:keyfind(vmq_tcp_transport_sup, 1,
                          supervisor:which_children(SupPid)),
            vmq_tcp_listener:accept(ListenerPid, ConSupPid);
        E -> E
    end.

stop_listener(SupPid, KillSessions) ->
    case supervisor:terminate_child(SupPid, vmq_tcp_listener) of
        ok when KillSessions ->
            supervisor:terminate_child(SupPid, vmq_tcp_transport_sup);
        ok ->
            ok;
        E ->
            E
    end.


restart_listener(SupPid) ->
    case {supervisor:restart_child(SupPid, vmq_tcp_listener),
          supervisor:restart_child(SupPid, vmq_tcp_transport_sup)} of
        {{ok, _}, {ok, _}} ->
            ok;
        {{ok, _}, {error, running}} ->
            ok;
        {{error, R1}, {error, R2}} ->
            {error, {R1, R2}}
    end.

listener(SupPid) ->
    case lists:keyfind(vmq_tcp_listener, 1, supervisor:which_children(SupPid)) of
        false ->
            {error, not_found};
        {_, restarting, worker, _} ->
            {error, restarting};
        {_, undefined, worker, _} ->
            {error, stopped};
        {_, ListenerPid, worker, _} when is_pid(ListenerPid) ->
            {ok, ListenerPid}
    end.

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
init([Addr, Port]) ->
    {ok, {{one_for_one, 5, 10},
          [
           {vmq_tcp_listener,
            {vmq_tcp_listener, start_link, [Addr, Port]},
            permanent, 5000, worker, [vmq_tcp_listener]},
           {vmq_tcp_transport_sup,
            {vmq_tcp_transport_sup, start_link, []},
            permanent, 5000, supervisor, [vmq_tcp_transport_sup]}
          ]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

