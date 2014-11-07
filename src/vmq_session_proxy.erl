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

-module(vmq_session_proxy).

-behaviour(gen_server).

%% API
-export([start_link/3,
         derefed/2,
         deliver/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {node,
                repl_pid,
                repl_mon,
                queue_pid,
                queue_mon,
                client_id,
                waiting}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Node, QPid, ClientId) ->
    gen_server:start_link(?MODULE, [Node, QPid, ClientId], []).

derefed(SessionProxy, MsgRef) ->
    gen_server:cast(SessionProxy, {derefed, MsgRef}).

deliver(SessionProxy, Term) ->
    gen_server:call(SessionProxy, {deliver, Term}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Node, QPid, ClientId]) ->
    QueueMon = monitor(process, QPid),
    {ok, #state{node=Node, client_id=ClientId, queue_pid=QPid,
                queue_mon=QueueMon}, 0}.

handle_call({deliver, Term}, From, State) ->
    {noreply, deliver(From, Term, State)}.

handle_cast({derefed, MsgRef}, #state{waiting={MsgRef, From}} = State) ->
    gen_server:reply(From, ok),
    {noreply, State#state{waiting=undefined}}.

handle_info({'DOWN', _MRef, process, Pid, Reason}, State) ->
    #state{repl_pid=ReplPid, client_id=ClientId} = State,
    case {Pid == ReplPid, Reason} of
        {true, normal} ->
            %% finished replicating
            {stop, normal, State};
        {true, OtherReason} ->
            lager:warning("replication process for client ~p died due to ~p",
                         [ClientId, OtherReason]),
            {stop, OtherReason, State};
        {false, Reason} ->
            %% session stopped during replication
            {stop, Reason, State}
    end;

handle_info(timeout, #state{node=Node, client_id=ClientId}= State) ->
    Self = self(),
    {ReplPid, ReplMon} =
    case node() of
        Node ->
            spawn_monitor(vmq_msg_store, deliver_from_store,
                          [ClientId, Self]);
        _ ->
            spawn_monitor(
              fun() ->
                      rpc:call(Node, vmq_msg_store, deliver_from_store,
                               [ClientId, Self])
              end)
    end,
    {noreply, State#state{repl_pid=ReplPid, repl_mon=ReplMon}}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
deliver(From, Term, State) ->
    #state{queue_pid=QPid, node=Node} = State,
    case Term of
        {RoutingKey, Payload, QoS, Dup, MsgRef} ->
            vmq_queue:post(QPid, {deliver, {RoutingKey, Payload,
                                QoS, false, Dup, {{self(), Node}, MsgRef}}}),
            State#state{waiting={MsgRef, From}};
        _ ->
            vmq_queue:post(QPid, {deliver_bin, Term}),
            gen_server:reply(From, ok),
            State
    end.
