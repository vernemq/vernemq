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
%%
-module(vmq_reg_leader).
-include("vmq_server.hrl").

-behaviour(gen_server).

%% API
-export([start_link/0,
         register_subscriber/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {req_queue=dict:new(), monitors=dict:new()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec register_subscriber(pid(), subscriber_id(), map()) ->
    {ok, boolean(), pid()} | {error, any()}.
register_subscriber(SessionPid, SubscriberId, QueueOpts) ->
    case vmq_cluster:is_ready() of
        true ->
            Nodes = vmq_cluster:nodes(),
            I = erlang:phash2(SubscriberId) rem length(Nodes) + 1,
            Leader = lists:nth(I, lists:sort(Nodes)),
            register_subscriber(Leader, SessionPid, SubscriberId, QueueOpts);
        false ->
            {error, not_ready}
    end.

-spec register_subscriber(node(), pid(), subscriber_id(), map()) ->
    {ok, boolean(), pid()} | {error, any()}.
register_subscriber(Leader, SessionPid, SubscriberId, QueueOpts) ->
    Req = {register_subscriber, node(), SessionPid, SubscriberId, QueueOpts},
    try gen_server:call({?MODULE, Leader}, Req, infinity)
    catch
        _:_ ->
            %% mostly happens in case of a netsplit
            %% this triggers the proper CONNACK leaving the
            %% client to retry the CONNECT
            {error, not_ready}
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, #state{}}.

handle_call({register_subscriber, Node, SessionPid,
             SubscriberId, QueueOpts}, From, State) ->
    {noreply, schedule_register(SubscriberId, {Node, SessionPid,
                                               QueueOpts, From}, State)}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', MRef, process, Pid, _},
            #state{monitors=M} = State) ->
    {ok, {SubscriberId, Pid, _}} = dict:find(MRef, M),
    {noreply, schedule_next(SubscriberId,
                            State#state{monitors=dict:erase(MRef, M)})}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
schedule_register(SubscriberId, {Node, SessionPid,
                                 QueueOpts, From} = Item,
                  #state{req_queue=R, monitors=M} = State) ->
    {NewR, NewM} =
    case dict:find(SubscriberId, R) of
        {ok, Q} ->
            {dict:store(SubscriberId, queue:in(Item, Q), R), M};
        error ->
            %% no waiting items
            {Pid, MRef} =
            register_subscriber_remote(Node, From, SubscriberId,
                                       SessionPid, QueueOpts),
            {dict:store(SubscriberId, queue:new(), R),
             dict:store(MRef, {SubscriberId, Pid, From}, M)}
    end,
    State#state{req_queue=NewR, monitors=NewM}.

schedule_next(SubscriberId, #state{req_queue=R, monitors=M} = State) ->
    {NewR, NewM} =
    case dict:find(SubscriberId, R) of
        {ok, Q} ->
            case queue:out(Q) of
                {{value, {Node, SessionPid, QueueOpts, From}}, NewQ} ->
                    {Pid, MRef} =
                    register_subscriber_remote(Node, From, SubscriberId,
                                                SessionPid, QueueOpts),
                    {dict:store(SubscriberId, NewQ, R),
                     dict:store(MRef, {SubscriberId, Pid, From}, M)};
                {empty, Q} ->
                    {dict:erase(SubscriberId, R), M}
            end;
        error ->
            {R, M}
    end,
    State#state{req_queue=NewR, monitors=NewM}.

register_subscriber_remote(Node, From, SubscriberId, SessionPid, QueueOpts) ->
    spawn_monitor(
      fun() ->
              Req = {finish_register_subscriber_by_leader, SessionPid, SubscriberId, QueueOpts},
              case catch gen_server:call({vmq_reg, Node}, Req, infinity) of
                  {'EXIT', Reason} ->
                      gen_server:reply(From, {error, Reason}),
                      exit(Reason);
                  Ret ->
                      gen_server:reply(From, Ret)
              end
      end).
