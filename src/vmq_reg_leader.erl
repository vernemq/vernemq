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

-behaviour(gen_server).

%% API
-export([start_link/0,
         register_subscriber/3,
         register_subscriber_by_leader/4]).

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

register_subscriber(SessionPid, SubscriberId, QueueOpts) ->
    case vmq_cluster:is_ready() of
        true ->
            case vmq_cluster:nodes() of
                [_] -> % single node system
                    vmq_reg:register_subscriber_(SessionPid, SubscriberId, QueueOpts);
                Nodes ->
                    I = erlang:phash2(SubscriberId) rem length(Nodes) + 1,
                    Leader = lists:nth(I, lists:sort(Nodes)),
                    rpc:call(Leader, ?MODULE, register_subscriber_by_leader,
                             [node(), SessionPid, SubscriberId, QueueOpts], 5000),
                    case vmq_reg:get_queue_pid(SubscriberId) of
                        not_found ->
                            exit({cant_register_subscriber_by_leader, queue_not_found});
                        QPid ->
                            {ok, QPid}
                    end
            end;
        false ->
            {error, not_ready}
    end.

register_subscriber_by_leader(Node, SessionPid, SubscriberId, QueueOpts) ->
    case vmq_cluster:is_ready() of
        true ->
            gen_server:call(?MODULE, {register_subscriber, Node, SessionPid,
                                      SubscriberId, QueueOpts},
                            infinity);
        false ->
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

handle_info({'DOWN', MRef, process, Pid, _Reason},
            #state{monitors=M} = State) ->
    {ok, {SubscriberId, Pid, From}} = dict:find(MRef, M),
    gen_server:reply(From, ok),
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
            register_subscriber_remote(Node, SubscriberId,
                                       SessionPid,
                                       QueueOpts),
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
                    register_subscriber_remote(Node, SubscriberId,
                                                SessionPid,
                                                QueueOpts),
                    {dict:store(SubscriberId, NewQ, R),
                     dict:store(MRef, {SubscriberId, Pid, From}, M)};
                {empty, Q} ->
                    {dict:erase(SubscriberId, R), M}
            end;
        error ->
            {R, M}
    end,
    State#state{req_queue=NewR, monitors=NewM}.

register_subscriber_remote(Node, SubscriberId, SessionPid, QueueOpts) ->
    spawn_monitor(
      fun() ->
              rpc:call(Node, vmq_reg, register_subscriber_,
                       [SessionPid, SubscriberId, QueueOpts])
      end).
