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
         register_client/4,
         register_client_by_leader/5]).

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

register_client(SessionPid, QPid, ClientId, CleanSession) ->
    case vmq_cluster:is_ready() of
        true ->
            case vmq_cluster:nodes() of
                [_] -> % single node system
                    vmq_reg:register_client_(SessionPid, QPid,
                                             ClientId, CleanSession);
                Nodes ->
                    I = erlang:phash2(ClientId) rem length(Nodes) + 1,
                    Leader = lists:nth(I, lists:sort(Nodes)),
                    rpc:call(Leader, ?MODULE, register_client_by_leader,
                             [node(), SessionPid, QPid,
                              ClientId, CleanSession], 5000)
            end;
        false ->
            {error, not_ready}
    end.

register_client_by_leader(Node, SessionPid, QPid, ClientId, CleanSession) ->
    case vmq_cluster:is_ready() of
        true ->
            gen_server:call(?MODULE, {register_client, Node, SessionPid, QPid,
                                      ClientId, CleanSession}, infinity);
        false ->
            {error, not_ready}
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, #state{}}.

handle_call({register_client, Node, SessionPid, QPid,
             ClientId, CleanSession}, From, State) ->
    {noreply, schedule_register(ClientId, {Node, SessionPid, QPid,
                                           CleanSession, From}, State)}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', MRef, process, Pid, _Reason},
            #state{monitors=M} = State) ->
    {ok, {ClientId, Pid, From}} = dict:find(MRef, M),
    gen_server:reply(From, ok),
    {noreply, schedule_next(ClientId,
                            State#state{monitors=dict:erase(MRef, M)})}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
schedule_register(ClientId, {Node, SessionPid, QPid,
                             CleanSession, From} = Item,
                  #state{req_queue=R, monitors=M} = State) ->
    {NewR, NewM} =
    case dict:find(ClientId, R) of
        {ok, Q} ->
            {dict:store(ClientId, queue:in(Item, Q), R), M};
        error ->
            %% no waiting items
            {Pid, MRef} =
                spawn_monitor(
                  fun() ->
                          register_client_remote(Node, ClientId, SessionPid,
                                                 QPid, CleanSession)
                  end),
            {dict:store(ClientId, queue:new(), R),
             dict:store(MRef, {ClientId, Pid, From}, M)}
    end,
    State#state{req_queue=NewR, monitors=NewM}.

schedule_next(ClientId, #state{req_queue=R, monitors=M} = State) ->
    {NewR, NewM} =
    case dict:find(ClientId, R) of
        {ok, Q} ->
            case queue:out(Q) of
                {{value, {Node, SessionPid, QPid, CleanSession, From}}, NewQ} ->
                    {Pid, MRef} =
                        register_client_remote_(Node,
                                                ClientId,
                                                SessionPid,
                                                QPid,
                                                CleanSession),
                    {dict:store(ClientId, NewQ, R),
                     dict:store(MRef, {ClientId, Pid, From}, M)};
                {empty, Q} ->
                    {dict:erase(ClientId, R), M}
            end;
        error ->
            {R, M}
    end,
    State#state{req_queue=NewR, monitors=NewM}.

register_client_remote(Node, ClientId, SessionPid, QPid, CleanSession) ->
    rpc:call(Node, vmq_reg, register_client_,
             [SessionPid, QPid, ClientId, CleanSession]).

register_client_remote_(Node, ClientId, SessionPid, QPid, CleanSession) ->
    spawn_monitor(
      fun() ->
              register_client_remote(Node, ClientId, SessionPid,
                                     QPid, CleanSession)
      end).
