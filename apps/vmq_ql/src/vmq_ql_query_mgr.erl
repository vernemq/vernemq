%% Copyright 2018 Erlio GmbH Basel Switzerland (http://erl.io)
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

-module(vmq_ql_query_mgr).

-behaviour(gen_server).

%% API
-export([start_link/1,
         start_link/2,
         fold/3,
         fold/4,
         fold_query/3,
         fold_query/4,
         cancel/1]).

-export([rpc_query/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).


-record(state, {owner,
                reply_from,
                idx=0,
                waiting=[],
                ready=[],
                result,
                errors=[]}).

%%%===================================================================
%%% API
%%%===================================================================
%%--------------------------------------------------------------------
start_link(Str) ->
    start_link(Str, []).

start_link(Str, Opts) ->
    start_link(self(), Str, Opts).

start_link(Owner, Str, Opts) ->
    gen_server:start_link(?MODULE, [Owner, Str, Opts], []).

fold_query(Fun, Acc, Query) ->
    fold_query(Fun, Acc, Query, []).

fold_query(Fun, Acc, Query, Opts) ->
    {ok, MgrPid} = start_link(Query, Opts),
    fold(Fun, Acc, MgrPid).

fold(Fun, Acc, MgrPid) ->
    fold(Fun, Acc, MgrPid, 10).

fold(Fun, Acc, MgrPid, Limit) ->
    {ResultTab, Errors} = gen_server:call(MgrPid, {fetch, Limit}, infinity),
    case Errors of
        [] ->
            ignore;
        _ ->
            io:format(user, "Query had errors ~p~n", [Errors])
    end,
    fold(Fun, Acc, MgrPid, ResultTab, ets:first(ResultTab), Limit).

fold(_, Acc, MgrPid, _, '$end_of_table', _) ->
   cancel(MgrPid),
   Acc;
fold(Fun, Acc, MgrPid, ResultTable, Key, Limit) ->
    [{_, Row}] = ets:lookup(ResultTable, Key),
    case ets:next(ResultTable, Key) of
        '$end_of_table' ->
            SizeOld = ets:info(ResultTable, size),
            {ResultTable, _} = gen_server:call(MgrPid, {fetch, Limit}, infinity),
            case ets:info(ResultTable, size) > SizeOld of
                true ->
                    fold(Fun, Fun(Row, Acc), MgrPid, ResultTable,
                         ets:next(ResultTable, Key), Limit);
                false ->
                    cancel(MgrPid),
                    Fun(Row, Acc)
            end;
        Next ->
            fold(Fun, Fun(Row, Acc), MgrPid, ResultTable, Next, Limit)
    end.

cancel(MgrPid) ->
    gen_server:cast(MgrPid, cancel).


rpc_query(Owner, QueryString) ->
    spawn(fun() ->
                  monitor(process, Owner),
                  rpc_query_(Owner, QueryString)
          end).
rpc_query_(Owner, QueryString) ->
    {ok, MgrPid} = start_link(self(), QueryString),
    rpc_query_loop(Owner, MgrPid).
rpc_query_loop(Owner, MgrPid) ->
    {RestRows, _} =
    fold(fun(Row, {[], _}) ->
                 receive
                     {fetch, N} ->
                         {[Row], N};
                     cancel ->
                         exit(normal);
                     {'DOWN', _, process, Owner, _Reason} ->
                         exit(normal)
                 end;
            (Row, {Rows, N}) when length(Rows) == N ->
                 Owner ! {query_result, self(), [Row|Rows]},
                 {[], undefined};
            (Row, {Rows, N}) ->
                 {[Row|Rows], N}
         end, {[], undefined}, MgrPid),
    case RestRows of
        [] ->
            ok;
        _ ->
            Owner ! {query_result, self(), RestRows}
    end,
    Owner ! end_of_result.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Owner, Str, Opts]) ->
    monitor(process, Owner),
    {QueryPids, BadNodes} = vmq_ql_query_sup:start_query(self(), Str, Opts),
    case BadNodes of
        [] ->
            Jobs =
            lists:foldl(fun(Pid, Acc) ->
                                [{node(Pid), Pid}|Acc]
                        end, [], QueryPids),
            {ok, #state{owner=Owner, waiting=Jobs}};
        _ ->
            {stop, {bad_nodes, BadNodes}}
    end.

handle_call({fetch, Limit}, From, #state{reply_from=undefined, errors=Errors} = State) ->
    #state{result=ResultTab} = NewState = ensure_result_tab(State),
    case results_ready(NewState) of
        true ->
            {reply, {ResultTab, filter_errors(Errors)}, fetch_results(Limit, NewState)};
        false ->
            {noreply, NewState#state{reply_from={From, Limit}}}
    end.

handle_cast(cancel, State) ->
    {stop, normal, State}.

handle_info({'DOWN', _, process, Owner, Reason}, #state{owner=Owner} = State) ->
    lager:debug("VMQL query owner ~p stopped due to ~p", [Owner, Reason]),
    {stop, normal, State};
handle_info({query_error, Node, JobPid, Reason}, #state{waiting=Waiting,
                                                        errors=Errors} = State) ->
    NewState = fetch_if_ready(State#state{waiting=lists:delete({Node, JobPid}, Waiting),
                                          errors=[Reason|Errors]}),
    {noreply, NewState};
handle_info({results_ready, Node, JobPid, _NumResults},
            #state{waiting=Waiting, ready=Ready} = State) ->
    State1 = State#state{waiting=lists:delete({Node, JobPid}, Waiting),
                           ready=[{Node, JobPid}|Ready]},
    State2 = fetch_if_ready(State1),
    {noreply, State2}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

fetch_if_ready(#state{reply_from=ReplyFrom, result=ResultTab, errors=Errors} = State) ->
    case results_ready(State) of
        true when ReplyFrom =/= undefined ->
            {From, Limit} = ReplyFrom,
            State1 = fetch_results(Limit, State#state{reply_from=undefined}),
            gen_server:reply(From, {ResultTab, filter_errors(Errors)}),
            State1;
        _ ->
            State
    end.

filter_errors([]) -> [];
filter_errors(Errors) -> lists:usort(Errors).

results_ready(#state{waiting=[],ready=[]}) -> true; %% no query worker alive
results_ready(#state{waiting=[]}) -> true;
results_ready(_) -> false.

ensure_result_tab(#state{result=undefined} = State) ->
    State#state{result=ets:new(?MODULE, [public, ordered_set])};
ensure_result_tab(State) -> State.

fetch_results(Limit, #state{idx=Idx, result=ResultTab, ready=Ready} = State) ->
    NewIdx =
    lists:foldl(
      fun({_Node, Pid}, AccIdx) ->
              lists:foldl(
                fun({RowKey, Row}, AccAccIdx) ->
                        ets:insert(ResultTab, {{RowKey, AccAccIdx}, Row}),
                        AccAccIdx + 1
                end, AccIdx, vmq_ql_query:fetch(Pid, desc, Limit))
      end, Idx, Ready),
    State#state{idx=NewIdx}.
