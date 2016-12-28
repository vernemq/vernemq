%% Copyright 2016 Erlio GmbH Basel Switzerland (http://erl.io)
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

-module(vmq_info).

-export([query/1]).

-export([session_info_items/0]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%    _   ____  _______    __
%%%   | | / /  |/  / __ \  / /
%%%   | |/ / /|_/ / /_/ / / /__
%%%   |___/_/  /_/\___\_\/____/
%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
query(Str) ->
    Parsed = vmq_ql:parse(Str),
    eval(proplists:get_value(type, Parsed), Parsed).

eval("SELECT", Query) ->
    From = proplists:get_value(from, Query),
    Fields = proplists:get_value(fields, Query),
    Where = proplists:get_value(where, Query),
    OrderBy = proplists:get_value(orderby, Query),
    Limit = proplists:get_value(limit, Query),
    select(Fields, From, Where, OrderBy, Limit).

order_by_key(Idx, [], _) -> Idx;
order_by_key(Idx, OrderBy, Row) ->
    {lists:reverse(lists:foldl(fun(Field, OrderByAcc) ->
                                       [maps:get(Field, Row, null)|OrderByAcc]
                               end, [], OrderBy)), Idx}.

raise_enough_data(Cnt, [], Limit, Exit) when is_integer(Limit) and (Cnt > Limit) ->
    exit(Exit);
raise_enough_data(_, _, _, _) -> ignore.


required_fields(Where) ->
    lists:foldl(fun(A, Atoms) when not is_boolean(A) ->
                        [A|Atoms];
                   (_, Atoms) -> Atoms
                end, [], prepare(Where, [])).

prepare([], Acc) -> lists:usort(Acc);

prepare([{op, V1, _Op, V2}|Rest], Acc) ->
    prepare(Rest, [V1, V2|Acc]);
prepare([{_, {op, V1, _Op, V2}}|Rest], Acc) ->
    prepare(Rest, [V1, V2|Acc]);
prepare([{_, Ops}|Rest], Acc) when is_list(Ops) ->
    prepare(Rest, prepare(Ops, Acc)).


filter_row([all], EmptyResultRow, Row) ->
    maps:merge(EmptyResultRow, Row);
filter_row(Fields, EmptyResultRow, Row) ->
    maps:merge(EmptyResultRow, maps:with(Fields, Row)).


eval_query([]) ->
    %% No WHERE clause was specified
    true;
eval_query([{op, V1, Op, V2}|Rest]) ->
    eval_query(Rest, eval_op(Op, V1, V2));
eval_query([Op|Rest]) when is_list(Op) ->
    eval_query(Rest, eval_query(Op)).

eval_query([], Bool) -> Bool;

eval_query([{'and', {op, V1, Op, V2}}|Rest], true) ->
    case eval_op(Op, V1, V2) of
        true -> eval_query(Rest, true);
        false -> false
    end;
eval_query([{'and', Ops}|Rest], true) when is_list(Ops) ->
   case eval_query(Ops) of
       true -> eval_query(Rest, true);
       false -> false
   end;
eval_query([{'or', {op, V1, Op, V2}}|Rest], false) ->
    case eval_op(Op, V1, V2) of
        false -> eval_query(Rest, false);
        true -> true
    end;
eval_query([{'or', Ops}|Rest], false) when is_list(Ops) ->
   case eval_query(Ops) of
       false -> eval_query(Rest, false);
       true -> true
   end;
eval_query([{'and', _}|_], false) -> false; % Always false
eval_query([{'or', _}|_], true) -> true. % Always true

eval_op(equals, V1, V2) -> v(V1) == v(V2);
eval_op(not_equals, V1, V2) -> v(V1) /= v(V2);
eval_op(lesser, V1, V2) -> v(V1) < v(V2);
eval_op(lesser_equals, V1, V2) -> v(V1) =< v(V2);
eval_op(greater, V1, V2) -> v(V1) > v(V2);
eval_op(greater_equals, V1, V2) -> v(V1) >= v(V2);
eval_op(like, V, V) -> true;
eval_op(like, V1, V2) ->
    case {v(V1), v(V2)} of
        {V, P} when (is_list(V) or is_binary(V))
                and (is_list(P) or is_binary(P)) ->
            case get({?MODULE, P}) of
                undefined ->
                    case re:compile(P) of
                        {ok, MP} ->
                            put({?MODULE, P}, MP),
                            re:run(V, MP) =/= nomatch;
                        {error, ErrSpec} ->
                            lager:error("can't compile regexp ~p due to ~p", [P, ErrSpec]),
                            false
                    end;
                MP ->
                    re:run(V, MP) =/= nomatch
            end;
        _ ->
            false
    end.

v(true) -> true;
v(false) -> false;
v(undefined) -> null;
v(V) when is_atom(V) ->
    lookup_ident(V);
v(V) when is_pid(V) ->
    list_to_binary(pid_to_list(V));
v(V) -> V.

lookup_ident(Ident) ->
    Row = get({?MODULE, row_data}),
    case maps:find(Ident, Row) of
        error -> undefined;
        {ok, V} -> V
    end.

load_all(FieldConfig, Row) ->
    load_all(FieldConfig, Row, []).

load_all([], Row, Acc) -> lists:flatten([Row|Acc]);
load_all([{Fields, InitFun}|Rest], Row, Acc) ->
      lists:foldl(fun(R, AccAcc) ->
                        AccAcc ++ load_all(Rest, R, [])
                end, Acc, InitFun(Fields, Row)).

load_rows([], _, _, Acc) -> lists:flatten(Acc);
load_rows(_, [], _, Acc) -> lists:flatten(Acc);
load_rows(_, _, [], Acc) -> lists:flatten(Acc);
load_rows(FieldConfig, RequiredFields, [Row|Rows], Acc) ->
    load_rows(FieldConfig, RequiredFields, Rows,
              [load_row(FieldConfig, RequiredFields, Row)|Acc]).

load_row([{Fields, InitFun}|Rest], RequiredFields, Row) ->
    Rows = InitFun(Fields, Row),
    case RequiredFields -- Fields of
        [] ->
            %% all data fetched to meet this query
            Rows;
        UpdatedRequiredFields ->
            load_rows(Rest, UpdatedRequiredFields, Rows, [])
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% VMQ Sessions specific
%%%
%%% TODO: Further abstract the select loop
%%% TODO: Create a own vmql behaviour

session_fields_config() ->
    [{[mountpoint,
       client_id,
       queue_pid], fun row_init/2},
     {[status,
       deliver_mode,
       queue_size,
       session_pid,
       is_offline,
       is_online,
       statename,
       deliver_mode,
       offline_messages,
       online_messages,
       num_sessions,
       clean_session,
       is_plugin],  fun queue_row_init/2},
     {[user,
       peer_host,
       peer_port,
       protocol,
       waiting_acks], fun session_row_init/2}
    ].

session_info_items() ->
    %% used in vmq_info_cli
    lists:flatten([Fields || {Fields, _} <- session_fields_config()]).

select(Fields, sessions, Where, OrderBy, Limit) ->
    RequiredFields = lists:usort((required_fields(Where)
                                  ++ Fields
                                  ++ OrderBy) -- [all]),
    EmptyResultRow = empty_result_row(Fields),
    Results = ets:new(?MODULE, []),
    try vmq_queue_sup_sup:fold_queues(
      fun({MP, ClientId}, QPid, Idx) ->
              InitRow = #{mountpoint => MP,
                          client_id => ClientId,
                          queue_pid => QPid},
              PreparedRows =
              case lists:member(all, Fields) of
                  true ->
                      load_all(session_fields_config(), InitRow);
                  false ->
                      load_row(session_fields_config(), RequiredFields, InitRow)
              end,
              lists:foldl(fun(Row, AccIdx) ->
                                   put({?MODULE, row_data}, Row),
                                   case eval_query(Where) of
                                       true ->
                                           raise_enough_data(Idx, OrderBy, Limit, enough_data),
                                           Key = order_by_key(Idx, OrderBy, Row),
                                           ets:insert(Results,
                                                      {Key, filter_row(Fields, EmptyResultRow, Row)}),
                                           AccIdx + 1;
                                       false -> AccIdx
                                   end
                           end, Idx, PreparedRows)
      end, 1) catch
        exit:enough_data ->
            %% Raising inside an ets:fold allows us to stop the fold
            ok
    end,
    try ets:foldl(
          fun({_, Row}, Acc) ->
                  raise_enough_data(length(Acc) + 1, [], Limit, {enough_data, Acc}),
                  [Row|Acc]
          end, [], Results)
    catch
        exit:{enough_data, Res} ->
            Res
    end.

empty_result_row([all]) ->
    Fields = lists:flatten([Fs || {Fs, _} <- session_fields_config()]),
    empty_result_row(Fields);
empty_result_row(Fields) ->
    maps:from_list([{F, null} || F <- Fields]).

row_init(_, Row) ->
    [Row].

queue_row_init(_, Row) ->
   QPid = maps:get(queue_pid, Row),
   QueueData = vmq_queue:info(QPid),
   case maps:get('sessions', QueueData) of
       [] ->
           % offline queue
           [maps:merge(Row, maps:remove('sessions', QueueData#{clean_session => false}))];
       Sessions ->
           QueueDataWithoutSessions = maps:remove('sessions', QueueData),
           Row1 = maps:merge(Row, QueueDataWithoutSessions),
           lists:foldl(fun({SessionPid, CleanSession}, Acc) ->
                               [maps:merge(Row1, #{session_pid => SessionPid,
                                                   clean_session => CleanSession}) | Acc]
                       end, [], Sessions)
   end.

session_row_init(_, Row) ->
    case maps:find(session_pid, Row) of
        error ->
            [Row];
        {ok, SessionPid} ->
            {ok, InfoItems} = vmq_mqtt_fsm:info(SessionPid, [user,
                                                             peer_host,
                                                             peer_port,
                                                             protocol,
                                                             waiting_acks]),
            [maps:merge(Row, maps:from_list(InfoItems))]
    end.
