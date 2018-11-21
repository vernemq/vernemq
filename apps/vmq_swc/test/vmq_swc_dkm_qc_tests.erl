-module(vmq_swc_dkm_qc_tests).
-include_lib("triq/include/triq.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([initial_state/0
         , command/1
         , precondition/2
         , postcondition/3
         , next_state/3
        ]).

-record(state, {dkm=vmq_swc_dkm:init(), mdkm=model_dkm_init()}).

prop_dkm_statem() ->
    ?FORALL(Cmds,
            triq_statem:commands(?MODULE),
            begin
                triq_statem:run_commands(?MODULE, Cmds),
                true
            end).

id() ->
    oneof([a,b,c,d,e]).

key() ->
    oneof([integer_to_binary(I) || I <- lists:seq(1, 100)]).

initial_state() ->
    #state{}.

command(State) ->
    ?LET({Id, Key}, {id(), key()},
         frequency([
                    {10, {call, vmq_swc_dkm, insert, [State#state.dkm, Id, next_cnt(Id), Key]}},
                    {8, {call, vmq_swc_dkm, mark_for_gc, [State#state.dkm, Key]}},
                    {3, {call, vmq_swc_dkm, prune, [State#state.dkm, Id, current_cnt(Id), []]}}
               ])).

precondition(State, {call, vmq_swc_dkm, mark_for_gc, [_DKM, Key]}) ->
    maps:is_key({key, Key}, State#state.mdkm);
precondition(_State, _Call) ->
    true.

postcondition(State0, {call, vmq_swc_dkm, prune, [_DKM, Id, Cnt, []]}, _DBOps) ->
    {_, State1} = model_dkm_prune(Id, Cnt, State0),
    vmq_swc_dkm:dkm(State1#state.dkm) == to_map(State1#state.mdkm);
postcondition(_State, _Call, _Res) ->
    true.

next_state(State, _Var, {call, vmq_swc_dkm, insert, [_DKM, Id, Cnt, Key]}) ->
    model_dkm_insert({Id, Cnt}, Key, State);
next_state(State, _Var, {call, vmq_swc_dkm, mark_for_gc, [_DKM, Key]}) ->
    model_dkm_remove(Key, State);
next_state(State0, _Var, {call,vmq_swc_dkm, prune, [_DKM, Id, Cnt, _Acc]}) ->
    {_, State1} = model_dkm_prune(Id, Cnt, State0),
    State1.

model_dkm_init() ->
    #{}.

to_map(MDKM) ->
    lists:foldl(fun({{dot, Dot}, Key}, Acc) ->
                      maps:put(Dot, Key, Acc);
                 (_, Acc) ->
                      Acc
              end, #{}, maps:to_list(MDKM)).

model_dkm_insert({Id, Cnt} = Dot, Key, #state{mdkm=MDKM} = State) ->
    {Gc, T} = maps:get({key, Key}, MDKM, {false, []}),
    M0 =
    case lists:keyfind(Id, 1, lists:reverse(lists:sort(T))) of
        false ->
            maps:put({key, Key}, {false, [Dot | T]}, MDKM);
        {_, CurrentCnt} when Cnt > CurrentCnt ->
            maps:put({key, Key}, {false, [Dot | T]}, MDKM);
        _ ->
            maps:put({key, Key}, {Gc, [Dot | T]}, MDKM)
    end,
    State#state{mdkm=maps:put({dot, Dot}, Key, M0)}.

model_dkm_remove(Key, #state{mdkm=MDKM} = State) ->
    case maps:get({key, Key}, MDKM, undefined) of
        undefined -> State;
        {_, T0} ->
            State#state{mdkm=maps:put({key, Key}, {true, T0}, MDKM)}
    end.

model_dkm_prune(Id, MinCnt, #state{mdkm=MDKM} = State) ->
    {DbOps, MDKM1} = lists:foldl(
              fun({{dot, {I, C} = Dot}, Key}, {Ops, Acc}) when (I == Id) and (C =< MinCnt) ->
                      {Gc, T0} = maps:get({key, Key}, Acc),
                      case T0 -- [Dot] of
                          [] when Gc ->
                              {[{dkm, sext:encode(Dot), '$deleted'},
                                {obj, Key, '$deleted'} | Ops],
                               maps:remove({dot, Dot}, maps:remove({key, Key}, Acc))};
                          [] ->
                              %% last dot, keep dot and object
                              {Ops, Acc};
                          T1 ->
                              {[{dkm, sext:encode(Dot), '$deleted'}|Ops]
                               , maps:remove({dot, Dot}, maps:put({key, Key}, {Gc, T1}, Acc))}
                      end;
                 (_, Acc) -> Acc
              end, {[], MDKM},
              %% sort the Dots, to ensure that we'll end up with the largest dot after prunning
              lists:sort(maps:to_list(MDKM))),
    {DbOps, State#state{mdkm=MDKM1}}.

current_cnt(Id) ->
    case get({cnt, Id}) of
        undefined ->
            0;
        Cnt ->
            Cnt + 1
    end.

next_cnt(Id) ->
    NextCnt = current_cnt(Id) + 1,
    put({cnt, Id}, NextCnt),
    NextCnt.

run_property_testing_test_() ->
    {timeout, 60, fun run_property_testing_case/0}.

run_property_testing_case() ->
    EunitLeader = erlang:group_leader(),
    erlang:group_leader(whereis(user), self()),
    Res = triq:module(?MODULE),
    erlang:group_leader(EunitLeader, self()),
    ?assert(Res).
