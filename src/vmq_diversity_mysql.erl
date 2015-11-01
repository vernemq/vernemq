-module(vmq_diversity_mysql).
-include_lib("emysql/include/emysql.hrl").

-export([install/1]).

-import(luerl_lib, [badarg_error/3]).

install(St) ->
    luerl_emul:alloc_table(table(), St).

table() ->
    [
     {<<"execute">>, {function, fun execute/2}}
    ].

execute(As, St) ->
    case As of
        [BPoolId, BQuery|Args] when is_binary(BPoolId)
                                    and is_binary(BQuery) ->
            PoolId =
            try list_to_existing_atom(binary_to_list(BPoolId)) of
                APoolId -> APoolId
            catch
                _:_ ->
                    lager:error("unknown pool ~p", [BPoolId]),
                    badarg_error(unknown_pool, As, St)
            end,

            try emysql:execute(PoolId, BQuery, Args) of
                #result_packet{} = Result ->
                    {Table, NewSt} = luerl:encode(emysql:as_proplist(Result), St),
                    {[Table], NewSt};
                #ok_packet{} ->
                    {[true], St};
                #error_packet{} ->
                    {[false], St};
                #eof_packet{} ->
                    {[false], St}
            catch
                E:R ->
                    lager:error("can't execute query ~p due to ~p", [BQuery, E, R]),
                    badarg_error(execute_equery, As, St)
            end;
        _ ->
            badarg_error(execute_parse, As, St)
    end.
