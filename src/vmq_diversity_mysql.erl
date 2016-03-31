-module(vmq_diversity_mysql).
-include_lib("emysql/include/emysql.hrl").

-export([install/1]).

-import(luerl_lib, [badarg_error/3]).

install(St) ->
    luerl_emul:alloc_table(table(), St).

table() ->
    [
     {<<"execute">>, {function, fun execute/2}},
     {<<"ensure_pool">>, {function, fun ensure_pool/2}}
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

ensure_pool(As, St) ->
    case As of
        [Config0|_] ->
            case luerl:decode(Config0, St) of
                Config when is_list(Config) ->
                    Options = vmq_diversity_utils:map(Config),
                    PoolId = vmq_diversity_utils:atom(maps:get(<<"pool_id">>,
                                                               Options,
                                                               pool_mysql)),

                    Size = vmq_diversity_utils:int(maps:get(<<"size">>,
                                                            Options, 5)),
                    User = vmq_diversity_utils:str(maps:get(<<"user">>,
                                                            Options, "")),
                    Password = vmq_diversity_utils:str(maps:get(<<"password">>,
                                                                Options, "")),
                    Host = vmq_diversity_utils:str(maps:get(<<"host">>,
                                                            Options, "127.0.0.1")),
                    Port = vmq_diversity_utils:int(maps:get(<<"port">>, Options,
                                                            3306)),
                    Database = vmq_diversity_utils:ustr(maps:get(<<"database">>,
                                                                 Options,
                                                                 undefined)),
                    Encoding = vmq_diversity_utils:atom(maps:get(<<"encoding">>,
                                                                 Options,
                                                                 latin1)),
                    NewOptions =
                    [{size, Size}, {user, User}, {password, Password},
                     {host, Host}, {port, Port}, {database, Database},
                     {encoding, Encoding}],
                    vmq_diversity_sup:start_all_pools(
                      [{mysql, [{id, PoolId}, {opts, NewOptions}]}], []),

                    % return to lua
                    {[true], St};
                _ ->
                    badarg_error(execute_parse, As, St)
            end;
        _ ->
            badarg_error(execute_parse, As, St)
    end.





