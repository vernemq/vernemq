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

-module(vmq_diversity_postgres).
-include_lib("luerl/include/luerl.hrl").

%% API functions
-export([
    install/1,
    squery/2,
    equery/3,

    %% exported for `vmq_diversity_cockroachdb`
    execute/2,
    ensure_pool/4
]).

-import(luerl_lib, [badarg_error/3]).

%%%===================================================================
%%% API functions
%%%===================================================================

install(St) ->
    luerl_emul:alloc_table(table(), St).

squery(PoolName, Sql) ->
    poolboy:transaction(
        PoolName,
        fun(Worker) ->
            vmq_diversity_worker_wrapper:apply(Worker, epgsql, squery, [Sql])
        end
    ).

equery(PoolName, Stmt, Params) ->
    poolboy:transaction(
        PoolName,
        fun(Worker) ->
            vmq_diversity_worker_wrapper:apply(Worker, epgsql, equery, [Stmt, Params])
        end
    ).

%%%===================================================================
%%% Internal functions
%%%===================================================================
table() ->
    [
        {<<"execute">>, #erl_func{code = fun execute/2}},
        {<<"ensure_pool">>, #erl_func{code = fun ensure_pool/2}},
        {<<"hash_method">>, #erl_func{code = fun hash_method/2}}
    ].

execute(As, St) ->
    case As of
        [BPoolId, BQuery | Args] when
            is_binary(BPoolId) and
                is_binary(BQuery)
        ->
            PoolId =
                try list_to_existing_atom(binary_to_list(BPoolId)) of
                    APoolId -> APoolId
                catch
                    _:_ ->
                        lager:error("unknown pool ~p", [BPoolId]),
                        badarg_error(unknown_pool, As, St)
                end,

            try equery(PoolId, BQuery, Args) of
                {ok, Columns, Rows} ->
                    %% SELECT
                    {Table, NewSt} = luerl:encode(build_result(Rows, Columns), St),
                    {[Table], NewSt};
                {ok, 0} ->
                    %% UPDATE failed
                    {[false], St};
                {ok, _} ->
                    %% UPDATE success
                    {[true], St};
                {ok, 0, _Columns, _Rows} ->
                    %% INSERT failed
                    {[false], St};
                {ok, _, _Columns, _Rows} ->
                    %% INSERT success
                    {[true], St};
                {error, not_connected} = E ->
                    throw(E);
                {error, _Error} ->
                    {[false], St}
            catch
                E:R ->
                    lager:error("can't execute query ~p due to ~p", [BQuery, {E, R}]),
                    badarg_error(execute_equery, As, St)
            end;
        _ ->
            badarg_error(execute_parse, As, St)
    end.

ensure_pool(As, St) ->
    ensure_pool(As, St, postgres, pool_postgres).

ensure_pool(As, St, DB, DefaultPoolId) ->
    case As of
        [Config0 | _] ->
            case luerl:decode(Config0, St) of
                Config when is_list(Config) ->
                    {ok, AuthConfigs} = application:get_env(vmq_diversity, db_config),
                    DefaultConf = proplists:get_value(DB, AuthConfigs),
                    Options = vmq_diversity_utils:map(Config),
                    PoolId = vmq_diversity_utils:atom(
                        maps:get(
                            <<"pool_id">>,
                            Options,
                            DefaultPoolId
                        )
                    ),

                    Size = vmq_diversity_utils:int(
                        maps:get(
                            <<"size">>,
                            Options,
                            proplists:get_value(pool_size, DefaultConf)
                        )
                    ),
                    User = vmq_diversity_utils:str(
                        maps:get(
                            <<"user">>,
                            Options,
                            proplists:get_value(user, DefaultConf)
                        )
                    ),
                    Password = vmq_diversity_utils:str(
                        maps:get(
                            <<"password">>,
                            Options,
                            proplists:get_value(password, DefaultConf)
                        )
                    ),
                    Host = vmq_diversity_utils:str(
                        maps:get(
                            <<"host">>,
                            Options,
                            proplists:get_value(host, DefaultConf)
                        )
                    ),
                    Port = vmq_diversity_utils:int(
                        maps:get(
                            <<"port">>,
                            Options,
                            proplists:get_value(port, DefaultConf)
                        )
                    ),
                    Database = vmq_diversity_utils:ustr(
                        maps:get(
                            <<"database">>,
                            Options,
                            proplists:get_value(database, DefaultConf)
                        )
                    ),
                    Ssl = vmq_diversity_utils:atom(
                        maps:get(
                            <<"ssl">>,
                            Options,
                            proplists:get_value(ssl, DefaultConf, false)
                        )
                    ),
                    SslOpts =
                        case Ssl of
                            true ->
                                CertFile = vmq_diversity_utils:str(
                                    maps:get(
                                        <<"certfile">>,
                                        Options,
                                        proplists:get_value(certfile, DefaultConf)
                                    )
                                ),
                                CaCertFile = vmq_diversity_utils:str(
                                    maps:get(
                                        <<"cacertfile">>,
                                        Options,
                                        proplists:get_value(cacertfile, DefaultConf)
                                    )
                                ),
                                KeyFile = vmq_diversity_utils:str(
                                    maps:get(
                                        <<"keyfile">>,
                                        Options,
                                        proplists:get_value(keyfile, DefaultConf)
                                    )
                                ),
                                L = [
                                    {certfile, CertFile},
                                    {cacertfile, CaCertFile},
                                    {keyfile, KeyFile}
                                ],
                                [P || {_, V} = P <- L, V /= ""];
                            false ->
                                []
                        end,
                    NewOptions =
                        [
                            {size, Size},
                            {user, User},
                            {password, Password},
                            {host, Host},
                            {port, Port},
                            {database, Database},
                            {ssl, Ssl},
                            {ssl_opts, SslOpts}
                        ],
                    vmq_diversity_sup:start_all_pools(
                        [{pgsql, [{id, PoolId}, {opts, NewOptions}]}], []
                    ),

                    % return to lua
                    {[true], St};
                _ ->
                    badarg_error(execute_parse, As, St)
            end;
        _ ->
            badarg_error(execute_parse, As, St)
    end.

build_result(Results, Columns) ->
    build_result(Results, [Name || {column, Name, _, _, _, _, _, _, _} <- Columns], []).

build_result([Result | Results], Names, Acc) ->
    build_result(Results, Names, [lists:zip(Names, tuple_to_list(Result)) | Acc]);
build_result([], _, Acc) ->
    lists:reverse(Acc).

hash_method(_, St) ->
    {ok, DBConfigs} = application:get_env(vmq_diversity, db_config),
    DefaultConf = proplists:get_value(postgres, DBConfigs),
    HashMethod = proplists:get_value(password_hash_method, DefaultConf),
    {[atom_to_binary(HashMethod, utf8)], St}.
