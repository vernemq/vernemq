%% Copyright 2018 Erlio GmbH Basel Switzerland (http://erl.io)
%% Copyright 2018-2024 Octavo Labs/VerneMQ (https://vernemq.com/)
%% and Individual Contributors.
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

-module(vmq_diversity_odbc).
-include_lib("kernel/include/logger.hrl").
-include_lib("luerl/include/luerl.hrl").

%% API functions
-export([
    install/1,
    squery/2,
    equery/3
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
            vmq_diversity_worker_wrapper:apply(Worker, odbc, sql_query, [Sql])
        end
    ).

equery(PoolName, Stmt, Params) ->
    poolboy:transaction(
        PoolName,
        fun(Worker) ->
            vmq_diversity_worker_wrapper:apply(Worker, odbc, param_query, [
                ensure_list(Stmt), Params
            ])
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
                        ?LOG_ERROR("unknown pool ~p", [BPoolId]),
                        badarg_error(unknown_pool, As, St)
                end,
            [MP, ClientId, Username, Password] = Args,

            Prms =
                [
                    {{sql_varchar, 10}, [ensure_list(MP)]},
                    {{sql_varchar, 128}, [ensure_list(ClientId)]},
                    {{sql_varchar, 128}, [ensure_list(Username)]},
                    {{sql_varchar, 128}, [ensure_list(Password)]}
                ],
            try equery(PoolId, BQuery, Prms) of
                {selected, Columns, Rows} ->
                    %% SELECT
                    {Table, NewSt} = luerl:encode(build_result(Rows, Columns), St),
                    {[Table], NewSt};
                {updated, 0} ->
                    %% UPDATE failed
                    {[false], St};
                {updated, _} ->
                    %% UPDATE success
                    {[true], St};
                {error, connection_closed} = E ->
                    throw(E);
                {error, _Error} ->
                    {[false], St}
            catch
                E:R ->
                    ?LOG_ERROR("can't execute query ~p due to ~p", [BQuery, {E, R}]),
                    badarg_error(execute_equery, As, St)
            end;
        _ ->
            badarg_error(execute_parse, As, St)
    end.

ensure_pool(As, St) ->
    ensure_pool(As, St, odbc, pool_odbc).

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
                    Driver = vmq_diversity_utils:ustr(
                        maps:get(
                            <<"driver">>,
                            Options,
                            proplists:get_value(driver, DefaultConf, undefined)
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
                            proplists:get_value(database, DefaultConf, undefined)
                        )
                    ),
                    Ssl = vmq_diversity_utils:atom(
                        maps:get(
                            <<"ssl">>,
                            Options,
                            proplists:get_value(ssl, DefaultConf, true)
                        )
                    ),
                    PWH = vmq_diversity_utils:atom(
                        maps:get(
                            <<"password_hash_method">>,
                            Options,
                            proplists:get_value(password_hash_method, DefaultConf)
                        )
                    ),
                    CS = vmq_diversity_utils:ustr(
                        maps:get(
                            <<"connection_string">>,
                            Options,
                            proplists:get_value(connection_string, DefaultConf, undefined)
                        )
                    ),
                    CaCertfile = vmq_diversity_utils:str(
                        maps:get(
                            <<"cacertfile">>,
                            Options,
                            proplists:get_value(cacertfile, DefaultConf, [])
                        )
                    ),
                    NewOptions =
                        [
                            {driver, Driver},
                            {size, Size},
                            {user, User},
                            {password, Password},
                            {host, Host},
                            {port, Port},
                            {database, Database},
                            {ssl, Ssl},
                            {password_hash_method, PWH},
                            {connection_string, CS},
                            {cacertfile, CaCertfile}
                        ],
                    vmq_diversity_sup:start_all_pools(
                        [{odbc, [{id, PoolId}, {opts, NewOptions}]}], []
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
    BinColumns = [list_to_bitstring(X) || X <- Columns],
    Result =
        case Results of
            [] ->
                [];
            [{PubACL, SubACL}] ->
                [lists:zip(BinColumns, [list_to_bitstring(PubACL), list_to_bitstring(SubACL)])]
        end,
    Result.

hash_method(_, St) ->
    {ok, DBConfigs} = application:get_env(vmq_diversity, db_config),
    DefaultConf = proplists:get_value(odbc, DBConfigs),
    HashMethod = proplists:get_value(password_hash_method, DefaultConf),
    ODBCFunc =
        case HashMethod of
            % MSSQL (untested):
            hashbytes256 -> <<"HASHBYTES('SHA2_256', password)">>;
            % MSSQL (untested):
            hashbytes512 -> <<"HASHBYTES('SHA2_512', password)">>;
            % MySQL:
            md5 -> <<"MD5(?)">>;
            % MySQL:
            sha1 -> <<"SHA1(?)">>;
            % CockroachDB:
            sha256_cockroachdb -> <<"SHA2(?)">>;
            % MySQL:
            sha256_mysql -> <<"SHA2(?, 256)">>;
            % MySQL:
            sha512_mysql -> <<"SHA2(?, 512)">>;
            % PostgreSQL (untested):
            sha256_pg -> <<"SHA256(?)">>;
            % PostgreSQL (untested):
            sha512_pg -> <<"SHA512(?)">>;
            % PostgreSQL:
            crypt -> <<"CRYPT(?, password)">>
        end,
    {[ODBCFunc], St}.

ensure_list(L) when is_list(L) -> L;
ensure_list(L) when is_binary(L) -> binary_to_list(L).
