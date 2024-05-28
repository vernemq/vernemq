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

-module(vmq_diversity_mysql2).
-include_lib("kernel/include/logger.hrl").
-include_lib("luerl/include/luerl.hrl").

-export([install/1]).

-import(luerl_lib, [badarg_error/3]).

install(St) ->
    luerl_emul:alloc_table(table(), St).

table() ->
    [
        {<<"execute">>, #erl_func{code = fun execute/2}},
        {<<"ensure_pool">>, #erl_func{code = fun ensure_pool/2}},
        {<<"hash_method">>, #erl_func{code = fun hash_method/2}}
    ].

equery(PoolName, Stmt, Params) ->
    poolboy:transaction(
        PoolName,
        fun(Worker) ->
            vmq_diversity_worker_wrapper:apply(Worker, mysql, query, [Stmt, Params])
        end
    ).

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

            try equery(PoolId, BQuery, Args) of
                {ok, Columns, Rows} ->
                    %% SELECT
                    {Table, NewSt} = luerl:encode(build_result(Rows, Columns), St),
                    {[Table], NewSt};
                ok ->
                    %% UPDATE success
                    {[true], St};
                {error, busy} = E ->
                    throw(E);
                {error, Error} ->
                    ?LOG_ERROR("MySQL error ~p~n", [Error]),
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
    case As of
        [Config0 | _] ->
            case luerl:decode(Config0, St) of
                Config when is_list(Config) ->
                    Options = vmq_diversity_utils:map(Config),
                    {ok, AuthConfigs} = application:get_env(vmq_diversity, db_config),
                    DefaultConf = proplists:get_value(mysql2, AuthConfigs),
                    PoolId = vmq_diversity_utils:atom(
                        maps:get(
                            <<"pool_id">>,
                            Options,
                            pool_mysql2
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
                    Encoding = vmq_diversity_utils:atom(
                        maps:get(
                            <<"encoding">>,
                            Options,
                            proplists:get_value(encoding, DefaultConf)
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
                                Verify = vmq_diversity_utils:atom(
                                    maps:get(
                                        <<"verify">>,
                                        Options,
                                        proplists:get_value(verify, DefaultConf)
                                    )
                                ),
                                Depth = vmq_diversity_utils:int(
                                    maps:get(
                                        <<"depth">>,
                                        Options,
                                        proplists:get_value(depth, DefaultConf)
                                    )
                                ),
                                CustomizeHostnameCheck0 = vmq_diversity_utils:atom(
                                    maps:get(
                                        <<"customize_hostname_check">>,
                                        Options,
                                        proplists:get_value(customize_hostname_check, DefaultConf)
                                    )
                                ),
                                SystemCAs = vmq_diversity_utils:atom(
                                    maps:get(
                                        <<"use_system_cas">>,
                                        Options,
                                        proplists:get_value(use_system_cas, DefaultConf)
                                    )
                                ),

                                L = [
                                    {certfile, CertFile},
                                    {keyfile, KeyFile},
                                    {verify, Verify},
                                    {depth, Depth},
                                    {server_name_indication, Host}
                                ],
                                MaybeHostNameCheck =
                                    case CustomizeHostnameCheck0 of
                                        'https' ->
                                            [
                                                {customize_hostname_check, [
                                                    {match_fun,
                                                        public_key:pkix_verify_hostname_match_fun(
                                                            https
                                                        )}
                                                ]}
                                                | L
                                            ];
                                        _ ->
                                            L
                                    end,
                                MaybeCacertfile =
                                    case CaCertFile of
                                        [] ->
                                            MaybeHostNameCheck;
                                        CF ->
                                            [{cacertfile, CF} | MaybeHostNameCheck]
                                    end,
                                MaybeSystemCAs =
                                    case SystemCAs of
                                        false ->
                                            MaybeCacertfile;
                                        true ->
                                            [{cacerts, public_key:cacerts_get()} | MaybeCacertfile]
                                    end,
                                [P || {_, V} = P <- MaybeSystemCAs, V /= ""];
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
                            {encoding, Encoding},
                            {ssl, Ssl},
                            {ssl_opts, SslOpts}
                        ],
                    vmq_diversity_sup:start_all_pools(
                        [{mysql2, [{id, PoolId}, {opts, NewOptions}]}], []
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
    build_result(Results, Columns, []).

build_result([Result | Results], Columns, Acc) ->
    build_result(Results, Columns, [
        lists:zip(Columns, Result)
        | Acc
    ]);
build_result([], _, Acc) ->
    lists:reverse(Acc).

hash_method(_, St) ->
    {ok, DBConfigs} = application:get_env(vmq_diversity, db_config),
    DefaultConf = proplists:get_value(mysql2, DBConfigs),
    HashMethod = proplists:get_value(password_hash_method, DefaultConf),
    MysqlFunc =
        case HashMethod of
            password -> <<"PASSWORD(?)">>;
            md5 -> <<"MD5(?)">>;
            sha1 -> <<"SHA1(?)">>;
            sha256 -> <<"SHA2(?, 256)">>
        end,
    {[MysqlFunc], St}.
