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

-module(vmq_diversity_http).
-include_lib("luerl/include/luerl.hrl").

-export([install/1]).

-import(luerl_lib, [badarg_error/3]).

install(St) ->
    luerl_emul:alloc_table(table(), St).

table() ->
    [
        {<<"get">>, #erl_func{code = fun get/2}},
        {<<"put">>, #erl_func{code = fun put/2}},
        {<<"post">>, #erl_func{code = fun post/2}},
        {<<"delete">>, #erl_func{code = fun delete/2}},
        {<<"body">>, #erl_func{code = fun body/2}},
        {<<"ensure_pool">>, #erl_func{code = fun ensure_pool/2}}
    ].

get(As, St) ->
    request(get, As, St).
put(As, St) ->
    request(put, As, St).
post(As, St) ->
    request(post, As, St).
delete(As, St) ->
    request(delete, As, St).

request(Method, [BPoolId, Url | Rest0] = As, St) when is_binary(Url) ->
    PoolId = pool_id(BPoolId, As, St),
    {Payload, Rest1} = decode_payload(Rest0, St),
    {Headers, _} = decode_headers(Rest1, St),
    case hackney:request(Method, Url, Headers, Payload, [{pool, PoolId}, with_body]) of
        {ok, StatusCode, RespHeaders, Body} ->
            Table = [
                {status, StatusCode},
                {headers, RespHeaders},
                %% We not longer have a reference, but we store
                %% the body in the reference to not break
                %% existing scripts. Earlier a reference was
                %% returned, but if the user didn't call body on
                %% it, the underlying connection would not be
                %% returned to the connection pool, drying up the
                %% pool.
                {ref, Body}
            ],
            {NewTable, NewSt} = luerl:encode(Table, St),
            {[NewTable], NewSt};
        {error, Reason} ->
            lager:error("http request failure for ~p ~p due to ~p", [Method, Url, Reason]),
            {[false], St}
    end.

body([Body | _], St) ->
    {[Body], St}.

decode_payload([Payload | Rest], _) when is_binary(Payload) ->
    {Payload, Rest};
decode_payload([T | Rest], St) when is_tuple(T) ->
    case luerl:decode(T, St) of
        [{K, _} | _] = KVPayload when is_binary(K) ->
            {{form, KVPayload}, Rest};
        _ ->
            {<<>>, Rest}
    end;
decode_payload(Rest, _) ->
    {<<>>, Rest}.

decode_headers([T | Rest], St) when is_tuple(T) ->
    case luerl:decode(T, St) of
        [{K, _} | _] = Headers when is_binary(K) ->
            {Headers, Rest};
        _ ->
            {[], Rest}
    end;
decode_headers(Rest, _) ->
    {[], Rest}.

ensure_pool(As, St) ->
    case As of
        [Config0 | _] ->
            case luerl:decode(Config0, St) of
                Config when is_list(Config) ->
                    Options = vmq_diversity_utils:map(Config),
                    PoolId = vmq_diversity_utils:atom(
                        maps:get(
                            <<"pool_id">>,
                            Options,
                            pool_http
                        )
                    ),

                    Size = vmq_diversity_utils:int(
                        maps:get(
                            <<"size">>,
                            Options,
                            10
                        )
                    ),
                    NewOptions = [{size, Size}],
                    vmq_diversity_sup:start_all_pools(
                        [{http, [{id, PoolId}, {opts, NewOptions}]}], []
                    ),

                    % return to lua
                    {[true], St};
                _ ->
                    badarg_error(execute_parse, As, St)
            end;
        _ ->
            badarg_error(execute_parse, As, St)
    end.

pool_id(BPoolId, As, St) ->
    try list_to_existing_atom(binary_to_list(BPoolId)) of
        APoolId -> APoolId
    catch
        _:_ ->
            lager:error("unknown pool ~p", [BPoolId]),
            badarg_error(unknown_pool, As, St)
    end.
