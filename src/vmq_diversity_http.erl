-module(vmq_diversity_http).

-export([install/1]).

-import(luerl_lib, [badarg_error/3]).

install(St) ->
    luerl_emul:alloc_table(table(), St).

table() ->
    [
     {<<"get">>, {function, fun get/2}},
     {<<"put">>, {function, fun put/2}},
     {<<"post">>, {function, fun post/2}},
     {<<"delete">>, {function, fun delete/2}},
     {<<"body">>, {function, fun body/2}}
    ].

get(As, St) ->
    request(get, As, St).
put(As, St) ->
    request(put, As, St).
post(As, St) ->
    request(post, As, St).
delete(As, St) ->
    request(delete, As, St).

request(Method, [BPoolId, Url|Rest0] = As, St) when is_binary(Url) ->
    PoolId = pool_id(BPoolId, As, St),
    {Payload, Rest1} = decode_payload(Rest0, St),
    {Headers, _} = decode_headers(Rest1, St),
    case hackney:request(Method, Url, Headers, Payload, [{pool, PoolId}]) of
        {ok, StatusCode, RespHeaders, ClientRef} ->
            BClientRef = term_to_binary(ClientRef),
            BBClientRef = <<"client-ref-", BClientRef/binary>>,
            Table = [{status, StatusCode},
                     {headers, RespHeaders},
                     {ref, BBClientRef}],
            {NewTable, NewSt} = luerl:encode(Table, St),
            {[NewTable], NewSt};
        {error, Reason} ->
            lager:error("http request failure for ~p ~p due to ~p", [Method, Url, Reason]),
            {[false], St}
    end.

body([<<"client-ref-", BClientRef/binary>> = Ref|_], St) ->
    ClientRef = binary_to_term(BClientRef),
    case hackney:body(ClientRef) of
        {ok, Body} ->
            {[Body], St};
        {error, Reason} ->
            lager:error("cant fetch response body for ~p due to ~p", [Ref, Reason]),
            {[false], St}
    end.

decode_payload([Payload|Rest], _) when is_binary(Payload) ->
    {Payload, Rest};
decode_payload([T|Rest], St) when is_tuple(T) ->
    case luerl:decode(T, St) of
        [{K, _}|_] = KVPayload when is_binary(K) ->
            {{form, KVPayload}, Rest};
        _ ->
            {<<>>, Rest}
    end;
decode_payload(Rest, _) -> {<<>>, Rest}.

decode_headers([T|Rest], St) when is_tuple(T) ->
    case luerl:decode(T, St) of
        [{K, _}|_] = Headers when is_binary(K) ->
            {Headers, Rest};
        _ ->
            {[], Rest}
    end;
decode_headers(Rest, _) -> {[], Rest}.

pool_id(BPoolId, As, St) ->
    try list_to_existing_atom(binary_to_list(BPoolId)) of
        APoolId -> APoolId
    catch
        _:_ ->
            lager:error("unknown pool ~p", [BPoolId]),
            badarg_error(unknown_pool, As, St)
    end.
