-module(vmq_diversity_json).

-export([install/1]).


install(St) ->
    luerl_emul:alloc_table(table(), St).

table() ->
    [
     {<<"decode">>, {function, fun decode/2}},
     {<<"encode">>, {function, fun encode/2}}
    ].

decode([Bin|_], St) when is_binary(Bin) ->
    try jsx:decode(Bin) of
        Result0 ->
            {Result1, NewSt} = luerl:encode(Result0, St),
            {[Result1], NewSt}
    catch
        _:_ ->
            {[false], St}
    end.

encode([T|_], St) when is_tuple(T) ->
    try jsx:encode(encode_value(luerl:decode(T, St))) of
        Result0 ->
            {[Result0], St}
    catch
        _:_ ->
            {[false], St}
    end.

encode_value(V) when is_list(V) ->
    encode_list(V, []);
encode_value(V) ->
    V.

encode_list([{K, V}|Rest], Acc) when is_integer(K) ->
    encode_list(Rest, [encode_value(V)|Acc]);
encode_list([{K, V}|Rest], Acc) when is_binary(K) ->
    encode_list(Rest, [{K, encode_value(V)}|Acc]);
encode_list([], Acc) -> lists:reverse(Acc).
