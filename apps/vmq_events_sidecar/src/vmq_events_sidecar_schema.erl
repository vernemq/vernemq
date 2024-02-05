-module(vmq_events_sidecar_schema).

-export([
    translate_sampling/2
]).

translate_sampling(Hook, Conf) ->
    HookSampler = lists:filter(
        fun({K, _V}) ->
            cuttlefish_variable:is_fuzzy_match(
                K, string:tokens("sample." ++ Hook ++ ".$criterion.percentage", ".")
            )
        end,
        Conf
    ),

    [
        begin
            case (P >= 0) and (P =< 100) of
                true ->
                    ok;
                _ ->
                    cuttlefish:invalid(
                        "sample." ++ Hook ++ "." ++ Criterion ++
                            ".percentage should be between 0 and 100, is " ++
                            integer_to_list(P)
                    )
            end,
            {Criterion, P}
        end
     || {[_, _, Criterion, _], P} <- HookSampler
    ].
