-module(vmq_util).

%% API
-export([
    nvl/2,
    lists_to_atom_tuple/1, lists_to_atom_tuple/2, lists_to_atom_tuple/3, lists_to_atom_tuple/4,
    nth/2,
    nth/3,
    concat_dot/2,
    to_int/2,
    to_bool/2,
    elem/2,
    mqtt_version_to_string/1
]).

% Returns the first non-undefined value
nvl(undefined, Value2) ->
    Value2;
nvl(Value1, _) ->
    Value1.

% Maps lists to a atom tuple
lists_to_atom_tuple(List1) -> {list_to_atom(List1)}.
lists_to_atom_tuple(List1, List2) -> {list_to_atom(List1), list_to_atom(List2)}.
lists_to_atom_tuple(List1, List2, List3) ->
    {list_to_atom(List1), list_to_atom(List2), list_to_atom(List3)}.
lists_to_atom_tuple(List1, List2, List3, undefined) ->
    {list_to_atom(List1), list_to_atom(List2), list_to_atom(List3)};
lists_to_atom_tuple(List1, List2, List3, List4) ->
    {list_to_atom(List1), list_to_atom(List2), list_to_atom(List3), list_to_atom(List4)}.

nth(Index, List, Default) ->
    case List of
        L when length(L) >= Index -> lists:nth(Index, List);
        _ -> Default
    end.
nth(Index, List) ->
    case List of
        L when length(L) >= Index -> lists:nth(Index, List);
        _ -> undefined
    end.
elem(Index, Tuple) ->
    Elem =
        case Tuple of
            L when tuple_size(L) >= Index -> element(Index, Tuple);
            _ -> undefined
        end,
    Elem.

atom_to_list_keep_undefined(Str) ->
    case Str of
        undefined -> undefined;
        _ -> atom_to_list(Str)
    end.

concat_dot(Str1, Str2) when is_atom(Str1), is_atom(Str2) ->
    concat_dot(atom_to_list_keep_undefined(Str1), atom_to_list_keep_undefined(Str2));
concat_dot(Str1, Str2) when is_atom(Str1) ->
    concat_dot(atom_to_list(Str1), Str2);
concat_dot(Str1, Str2) ->
    case {Str1, Str2} of
        {undefined, S} -> S;
        {S, undefined} -> S;
        {A, B} -> A ++ "." ++ B
    end.

% Converts binary or list to integer, in case no valid integer has been provider a default is returned
to_int(I, _) when is_integer(I) -> I;
to_int(Bin, Default) when is_binary(Bin) ->
    try
        Int = binary_to_integer(Bin),
        Int
    catch
        error:badarg -> Default
    end;
to_int(List, Default) when is_list(List) ->
    try
        Int = list_to_integer(List),
        Int
    catch
        error:badarg -> Default
    end;
to_int(_, Default) ->
    Default.

% Converts binary or list to boolean, in case no valid boolean has been provider a default is returned
to_bool(B, _) when is_boolean(B) -> B;
to_bool(Bin, Default) when is_binary(Bin) ->
    try
        Bool = binary_to_existing_atom(Bin),
        case is_boolean(Bool) of
            true -> Bool;
            _ -> Default
        end
    catch
        error:badarg -> Default
    end;
to_bool(List, Default) when is_list(List) ->
    try
        Bool = list_to_existing_atom(List),
        case is_boolean(Bool) of
            true -> Bool;
            _ -> Default
        end
    catch
        error:badarg -> Default
    end;
to_bool(_, Default) ->
    Default.

mqtt_version_to_string([]) ->
    "";
mqtt_version_to_string(VersionList) ->
    VersionString = lists:map(
        fun(Version) ->
            case Version of
                3 -> "mqtt 3.1";
                4 -> "mqtt 3.1.1";
                5 -> "mqtt 5";
                131 -> "mqtt 3.1 (bridge)";
                132 -> "mqtt 3.1.1 (bridge)";
                _ -> "unknown"
            end
        end,
        VersionList
    ),
    string:join(VersionString, ", ").
