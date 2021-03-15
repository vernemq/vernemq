%% -*- erlang -*-
main([ThisNode]) ->
    code:add_paths(filelib:wildcard("/usr/lib/vernemq/lib/*/ebin")),
    FileName = "/var/lib/vernemq/meta/peer_service/cluster_state",
    case filelib:is_regular(FileName) of
        true ->
            {ok, Bin} = file:read_file(FileName),
            {ok, State} = riak_dt_orswot:from_binary(Bin),
            AThisNode = list_to_atom(ThisNode),
            TargetNodes = riak_dt_orswot:value(State) -- [AThisNode],
            L = lists:foldl(
                  fun(N, Acc) ->
                          Acc ++ atom_to_list(N) ++ "\n" 
                  end, "", TargetNodes),
            io:format(L);
        false ->
            io:format("")
    end.
