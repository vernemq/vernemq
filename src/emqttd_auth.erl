-module(emqttd_auth).
-export([authenticate/4]).

authenticate(SrcIp, ClientId, User, Password) ->
    io:format("auth client ~p from ~p with username ~p and password ~p~n", [ClientId, SrcIp, User, Password]),
    %% return {error, unknown} ---> other auth providers are tried
    %% return {error, not_authorized} --> error msg is sent to client
    %% return {error, invalid_credentials} --> error msg is sent to client
    %% return ok --> success msg is sent to client
    ok.

