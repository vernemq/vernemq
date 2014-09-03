-module(emqttd_test_msg_log_handler).

-export([handle/4]).

handle(_ConnPid, _ClientId, _Topic, _Payload) ->
    %io:format("-- ~p ~p ~p ~p~n", [ConnPid, ClientId, Topic, Payload]).
    ok.
