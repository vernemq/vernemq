-module(emqttd_test_msg_log_handler).

-export([handle/4]).

handle(ConnPid, ClientId, Topic, Payload) ->
    io:format("-- ~p ~p ~p ~p~n", [ConnPid, ClientId, Topic, Payload]).
