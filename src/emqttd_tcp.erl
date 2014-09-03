-module(emqttd_tcp).
-behaviour(ranch_protocol).

-export([start_link/4]).
-export([init/4]).

start_link(Ref, Socket, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
    {ok, Pid}.

init(Ref, Socket, Transport, Opts) ->
    ok = ranch:accept_ack(Ref),
    ok = Transport:setopts(Socket, [{nodelay, true},
                                    {packet, raw},
                                    {active, once}]),
    {ok, Peer} = Transport:peername(Socket),
    loop(Socket, Transport, emqttd_fsm:init(Peer, fun(Bin) ->
                                                    Transport:send(Socket, Bin)
                                            end, Opts)).

loop(Socket, Transport, stop) ->
    Transport:close(Socket);
loop(Socket, Transport, FSMState) ->
    Transport:setopts(Socket, [{active, once}]),
    receive
        {tcp, Socket, Data} ->
            loop(Socket, Transport,
                 emqttd_fsm:handle_input(Data, FSMState));
        {ssl, Socket, Data} ->
            loop(Socket, Transport,
                 emqttd_fsm:handle_input(Data, FSMState));
        {tcp_closed, Socket} ->
            emqttd_fsm:handle_close(FSMState);
        {ssl_closed, Socket} ->
            emqttd_fsm:handle_close(FSMState);
        {tcp_error, Socket, Reason} ->
            emqttd_fsm:handle_error(Reason, FSMState);
        {ssl_error, Socket, Reason} ->
            emqttd_fsm:handle_error(Reason, FSMState);
        Msg ->
            loop(Socket, Transport,
                 emqttd_fsm:handle_fsm_msg(Msg, FSMState))
    end.


