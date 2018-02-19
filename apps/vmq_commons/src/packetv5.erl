-module(packetv5).
-include_lib("vmq_commons/include/vmq_types_mqtt5.hrl").
-export([do_client_connect/2,
         receive_frame/1,
         receive_frame/3]).

do_client_connect(ConnectPacket, Opts) ->
    Host = proplists:get_value(hostname, Opts, "localhost"),
    Port = proplists:get_value(port, Opts, 1888),
    Timeout = proplists:get_value(timeout, Opts, 60000),
    Transport = proplists:get_value(transport, Opts, gen_tcp),
    case Transport:connect(Host, Port, [binary, {reuseaddr, true},{active, false}, {packet, raw}], Timeout) of
        {ok, Socket} ->
            Transport:send(Socket, ConnectPacket),
            case receive_frame(Socket) of
                {ok, Frame, Rest} ->
                    {ok, Socket, Frame, Rest};
                E ->
                    Transport:close(Socket),
                    E
            end
    end.

receive_frame(Socket) ->
    receive_frame(gen_tcp, Socket).
receive_frame(Transport, Socket) ->
    receive_frame(Transport, Socket, 60000).
receive_frame(Transport, Socket, Timeout) ->
    receive_frame_(Transport, Socket, Timeout, <<>>).

receive_frame_(Transport, Socket, Timeout, Incomplete) ->
    case Transport:recv(Socket, 0, Timeout) of
        {ok, Data} ->
            NewData = <<Incomplete/binary, Data/binary>>,
            case vmq_parser_mqtt5:parse(NewData) of
                more ->
                    receive_frame_(Transport, Socket, Timeout, NewData);
                {error, R} ->
                    {error, R};
                {Frame, Rest} ->
                    {ok, Frame, Rest}
            end;
        E -> E
    end.

