-module(vmq_ws).
-behaviour(cowboy_websocket_handler).
-include_lib("emqtt_commons/include/emqtt_frame.hrl").

-export([init/3]).
-export([websocket_init/3]).
-export([websocket_handle/3]).
-export([websocket_info/3]).
-export([websocket_terminate/3]).

-define(SUBPROTO, <<"mqttv3.1">>).

-record(st, {session,
             session_monitor,
             parser_state=emqtt_frame:initial_state(),
             buffer= <<>>}).

-spec init({'tcp','http'},_,_) -> {'upgrade','protocol','cowboy_websocket'}.
init({tcp, http}, _Req, _Opts) ->
    {upgrade, protocol, cowboy_websocket}.

-spec websocket_init(_,cowboy_req:req(),maybe_improper_list()) -> {'shutdown',cowboy_req:req()} | {'ok',cowboy_req:req(),'stop' | {_,_}}.
websocket_init(_TransportName, Req, Opts) ->

    case cowboy_req:parse_header(<<"sec-websocket-protocol">>, Req) of
        {ok, undefined, Req2} ->
            {SessionPid, MRef} = start_session(Req2, Opts),
            {ok, Req2, #st{session=SessionPid, session_monitor=MRef}};
        {ok, Subprotocols, Req2} ->
            case lists:member(?SUBPROTO, Subprotocols) of
                true ->
                    Req3 = cowboy_req:set_resp_header(<<"sec-websocket-protocol">>,
                                                      <<"mqttv3.1">>, Req2),
                    {SessionPid, MRef} = start_session(Req3, Opts),
                    {ok, Req3, #st{session=SessionPid, session_monitor=MRef}};
                false ->
                    {shutdown, Req2}
            end
    end.

-spec websocket_handle({binary, binary()} | any(), cowboy_req:req(), any()) -> {ok | shutdown, cowboy_req:req(), any()}.
websocket_handle({binary, Data}, Req, #st{session=SessionPid, buffer=Buffer,
                                             parser_state=ParserState} = State) ->
    NewParserState = process_bytes(SessionPid, <<Buffer/binary, Data/binary>>, ParserState),
    vmq_systree:incr_bytes_received(byte_size(Data)),
    {ok, Req, State#st{parser_state=NewParserState}};

websocket_handle(_Data, Req, State) ->
    {ok, Req, State}.

-spec websocket_info({reply, binary()} | any(), cowboy_req:req(), any()) -> {ok | shutdown, cowboy_req:req(), any()}.
websocket_info({reply, Frame}, Req, State) ->
    Data =
    case is_binary(Frame) of
        true ->
            Frame;
        false ->
            emqtt_frame:serialise(Frame)
    end,
    vmq_systree:incr_bytes_sent(byte_size(Data)),
    {reply, {binary, Data}, Req, State};
websocket_info({'DOWN', _, process, Pid, Reason}, Req, State) ->
    %% session stopped
    lager:info("[~p] stop websocket session due to ~p", [Pid, Reason]),
    {shutdown, Req, State#st{session_monitor=undefined}}.

-spec websocket_terminate(_, cowboy_req:req(), any()) -> ok.
websocket_terminate(_Reason, _Req, #st{session=SessionPid, session_monitor=MRef}) ->
    case MRef of
        undefined -> ok;
        _ -> demonitor(MRef, [flush])
    end,
    case is_process_alive(SessionPid) of
        true ->
            vmq_session:stop(SessionPid);
        false ->
            ok
    end,
    ok.


start_session(Req, Opts) ->
    Self = self(),
    {Peer, _} = cowboy_req:peer(Req),
    {ok, SessionPid} = vmq_session:start(self(), Peer,
                              fun(Frame) ->
                                        send(Self, {reply, Frame})
                                end, Opts),
    MRef = monitor(process, SessionPid),
    vmq_systree:incr_socket_count(),
    {SessionPid, MRef}.

-spec send(pid(),{'reply',_}) -> 'ok' | {'error','process_not_alive'}.
send(Pid, Msg) ->
    case is_process_alive(Pid) of
        true ->
            Pid ! Msg,
            ok;
        false ->
            {error, process_not_alive}
    end.

process_bytes(SessionPid, Bytes, ParserState) ->
    case emqtt_frame:parse(Bytes, ParserState) of
        {more, NewParserState} ->
            NewParserState;
        {ok, #mqtt_frame{} = Frame, Rest} ->
            vmq_systree:incr_messages_received(),
            vmq_session:in(SessionPid, Frame),
            process_bytes(SessionPid, Rest, emqtt_frame:initial_state());
        {error, Reason} ->
            io:format("parse error ~p~n", [Reason]),
            emqtt_frame:initial_state()
    end.
