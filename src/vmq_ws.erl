-module(vmq_ws).
-behaviour(cowboy_websocket_handler).

-export([init/3]).
-export([websocket_init/3]).
-export([websocket_handle/3]).
-export([websocket_info/3]).
-export([websocket_terminate/3]).

-define(SUBPROTO, <<"mqttv3.1">>).

-spec init({'tcp','http'},_,_) -> {'upgrade','protocol','cowboy_websocket'}.
init({tcp, http}, _Req, _Opts) ->
    {upgrade, protocol, cowboy_websocket}.

-spec websocket_init(_,cowboy_req:req(),maybe_improper_list()) -> {'shutdown',cowboy_req:req()} | {'ok',cowboy_req:req(),'stop' | {_,_}}.
websocket_init(_TransportName, Req, Opts) ->
    Self = self(),
    {Ip, _Port} = cowboy_req:peer(Req),
    State = vmq_fsm:init(Ip, fun(Bin) ->
                                        vmq_systree:incr_bytes_sent(byte_size(Bin)),
                                        send(Self, {reply, Bin})
                                end, Opts),
    case cowboy_req:parse_header(<<"sec-websocket-protocol">>, Req) of
        {ok, undefined, Req2} ->
            vmq_systree:incr_socket_count(),
            {ok, Req2, State};
        {ok, Subprotocols, Req2} ->
            case lists:member(?SUBPROTO, Subprotocols) of
                true ->
                    Req3 = cowboy_req:set_resp_header(<<"sec-websocket-protocol">>,
                                                      <<"mqttv3.1">>, Req2),
                    vmq_systree:incr_socket_count(),
                    {ok, Req3, State};
                false ->
                    {shutdown, Req2}
            end
    end.

-spec websocket_handle({binary, binary()} | any(), cowboy_req:req(), any()) -> {ok | shutdown, cowboy_req:req(), any()}.
websocket_handle({binary, Data}, Req, FSMState) ->
    vmq_systree:incr_bytes_received(byte_size(Data)),
    case vmq_fsm:handle_input(Data, FSMState) of
        stop ->
            {shutdown, Req, FSMState};
        NewFSMState ->
            {ok, Req, NewFSMState}
    end;

websocket_handle(_Data, Req, State) ->
    {ok, Req, State}.

-spec websocket_info({reply, binary()} | any(), cowboy_req:req(), any()) -> {ok | shutdown, cowboy_req:req(), any()}.
websocket_info({reply, Data}, Req, State) ->
    {reply, {binary, Data}, Req, State};
websocket_info(Info, Req, FSMState) ->
    case vmq_fsm:handle_fsm_msg(Info, FSMState) of
        stop ->
            {shutdown, Req, FSMState};
        NewFSMState ->
            {ok, Req, NewFSMState}
    end.



-spec websocket_terminate(_, cowboy_req:req(), any()) -> ok.
websocket_terminate(_Reason, _Req, FSMState) ->
    vmq_fsm:handle_close(FSMState),
    vmq_systree:decr_socket_count(),
    ok.


-spec send(pid(),{'reply',_}) -> 'ok' | {'error','process_not_alive'}.
send(Pid, Msg) ->
    case is_process_alive(Pid) of
        true ->
            Pid ! Msg,
            ok;
        false ->
            {error, process_not_alive}
    end.
