-module(emqttd_ws).
-behaviour(cowboy_websocket_handler).

-export([init/3]).
-export([websocket_init/3]).
-export([websocket_handle/3]).
-export([websocket_info/3]).
-export([websocket_terminate/3]).

-define(SUBPROTO, <<"mqttv3.1">>).

init({tcp, http}, _Req, _Opts) ->
    {upgrade, protocol, cowboy_websocket}.

websocket_init(_TransportName, Req, Opts) ->
    Self = self(),
    State = emqttd_fsm:init("ws peer", fun(Bin) -> send(Self, {reply, Bin}) end, Opts),
    case cowboy_req:parse_header(<<"sec-websocket-protocol">>, Req) of
        {ok, undefined, Req2} ->
            {ok, Req2, State};
        {ok, Subprotocols, Req2} ->
            case lists:member(?SUBPROTO, Subprotocols) of
                true ->
                    Req3 = cowboy_req:set_resp_header(<<"sec-websocket-protocol">>,
                                                      <<"mqttv3.1">>, Req2),
                    {ok, Req3, State};
                false ->
                    {shutdown, Req2}
            end
    end.

websocket_handle({binary, Data}, Req, FSMState) ->
    case emqttd_fsm:handle_input(Data, FSMState) of
        stop ->
            {shutdown, Req, FSMState};
        NewFSMState ->
            {ok, Req, NewFSMState}
    end;

websocket_handle(_Data, Req, State) ->
    {ok, Req, State}.

websocket_info({reply, Data}, Req, State) ->
    {reply, {binary, Data}, Req, State};
websocket_info(Info, Req, FSMState) ->
    case emqttd_fsm:handle_fsm_msg(Info, FSMState) of
        stop ->
            {shutdown, Req, FSMState};
        NewFSMState ->
            {ok, Req, NewFSMState}
    end.



websocket_terminate(_Reason, _Req, FSMState) ->
    emqttd_fsm:handle_close(FSMState),
    ok.


send(Pid, Msg) ->
    case is_process_alive(Pid) of
        true ->
            Pid ! Msg,
            ok;
        false ->
            {error, process_not_alive}
    end.
