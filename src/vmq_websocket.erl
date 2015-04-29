%% Copyright 2014 Erlio GmbH Basel Switzerland (http://erl.io)
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(vmq_websocket).
-export([init/2]).
-export([websocket_handle/3]).
-export([websocket_info/3]).

-record(st, {buffer= <<>>,
             parser_state,
             session,
             cpulevel=0,
             bytes_recv={os:timestamp(), 0},
             bytes_send={os:timestamp(), 0}}).

-define(SUPPORTED_PROTOCOLS, [<<"mqttv3.1">>, <<"mqtt">>]).

init(Req, Opts) ->
    case cowboy_req:parse_header(<<"sec-websocket-protocol">>, Req) of
        undefined ->
            init_(Req, Opts);
        [SubProtocol] ->
            case lists:member(SubProtocol, ?SUPPORTED_PROTOCOLS) of
                true ->
                    Req2 = cowboy_req:set_resp_header(<<"sec-websocket-protocol">>, <<"mqttv3.1">>, Req),
                    init_(Req2, Opts);
                false ->
                    {stop, Req, undefined}
            end
    end.

init_(Req, Opts) ->
    Peer = cowboy_req:peer(Req),
    Self = self(),
    SendFun = fun(F) -> send(Self, F), ok end,
    {ok, SessionPid} = vmq_session:start_link(Peer, SendFun, Opts),
    process_flag(trap_exit, true),
    _ = vmq_exo:incr_socket_count(),
    {cowboy_websocket, Req, #st{session=SessionPid}}.

websocket_handle({binary, Bytes}, Req, State) ->
    #st{session=SessionPid,
        parser_state=ParserState,
        cpulevel=CpuLevel,
        bytes_recv={TS, V}} = State,
    case process_bytes(SessionPid, Bytes, ParserState) of
        {ok, NewParserState} ->
            {M, S, _} = TS,
            NrOfBytes = byte_size(Bytes),
            BytesRecvLastSecond = V + NrOfBytes,
            {NewCpuLevel, NewBytesRecv} =
            case os:timestamp() of
                {M, S, _} = NewTS ->
                    {CpuLevel, {NewTS, BytesRecvLastSecond}};
                NewTS ->
                    _ = vmq_exo:incr_bytes_received(BytesRecvLastSecond),
                    {vmq_sysmon:cpu_load_level(), {NewTS, 0}}
            end,
            {ok, Req, maybe_throttle(State#st{parser_state=NewParserState,
                                              bytes_recv=NewBytesRecv,
                                              cpulevel=NewCpuLevel})};
        {throttled, HoldBackBuf} ->
            timer:sleep(1000),
            websocket_handle({binary, HoldBackBuf}, Req,
                             State#st{parser_state= <<>>});
        error ->
            {stop, Req, State}
    end;

websocket_handle(_Data, Req, State) ->
    {ok, Req, State}.

websocket_info({send, Bin}, Req, State) ->
    {reply, {binary, Bin}, Req, State};
websocket_info({send_frames, Frames}, Req, #st{bytes_send={{M, S, _}, V}} = State) ->
    Data = lists:foldl(fun(Frame, Acc) ->
                               Bin = vmq_parser:serialise(Frame),
                               [Bin|Acc]
                       end, [], Frames),
    NrOfBytes = iolist_size(Data),
    NewBytesSend =
    case os:timestamp() of
        {M, S, _} = TS ->
            {TS, V + NrOfBytes};
        TS ->
            _ = vmq_exo:incr_bytes_sent(V + NrOfBytes),
            {TS, 0}
    end,
    {reply, {binary, lists:reverse(Data)}, Req, State#st{bytes_send=NewBytesSend}};

websocket_info({'EXIT', _, Reason}, Req, #st{session=SessionPid} = State) ->
    case Reason of
        normal ->
            lager:debug("[~p] ws session normally stopped", [SessionPid]);
        shutdown ->
            lager:debug("[~p] ws session stopped due to shutdown", [SessionPid]);
        _ ->
            lager:warning("[~p] ws session stopped abnormally due to ~p", [SessionPid, Reason])
    end,
    {shutdown, Req, State};
websocket_info(_Info, Req, State) ->
    {ok, Req, State}.


maybe_throttle(#st{cpulevel=1} = State) ->
    timer:sleep(10),
    State;
maybe_throttle(#st{cpulevel=2} = State) ->
    timer:sleep(20),
    State;
maybe_throttle(#st{cpulevel=L} = State) when L > 2->
    timer:sleep(100),
    State;
maybe_throttle(State) ->
    State.


send(TransportPid, Bin) when is_binary(Bin) ->
    TransportPid ! {send, Bin},
    ok;
send(TransportPid, [F|_] = Frames) when is_tuple(F) ->
    TransportPid ! {send_frames, Frames},
    ok;
send(TransportPid, Frame) when is_tuple(Frame) ->
    TransportPid ! {send_frames, [Frame]},
    ok.

process_bytes(SessionPid, Bytes, undefined) ->
    process_bytes(SessionPid, Bytes, <<>>);
process_bytes(SessionPid, Bytes, ParserState) ->
    NewParserState = <<ParserState/binary, Bytes/binary>>,
    case vmq_parser:parse(NewParserState) of
        more ->
            {ok, NewParserState};
        {error, _} ->
            error;
        {Frame, Rest} ->
            Ret = vmq_session:in(SessionPid, Frame),
            case Ret of
                throttle ->
                    {throttled, Rest};
                _ ->
                    process_bytes(SessionPid, Rest, <<>>)
            end;
        error ->
            error
    end.
