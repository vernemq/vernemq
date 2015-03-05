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
             bytes_recv={os:timestamp(), 0},
             bytes_send={os:timestamp(), 0}}).

init(Req, Opts) ->
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
        bytes_recv={{M, S, _}, V}} = State,
    NewParserState = process_bytes(SessionPid, Bytes, ParserState),
    NrOfBytes = byte_size(Bytes),
    NewBytesRecv =
    case os:timestamp() of
        {M, S, _} = NewTS ->
            {NewTS, V + NrOfBytes};
        NewTS ->
            _ = vmq_exo:incr_bytes_received(V + NrOfBytes),
            {NewTS, 0}
    end,
    {ok, Req, State#st{parser_state=NewParserState, bytes_recv=NewBytesRecv}};

websocket_handle(_Data, Req, State) ->
    {ok, Req, State}.

websocket_info({send, Bin}, Req, State) ->
    {reply, {binary, Bin}, Req, State};
websocket_info({send_frames, Frames}, Req, #st{bytes_send={{M, S, _}, V}} = State) ->
    Data = lists:foldl(fun(Frame, Acc) ->
                               Bin = emqtt_frame:serialise(Frame),
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
    process_bytes(SessionPid, Bytes, emqtt_frame:initial_state());
process_bytes(SessionPid, Bytes, ParserState) ->
    case emqtt_frame:parse(Bytes, ParserState) of
        {more, NewParserState} ->
            NewParserState;
        {ok, Frame, Rest} ->
            vmq_session:in(SessionPid, Frame),
            process_bytes(SessionPid, Rest, emqtt_frame:initial_state());
        {error, _Reason} ->
            emqtt_frame:initial_state()
    end.
