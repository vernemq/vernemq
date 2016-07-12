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

-module(vmq_ranch).
-include("vmq_server.hrl").
-behaviour(ranch_protocol).

%% API.
-export([start_link/4]).

-export([init/4,
         loop/1]).

-export([system_continue/3]).
-export([system_terminate/4]).
-export([system_code_change/4]).

-record(st, {socket,
             buffer= <<>>,
             fsm_mod,
             fsm_state,
             proto_tag,
             pending=[],
             throttled=false}).

%% API.
start_link(Ref, Socket, Transport, Opts) ->
    Pid = proc_lib:spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
    {ok, Pid}.

init(Ref, Socket, Transport, Opts) ->
    ok = ranch:accept_ack(Ref),
    case Transport:peername(Socket) of
        {ok, Peer} ->
            NewOpts =
            case Transport of
                ranch_ssl ->
                    case proplists:get_value(use_identity_as_username, Opts, false) of
                        false ->
                            Opts;
                        true ->
                            [{preauth, vmq_ssl:socket_to_common_name(Socket)}|Opts]
                    end;
                _ ->
                    Opts
            end,
            FsmMod = proplists:get_value(fsm_mod, Opts, vmq_mqtt_fsm),
            FsmState = FsmMod:init(Peer, NewOpts),

            MaskedSocket = mask_socket(Transport, Socket),
            %% tune buffer sizes
            {ok, BufSizes} = getopts(MaskedSocket, [sndbuf, recbuf, buffer]),
            BufSize = lists:max([Sz || {_, Sz} <- BufSizes]),
            setopts(MaskedSocket, [{buffer, BufSize}]),

            %% start accepting messages
            active_once(MaskedSocket),
            process_flag(trap_exit, true),
            _ = vmq_metrics:incr_socket_open(),
            loop(#st{socket=MaskedSocket,
                     fsm_state=FsmState,
                     fsm_mod=FsmMod,
                     proto_tag=proto_tag(Transport)});
        {error, Reason} ->
            lager:debug("Could not get socket peername: ~p", [Reason]),
            %% It's not really "ok", but there's no reason for the
            %% listener to crash just because this socket had an
            %% error.
            %%
            %% not going through teardown, because no session was initialized
            ok
    end.

mask_socket(ranch_tcp, Socket) -> Socket;
mask_socket(ranch_ssl, Socket) -> {ssl, Socket}.

loop(State) ->
    loop_(State).

loop_(#st{pending=[]} = State) ->
    receive
        M ->
            loop_(handle_message(M, State))
    end;
loop_(#st{} = State) ->
    receive
        M ->
            loop_(handle_message(M, State))
    after
        0 ->
            loop_(internal_flush(State))
    end;
loop_({exit, Reason, State}) ->
    _ = internal_flush(State),
    teardown(State, Reason).

teardown(#st{socket = Socket}, Reason) ->
    case Reason of
        normal ->
            lager:debug("[~p] session normally stopped", [self()]);
        shutdown ->
            lager:debug("[~p] session stopped due to shutdown", [self()]);
        _ ->
            lager:warning("[~p] session stopped abnormally due to '~p'", [self(), Reason])
    end,
    _ = vmq_metrics:incr_socket_close(),
    close(Socket),
    ok.

close({ssl, Socket}) ->
    ssl:close(Socket);
close(Socket) ->
    inet:close(Socket).

proto_tag(ranch_tcp) -> {tcp, tcp_closed, tcp_error};
proto_tag(ranch_ssl) -> {ssl, ssl_closed, ssl_error}.

active_once({ssl, Socket}) ->
    ssl:setopts(Socket, [{active, once}]);
active_once(Socket) ->
    inet:setopts(Socket, [{active, once}]).

getopts({ssl, Socket}, Opts) ->
    ssl:getopts(Socket, Opts);
getopts(Socket, Opts) ->
    inet:getopts(Socket, Opts).

setopts({ssl, Socket}, Opts) ->
    ssl:setopts(Socket, Opts);
setopts(Socket, Opts) ->
    inet:setopts(Socket, Opts).

handle_message({Proto, _, Data}, #st{proto_tag={Proto, _, _}, fsm_mod=FsmMod} = State) ->
    #st{fsm_state=FsmState0,
        socket=Socket,
        pending=Pending,
        buffer=Buffer} = State,
    NrOfBytes = byte_size(Data),
    _ = vmq_metrics:incr_bytes_received(NrOfBytes),
    case FsmMod:data_in(<<Buffer/binary, Data/binary>>, FsmState0) of
        {ok, FsmState1, Rest, Out} ->
            case active_once(Socket) of
                ok ->
                    maybe_flush(State#st{fsm_state=FsmState1,
                                         pending=[Pending|Out],
                                         buffer=Rest});
                {error, Reason} ->
                    {exit, Reason, State#st{pending=[Pending|Out],
                                               fsm_state=FsmState1}}
            end;
        {stop, Reason, Out} ->
            {exit, Reason, State#st{pending=[Pending|Out]}};
        {throttle, FsmState1, Rest, Out} ->
            erlang:send_after(1000, self(), restart_work),
            maybe_flush(State#st{fsm_state=FsmState1,
                                 pending=[Pending|Out],
                                 throttled=true,
                                 buffer=Rest});
        {error, Reason, Out} ->
            lager:debug("[~p][~p] parse error '~p' for data: ~p and  parser state: ~p",
                        [Proto, self(), Reason, Data, Buffer]),
            {exit, Reason, State#st{pending=[Pending|Out]}}
    end;
handle_message({ProtoClosed, _}, #st{proto_tag={_, ProtoClosed, _}, fsm_mod=FsmMod} = State) ->
    %% we regard a tcp_closed as 'normal'
    _ = FsmMod:msg_in(disconnect, State#st.fsm_state),
    {exit, normal, State};
handle_message({ProtoErr, _, Error}, #st{proto_tag={_, _, ProtoErr}} = State) ->
    _ = vmq_metrics:incr_socket_error(),
    {exit, Error, State};
handle_message({FsmMod, Msg}, #st{pending=Pending, fsm_state=FsmState0, fsm_mod=FsmMod} = State) ->
    case FsmMod:msg_in(Msg, FsmState0) of
        {ok, FsmState1, Out} ->
            maybe_flush(State#st{fsm_state=FsmState1,
                                 pending=[Pending|Out]});
        {stop, Reason, Out} ->
            {exit, Reason, State#st{pending=[Pending|Out]}}
    end;
handle_message({inet_reply, _, ok}, State) ->
    State;
handle_message({inet_reply, _, Status}, State) ->
    {exit, {send_failed, Status}, State};
handle_message(restart_work, #st{throttled=true} = State) ->
    #st{proto_tag={Proto, _, _}, socket=Socket} = State,
    handle_message({Proto, Socket, <<>>}, State#st{throttled=false});
handle_message({'EXIT', _Parent, Reason}, #st{fsm_state=FsmState0, fsm_mod=FsmMod} = State) ->
    _ = FsmMod:msg_in(disconnect, FsmState0),
    {exit, Reason, State};
handle_message({system, From, Request}, State) ->
    %% Not sure if passing the parent as undefined is really allowed,
    %% but process state inspection at least seem to work fine.
    Parent = undefined,
    sys:handle_system_msg(Request, From, Parent, ?MODULE, [], State);
handle_message(OtherMsg, #st{fsm_state=FsmState0, fsm_mod=FsmMod, pending=Pending} = State) ->
    case FsmMod:msg_in(OtherMsg, FsmState0) of
        {ok, FsmState1, Out} ->
            maybe_flush(State#st{fsm_state=FsmState1,
                                 pending=[Pending|Out]});
        {stop, Reason, Out} ->
            {exit, Reason, State#st{pending=[Pending|Out]}}
    end.

%% This magic number is the tcp-over-ethernet MSS (1460)
%% The idea is that we want to flush just before exceeding the MSS.
-define(FLUSH_THRESHOLD, 1456).
maybe_flush(#st{pending=Pending} = State) ->
    case iolist_size(Pending) >= ?FLUSH_THRESHOLD of
        true ->
            internal_flush(State);
        false ->
            State
    end.

internal_flush(#st{pending=Pending, socket=Socket} = State) ->
    case iolist_size(Pending) of
        0 -> State#st{pending=[]};
        NrOfBytes ->
            case port_cmd(Socket, Pending) of
                ok ->
                    _ = vmq_metrics:incr_bytes_sent(NrOfBytes),
                    State#st{pending=[]};
                {error, Reason} ->
                    {exit, Reason, State}
            end
    end.

%% gen_tcp:send/2 does a selective receive of {inet_reply, Sock,
%% Status} to obtain the result. That is bad when it is called from
%% the writer since it requires scanning of the writers possibly quite
%% large message queue.
%%
%% So instead we lift the code from prim_inet:send/2, which is what
%% gen_tcp:send/2 calls, do the first half here and then just process
%% the result code in handle_message/3 as and when it arrives.
%%
%% This means we may end up happily sending data down a closed/broken
%% socket, but that's ok since a) data in the buffers will be lost in
%% any case (so qualitatively we are no worse off than if we used
%% gen_tcp:send/2), and b) we do detect the changed socket status
%% eventually, i.e. when we get round to handling the result code.
%%
%% Also note that the port has bounded buffers and port_command blocks
%% when these are full. So the fact that we process the result
%% asynchronously does not impact flow control.
port_cmd(Socket, Data) ->
    try
        port_cmd_(Socket, Data),
        ok
    catch
        error:Error ->
            {error, Error}
    end.

port_cmd_({ssl, Socket}, Data) ->
    case ssl:send(Socket, Data) of
        ok ->
            self() ! {inet_reply, Socket, ok},
            true;
        {error, Reason} ->
            erlang:error(Reason)
    end;
port_cmd_(Socket, Data) ->
    erlang:port_command(Socket, Data).

system_continue(_, _, State) ->
	loop(State).

-spec system_terminate(any(), _, _, _) -> no_return().
system_terminate(Reason, _, _, State) ->
	teardown(State, Reason).

system_code_change(Misc, _, _, _) ->
	{ok, Misc}.
