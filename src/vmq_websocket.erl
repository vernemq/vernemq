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
-export([init/3]).
-export([websocket_init/3]).
-export([websocket_handle/3]).
-export([websocket_info/3]).
-export([websocket_terminate/3]).

-record(st, {buffer= <<>>,
             fsm_mod,
             fsm_state,
             bytes_recv={os:timestamp(), 0},
             bytes_sent={os:timestamp(), 0}}).

-define(SUPPORTED_PROTOCOLS, [<<"mqttv3.1">>, <<"mqtt">>]).


init(_Type, Req, Opts) ->
    {upgrade, protocol, cowboy_websocket, Req, Opts}.

websocket_init(_Type, Req, Opts) ->
    case cowboy_req:parse_header(<<"sec-websocket-protocol">>, Req) of
        {undefined, Req2} ->
            init_(Req2, Opts);
        {ok, [SubProtocol], Req2} ->
            case lists:member(SubProtocol, ?SUPPORTED_PROTOCOLS) of
                true ->
                    Req3 = cowboy_req:set_resp_header(<<"sec-websocket-protocol">>, SubProtocol, Req2),
                    init_(Req3, Opts);
                false ->
                    {shutdown, Req2}
            end
    end.

init_(Req, Opts) ->
    {Peer, Req1} = cowboy_req:peer(Req),
    FsmMod = proplists:get_value(fsm_mod, Opts, vmq_mqtt_fsm),
    FsmState = FsmMod:init(Peer, Opts),
    _ = vmq_metrics:incr_socket_open(),
    {ok, Req1, #st{fsm_state=FsmState, fsm_mod=FsmMod}}.

websocket_handle({binary, Data}, Req, State) ->
    #st{fsm_state=FsmState0,
        fsm_mod=FsmMod,
        buffer=Buffer} = State,
    NrOfBytes = byte_size(Data),
    _ = vmq_metrics:incr_bytes_received(NrOfBytes),
    handle_fsm_return(
      FsmMod:data_in(<<Buffer/binary, Data/binary>>, FsmState0),
      Req, State);
websocket_handle(_Data, Req, State) ->
    {ok, Req, State}.

websocket_info({?MODULE, terminate}, Req, State) ->
    {shutdown, Req, State};
websocket_info({FsmMod, Msg}, Req, #st{fsm_mod=FsmMod, fsm_state=FsmState} = State) ->
    handle_fsm_return(FsmMod:msg_in(Msg, FsmState), Req, State);
websocket_info(_Info, Req, State) ->
    {ok, Req, State}.

websocket_terminate(_Reason, _Req, #st{fsm_state=terminated}) ->
    _ = vmq_metrics:incr_socket_close(),
    ok;
websocket_terminate(_Reason, _Req, #st{fsm_mod=FsmMod, fsm_state=FsmState}) ->
    _ = FsmMod:msg_in(disconnect, FsmState),
    _ = vmq_metrics:incr_socket_close(),
    ok.

handle_fsm_return({ok, FsmState, Rest, Out}, Req, State) ->
    maybe_reply(Out, Req, State#st{fsm_state=FsmState, buffer=Rest});
handle_fsm_return({throttle, FsmState, Rest, Out}, Req, State) ->
    timer:sleep(1000),
    maybe_reply(Out, Req, State#st{fsm_state=FsmState, buffer=Rest});
handle_fsm_return({ok, FsmState, Out}, Req, State) ->
    maybe_reply(Out, Req, State#st{fsm_state=FsmState});
handle_fsm_return({stop, normal, Out}, Req, State) ->
    lager:debug("[~p] ws session normally stopped", [self()]),
    self() ! {?MODULE, terminate},
    maybe_reply(Out, Req, State#st{fsm_state=terminated});
handle_fsm_return({stop, shutdown, Out}, Req, State) ->
    lager:debug("[~p] ws session stopped due to shutdown", [self()]),
    self() ! {?MODULE, terminate},
    maybe_reply(Out, Req, State#st{fsm_state=terminated});
handle_fsm_return({stop, Reason, Out}, Req, State) ->
    lager:warning("[~p] ws session stopped abnormally due to '~p'", [self(), Reason]),
    self() ! {?MODULE, terminate},
    maybe_reply(Out, Req, State#st{fsm_state=terminated});
handle_fsm_return({error, Reason, Out}, Req, State) ->
    lager:warning("[~p] ws session error, force terminate due to '~p'", [self(), Reason]),
    self() ! {?MODULE, terminate},
    maybe_reply(Out, Req, State#st{fsm_state=terminated}).

maybe_reply(Out, Req, State) ->
    case iolist_size(Out) of
        0 ->
            {ok, Req, State};
        NrOfBytes ->
            _ = vmq_metrics:incr_bytes_sent(NrOfBytes),
            {reply, {binary, Out}, Req, State}
    end.
