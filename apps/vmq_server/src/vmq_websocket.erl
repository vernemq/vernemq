%% Copyright 2018 Erlio GmbH Basel Switzerland (http://erl.io)
%% Copyright 2018-2024 Octavo Labs/VerneMQ (https://vernemq.com/)
%% and Individual Contributors.
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
-behaviour(cowboy_websocket).

-include("vmq_server.hrl").
-include_lib("kernel/include/logger.hrl").

-define(SEC_WEBSOCKET_PROTOCOL, <<"sec-websocket-protocol">>).

-export([init/2]).

-export([
    websocket_init/1,
    websocket_handle/2,
    websocket_info/2,
    terminate/3
]).

-export([add_socket/2]).

-record(state, {
    buffer = <<>>,
    fsm_mod,
    fsm_state,
    type,
    socket,
    peer,
    bytes_recv = {os:timestamp(), 0},
    bytes_sent = {os:timestamp(), 0}
}).

-define(TO_SESSION, to_session_fsm).
-define(SUPPORTED_PROTOCOLS, [<<"mqttv3.1">>, <<"mqtt">>]).

init(Req, Opts) ->
    Type = proplists:get_value(type, Opts),
    case add_websocket_sec_header(Req) of
        {ok, Req0} ->
            ProxyInfo = maps:find(proxy_header, Req0),
            ProxyInfo0 =
                case ProxyInfo of
                    error -> error;
                    {ok, PI} -> PI
                end,
            Peer =
                case ProxyInfo0 of
                    % with proxy protocol, but 'local'
                    #{command := local, version := _} ->
                        cowboy_req:peer(Req0);
                    #{
                        src_address := SrcAddr,
                        src_port := SrcPort
                    } ->
                        {SrcAddr, SrcPort};
                    % WS request without proxy_protocol (might have XFF)
                    error ->
                        XFF_On = proplists:get_value(xff_proxy, Opts, false),
                        case XFF_On of
                            true ->
                                {ok, NewPeer} = vmq_proxy_xff:new_peer(
                                    Req0,
                                    proplists:get_value(proxy_xff_trusted_intermediate, Opts, "")
                                ),
                                NewPeer;
                            _ ->
                                cowboy_req:peer(Req0)
                        end
                end,
            FsmMod = proplists:get_value(fsm_mod, Opts, vmq_mqtt_pre_init),
            FsmState =
                case Type of
                    mqttwss ->
                        Cert = cowboy_req:cert(Req),
                        Opts1 =
                            case proplists:get_value(forward_connection_opts, Opts, false) of
                                true -> [{conn_opts, #{client_cert => Cert}} | Opts];
                                false -> Opts
                            end,
                        case proplists:get_value(use_identity_as_username, Opts, false) of
                            false ->
                                FsmMod:init(Peer, Opts1);
                            true ->
                                FsmMod:init(Peer, [
                                    {preauth, vmq_ssl:cert_to_common_name(Cert)} | Opts1
                                ])
                        end;
                    %mqttws
                    _ ->
                        case proplists:get_value(proxy_protocol_use_cn_as_username, Opts, false) of
                            false ->
                                % No proxy protocol but we still might have a x-forwarded CN
                                RequireXFFCN = proplists:get_value(
                                    xff_use_cn_as_username, Opts, false
                                ),
                                XFF_On1 = proplists:get_value(xff_proxy, Opts, false),
                                case RequireXFFCN of
                                    false ->
                                        FsmMod:init(Peer, Opts);
                                    true when XFF_On1 == true ->
                                        CNHeaderName = proplists:get_value(
                                            xff_cn_header, Opts, <<"x-ssl-client-cn">>
                                        ),
                                        HN = ensure_binary(CNHeaderName),
                                        case cowboy_req:header(HN, Req0) of
                                            undefined ->
                                                {vmq_cowboy_websocket, Req0,
                                                    {error, no_xff_cn_username}};
                                            <<>> ->
                                                {vmq_cowboy_websocket, Req0,
                                                    {error, no_xff_cn_username}};
                                            XFFCN ->
                                                FsmMod:init(Peer, [{preauth, XFFCN} | Opts])
                                        end;
                                    _ ->
                                        {vmq_cowboy_websocket, Req0, {error, xff_not_allowed}}
                                end;
                            true ->
                                case ProxyInfo0 of
                                    error ->
                                        FsmMod:init(Peer, Opts);
                                    % Note: as 'proxy_protocol_use_cn_as_username' historically
                                    % defaults to 'true', we do not return an error here but fall
                                    % back to the provided MQTT username.
                                    % We expected SSL information from the Proxy protocol but did not get
                                    % any.
                                    #{ssl := #{cn := CN}} ->
                                        FsmMod:init(Peer, [{preauth, CN} | Opts]);
                                    #{command := _} ->
                                        FsmMod:init(Peer, Opts)
                                end
                        end
                end,
            WsOpts0 = proplists:get_value(ws_opts, Opts, #{idle_timeout => infinity}),
            MaxFrameSize = application:get_env(vmq_server, max_ws_frame_size, 268435456),
            WsOpts = maps:merge(#{compress => true, max_frame_size => MaxFrameSize}, WsOpts0),
            {vmq_cowboy_websocket, Req0,
                #state{
                    peer = Peer,
                    fsm_state = FsmState,
                    fsm_mod = FsmMod,
                    type = Type
                },
                WsOpts};
        {error, unsupported_protocol} ->
            {vmq_cowboy_websocket, Req, {error, unsupported_protocol}}
    end.

websocket_init(#state{fsm_state = {vmq_cowboy_websocket, _, E}} = _State) ->
    ?LOG_DEBUG("websocket init error ~p~n", [E]),
    _ = vmq_metrics:incr_socket_open(),
    {stop, #state{fsm_state = terminated}};
websocket_init(State) ->
    _ = vmq_metrics:incr_socket_open(),
    {ok, State, hibernate}.

websocket_handle(_, #state{fsm_state = terminated} = State) ->
    %% handle `terminated` state as in `websocket_info/3`.
    {stop, State};
websocket_handle({binary, Data}, State) ->
    #state{
        fsm_state = FsmState0,
        fsm_mod = FsmMod,
        buffer = Buffer
    } = State,
    NrOfBytes = byte_size(Data),
    _ = vmq_metrics:incr_bytes_received(NrOfBytes),
    handle_fsm_return(
        FsmMod:data_in(<<Buffer/binary, Data/binary>>, FsmState0),
        State
    );
websocket_handle(_Data, State) ->
    {ok, State, hibernate}.

websocket_info({?MODULE, terminate}, State) ->
    {stop, State};
websocket_info(
    {set_sock_opts, Opts},
    #state{
        type = Type,
        socket = Socket
    } = State
) ->
    case Type of
        mqttws ->
            inet:setopts(Socket, Opts);
        mqttwss ->
            ssl:setopts(Socket, Opts)
    end,
    {ok, State, hibernate};
websocket_info({?TO_SESSION, _}, #state{fsm_state = terminated} = State) ->
    % We got an intermediate message before retrieving {?MODULE, terminate}.
    %
    % The reason for this is that cowboy doesn't provide an equivalent to
    % the gen_server {stop, Reason, Reply, State} which would enable to
    % terminate the websocket session but previously send out some final
    % bytes over the socket.
    %
    % In order to overcome this limitation, we don't immediately return a
    % {shutdown, Req, State} when we handle a {stop, Reason, Reply} message
    % but send the date out and transition into `fsm_state=terminated`. To
    % finally shutdown the session we send a {?MODULE, terminate} message
    % to ourself that is handled here.
    {shutdown, State};
websocket_info({?TO_SESSION, Msg}, #state{fsm_mod = FsmMod, fsm_state = FsmState} = State) ->
    handle_fsm_return(FsmMod:msg_in(Msg, FsmState), State);
websocket_info(_Info, State) ->
    {ok, State, hibernate}.

terminate(_Reason, _Req, #state{fsm_state = terminated}) ->
    _ = vmq_metrics:incr_socket_close(),
    ok;
terminate(_Reason, _Req, #state{fsm_mod = FsmMod, fsm_state = FsmState}) ->
    _ = FsmMod:msg_in({disconnect, ?NORMAL_DISCONNECT}, FsmState),
    _ = vmq_metrics:incr_socket_close(),
    ok.

%% Internal

handle_fsm_return({ok, FsmState, Rest, Out}, State) ->
    maybe_reply(Out, State#state{fsm_state = FsmState, buffer = Rest});
handle_fsm_return({switch_fsm, NewFsmMod, FsmState0, Rest, Out}, State) ->
    maybe_reply(Out, State#state{fsm_mod = NewFsmMod, fsm_state = FsmState0, buffer = Rest});
handle_fsm_return({throttle, MilliSecs, FsmState, Rest, Out}, State) ->
    timer:sleep(MilliSecs),
    maybe_reply(Out, State#state{fsm_state = FsmState, buffer = Rest});
handle_fsm_return({ok, FsmState, Out}, State) ->
    maybe_reply(Out, State#state{fsm_state = FsmState});
handle_fsm_return({stop, normal, Out}, State) ->
    ?LOG_DEBUG("ws session normally stopped", []),
    self() ! {?MODULE, terminate},
    maybe_reply(Out, State#state{fsm_state = terminated});
handle_fsm_return({stop, shutdown, Out}, State) ->
    ?LOG_DEBUG("ws session stopped due to shutdown", []),
    self() ! {?MODULE, terminate},
    maybe_reply(Out, State#state{fsm_state = terminated});
handle_fsm_return({stop, Reason, Out}, #state{fsm_mod = FsmMod, fsm_state = FsmState} = State) ->
    SubscriberId = apply(FsmMod, subscriber, [FsmState]),
    ?LOG_WARNING("ws session for client ~p stopped abnormally due to '~p'", [
        SubscriberId, Reason
    ]),
    self() ! {?MODULE, terminate},
    maybe_reply(Out, State#state{fsm_state = terminated});
handle_fsm_return({error, Reason, Out}, #state{fsm_mod = FsmMod, fsm_state = FsmState} = State) ->
    SubscriberId = apply(FsmMod, subscriber, [FsmState]),
    ?LOG_WARNING("ws session error for client ~p, force terminate due to '~p'", [
        SubscriberId, Reason
    ]),
    self() ! {?MODULE, terminate},
    maybe_reply(Out, State#state{fsm_state = terminated}).

maybe_reply(Out, State) ->
    case iolist_size(Out) of
        0 ->
            {ok, State, hibernate};
        NrOfBytes ->
            _ = vmq_metrics:incr_bytes_sent(NrOfBytes),
            {reply, {binary, Out}, State, hibernate}
    end.

add_websocket_sec_header(Req) ->
    case cowboy_req:parse_header(?SEC_WEBSOCKET_PROTOCOL, Req, []) of
        [] ->
            {error, unsupported_protocol};
        SubProtocols ->
            case select_protocol(SubProtocols, ?SUPPORTED_PROTOCOLS) of
                {ok, SubProtocol} ->
                    {ok, cowboy_req:set_resp_header(?SEC_WEBSOCKET_PROTOCOL, SubProtocol, Req)};
                {error, _} = E ->
                    E
            end
    end.

select_protocol([], _) ->
    {error, unsupported_protocol};
select_protocol([Want | Rest], Have) ->
    case lists:member(Want, ?SUPPORTED_PROTOCOLS) of
        true ->
            {ok, Want};
        _ ->
            select_protocol(Rest, Have)
    end.

add_socket(Socket, State) ->
    State#state{socket = Socket}.

ensure_binary(L) when is_list(L) -> list_to_binary(L);
ensure_binary(L) when is_binary(L) -> L;
ensure_binary(undefined) -> undefined.
