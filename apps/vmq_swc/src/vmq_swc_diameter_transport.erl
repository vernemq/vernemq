%% Copyright 2018 Octavo Labs AG Zurich Switzerland (https://octavolabs.com)
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

-module(vmq_swc_diameter_transport).
-include("vmq_swc.hrl").
-include_lib("diameter/include/diameter.hrl").
-include("vmq_diameter.hrl").

-export([init/1,
         terminate/1,
         start_listener/3,
         start_connection/4,
         stop_connection/2,
         listener_address_port/1,
         rpc/5,
         rpc_cast/5]).

-export([peer_up/4,
         peer_down/4,
         pick_peer/5,
         prepare_request/4,
         prepare_retransmit/4,
         handle_answer/5,
         handle_error/5,
         handle_request/4]).

-define(SWC_SERVICE, swc_service).
-define(DIAMETER_APP, swc).
-define(DIAMETER_DICT, vmq_diameter).
-define(DIAMETER_VENDOR_ID, 888888).
-define(DIAMETER_PRODUCT_NAME, <<"VerneMQ SWC">>).


init(#swc_config{group=SwcGroup, peer=Peer} = Config) ->
    % ensure it's stopped
    terminate(Config),
    diameter:start(),
    ApplicationOptions = [{alias, ?DIAMETER_APP},
                          {dictionary, ?DIAMETER_DICT},
                          {module, [?MODULE, [Config]]} %% this appends the Config to every callback
                         ],
    Options = [{application, ApplicationOptions},
               {decode_format, record},
               {string_decode, false},
               {traffic_counters, true},
               {use_shared_peers, false},
               {restrict_connections, false},
               {'Origin-Host', Peer},
               {'Origin-Realm', realm(SwcGroup)},
               {'Auth-Application-Id', [?DIAMETER_DICT:id()]},
               {'Vendor-Id', ?DIAMETER_VENDOR_ID},
               {'Product-Name', ?DIAMETER_PRODUCT_NAME}],
    ok = diameter:start_service(SwcGroup, Options).

terminate(#swc_config{group=SwcGroup}) ->
    _ = diameter:stop_service(SwcGroup),
    _ = diameter:stop().

start_listener(#swc_config{group=SwcGroup}, Port, TCPConfig) ->
    TransportModule = application:get_env(vmq_swc, transport_module, diameter_tcp),
    TransportConfig = application:get_env(vmq_swc, transport_listener_config, [{reuseaddr, true},
                                                                               {port, Port}|TCPConfig]),
    ListenerOptions = [{length_errors, handle},
                       {pool_size, 10},
                       {transport_module, TransportModule},
                       {transport_config, ensure_module_on_top(TransportConfig)}],
    {ok, _Ref} = diameter:add_transport(SwcGroup, {listen, ListenerOptions}).

start_connection(#swc_config{group=SwcGroup}, DestinationIP, DestinationPort, TCPConfig) ->
    TransportModule = application:get_env(vmq_swc, transport_module, diameter_tcp),
    TransportConfig = application:get_env(vmq_swc, transport_connect_config, [{reuseaddr, true},
                                                                              {raddr, DestinationIP},
                                                                              {rport, DestinationPort}|TCPConfig]),
    ConnectOptions = [{length_errors, handle},
                       {pool_size, 10},
                       {transport_module, TransportModule},
                       {transport_config, ensure_module_on_top(TransportConfig)}],
    {ok, _Ref} = diameter:add_transport(SwcGroup, {connect, ConnectOptions}).

ensure_module_on_top(Opts) ->
    %% diameter_tcp requires that the module option is the first element of the list to be recognized.
    %% this is actually only relevant for testing using the vmq_swc_proxy_diameter_transport module.
    {[T], Rest} = proplists:split(Opts, [module]),
    T ++ Rest.

stop_connection(#swc_config{group=SwcGroup}, TRef) ->
    diameter:remove_transport(SwcGroup, TRef).

rpc(#swc_config{group=SwcGroup, peer=Peer} = _Config, RemotePeer, Module, Function, Args) ->
    Timeout = 5000,
    Request = #'Request'{
                    'Origin-Host'=Peer,
                    'Origin-Realm'=realm(SwcGroup),
                    'Payload'=term_to_binary({Module, Function, Args})
                },
    diameter_call(SwcGroup, Request, Timeout, [{filter, {host, RemotePeer}}]).

rpc_cast(#swc_config{group=SwcGroup, peer=Peer} = _Config, RemotePeer, Module, Function, Args) ->
    Timeout = 5000,
    Request = #'Request'{
                    'Origin-Host'=Peer,
                    'Origin-Realm'=realm(SwcGroup),
                    'Payload'=term_to_binary({Module, Function, Args})
                },
    diameter_call(SwcGroup, Request, Timeout, [detach, {filter, {host, RemotePeer}}]).

 diameter_call(SwcGroup, Request, Timeout, Opts) ->
    case diameter:call(SwcGroup, ?DIAMETER_APP, Request, [{timeout, Timeout} | Opts]) of
        ok -> ok;
        {ok, Response} ->
            Response;
        {error, Reason} ->
            {error, Reason}
    end.

listener_address_port(SwcGroup) ->
    Listeners =
    lists:filter(fun(Transport) ->
                         {type, listen} == lists:keyfind(type, 1, Transport)
                 end, diameter_service:info(SwcGroup, transport)),
    case Listeners of
        [] ->
            {error, no_listener_defined};
        [L|_] ->
            Options = proplists:get_value(options, L, []),
            TransportConfig = proplists:get_value(transport_config, Options, []),
            IP = proplists:get_value(ip, TransportConfig, {127,0,0,1}),
            Port = proplists:get_value(port, TransportConfig),
            {ok, {IP, Port}}
    end.

realm(SwcGroup) ->
    atom_to_binary(SwcGroup, utf8).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Diameter Callbacks
%%%

peer_up(_SvcName, {_, Caps} = _Peer, State, [#swc_config{group=SwcGroup} = Config]) ->
    #diameter_caps{origin_host={_, OH}} = Caps,
    vmq_swc_group_membership:peer_event(Config, {up, OH}),
    lager:info("SWC[~p] Diameter Peer ~p up", [SwcGroup, OH]),
    State.

peer_down(_SvcName, {_, Caps} = _Peer, State, [#swc_config{group=SwcGroup} = Config]) ->
    #diameter_caps{origin_host={_, OH}} = Caps,
    vmq_swc_group_membership:peer_event(Config, {down, OH}),
    lager:info("SWC[~p] Diameter Peer ~p down", [SwcGroup, OH]),
    State.

pick_peer(LocalCandidates, _RemoteCandidates, _SvcName, _State, _Extra) ->
    Candidate = lists:nth(rand:uniform(length(LocalCandidates)), LocalCandidates),
    {ok, Candidate}.

prepare_request(Packet, _SvcName, _Peer, _Extra) ->
    {send, Packet}.

prepare_retransmit(Packet, _SvcName, _Peer, _Extra) ->
    {send, Packet}.

handle_answer(#diameter_packet{msg=Msg, errors=[]} = _Packet, _Request, _SvcName, _Peer, _Extra) ->
    #'Response'{'Payload'=BinPayload} = Msg,
    Payload = binary_to_term(BinPayload),
    {ok, Payload}.

handle_error(Reason, _Request, _SvcName, _Peer, _Extra) ->
    {error, Reason}.

handle_request(#diameter_packet{msg=Msg, errors=[]} = _Packet, _SvcName, {_, Caps} = _Peer, [_Config] = Extra) ->
    #diameter_caps{origin_realm={_, OR},
                   origin_host={_, OH}} = Caps,
    #'Request'{'Payload'=RequestPayload} = Msg,

    ResponsePayload =
    try
        {Module, Function, Args} = binary_to_term(RequestPayload),
        apply(Module, Function, Args ++ Extra)
    catch
        _:_ ->
            {error, remote_exception}
    end,
    Response = #'Response'{
                  'Result-Code' = 2001,
                  'Origin-Host' = OH,
                  'Origin-Realm' = OR,
                  'Payload'=term_to_binary(ResponsePayload)
                 },
    {reply, Response}.
