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

-module(vmq_ranch_config).

%% API
-export([start_listener/4,
         reconfigure_listeners/1,
         stop_listener/2,
         stop_listener/3,
         delete_listener/2,
         restart_listener/2,
         get_listener_config/2,
         listeners/0]).

listener_sup_sup(Addr, Port) ->
    AAddr = addr(Addr),
    Ref = listener_name(AAddr, Port),
    case lists:keyfind({ranch_listener_sup, Ref}, 1, supervisor:which_children(ranch_sup)) of
        false ->
            {error, not_found};
        {_, ListenerSupSupPid, supervisor, _} when is_pid(ListenerSupSupPid) ->
            {ok, ListenerSupSupPid}
    end.


stop_listener(Addr, Port) ->
    stop_listener(Addr, Port, false).
stop_listener(Addr, Port, KillSessions) ->
    case listener_sup_sup(Addr, Port) of
        {ok, Pid} when KillSessions ->
            supervisor:terminate_child(Pid, ranch_conns_sup);
        {ok, Pid} ->
            supervisor:terminate_child(Pid, ranch_acceptors_sup);
        E -> E
    end.

restart_listener(Addr, Port) ->
    case listener_sup_sup(Addr, Port) of
        {ok, Pid} ->
            case {supervisor:restart_child(Pid, ranch_conns_sup),
                  supervisor:restart_child(Pid, ranch_acceptors_sup)} of
                {{ok, _}, {ok, _}} ->
                    ok;
                {{error, running}, {ok, _}} ->
                    ok;
                {{error, R1}, {error, R2}} ->
                    {error, {R1, R2}}
            end;
        E ->
            E
    end.

delete_listener(Addr, Port) ->
    AAddr = addr(Addr),
    Ref = listener_name(AAddr, Port),
    delete_listener(Ref).

delete_listener(ListenerRef) ->
    case supervisor:terminate_child(ranch_sup, {ranch_listener_sup, ListenerRef}) of
        {error, _} ->
            ok;
        ok ->
            _ = supervisor:delete_child(ranch_sup, {ranch_listener_sup, ListenerRef}),
            ranch_server:cleanup_listener_opts(ListenerRef)
    end.

start_listener(Type, Addr, Port, Opts) when is_list(Opts) ->
    TCPOpts = vmq_config:get_env(tcp_listen_options),
    TransportOpts = TCPOpts ++ transport_opts_for_type(Type, Opts),
    start_listener(Type, Addr, Port, {TransportOpts, Opts});
start_listener(Type, Addr, Port, {TransportOpts, Opts}) ->
    AAddr = addr(Addr),
    Ref = listener_name(AAddr, Port),
    MaxConns = proplists:get_value(max_connections, Opts,
                                   vmq_config:get_env(max_connections)),
    NrOfAcceptors = proplists:get_value(nr_of_acceptors, Opts,
                                        vmq_config:get_env(nr_of_acceptors)),
    case ranch:start_listener(Ref, NrOfAcceptors, transport_for_type(Type),
                              [{ip, AAddr}, {port, Port}|TransportOpts],
                              protocol_for_type(Type),
                              protocol_opts_for_type(Type, Opts)) of
        {ok, _} ->
            ranch:set_max_connections(Ref, MaxConns);
        E ->
            E
    end.


listeners() ->
    lists:foldl(
      fun ({ranch_server, _, _, _}, Acc) ->
              Acc;
          ({{ranch_listener_sup, {Ip, Port}}, Status, supervisor, _}, Acc) ->
              {ok, {Type, Opts}} = get_listener_config(Ip, Port),
              MountPoint = proplists:get_value(mountpoint, Opts, ""),
              MaxConnections = proplists:get_value(max_connections, Opts,
                                                  vmq_config:get_env(max_connections)),
              Status1 =
              case Status of
                  restarting -> restarting;
                  undefined -> stopped;
                  Pid when is_pid(Pid) ->
                      case lists:keyfind(ranch_acceptors_sup, 1, supervisor:which_children(Pid)) of
                          false ->
                              not_found;
                          {_, restarting, supervisor, _} ->
                              restarting;
                          {_, undefined, supervisor, _} ->
                              stopped;
                          {_, AcceptorPid, supervisor, _} when is_pid(AcceptorPid) ->
                              running
                      end
              end,
              StrIp = inet:ntoa(Ip),
              StrPort = integer_to_list(Port),
              [{Type, StrIp, StrPort, Status1, MountPoint, MaxConnections}|Acc]
      end, [], supervisor:which_children(ranch_sup)).

get_listener_config(Addr, Port) ->
    Key = {Addr, Port},
    {TCP, SSL, WS, WSS} = vmq_config:get_env(listeners),
    case [{M, Opts} ||{M, Opts} <-
                      [{tcp, proplists:get_value(Key, TCP, nil)},
                       {ssl, proplists:get_value(Key, SSL, nil)},
                       {ws, proplists:get_value(Key, WS, nil)},
                       {wss, proplists:get_value(Key, WSS, nil)}],
                      Opts /= nil] of
        [{TransportMod, Opts}] -> {ok, {TransportMod, Opts}};
        [] -> {error, not_found}
    end.


reconfigure_listeners(_) ->
    TCPListenOptions = vmq_config:get_env(tcp_listen_options),
    {TCP, SSL, WS, WSS} = vmq_config:get_env(listeners),
    Listeners = supervisor:which_children(ranch_sup),
    reconfigure_listeners(tcp, Listeners, TCP, TCPListenOptions),
    reconfigure_listeners(ssl, Listeners, SSL, TCPListenOptions),
    reconfigure_listeners(ws, Listeners, WS, TCPListenOptions),
    reconfigure_listeners(wss, Listeners, WSS, TCPListenOptions),
    stop_and_delete_unused(Listeners, lists:flatten([TCP, SSL, WS, WSS])).

addr(Addr) when is_list(Addr) ->
    {ok, Ip} = inet:parse_address(Addr),
    Ip;
addr(Addr) -> Addr.


reconfigure_listeners(Type, Listeners, [{{Addr, Port}, Opts}|Rest], TCPOpts) ->
    TransportOpts = TCPOpts ++ transport_opts_for_type(Type, Opts),
    start_listener(Type, Addr, Port, {TransportOpts, Opts}),
    reconfigure_listeners(Type, Listeners, Rest, TCPOpts);
reconfigure_listeners(_, _, [], _) -> ok.

stop_and_delete_unused(Listeners, Config) ->
    ListenersToDelete =
    lists:foldl(fun ({{Addr, Port}, _}, Acc) ->
                        Ref = listener_name(addr(Addr), Port),
                        lists:keydelete({ranch_listener_sup, Ref}, 1, Acc)
                end, Listeners, Config),
    lists:foreach(fun({Ref, _, _, _}) ->
                          delete_listener(Ref)
                  end, ListenersToDelete).

listener_name(Ip, Port) ->
    {Ip, Port}.

transport_for_type(tcp) -> ranch_tcp;
transport_for_type(ssl) -> ranch_ssl;
transport_for_type(ws) -> ranch_tcp;
transport_for_type(wss) -> ranch_ssl.

protocol_for_type(tcp) -> vmq_ranch;
protocol_for_type(ssl) -> vmq_ranch;
protocol_for_type(ws) -> cowboy_protocol;
protocol_for_type(wss) -> cowboy_protocol.

transport_opts_for_type(Type, Opts) ->
    transport_opts(transport_for_type(Type), Opts).
transport_opts(ranch_tcp, _) -> [];
transport_opts(ranch_ssl, Opts) -> vmq_ssl:opts(Opts).

protocol_opts_for_type(Type, Opts) ->
    protocol_opts(protocol_for_type(Type), Opts).
protocol_opts(vmq_ranch, Opts) -> Opts;
protocol_opts(cowboy_protocol, Opts) ->
    Dispatch = cowboy_router:compile(
                 [{'_', [{"/mqtt", vmq_websocket, [Opts]}]}
                 ]),
    [{env, [{dispatch, Dispatch}]}].

