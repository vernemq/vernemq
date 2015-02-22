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

-module(vmq_tcp_listener_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,
         start_listener/2,
         reconfigure_listeners/1,
         stop_listener/2,
         stop_listener/3,
         delete_listener/2,
         restart_listener/2,
         get_listener_config/2,
         listeners/0]).

%% Supervisor callbacks
-export([init/1]).

-define(CHILD(Id, Mod, Type, Args), {Id, {Mod, start_link, Args},
                                     permanent, 5000, Type, [Mod]}).

-define(SUP, ?MODULE).

-type transport_mod() :: vmq_tcp_transport
                      | vmq_ws_transport
                      | vmq_ssl_transport
                      | vmq_wss_transport.
%%%===================================================================
%%% API functions
%%%===================================================================
start_link() ->
    supervisor:start_link({local, ?SUP}, ?MODULE, []).


listener_sup_sup(Addr, Port) ->
    AAddr = addr(Addr),
    Ref = listener_name(AAddr, Port),
    case lists:keyfind(Ref, 1, supervisor:which_children(?SUP)) of
        false ->
            {error, not_found};
        {_, ListenerSupSupPid, supervisor, _} when is_pid(ListenerSupSupPid) ->
            {ok, ListenerSupSupPid}
    end.


stop_listener(Addr, Port) ->
    stop_listener(Addr, Port, false).
stop_listener(Addr, Port, KillSessions) ->
    case listener_sup_sup(Addr, Port) of
        {ok, Pid} ->
            vmq_tcp_listener_sup_sup:stop_listener(Pid, KillSessions);
        E -> E
    end.

delete_listener(Addr, Port) ->
    AAddr = addr(Addr),
    Ref = listener_name(AAddr, Port),
    delete_listener(Ref).

delete_listener(ListenerRef) ->
    case supervisor:terminate_child(?SUP, ListenerRef) of
        {error, _} ->
            ok;
        ok ->
            supervisor:delete_child(?SUP, ListenerRef)
    end.

start_listener(Addr, Port) ->
    AAddr = addr(Addr),
    case get_listener_config(AAddr, Port) of
        {ok, {TransportMod, Opts}} ->
            MountPoint = proplists:get_value(mountpoint, Opts, ""),
            MaxConns = proplists:get_value(max_connections, Opts,
                                           vmq_config:get_env(max_connections)),
            Ref = listener_name(AAddr, Port),
            TCPOpts = vmq_config:get_env(tcp_listen_options),
            TransportOpts = {TCPOpts, transport_opts(TransportMod, Opts)},
            start_listener(TransportMod, Ref, AAddr, Port,
                           MaxConns, MountPoint, TransportOpts);
        E ->
            E
    end.

start_listener(TransportMod, Ref, Addr, Port, MaxConns, MountPoint, TransportOpts) ->
    ChildSpec = {Ref,
                 {vmq_tcp_listener_sup_sup, start_link, [Addr, Port]},
                 permanent, 5000, supervisor, [vmq_tcp_listener_sup_sup]},
    case supervisor:start_child(?SUP, ChildSpec) of
        {ok, SupSupPid} ->
            case vmq_tcp_listener_sup_sup:setopts(SupSupPid, TransportMod, MaxConns,
                                                  MountPoint, TransportOpts) of
                ok ->
                    vmq_tcp_listener_sup_sup:accept(SupSupPid);
                E -> E
            end;
        E ->
            E
    end.

restart_listener(Addr, Port) ->
    case listener_sup_sup(Addr, Port) of
        {ok, Pid} ->
            vmq_tcp_listener_sup_sup:restart_listener(Pid);
        E ->
            E
    end.

listeners() ->
    lists:foldl(
      fun ({{vmq_tcp_listener_sup_sup, Ip, Port}, Status, supervisor, _}, Acc) ->
              {ok, {TransportMod, Opts}} = get_listener_config(Ip, Port),
              MountPoint = proplists:get_value(mountpoint, Opts, ""),
              MaxConnections = proplists:get_value(max_connections, Opts,
                                                  vmq_config:get_env(max_connections)),
              Status1 =
              case Status of
                  restarting -> restarting;
                  undefined -> stopped;
                  Pid when is_pid(Pid) ->
                      vmq_tcp_listener_sup_sup:status(Pid)
              end,
              Type =
              case TransportMod of
                  vmq_tcp_transport -> 'TCP';
                  vmq_ws_transport -> 'WS';
                  vmq_ssl_transport -> 'SSL';
                  vmq_wss_transport -> 'WSS'
              end,
              StrIp = inet:ntoa(Ip),
              StrPort = integer_to_list(Port),
              [{Type, StrIp, StrPort, Status1, MountPoint, MaxConnections}|Acc]
      end, [], supervisor:which_children(?SUP)).

get_listener_config(Addr, Port) ->
    Key = {Addr, Port},
    {TCP, SSL, WS, WSS} = vmq_config:get_env(listeners),
    case [{M, Opts} ||{M, Opts} <-
                      [{vmq_tcp_transport, proplists:get_value(Key, TCP, nil)},
                       {vmq_ssl_transport, proplists:get_value(Key, SSL, nil)},
                       {vmq_ws_transport, proplists:get_value(Key, WS, nil)},
                       {vmq_wss_transport, proplists:get_value(Key, WSS, nil)}],
                      Opts /= nil] of
        [{TransportMod, Opts}] -> {ok, {TransportMod, Opts}};
        [] -> {error, not_found}
    end.


reconfigure_listeners(_) ->
    TCPListenOptions = vmq_config:get_env(tcp_listen_options),
    {TCP, SSL, WS, WSS} = vmq_config:get_env(listeners),
    Listeners = supervisor:which_children(?SUP),
    reconfigure_listeners(vmq_tcp_transport, Listeners, TCP, TCPListenOptions),
    reconfigure_listeners(vmq_ssl_transport, Listeners, SSL, TCPListenOptions),
    reconfigure_listeners(vmq_ws_transport, Listeners, WS, TCPListenOptions),
    reconfigure_listeners(vmq_wss_transport, Listeners, WSS, TCPListenOptions),
    stop_and_delete_unused(Listeners, lists:flatten([TCP, SSL, WS, WSS])).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @spec init(Args) -> {ok, {SupFlags, [ChildSpec]}} |
%%                     ignore |
%%                     {error, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    {ok, {{one_for_one, 5, 10}, []}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

addr(Addr) when is_list(Addr) ->
    {ok, Ip} = inet:parse_address(Addr),
    Ip;
addr(Addr) -> Addr.


reconfigure_listeners(Type, Listeners, [{{Addr, Port}, Opts}|Rest], TCPOpts) ->
    reconfigure_single_listener(Type, Listeners, Addr, Port, Opts, TCPOpts),
    reconfigure_listeners(Type, Listeners, Rest, TCPOpts);
reconfigure_listeners(_, _, [], _) -> ok.

reconfigure_single_listener(Type, Listeners, Addr, Port, Opts, TCPOpts) ->
    AAddr = addr(Addr),
    Ref = listener_name(AAddr, Port),
    TransportOpts = {TCPOpts, transport_opts(Type, Opts)},
    MountPoint = proplists:get_value(mountpoint, Opts, ""),
    MaxConns = proplists:get_value(max_connections, Opts,
                                   vmq_config:get_env(max_connections)),
    case lists:keyfind(Ref, 1, Listeners) of
        false -> % new listener
            start_listener(Type, Ref, AAddr, Port, MaxConns,
                           MountPoint, TransportOpts);
        {_, Pid, _, _} when is_pid(Pid) -> % change existing listener
            %% change listener
            vmq_tcp_listener_sup_sup:setopts(Pid, Type, MaxConns,
                                             MountPoint, TransportOpts);
        _ ->
            ok
    end.


stop_and_delete_unused(Listeners, Config) ->
    ListenersToDelete =
    lists:foldl(fun({{Addr, Port}, _}, Acc) ->
                        Ref = listener_name(addr(Addr), Port),
                        lists:keydelete(Ref, 1, Acc)
                end, Listeners, Config),
    lists:foreach(fun({Ref, _, _, _}) ->
                          delete_listener(Ref)
                  end, ListenersToDelete).

-spec listener_name(inet:ip_address(),
                    inet:port_number()) ->
                           {'vmq_tcp_listener_sup_sup',
                            inet:ip_address(), inet:port_number()}.
listener_name(Ip, Port) ->
    {vmq_tcp_listener_sup_sup, Ip, Port}.

-spec transport_opts(transport_mod(), _) -> [{atom(), any()}].
transport_opts(Mod, Opts) ->
    apply(Mod, opts, [Opts]).
