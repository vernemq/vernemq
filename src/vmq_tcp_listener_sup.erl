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
         reconfigure_single_listener/6,
         reconfigure_listeners/1,
         listener_name/2,
         addr/1]).

%% Supervisor callbacks
-export([init/1]).

-define(CHILD(Id, Mod, Type, Args), {Id, {Mod, start_link, Args},
                                     permanent, 5000, Type, [Mod]}).

-define(SUP, ?MODULE).

-type transport_mod() :: vmq_tcp_transport
                      | vmq_ws_tranport
                      | vmq_ssl_transport
                      | vmq_wss_transport.
%%%===================================================================
%%% API functions
%%%===================================================================
start_link() ->
    supervisor:start_link({local, ?SUP}, ?MODULE, []).

reconfigure_listeners(ListenerConfig) ->
    TCPListenOptions = proplists:get_value(tcp_listen_options,
                                           ListenerConfig,
                                           vmq_config:get_env(tcp_listen_options)),
    {TCP, SSL, WS, WSS} = proplists:get_value(listeners,
                                         ListenerConfig,
                                         vmq_config:get_env(listeners)),
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
-spec start_listener(transport_mod(),
                     string() | inet:ip_address(), inet:port_number(),
                     string(), {[any()],[any()]}) -> {'ok',pid()}.
start_listener(TransportMod, Addr, Port, MountPoint, TransportOpts) ->
    AAddr = addr(Addr),
    Ref = listener_name(AAddr, Port),
    ChildSpec = {Ref,
                 {vmq_tcp_listener, start_link, [AAddr, Port]},
                 permanent, 5000, worker, [vmq_tcp_listener]},
    case supervisor:start_child(?SUP, ChildSpec) of
        {ok, Pid} ->
            case vmq_tcp_listener:setopts(Pid, TransportMod,
                                      MountPoint, TransportOpts) of
                ok ->
                    vmq_tcp_listener:accept(Pid),
                    lager:info("started ~p on ~p:~p", [TransportMod, Addr, Port]),
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            FReason = inet:format_error(Reason),
            lager:error("can't start ~p on ~p:~p due to ~p",
                        [TransportMod, Addr, Port, {Reason, FReason}]),
            {error, Reason}
    end.

addr(Addr) when is_list(Addr) ->
    {ok, Ip} = inet:parse_address(Addr),
    Ip;
addr(Addr) -> Addr.


reconfigure_listeners(Type, Listeners, [{{Addr, Port}, Opts}|Rest], TCPOpts) ->
    reconfigure_single_listener(Type, Listeners, Addr, Port, Opts, TCPOpts),
    reconfigure_listeners(Type, Listeners, Rest, TCPOpts);
reconfigure_listeners(_, _, [], _) -> ok.

reconfigure_single_listener(Type, Listeners, Addr, Port, Opts, TCPOpts) ->
    Ref = listener_name(addr(Addr),Port),
    TransportOpts = {TCPOpts, transport_opts(Type, Opts)},
    MountPoint = proplists:get_value(mountpoint, Opts, ""),
    case lists:keyfind(Ref, 1, Listeners) of
        false -> % new listener
            start_listener(Type, Addr, Port, MountPoint, TransportOpts);
        {_, Pid, _, _} when is_pid(Pid) -> % change existing listener
            %% change listener
            vmq_tcp_listener:setopts(Pid, Type, MountPoint, TransportOpts);
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
                          supervisor:terminate_child(?SUP, Ref),
                          supervisor:delete_child(?SUP, Ref)
                  end, ListenersToDelete).

-spec listener_name(inet:ip_address(),
                    inet:port_number()) ->
                           {'vmq_tcp_listener',
                            inet:ip_address(), inet:port_number()}.
listener_name(Ip, Port) ->
    {vmq_tcp_listener, Ip, Port}.

-spec transport_opts(transport_mod(), _) -> [{atom(), any()}].
transport_opts(Mod, Opts) ->
    apply(Mod, opts, [Opts]).
