-module(emqttd_bridge_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(Id, Mod, Type, Args), {Id, {Mod, start_link, Args}, permanent, 5000, Type, [Mod]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, RegistryMFA} = application:get_env(emqttd_bridge, registry_mfa),
    {ok, {TCPConfig, SSLConfig}} = application:get_env(emqttd_bridge, config),
    TCPChildSpecs =
    [?CHILD(list_to_atom(lists:flatten(["emqttd_bridge-", Host, ":", integer_to_list(Port)])),
            emqttd_bridge, worker, [RegistryMFA,
                                    proplists:get_value(topics, Opts),
                                    client_opts(tcp, Host, Port, Opts)])
     || {{Host, Port}, Opts} <- TCPConfig],
    SSLChildSpecs =
    [?CHILD(list_to_atom(lists:flatten(["emqttd_bridge-", Host, ":", integer_to_list(Port)])),
            emqttd_bridge, worker, [RegistryMFA,
                                    proplists:get_value(topics, Opts),
                                    client_opts(ssl, Host, Port, Opts)])
     || {{Host, Port}, Opts} <- SSLConfig],
    {ok, { {one_for_one, 5, 10}, TCPChildSpecs ++ SSLChildSpecs} }.

client_opts(tcp, Host, Port, Opts) ->
    [{host, Host},
     {port, Port},
     {username, proplists:get_value(username, Opts)},
     {password, proplists:get_value(password, Opts)},
     {client,   proplists:get_value(client_id, Opts)},
     {clean_session, proplists:get_value(clean_session, Opts)},
     {keepalive_interval, proplists:get_value(keepalive_interval, Opts)},
     {reconnect_timeout, proplists:get_value(restart_timeout, Opts)},
     {transport, {gen_tcp, []}}
     |case proplists:get_value(try_private, Opts) of
          true ->
              [{proto_version, 131}]; %% non-spec
          false ->
              []
      end];
client_opts(ssl, Host, Port, Opts) ->
    TCPOpts = client_opts(tcp, Host, Port, Opts),
    SSLOpts = [{certfile, proplists:get_value(certfile, Opts)},
               {cacertfile, proplists:get_value(cafile, Opts)},
               {keyfile, proplists:get_value(keyfile, Opts)},
               {verify, case proplists:get_value(insecure, Opts) of
                            true -> verify_none;
                            _ -> verify_peer
                        end},
               {versions, case proplists:get_value(tls_version, Opts) of
                              undefined -> undefined;
                              V -> [V]
                          end},
               {psk_identity, proplists:get_value(identity, Opts)},
               {user_lookup_fun, case {proplists:get_value(identity, Opts) == undefined,
                                       proplists:get_value(psk, Opts)}
                                 of
                                     {Identity, Psk}
                                       when is_list(Identity) and is_list(Psk) ->
                                         BinPsk = to_bin(Psk),
                                         {fun(psk, I, _) when I == Identity ->
                                                  {ok, BinPsk};
                                             (_, _, _) -> error
                                          end, []};
                                     _ -> undefined
                                 end}
                ],

    lists:keyreplace(transport, 1, TCPOpts,
                     {transport, {ssl, [P||{_,V}=P <- SSLOpts, V /= undefined]}}).

%% @spec to_bin(string()) -> binary()
%% @doc Convert a hexadecimal string to a binary.
to_bin(L) ->
    to_bin(L, []).

%% @doc Convert a hex digit to its integer value.
dehex(C) when C >= $0, C =< $9 ->
    C - $0;
dehex(C) when C >= $a, C =< $f ->
    C - $a + 10;
dehex(C) when C >= $A, C =< $F ->
    C - $A + 10.

to_bin([], Acc) ->
    iolist_to_binary(lists:reverse(Acc));
to_bin([C1, C2 | Rest], Acc) ->
    to_bin(Rest, [(dehex(C1) bsl 4) bor dehex(C2) | Acc]).
