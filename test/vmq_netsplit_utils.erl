-module(vmq_netsplit_utils).
-export([test/3,
         reset_tables/1,
         partition_network/1,
         fix_network/2,
         get_port/1,
         check_connected/1,
         configure_trade_consistency/1,
         ensure_not_ready/1,
         call_proxy/4,
         proxy_multicall/4]).

-export([start_app/3,
         proxy/0,
         wait_til_ready/0]).

-define(LISTENER(Port), {{{127, 0, 0, 1}, Port}, [{max_connections, infinity},
                                                  {nr_of_acceptors, 10},
                                                  {mountpoint, ""}]}).

-define(NR_OF_NODES, 5).
-define(INIT_PORT, 18880).

-export([hook_auth_on_publish/6,
         hook_auth_on_subscribe/3]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Setup Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
test(NetTickTime, Timeout, TestsFun) ->
    {timeout, Timeout,
     {setup,
      fun() -> setup(NetTickTime) end,
      fun(Nodes) -> teardown(Nodes) end,
      TestsFun
     }
    }.

reset_tables([N|_]) ->
    call_proxy(N, vmq_reg, reset_all_tables, [[]]).

setup(NetTickTime) ->
    {A, B, C} = now(),
    random:seed(A, B, C),
    epmdpxy:start(vmq_server_test_master, epmd_port()),
    timer:sleep(1000),
    io:format(user, "Started EPMDPXY on node ~p with EPMD port ~p~n",
              [node(), epmd_port()]),
    net_kernel:set_net_ticktime(NetTickTime, NetTickTime),
    Hosts = hosts(),
    [DiscoveryNode|_] = Ns = start_slaves(Hosts),
    try
        [ok = rpc:call(Node, ?MODULE, start_app, [NetTickTime, DiscoveryNode, I])
         || {I, Node} <- lists:zip(lists:seq(1, length(Ns)), Ns)],
        wait_till_cluster_ready(Ns),
        Ns
    catch
        _:R ->
            stop_slaves(Ns),
            application:stop(epmdpxy),
            exit(R)
    end.

teardown(Nodes) ->
    stop_slaves(Nodes),
    application:stop(epmdpxy).

partition_network(Nodes) ->
    %% Create Partitions
    Size = ?NR_OF_NODES div 2,
    Island1 = lists:sublist(Nodes, Size),
    Island2 = Nodes -- Island1,
    io:format(user, "Create two partitions ~p and ~p~n", [Island1, Island2]),
    epmdpxy:cut_cables(Island1, Island2),
    {Island1, Island2}.

fix_network(Island1, Island2) ->
    epmdpxy:fix_cables(Island1, Island2).

get_port([Node|_]) ->
    [Name, _] = re:split(atom_to_list(Node), "@", [{return, list}]),
    [I|_] = lists:reverse(Name),
    ?INIT_PORT + list_to_integer([I]).






check_connected(Nodes) ->
    check_connected(Nodes, 0).
check_connected([N1, N2|Rest] = Nodes, I) when length(Nodes) < I ->
    %% ensure all nodes are connected
    [N2|Rest] = call_proxy(N1, erlang, nodes, []) -- [node()],
    check_connected([N2|Rest] ++ [N1], I + 1);
check_connected(_ , _) -> ok.

configure_trade_consistency(Nodes) ->
    S= proxy_multicall(Nodes, application, set_env, [vmq_server,
                                                  trade_consistency,
                                                  true]),
    %% we must also allow multiple sessions, if we want to let
    %% new clients register during netsplit, otherwise
    %% vmq_reg_leader will complain about the unstable cluster
    S= proxy_multicall(Nodes, application, set_env, [vmq_server,
                                                  allow_multiple_sessions,
                                                  true]),
    C= proxy_multicall(Nodes, vmq_config, configure_node, []),
    io:format(user, "configurge trade consistency ~p ~p~n", [S, C]),
    ok.

ensure_not_ready(Nodes) ->
    Ready = proxy_multicall(Nodes, vmq_cluster, is_ready, []),
    false == lists:member(true, Ready).

hosts() ->
    [list_to_atom("vmq"++integer_to_list(I))
     || I <- lists:seq(1, ?NR_OF_NODES)].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
hook_auth_on_publish(_, _, _, _, _, _) -> ok.
hook_auth_on_subscribe(_, _, _) -> ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_app(NetTickTime, DiscoveryNode, I) ->
    Port = ?INIT_PORT + I,
    net_kernel:set_net_ticktime(NetTickTime, NetTickTime),
    application:load(vmq_server),
    application:set_env(vmq_server, allow_anonymous, true),
    application:set_env(vmq_server, listeners, [{mqtt, [?LISTENER(Port)]}]),
    case I of
        1 ->
            % we are the discovery node
            vmq_server:start_no_auth();
        _ ->
            pong = net_adm:ping(DiscoveryNode),
            vmq_server:start_no_auth(DiscoveryNode)
    end,
    vmq_plugin_mgr:enable_module_plugin(auth_on_publish, ?MODULE,
                                        hook_auth_on_publish, 6),
    vmq_plugin_mgr:enable_module_plugin(auth_on_subscribe, ?MODULE,
                                        hook_auth_on_subscribe, 3),

    io:fwrite(user, "app started ~p~n", [node()]).

wait_till_cluster_ready([N|Nodes]) ->
    call_proxy(N, ?MODULE, wait_til_ready, []),
    wait_till_cluster_ready(Nodes);
wait_till_cluster_ready([]) -> ok.

wait_til_ready() ->
    wait_til_ready(vmq_cluster:is_ready(), 100).

wait_til_ready(true, _) -> ok;
wait_til_ready(false, I) when I > 0 ->
    timer:sleep(500),
    wait_til_ready(vmq_cluster:is_ready(), I - 1);
wait_til_ready(_, _) ->
    exit(not_ready).


%% from uwiger/locks
-define(PROXY, vmq_server_test_proxy).
proxy() ->
    register(?PROXY, self()),
    process_flag(trap_exit, true),
    proxy_loop().

proxy_loop() ->
    receive
        {From, Ref, apply, M, F, A} ->
            From ! {Ref, (catch apply(M,F,A))};
        _ ->
            ok
    end,
    proxy_loop().

proxy_multicall(Ns, M, F, A) ->
    [call_proxy(N, M, F, A) || N <- Ns].

call_proxy(N, M, F, A) ->
    Ref = erlang:monitor(process, {?PROXY, N}),
    {?PROXY, N} ! {self(), Ref, apply, M, F, A},
    receive
        {'DOWN', Ref, _, _, Reason} ->
            error({proxy_died, N, Reason});
        {Ref, Result} ->
            Result
    after 5000 ->
              error(proxy_call_timeout)
    end.

start_slaves(Ns) ->
    Nodes = [start_slave(N) || N <- Ns],
    Nodes.

start_slave(Name) ->
    {Pa, Pz} = paths(),
    Paths = "-pa ./ -pz ../ebin" ++
    lists:flatten([[" -pa " ++ Path || Path <- Pa],
                   [" -pz " ++ Path || Path <- Pz]]),
    {ok, Node} = ct_slave:start(host(), Name, [{erl_flags, Paths},
                                               {monitor_master, true}]),
    spawn(Node, ?MODULE, proxy, []),
    Node.

stop_slaves(Ns) ->
    [ok = stop_slave(N) || N <- Ns],
    ok.

stop_slave(N) ->
    try erlang:monitor_node(N, true) of
        true ->
            rpc:call(N, erlang, halt, []),
            receive
                {nodedown, N} -> ok
            after 10000 ->
                      erlang:error(slave_stop_timeout)
            end
    catch
        error:badarg ->
            ok
    end.

paths() ->
    Path = code:get_path(),
    {ok, [[Root]]} = init:get_argument(root),
    {Pas, Rest} = lists:splitwith(fun(P) ->
                                          not lists:prefix(Root, P)
                                  end, Path),
    Pzs = lists:filter(fun(P) ->
                               not lists:prefix(Root, P)
                       end, Rest),
    {Pas, Pzs}.

host() ->
    [_Name, Host] = re:split(atom_to_list(node()), "@", [{return, list}]),
    list_to_atom(Host).

epmd_port() ->
    {ok, [[StrEPMD_PORT]|_]} = init:get_argument(epmd_port),
    list_to_integer(StrEPMD_PORT).

