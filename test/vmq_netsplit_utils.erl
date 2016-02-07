-module(vmq_netsplit_utils).
-export([setup/1,
         teardown/1,
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

-define(NR_OF_NODES, 5).
-define(INIT_PORT, 18880).
-define(DEFAULT_EPMDPXY_PORT, 4369).

-export([hook_auth_on_publish/6,
         hook_auth_on_subscribe/3]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Setup Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

setup(NetTickTime) ->
    {A, B, C} = os:timestamp(),
    random:seed(A, B, C),
    os:cmd("killall epmd"),
    epmdpxy:start(?DEFAULT_EPMDPXY_PORT),
    timer:sleep(1000),
    ct:pal("Started EPMDPXY on node ~p with EPMD port ~p~n",
              [node(), ?DEFAULT_EPMDPXY_PORT]),
    vmq_test_utils:maybe_start_distribution(vmq_ct_master),
    set_net_ticktime(NetTickTime),
    Hosts = hosts(),
    [DiscoveryNode|_] = Ns = start_slaves(NetTickTime, Hosts),
    ct_cover:add_nodes(Ns),
    try
        [ok = rpc:call(Node, ?MODULE, start_app, [NetTickTime, DiscoveryNode, I])
         || {I, Node} <- lists:zip(lists:seq(1, length(Ns)), Ns)],
        wait_till_cluster_ready(Ns),
        lists:sort(Ns)
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
    proxy_multicall(Nodes, vmq_server_cmd, set_config, [
                                                  trade_consistency,
                                                  true]),
    %% we must also allow multiple sessions, if we want to let
    %% new clients register during netsplit, otherwise
    %% vmq_reg_leader will complain about the unstable cluster
    proxy_multicall(Nodes, vmq_server_cmd, set_config, [
                                                  allow_multiple_sessions,
                                                  true]),
    ok.

ensure_not_ready(Nodes) ->
    ensure_not_ready(Nodes, 60000).

ensure_not_ready(_, 0) ->
    ct:pal("cluster can't agree on cluster state on time"),
    false;
ensure_not_ready(Nodes, Max) ->
    Ready = proxy_multicall(Nodes, vmq_cluster, is_ready, []),
    case lists:member(true, Ready) of
        false ->
            ct:pal("cluster is not ready anymore"),
            true;
        _ ->
            ct:pal("still no consensus about cluster state ~p", [Ready]),
            timer:sleep(1000),
            ensure_not_ready(Nodes, Max - 1000)
    end.

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
    ok = set_net_ticktime(NetTickTime),
    vmq_test_utils:setup(),
    application:load(vmq_server),
    vmq_server_cmd:set_config(allow_anonymous, true),
    vmq_server_cmd:set_config(retry_interval, 10),
    vmq_server_cmd:listener_start(Port, []),
    case I of
        1 ->
            % we are the discovery node
            ignore;
        _ ->
            vmq_server_cmd:node_join(DiscoveryNode)
    end,
    vmq_plugin_mgr:enable_module_plugin(auth_on_publish, ?MODULE,
                                        hook_auth_on_publish, 6),
    vmq_plugin_mgr:enable_module_plugin(auth_on_subscribe, ?MODULE,
                                        hook_auth_on_subscribe, 3),

    io:fwrite(user, "vernemq started ~p~n", [node()]).

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

start_slaves(NetTickTime, Ns) ->

    Nodes = [start_slave(NetTickTime, N) || N <- Ns],
    Nodes.

start_slave(NetTickTime, Name) ->
    CodePath = lists:filter(fun filelib:is_dir/1, code:get_path()),
    NodeConfig = [{monitor_master, true},
                  {erl_flags, "-kernel net_ticktime " ++ integer_to_list(NetTickTime)},
                  {startup_functions,
                   [{code, set_path, [CodePath]}]}],
    {ok, Node} = ct_slave:start(host(), Name, NodeConfig),
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

host() ->
    [_Name, Host] = re:split(atom_to_list(node()), "@", [{return, list}]),
    list_to_atom(Host).

set_net_ticktime(NetTickTime) ->
    ct:pal("change net_ticktime on node ~p to ~p initiated", [node(), NetTickTime]),
    case net_kernel:set_net_ticktime(NetTickTime, NetTickTime) of
        unchanged ->
            ct:pal("net_ticktime on node ~p changed", [node()]),
            ok;
        change_initiated ->
            wait_till_net_tick_converged(NetTickTime);
        {ongoing_change_to, _} ->
            wait_till_net_tick_converged(NetTickTime)
    end.

wait_till_net_tick_converged(NetTickTime) ->
    case net_kernel:get_net_ticktime() of
        NetTickTime ->
            ct:pal("net_ticktime on node ~p changed", [node()]),
            ok;
        {ongoing_change_to, NetTickTime} ->
            timer:sleep(1000),
            wait_till_net_tick_converged(NetTickTime)
    end.
