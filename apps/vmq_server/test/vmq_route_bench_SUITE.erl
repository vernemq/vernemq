-module(vmq_route_bench_SUITE).

%% Note: This directive should only be used in test suites.
-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include("../src/vmq_server.hrl").

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

suite() ->
    [{timetrap,{minutes,10}}].

init_per_suite(Config) ->
    S = vmq_test_utils:get_suite_rand_seed(),
    %lager:start(),
    %% this might help, might not...
    os:cmd(os:find_executable("epmd")++" -daemon"),
    {ok, Hostname} = inet:gethostname(),
    case net_kernel:start([list_to_atom("runner@"++Hostname), shortnames]) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,
    lager:info("node name ~p", [node()]),
    [S|Config].
    %% vmq_test_utils:setup(),
    %% vmq_server_cmd:set_config(allow_anonymous, true),
    %% vmq_server_cmd:set_config(retry_interval, 10),
    %% vmq_server_cmd:set_config(max_client_id_size, 1000),
    %% vmq_server_cmd:listener_start(1888, [{allowed_protocol_versions, "3,4,5"}]),
    %% Config.

end_per_suite(_Config) ->
    application:stop(lager),
    ok.

init_per_group(_GroupName, Config) ->
    Config.

end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(Case, Config) ->
    vmq_test_utils:seed_rand(Config),
    Nodes = vmq_cluster_test_utils:pmap(
              fun({N, P}) ->
                      Node = vmq_cluster_test_utils:start_node(N, Config, Case),
                      {ok, _} = rpc:call(Node, vmq_server_cmd, listener_start,
                                         [P, []]),
                      %% allow all
                      ok = rpc:call(Node, vmq_auth, register_hooks, []),
                      {Node, P}
              end, [{test1, 18883},
                    {test2, 18884},
                    {test3, 18885}]),
    {CoverNodes, _} = lists:unzip(Nodes),
    {ok, _} = ct_cover:add_nodes(CoverNodes),
    [{nodes, Nodes},{nodenames, [test1, test2, test3]}|Config].

end_per_testcase(_TestCase, _Config) ->
    vmq_cluster_test_utils:pmap(fun(Node) -> ct_slave:stop(Node) end,
                                [test1, test2, test3]),
    ok.

groups() ->
    [].

all() ->
    [measure_ssubs
     %%measure_subs
    ].


%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

bench_ssubs(Config) ->
    ok = ensure_cluster(Config),
    [N1,N2,_N3] = Nodes = nodes_(Config),

    set_shared_subs_policy(local_only, nodenames(Config)),
    LocalSubs = 12,
    RemoteSubs = 132,
    _N1Subs = connect_subscribers(<<"$share/group/sharedtopic">>, LocalSubs, [N1]),
    _N2Subs = connect_subscribers(<<"$share/group/sharedtopic">>, RemoteSubs, [N2]),

    {N1Name, _} = N1,
    ok = wait_until_converged(Nodes,
                              fun(N) ->
                                      rpc:call(N, vmq_reg, total_subscriptions, [])
                              end, [{total, LocalSubs+RemoteSubs}]),
    {ok, Res} = rpc:call(N1Name, vmq_bench_utils, publish, [[10000,10000,10000], [<<"sharedtopic">>], <<"payload">>]),
    lists:foreach(
      fun({N, T1, T2}) ->
              iotime(N, sharedsubs, T1, T2)
      end, Res).



connect_subscribers(_Topic, 0, _) -> [];
connect_subscribers(Topic, Number, Nodes) ->
    [begin

         {_, Port} = random_node(Nodes),
         Connect = packet:gen_connect("subscriber-" ++ integer_to_list(I) ++ "-node-" ++
                                          integer_to_list(Port),
                                      [{keepalive, 60}, {clean_session, true}]),
         Connack = packet:gen_connack(0),
         Subscribe = packet:gen_subscribe(1, [Topic], 1),
         Suback = packet:gen_suback(1, 1),
         %% TODO: make it connect to random node instead
         {ok, Socket} = packet:do_client_connect(Connect, Connack, [{port, Port}]),
         ok = gen_tcp:send(Socket, Subscribe),
         ok = packet:expect_packet(Socket, "suback", Suback),
         Socket
     end || I <- lists:seq(1,Number)].

ensure_cluster(Config) ->
    vmq_cluster_test_utils:ensure_cluster(Config).

wait_until_converged(Nodes, Fun, ExpectedReturn) ->
    {NodeNames, _} = lists:unzip(Nodes),
    vmq_cluster_test_utils:wait_until(
      fun() ->
              lists:all(fun(X) -> X == true end,
                        vmq_cluster_test_utils:pmap(
                          fun(Node) ->
                                  ExpectedReturn == Fun(Node)
                          end, NodeNames))
      end, 60*2, 500).

nodenames(Config) ->
    {NodeNames, _} = lists:unzip(nodes_(Config)),
    NodeNames.

nodes_(Config) ->
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    Nodes.

random_node(Nodes) ->
    lists:nth(rand:uniform(length(Nodes)), Nodes).

set_shared_subs_policy(Policy, Nodes) ->
    lists:foreach(
      fun(N) ->
              {ok, []} = rpc:call(N, vmq_server_cmd, set_config, [shared_subscription_policy, Policy])
      end,
      Nodes).

bench_ssubs2(_Config) ->
    SubscribeTopic = [<<"$share">>, <<"group">>, <<"topic">>],

    RoutingKey = [<<"topic">>],
    PubFun = fun() -> vmq_reg:publish(false, vmq_reg_trie, <<"PublishClientId">>,
                                      #vmq_msg{mountpoint="",
                                               routing_key = RoutingKey,
                                               payload= <<"hello">>,
                                               retain=false,
                                               sg_policy=local_only,
                                               qos=0,
                                               msg_ref=vmq_mqtt_fsm_util:msg_ref(),
                                               properties=#{}})
             end,
    lists:foldl(
      fun(NSubs, Acc) ->
              [ setup_subscriber([{SubscribeTopic, 0}]) || _ <- lists:seq(1,NSubs) ],
              %% wait a bit to give VerneMQ a change to ensure the
              %% subscription has been registered.
              timer:sleep(200),
              io:format(user, "Subscribers: ~p~n", [Acc+NSubs]),
              lists:foreach(
                fun(N) ->
                        T1 = erlang:monotonic_time(millisecond),
                        [PubFun()|| _ <- lists:seq(1,N)],
                        T2 = erlang:monotonic_time(millisecond),
                        iotime(N, shared_sub, T1, T2)
                end, [16000,32000,64000]),
              Acc + NSubs
      end, 0, [1, 7, 24, 32]).



    %% Fold = vmq_reg_view:fold(vmq_reg_trie, {"", <<"PublishClientId">>}, RoutingKey, fun(A,B,Acc) -> [{A,B}|Acc] end, []),
    %% io:format(user, "XXX fold: ~p~n", [Fold]),


setup_subscriber(Subs) ->
    ClientId = erlang:list_to_binary(lists:flatten(io_lib:format("~p", [erlang:unique_integer()]))),
    setup_subscriber({"", ClientId}, Subs).

setup_subscriber(SubscriberId, Subs) ->
    SessionPid = spawn_link(fun() -> mock_session(undefined) end),
    QueueOpts = maps:merge(vmq_queue:default_opts(),
                           #{max_online_messages => 1,
                             max_offline_messages => 1,
                             cleanup_on_disconnect => true}),

    {ok, #{session_present := false,
           queue_pid := _QPid}} = vmq_reg:register_subscriber_(SessionPid, SubscriberId, false, QueueOpts, 10),
    SubAckList = [ QoS || {_, QoS} <- Subs],
    {ok, SubAckList} = vmq_reg:subscribe(false, SubscriberId, Subs).

%%
mock_session(_Parent) ->
    receive
        %% {to_session_fsm, {mail, QPid, new_data}} ->
        %%     %%io:format(user, "XXX: mail new data~n", []),
        %%     vmq_queue:active(QPid),
        %%     mock_session(Parent);
        %% {to_session_fsm, {mail, QPid, Msgs, _, _}} ->
        %%     vmq_queue:notify(QPid),
        %%     timer:sleep(100),
        %%     %%io:format(user, "XXX: mail from queue ~p~n", [Msgs]),
        %%     case Parent of
        %%         undefined -> ok;
        %%         _ when is_pid(Parent) ->
        %%             Parent ! {received, QPid, Msgs}
        %%     end,
        %%     mock_session(Parent);
        {go_down_in, Ms} ->
            timer:sleep(Ms)
        %%_ -> % go down
          %%  ok
    end.


iotime(Num, Type,  T1, T2) ->
    io:format(user, "~p ~p: Elapsed time ~ps~n", [Num, Type, (T2 - T1)/1000]).
