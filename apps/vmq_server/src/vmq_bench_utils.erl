-module(vmq_bench_utils).

-compile([export_all]).
-compile(nowarn_export_all).

-include("vmq_server.hrl").



publish(Runs, RoutingKey, Payload) ->
    PubFun = fun() -> vmq_reg:publish(false, vmq_reg_trie, <<"PublishClientId">>,
                                      #vmq_msg{mountpoint="",
                                               routing_key = RoutingKey,
                                               payload = Payload,
                                               retain=false,
                                               sg_policy=local_only,
                                               qos=0,
                                               msg_ref=vmq_mqtt_fsm_util:msg_ref(),
                                               properties=#{}})
             end,
    Res = lists:foldl(
      fun(N, Acc) ->
              T1 = erlang:monotonic_time(millisecond),
              [PubFun()|| _ <- lists:seq(1,N)],
              T2 = erlang:monotonic_time(millisecond),
              [{N, T1, T2}|Acc]
      end, [], Runs),
    {ok, lists:reverse(Res)}.
