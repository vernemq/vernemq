-module(vmq_bench_metrics).

-export([do_work/0,
         do_work_par/0]).

do_work() ->
    Funs = [fun vmq_metrics:incr_socket_open/0,
            fun vmq_metrics:incr_socket_close/0,
            fun vmq_metrics:incr_socket_error/0,
            fun vmq_metrics:incr_mqtt_connect_received/0,
            fun vmq_metrics:incr_mqtt_publish_received/0,
            fun vmq_metrics:incr_mqtt_error_auth_publish/0,
            fun vmq_metrics:incr_mqtt_error_auth_subscribe/0,
            fun vmq_metrics:incr_mqtt_error_invalid_msg_size/0,
            fun vmq_metrics:incr_mqtt_error_publish/0,
            fun vmq_metrics:incr_mqtt_error_invalid_pubcomp/0],
    lists:foreach(
      fun(_) ->
              [F() || F <- Funs]
      end, lists:seq(1,1000)).

do_work_par() ->
    Funs = [fun vmq_metrics:incr_socket_open/0,
            fun vmq_metrics:incr_socket_open/0,
            fun vmq_metrics:incr_socket_open/0,
            fun vmq_metrics:incr_socket_open/0,
            fun vmq_metrics:incr_socket_open/0,
            fun vmq_metrics:incr_socket_open/0,
            fun vmq_metrics:incr_socket_open/0,
            fun vmq_metrics:incr_socket_open/0,
            fun vmq_metrics:incr_socket_open/0,
            fun vmq_metrics:incr_socket_open/0],
    Caller = self(),
    Ref = make_ref(),
    lists:foreach(
      fun(_) ->
              spawn(
                fun() ->
                        lists:foreach(
                          fun(_) ->
                              [F() || F <- Funs]
                          end, lists:seq(1,100)),
                        Caller ! {Ref, done}
                end)
      end, lists:seq(1,10000)),
    wait_until_done(Ref, 10000).

wait_until_done(_, 0) ->
    done;
wait_until_done(Ref, ToGo) ->
    receive
        {Ref, done} ->
            wait_until_done(Ref, ToGo - 1)
    end.
