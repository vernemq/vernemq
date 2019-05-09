-module(vmq_bench_bcrypt).

-export([do_work/3]).

do_work(Verifications, ProcessCount, Fun) ->
    Caller = self(),
    Ref = make_ref(),
    lists:foreach(
      fun(_) ->
              spawn(
                fun() ->
                        lists:foreach(
                          fun(_) ->
                                  Fun()
                          end, lists:seq(1,Verifications)),
                        Caller ! {Ref, done}
                end)
      end, lists:seq(1,ProcessCount)),
    wait_until_done(Ref, ProcessCount).

wait_until_done(_, 0) ->
    done;
wait_until_done(Ref, ToGo) ->
    receive
        {Ref, done} ->
            wait_until_done(Ref, ToGo - 1)
    end.
