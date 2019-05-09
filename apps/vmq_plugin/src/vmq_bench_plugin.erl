
-module(vmq_bench_plugin).

-export([do_work/0,init/0]).

-export([my_hook_1/5]).
-export([my_hook_2/5]).
-export([my_hook_3/5]).

my_hook_1(_,_,_,_,_) ->
    next.
my_hook_2(_,_,_,_,_) ->
    next.
my_hook_3(_,_,_,_,_) ->
    ok.

init() ->
    vmq_plugin_mgr:enable_module_plugin(my_hook, ?MODULE, my_hook_1, 5),
    vmq_plugin_mgr:enable_module_plugin(my_hook, ?MODULE, my_hook_2, 5),
    vmq_plugin_mgr:enable_module_plugin(my_hook, ?MODULE, my_hook_3, 5).

do_work() ->
    Funs = [fun() -> vmq_plugin:all_till_ok(my_hook, [a,b,c,d,e]) end,
            fun() -> vmq_plugin:all_till_ok(my_hook, [b,c,d,e,f]) end,
            fun() -> vmq_plugin:all_till_ok(my_hook, [c,d,e,f,g]) end,
            fun() -> vmq_plugin:all_till_ok(my_hook, [d,e,f,g,h]) end,
            fun() -> vmq_plugin:all_till_ok(my_hook, [e,f,g,h,i]) end],
    lists:foreach(
      fun(_) ->
              [F() || F <- Funs]
      end, lists:seq(1,1000)).
