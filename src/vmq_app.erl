-module(vmq_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% HiPE compilation uses multiple cores anyway, but some bits are
%% IO-bound so we can go faster if we parallelise a bit more. In
%% practice 2 processes seems just as fast as any other number > 1,
%% and keeps the progress bar realistic-ish.
-define(HIPE_PROCESSES, 2).

%% ===================================================================
%% Application callbacks
%% ===================================================================

-spec start(_,_) -> 'ignore' | {'error',_} | {'ok',pid()}.
start(_StartType, _StartArgs) ->
    Success = maybe_hipe_compile(),
    warn_if_hipe_compilation_failed(Success),
    vmq_sup:start_link().

-spec stop(_) -> 'ok'.
stop(_State) ->
    ok.


%% HiPE compilation happens before we have log handlers - so we have
%% to io:format/2, it's all we can do.

maybe_hipe_compile() ->
    {ok, Want} = application:get_env(vmq_server, hipe_compile),
    Can = code:which(hipe) =/= non_existing,
    case {Want, Can} of
        {true,  true}  -> hipe_compile(),
                          true;
        {true,  false} -> false;
        {false, _}     -> true
    end.

warn_if_hipe_compilation_failed(true) ->
    ok;
warn_if_hipe_compilation_failed(false) ->
    error_logger:warning_msg(
      "Not HiPE compiling: HiPE not found in this Erlang installation.~n").

%% HiPE compilation happens before we have log handlers and can take a
%% long time, so make an exception to our no-stdout policy and display
%% progress via stdout.
hipe_compile() ->
    {ok, HipeModulesAll} = application:get_env(vmq_server, hipe_modules),
    HipeModules = [HM || HM <- HipeModulesAll, code:which(HM) =/= non_existing],
    Count = length(HipeModules),
    io:format("~nHiPE compiling:  |~s|~n                 |",
              [string:copies("-", Count)]),
    T1 = erlang:now(),
    PidMRefs = [spawn_monitor(fun () -> [begin
                                             {ok, M} = hipe:c(M, [o1]),
                                             io:format("#")
                                         end || M <- Ms]
                              end) ||
                   Ms <- split(HipeModules, ?HIPE_PROCESSES)],
    [receive
         {'DOWN', MRef, process, _, normal} -> ok;
         {'DOWN', MRef, process, _, Reason} -> exit(Reason)
     end || {_Pid, MRef} <- PidMRefs],
    T2 = erlang:now(),
    io:format("|~n~nCompiled ~B modules in ~Bs~n",
              [Count, timer:now_diff(T2, T1) div 1000000]).

split(L, N) -> split0(L, [[] || _ <- lists:seq(1, N)]).

split0([],       Ls)       -> Ls;
split0([I | Is], [L | Ls]) -> split0(Is, Ls ++ [[I | L]]).
