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

-spec start(_,_) -> {'error',_} | {'ok',pid()} | {'ok', pid(), _}.
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
    io:format(
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
                                             {ok, M} = hipe:c(M, [o3])
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
