%% Copyright 2016 Erlio GmbH Basel Switzerland (http://erl.io)
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
-module(vmq_queue_sup_sup).

-behaviour(supervisor).

%% API
-export([start_link/3,
         start_queue/1,
         get_queue_pid/1,
         fold_queues/2,
         summary/0,
         nr_of_queues/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link(Shutdown, MaxR, MaxT) ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, [Shutdown, MaxR, MaxT]).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([Shutdown, MaxR, MaxT]) ->
    NumSups = application:get_env(vmq_server, queue_sup_sup_children, 25),
    SupFlags =
        {one_for_one, 1, 5},
    ChildSpec =
        fun(RegName, QueueTabId) ->
                {{RegName, QueueTabId},
                 {vmq_queue_sup, start_link, [Shutdown, RegName, QueueTabId, MaxR, MaxT]},
                 permanent, 5000, supervisor, [vmq_queue_sup]}
        end,
                    
    ChildSpecs =
        [ChildSpec(
           binary_to_atom(
             <<"vmq_queue_sup_", (integer_to_binary(N))/binary>>, utf8),
           binary_to_atom(
             <<"vmq_queue_sup_", (integer_to_binary(N))/binary, "_tab">>, utf8))
         || N <- lists:seq(1,NumSups)],
    {ok, {SupFlags, ChildSpecs}}.
%%====================================================================
%% Internal functions
%%====================================================================

start_queue(SubscriberId) ->
    start_queue(SubscriberId, true).

start_queue(SubscriberId, Clean) ->
    %% Always map the same subscriber to the same supervisor
    %% as we may have concurrent attempts at setting up the
    %% queue. vmq_queue_sup:start_queue/3 prevents duplicates
    %% as long as it's under the same supervisor.
    SupName =
        subscriberid_to_supname(SubscriberId),
    vmq_queue_sup:start_queue(SupName, SubscriberId, Clean).

num_child_sups() ->
    application:get_env(vmq_server, queue_sup_sup_children, 25).

subscriberid_to_supname(SubscriberId) ->
    SupName = <<"vmq_queue_sup_",
                 (integer_to_binary(erlang:phash2(SubscriberId, num_child_sups()) + 1))/binary>>,
    erlang:binary_to_atom(SupName, utf8).

gen_tabid(N) ->
    TabId = <<"vmq_queue_sup_", (integer_to_binary(N))/binary, "_tab">>,
    erlang:binary_to_atom(TabId, utf8).

subscriberid_to_tabid(SubscriberId) ->
    TabId = gen_tabid(erlang:phash2(SubscriberId, num_child_sups()) + 1),
    erlang:binary_to_atom(TabId, utf8).

get_queue_pid(SubscriberId) ->
    QueueTabId = subscriberid_to_tabid(SubscriberId),
    vmq_queue_sup:get_queue_pid(QueueTabId, SubscriberId).

fold_queues(FoldFun, Acc) ->
    lists:foldl(
      fun(QueueTabId, AccAcc) ->
              vmq_queue_sup:fold_queues(QueueTabId, FoldFun, AccAcc)
      end,
      Acc, 
      child_tab_ids()).

summary() ->
    fold_queues(
      fun(_, QPid, {AccOnline, AccWait, AccDrain, AccOffline, AccStoredMsgs} = Acc) ->
              try vmq_queue:status(QPid) of
                  {_, _, _, _, true} ->
                      %% this is a queue belonging to a plugin... ignore it
                      Acc;
                  {online, _, TotalStoredMsgs, _, _} ->
                      {AccOnline + 1, AccWait, AccDrain, AccOffline, AccStoredMsgs + TotalStoredMsgs};
                  {wait_for_offline, _, TotalStoredMsgs, _, _} ->
                      {AccOnline, AccWait + 1, AccDrain, AccOffline, AccStoredMsgs + TotalStoredMsgs};
                  {drain, _, TotalStoredMsgs, _, _} ->
                      {AccOnline, AccWait, AccDrain + 1, AccOffline, AccStoredMsgs + TotalStoredMsgs};
                  {offline, _, TotalStoredMsgs, _, _} ->
                      {AccOnline, AccWait, AccDrain, AccOffline + 1, AccStoredMsgs + TotalStoredMsgs}
              catch
                  _:_ ->
                      %% queue stopped in the meantime, that's ok.
                      Acc
              end
      end, {0, 0, 0, 0, 0}).

child_tab_ids() ->
    [ gen_tabid(N) || N <- lists:seq(1, num_child_sups()) ].

nr_of_queues() ->
    lists:sum(
      [vmq_queue_sup:nr_of_queues(QueueTabId) || QueueTabId <-child_tab_ids()]
     ).
