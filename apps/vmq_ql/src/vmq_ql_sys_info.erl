%% Copyright 2018 Erlio GmbH Basel Switzerland (http://erl.io)
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

-module(vmq_ql_sys_info).
-behaviour(vmq_ql_query).
-include("vmq_ql.hrl").

-export([fields_config/0,
         fold_init_rows/4]).

fields_config() ->
    ProcessBase = #vmq_ql_table{
                   name =       process_base,
                   depends_on = [],
                   provides =   [pid, status, priority, message_queue_len, total_heap_size,
                                 heap_size, stack_size, reductions],
                   init_fun =   fun row_init/1,
                   include_if_all = true
                  },

    [ProcessBase].

fold_init_rows(_, Fun, Acc, _) ->
    lists:foldl(
      fun(Pid, AccAcc) ->
              InitRow = #{node => atom_to_binary(node(), utf8),
                          pid => Pid},
              Fun(InitRow, AccAcc)
      end, Acc, erlang:processes()).

row_init(Row) ->
    Pid = maps:get(pid, Row),
    ProcInfo = [status, priority, message_queue_len, total_heap_size,
                heap_size, stack_size, reductions],
    case erlang:process_info(Pid, ProcInfo) of
        undefined ->
            [];
        Infos ->
            [maps:merge(Row, maps:from_list(Infos))]
    end.
