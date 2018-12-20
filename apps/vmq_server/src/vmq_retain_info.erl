%% Copyright 2018 Octavo Labs AG Basel Switzerland (http://octavolabs.com)
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
-module(vmq_retain_info).
-behaviour(vmq_ql_query).
-include_lib("vmq_ql/include/vmq_ql.hrl").
-include("vmq_server.hrl").

-export([fields_config/0,
         fold_init_rows/4]).

fields_config() ->
    Base = #vmq_ql_table{
              name           = retain_base,
              depends_on     = [],
              provides       = [mountpoint, topic, payload],
              init_fun       = fun row_init/1,
              include_if_all = true
             },
    [Base].

fold_init_rows(_, Fun, Acc, [Query]) ->
    Topic = re:split(maps:get({topic,equals}, Query, <<"#">>), <<"/">>),
    Payload = maps:get({payload,equals}, Query, undefined),
    MP = maps:get({mp,equals}, Query, ""),
    FoldFun =
        fun({T, #retain_msg{payload=P}}, AccAcc) ->
                case match(Topic, T, Payload, P) of
                    true ->
                        Row = #{topic => iolist_to_binary(vmq_topic:unword(T)),
                                payload => P,
                                mountpoint => MP},
                        Fun(Row, AccAcc);
                    false ->
                        AccAcc
                end;
           ({T, P}, AccAcc) ->
                case match(Topic, T, Payload, P) of
                    true ->
                        Row = #{topic => iolist_to_binary(vmq_topic:unword(T)),
                                payload => P,
                                mountpoint => MP},
                        Fun(Row, AccAcc);
                    false ->
                        AccAcc
                end
        end,
    vmq_retain_srv:match_fold(FoldFun, Acc, MP, Topic).

match(WantTopic, GotTopic, WantPayload, GotPayload) ->
    vmq_topic:match(GotTopic, WantTopic) andalso match_payload(WantPayload, GotPayload).

match_payload(undefined, _) ->
    true;
match_payload(P, P) ->
    true;
match_payload(_ , _) ->
    false.

row_init(Row) -> [Row].
