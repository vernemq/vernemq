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

-module(vmq_info).
-behaviour(vmq_ql_query).
-include("vmq_server.hrl").
-include_lib("vmq_ql/include/vmq_ql.hrl").

-export([fields_config/0,
         fold_init_rows/4]).

%% used by vmq_info_cli
-export([session_info_items/0]).

fields_config() ->
    QueueBase = #vmq_ql_table{
                   name =       queue_base,
                   depends_on = [],
                   provides =   [node, mountpoint, client_id, queue_pid],
                   init_fun =   fun row_init/1,
                   include_if_all = true
                  },
    Queues = #vmq_ql_table{
                   name =       queues,
                   depends_on = [QueueBase],
                   provides = [queue_size,
                               session_pid,
                               is_offline,
                               is_online,
                               statename,
                               deliver_mode,
                               offline_messages,
                               online_messages,
                               num_sessions,
                               clean_session,
                               is_plugin],
                   init_fun = fun queue_row_init/1,
                   include_if_all = true
               },
    Sessions = #vmq_ql_table{
                    name =      sessions,
                    depends_on = [Queues],
                    provides = [user,
                                peer_host,
                                peer_port,
                                protocol,
                                waiting_acks],
                    init_fun = fun session_row_init/1,
                    include_if_all = false
                 },
    Subscriptions = #vmq_ql_table{
                    name =      subscriptions,
                    depends_on = [QueueBase],
                    provides = [topic, qos],
                    init_fun = fun subscription_row_init/1,
                    include_if_all = false
                    },
    MessageRefs = #vmq_ql_table{
                    name =      message_refs,
                    depends_on = [QueueBase],
                    provides = [msg_ref],
                    init_fun = fun message_ref_row_init/1,
                    include_if_all = false
                    },
    Messages = #vmq_ql_table{
                    name =      messages,
                    depends_on = [MessageRefs],
                    provides = [msg_qos, routing_key, dup, payload],
                    init_fun = fun message_row_init/1,
                    include_if_all = false
                    },

    [QueueBase, Queues, Sessions, Subscriptions, MessageRefs, Messages].


session_info_items() ->
    %% used in vmq_info_cli
    lists:flatten([Fields || #vmq_ql_table{provides=Fields} <- fields_config()]).

%% For now we only optimize the exact case with the predicates in a
%% specific order (MP,ClientID).
fold_init_rows(_, Fun, Acc, [#{{mountpoint,equals} := MP,
                               {client_id,equals} := ClientId}]) ->
    case vmq_queue_sup_sup:get_queue_pid({MP, ClientId}) of
        not_found -> [];
        QPid ->
            InitRow = #{node => atom_to_binary(node(),utf8),
                        mountpoint => list_to_binary(MP),
                        '__mountpoint' => MP,
                        client_id => ClientId,
                        queue_pid => QPid},
            [Fun(InitRow, Acc)]
    end;
fold_init_rows(_, Fun, Acc,_) ->
    vmq_queue_sup_sup:fold_queues(
      fun({MP, ClientId}, QPid, AccAcc) ->
              InitRow = #{node => atom_to_binary(node(),utf8),
                          mountpoint => list_to_binary(MP),
                          '__mountpoint' => MP,
                          client_id => ClientId,
                          queue_pid => QPid},
              Fun(InitRow, AccAcc)
      end, Acc).

row_init(Row) ->
    [Row].

queue_row_init(Row) ->
   QPid = maps:get(queue_pid, Row),
   QueueData = vmq_queue:info(QPid),
   case maps:get('sessions', QueueData) of
       [] ->
           % offline queue
           [maps:merge(Row, maps:remove('sessions', QueueData#{clean_session => false}))];
       Sessions ->
           QueueDataWithoutSessions = maps:remove('sessions', QueueData),
           Row1 = maps:merge(Row, QueueDataWithoutSessions),
           lists:foldl(fun({SessionPid, CleanSession}, Acc) ->
                               [maps:merge(Row1, #{session_pid => SessionPid,
                                                   clean_session => CleanSession}) | Acc]
                       end, [], Sessions)
   end.

session_row_init(Row) ->
    case maps:find(session_pid, Row) of
        error ->
            [Row];
        {ok, SessionPid} ->
            {ok, InfoItems} = vmq_mqtt_fsm:info(SessionPid, [user,
                                                             peer_host,
                                                             peer_port,
                                                             protocol,
                                                             waiting_acks]),
            [maps:merge(Row, maps:from_list(InfoItems))]
    end.

subscription_row_init(Row) ->
    SubscriberId = {maps:get('__mountpoint', Row), maps:get(client_id, Row)},
    Subs = vmq_reg:subscriptions_for_subscriber_id(SubscriberId),
    vmq_subscriber:fold(fun({Topic, QoS, _Node}, Acc) ->
                                [maps:merge(Row, #{topic => iolist_to_binary(vmq_topic:unword(Topic)),
                                                   qos => QoS})|Acc]
                        end, [], Subs).

message_ref_row_init(Row) ->
    SubscriberId = {maps:get('__mountpoint', Row), maps:get(client_id, Row)},
    case vmq_plugin:only(msg_store_find, [SubscriberId]) of
        {ok, MsgRefs} ->
            lists:foldl(fun(MsgRef, Acc) ->
                                [maps:merge(Row, #{'__msg_ref' => MsgRef,
                                                   'msg_ref' => list_to_binary(base64:encode_to_string(MsgRef))})|Acc]
                        end, [], MsgRefs);
        {error, _} ->
            [Row]
    end.

message_row_init(Row) ->
    SubscriberId = {maps:get('__mountpoint', Row), maps:get(client_id, Row)},
    MsgRef = maps:get('__msg_ref', Row),
    case vmq_plugin:only(msg_store_read, [SubscriberId, MsgRef]) of
        {ok, #vmq_msg{msg_ref=MsgRef, qos=QoS,
                      dup=Dup, routing_key=RoutingKey,
                      payload=Payload}} ->
            [maps:merge(Row, #{msg_qos => QoS,
                               routing_key => iolist_to_binary(vmq_topic:unword(RoutingKey)),
                               dup => Dup,
                               payload => Payload})];
        _ ->
            [Row]
    end.
