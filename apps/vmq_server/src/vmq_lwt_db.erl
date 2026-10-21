%% Copyright 2018-2024 Octavo Labs/VerneMQ (https://vernemq.com/)
%% and Individual Contributors.
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

%% Replicated last wills (opt-in, `replicate_lwt = on`).
%%
%% A will normally lives only in the session process, so it is lost when the
%% node holding the session dies without running any code. With replication on,
%% every accepted will is also stored in the cluster metadata, and fire/1 lets a
%% surviving node publish the wills of a node that is known to be gone. A node
%% that restarts under the same name publishes its previous incarnation's wills
%% itself with fire_stale/0.
%%
%% Entries are keyed by {SubscriberId, Node}: a node only writes and deletes its
%% own keys, so a session moving between nodes never races another node's write.
%% The exception is fire/1, which removes a dead node's keys.
-module(vmq_lwt_db).
-include_lib("vmq_commons/include/vmq_types.hrl").
-include_lib("kernel/include/logger.hrl").
-include("vmq_server.hrl").

-export([
    store/3,
    session_down/2,
    fire/1,
    fire_stale/0
]).

-define(LWT_DB, {vmq, lwt}).

%% Called by the session once its will is accepted.
-spec store(subscriber_id(), username(), msg()) -> ok.
store(SubscriberId, User, Msg) ->
    _ =
        case enabled() of
            true -> vmq_metadata:put(?LWT_DB, {SubscriberId, node()}, {self(), User, Msg});
            false -> ok
        end,
    ok.

%% Called by the queue for every session that ends on this node, however it
%% ended: from here on the session's own code (or nothing) handled the will.
-spec session_down(subscriber_id(), pid()) -> ok.
session_down(SubscriberId, SessionPid) ->
    _ =
        case enabled() of
            true -> delete({SubscriberId, node()}, SessionPid);
            false -> ok
        end,
    ok.

%% Publishes, from this node, the wills of the sessions that were online on
%% DeadNode and whose client has not registered anywhere since. Only call this
%% for a node that is really gone: in a netsplit its clients are still connected,
%% and their wills would be false. Returns {Published, Skipped}.
%%
%% Limitations: an MQTT 5 Will Delay Interval is not honoured here (the will is
%% published at once), and a delayed will that was already pending in a queue
%% on DeadNode is lost.
-spec fire(node()) -> {non_neg_integer(), non_neg_integer()}.
fire(DeadNode) when DeadNode =/= node() ->
    fire(DeadNode, fun(_SessionPid) -> true end).

%% Publishes the wills this node stored before it was restarted under the same
%% name (a crashed container that comes back with the same address): sessions
%% of the previous incarnation are gone, but their entries are still here.
%% Entries of sessions that are alive now are left alone, and so are clients
%% that have registered again since. Returns {Published, Skipped}.
-spec fire_stale() -> {non_neg_integer(), non_neg_integer()}.
fire_stale() ->
    fire(node(), fun(SessionPid) -> not is_alive(SessionPid) end).

fire(Node, IsGone) ->
    Entries = vmq_metadata:fold(
        ?LWT_DB,
        fun
            ({{SubscriberId, N}, {SessionPid, _, _} = Will}, Acc) when N == Node ->
                case IsGone(SessionPid) of
                    true -> [{SubscriberId, Will} | Acc];
                    false -> Acc
                end;
            (_, Acc) ->
                Acc
        end,
        []
    ),
    Result = lists:foldl(
        fun({SubscriberId, {SessionPid, User, Msg}}, {Published, Skipped}) ->
            Acc =
                case registered_on(Node, SubscriberId) of
                    true ->
                        publish(SubscriberId, User, Msg),
                        {Published + 1, Skipped};
                    false ->
                        {Published, Skipped + 1}
                end,
            delete({SubscriberId, Node}, SessionPid),
            Acc
        end,
        {0, 0},
        Entries
    ),
    ?LOG_INFO("published ~p wills of node ~p, skipped ~p (client reconnected)", [
        element(1, Result), Node, element(2, Result)
    ]),
    Result.

%% Deletes the entry only if it still belongs to that session, so that a newer
%% session's will stored in the meantime is kept.
delete(Key, SessionPid) ->
    case vmq_metadata:get(?LWT_DB, Key) of
        {SessionPid, _, _} -> vmq_metadata:delete(?LWT_DB, Key);
        _ -> ok
    end.

%% A pid of a previous incarnation of this node is not a local process any more.
is_alive(Pid) ->
    try
        is_process_alive(Pid)
    catch
        error:badarg -> false
    end.

enabled() ->
    vmq_config:get_env(replicate_lwt, false).

%% A client that reconnected elsewhere was remapped to that node by
%% vmq_reg:maybe_remap_subscriber/2, so its will must not be sent.
registered_on(Node, SubscriberId) ->
    case vmq_subscriber_db:read(SubscriberId) of
        undefined -> false;
        Subs -> lists:member(Node, vmq_subscriber:get_nodes(Subs))
    end.

publish({_, ClientId} = SubscriberId, User, Msg0) ->
    #vmq_msg{qos = QoS, routing_key = Topic, payload = Payload, retain = IsRetain} = Msg0,
    Msg = Msg0#vmq_msg{msg_ref = vmq_mqtt_fsm_util:msg_ref()},
    RegView = vmq_config:get_env(default_reg_view, vmq_reg_trie),
    %% The dead node may still be a cluster member, so the cluster is not
    %% ready; publish regardless, as the session would have.
    case vmq_reg:publish(true, RegView, ClientId, Msg) of
        {ok, _} ->
            _ = vmq_plugin:all(on_publish, [User, SubscriberId, QoS, Topic, Payload, IsRetain]),
            ok;
        Error ->
            ?LOG_WARNING("can't publish replicated will of ~p: ~p", [SubscriberId, Error])
    end.
