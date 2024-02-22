%% Copyright 2024- Octavo Labs/VerneMQ (https://vernemq.com/)
%% and Individual Contributors.
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

-module(vmq_queue_impl).
-include_lib("vmq_commons/include/vmq_types.hrl").

% Start Queue
-callback start_link(SubscriberId :: subscriber_id(), Clean :: boolean()) ->
    {ok, Pid :: pid()} | ignore | {error, Error :: term()}.

% From session FSMs
-callback active(Queue :: pid()) -> ok.
-callback notify(Queue :: pid()) -> ok.
-callback notify_recv(Queue :: pid()) -> ok.
-callback set_last_waiting_acks(Queue :: pid(), WAcks :: map(), NextMsgId :: msg_id()) ->
    ok.

% vmq_reg, vmq_cluster_com, vmq_shared_subscriptions
-callback enqueue(Queue :: pid(), Msg :: msg()) -> ok.
-callback enqueue_many(Queue :: pid(), Msgs :: list()) -> ok.
-callback enqueue_many(Queue :: pid(), Msgs :: list(), Opts :: map()) -> ok.

% vmq_reg, session FSMs (& misc)
-callback add_session(Queue :: pid(), Session :: pid(), Opts :: map()) ->
    {ok, #{initial_msg_id := msg_id()}}
    | {error, any()}.
-callback get_sessions(Queue :: pid()) -> [pid()].
-callback set_opts(Queue :: pid(), Opts :: list()) -> ok.
-callback get_opts(Queue :: pid()) -> term().
-callback default_opts() -> map().

% Called by MQTT v5 FSM
-callback set_delayed_will(Queue :: pid(), Fun :: fun(), Delay :: integer()) -> ok | term().

% Administrative (vmq_reg, vmq_info_cli)
-callback cleanup(Queue :: pid(), Reason :: term()) -> ok | term().
-callback force_disconnect(Queue :: pid(), Reason :: term()) -> ok | normal | term().
-callback force_disconnect(Queue :: pid(), Reason :: term(), DoCleanup :: boolean()) ->
    ok | normal | term().

% Internal calls for Queue info or status
-callback status(Queue :: pid()) ->
    {
        StateName :: atom(),
        % Mode to be deprecated
        Mode :: fanout,
        TotalStoredMsgs :: non_neg_integer(),
        NrSessions :: non_neg_integer(),
        IsPlugin :: boolean()
    }.
-callback info(Queue :: pid()) ->
    Info :: #{
        is_offline => boolean(),
        is_online => boolean(),
        statename => StateName :: atom(),
        % Mode to be deprecated
        deliver_mode => fanout,
        offline_messages => non_neg_integer(),
        online_messages => non_neg_integer(),
        % num_sessions to be deprecated
        num_sessions => non_neg_integer(),
        is_plugin => IsPlugin :: boolean(),
        sessions => SessionInfo :: list(),
        started_at => StartedAt :: non_neg_integer()
    }.
