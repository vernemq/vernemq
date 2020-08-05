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
%%
-module(vmq_systree).
-include("vmq_server.hrl").

-behaviour(gen_server).

-include("vmq_metrics.hrl").

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).
-define(DEFAULT_INTERVAL, 20000).
-define(DEFAULT_PREFIX, [<<"$SYS">>, list_to_binary(atom_to_list(node()))]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    Enabled = vmq_config:get_env(systree_enabled, false),
    {ok, Enabled, 1000}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(timeout, false) ->
    Enabled = vmq_config:get_env(systree_enabled, false),
    {noreply, Enabled, 30000};
handle_info(timeout, true) ->
    case vmq_config:get_env(systree_enabled, false) of
        true ->
            Interval = vmq_config:get_env(systree_interval, ?DEFAULT_INTERVAL),
            Prefix = vmq_config:get_env(systree_prefix, ?DEFAULT_PREFIX),
            RegView =vmq_config:get_env(systree_reg_view,
                                        vmq_config:get_env(default_reg_view, vmq_reg_trie)),
            MP = vmq_config:get_env(systree_mountpoint, ""),
            %% We have to pass in something looking like a
            %% subscriberid to the publish function.
            ClientId = ?INTERNAL_CLIENT_ID,
            MsgTmpl = #vmq_msg{
                         mountpoint=MP,
                         qos=vmq_config:get_env(systree_qos, 0),
                         retain=vmq_config:get_env(systree_retain, false),
                         sg_policy=vmq_config:get_env(shared_subscription_policy, prefer_local)
                        },
            CAPPublish = true,
            lists:foreach(
              fun
                  ({#metric_def{type=histogram, name=Metric}, {Count, Sum, Buckets}}) ->
                      SMetric = atom_to_list(Metric),
                      CountTmp = {"_count", Count},
                      SumTmp = {"_sum", Sum},
                      Tmp = maps:fold(
                              fun(Bucket, BucketValue, Acc) ->
                                      [{case Bucket of
                                            infinity -> "_bucket_inf";
                                            _ -> "_bucket_" ++ val(Bucket)
                                        end,
                                        BucketValue} | Acc]
                              end, [CountTmp, SumTmp], Buckets),
                      lists:foreach(
                        fun({Suffix, BucketValue}) ->
                                vmq_reg:publish(CAPPublish, RegView, ClientId,
                                                MsgTmpl#vmq_msg{
                                                  routing_key=key(Prefix, SMetric ++ Suffix),
                                                  payload=val(BucketValue),
                                                  msg_ref=vmq_mqtt_fsm_util:msg_ref()
                                                 })
                        end, Tmp);

                  ({#metric_def{name=Metric}, Val}) ->
                      vmq_reg:publish(CAPPublish, RegView, ClientId, MsgTmpl#vmq_msg{
                                                                       routing_key=key(Prefix, Metric),
                                                                       payload=val(Val),
                                                                       msg_ref=vmq_mqtt_fsm_util:msg_ref()
                                                                      })
              end, vmq_metrics:metrics()),
            {noreply, true, Interval};
        false ->
            {noreply, false, 30000}
    end;
handle_info(Info, State) ->
    lager:warning("vmq_systree received unexpected message ~p~n", [Info]),
    {noreply, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
        {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

key(Prefix, Metric) when is_atom(Metric) ->
    key(Prefix, atom_to_list(Metric));
key(Prefix, Metric) ->
    Prefix ++ re:split(Metric, "_").

val(V) when is_integer(V) -> integer_to_binary(V);
val(V) when is_float(V) -> float_to_binary(V);
val(_) -> <<"0">>.
