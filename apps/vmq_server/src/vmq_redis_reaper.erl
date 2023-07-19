-module(vmq_redis_reaper).

-behaviour(gen_server).

-include("vmq_server.hrl").

%% API functions
-export([start_link/1]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {shard_clients, interval, max_msgs, max_clients, dead_node}).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link(DeadNode) ->
    gen_server:start_link(?MODULE, DeadNode, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================
init(DeadNode) ->
    Interval = application:get_env(vmq_server, redis_reaper_interval, 100),
    MaxClients = application:get_env(vmq_server, redis_reaper_max_clients, 20),
    MaxMsgs = application:get_env(vmq_server, redis_main_queue_max_msgs, 5),
    RedisMsgShards = application:get_env(vmq_server, num_redis_main_queue_shards, 1),

    ShardClients = [
        list_to_atom("redis_queue_consumer_client_" ++ integer_to_list(Id))
     || Id <- lists:seq(0, RedisMsgShards - 1)
    ],

    erlang:send_after(0, self(), reap_messages),

    {ok, #state{
        interval = Interval,
        max_clients = MaxClients,
        max_msgs = MaxMsgs,
        shard_clients = ShardClients,
        dead_node = DeadNode
    }}.

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
handle_info(
    reap_messages,
    #state{
        dead_node = DeadNode,
        max_msgs = MaxMsgs,
        shard_clients = ShardClients,
        interval = Interval
    } = State
) ->
    MainQueue = "mainQueue::" ++ atom_to_list(DeadNode),

    NextStep = lists:foldl(
        fun(RedisClient, Acc) ->
            case
                vmq_redis:query(
                    RedisClient,
                    [
                        ?FCALL,
                        ?POLL_MAIN_QUEUE,
                        1,
                        MainQueue,
                        MaxMsgs
                    ],
                    ?FCALL,
                    ?POLL_MAIN_QUEUE
                )
            of
                {ok, undefined} ->
                    Acc;
                {ok, Msgs} ->
                    lists:foreach(
                        fun([SubBin, MsgBin, TimeInQueue]) ->
                            vmq_metrics:pretimed_measurement(
                                {?MODULE, time_spent_in_main_queue},
                                binary_to_integer(TimeInQueue)
                            ),
                            case binary_to_term(SubBin) of
                                {_, _CId} = SId ->
                                    case vmq_reg:migrate_offline_queue(SId, DeadNode) of
                                        {error, _} ->
                                            ignore;
                                        LocalNode when LocalNode == node() ->
                                            {SubInfo, Msg} = binary_to_term(MsgBin),
                                            vmq_reg:enqueue_msg({SId, SubInfo}, Msg);
                                        RemoteNode ->
                                            vmq_redis_queue:enqueue(RemoteNode, SubBin, MsgBin)
                                    end;
                                RandSubs when is_list(RandSubs) ->
                                    vmq_shared_subscriptions:publish_to_group(
                                        binary_to_term(MsgBin),
                                        RandSubs,
                                        {0, 0}
                                    );
                                UnknownMsg ->
                                    lager:error("Unknown Msg in Redis Main Queue : ~p", [UnknownMsg])
                            end
                        end,
                        Msgs
                    ),
                    reap_messages;
                Res ->
                    lager:warning("~p", [Res]),
                    Acc
            end
        end,
        reap_subscribers,
        ShardClients
    ),

    erlang:send_after(Interval, self(), NextStep),
    {noreply, State};
handle_info(
    reap_subscribers,
    #state{
        dead_node = DeadNode,
        max_msgs = MaxClients,
        interval = Interval
    } = State
) ->
    case
        vmq_redis:query(
            vmq_redis_client,
            [
                ?FCALL,
                ?REAP_SUBSCRIBERS,
                0,
                DeadNode,
                node(),
                MaxClients
            ],
            ?FCALL,
            ?REAP_SUBSCRIBERS
        )
    of
        {ok, ClientList} when is_list(ClientList) ->
            lists:foreach(
                fun([MP, ClientId]) ->
                    SubscriberId = {binary_to_list(MP), ClientId},
                    {ok, _QueuePresent, _QPid} = vmq_queue_sup_sup:start_queue(SubscriberId, false)
                end,
                ClientList
            ),
            erlang:send_after(Interval, self(), reap_subscribers),
            {noreply, State};
        {ok, undefined} ->
            {stop, normal, State};
        Res ->
            lager:warning("~p", [Res]),
            {stop, normal, State}
    end;
handle_info(_Info, State) ->
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
