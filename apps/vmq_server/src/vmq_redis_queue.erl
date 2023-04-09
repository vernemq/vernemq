-module(vmq_redis_queue).
-author("dhruvjain").

-behaviour(gen_server).

-include("vmq_server.hrl").

%% API functions
-export([start_link/2, enqueue/3]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {shard, interval, timer}).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link(RegName, RedisNode) ->
    gen_server:start_link({local, RegName}, ?MODULE, [RedisNode], []).

enqueue(Node, SubscriberBin, MsgBin) when is_binary(SubscriberBin) and is_binary(MsgBin) ->
    RedisClient = gen_redis_producer_client(SubscriberBin),
    MainQueueKey = "mainQueue" ++ "::" ++ atom_to_list(Node),
    case
        vmq_redis:query(
            RedisClient,
            [
                ?FCALL,
                ?ENQUEUE_MSG,
                1,
                MainQueueKey,
                SubscriberBin,
                MsgBin
            ],
            ?FCALL,
            ?ENQUEUE_MSG
        )
    of
        {ok, MainQueueSize} ->
            vmq_metrics:pretimed_measurement(
                {redis_main_queue, size, [{broker_node, Node}, {redis_client, RedisClient}]},
                binary_to_integer(MainQueueSize)
            ),
            ok;
        {error, _} = Res ->
            Res
    end.

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================
init([RedisShard]) ->
    {ok, init_state(RedisShard, #state{})}.
init_state(RedisShard, State) ->
    Interval = application:get_env(vmq_server, redis_queue_sleep_interval, 0),
    NTRef = erlang:send_after(Interval, self(), poll_redis_main_queue),
    State#state{shard = RedisShard, interval = Interval, timer = NTRef}.

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
handle_info(poll_redis_main_queue, #state{shard = RedisNode, interval = Interval} = State) ->
    MainQueue = "mainQueue::" ++ atom_to_list(node()),
    case
        vmq_redis:query(
            RedisNode,
            [
                ?FCALL,
                ?POLL_MAIN_QUEUE,
                1,
                MainQueue,
                20
            ],
            ?FCALL,
            ?POLL_MAIN_QUEUE
        )
    of
        {ok, undefined} ->
            erlang:send_after(Interval, self(), poll_redis_main_queue);
        {ok, Msgs} ->
            lists:foreach(
                fun([SubBin, MsgBin, TimeInQueue]) ->
                    vmq_metrics:pretimed_measurement(
                        {?MODULE, time_spent_in_main_queue},
                        binary_to_integer(TimeInQueue)
                    ),
                    case binary_to_term(SubBin) of
                        {_, _CId} = SId ->
                            {SubInfo, Msg} = binary_to_term(MsgBin),
                            vmq_reg:enqueue_msg({SId, SubInfo}, Msg);
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
            erlang:send_after(0, self(), poll_redis_main_queue);
        _ ->
            erlang:send_after(Interval, self(), poll_redis_main_queue)
    end,
    {noreply, State};
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
gen_redis_producer_client(T) ->
    NumRedisShards = application:get_env(vmq_server, num_redis_queue_shards, 1),
    Id = erlang:phash2(T, NumRedisShards),
    list_to_atom("redis_queue_" ++ ?PRODUCER ++ "_client_" ++ integer_to_list(Id)).
