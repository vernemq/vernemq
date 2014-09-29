-module(emqttd_systree).

-behaviour(gen_server).

%% API
-export([start_link/1]).

-export([incr_bytes_received/1,
         incr_bytes_sent/1,
         incr_active_clients/0,
         decr_active_clients/0,
         incr_inactive_clients/0,
         decr_inactive_clients/0,
         incr_messages_received/0,
         incr_messages_sent/0,
         incr_publishes_dropped/0,
         incr_publishes_received/0,
         incr_publishes_sent/0,
         incr_inflight_count/0,
         decr_inflight_count/0,
         incr_retained_count/0,
         decr_retained_count/0,
         incr_subscription_count/0,
         decr_subscription_count/0,
         incr_socket_count/0,
         decr_socket_count/0,
         incr_connect_received/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {interval=60000}).
-define(TABLE, emqttd_systree).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Interval) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Interval], []).

incr_bytes_received(V) ->
    update_item(local_bytes_received, [{2,V},{3,V},{4,V},{5,V}]).

incr_bytes_sent(V) ->
    update_item(local_bytes_sent, [{2,V},{3,V},{4,V},{5,V}]).

incr_active_clients() ->
    update_item(local_active_clients, [{2,1},{3,1},{4,1},{5,1}]).

decr_active_clients() ->
    update_item(local_active_clients, {2,-1, 0, 0}).

incr_inactive_clients() ->
    update_item(local_inactive_clients, {2, 1}).

decr_inactive_clients() ->
    update_item(local_inactive_clients, {2, -1, 0, 0}).

incr_messages_received() ->
    update_item(local_messages_received, [{2,1},{3,1},{4,1},{5,1}]).

incr_messages_sent() ->
    update_item(local_messages_sent, [{2,1},{3,1},{4,1},{5,1}]).

incr_publishes_dropped() ->
    update_item(local_publishes_dropped, [{2,1},{3,1},{4,1},{5,1}]).

incr_publishes_received() ->
    update_item(local_publishes_received, [{2,1},{3,1},{4,1},{5,1}]).

incr_publishes_sent() ->
    update_item(local_publishes_sent, [{2,1},{3,1},{4,1},{5,1}]).

incr_inflight_count() ->
    update_item(local_inflight_count, {2, 1}).

decr_inflight_count() ->
    update_item(local_inflight_count, {2, -1, 0, 0}).

incr_retained_count() ->
    update_item(local_retained_count, {2, 1}).

decr_retained_count() ->
    update_item(local_retained_count, {2, -1, 0, 0}).

incr_subscription_count() ->
    update_item(local_subscription_count, {2, 1}).

decr_subscription_count() ->
    update_item(local_subscription_count, {2, -1, 0, 0}).

incr_socket_count() ->
    update_item(local_socket_count, {2, 1}).

decr_socket_count() ->
    update_item(local_socket_count, {2, -1, 0, 0}).

incr_connect_received() ->
    update_item(local_connect_received, [{2,1},{3,1},{4,1},{5,1}]).

items() ->
    [{local_bytes_received,0,0,0,0},
     {local_bytes_sent,0,0,0,0},
     {local_active_clients,0},
     {local_inactive_clients,0},
     {local_messages_received, 0,0,0,0},
     {local_messages_sent, 0,0,0,0},
     {local_publishes_dropped, 0,0,0,0},
     {local_publishes_received, 0,0,0,0},
     {local_publishes_sent, 0,0,0,0},
     {local_inflight_count, 0},
     {local_retained_count, 0},
     {local_subscription_count, 0},
     {local_socket_count, 0},
     {local_connect_received, 0,0,0,0}].

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init([integer()]) -> {'ok',#state{}}.
init([Interval]) ->
    ets:new(?TABLE, [public, named_table, {write_concurrency, true}]),
    true = ets:insert(?TABLE, items()),
    {ok, #state{interval=Interval}}.

-spec handle_call(_, _, #state{}) -> {reply, ok, #state{}}.
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

-spec handle_cast(_, #state{}) -> {noreply, #state{}}.
handle_cast(_Msg, State) ->
    {noreply, State}.

-spec handle_info(_,_) -> {'noreply',_}.
handle_info(_Info, State) ->
    {noreply, State}.

-spec terminate(_,#state{}) -> 'ok'.
terminate(_Reason, _State) ->
    ok.

-spec code_change(_,_,_) -> {'ok',_}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
update_item(Key, UpdateOp) ->
    try
        ets:update_counter(?TABLE, Key, UpdateOp)
    catch error:badarg -> error
    end.
