%% Copyright 2014 Erlio GmbH Basel Switzerland (http://erl.io)
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

-module(vmq_exo).
-behaviour(gen_server).
-export([incr_bytes_received/1,
         incr_bytes_sent/1,
         incr_expired_clients/0,
         incr_messages_received/1,
         incr_messages_sent/1,
         incr_publishes_dropped/1,
         incr_publishes_received/1,
         incr_publishes_sent/1,
         incr_subscription_count/0,
         decr_subscription_count/0,
         incr_socket_count/0,
         incr_connect_received/0,
         entries/0,
         entries/1]).

%% API functions
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {history=[]}).

%%%===================================================================
%%% API functions
%%%===================================================================

incr_bytes_received(V) ->
    incr_item([bytes, received], V).

incr_bytes_sent(V) ->
    incr_item([bytes, sent], V).

incr_expired_clients() ->
    incr_item([expired_clients], 1).

incr_messages_received(V) ->
    incr_item([messages, received], V).

incr_messages_sent(V) ->
    incr_item([messages, sent], V).

incr_publishes_dropped(V) ->
    incr_item([publishes, dropped], V).

incr_publishes_received(V) ->
    incr_item([publishes, received], V).

incr_publishes_sent(V) ->
    incr_item([publishes, sent], V).

incr_subscription_count() ->
    incr_item([subscriptions], 1).

decr_subscription_count() ->
    incr_item([subscriptions], -1).

incr_socket_count() ->
    incr_item([sockets], 1).

incr_connect_received() ->
    incr_item([connects, received], 1).

incr_item(Entry, Val) ->
    exometer:update_or_create(Entry ++ ['last_sec'], Val),
    exometer:update_or_create(Entry ++ ['last_10sec'], Val),
    exometer:update_or_create(Entry ++ ['last_30sec'], Val),
    exometer:update_or_create(Entry ++ ['last_min'], Val),
    exometer:update_or_create(Entry ++ ['last_5min'], Val).

entries() ->
    {ok, entries(undefined)}.

entries(undefined) ->
    [
     {[memory], {function, erlang, memory, [], proplist,
                 [total, processes, system,
                  atom, binary, code, ets]}, []},
     {[subscriptions], {function, vmq_reg, total_subscriptions, [], proplist, [total]}, []},
     {[clients], {function, vmq_reg, client_stats, [], proplist,
                  [total, active, inactive]}, []}
     | counter_entries()];
entries({ReporterMod, Interval}) ->
    subscribe(ReporterMod, entries(undefined), Interval).

counter_entries() ->
    [
     {[bytes, received, last_sec], counter, [{snmp, []}]},
     {[bytes, received, last_10sec], counter, [{snmp, []}]},
     {[bytes, received, last_30sec], counter, [{snmp, []}]},
     {[bytes, received, last_min], counter, [{snmp, []}]},
     {[bytes, received, last_5min], counter, [{snmp, []}]},

     {[bytes, sent, last_sec], counter, [{snmp, []}]},
     {[bytes, sent, last_10sec], counter, [{snmp, []}]},
     {[bytes, sent, last_30sec], counter, [{snmp, []}]},
     {[bytes, sent, last_min], counter, [{snmp, []}]},
     {[bytes, sent, last_5min], counter, [{snmp, []}]},

     {[messages, received, last_sec], counter, [{snmp, []}]},
     {[messages, received, last_10sec], counter, [{snmp, []}]},
     {[messages, received, last_30sec], counter, [{snmp, []}]},
     {[messages, received, last_min], counter, [{snmp, []}]},
     {[messages, received, last_5min], counter, [{snmp, []}]},

     {[messages, sent, last_sec], counter, [{snmp, []}]},
     {[messages, sent, last_10sec], counter, [{snmp, []}]},
     {[messages, sent, last_30sec], counter, [{snmp, []}]},
     {[messages, sent, last_min], counter, [{snmp, []}]},
     {[messages, sent, last_5min], counter, [{snmp, []}]},

     {[publishes, dropped, last_sec], counter, [{snmp, []}]},
     {[publishes, dropped, last_10sec], counter, [{snmp, []}]},
     {[publishes, dropped, last_30sec], counter, [{snmp, []}]},
     {[publishes, dropped, last_min], counter, [{snmp, []}]},
     {[publishes, dropped, last_5min], counter, [{snmp, []}]},

     {[publishes, received, last_sec], counter, [{snmp, []}]},
     {[publishes, received, last_10sec], counter, [{snmp, []}]},
     {[publishes, received, last_30sec], counter, [{snmp, []}]},
     {[publishes, received, last_min], counter, [{snmp, []}]},
     {[publishes, received, last_5min], counter, [{snmp, []}]},

     {[publishes, sent, last_sec], counter, [{snmp, []}]},
     {[publishes, sent, last_10sec], counter, [{snmp, []}]},
     {[publishes, sent, last_30sec], counter, [{snmp, []}]},
     {[publishes, sent, last_min], counter, [{snmp, []}]},
     {[publishes, sent, last_5min], counter, [{snmp, []}]},

     {[connects, received, last_sec], counter, [{snmp, []}]},
     {[connects, received, last_10sec], counter, [{snmp, []}]},
     {[connects, received, last_30sec], counter, [{snmp, []}]},
     {[connects, received, last_min], counter, [{snmp, []}]},
     {[connects, received, last_5min], counter, [{snmp, []}]},

     {[sockets, last_sec], counter, [{snmp, []}]},
     {[sockets, last_10sec], counter, [{snmp, []}]},
     {[sockets, last_30sec], counter, [{snmp, []}]},
     {[sockets, last_min], counter, [{snmp, []}]},
     {[sockets, last_5min], counter, [{snmp, []}]}
    ].

subscribe(ReporterMod, [{Metric, histogram, _}|Rest], Interval) ->
    Datapoints = [max, min, mean, median],
    subscribe(ReporterMod, Metric, Datapoints, Interval),
    subscribe(ReporterMod, Rest, Interval);
subscribe(ReporterMod, [{Metric, {function, _, _, _, proplist, Items}, _}|Rest],
          Interval) ->
    subscribe(ReporterMod, Metric, Items, Interval),
    subscribe(ReporterMod, Rest, Interval);
subscribe(ReporterMod, [{Metric, {function, _M, _F, _A, value, _}, _}|Rest], Interval) ->
    subscribe(ReporterMod, Metric, default, Interval),
    subscribe(ReporterMod, Rest, Interval);
subscribe(ReporterMod, [{Metric, _, _}|Rest], Interval) ->
    subscribe(ReporterMod, Metric, value, Interval),
    subscribe(ReporterMod, Rest, Interval);
subscribe(_, [], _) -> ok.

subscribe(ReporterMod, Metric, [Datapoint|Rest], Interval) when is_atom(Datapoint) ->
    subscribe(ReporterMod, Metric, Datapoint, Interval),
    subscribe(ReporterMod, Metric, Rest, Interval);
subscribe(ReporterMod, Metric, Datapoint, Interval) when is_atom(Datapoint) ->
    case exometer_report:subscribe(ReporterMod, Metric, Datapoint, Interval) of
        ok ->
            ok;
        E ->
            exit({exometer_report_subscribe, E, ReporterMod, Metric, Datapoint})
    end;
subscribe(_, _, [], _) -> ok.

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

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
    timer:send_interval(1000, calc_stats),
    {ok, #state{}}.

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
handle_info(calc_stats, #state{history=History} = State) ->
    %% this is (MUST be) called every second!
    NewHistory =
    lists:foldl(
      fun({Entry,_,_}, AccHistory) ->
              case lists:reverse(Entry) of
                  [last_sec|Tail] ->
                     %% only call this if we hit the 'last_sec' counter
                      case exometer:get_value(Entry) of
                          {error, not_found} ->
                              ignore;
                          {ok, [{value, Val}|_]} ->
                              exometer:update(Entry, -Val),
                              H1 = update_sliding_windows(last_10sec, Tail, Val, AccHistory),
                              H2 = update_sliding_windows(last_30sec, Tail, Val, H1),
                              H3 = update_sliding_windows(last_min, Tail, Val, H2),
                              update_sliding_windows(last_5min, Tail, Val, H3)
                      end;
                  _ ->
                      AccHistory
              end
      end, History, counter_entries()),
    {noreply, State#state{history=NewHistory}}.

update_sliding_windows(last_10sec, Base, Val, History) ->
    update_sliding_windows(10, lists:reverse([last_10sec | Base]), Val, History);
update_sliding_windows(last_30sec, Base, Val, History) ->
    update_sliding_windows(30, lists:reverse([last_30sec | Base]), Val, History);
update_sliding_windows(last_min, Base, Val, History) ->
    update_sliding_windows(60, lists:reverse([last_min | Base]), Val, History);
update_sliding_windows(last_5min, Base, Val, History) ->
    update_sliding_windows(300, lists:reverse([last_5min | Base]), Val, History);

update_sliding_windows(Size, MetricName, SecVal, History) ->
    case lists:keyfind(MetricName, 1, History) of
        false ->
            InitSlidingWindow = [0 || _ <- lists:seq(1, Size - 1)],
            [{MetricName, [SecVal|InitSlidingWindow]}|History];
        {_, SlidingWindow} ->
            case lists:last(SlidingWindow) of
                0 ->
                    lists:keyreplace(MetricName, 1, History ,
                                     {MetricName, [SecVal|lists:droplast(SlidingWindow)]});
                OldestVal ->
                    exometer:update(MetricName, -OldestVal),
                    lists:keyreplace(MetricName, 1, History ,
                                     {MetricName, [SecVal|lists:droplast(SlidingWindow)]})
            end
    end.




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
