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

-module(vmq_sysmon).

-behaviour(gen_server).

%% API
-export([start_link/0,
         cpu_load_level/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {samples=queue:new()}).

-define(SAMPLE_INTERVAL, 2000).
-define(NR_OF_SAMPLES, 10).

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
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

cpu_load_level() ->
    [{_, Level}] = ets:lookup(?MODULE, cpu_load_level),
    Level.


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
    _ = cpu_sup:util([per_cpu]), % first return value is rubbish, per the docs
    _ = ets:new(?MODULE, [public, named_table, {read_concurrency, true}]),

    %% Add our system_monitor event handler.  We do that here because
    %% we have a process at our disposal (i.e. ourself) to receive the
    %% notification in the very unlikely event that the
    %% riak_core_sysmon_handler has crashed and been removed from the
    %% riak_sysmon_handler gen_event server.  (If we had a supervisor
    %% or app-starting process add the handler, then if the handler
    %% crashes, nobody will act on the crash notification.)
    vmq_sysmon_handler:add_handler(),
    {ok, #state{}, 0}.

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
handle_info(timeout, #state{samples=Samples} = State) ->
    CPUAvg = cpu_sample(),
    NewSamples =
    case queue:len(Samples) of
        L when L >= ?NR_OF_SAMPLES ->
            queue:in(CPUAvg, queue:drop(Samples));
        _ ->
            queue:in(CPUAvg, Samples)
    end,
    Level = calc(NewSamples),
    ets:insert(?MODULE, {cpu_load_level, Level}),
    {noreply, State#state{samples=NewSamples}, ?SAMPLE_INTERVAL};
handle_info({gen_event_EXIT, riak_core_sysmon_handler, _}, State) ->
    %% SASL will create an error message, no need for us to duplicate it.
    %%
    %% Our handler should never crash, but it did indeed crash.  If
    %% there's a pathological condition somewhere that's generating
    %% lots of unforseen things that crash core's custom handler, we
    %% could make things worse by jumping back into the exploding
    %% volcano.  Wait a little bit before jumping back.  Besides, the
    %% system_monitor data is nice but is not critical: there is no
    %% need to make things worse if things are indeed bad, and if we
    %% miss a few seconds of system_monitor events, the world will not
    %% end.
    timer:sleep(2000),
    vmq_sysmon_handler:add_handler(),
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
calc(Samples) ->
    case queue:peek_r(Samples) of
        {value, Sample} when Sample >= 100 -> 3;
        {value, Sample} when Sample >= 90 -> 2;
        {value, Sample} when Sample >= 80 -> 1;
        _ -> 0 % includes queue is empty
    end.

cpu_sample() ->
    case cpu_sup:util([per_cpu]) of
        Info when is_list(Info) ->
            Utils = [U || {_, U, _, _} <- Info],
            case Utils of
                [U] ->
                    %% only one cpu
                    U;
                [_, _|_] ->
                    %% This is a form of ad-hoc averaging, which tries to
                    %% account for the possibility that the application
                    %% loads the cores unevenly.
                    calc_avg_util(Utils)
            end;
        _ ->
            undefined
    end.

calc_avg_util(Utils) ->
    case minmax(Utils) of
        {A, B} when B-A > 50 ->
            %% very uneven load
            High = [U || U <- Utils,
                         B - U > 20],
            lists:sum(High) / length(High);
        {Low, High} ->
            (High + Low) / 2
    end.

minmax([H | T]) ->
    lists:foldl(
      fun(X, {Min, Max}) ->
              {erlang:min(X, Min), erlang:max(X, Max)}
      end, {H, H}, T).
