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
-module(vmq_coalesce).

-behaviour(gen_server).

%% API
-export([start_link/0,
         start_link/1]).

-export([put/3,
         get/2,
         delete/2,
         take/1,
         info/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, format_status/2]).

-define(SERVER, ?MODULE).

-record(state, {
                %% make the write fun overridable for testing.
                write_fun :: function()
               }).
-define(TOMBSTONE, '$deleted').

%%%===================================================================
%%% API
%%%===================================================================

-type sid() :: any().
-type md() :: any().

-define(DATA_TBL, vmq_coalesce_data_table).
-define(IDX_TBL, vmq_coalesce_timeindex_table).
-define(STATS_TBL, vmq_coalesce_stats).

-spec put(any(), sid(), md()) -> true.
put(FullPrefix, SId, NewMD) ->
    Key = {FullPrefix, SId},
    ets:insert(?DATA_TBL, {Key, NewMD}),
    case ets:last(?IDX_TBL) of
        '$end_of_table' ->
            ets:insert(?IDX_TBL, {0, Key}),
            %% wake up writer, there's work to do
            ?SERVER ! wakeup;
        Last ->
            ets:insert(?IDX_TBL, {Last+1, Key})
    end,
    ets:update_counter(?STATS_TBL, writes_logical, 1, {writes_logical,0}),
    true.

get(FullPrefix, SId) ->
    case ets:lookup(?DATA_TBL, {FullPrefix, SId}) of
        [] -> undefined;
        [{_Key, Val}] -> Val
    end.

delete(FullPrefix, SId) ->
    %% TODO: this only works because the tombstone is the same at the
    %% metadata level...
    put(FullPrefix, SId, ?TOMBSTONE).

-spec take(integer()) -> [{integer(), {{any(), sid()}, md()}}].
take(N) ->
    {Count, MD} =
        try
            ets:foldl(
              fun({LTs, Key}, {Ctr, Acc}) when Ctr < N ->
                      ets:delete(?IDX_TBL, LTs),
                      case ets:take(?DATA_TBL, Key) of
                          [] ->
                              {Ctr, Acc};
                          [E] ->
                              {Ctr+1, [E|Acc]}
                      end;
                 (_, {_, _}=Res) ->
                      throw(Res)
              end, {0, []}, ?IDX_TBL)
        catch
            throw:{_,_}=Res ->
                Res
        end,
    {Count, lists:reverse(MD)}.

info() ->
    gen_server:call(?SERVER, info).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> {ok, Pid :: pid()} |
                      {error, Error :: {already_started, pid()}} |
                      {error, Error :: term()} |
                      ignore.
start_link() ->
    start_link([]).

start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Args, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%% @end
%%--------------------------------------------------------------------

-spec init(Args :: term()) -> {ok, State :: term()} |
                              {ok, State :: term(), Timeout :: timeout()} |
                              {ok, State :: term(), hibernate} |
                              {stop, Reason :: term()} |
                              ignore.
init(Args) ->
    %% entries are on the form {SId, Metadata}
    ets:new(?DATA_TBL, [named_table,  public]),
    ets:new(?IDX_TBL, [named_table, ordered_set, public]),
    ets:new(?STATS_TBL, [named_table, public]),
    DefaultWFun =
        fun(FullPrefix, SubscriberId, Value) ->
                vmq_plugin:only(metadata_put, [FullPrefix, SubscriberId, Value])
        end,
    {ok, #state{
            write_fun = proplists:get_value(write_fun, Args, DefaultWFun)
           }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), term()}, State :: term()) ->
                         {reply, Reply :: term(), NewState :: term()} |
                         {reply, Reply :: term(), NewState :: term(), Timeout :: timeout()} |
                         {reply, Reply :: term(), NewState :: term(), hibernate} |
                         {noreply, NewState :: term()} |
                         {noreply, NewState :: term(), Timeout :: timeout()} |
                         {noreply, NewState :: term(), hibernate} |
                         {stop, Reason :: term(), Reply :: term(), NewState :: term()} |
                         {stop, Reason :: term(), NewState :: term()}.
handle_call(info, _From, State) ->
    M1 = maps:from_list(ets:tab2list(?STATS_TBL)),
    Reply = maps:merge(M1, #{data_objects => proplists:get_value(size, ets:info(?DATA_TBL)),
                             time_indexes => proplists:get_value(size, ets:info(?IDX_TBL))}),
    {reply, Reply, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: term()) ->
                         {noreply, NewState :: term()} |
                         {noreply, NewState :: term(), Timeout :: timeout()} |
                         {noreply, NewState :: term(), hibernate} |
                         {stop, Reason :: term(), NewState :: term()}.
handle_cast(_Request, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: term()) ->
                         {noreply, NewState :: term()} |
                         {noreply, NewState :: term(), Timeout :: timeout()} |
                         {noreply, NewState :: term(), hibernate} |
                         {stop, Reason :: normal | term(), NewState :: term()}.
handle_info(wakeup, #state{write_fun=WFun}=State) ->
    case take(1000) of
        {0, []} ->
            ok;
        {Count, Values} ->
            lists:map(
              fun({{FullPrefix,SubscriberId}, Value}) ->
                      WFun(FullPrefix, SubscriberId, Value)

              end, Values),
            %% TODO: Optimize this to be exact and avoid
            %% sending more than the necessary amount of
            %% `wakeup` messages.
            ?SERVER ! wakeup,
            ets:update_counter(?STATS_TBL, writes_actual, Count, {writes_actual, 0})
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
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: normal | shutdown | {shutdown, term()} | term(),
                State :: term()) -> any().
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()},
                  State :: term(),
                  Extra :: term()) -> {ok, NewState :: term()} |
                                      {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called for changing the form and appearance
%% of gen_server status when it is returned from sys:get_status/1,2
%% or when it appears in termination error logs.
%% @end
%%--------------------------------------------------------------------
-spec format_status(Opt :: normal | terminate,
                    Status :: list()) -> Status :: term().
format_status(_Opt, Status) ->
    Status.

%%%===================================================================
%%% Internal functions
%%%===================================================================
