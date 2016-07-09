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

-module(vmq_retain_srv).

-behaviour(gen_server).

%% API functions
-export([start_link/0,
         insert/3,
         delete/2,
         match_fold/4,
         stats/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).

-define(RETAIN_DB, {vmq, retain}).
-define(RETAIN_CACHE, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    case lists:member(?RETAIN_CACHE, ets:all()) of
        true ->
            ignore;
        false ->
            ets:new(?RETAIN_CACHE, [public, named_table,
                                    {read_concurrency, true},
                                    {write_concurrency, true}])
    end,
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

delete(MP, RoutingKey) ->
    gen_server:call(?MODULE, {delete, {MP, RoutingKey}}, infinity).

insert(MP, RoutingKey, Message) ->
    ets:insert(?RETAIN_CACHE, {{MP, RoutingKey}, Message, true}).

match_fold(FoldFun, Acc, MP, Topic) ->
    ets:foldl(
      fun({{M, T}, Payload, _}, AccAcc) when M == MP ->
              case vmq_topic:match(T, Topic) of
                  true ->
                      FoldFun({T, Payload}, AccAcc);
                  false ->
                      AccAcc
              end;
         (_, AccAcc) ->
              AccAcc
      end, Acc, ?RETAIN_CACHE).

stats() ->
    case ets:info(?RETAIN_CACHE, size) of
        undefined -> {0, 0};
        V ->
            M = ets:info(?RETAIN_CACHE, memory),
            {V, M}
    end.


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
    plumtree_metadata_manager:subscribe(?RETAIN_DB),
    plumtree_metadata:fold(
      fun({MPTopic, '$deleted'}, _) ->
              ets:delete(?RETAIN_CACHE, MPTopic);
         ({MPTopic, Msg}, _) ->
              ets:insert(?RETAIN_CACHE, [{MPTopic, Msg, false}])
      end, ok, ?RETAIN_DB, [{resolver, lww}]),
    erlang:send_after(vmq_config:get_env(retain_persist_interval, 1000),
                      self(), persist),
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
handle_call({delete, MPTopic}, _From, State) ->
    plumtree_metadata:delete(?RETAIN_DB, MPTopic),
    {reply, ok, State}.

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
handle_info({deleted, ?RETAIN_DB, Key, _Val}, State) ->
    ets:delete(?RETAIN_CACHE, Key),
    {noreply, State};
handle_info({updated, ?RETAIN_DB, Key, _OldVal, NewVal}, State) ->
    ets:insert(?RETAIN_CACHE, {Key, NewVal, false}),
    {noreply, State};
handle_info(persist, State) ->
    ets:foldl(fun persist/2, undefined, ?RETAIN_CACHE),
    erlang:send_after(vmq_config:get_env(retain_persist_interval, 1000),
                      self(), persist),
    {noreply, State};
handle_info(_, State) ->
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
persist({MPTopic, Message, true}, Acc) ->
    plumtree_metadata:put(?RETAIN_DB, MPTopic, Message),
    Acc;
persist({_, _, false}, Acc) -> Acc.
