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

-module(vmq_retain_srv).

-dialyzer(no_undefined_callbacks).
-behaviour(gen_server2).

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
-define(RETAIN_UPDATE, vmq_retain_srv_updates).

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
            ets:new(?RETAIN_CACHE, [public, ordered_set, named_table,
                                    {read_concurrency, true},
                                    {write_concurrency, true}]),
            ets:new(?RETAIN_UPDATE, [public, named_table,
                                     {write_concurrency, true}])
    end,
    gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).

delete(MP, RoutingKey) ->
    Key = {MP, RoutingKey},
    ets:delete(?RETAIN_CACHE, Key),
    ets:update_counter(?RETAIN_UPDATE, Key, 1, {Key, 0}).

insert(MP, RoutingKey, Message) ->
    Key = {MP, RoutingKey},
    ets:insert(?RETAIN_CACHE, {Key, Message}),
    ets:update_counter(?RETAIN_UPDATE, Key, 1, {Key, 0}).

match_fold(FoldFun, Acc, MP, Topic) ->
    case has_wildcard(Topic) of
        true ->
            %% the performance is worse the earlier a wildcard occurs
            %% in the subscription topic. Worst case is a # pr + as
            %% the first topic element which will cause the entire
            %% table to be scanned.
            MatchTopic = topic2ms(Topic),
            MS = [{{{MP, MatchTopic}, '_'}, [], ['$_']}],
            lists:foldl(
              fun({{_M,T}, Payload}, AccAcc) ->
                      case vmq_topic:match(T, Topic) of
                          true ->
                              FoldFun({T, Payload}, AccAcc);
                          false ->
                              AccAcc
                      end;
                 (_, AccAcc) -> AccAcc
              end, Acc, ets:select(?RETAIN_CACHE, MS));
        false ->
            case ets:lookup(?RETAIN_CACHE, {MP, Topic}) of
                 [] -> Acc;
                 [{_, Payload}] ->
                    FoldFun({Topic, Payload}, Acc)
            end
    end.

-dialyzer({no_improper_lists, topic2ms/1}).
topic2ms([]) -> [];
topic2ms([<<"#">>]) ->
    '_';
topic2ms([E, <<"#">>]) ->
    [e2ms(E) | '_'];
topic2ms([E|Rest]) ->
    [e2ms(E)|topic2ms(Rest)].

e2ms(<<"+">>) -> '_';
e2ms(E) -> E.

stats() ->
    case ets:info(?RETAIN_CACHE, size) of
        undefined -> {0, 0};
        V ->
            MC = ets:info(?RETAIN_CACHE, memory),
            case ets:info(?RETAIN_UPDATE, memory) of
                undefined ->
                    {V, MC*erlang:system_info(wordsize)};
                MU ->
                    {V, (MC+MU)*erlang:system_info(wordsize)}
            end
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
    vmq_metadata:subscribe(?RETAIN_DB),
    vmq_metadata:fold(?RETAIN_DB,
      fun({MPTopic, '$deleted'}, _) ->
              ets:delete(?RETAIN_CACHE, MPTopic);
         ({MPTopic, Msg}, _) ->
              ets:insert(?RETAIN_CACHE, [{MPTopic, Msg}])
      end, ok),
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
handle_call(_Req, _From, State) ->
    {reply, {error, not_implemented}, State}.

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
    ets:insert(?RETAIN_CACHE, {Key, NewVal}),
    {noreply, State};
handle_info(persist, State) ->
    ets:foldl(fun persist/2, ignore, ?RETAIN_UPDATE),
    ets:match_delete(?RETAIN_UPDATE, {'_', 0}),
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
persist({Key, Counter}, _) ->
    case ets:lookup(?RETAIN_CACHE, Key) of
        [] ->
            %% cache line was deleted
            vmq_metadata:delete(?RETAIN_DB, Key);
        [{_, Message}] ->
            vmq_metadata:put(?RETAIN_DB, Key, Message)
    end,
    %% If a concurrent insert happened during the fold then the
    %% current counter value will be bigger than Counter, So
    %% decrementing by Counter means the resulting value will be
    %% greater than zero and the key/value will be persisted in the
    %% next persistence loop. In other words it is recorded that we
    %% have persisted Counter updates, but not more than that.
    ets:update_counter(?RETAIN_UPDATE, Key, -Counter).

has_wildcard([<<"+">>|_]) -> true;
has_wildcard([<<"#">>]) -> true;
has_wildcard([_|Rest]) -> has_wildcard(Rest);
has_wildcard([]) -> false.
