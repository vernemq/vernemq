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

-module(vmq_config).

-behaviour(gen_server).

%% API
-export([start_link/0,
         change_config_now/3,
         reset/0,
         get_env/1,
         get_env/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).
-define(TABLE, ?MODULE).

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

change_config_now(_, _, _) ->
    %% upon a config change we flush the table
    reset().

reset() ->
    ets:delete_all_objects(?TABLE).

get_env(Key) ->
    get_env(Key, undefined).

get_env(Key, Default) ->
    try ets:lookup(?TABLE, Key) of
        [{_, undefined}] -> Default;
        [{_, Val}] -> Val;
        [] ->
            AppEnvVal = application:get_env(vmq_server, Key, undefined),
            ets:insert(?TABLE, {Key, AppEnvVal}),
            case AppEnvVal of
                undefined ->
                    Default;
                _ ->
                    AppEnvVal
            end
    catch
        _:_ ->
            %% in between gen_server restart
            application:get_env(vmq_server, Key, Default)
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([]) ->
    ets:new(?TABLE, [public, named_table, {read_concurrency, true}]),
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

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
