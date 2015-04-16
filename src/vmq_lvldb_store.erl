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

-module(vmq_lvldb_store).

-behaviour(gen_server).
-behaviour(msg_store_plugin).

%% API
-export([start_link/1,
         msg_store_write_sync/2,
         msg_store_write_async/2,
         msg_store_delete_sync/1,
         msg_store_delete_async/1,
         msg_store_read/1,
         msg_store_fold/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {bucket, waiting}).

%%%===================================================================
%%% API
%%%===================================================================
start_link(Id) ->
    gen_server:start_link(?MODULE, [Id], []).

msg_store_write_sync(Key, Val) ->
    call(Key, {write, Key, Val}).

msg_store_write_async(Key, Val) ->
    cast(Key, {write, Key, Val}).

msg_store_delete_sync(Key) ->
   call(Key, {delete, Key}).

msg_store_delete_async(Key) ->
    cast(Key, {delete, Key}).

msg_store_read(Key) ->
    call(Key, {read, Key}).

msg_store_fold(Fun, Acc) ->
    [Coordinator|_] = Pids = vmq_lvldb_store_sup:get_bucket_pids(),
    gen_server:call(Coordinator, {coordinate_fold, Fun, Acc, Pids}, infinity).

fold(BucketPid, Fun, Acc) ->
    gen_server:call(BucketPid, {fold, Fun, Acc}, infinity).


call(Key, Req) ->
    case vmq_lvldb_store_sup:get_bucket_pid(Key) of
        {ok, BucketPid} ->
            gen_server:call(BucketPid, Req, infinity);
        {error, Reason} ->
            {error, Reason}
    end.

cast(Key, Req) ->
    case vmq_lvldb_store_sup:get_bucket_pid(Key) of
        {ok, BucketPid} ->
            gen_server:cast(BucketPid, Req);
        {error, Reason} ->
            {error, Reason}
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
init([Id]) ->
    BucketDir =
    case application:get_env(vmq_server, lvldb_store_dir, []) of
        [] ->
            Dir = filename:join(["VERNEMQ."++atom_to_list(node()),
                                 "lvldb", integer_to_list(Id)]),
            ok = filelib:ensure_dir(Dir),
            Dir;
        Dir when is_list(Dir) ->
            case filelib:is_dir(Dir) of
                true ->
                    DDir = filename:join(Dir, integer_to_list(Id)),
                    ok = filelib:ensure_dir(DDir),
                    DDir;
                false ->
                    error_logger:error_msg(
                      "Directory ~p is not available!!! We stop here!!!",
                      [Dir]),
                    exit(msg_store_directory_not_available)
            end
    end,
    process_flag(trap_exit, true),
    {ok, TabRef} = eleveldb:open(BucketDir, [{create_if_missing, true}]),
    {ok, #state{bucket=TabRef}}.

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
handle_call({coordinate_fold, Fun, Acc, Pids}, From, State) ->
    CoordinatorPid = self(),
    [spawn(fun() ->
                   Res = fold(Pid, Fun, Acc),
                   CoordinatorPid ! {fold_res, Res}
           end) || Pid <- Pids],
    Res = handle_req({fold, Fun, Acc}, State#state.bucket),
    {noreply, State#state{waiting={From, length(Pids), [Res]}}};
handle_call(Request, _From, State) ->
    {reply, handle_req(Request, State#state.bucket), State}.

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
handle_cast(Request, State) ->
    handle_req(Request, State#state.bucket),
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
handle_info({fold_res, Res}, #state{waiting={From, Waiting, ResAcc}} = State) ->
    NewState =
    case Waiting of
        1 ->
            gen_server:reply(From, lists:flatten([Res|ResAcc])),
            State#state{waiting=undefined};
        _ ->
            State#state{waiting={From, Waiting - 1, [Res|ResAcc]}}
    end,
    {noreply, NewState}.

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
terminate(_Reason, State) ->
    eleveldb:close(State#state.bucket).

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
handle_req({write, Key, Val}, Bucket) ->
    eleveldb:put(Bucket, Key, Val, []);
handle_req({delete, Key}, Bucket) ->
    eleveldb:delete(Bucket, Key, []);
handle_req({read, Key}, Bucket) ->
    eleveldb:get(Bucket, Key, []);
handle_req({fold, Fun, Acc}, Bucket) ->
    eleveldb:fold(Bucket, fun({K,V}, AccAcc) -> Fun(K, V, AccAcc) end, Acc, []).
