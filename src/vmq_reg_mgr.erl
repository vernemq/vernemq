%% Copyright 2016 Erlio GmbH Basel Switzerland (http://erl.io)
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

-module(vmq_reg_mgr).

-behaviour(gen_server).

%% API functions
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {status=init,
                event_handler,
                event_queue=queue:new(),
                migrations=maps:new()}).

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
    Self = self(),
    spawn_link(
      fun() ->
              vmq_reg:fold_subscribers(fun setup_queue/3, ignore),
              Self ! all_queues_setup
      end),
    EventHandler = vmq_reg:subscribe_subscriber_changes(),
    {ok, #state{event_handler=EventHandler}}.

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
handle_info(all_queues_setup, #state{event_handler=Handler,
                                     event_queue=Q,
                                     migrations=Migrations} = State) ->
    lists:foreach(fun(Event) ->
                          handle_event(Handler, Event, Migrations)
                  end, queue:to_list(Q)),
    {noreply, State#state{status=ready, event_queue=undefined}};
handle_info({'DOWN', MRef, process, _Pid, Reason}, #state{migrations=Migrations} = State) ->
    {SubscriberId, Node} = maps:get(MRef, Migrations),
    erase(SubscriberId),
    case Reason of
        normal ->
            lager:debug("Queue for subscriber ~p successfully migrated to ~p",
                        [SubscriberId, Node]);
        Other ->
            lager:warning("Error during Queue migration for subscriber ~p to
node ~p, due to ~p", [SubscriberId, Node, Other])
    end,
    {noreply, State#state{migrations=maps:remove(MRef, Migrations)}};
handle_info(Event, #state{status=init, event_queue=Q} = State) ->
    {noreply, State#state{event_queue=queue:in(Event, Q)}};
handle_info(Event, #state{event_handler=Handler, migrations=Migrations} = State) ->
    NewMigrations = handle_event(Handler, Event, Migrations),
    {noreply, State#state{migrations=NewMigrations}}.

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
setup_queue(SubscriberId, Nodes, Acc) ->
    case lists:member(node(), Nodes) of
        true ->
            vmq_queue_sup:start_queue(SubscriberId);
        false ->
            Acc
    end.

handle_event(Handler, Event, Migrations) ->
    case Handler(Event) of
        {delete, _, _} ->
            %% TODO: we might consider a queue cleanup here.
            Migrations;
        {update, _SubscriberId, [], []} ->
            %% noop
            Migrations;
        {update, SubscriberId, _OldSubs, NewSubs} ->
            case ensure_queue_present(SubscriberId,
                                 NewSubs,
                                 local_subs(NewSubs)) of
                ignore ->
                    Migrations;
                {ok, AsyncMigrateRef, NewNode} ->
                    maps:put(AsyncMigrateRef, {SubscriberId, NewNode},
                             Migrations)
            end;
        ignore ->
            Migrations
    end.

ensure_queue_present(SubscriberId, _, true) ->
    ensure_local_queue_present(SubscriberId);
ensure_queue_present(SubscriberId, NewSubs, false) ->
    ensure_remote_queue_present(SubscriberId, NewSubs,
                               vmq_queue_sup:get_queue_pid(SubscriberId)).

ensure_local_queue_present(SubscriberId) ->
    %% queue migration is triggered by the remote nodes,
    %% as they'll end up calling ensure_remote_queue_present/3
    case vmq_queue_sup:get_queue_pid(SubscriberId) of
        not_found ->
            vmq_queue_sup:start_queue(SubscriberId);
        Pid when is_pid(Pid) ->
            ignore
    end.

ensure_remote_queue_present(_, _, not_found) ->
    ignore;
ensure_remote_queue_present(SubscriberId, NewSubs, QPid) ->
    %% no queue required on this node
    case is_allow_multi(QPid) of
        true ->
            %% no need to migrate if we allow multiple sessions
            ignore;
        false ->
            %% migrate this queue
            NewNodes = [Node || {_, _, Node} <- NewSubs],
            initiate_queue_migration(SubscriberId, QPid, NewNodes)
    end.

initiate_queue_migration(SubscriberId, QPid, [Node]) ->
    queue_migration_async(SubscriberId, QPid, Node);
initiate_queue_migration(SubscriberId, QPid, []) ->
    lager:warning("can't migrate the queue[~p] for subscriber ~p due to no responsible remote node found", [QPid, SubscriberId]),
    ignore;
initiate_queue_migration(SubscriberId, QPid, [Node|_]) ->
    lager:warning("more than one remote nodes found for migrating queue[~p] for subscriber ~p, use ~p", [QPid, SubscriberId, Node]),
    queue_migration_async(SubscriberId, QPid, Node).

queue_migration_async(SubscriberId, QPid, Node) ->
    case get(SubscriberId) of
        undefined ->
            {_, MRef} =
            spawn_monitor(
              fun() ->
                      case rpc:call(Node, vmq_queue_sup, start_queue,
                                    [SubscriberId]) of
                          {ok, _, RemoteQPid} ->
                              vmq_queue:migrate(QPid, RemoteQPid);
                          {badrpc, Reason} ->
                              exit({cant_start_queue, Node, SubscriberId, Reason})
                      end
              end),
            put(SubscriberId, QPid),
            {ok, MRef, Node};
        _ ->
            ignore
    end.

local_subs(Subs) ->
    length([Node || {_, _, Node} <- Subs, Node == node()]) > 0.

is_allow_multi(QPid) ->
    #{allow_multiple_sessions := AllowMulti} = vmq_queue:get_opts(QPid),
    AllowMulti.
