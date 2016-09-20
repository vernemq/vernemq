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
                event_queue=queue:new()}).

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
                                     event_queue=Q} = State) ->
    lists:foreach(fun(Event) ->
                          handle_event(Handler, Event)
                  end, queue:to_list(Q)),
    {noreply, State#state{status=ready, event_queue=undefined}};
handle_info(Event, #state{status=init, event_queue=Q} = State) ->
    {noreply, State#state{event_queue=queue:in(Event, Q)}};
handle_info(Event, #state{event_handler=Handler} = State) ->
    handle_event(Handler, Event),
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
setup_queue(SubscriberId, Nodes, Acc) ->
    case lists:member(node(), Nodes) of
        true ->
            vmq_queue_sup_sup:start_queue(SubscriberId);
        false ->
            Acc
    end.

handle_event(Handler, Event) ->
    case Handler(Event) of
        {delete, _, _} ->
            %% TODO: we might consider a queue cleanup here.
            ignore;
        {update, _SubscriberId, [], []} ->
            %% noop
            ignore;
        {update, SubscriberId, _, NewSubs} ->
            ensure_queue_present(SubscriberId, NewSubs, local_subs(NewSubs));
        ignore ->
            ignore
    end.

ensure_queue_present(SubscriberId, _, true) ->
    %% Local Subscriptions found, no need to kick of migration
    %% queue migration is triggered by the remote nodes,
    %% as they'll end up calling ensure_remote_queue_present/3
    case vmq_queue_sup_sup:get_queue_pid(SubscriberId) of
        not_found ->
            vmq_queue_sup_sup:start_queue(SubscriberId);
        Pid when is_pid(Pid) ->
            ignore
    end;
ensure_queue_present(SubscriberId, NewSubs, false) ->
    %% No local Subscriptions found,
    %% maybe kick off migration
    ensure_remote_queue_present(SubscriberId, NewSubs,
                               vmq_queue_sup_sup:get_queue_pid(SubscriberId)).

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
    lager:warning("can't migrate the queue[~p] for subscriber ~p due to no responsible remote node found", [QPid, SubscriberId]);
initiate_queue_migration(SubscriberId, QPid, [Node|_]) ->
    lager:warning("more than one remote nodes found for migrating queue[~p] for subscriber ~p, use ~p", [QPid, SubscriberId, Node]),
    queue_migration_async(SubscriberId, QPid, Node).

queue_migration_async(SubscriberId, QPid, Node) ->
    %% we use the Node of the 'new' Queue as SyncNode.
    vmq_reg_sync:async({migrate, SubscriberId},
                      fun() ->
                              case rpc:call(Node, vmq_queue_sup_sup, start_queue,
                                            [SubscriberId]) of
                                  {ok, _, RemoteQPid} ->
                                      vmq_queue:migrate(QPid, RemoteQPid);
                                  {E, Reason} when (E == error) or (E == badrpc) ->
                                      exit({cant_start_queue, Node, SubscriberId, Reason})
                              end
                      end, Node, 60000).

local_subs(Subs) ->
    length([Node || {_, _, Node} <- Subs, Node == node()]) > 0.

is_allow_multi(QPid) ->
    #{allow_multiple_sessions := AllowMulti} = vmq_queue:get_opts(QPid),
    AllowMulti.
