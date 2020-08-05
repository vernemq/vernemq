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

-module(vmq_reg_mgr).

-include("vmq_server.hrl").

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
setup_queue(SubscriberId, Subs, Acc) ->
    case lists:keyfind(node(), 1, Subs) of
        {_Node, false, _Subs} ->
            vmq_queue_sup_sup:start_queue(SubscriberId, false);
        _ ->
            Acc
    end.

handle_event(Handler, Event) ->
    case Handler(Event) of
        {delete, _SubscriberId, _} ->
            %% TODO: we might consider a queue cleanup here.
            ignore;
        {update, _SubscriberId, [], []} ->
            %% noop
            ignore;
        {update, SubscriberId, _OldSubs, NewSubs} ->
            handle_new_sub_event(SubscriberId, NewSubs);
        ignore ->
            ignore
    end.

handle_new_sub_event(SubscriberId, Subs) ->
    Sessions = vmq_subscriber:get_sessions(Subs),
    case lists:keymember(node(), 1, Sessions) of
        true ->
            %% The local queue will have been started directly via
            %% `vmq_reg:register_subscriber` in the fsm.
            ignore;
        false ->
            %% we may migrate an existing queue to a remote queue
            %% Do we have a local queue to migrate?
            case vmq_queue_sup_sup:get_queue_pid(SubscriberId) of
                not_found ->
                    ignore;
                QPid ->
                    case is_allow_multi(QPid) of
                        true ->
                            %% no need to migrate if we allow multiple sessions
                            ignore;
                        false ->
                            handle_new_remote_subscriber(SubscriberId, QPid, Sessions);
                        error ->
                            ignore
                    end
            end
    end.

%% Local queue found, only migrate this queue if
%% 1. NewQueue.clean_session = false AND
%% 2. OldQueue.allow_multiple_sessions = false AND
%% 3. Only one concurrent Session exist
%% if OldQueue.clean_session = true, queue migration is
%% aborted by the OldQueue process.
handle_new_remote_subscriber(SubscriberId, QPid, [{NewNode, false}]) ->
    % Case 1. AND 3.
    migrate_queue(SubscriberId, QPid, NewNode);
handle_new_remote_subscriber(SubscriberId, QPid, [{_NewNode, true}]) ->
    %% New remote queue uses clean_session=true, we have to wipe this local session
    cleanup_queue(SubscriberId, ?SESSION_TAKEN_OVER, QPid);
handle_new_remote_subscriber(SubscriberId, QPid, Sessions) ->
    % Case Not 3.
    %% Do we have available remote sessions
    case [N || {N, false} <- Sessions] of
        [NewNode|_] ->
            %% Best bet to use this session
            lager:warning("more than one remote nodes found for migrating queue[~p] for subscriber ~p, use ~p", [QPid, SubscriberId, NewNode]),
            migrate_queue(SubscriberId, QPid, NewNode);
        [] ->
            lager:warning("can't migrate the queue[~p] for subscriber ~p due to no responsible remote node found", [QPid, SubscriberId])
    end.

migrate_queue(SubscriberId, QPid, Node) ->
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

cleanup_queue(SubscriberId, Reason, QPid) ->
    vmq_reg_sync:async({cleanup, SubscriberId},
                       fun() ->
                               vmq_queue:cleanup(QPid, Reason)
                       end, node(), 60000).

is_allow_multi(QPid) ->
    case vmq_queue:get_opts(QPid) of
        #{allow_multiple_sessions := AllowMulti} -> AllowMulti;
        {error, _} -> error
    end.
