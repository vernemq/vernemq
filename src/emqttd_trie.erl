%% @type dictionary() = tuple().
%%   Same as from {@link dict:new(). dict:new()}; used in this module as State.
%%
%% @type info() = term(). Opaque state of the gen_leader behaviour.
%% @type state() = dictionary().
%%    Internal server state; In the general case, it can be any term.
%% @type broadcast() = term().
%%    Whatever the leader decides to broadcast to the candidates.
%% @type reason()  = term(). Error information.
%% @type commonReply() = {ok, state()} |
%%                       {ok, broadcast(), state()} |
%%                       {stop, reason(), state()}.
%%   Common set of valid replies from most callback functions.
%%
-module(emqttd_trie).
-include("emqtt_internal.hrl").

-behaviour(locks_leader).

-export([start_link/2,
         subscribe/3,
         unsubscribe/2,
         add_node/1,
         del_node/1,
         which_nodes/0,
         get_subscriptions/0,
         publish/3,
         route/3,
         register_client/2,
         disconnect_client/1,
         cleanup_client/1,
         match/1]).

-export([init/1,
	 elected/3,
	 surrendered/3,
	 handle_DOWN/3,
	 handle_leader_call/4,
	 handle_leader_cast/3,
	 from_leader/3,
	 handle_call/4,
	 handle_cast/3,
	 handle_info/3,
	 terminate/2,
	 code_change/4]).

-record(st, {am_leader = false, subs, nodes=[], ready=false}).
-record(client, {id, node, pid}).

start_link(Nodes, SubscriptionDir) ->
    locks_leader:start_link(?MODULE, ?MODULE, [Nodes, SubscriptionDir], []).

subscribe(ClientId, Topic, QoS) ->
    case locks_leader:leader_call(?MODULE, {subscribe, node(), ClientId, Topic, QoS}) of
        ok ->
            emqttd_msg_store:deliver_retained(self(), Topic, QoS);
        {error, Reason} ->
            {error, Reason}
    end.

unsubscribe(ClientId, Topic) ->
    locks_leader:leader_call(?MODULE, {unsubscribe, ClientId, Topic}).

get_subscriptions() ->
    locks_leader:call(?MODULE, get_subscriptions).

add_node(Node) when is_atom(Node) ->
    locks_leader:leader_call(?MODULE, {add_node, Node}).

del_node(Node) when is_atom(Node) ->
    locks_leader:leader_call(?MODULE, {del_node, Node}).

which_nodes() ->
    gen_server:call(?MODULE, which_nodes).



register_client(ClientId, CleanSession) ->
    locks_leader:leader_call(?MODULE, {register_client, node(), CleanSession, ClientId, self()}).

%% delete retained message
publish(RoutingKey, <<>>, true) ->
    locks_leader:leader_call(?MODULE, {reset_retain_msg, RoutingKey});

%publish to cluster node.
publish(RoutingKey, Payload, IsRetain) when is_list(RoutingKey) and is_binary(Payload) ->
    case IsRetain of
        true ->
            case locks_leader:leader_call(?MODULE, {retain_msg, RoutingKey, Payload}) of
                ok ->
                    publish_(RoutingKey, Payload);
                {error, Reason} ->
                    {error, Reason}
            end;
        false ->
            case ets:lookup(emqttd_status, ready) of
                [{ready, true}] ->
                    publish_(RoutingKey, Payload);
                _ ->
                    {error, maybe_net_split}
            end
    end.

publish_(RoutingKey, Payload) ->
    lists:foreach(
      fun(#topic{name=Name, node=Node}) ->
              case Node == node() of
                  true ->
                      route(Name, RoutingKey, Payload);
                  false ->
                      rpc:call(Node, ?MODULE, route, [Name, RoutingKey, Payload])
              end
      end, match(RoutingKey)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% RPC Callbacks / Maintenance
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
cleanup_client(ClientId) ->
    locks_leader:leader_call(?MODULE, {cleanup_client, ClientId}).

disconnect_client(ClientPid) when is_pid(ClientPid) ->
    emqttd_handler_fsm:disconnect(ClientPid);

disconnect_client(ClientId) ->
    case get_client_pid(ClientId) of
        {ok, ClientPid} -> disconnect_client(ClientPid);
        E -> E
    end.
%route locally, should only be called by publish
route(Topic, RoutingKey, Payload) ->
    UnroutableClients =
    lists:foldl(fun(#subscriber{qos=Qos, client=ClientId}, Acc) ->
                        case get_client_pid(ClientId) of
                            {ok, ClientPid} ->
                                emqttd_handler_fsm:deliver(ClientPid, RoutingKey, Payload, Qos, false),
                                Acc;
                            {error, not_found} when Qos > 0 ->
                                [{ClientId, Qos}|Acc];
                            {error, not_found} ->
                                Acc
                        end
                end, [], ets:lookup(emqttd_subscriber, Topic)),
    io:format("unroutable client ~p~n", [UnroutableClients]),
    emqttd_msg_store:persist_for_later(UnroutableClients, RoutingKey, Payload).

match(Topic) when is_list(Topic) ->
    TrieNodes = trie_match(emqtt_topic:words(Topic)),
    Names = [Name || #trie_node{topic=Name} <- TrieNodes, Name=/= undefined],
    lists:flatten([ets:lookup(emqttd_trie_topic, Name) || Name <- Names]).

%% @spec init(Arg::term()) -> {ok, State}
%%
%%   State = state()
%%
%% @doc Equivalent to the init/1 function in a gen_server.
%%
init([Nodes, SubscriptionDir]) ->
    io:fwrite("init([])~n"),
    ets:new(emqttd_client, [named_table, public, {read_concurrency, true}, {keypos, 2}]),
    ets:new(emqttd_trie, [named_table, public, {read_concurrency, true}, {keypos, 2}]),
    ets:new(emqttd_trie_node, [named_table, public, {read_concurrency, true}, {keypos, 2}]),
    ets:new(emqttd_trie_topic, [named_table, public, bag, {read_concurrency, true}, {keypos, 2}]),
    ets:new(emqttd_subscriber, [{read_concurrency, true}, public, bag, named_table, {keypos, 2}]),
    ets:new(emqttd_status, [{read_concurrency, true}, public, named_table]),

    Subscriptions = bitcask:open(SubscriptionDir, [read_write]),
    bitcask:fold(Subscriptions,
      fun(Key, Val, _) ->
              {ClientId, Topic} = binary_to_term(Key),
              {_TS, Node, QoS} = binary_to_term(Val),
              add_topic_(Node, ClientId, binary_to_list(Topic), QoS)
      end, []),
    NewNodes = [{N, node_alive(N)} || N <- Nodes],
    erlang:send_after(5000, self(), check_connectivity),
    {ok, #st{subs=Subscriptions, nodes=NewNodes, ready=ready(NewNodes)}}.

%% @spec elected(State::state(), I::info(), Cand::pid() | undefined) ->
%%   {ok, Broadcast, NState}
%% | {reply, Msg, NState}
%% | {ok, AmLeaderMsg, FromLeaderMsg, NState}
%% | {error, term()}
%%
%%     Broadcast = broadcast()
%%     NState    = state()
%%
%% @doc Called by the leader when it is elected leader, and each time a
%% candidate recognizes the leader.
%%
%% This function is only called in the leader instance, and `Broadcast'
%% will be sent to all candidates (when the leader is first elected),
%% or to the new candidate that has appeared.
%%
%% `Broadcast' might be the same as `NState', but doesn't have to be.
%% This is up to the application.
%%
%% If `Cand == undefined', it is possible to obtain a list of all new
%% candidates that we haven't synced with (in the normal case, this will be
%% all known candidates, but if our instance is re-elected after a netsplit,
%% the 'new' candidates will be the ones that haven't yet recognized us as
%% leaders). This gives us a chance to talk to them before crafting our
%% broadcast message.
%%
%% We can also choose a different message for the new candidates and for
%% the ones that already see us as master. This would be accomplished by
%% returning `{ok, AmLeaderMsg, FromLeaderMsg, NewState}', where
%% `AmLeaderMsg' is sent to the new candidates (and processed in
%% {@link surrendered/3}, and `FromLeaderMsg' is sent to the old
%% (and processed in {@link from_leader/3}).
%%
%% If `Cand == Pid', a new candidate has connected. If this affects our state
%% such that all candidates need to be informed, we can return `{ok, Msg, NSt}'.
%% If, on the other hand, we only need to get the one candidate up to speed,
%% we can return `{reply, Msg, NSt}', and only the candidate will get the
%% message. In either case, the candidate (`Cand') will receive the message
%% in {@link surrendered/3}. In the former case, the other candidates will
%% receive the message in {@link from_leader/3}.
%% @end
%%
elected(S, I, _) ->
    io:fwrite("elected leader, merging~n", []),
    {KeyCount, _} = bitcask:status(S#st.subs),
    Bloom =
    lists:foldl(
      fun(K, Bl) ->
              emqttd_bloom:add_element(K, Bl)
      end, emqttd_bloom:new(KeyCount + 100), bitcask:list_keys(S#st.subs)),
    {Good, _Bad} = locks_leader:ask_candidates({find_unknowns, Bloom}, I),
    %% add unknown topics
    lists:foreach(fun({Key, Value}) ->
                          bitcask:put(S#st.subs, Key, Value),
                          {ClientId, Topic} = binary_to_term(Key),
                          {_TS, Node, QoS} = binary_to_term(Value),
                          add_topic_(Node, ClientId, binary_to_list(Topic), QoS)
                  end, remove_old_duplicates(Good)),

    {New, KVPairs} =
    bitcask:fold(S#st.subs,
                 fun
                     (Key, Val, {New, Repl}) when length(Repl) == 10 ->
                         {[replicate([{Key, Val}|Repl], I, [])|New], []};
                     (Key, Val, {New, Repl}) ->
                         {New, [{Key, Val}|Repl]}
                 end, {[],[]}),
    NewItems = lists:flatten([replicate(KVPairs, I, [])|New]),

    lists:foreach(fun({Key, Value}) ->
                          bitcask:put(S#st.subs, Key, Value),
                          {ClientId, Topic} = binary_to_term(Key),
                          {_TS, Node, QoS} = binary_to_term(Value),
                          add_topic_(Node, ClientId, binary_to_list(Topic), QoS)
                  end, NewItems),

    case locks_leader:new_candidates(I) of
        [] ->
            NewNodes = [{N, node_alive(N)}|| {N,_} <- S#st.nodes],
            {ok, {sync, {node_sync, NewNodes}}, S#st{nodes=NewNodes, am_leader=true, ready=ready(NewNodes)}};
        Cands ->
            NodeNames = [node(C) || C <- Cands],
            Nodes1 =
            lists:map(fun({Node, Status}) ->
                              case lists:member(Node, NodeNames) of
                                  true ->
                                      {Node, true};
                                  false ->
                                      {Node, Status}
                              end
                      end, S#st.nodes),
            Nodes2 = merge_nodes(Nodes1, I),
            Nodes3 = [{N, node_alive(N)}||{N, _} <- Nodes2],
            io:fwrite("New candidates = ~p~n", [Cands]),
            {ok, {sync, {node_sync, Nodes3}}, S#st{am_leader = true, nodes=Nodes3, ready=ready(Nodes3)}}
    end.

replicate([], _I, New) -> New;
replicate(KVPairs, I, New) ->
    {Good, _Bad} = locks_leader:ask_candidates({replicate, KVPairs}, I),
    NewNew = remove_old_duplicates(Good),
    replicate(NewNew, I, [NewNew|New]).

remove_old_duplicates(ListOfKVPairs) ->
    remove_old_duplicates(
      lists:reverse(
        lists:flatten(
          lists:usort(
            lists:foldl(
              fun({_, KVPairs}, Acc) ->
                      [KVPairs|Acc]
              end, [], ListOfKVPairs)))), []).

remove_old_duplicates([{Key, _}|Rest], [{Key, _}|_] = Acc) ->
    remove_old_duplicates(Rest, Acc);
remove_old_duplicates([{Key, Val}|Rest], Acc) ->
    remove_old_duplicates(Rest, [{Key, Val}|Acc]);
remove_old_duplicates([], Acc) ->
    Acc.


%% @spec surrendered(State::state(), Synch::broadcast(), I::info()) ->
%%          {ok, NState}
%%
%%    NState = state()
%%
%% @doc Called by each candidate when it recognizes another instance as
%% leader.
%%
%% Strictly speaking, this function is called when the candidate
%% acknowledges a leader and receives a Synch message in return.
%% @end
surrendered(S, {sync, {node_sync, LeaderNodes}}, _I) ->
    io:fwrite("surrendered()~n"),
    {ok, S#st{am_leader = false, nodes=LeaderNodes, ready=ready(LeaderNodes)}}.

%% @spec handle_DOWN(Candidate::pid(), State::state(), I::info()) ->
%%    {ok, NState} | {ok, Broadcast, NState}
%%
%%   Broadcast = broadcast()
%%   NState    = state()
%%
%% @doc Called by the leader when it detects loss of a candidate.
%%
%% If the function returns a `Broadcast' object, this will be sent to all
%% candidates, and they will receive it in the function {@link from_leader/3}.
%% @end
handle_DOWN(Pid, S, _I) ->
    NodeName = node(Pid),
    NewNodes = lists:keyreplace(NodeName, 1, S#st.nodes, {NodeName, false}),
    io:fwrite("handle_DOWN(~p,Dict,E)~n", [NodeName]),
    {ok, NewNodes, S#st{nodes=NewNodes, ready=ready(NewNodes)}}.

%% @spec handle_leader_call(Msg::term(), From::callerRef(), State::state(),
%%                          I::info()) ->
%%    {reply, Reply, NState} |
%%    {reply, Reply, Broadcast, NState} |
%%    {noreply, state()} |
%%    {stop, Reason, Reply, NState} |
%%    commonReply()
%%
%%   Broadcast = broadcast()
%%   NState    = state()
%%
%% @doc Called by the leader in response to a
%% {@link locks_leader:leader_call/2. leader_call()}.
%%
%% If the return value includes a `Broadcast' object, it will be sent to all
%% candidates, and they will receive it in the function {@link from_leader/3}.
handle_leader_call({subscribe, Node, ClientId, Topic, QoS}, _From, State, _I) ->
    case State#st.ready of
        true ->
            add_topic(State#st.subs, Node, ClientId, Topic, QoS),
            {reply, ok, {sync, {subscribe, Node, ClientId, Topic, QoS}}, State};
        false ->
            %% trading availability for CP,
            {reply, {error, maybe_net_split}, State}
    end;

handle_leader_call({unsubscribe, ClientId, Topic}, _From, State, _I) ->
    case State#st.ready of
        true ->
            del_topic(State#st.subs, ClientId, Topic),
            {reply, ok, {sync, {unsubscribe, ClientId, Topic}}, State};
        false ->
            %% trading availability for CP,
            {reply, {error, maybe_net_split}, State}
    end;

handle_leader_call({add_node, Node}, _From, #st{nodes=Nodes} = State, _I) ->
    NewNodes =
    case lists:keyfind(Node, 1, Nodes) of
        {Node, _} ->
            lists:keyreplace(Node, 1, Nodes, {Node, node_alive(Node)});
        false ->
            [{Node, node_alive(Node)}|Nodes]
    end,
    {reply, ok, {sync, {node_sync, NewNodes}}, State#st{nodes=NewNodes, ready=ready(NewNodes)}};

handle_leader_call({del_node, Node}, _From, #st{nodes=Nodes} = State, _I) ->
    NewNodes = lists:keydelete(Node, 1, Nodes),
    {reply, ok, {sync, {node_sync, NewNodes}}, State#st{nodes=NewNodes}};

handle_leader_call({register_client, Node, CleanSession, ClientId, Pid}, _From, State, _I) ->
    case State#st.ready of
        true ->
            case Node == node() of
                true ->
                    disconnect_client(ClientId), %% disconnect in case we already have such a client id
                    ets:insert(emqttd_client, #client{id=ClientId, node=Node, pid=Pid}),
                    monitor(process, Pid);
                false ->
                    ok
            end,
            %% this will also cleanup the message store
            cleanup_client(State#st.subs, CleanSession, ClientId),
            case CleanSession of
                false ->
                    emqttd_msg_store:deliver_from_store(ClientId, Pid);
                true ->
                    ok
            end,
            {reply, ok, {sync, {register_client, Node, CleanSession, ClientId, Pid}}, State};
        false ->
            {reply, {error, maybe_net_split}, State}
    end;

handle_leader_call({cleanup, ClientId}, _From, State, _I) ->
    case State#st.ready of
        true ->
            {reply, cleanup_client(State#st.subs, true, ClientId), {sync, {cleanup, ClientId}}, State};
        false ->
            {reply, {error, maybe_net_split}, State}
    end;

handle_leader_call({retain_msg, RoutingKey, Payload}, _From, State, _I) ->
    case State#st.ready of
        true ->
            ok = emqttd_msg_store:persist_retain_msg(RoutingKey, Payload),
            {reply, ok, {sync, {retain_msg, RoutingKey, Payload}}, State};
        false ->
            {reply, {error, maybe_net_split}, State}
    end;

handle_leader_call({reset_retain_msg, RoutingKey}, _From, State, _I) ->
    case State#st.ready of
        true ->
            ok = emqttd_msg_store:reset_retained_msg(RoutingKey),
            {reply, ok, {sync, {reset_retain_msg, RoutingKey}}, State};
        false ->
            {reply, {error, maybe_net_split}, State}
    end.




%% @spec handle_leader_cast(Msg::term(), State::term(), I::info()) ->
%%   commonReply()
%%
%% @doc Called by the leader in response to a {@link locks_leader:leader_cast/2.
%% leader_cast()}.
%% @end
handle_leader_cast(_Msg, S, _I) ->
    io:fwrite("handle_leader_cast(~p, S, I)~n", [_Msg]),
    {ok, S}.

%% @spec from_leader(Msg::term(), State::state(), I::info()) ->
%%    {ok, NState}
%%
%%   NState = state()
%%
%% @doc Called by each candidate in response to a message from the leader.
%% @end
from_leader({sync, {subscribe, Node, ClientId, Topic, QoS}}, State, _I) ->
    add_topic(State#st.subs, Node, ClientId, Topic, QoS),
    {ok, State};
from_leader({sync, {unsubscribe, ClientId, Topic}}, State, _I) ->
    del_topic(State#st.subs, ClientId, Topic),
    {ok, State};

from_leader({sync, {node_sync, Nodes}}, #st{nodes=OldNodes} = S, _I) ->
    Member = lists:member(node(), Nodes),
    case lists:member(node(), OldNodes) of
        true when Member ->
            %% no action;
            ok;
        true ->
            %% we got removed
            io:format("we got removed~n");
        false when Member ->
            %% we got added
            io:format("we got added~n");
        false ->
            %% no action
            ok
    end,
    {ok, S#st{nodes=Nodes, ready=ready(Nodes)}};

from_leader({sync, {register_client, Node, CleanSession, ClientId, Pid}}, S, _I) ->
    case Node == node() of
        true ->
            disconnect_client(ClientId), %% disconnect in case we already have such a client id
            ets:insert(emqttd_client, #client{id=ClientId, node=Node, pid=Pid}),
            monitor(process, Pid);
        false ->
            ok
    end,
    %% this will also cleanup the message store
    cleanup_client(S#st.subs, CleanSession, ClientId),
    case CleanSession of
        false ->
            emqttd_msg_store:deliver_from_store(ClientId, Pid);
        true ->
            ok
    end,
    {ok, S};

from_leader({sync, {cleanup, ClientId}}, S, _I) ->
    cleanup_client(S#st.subs, true, ClientId),
    {ok, S};

from_leader({sync, {retain_msg, RoutingKey, Payload}}, S, _I) ->
    ok = emqttd_msg_store:persist_retain_msg(RoutingKey, Payload),
    {ok, S};

from_leader({sync, {reset_retain_msg, RoutingKey}}, S, _I) ->
    emqttd_msg_store:reset_retained_msg(RoutingKey),
    {ok, S}.

%% @spec handle_call(Request::term(), From::callerRef(), State::state(),
%%                   I::info()) ->
%%    {reply, Reply, NState}
%%  | {noreply, NState}
%%  | {stop, Reason, Reply, NState}
%%  | commonReply()
%%
%% @doc Equivalent to `Mod:handle_call/3' in a gen_server.
%%
%% Note the difference in allowed return values. `{ok,NState}' and
%% `{noreply,NState}' are synonymous.
%%
%% `{noreply,NState}' is allowed as a return value from `handle_call/3',
%% since it could arguably add some clarity, but mainly because people are
%% used to it from gen_server.
%% @end
%%
handle_call({find_unknowns, Bloom}, _From, S, _I) ->
    Unknowns =
    lists:foldl(
      fun(K, Acc) ->
              case emqttd_bloom:is_element(K, Bloom) of
                  false ->
                      {ok, Val} = bitcask:get(S#st.subs, K),
                      [{K, Val}|Acc];
                  true ->
                      Acc
              end
      end, [], bitcask:list_keys(S#st.subs)),
    {reply, Unknowns, S};


handle_call({replicate, KVPairs}, _From, S, _I) ->
    Subs = S#st.subs,
    NewerItems =
    lists:foldl(
      fun({Key,Val}, Acc) ->
              {ClientId, Topic} = binary_to_term(Key),
              {TS, Node, QoS} = binary_to_term(Val),
              case bitcask:get(Subs, Key) of
                  not_found ->
                      bitcask:put(Subs, Key, Val),
                      add_topic_(Node, ClientId, binary_to_list(Topic), QoS),
                      Acc;
                  {ok, OwnVal} ->
                      case binary_to_term(OwnVal) of
                          {OwnTS, _, _} when OwnTS > TS ->
                              [{Key, OwnVal}|Acc];
                          _ ->
                              bitcask:put(Subs, Key, Val),
                              add_topic_(Node, ClientId, binary_to_list(Topic), QoS),
                              Acc
                      end
              end
      end, [], KVPairs),
    {reply, NewerItems, S};

handle_call(get_subscriptions, _From, S, _I) ->
    Subs =
    bitcask:fold( S#st.subs,
      fun(Key, Val, Acc) ->
              {ClientId, Topic} = binary_to_term(Key),
              {TS, Node, QoS} = binary_to_term(Val),
              [{ClientId, Topic, TS, Node, QoS}|Acc]
      end, []),
    {reply, Subs, S};


handle_call(which_nodes, _From, S, _I) ->
    {reply, S#st.nodes, S};

handle_call({cleanup_client, ClientId}, _From, State, _I) ->
    {reply, cleanup_client(State#st.subs, true, ClientId), State}.



%% @spec handle_cast(Msg::term(), State::state(), I::info()) ->
%%    {noreply, NState}
%%  | commonReply()
%%
%% @doc Equivalent to `Mod:handle_call/3' in a gen_server, except
%% (<b>NOTE</b>) for the possible return values.
%%
handle_cast(_Msg, S, _I) ->
    {noreply, S}.

%% @spec handle_info(Msg::term(), State::state(), I::info()) ->
%%     {noreply, NState}
%%   | commonReply()
%%
%% @doc Equivalent to `Mod:handle_info/3' in a gen_server,
%% except (<b>NOTE</b>) for the possible return values.
%%
%% This function will be called in response to any incoming message
%% not recognized as a call, cast, leader_call, leader_cast, from_leader
%% message, internal leader negotiation message or system message.
%% @end
handle_info(check_connectivity, S, _I) ->
    [node_alive(Node) || {Node, false} <- S#st.nodes],
    erlang:send_after(5000, self(), check_connectivity),
    {noreply, S#st{ready=ready(S#st.nodes)}};

handle_info({'DOWN', _, process, ClientPid, _}, State, _I) ->
    case ets:match_object(emqttd_client, #client{pid=ClientPid, _='_'}) of
        [] -> ignore;
        [#client{id=ClientId}] ->
            ets:delete(emqttd_client, ClientId)
    end,
    {noreply, State};

handle_info(_Msg, S, _I) ->
    {noreply, S}.

%% @spec code_change(FromVsn::string(), OldState::term(),
%%                   I::info(), Extra::term()) ->
%%       {ok, NState}
%%
%%    NState = state()
%%
%% @doc Similar to `code_change/3' in a gen_server callback module, with
%% the exception of the added argument.
%% @end
code_change(_FromVsn, S, _I, _Extra) ->
    {ok, S}.

%% @spec terminate(Reason::term(), State::state()) -> Void
%%
%% @doc Equivalent to `terminate/2' in a gen_server callback
%% module.
%% @end
terminate(_Reason, S) ->
    bitcask:close(S#st.subs),
    ok.




ready(Nodes) ->
    Ready = [ok || {_,false} <- Nodes] == [],
    ets:insert(emqttd_status, {ready, Ready}),
    Ready.

node_alive(Node) ->
    net_adm:ping(Node) == pong.

merge_nodes(Nodes, I) ->
    {Good, _Bad} = locks_leader:ask_candidates(which_nodes, I),
    lists:ukeysort(1, lists:flatten(lists:foldl(
        fun({_, Nodes2}, Acc) ->
                [Nodes2|Acc]
        end, [Nodes], Good))).


add_topic(St, Node, ClientId, Topic, QoS) when is_list(Topic) ->
    ok = bitcask:put(St, term_to_binary({ClientId, list_to_binary(Topic)}), term_to_binary({now(), Node, QoS})),
    add_topic_(Node, ClientId, Topic, QoS).

add_topic_(Node, ClientId, Topic, QoS) ->
    ets:insert(emqttd_trie_topic, #topic{name=Topic, node=Node}),
    case ets:lookup(emqttd_trie_node, Topic) of
        [TrieNode=#trie_node{topic=undefined}] ->
            ets:insert(emqttd_trie_node, TrieNode#trie_node{topic=Topic});
        [#trie_node{topic=Topic}] ->
            ignore;
        [] ->
            %add trie path
            [trie_add_path(Triple) || Triple <- emqtt_topic:triples(Topic)],
            %add last node
            ets:insert(emqttd_trie_node, #trie_node{node_id=Topic, topic=Topic})
    end,
    ets:insert(emqttd_subscriber, #subscriber{topic=Topic, qos=QoS, client=ClientId}).


del_topic(St, ClientId, Topic) when is_list(Topic) ->
    Key = term_to_binary({ClientId, list_to_binary(Topic)}),
    case bitcask:get(St, Key) of
        {ok, Val} ->
            {_TS, Node, _QoS} = binary_to_term(Val),
            ok = bitcask:delete(St, Key),
            del_topic_(Node, ClientId, Topic);
        _ ->
            ok
    end.

del_topic_(Node, ClientId, Topic) ->
    ets:match_delete(emqttd_subscriber, #subscriber{topic=Topic, client=ClientId, _='_'}),
    case ets:member(emqttd_subscriber, Topic) of
        false ->
            ets:delete_object(emqttd_trie_topic, #topic{name=Topic, node=Node}),
            case ets:lookup(emqttd_trie_topic, Topic) of
                [] -> trie_delete(Topic);
                _ ->
                    ignore
            end;
        true ->
            ok
    end.

trie_delete(Topic) ->
    case ets:lookup(emqttd_trie_node, Topic) of
        [#trie_node{edge_count=0}] ->
            ets:delete(emqttd_trie_node, Topic),
            trie_delete_path(lists:reverse(emqtt_topic:triples(Topic)));
        [TrieNode] ->
            ets:insert(emqttd_trie_node, TrieNode#trie_node{topic=Topic});
        [] ->
            ignore
    end.

trie_match(Words) ->
    trie_match(root, Words, []).

trie_match(NodeId, [], ResAcc) ->
    ets:lookup(emqttd_trie_node, NodeId) ++ 'trie_match_#'(NodeId, ResAcc);

trie_match(NodeId, [W|Words], ResAcc) ->
    lists:foldl(
      fun(WArg, Acc) ->
              case ets:lookup(emqttd_trie, #trie_edge{node_id=NodeId, word=WArg}) of
                  [#trie{node_id=ChildId}] -> trie_match(ChildId, Words, Acc);
                  [] -> Acc
              end
      end, 'trie_match_#'(NodeId, ResAcc), [W, "+"]).

'trie_match_#'(NodeId, ResAcc) ->
    case ets:lookup(emqttd_trie, #trie_edge{node_id=NodeId, word="#"}) of
        [#trie{node_id=ChildId}] ->
            ets:lookup(emqttd_trie_node, ChildId) ++ ResAcc;
        [] ->
            ResAcc
    end.

trie_add_path({Node, Word, Child}) ->
    Edge = #trie_edge{node_id=Node, word=Word},
    case ets:lookup(emqttd_trie_node, Node) of
        [TrieNode = #trie_node{edge_count=Count}] ->
            case ets:lookup(emqttd_trie, Edge) of
                [] ->
                    ets:insert(emqttd_trie_node, TrieNode#trie_node{edge_count=Count+1}),
                    ets:insert(emqttd_trie, #trie{edge=Edge, node_id=Child});
                [_] ->
                    ok
            end;
        [] ->
            ets:insert(emqttd_trie_node, #trie_node{node_id=Node, edge_count=1}),
            ets:insert(emqttd_trie, #trie{edge=Edge, node_id=Child})
    end.

trie_delete_path([]) ->
    ok;
trie_delete_path([{NodeId, Word, _}|RestPath]) ->
    Edge = #trie_edge{node_id=NodeId, word=Word},
    ets:delete(emqttd_trie, Edge),
    case ets:lookup(emqttd_trie_node, NodeId) of
        [#trie_node{edge_count=1, topic=undefined}] ->
            ets:delete(emqttd_trie_node, NodeId),
            trie_delete_path(RestPath);
        [TrieNode=#trie_node{edge_count=1, topic=_}] ->
            ets:insert(emqttd_trie_node, TrieNode#trie_node{edge_count=0});
        [TrieNode=#trie_node{edge_count=Count}] ->
            ets:insert(emqttd_trie_node, TrieNode#trie_node{edge_count=Count-1});
        [] ->
            throw({notfound, NodeId})
    end.


get_client_pid(ClientId) ->
    case ets:lookup(emqttd_client, ClientId) of
        [#client{node=Node, pid=ClientPid}] when Node == node() ->
            {ok, ClientPid};
        _ ->
            {error, not_found}
    end.


cleanup_client(_, false, _) -> ok;
cleanup_client(St, true, ClientId) ->
    emqttd_msg_store:clean_session(ClientId),
    case ets:match_object(emqttd_subscriber, #subscriber{client=ClientId, _='_'}) of
        [] -> ignore;
        Subs ->
            [del_topic(St, ClientId, Topic)
             || #subscriber{topic=Topic} <- Subs]
    end.
