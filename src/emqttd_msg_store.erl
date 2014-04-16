-module(emqttd_msg_store).
-behaviour(gen_server).

-export([start_link/1,
         persist_for_later/3,
         deliver_from_store/2,
         deliver_retained/3,
         get_msg_from_msg_store/2,
         clean_session/1,
         persist_retain_msg/2,
         remove_retained_msg/1,
         reset_retained_msg/1
         ]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).


-record(state, {store}).
-record(msg_idx_item, {id, node, qos, msg_ref, ts}).

start_link(MsgStoreDir) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [MsgStoreDir], []).

persist_for_later([], _, _) -> ok;
persist_for_later(UnroutableClients, RoutingKey, Payload) ->
    gen_server:call(?MODULE, {persist, UnroutableClients, RoutingKey, Payload}).

deliver_from_store(ClientId, ClientPid) ->
    gen_server:call(?MODULE, {deliver, ClientId, ClientPid}).

deliver_retained(ClientPid, Topic, QoS) ->
    gen_server:call(?MODULE, {deliver_retained, ClientPid, Topic, QoS}).

clean_session(ClientId) ->
    gen_server:call(?MODULE, {clean_session, ClientId}),
    [rpc:call(N, ?MODULE, clean_session, [ClientId]) || N <- nodes()],
    ok.

persist_retain_msg(RoutingKey, Msg) ->
    gen_server:call(?MODULE, {persist_retain_msg, RoutingKey, Msg}).

remove_retained_msg(RoutingKey) ->
    %% should only be called through RPC
    gen_server:call(?MODULE, {remove_retained_msg, RoutingKey}).

reset_retained_msg(RoutingKey) ->
    gen_server:call(?MODULE, {reset_retained_msg, RoutingKey}).

get_msg_from_msg_store(MsgRef, IsRetain) ->
    gen_server:call(?MODULE, {get_msg, MsgRef, IsRetain}).

init([MsgStoreDir]) ->
    filelib:ensure_dir(MsgStoreDir),
    case mnesia:create_table(msg_idx_item, [
                                            {disc_copies, [node()]},
                                            {type, bag},
                                            {attributes, record_info(fields, msg_idx_item)}]) of
        {atomic, ok} ->
            mnesia:add_table_copy(msg_idx_item, node(), ram_copies);
        {aborted, {already_exists, msg_idx_item}} ->
            mnesia:add_table_copy(msg_idx_item, node(), ram_copies)
    end,
    MsgStore = bitcask:open(MsgStoreDir, [read_write]),
    {ok, #state{store=MsgStore}}.

handle_call({persist, Clients, RoutingKey, Payload}, _From, State) ->
    #state{store=MsgStore} = State,
    MsgRef = crypto:hash(md5, [RoutingKey, Payload]),
    BRoutingKey = list_to_binary(RoutingKey),
    ok = bitcask:put(MsgStore, MsgRef, <<(byte_size(BRoutingKey)):16, BRoutingKey/binary, Payload/binary>>),
	case mnesia:transaction(
           fun() ->
                   lists:foreach(fun({ClientId, QoS}) ->
                                         mnesia:write(#msg_idx_item{id={client, ClientId},
                                                                    node=node(),
                                                                    qos=QoS,
                                                                    msg_ref=MsgRef,
                                                                    ts=now()})
                                 end, Clients)
           end) of
        {atomic, _} ->
            {reply, ok, State};
        {aborted, Reason} ->
            maybe_delete_from_msg_store(MsgStore, MsgRef),
            {reply, {error, Reason}, State}
    end;

handle_call({deliver, ClientId, ClientPid}, _From, State) ->
    #state{store=MsgStore} = State,
    case mnesia:transaction(
           fun() ->
                   lists:foreach(fun
                                     (#msg_idx_item{msg_ref=MsgRef, node=Node,
                                                    qos=QoS} = Obj) when Node == node() ->
                                         {ok, <<RLen:16, RoutingKey:RLen/binary,
                                                Payload/binary>>} = bitcask:get(MsgStore, MsgRef),
                                         emqttd_connection_reg:send_to_client(
                                           ClientPid, RoutingKey, Payload, QoS),
                                         mnesia:delete_object(Obj),
                                         maybe_delete_from_msg_store(MsgStore, MsgRef);
                                     (#msg_idx_item{msg_ref=MsgRef, node=Node,
                                                    qos=QoS} = Obj) ->
                                         {ok, <<RLen:16, RoutingKey:RLen/binary,
                                                Payload/binary>>} = rpc:call(Node, ?MODULE, get_msg_from_msg_store, [MsgRef, false]),
                                         emqttd_connection_reg:send_to_client(
                                           ClientPid, RoutingKey, Payload, QoS),
                                         mnesia:delete_object(Obj)
                                 end, mnesia:read(msg_idx_item, {client, ClientId}))
           end) of
        {atomic, _} ->
            {reply, ok, State};
        {aborted, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call({deliver_retained, ClientPid, SubTopic, QoS}, _From, State) ->
    #state{store=MsgStore} = State,
    case mnesia:transaction(
           fun() ->
                   Words = emqtt_topic:words(SubTopic),
                   lists:foreach(fun(#msg_idx_item{id={retain, RoutingKey},
                                                   msg_ref=MsgRef, node=Node}) ->
                                         RWords = emqtt_topic:words(RoutingKey),
                                         case emqtt_topic:match(RWords, Words) of
                                             true when Node == node() ->
                                                 {ok, Payload} = bitcask:get(MsgStore, MsgRef),
                                                 ClientPid ! {route_retained, RoutingKey, Payload, QoS};
                                             true ->
                                                 {ok, Payload} = rpc:call(Node, ?MODULE, get_msg_from_msg_store, [MsgRef, true]),
                                                 ClientPid ! {route_retained, RoutingKey, Payload, QoS};
                                             false ->
                                                 ignore
                                         end
                                 end, mnesia:match_object(#msg_idx_item{id={retain, '_'}, _='_'}))
           end) of
        {atomic, _} ->
            {reply, ok, State};
        {aborted, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call({clean_session, ClientId}, _From, State) ->
    #state{store=MsgStore} = State,
    case mnesia:transaction(
           fun() ->
                   lists:foreach(fun
                                     (#msg_idx_item{msg_ref=MsgRef, node=Node} = Obj) when Node == node() ->
                                         mnesia:delete_object(Obj),
                                         maybe_delete_from_msg_store(MsgStore, MsgRef);
                                     (_) -> ok
                                 end, mnesia:read(msg_idx_item, {client, ClientId}))
           end) of
        {atomic, _} ->
            {reply, ok, State};
        {aborted, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call({persist_retain_msg, RoutingKey, Message}, _From, State) ->
    #state{store=MsgStore} = State,
    MsgRef = crypto:hash(md5, [RoutingKey]),
    ok = bitcask:put(MsgStore, MsgRef, Message),
	case mnesia:transaction(
           fun() ->
                   case mnesia:read(msg_idx_item, {retain, RoutingKey}) of
                       [#msg_idx_item{node=Node}] when Node /= node() ->
                           ok = rpc:call(Node, ?MODULE, remove_retained_msg, [RoutingKey]);
                       _ ->
                           ok
                   end,
                   mnesia:write(#msg_idx_item{id={retain, RoutingKey},
                                              node=node(),
                                              msg_ref=MsgRef,
                                              ts=now()})
           end) of
        {atomic, _} ->
            {reply, ok, State};
        {aborted, Reason} ->
            bitcask:delete(MsgStore, MsgRef),
            {reply, {error, Reason}, State}
    end;

handle_call({reset_retained_msg, RoutingKey}, _From, State) ->
    #state{store=MsgStore} = State,
    MsgRef = crypto:hash(md5, [RoutingKey]),
	case mnesia:transaction(
           fun() ->
                   case mnesia:read(msg_idx_item, {retain, RoutingKey}) of
                       [#msg_idx_item{node=Node}] when Node /= node() ->
                           ok = rpc:call(Node, ?MODULE, remove_retained_msg, [RoutingKey]);
                       _ ->
                           bitcask:delete(MsgStore, MsgRef),
                           ok
                   end,
                   mnesia:delete({msg_idx_item, {retain, RoutingKey}})
           end) of
        {atomic, _} ->
            {reply, ok, State};
        {aborted, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call({remove_retained_msg, RoutingKey}, _From, State) ->
    #state{store=MsgStore} = State,
    MsgRef = crypto:hash(md5, [RoutingKey]),
    bitcask:delete(MsgStore, MsgRef),
    {reply, ok, State};

handle_call({get_msg, MsgRef, IsRetain}, _From, State) ->
    #state{store=MsgStore} = State,
    {ok, Msg} = bitcask:get(MsgStore, MsgRef),
    case IsRetain of
        false ->
            maybe_delete_from_msg_store(MsgStore, MsgRef);
        true ->
            ok
    end,
    {reply, {ok, Msg}, State}.

handle_cast(_Req, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

maybe_delete_from_msg_store(MsgStore, MsgRef) ->
    case mnesia:dirty_match_object(#msg_idx_item{msg_ref=MsgRef, node=node(), _='_'}) of
        [] ->
            bitcask:delete(MsgStore, MsgRef);
        _ ->
            ok
    end.
