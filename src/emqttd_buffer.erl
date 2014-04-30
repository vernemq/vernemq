-module(emqttd_buffer).
-behaviour(gen_server).

-export([start_link/2,
         store_and_forward/4,
         store/5,
         release_and_forward/2
         ]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).


-record(state, {store}).
-define(SUP, emqttd_buffer_sup).
-define(TIMEOUT, 1000).

start_link(BufferDir, Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [BufferDir, atom_to_list(Name)], []).

store_and_forward(ClientId, RoutingKey, Message, IsRetain) ->
    gen_server:call(get_buffer(ClientId), {store_and_forward, ClientId, {RoutingKey, Message, IsRetain}}).

store(ClientId, MessageId, RoutingKey, Message, IsRetain) ->
    gen_server:call(get_buffer(ClientId), {store, ClientId, MessageId, {RoutingKey, Message, IsRetain}}).

release_and_forward(ClientId, MessageId) ->
    gen_server:call(get_buffer(ClientId), {release_and_forward, ClientId, MessageId}).


init([BufferDir, Name]) ->
    filelib:ensure_dir(BufferDir),
    MsgStore = bitcask:open(filename:join(BufferDir, Name), [read_write]),
    {ok, #state{store=MsgStore}, ?TIMEOUT}.

handle_call({store_and_forward, ClientId, {RoutingKey, Payload, IsRetain} = Item}, _From, State) ->
    #state{store=MsgStore} = State,
    BClientId = list_to_binary(ClientId),
    ok = bitcask:put(MsgStore, BClientId, term_to_binary(Item)),
    emqttd_trie:publish(RoutingKey, Payload, IsRetain),
    ok = bitcask:delete(MsgStore, BClientId),
    {reply, ok, State};

handle_call({store, ClientId, MessageId, Item}, _From, State) ->
    #state{store=MsgStore} = State,
    ItemId = term_to_binary({ClientId, MessageId}),
    ok = bitcask:put(MsgStore, ItemId, term_to_binary(Item)),
    {reply, ok, State};

handle_call({release_and_forward, ClientId, MessageId}, _From, State) ->
    #state{store=MsgStore} = State,
    ItemId = term_to_binary({ClientId, MessageId}),
    case bitcask:get(MsgStore, ItemId) of
        {ok, BItem} ->
            {RoutingKey, Payload, IsRetain} = binary_to_term(BItem),
            emqttd_trie:publish(RoutingKey, Payload, IsRetain),
            ok = bitcask:delete(MsgStore, ItemId);
        _ ->
            ignore_not_found
    end,
    {reply, ok, State}.

handle_cast(_Req, State) ->
    {noreply, State}.

handle_info(timeout, State) ->
    #state{store=MsgStore} = State,
    bitcask:fold(MsgStore, fun(ItemId, BItem, Acc) ->
                                   {RoutingKey, Payload, IsRetain} = binary_to_term(BItem),
                                   emqttd_trie:publish(RoutingKey, Payload, IsRetain),
                                   io:format("publish from old buffer ~p ~n", [RoutingKey]),
                                   ok = bitcask:delete(MsgStore, ItemId),
                                   Acc
                           end, []),
    {noreply, State}.

terminate(_Reason, State) ->
    #state{store=MsgStore} = State,
    bitcask:close(MsgStore),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

get_buffer(ClientId) ->
    Children = supervisor:which_children(?SUP),
    {_Name, Pid, worker, _} = lists:nth(erlang:phash2(ClientId) rem length(Children), Children),
    Pid.
