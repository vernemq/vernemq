%% Copyright Gojek

-module(vmq_events_sidecar_plugin).

-include("../include/vmq_events_sidecar.hrl").
-include_lib("vernemq_dev/include/vernemq_dev.hrl").

-behaviour(gen_server).
-behaviour(on_register_hook).
-behaviour(on_publish_hook).
-behaviour(on_subscribe_hook).
-behaviour(on_unsubscribe_hook).
-behaviour(on_deliver_hook).
-behaviour(on_offline_message_hook).
-behaviour(on_client_wakeup_hook).
-behaviour(on_client_offline_hook).
-behaviour(on_client_gone_hook).
-behaviour(on_session_expired_hook).
-behaviour(on_delivery_complete_hook).

-export([on_register/3,
         on_publish/6,
         on_subscribe/3,
         on_unsubscribe/3,
         on_deliver/6,
         on_offline_message/5,
         on_client_wakeup/1,
         on_client_offline/1,
         on_client_gone/1,
         on_session_expired/1,
         on_delivery_complete/6]).

%% API
-export([start_link/0
        ,enable_event/1
        ,disable_event/1
        ,all_hooks/0
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {}).
-define(TBL, vmq_events_sidecar_table).

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
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec enable_event(hook_name()) -> any().
enable_event(HookName) when is_atom(HookName) ->
    gen_server:call(?MODULE, {enable_event, HookName}).

-spec disable_event(hook_name()) -> any().
disable_event(HookName) when is_atom(HookName) ->
    gen_server:call(?MODULE, {disable_event, HookName}).

-spec all_hooks() -> any().
all_hooks() ->
    ets:foldl(fun({HookName}, Acc) ->
                      [{HookName}|Acc]
              end,
             [],
             ?TBL).

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
    process_flag(trap_exit, true),
    ets:new(?TBL, [public, ordered_set, named_table, {read_concurrency, true}]),
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

handle_call({enable_event, Hook}, _From, State) ->
    Reply =
        case ets:lookup(?TBL, Hook) of
            [] ->
                enable_hook(Hook),
                ets:insert(?TBL, {Hook}),
                ok;
            [{_}] ->
              {error, already_registered}
        end,
    {reply, Reply, State};
handle_call({disable_event, Hook}, _From, State) ->
    Reply =
        case ets:lookup(?TBL, Hook) of
            [] ->
                {error, not_found};
            [{_}] ->
                disable_hook(Hook),
                ets:delete(?TBL, Hook),
                ok
        end,
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
handle_info(_Info, State) ->
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
%%% Hook functions
%%%===================================================================
%% called as an all_till_ok hook
-spec on_register(peer(), subscriber_id(), username()) -> 'next'.
on_register(Peer, SubscriberId, UserName) ->
    {PPeer, Port} = peer(Peer),
    {MP, ClientId} = subscriber_id(SubscriberId),
    send_event({on_register, {MP,
                              ClientId,
                              PPeer,
                              Port,
                              normalise(UserName)}}).

-spec on_publish(username(), subscriber_id(), qos(), topic(), payload(), flag()) -> 'next'.
on_publish(UserName, SubscriberId, QoS, Topic, Payload, IsRetain) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    send_event({on_publish, {MP,
                            ClientId,
                            normalise(UserName),
                            QoS,
                            unword(Topic),
                            Payload,
                            IsRetain}}).

-spec on_subscribe(username(), subscriber_id(), [topic()]) -> 'next'.
on_subscribe(UserName, SubscriberId, Topics) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    send_event({on_subscribe, {MP,
                              ClientId,
                              normalise(UserName),
                              [[unword(T), from_internal_qos(QoS)] || {T, QoS} <- Topics]}}).

-spec on_unsubscribe(username(), subscriber_id(), [topic()]) ->
    'next' | 'ok' | {'ok',on_unsubscribe_hook:unsub_modifiers()}.
on_unsubscribe(UserName, SubscriberId, Topics) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    send_event({on_unsubscribe, {MP,
                                ClientId,
                                normalise(UserName),
                                [unword(T) || T <- Topics]}}).

-spec on_deliver(username(), subscriber_id(), qos(), topic(), payload(), flag()) ->
    'next' | 'ok' | {'ok', payload()|[on_deliver_hook:msg_modifier()]}.
on_deliver(UserName, SubscriberId, QoS, Topic, Payload, IsRetain) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    send_event({on_deliver, {MP,
                            ClientId,
                            normalise(UserName),
                            QoS,
                            unword(Topic),
                            Payload,
                            IsRetain}}).

-spec on_delivery_complete(username(), subscriber_id(), qos(), topic(), payload(), flag()) -> 'next'.
on_delivery_complete(UserName, SubscriberId, QoS, Topic, Payload, IsRetain) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    send_event({on_delivery_complete, {MP,
                                      ClientId,
                                      normalise(UserName),
                                      QoS,
                                      unword(Topic),
                                      Payload,
                                      IsRetain}}).

-spec on_offline_message(subscriber_id(), qos(), topic(), payload(), flag()) -> 'next'.
on_offline_message(SubscriberId, QoS, Topic, Payload, IsRetain) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    send_event({on_offline_message, {MP,
                                    ClientId,
                                    QoS,
                                    unword(Topic),
                                    Payload,
                                    IsRetain}}).

-spec on_client_wakeup(subscriber_id()) -> 'next'.
on_client_wakeup(SubscriberId) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    send_event({on_client_wakeup, {MP,
                                  ClientId}}).

-spec on_client_offline(subscriber_id()) -> 'next'.
on_client_offline(SubscriberId) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    send_event({on_client_offline, {MP,
                                    ClientId}}).

-spec on_client_gone(subscriber_id()) -> 'next'.
on_client_gone(SubscriberId) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    send_event({on_client_gone, {MP,
                                ClientId}}).

-spec on_session_expired(subscriber_id()) -> 'next'.
on_session_expired(SubscriberId) ->
    {MP, ClientId} = subscriber_id(SubscriberId),
    send_event({on_session_expired, {MP,
                                    ClientId}}).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec enable_hook(hook_name()) -> 'ok' | {'error','no_matching_callback_found'}.
enable_hook(HookName) ->
    check_exported_callback(HookName, ?MODULE:module_info(exports)).

-spec disable_hook(hook_name()) -> 'ok' | {'error','no_matching_callback_found'}.
disable_hook(HookName) ->
    uncheck_exported_callback(HookName, ?MODULE:module_info(exports)).

-spec check_exported_callback(hook_name(),maybe_improper_list()) -> 'ok' | {'error','no_matching_callback_found'}.
check_exported_callback(HookName, [{HookName, _Arity}|_]) ->
    ok;
check_exported_callback(HookName, [_|Exports]) ->
    check_exported_callback(HookName, Exports);
check_exported_callback(_, []) -> {error, no_matching_callback_found}.

-spec uncheck_exported_callback(hook_name(),maybe_improper_list()) -> 'ok' | {'error','no_matching_callback_found'}.
uncheck_exported_callback(HookName, [{HookName, _Arity}|_]) ->
    ok;
uncheck_exported_callback(HookName, [_|Exports]) ->
    uncheck_exported_callback(HookName, Exports);
uncheck_exported_callback(_, []) -> {error, no_matching_callback_found}.

-spec send_event(tuple()) -> 'next' | 'ok'.
send_event({HookName, EventPayload}) ->
    case ets:lookup(?TBL, HookName) of
        [] ->
            next;
        [{_}] ->
            vmq_metrics:incr_sidecar_events(HookName),
            case shackle:cast(?APP, {HookName, os:system_time(), EventPayload}, undefined) of
                {ok, _} -> ok;
                {error, Reason} ->
                    lager:error("Error sending event(shackle:cast): ~p", [Reason]),
                    vmq_metrics:incr_sidecar_events_error(HookName),
                    next
            end
    end.

-spec normalise(_) -> any().
normalise(undefined) ->
  <<>>;
normalise(Val) ->
  Val.

-spec unword(topic()) -> binary().
unword(T) ->
    iolist_to_binary(vmq_topic:unword(T)).

-spec peer({inet:ip_address(), integer()}) ->
     {'undefined' | binary(),'undefined' | integer()}.
peer({Peer, Port}) when is_tuple(Peer) and is_integer(Port) ->
    case inet:ntoa(Peer) of
        {error, einval} ->
            {undefined, undefined};
        PeerStr ->
            {list_to_binary(PeerStr), Port}
    end.

-spec subscriber_id(subscriber_id()) -> {binary(), client_id()}.
subscriber_id({"", ClientId}) -> {<<>>, ClientId};
subscriber_id({MP, ClientId}) -> {list_to_binary(MP), ClientId}.

-spec from_internal_qos('not_allowed' | integer() | {integer(),map()}) -> integer() | {integer(),map()}.
from_internal_qos(not_allowed) ->
    128;
from_internal_qos(V) when is_integer(V) ->
    V;
from_internal_qos({QoS, Opts}) when is_integer(QoS),
                                    is_map(Opts) ->
    {QoS, Opts}.
