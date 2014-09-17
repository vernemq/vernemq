-module(emqttd_bridge).

-behaviour(gen_emqtt).

%% API
-export([start_link/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([on_connect/1,
         on_connect_error/2,
         on_disconnect/1,
         on_subscribe/2,
         on_unsubscribe/2,
         on_publish/3]).


-record(state, {config=[], subscriptions=[], publish_fun, subscribe_fun}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(RegistryMFA, BridgeConfig, ClientOpts) ->
    gen_emqtt:start_link(?MODULE, [RegistryMFA, BridgeConfig], ClientOpts).

%%%===================================================================
%%% gen_emqtt callbacks
%%%===================================================================
on_connect(State) ->
    #state{config=Config, subscribe_fun=SubscribeFun} = State,
    Subscriptions =
    lists:foldl(fun({Topic, Direction, QoS, LocalPrefix, RemotePrefix}, Acc) ->
                        case Direction of
                            in ->
                                RemoteTopic = lists:flatten([LocalPrefix, Topic]),
                                gen_emqtt:subscribe(self(), RemoteTopic, QoS),
                                [{{in, RemoteTopic}, LocalPrefix}|Acc];
                            out ->
                                LocalTopic = lists:flatten([RemotePrefix, Topic]),
                                ok = SubscribeFun(LocalTopic),
                                [{{out, LocalTopic}, QoS, RemotePrefix}|Acc]
                        end
                end, [], Config),
    {ok, State#state{subscriptions=Subscriptions}}.

on_connect_error(Reason, State) ->
    io:format("--- error ~p~n", [Reason]),
    {ok, State}.

on_disconnect(State) ->
    {ok, State}.

on_subscribe(_Topics, State) ->
    {ok, State}.

on_unsubscribe(_Topics, State) ->
    {ok, State}.

on_publish(Topic, Payload, #state{subscriptions=Subscriptions, publish_fun=PublishFun} = State) ->
    io:format("--- publish ~p ~p~n", [Topic, Payload]),
    case lists:keyfind({in, Topic}, 1, Subscriptions) of
        {_, LocalPrefix} ->
            ok = PublishFun(lists:flatten([LocalPrefix, Topic]), Payload);
        _ ->
            ignore
    end,
    {ok, State}.

%
init([RegistryMFA, BridgeConfig]) ->
    {M,F,A} = RegistryMFA,
    {RegisterFun, PublishFun, SubscribeFun} = apply(M,F,A),
    true = is_function(RegisterFun, 0),
    true = is_function(PublishFun, 2),
    true = is_function(SubscribeFun, 1),
    ok = RegisterFun(),
    {ok, #state{config=BridgeConfig,
                publish_fun=PublishFun,
                subscribe_fun=SubscribeFun}}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast(_Req, State) ->
    {noreply, State}.
handle_info({deliver, Topic, Payload, 0, _IsRetained, _IsDup, _Ref},
            #state{subscriptions=Subscriptions} = State) ->
    Words = emqtt_topic:words(Topic),
    lists:foreach(
      fun({{out, T}, QoS, RemotePrefix}) ->
              TWords = emqtt_topic:words(T),
              case emqtt_topic:match(Words, TWords) of
                  true ->
                      io:format("--- deliver ~p ~p~n", [Topic, Payload]),
                      ok = gen_emqtt:publish(self(), lists:flatten([RemotePrefix, Topic]) , Payload, QoS);
                  false ->
                      ok
              end;
         (_) ->
              ok
      end, Subscriptions),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
