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

-module(vmq_bridge).

-behaviour(gen_emqtt).

%% API
-export([start_link/3,
         setopts/3]).

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


-record(state, {
          host,
          port,
          publish_fun,
          subscribe_fun,
          unsubscribe_fun,
          opts,
          type,
          client_pid,
          bridge_transport,
          topics,
          client_opts,
          subscriptions=[]}).

%%%===================================================================
%%% API
%%%===================================================================
start_link(Host, Port, RegistryMFA) ->
    gen_server:start_link(?MODULE, [Host, Port, RegistryMFA], []).

setopts(BridgePid, Type, Opts) ->
    gen_server:call(BridgePid, {setopts, Type, Opts}).

%%%===================================================================
%%% gen_emqtt callbacks
%%%===================================================================
on_connect({coord, CoordinatorPid} = State) ->
    CoordinatorPid ! connected,
    {ok, State}.

on_connect_error(Reason, State) ->
    lager:error("connection failed due to ~p", [Reason]),
    {ok, State}.

on_disconnect(State) ->
    {ok, State}.

on_subscribe(_Topics, State) ->
    {ok, State}.

on_unsubscribe(_Topics, State) ->
    {ok, State}.

on_publish(Topic, Payload, {coord, CoordinatorPid} = State) ->
    CoordinatorPid ! {deliver_remote, Topic, Payload},
    {ok, State}.

init([Host, Port, RegistryMFA]) ->
    {M,F,A} = RegistryMFA,
    {RegisterFun, PublishFun, {SubscribeFun, UnsubscribeFun}} = apply(M,F,A),
    true = is_function(RegisterFun, 0),
    true = is_function(PublishFun, 3),
    true = is_function(SubscribeFun, 1),
    true = is_function(UnsubscribeFun, 1),
    ok = RegisterFun(),
    {ok, #state{host=Host,
                port=Port,
                publish_fun=PublishFun,
                subscribe_fun=SubscribeFun,
                unsubscribe_fun=UnsubscribeFun}};
init([{coord, _CoordinatorPid} = State]) ->
    {ok, State}.

handle_call({setopts, Type, Opts}, _From,
            #state{host=Host, port=Port, client_pid=undefined} = State) ->
    ClientOpts = client_opts(Type, Host, Port, Opts),
    {ok, Pid} = gen_emqtt:start_link(?MODULE, [{coord, self()}], ClientOpts),
    {reply, ok, State#state{client_pid=Pid,
                            opts=Opts,
                            type=Type}};
handle_call({setopts, Type, Opts}, _From, #state{type=Type, opts=Opts} = State) ->
    {reply, ok, State};
handle_call({setopts, Type, Opts}, _From, #state{type=Type, opts=OldOpts,
                                                 host=Host, port=Port,
                                                 client_pid=ClientPid,
                                                 subscriptions=Subscriptions,
                                                 subscribe_fun=SubscribeFun,
                                                 unsubscribe_fun=UnsubscribeFun
                                                } = State) ->
    NewState =
    case lists:keydelete(topics, 1, Opts)
         == lists:keydelete(topics, 1, OldOpts) of
        true ->
            % Client Options did not change
            % maybe subscriptions changed
            Topics = proplists:get_value(topics, Opts),
            NewSubscriptions =
            case proplists:get_value(topics, OldOpts) of
                Topics ->
                    %% subscriptions did not change
                    Subscriptions;
                _ ->
                    bridge_unsubscribe(ClientPid, Subscriptions, UnsubscribeFun),
                    bridge_subscribe(ClientPid, Topics, SubscribeFun, [])
            end,
            State#state{opts=Opts, subscriptions=NewSubscriptions};
        false ->
            % Client Options changed
            % stop client
            bridge_unsubscribe(ClientPid, Subscriptions, UnsubscribeFun),
            ok = gen_emqtt:cast(ClientPid, {coord, self(), stop}),
            % restart client
            ClientOpts = client_opts(Type, Host, Port, Opts),
            {ok, Pid} = gen_emqtt:start_link(?MODULE, [{coord, self()}], ClientOpts),
            State#state{client_pid=Pid, opts=Opts}
    end,
    {reply, ok, NewState};
handle_call({setopts, Type, Opts}, _From, #state{host=Host, port=Port,
                                                 client_pid=ClientPid,
                                                 subscriptions=Subscriptions,
                                                 unsubscribe_fun=UnsubscribeFun
                                                } = State) ->
    %% Other Type (ssl or tcp) -> stop client
    bridge_unsubscribe(ClientPid, Subscriptions, UnsubscribeFun),
    ok = gen_emqtt:cast(ClientPid, {coord, self(), stop}),
    % restart client
    ClientOpts = client_opts(Type, Host, Port, Opts),
    {ok, Pid} = gen_emqtt:start_link(?MODULE, [{coord, self()}], ClientOpts),
    {reply, ok, State#state{client_pid=Pid, opts=Opts, type=Type}};

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({coord, CoordinatorPid, stop}, {coord, CoordinatorPid} = State) ->
    {stop, normal, State};
handle_cast(_Req, State) ->
    {noreply, State}.

handle_info(connected, #state{client_pid=Pid, opts=Opts,
                              subscribe_fun=SubscribeFun,
                              host=Host, port=Port} = State) ->
    lager:debug("connected to: ~s:~p", [Host,Port]),
    Topics = proplists:get_value(topics, Opts),
    Subscriptions = bridge_subscribe(Pid, Topics, SubscribeFun, []),
    {noreply, State#state{subscriptions=Subscriptions}};
handle_info({deliver_remote, Topic, Payload},
            #state{publish_fun=PublishFun, subscriptions=Subscriptions} = State) ->
    lists:foreach(
      fun({{in, T}, LocalPrefix}) ->
              case vmq_topic:match(Topic, T) of
                  true ->
                      ok = PublishFun(routing_key(LocalPrefix, Topic), Payload,
                                     #{});
                  false ->
                      ok
              end;
         (_) ->
              ok
      end, Subscriptions),
    {noreply, State};
handle_info({deliver, Topic, Payload, _QoS, _IsRetained, _IsDup},
            #state{subscriptions=Subscriptions, client_pid=ClientPid} = State) ->
    lists:foreach(
      fun({{out, T}, QoS, RemotePrefix}) ->
              case vmq_topic:match(Topic, T) of
                  true ->
                      ok = gen_emqtt:publish(ClientPid, routing_key(RemotePrefix, Topic) ,
                                             Payload, QoS);
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

bridge_subscribe(Pid, [{Topic, in, QoS, LocalPrefix, _} = BT|Rest],
                 SubscribeFun, Acc) ->
    case vmq_topic:validate_topic(subscribe, list_to_binary(Topic)) of
        {ok, TTopic} ->
            gen_emqtt:subscribe(Pid, TTopic, QoS),
            bridge_subscribe(Pid, Rest, SubscribeFun, [{{in, TTopic},
                                                        validate_prefix(LocalPrefix)}|Acc]);
        {error, Reason} ->
            error_logger:warning_msg("can't validate bridge topic conf ~p due to ~p",
                                     [BT, Reason]),
            bridge_subscribe(Pid, Rest, SubscribeFun, Acc)
    end;
bridge_subscribe(Pid, [{Topic, out, QoS, _, RemotePrefix} = BT|Rest],
                 SubscribeFun, Acc) ->
    case vmq_topic:validate_topic(subscribe, list_to_binary(Topic)) of
        {ok, TTopic} ->
            {ok, _} = SubscribeFun(TTopic),
            bridge_subscribe(Pid, Rest, SubscribeFun, [{{out, TTopic}, QoS,
                                                        validate_prefix(RemotePrefix)}|Acc]);
        {error, Reason} ->
            error_logger:warning_msg("can't validate bridge topic conf ~p due to ~p",
                                     [BT, Reason]),
            bridge_subscribe(Pid, Rest, SubscribeFun, Acc)
    end;

bridge_subscribe(Pid, [{Topic, both, QoS, LocalPrefix, RemotePrefix} = BT|Rest],
                 SubscribeFun, Acc) ->
    case vmq_topic:validate_topic(subscribe, list_to_binary(Topic)) of
        {ok, TTopic} ->
            gen_emqtt:subscribe(Pid, TTopic, QoS),
            {ok, _} = SubscribeFun(TTopic),
            bridge_subscribe(Pid, Rest, SubscribeFun, [{{in, TTopic},
                                                        validate_prefix(LocalPrefix)},
                                                       {{out, TTopic}, QoS,
                                                        validate_prefix(RemotePrefix)}|Acc]);
        {error, Reason} ->
            error_logger:warning_msg("can't validate bridge topic conf ~p due to ~p",
                                     [BT, Reason]),
            bridge_subscribe(Pid, Rest, SubscribeFun, Acc)
    end;
bridge_subscribe(_, [], _, Acc) -> Acc.


bridge_unsubscribe(Pid, [{{in, Topic}, _}|Rest], UnsubscribeFun) ->
    gen_emqtt:unsubscribe(Pid, Topic),
    bridge_unsubscribe(Pid, Rest, UnsubscribeFun);
bridge_unsubscribe(Pid, [{{out, Topic}, _, _}|Rest], UnsubscribeFun) ->
    UnsubscribeFun(Topic),
    bridge_unsubscribe(Pid, Rest, UnsubscribeFun);
bridge_unsubscribe(_, [], _) ->
    ok.

validate_prefix(undefined) -> undefined;
validate_prefix([W|_] = Prefix) when is_binary(W) -> Prefix;
validate_prefix(Prefix) when is_list(Prefix) ->
    validate_prefix(list_to_binary(Prefix));
validate_prefix(Prefix) ->
    case vmq_topic:validate_topic(publish, Prefix) of
        {error, no_empty_topic_allowed} ->
            undefined;
        {ok, ParsedPrefix} ->
            ParsedPrefix
    end.

routing_key(undefined, Topic) -> Topic;
routing_key(Prefix, Topic) -> lists:flatten([Prefix, Topic]).

client_opts(tcp, Host, Port, Opts) ->
    OOpts =
    [{host, Host},
     {port, Port},
     {username, proplists:get_value(username, Opts)},
     {password, proplists:get_value(password, Opts)},
     {client,   proplists:get_value(client_id, Opts)},
     {clean_session, proplists:get_value(cleansession, Opts, false)},
     {keepalive_interval, proplists:get_value(keepalive_interval, Opts)},
     {reconnect_timeout, proplists:get_value(restart_timeout, Opts)},
     {transport, {gen_tcp, []}}
     |case proplists:get_value(try_private, Opts, true) of
          true ->
              [{proto_version, 131}]; %% non-spec
          false ->
              []
      end],
    [P || {_, V}=P <- OOpts, V /= undefined];
client_opts(ssl, Host, Port, Opts) ->
    TCPOpts = client_opts(tcp, Host, Port, Opts),
    SSLOpts = [{certfile, proplists:get_value(certfile, Opts)},
               {cacertfile, proplists:get_value(cafile, Opts)},
               {keyfile, proplists:get_value(keyfile, Opts)},
               {verify, case proplists:get_value(insecure, Opts) of
                            true -> verify_none;
                            _ -> verify_peer
                        end},
               {versions, case proplists:get_value(tls_version, Opts) of
                              undefined -> undefined;
                              V -> [V]
                          end},
               {psk_identity, proplists:get_value(identity, Opts)},
               {user_lookup_fun, case {proplists:get_value(identity, Opts),
                                       proplists:get_value(psk, Opts)}
                                 of
                                     {Identity, Psk}
                                       when is_list(Identity) and is_list(Psk) ->
                                         BinPsk = to_bin(Psk),
                                         {fun(psk, I, _) when I == Identity ->
                                                  {ok, BinPsk};
                                             (_, _, _) -> error
                                          end, []};
                                     _ -> undefined
                                 end}
                ],

    lists:keyreplace(transport, 1, TCPOpts,
                     {transport, {ssl, [P||{_,V}=P <- SSLOpts, V /= undefined]}}).

%% @spec to_bin(string()) -> binary()
%% @doc Convert a hexadecimal string to a binary.
to_bin(L) ->
    to_bin(L, []).

%% @doc Convert a hex digit to its integer value.
dehex(C) when C >= $0, C =< $9 ->
    C - $0;
dehex(C) when C >= $a, C =< $f ->
    C - $a + 10;
dehex(C) when C >= $A, C =< $F ->
    C - $A + 10.

to_bin([], Acc) ->
    iolist_to_binary(lists:reverse(Acc));
to_bin([C1, C2 | Rest], Acc) ->
    to_bin(Rest, [(dehex(C1) bsl 4) bor dehex(C2) | Acc]).
