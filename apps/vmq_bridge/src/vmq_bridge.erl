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

-behaviour(gen_mqtt_client).

%% API
-export([
    start_link/5,
    info/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3,
    get_metrics/1
]).

-export([
    on_connect/1,
    on_connect_error/2,
    on_disconnect/1,
    on_subscribe/2,
    on_unsubscribe/2,
    on_publish/4
]).

% gen_mqtt stats callback
-export([stats/2]).

-record(state, {
    name,
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
    counters_ref,
    internal_subscriber,
    %% subscriptions on the remote broker
    subs_remote = [],
    %% subscriptions on the local broker
    subs_local = []
}).

%%%===================================================================
%%% API
%%%===================================================================
start_link(Type, Host, Port, RegistryMFA, Opts) ->
    gen_server:start_link(?MODULE, [Type, Host, Port, RegistryMFA, Opts], []).

info(Pid) ->
    gen_server:call(Pid, info, infinity).

get_metrics(Pid) ->
    % With the Bridge plugin running, the metrics system will call in here.
    gen_server:call(Pid, get_metrics, 1000).
% We'll wait for a maximum of 1 second for the Bridge metrics to not
% delay other metrics. If the Bridge is not able to deliver
% the metrics in 1 second,  Bridge metrics will not be included in the metrics call.
% (like 'vmq-admin metrics show' etc.)

%%%===================================================================
%%% gen_mqtt_client callbacks. State = {coord, CoordPid}. CoordPid = self()
%%%===================================================================
on_connect({coord, CoordinatorPid} = State) ->
    CoordinatorPid ! connected,
    {ok, State}.

on_connect_error(Reason, State) ->
    lager:error("connection failed due to ~p", [Reason]),
    {ok, State}.

on_disconnect(State) ->
    {ok, State}.

on_subscribe(Topics, {coord, CoordPid} = State) ->
    FailedTopics = [
        {Topic, ResponseQoS}
     || {Topic, ResponseQoS} <- Topics, ResponseQoS == not_allowed
    ],
    case FailedTopics of
        [] ->
            lager:info("Bridge Pid ~p is subscribing to Topics: ~p~n", [CoordPid, Topics]);
        _ ->
            lager:warning(
                "Bridge Pid ~p had subscription failure codes in SUBACK for topics ~p~n", [
                    CoordPid, FailedTopics
                ]
            )
    end,
    {ok, State}.

on_unsubscribe(_Topics, State) ->
    {ok, State}.

on_publish(Topic, Payload, Opts, {coord, CoordinatorPid} = State) ->
    CoordinatorPid ! {deliver_remote, Topic, Payload, Opts},
    {ok, State}.

init([Type, Host, Port, RegistryMFA, Opts]) ->
    {M, F, [A, OptMap]} = RegistryMFA,
    {ok, #{
        publish_fun := PublishFun,
        register_fun := RegisterFun,
        subscribe_fun := SubscribeFun,
        unsubscribe_fun := UnsubscribeFun
    }} =
        apply(M, F, [A, OptMap]),
    true = is_function(RegisterFun, 0),
    true = is_function(PublishFun, 3),
    true = is_function(SubscribeFun, 1),
    true = is_function(UnsubscribeFun, 1),
    Name = proplists:get_value(name, Opts),
    ok = RegisterFun(),
    self() ! init_client,
    {ok, #state{
        name = Name,
        type = Type,
        host = Host,
        port = Port,
        opts = Opts,
        publish_fun = PublishFun,
        subscribe_fun = SubscribeFun,
        unsubscribe_fun = UnsubscribeFun
    }};
init([{coord, _CoordinatorPid} = State]) ->
    {ok, State}.

handle_call(info, _From, #state{client_pid = Pid} = State) ->
    {ResponseType, Info} =
        case Pid of
            undefined ->
                {error, not_started};
            Pid ->
                gen_mqtt_client:info(Pid)
        end,
    {reply, {ResponseType, Info}, State};
handle_call(get_metrics, _From, #state{counters_ref = CR, client_pid = Pid} = State) ->
    {ResponseType, Info} =
        case Pid of
            undefined ->
                {error, not_started};
            Pid ->
                gen_mqtt_client:metrics(Pid, CR)
        end,
    {reply, {ResponseType, Info}, State};
handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({coord, CoordinatorPid, stop}, {coord, CoordinatorPid} = State) ->
    {stop, normal, State};
handle_cast(_Req, State) ->
    {noreply, State}.

handle_info(
    init_client,
    #state{
        type = Type,
        host = Host,
        port = Port,
        opts = Opts,
        client_pid = undefined,
        subscribe_fun = SubscribeFun
    } = State
) ->
    CountersRef = counters:new(6, [atomics]),
    {ok, Pid} = start_client(Type, Host, Port, [{counters_ref, CountersRef} | Opts]),
    Topics = proplists:get_value(topics, Opts),
    Subscriptions = bridge_subscribe(local, Pid, Topics, SubscribeFun, []),
    {noreply, State#state{
        client_pid = Pid,
        subs_local = Subscriptions,
        counters_ref = CountersRef
    }};
handle_info(
    connected,
    #state{
        name = Name,
        host = Host,
        port = Port,
        client_pid = Pid,
        opts = Opts,
        subscribe_fun = SubscribeFun
    } = State
) ->
    lager:info("Bridge ~s connected to ~s:~p.~n", [Name, Host, Port]),
    Topics = proplists:get_value(topics, Opts),
    Subscriptions = bridge_subscribe(remote, Pid, Topics, SubscribeFun, []),
    {noreply, State#state{subs_remote = Subscriptions}};
handle_info(
    {deliver_remote, Topic, Payload, #{qos := QoS, retain := Retain}},
    #state{publish_fun = PublishFun, subs_remote = Subscriptions} = State
) ->
    %% publish an incoming message from the remote broker locally if
    %% we have a matching subscription
    lists:foreach(
        fun
            ({{in, T}, {LocalPrefix, RemotePrefix}}) ->
                case match(Topic, T) of
                    true ->
                        % ignore if we're ready or not.
                        PublishFun(swap_prefix(RemotePrefix, LocalPrefix, Topic), Payload, #{
                            qos => QoS, retain => Retain
                        });
                    false ->
                        ok
                end;
            (_) ->
                ok
        end,
        Subscriptions
    ),
    {noreply, State};
handle_info(
    {deliver, Topic, Payload, _QoS, IsRetained, _IsDup},
    #state{subs_local = Subscriptions, client_pid = ClientPid} = State
) ->
    %% forward matching, locally published messages to the remote broker.
    lists:foreach(
        fun
            ({{out, T}, QoS, {LocalPrefix, RemotePrefix}}) ->
                case match(Topic, T) of
                    true ->
                        ok = gen_mqtt_client:publish(
                            ClientPid,
                            swap_prefix(LocalPrefix, RemotePrefix, Topic),
                            Payload,
                            QoS,
                            IsRetained
                        );
                    false ->
                        ok
                end;
            (_) ->
                ok
        end,
        Subscriptions
    ),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

bridge_subscribe(
    remote = Type,
    Pid,
    [{Topic, in, QoS, LocalPrefix, RemotePrefix} = BT | Rest],
    SubscribeFun,
    Acc
) ->
    case vmq_topic:validate_topic(subscribe, list_to_binary(Topic)) of
        {ok, TTopic} ->
            RemoteTopic = add_prefix(validate_prefix(RemotePrefix), TTopic),
            gen_mqtt_client:subscribe(Pid, RemoteTopic, QoS),
            bridge_subscribe(
                Type,
                Pid,
                Rest,
                SubscribeFun,
                [
                    {{in, RemoteTopic}, {
                        validate_prefix(LocalPrefix), validate_prefix(RemotePrefix)
                    }}
                    | Acc
                ]
            );
        {error, Reason} ->
            error_logger:warning_msg(
                "can't validate bridge topic conf ~p due to ~p",
                [BT, Reason]
            ),
            bridge_subscribe(Type, Pid, Rest, SubscribeFun, Acc)
    end;
bridge_subscribe(
    local = Type,
    Pid,
    [{Topic, out, QoS, LocalPrefix, RemotePrefix} = BT | Rest],
    SubscribeFun,
    Acc
) ->
    case vmq_topic:validate_topic(subscribe, list_to_binary(Topic)) of
        {ok, TTopic} ->
            LocalTopic = add_prefix(validate_prefix(LocalPrefix), TTopic),
            {ok, _} = SubscribeFun(LocalTopic),
            bridge_subscribe(
                Type,
                Pid,
                Rest,
                SubscribeFun,
                [
                    {{out, LocalTopic}, QoS, {
                        validate_prefix(LocalPrefix), validate_prefix(RemotePrefix)
                    }}
                    | Acc
                ]
            );
        {error, Reason} ->
            error_logger:warning_msg(
                "can't validate bridge topic conf ~p due to ~p",
                [BT, Reason]
            ),
            bridge_subscribe(Type, Pid, Rest, SubscribeFun, Acc)
    end;
bridge_subscribe(
    Type,
    Pid,
    [{Topic, both, QoS, LocalPrefix, RemotePrefix} = BT | Rest],
    SubscribeFun,
    Acc
) ->
    case vmq_topic:validate_topic(subscribe, list_to_binary(Topic)) of
        {ok, TTopic} ->
            case Type of
                remote ->
                    RemoteTopic = add_prefix(validate_prefix(RemotePrefix), TTopic),
                    gen_mqtt_client:subscribe(Pid, RemoteTopic, QoS),
                    bridge_subscribe(
                        Type,
                        Pid,
                        Rest,
                        SubscribeFun,
                        [
                            {{in, RemoteTopic}, {
                                validate_prefix(LocalPrefix), validate_prefix(RemotePrefix)
                            }}
                            | Acc
                        ]
                    );
                local ->
                    LocalTopic = add_prefix(validate_prefix(LocalPrefix), TTopic),
                    {ok, _} = SubscribeFun(LocalTopic),
                    bridge_subscribe(
                        Type,
                        Pid,
                        Rest,
                        SubscribeFun,
                        [
                            {{out, LocalTopic}, QoS, {
                                validate_prefix(LocalPrefix), validate_prefix(RemotePrefix)
                            }}
                            | Acc
                        ]
                    )
            end;
        {error, Reason} ->
            error_logger:warning_msg(
                "can't validate bridge topic conf ~p due to ~p",
                [BT, Reason]
            ),
            bridge_subscribe(Type, Pid, Rest, SubscribeFun, Acc)
    end;
bridge_subscribe(Type, Pid, [_ | Rest], SubscribeFun, Acc) ->
    bridge_subscribe(Type, Pid, Rest, SubscribeFun, Acc);
bridge_subscribe(_, _, [], _, Acc) ->
    Acc.

start_client(Type, Host, Port, Opts) ->
    CR = proplists:get_value(counters_ref, Opts),
    ClientOpts = client_opts(Type, Host, Port, Opts),
    {ok, Pid} = gen_mqtt_client:start_link(?MODULE, [{coord, self()}], [
        {info_fun, {fun stats/2, CR}} | ClientOpts
    ]),
    ets:insert(vmq_bridge_meta, {Pid}),
    {ok, Pid}.

validate_prefix(undefined) ->
    undefined;
validate_prefix([W | _] = Prefix) when is_binary(W) -> Prefix;
validate_prefix(Prefix) when is_list(Prefix) ->
    validate_prefix(list_to_binary(Prefix));
validate_prefix(Prefix) ->
    case vmq_topic:validate_topic(publish, Prefix) of
        {error, no_empty_topic_allowed} ->
            undefined;
        {ok, ParsedPrefix} ->
            ParsedPrefix
    end.

add_prefix(undefined, Topic) -> Topic;
add_prefix(Prefix, Topic) -> lists:flatten([Prefix, Topic]).

remove_prefix(undefined, Topic) -> Topic;
remove_prefix([], Topic) -> Topic;
remove_prefix([H | Prefix], [H | Topic]) -> remove_prefix(Prefix, Topic);
remove_prefix([Prefix], Topic) -> remove_prefix(Prefix, Topic);
remove_prefix(Prefix, [Prefix | Rest]) -> Rest.

swap_prefix(OldPrefix, NewPrefix, Topic) ->
    add_prefix(NewPrefix, remove_prefix(OldPrefix, Topic)).

client_opts(tcp, Host, Port, Opts) ->
    OOpts =
        [
            {host, Host},
            {port, Port},
            {username, proplists:get_value(username, Opts)},
            {password, proplists:get_value(password, Opts)},
            {client, proplists:get_value(client_id, Opts)},
            {clean_session, proplists:get_value(cleansession, Opts, false)},
            {keepalive_interval, proplists:get_value(keepalive_interval, Opts)},
            {reconnect_timeout, proplists:get_value(restart_timeout, Opts)},
            {retry_interval, proplists:get_value(retry_interval, Opts)},
            {max_queue_size, proplists:get_value(max_outgoing_buffered_messages, Opts)},
            {transport, {gen_tcp, []}}
            | case
                {
                    proplists:get_value(try_private, Opts, true),
                    proplists:get_value(mqtt_version, Opts, 3)
                }
            of
                {true, 3} ->
                    %% non-spec
                    [{proto_version, 131}];
                {true, 4} ->
                    %% non-spec
                    [{proto_version, 132}];
                {false, MqttVersion} ->
                    [{proto_version, MqttVersion}]
            end
        ],
    [P || {_, V} = P <- OOpts, V /= undefined];
client_opts(ssl, Host, Port, Opts) ->
    TCPOpts = client_opts(tcp, Host, Port, Opts),
    SSLOpts = [
        {certfile, proplists:get_value(certfile, Opts)},
        {cacertfile, proplists:get_value(cafile, Opts)},
        {keyfile, proplists:get_value(keyfile, Opts)},
        {depth, proplists:get_value(depth, Opts)},
        {verify,
            case proplists:get_value(insecure, Opts) of
                true -> verify_none;
                _ -> verify_peer
            end},
        {versions,
            case proplists:get_value(tls_version, Opts) of
                undefined -> undefined;
                V -> [V]
            end},
        {psk_identity, proplists:get_value(identity, Opts)},
        {user_lookup_fun,
            case {proplists:get_value(identity, Opts), proplists:get_value(psk, Opts)} of
                {Identity, Psk} when
                    is_list(Identity) and is_list(Psk)
                ->
                    BinPsk = to_bin(Psk),
                    {
                        fun
                            (psk, I, _) when I == Identity ->
                                {ok, BinPsk};
                            (_, _, _) ->
                                error
                        end,
                        []
                    };
                _ ->
                    undefined
            end}
    ],
    SSLOpts1 =
        case proplists:get_value(customize_hostname_check, Opts) of
            'https' ->
                [
                    {customize_hostname_check, [
                        {match_fun, public_key:pkix_verify_hostname_match_fun(https)}
                    ]}
                    | SSLOpts
                ];
            _ ->
                SSLOpts
        end,
    SSLOpts2 =
        case proplists:get_value(sni, Opts) of
            undefined -> SSLOpts1;
            SNI -> [{server_name_indication, SNI} | SSLOpts1]
        end,

    lists:keyreplace(
        transport,
        1,
        TCPOpts,
        {transport, {ssl, [P || {_, V} = P <- SSLOpts2, V /= undefined]}}
    ).

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

match(T1, [<<"$share">>, _Grp | T2]) ->
    vmq_topic:match(T1, T2);
match(T1, T2) ->
    vmq_topic:match(T1, T2).

%% ------------------------------------------------
%% Gen_MQTT Info Callbacks
%% ------------------------------------------------

stats({publish_out, _MsgId, QoS}, CR) ->
    case QoS of
        % vmq_bridge_publish_out_0)
        0 -> counters:add(CR, 1, 1);
        % vmq_bridge_publish_out_1);
        1 -> counters:add(CR, 2, 1);
        %vmq_bridge_publish_out_2)
        2 -> counters:add(CR, 3, 1)
    end,
    CR;
stats({publish_in, _MsgId, _Payload, QoS}, CR) ->
    case QoS of
        % vmq_bridge_publish_in_0);
        0 -> counters:add(CR, 4, 1);
        % vmq_bridge_publish_in_1);
        1 -> counters:add(CR, 5, 1);
        % vmq_bridge_publish__in_2)
        2 -> counters:add(CR, 6, 1)
    end,
    CR;
% Stubs for more info functions

% log connection attempt
stats({connect_out, _ClientId}, State) ->
    State;
stats({connack_in, _ClientId}, State) ->
    State;
stats({reconnect, _ClientId}, State) ->
    State;
stats({puback_in, _MsgId}, State) ->
    State;
stats({puback_out, _MsgId}, State) ->
    State;
stats({suback, _MsgId}, State) ->
    State;
stats({subscribe_out, _MsgId}, State) ->
    State;
stats({unsubscribe_out, _MsgId}, State) ->
    State;
stats({unsuback, _MsgId}, State) ->
    State;
stats({pubrec_in, _MsgId}, State) ->
    State;
stats({pubrec_out, _MsgId}, State) ->
    State;
stats({pubrel_out, _MsgId}, State) ->
    State;
stats({pubrel_in, _MsgId}, State) ->
    State;
stats({pubcomp_in, _MsgId}, State) ->
    State;
stats({pubcomp_out, _MsgId}, State) ->
    State.
