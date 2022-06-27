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
%%%-------------------------------------------------------------------

-module(vmq_diversity_cache).
-include_lib("luerl/include/luerl.hrl").

-dialyzer(no_undefined_callbacks).
-behaviour(gen_server2).

%% API
-export([
    start_link/0,
    install/1,
    match_publish_acl/6,
    match_subscribe_acl/4,
    clear_cache/0,
    clear_cache/2,
    entries/2
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER, ?MODULE).
-define(MAX_PAYLOAD_SIZE, 268435456).

-import(luerl_lib, [badarg_error/3]).

-record(state, {
    %% logical timestamp
    lts = 0 :: non_neg_integer()
}).

-record(publish_acl, {
    pattern,
    max_qos = 2,
    max_payload_size = ?MAX_PAYLOAD_SIZE,
    % use 0 | 1 instead of boolean for comparison
    allowed_retain = bit(true),
    modifiers
}).
-record(subscribe_acl, {
    pattern,
    max_qos = 2,
    modifiers
}).

-define(ms(Key),
    %% Key = {<<>>, <<"clientid">>},
    %% ets:fun2ms(fun({{AKey, '_'}, '_', '_'}=A) when AKey =:= Key -> A end).
    [{{{'$1', '_'}, '_', '_'}, [{'=:=', '$1', {const, Key}}], ['$_']}]
).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [], []).

install(St) ->
    luerl_emul:alloc_table(table(), St).

-spec match_publish_acl(binary(), binary(), 0 | 1 | 2, [binary()], binary(), boolean()) ->
    true | [{atom(), any()}] | false | no_cache.
match_publish_acl(MP, ClientId, QoS, Topic, Payload, IsRetain) ->
    match_acl(
        MP,
        ClientId,
        #publish_acl{
            pattern = Topic,
            max_qos = QoS,
            max_payload_size = byte_size(Payload),
            allowed_retain = bit(IsRetain)
        }
    ).

-spec match_subscribe_acl(binary(), binary(), [binary()], 0 | 1 | 2) ->
    true | [{[binary()], 0 | 1 | 2}] | false | no_cache.
match_subscribe_acl(MP, ClientId, Topic, QoS) ->
    match_acl(MP, ClientId, #subscribe_acl{pattern = Topic, max_qos = QoS}).

match_acl(MP, ClientId, Input) ->
    Key = key(MP, ClientId),
    match(Key, Input).

clear_cache() ->
    _ = [ets:delete_all_objects(table(T)) || T <- [cache, publish, subscribe]],
    ok.

clear_cache(MP, ClientId) ->
    Key = key(MP, ClientId),
    case ets:select(table(cache), ?ms(Key), 1) of
        '$end_of_table' ->
            ok;
        {[{{Key, Lts}, PubAclHashes, SubAclHashes} | _], _} ->
            %% first entry is the oldest which we'll delete
            gen_server2:call(?MODULE, {delete_cache, {Key, Lts}, PubAclHashes, SubAclHashes})
    end.

entries(MP, ClientId) when is_binary(MP) ->
    Key = key(MP, ClientId),
    case ets:select(table(cache), ?ms(Key)) of
        [] ->
            [];
        [{_, PubAclHashes, SubAclHashes}] ->
            [
                {publish, entries_(publish, PubAclHashes)},
                {subscribe, entries_(subscribe, SubAclHashes)}
            ]
    end.

entries_(Type, Hashes) ->
    lists:foldl(
        fun(H, Acc) ->
            [{_, Acl, _}] = ets:lookup(table(Type), H),
            [Acl | Acc]
        end,
        [],
        Hashes
    ).

%%%===================================================================
%%% Luerl specific
%%%===================================================================
table() ->
    [
        {<<"insert">>, #erl_func{code = fun insert/2}},
        %% only for testing purposes
        {<<"match_subscribe">>, #erl_func{code = fun match_subscribe/2}},
        {<<"match_publish">>, #erl_func{code = fun match_publish/2}}
    ].

decode_acl(Acl, St) when is_tuple(Acl) ->
    luerl:decode(Acl, St);
decode_acl(_, _) ->
    undefined.

insert(As, St) ->
    case As of
        [MP, ClientId, User, PubAcls, SubAcls] when
            is_binary(MP) and
                is_binary(ClientId) and
                is_binary(User)
        ->
            VPubAcls = validate_acls(
                MP, User, ClientId, #publish_acl{}, decode_acl(PubAcls, St), []
            ),
            VSubAcls = validate_acls(
                MP, User, ClientId, #subscribe_acl{}, decode_acl(SubAcls, St), []
            ),
            Key = key(MP, ClientId),
            gen_server2:call(
                ?MODULE,
                {insert_cache, Key, VPubAcls, VSubAcls}
            ),
            {[true], St};
        _ ->
            badarg_error(execute_parse, As, St)
    end.

match_subscribe(As, St) ->
    case As of
        [MP, ClientId, Topic, QoS] when
            is_binary(MP) and
                is_binary(ClientId) and
                is_binary(Topic) and
                is_number(QoS)
        ->
            case vmq_topic:validate_topic(subscribe, Topic) of
                {ok, Words} ->
                    case match_subscribe_acl(MP, ClientId, Words, trunc(QoS)) of
                        true ->
                            {[true], St};
                        Modifiers0 when is_list(Modifiers0) ->
                            %% the subscribe-modifiers have the form:
                            %% [{Topic :: list(binary), QoS :: 0 | 1 | 2}, ...]
                            %% and have to be transposed to
                            %% [[Topic :: binary, QoS], ...]
                            Modifiers1 =
                                [
                                    begin
                                        [iolist_to_binary(vmq_topic:unword(ModT)), ModQ]
                                    end
                                 || {ModT, ModQ} <- Modifiers0
                                ],
                            {Modifiers2, NewSt} = luerl:encode(Modifiers1, St),
                            {[Modifiers2], NewSt};
                        _ ->
                            {[false], St}
                    end;
                _ ->
                    badarg_error(execute_parse, As, St)
            end;
        _ ->
            badarg_error(execute_parse, As, St)
    end.

match_publish(As, St) ->
    case As of
        [MP, ClientId, Topic, QoS, Payload, IsRetain] when
            is_binary(MP) and
                is_binary(ClientId) and
                is_number(QoS) and
                is_binary(Topic) and
                is_binary(Payload) and
                is_boolean(IsRetain)
        ->
            case vmq_topic:validate_topic(publish, Topic) of
                {ok, Words} ->
                    case match_publish_acl(MP, ClientId, trunc(QoS), Words, Payload, IsRetain) of
                        true ->
                            {[true], St};
                        Modifiers0 when is_list(Modifiers0) ->
                            Modifiers1 =
                                case lists:keyfind(topic, 1, Modifiers0) of
                                    false ->
                                        Modifiers0;
                                    {_, ModT} ->
                                        lists:keyreplace(
                                            topic,
                                            1,
                                            Modifiers0,
                                            {topic, iolist_to_binary(vmq_topic:unword(ModT))}
                                        )
                                end,
                            Modifiers2 =
                                case lists:keyfind(mountpoint, 1, Modifiers1) of
                                    false ->
                                        Modifiers1;
                                    {_, ModMP} ->
                                        lists:keyreplace(
                                            mountpoint,
                                            1,
                                            Modifiers1,
                                            {mountpoint, list_to_binary(ModMP)}
                                        )
                                end,
                            {Modifiers3, NewSt} = luerl:encode(Modifiers2, St),
                            {[Modifiers3], NewSt};
                        _ ->
                            {[false], St}
                    end;
                _ ->
                    badarg_error(execute_parse, As, St)
            end;
        _ ->
            badarg_error(execute_parse, As, St)
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([]) ->
    _ = [
        ets:new(table(T), [
            public,
            named_table,
            {read_concurrency, true},
            {write_concurrency, true}
        ])
     || T <- [publish, subscribe]
    ],
    _ = ets:new(table(cache), [
        public,
        named_table,
        {read_concurrency, true},
        {write_concurrency, true},
        ordered_set
    ]),
    {ok, #state{}}.

handle_call({insert_cache, Key, PubAcls, SubAcls}, _From, #state{lts = Lts} = State) ->
    insert_cache(Key, Lts + 1, PubAcls, SubAcls),
    {reply, ok, State#state{lts = Lts + 1}};
handle_call({delete_cache, {Key, Lts}, PubAclHashes, SubAclHashes}, _From, State) ->
    delete_cache_(table(publish), PubAclHashes),
    delete_cache_(table(subscribe), SubAclHashes),
    ets:delete(table(cache), {Key, Lts}),
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
validate_acls(_, _, _, _, undefined, Acc) ->
    Acc;
validate_acls(MP, User, ClientId, AclRec, [{_, Acl} | Rest], Acc) when
    is_list(Acl)
->
    validate_acls(
        MP,
        User,
        ClientId,
        AclRec,
        Rest,
        [
            validate_acl(
                MP,
                User,
                ClientId,
                AclRec,
                Acl
            )
            | Acc
        ]
    );
validate_acls(_, _, _, _, [], Acc) ->
    Acc.

validate_acl(MP, User, ClientId, Rec0, [{<<"max_qos">>, MaxQoS} | Rest]) when
    is_number(MaxQoS) and (MaxQoS >= 0) and (MaxQoS =< 2)
->
    Type = type(Rec0),
    Rec1 =
        case Type of
            publish ->
                Rec0#publish_acl{max_qos = trunc(MaxQoS)};
            subscribe ->
                Rec0#subscribe_acl{max_qos = trunc(MaxQoS)}
        end,
    validate_acl(MP, User, ClientId, Rec1, Rest);
validate_acl(MP, User, ClientId, Rec0, [{<<"modifiers">>, Modifiers} | Rest]) when
    is_list(Modifiers)
->
    Rec1 =
        case type(Rec0) of
            publish ->
                Rec0#publish_acl{
                    modifiers = validate_modifiers(publish, Modifiers)
                };
            subscribe ->
                Rec0#subscribe_acl{
                    modifiers = validate_modifiers(subscribe, Modifiers)
                }
        end,
    validate_acl(MP, User, ClientId, Rec1, Rest);
validate_acl(MP, User, ClientId, #publish_acl{} = Rec0, [{<<"max_payload_size">>, MaxSize} | Rest]) when
    is_number(MaxSize) and (MaxSize >= 0) and (MaxSize =< ?MAX_PAYLOAD_SIZE)
->
    Rec1 = Rec0#publish_acl{max_payload_size = trunc(MaxSize)},
    validate_acl(MP, User, ClientId, Rec1, Rest);
validate_acl(MP, User, ClientId, #publish_acl{} = Rec0, [
    {<<"allowed_retain">>, AllowedRetain} | Rest
]) when
    is_boolean(AllowedRetain)
->
    Rec1 = Rec0#publish_acl{
        allowed_retain = bit(AllowedRetain)
    },
    validate_acl(MP, User, ClientId, Rec1, Rest);
validate_acl(MP, User, ClientId, Rec0, [{<<"pattern">>, Pattern} | Rest]) when is_binary(Pattern) ->
    Type = type(Rec0),
    %% we use validate_topic(subscribe... because this would allow that
    %% an ACL contains wildcards
    Rec1 =
        case vmq_topic:validate_topic(subscribe, Pattern) of
            {ok, Words} when Type == publish ->
                Rec0#publish_acl{pattern = subst(MP, User, ClientId, Words, [])};
            {ok, Words} when Type == subscribe ->
                Rec0#subscribe_acl{pattern = subst(MP, User, ClientId, Words, [])};
            {error, Reason} ->
                lager:error(
                    "can't validate ACL topic ~p for client ~p due to ~p",
                    [Pattern, ClientId, Reason]
                ),
                Rec0
        end,
    validate_acl(MP, User, ClientId, Rec1, Rest);
validate_acl(MP, User, ClientId, Rec, [UnknownProp | Rest]) ->
    lager:warning("unknown property ~p for ACL ~p", [UnknownProp, Rec]),
    validate_acl(MP, User, ClientId, Rec, Rest);
validate_acl(_, _, _, Rec, []) ->
    Rec.

validate_modifiers(Type, Mods0) ->
    Mods1 = vmq_diversity_utils:convert(Mods0),
    Ret =
        case Type of
            publish ->
                vmq_plugin_util:check_modifiers(auth_on_publish, Mods1);
            subscribe ->
                %% massage the modifiers to take the same form as it were returned by
                %% the callback directly
                %% in Lua: { {topic, qos}, ... }
                Mods2 = vmq_diversity_utils:normalize_subscribe_topics(Mods1),
                vmq_plugin_util:check_modifiers(auth_on_subscribe, Mods2)
        end,
    case Ret of
        error ->
            lager:error("can't validate modifiers ~p for ~p ACL", [Type, Mods0]),
            undefined;
        _ ->
            Ret
    end.

subst(MP, User, ClientId, [<<"%u">> | Rest], Acc) ->
    subst(MP, User, ClientId, Rest, [User | Acc]);
subst(MP, User, ClientId, [<<"%c">> | Rest], Acc) ->
    subst(MP, User, ClientId, Rest, [ClientId | Acc]);
subst(MP, User, ClientId, [<<"%m">> | Rest], Acc) ->
    subst(MP, User, ClientId, Rest, [MP | Acc]);
subst(MP, User, ClientId, [W | Rest], Acc) ->
    subst(MP, User, ClientId, Rest, [W | Acc]);
subst(_MP, _User, _ClientId, [], Acc) ->
    lists:reverse(Acc).

key(MP, ClientId) when
    is_binary(MP) and
        is_binary(ClientId)
->
    {MP, ClientId}.

match(Key, Input) ->
    Type = type(Input),
    %% in rare cases we may have more elements, then always use the
    %% most recently added acl rules (the last).
    case ets:select_reverse(table(cache), ?ms(Key), 1) of
        '$end_of_table' ->
            no_cache;
        {[{_, PubAclHashes, _}], _} when Type == publish ->
            match_(table(Type), Input, PubAclHashes);
        {[{_, _, SubAclHashes}], _} when Type == subscribe ->
            match_(table(Type), Input, SubAclHashes)
    end.

match_(Table, Input, [AclHash | Rest]) ->
    case ets:lookup(Table, AclHash) of
        [{_, Acl, _Counter}] ->
            case match_input_with_acl(Input, Acl) of
                false ->
                    match_(Table, Input, Rest);
                Ret ->
                    Ret
            end;
        _ ->
            match_(Table, Input, Rest)
    end;
match_(_, _, []) ->
    false.

match_input_with_acl(
    #publish_acl{
        pattern = InputTopic,
        max_qos = InputQoS,
        max_payload_size = InputPayloadSize,
        allowed_retain = InputRetain
    },
    #publish_acl{
        pattern = AclTopic,
        max_qos = MaxQoS,
        max_payload_size = MaxPayloadSize,
        allowed_retain = AllowedRetain,
        modifiers = Modifiers
    }
) when
    (InputQoS =< MaxQoS) and
        (InputPayloadSize =< MaxPayloadSize) and
        (InputRetain =< AllowedRetain)
->
    case vmq_topic:match(InputTopic, AclTopic) of
        true when Modifiers =/= undefined ->
            Modifiers;
        true ->
            true;
        false ->
            false
    end;
match_input_with_acl(
    #subscribe_acl{pattern = InputTopic, max_qos = InputQoS},
    #subscribe_acl{
        pattern = AclTopic,
        max_qos = MaxQoS,
        modifiers = Modifiers
    }
) when
    (InputQoS =< MaxQoS)
->
    case vmq_topic:match(InputTopic, AclTopic) of
        true when Modifiers =/= undefined ->
            Modifiers;
        true ->
            true;
        false ->
            false
    end;
match_input_with_acl(_, _) ->
    false.

insert_cache(Key, Lts, PubAcls, SubAcls) ->
    PubAclHashes = insert_cache_(table(publish), PubAcls, []),
    SubAclHashes = insert_cache_(table(subscribe), SubAcls, []),
    ets:insert(table(cache), {{Key, Lts}, PubAclHashes, SubAclHashes}).

insert_cache_(Table, [Rec | Rest], Acc) ->
    AclHash = crypto:hash(sha, term_to_binary(Rec)),
    ets:update_counter(
        Table,
        AclHash,
        % Update Op
        {3, 1},
        {AclHash, Rec, 0}
    ),
    insert_cache_(Table, Rest, [AclHash | Acc]);
insert_cache_(_, [], Acc) ->
    Acc.

delete_cache_(Table, [AclHash | Rest]) ->
    case
        ets:update_counter(
            Table,
            AclHash,
            % Update Op
            {3, -1},
            {AclHash, ignored, 0}
        )
    of
        R when R =< 0 ->
            ets:delete(Table, AclHash);
        _ ->
            ignore
    end,
    delete_cache_(Table, Rest);
delete_cache_(_, []) ->
    ok.

table(cache) -> vmq_diversity_cache;
table(publish) -> vmq_diversity_pub_cache;
table(subscribe) -> vmq_diversity_sub_cache.

type(#publish_acl{}) -> publish;
type(#subscribe_acl{}) -> subscribe.

bit(true) -> 1;
bit(false) -> 0;
bit(B) when is_integer(B) -> B.
