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

-module(vmq_enhanced_auth).
-behaviour(auth_on_register_hook).
-behaviour(auth_on_subscribe_hook).
-behaviour(auth_on_publish_hook).
-behaviour(auth_on_subscribe_m5_hook).
-behaviour(auth_on_publish_m5_hook).
-behaviour(on_config_change_hook).

-export([
    start/0,
    stop/0,
    init/0,
    load_from_file/1,
    load_from_list/1,
    check/4,
    set_acl_version_metrics/1
]).

-export([
    auth_on_subscribe/3,
    auth_on_publish/6,
    auth_on_subscribe_m5/4,
    auth_on_publish_m5/7,
    auth_on_register/5,
    change_config/1
]).

-import(vmq_topic, [words/1, match/2]).

-include_lib("vmq_server/src/vmq_server.hrl").

-define(TABLES, [
    vmq_enhanced_auth_acl_read_pattern,
    vmq_enhanced_auth_acl_write_pattern,
    vmq_enhanced_auth_acl_read_all,
    vmq_enhanced_auth_acl_write_all,
    vmq_enhanced_auth_acl_read_user,
    vmq_enhanced_auth_acl_write_user,
    vmq_enhanced_auth_acl_read_token,
    vmq_enhanced_auth_acl_write_token
]).
-define(ARGS_EXTRACT_REGEX, "\\(\s*(u|c)\s*,\s*(:|-|\\|)\s*,\s*([0-9]+)\s*\\)").
-define(REGEX_TABLE, vmq_enhanced_auth_regex_table).
-define(TABLE_OPTS, [public, named_table, {read_concurrency, true}]).
-define(USER_SUP, <<"%u">>).
-define(CLIENT_SUP, <<"%c">>).
-define(MOUNTPOINT_SUP, <<"%m">>).
-define(TOPIC_LABEL_TABLE, topic_labels).

-include_lib("vmq_commons/include/vmq_types.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(setup(F), {setup, fun setup/0, fun teardown/1, F}).
-endif.

-define(SecretKey, application:get_env(vmq_enhanced_auth, secret_key, undefined)).
-define(EnableAuthOnRegister, application:get_env(vmq_enhanced_auth, enable_jwt_auth, false)).
-define(EnableAclHooks, application:get_env(vmq_enhanced_auth, enable_acl_hooks, false)).
-define(RegView, application:get_env(vmq_server, default_reg_view, vmq_reg_trie)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Plugin Callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start() ->
    {ok, _} = application:ensure_all_started(vmq_enhanced_auth),
    vmq_enhanced_auth_cli:register(),
    ok.

stop() ->
    application:stop(vmq_enhanced_auth).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
change_config(Configs) ->
    case lists:keyfind(vmq_enhanced_auth, 1, Configs) of
        false ->
            ok;
        _ ->
            vmq_enhanced_auth_reloader:change_config_now()
    end.

auth_on_subscribe(User, SubscriberId, TopicList) ->
    auth_on_subscribe(User, SubscriberId, TopicList, []).

auth_on_subscribe(_, _, [], Modifiers) ->
    {ok, lists:reverse(Modifiers)};
auth_on_subscribe(User, SubscriberId, TopicList, Modifiers) ->
    auth_on_subscribe(?RegView, User, SubscriberId, TopicList, Modifiers).

auth_on_subscribe(RegView, User, SubscriberId, [{Topic, Qos} | Rest], Modifiers) ->
    D = is_topic_invalid(RegView, Topic) orelse is_acl_auth_disabled(),
    if
        D ->
            next;
        true ->
            case check(read, Topic, User, SubscriberId) of
                {true, MatchedAcl} ->
                    auth_on_subscribe(
                        User,
                        SubscriberId,
                        Rest,
                        [{Topic, Qos, MatchedAcl} | Modifiers]
                    );
                false ->
                    ModTopic = {Topic, not_allowed, #matched_acl{}},
                    auth_on_subscribe(
                        User,
                        SubscriberId,
                        Rest,
                        [ModTopic | Modifiers]
                    )
            end
    end.

auth_on_publish(User, SubscriberId, _Qos, Topic, _, _) ->
    D = is_acl_auth_disabled(),
    if
        D ->
            next;
        true ->
            case check(write, Topic, User, SubscriberId) of
                {true, MatchedAcl} ->
                    {ok, [{matched_acl, MatchedAcl}]};
                false ->
                    next
            end
    end.

auth_on_subscribe_m5(_, _, []) ->
    ok;
auth_on_subscribe_m5(User, SubscriberId, [{Topic, _Qos} | Rest]) ->
    case check(read, Topic, User, SubscriberId) of
        {true, _} ->
            auth_on_subscribe(User, SubscriberId, Rest);
        false ->
            next
    end.
auth_on_subscribe_m5(User, SubscriberId, Topics, _Props) ->
    auth_on_subscribe_m5(User, SubscriberId, Topics).

auth_on_publish_m5(User, SubscriberId, QoS, Topic, Payload, IsRetain, _Props) ->
    auth_on_publish(User, SubscriberId, QoS, Topic, Payload, IsRetain).

auth_on_register(
    {_IpAddr, _Port} = _Peer,
    {_MountPoint, _ClientId} = _SubscriberId,
    UserName,
    Password,
    _CleanSession
) ->
    %% do whatever you like with the params, all that matters
    %% is the return value of this function
    %%
    %% 1. return 'ok' -> CONNECT is authenticated
    %% 2. return 'next' -> leave it to other plugins to decide
    %% 3. return {ok, [{ModifierKey, NewVal}...]} -> CONNECT is authenticated, but we might want to set some options used throughout the client session:
    %%      - {mountpoint, NewMountPoint::string}
    %%      - {clean_session, NewCleanSession::boolean}
    %% 4. return {error, invalid_credentials} -> CONNACK_CREDENTIALS is sent
    %% 5. return {error, whatever} -> CONNACK_AUTH is sent

    %% we return 'ok'
    D = is_auth_on_register_disabled(),
    if
        D ->
            next;
        true ->
            {Result, Claims} = verify(Password, ?SecretKey),
            if
                Result =:= ok -> check_rid(Claims, UserName);
                %else block
                true -> {error, invalid_signature}
            end
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Internal+
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init() ->
    insert_regex(),
    lists:foreach(
        fun(T) ->
            case lists:member(T, ets:all()) of
                true -> ok;
                false -> ets:new(T, ?TABLE_OPTS)
            end
        end,
        ?TABLES
    ).

set_acl_version_metrics(File) ->
    case vmq_util:extract_version(File) of
        Version when is_list(Version) ->
            vmq_metrics:update_config_version_metric(acl_version, Version);
        nomatch ->
            vmq_metrics:update_config_version_metric(acl_version, "N/A");
        {error, Reason} ->
            error_logger:error_msg("can't load acl file ~p due to ~p", [File, Reason]),
            ok
    end.

load_from_file(File) ->
    case file:open(File, [read, binary]) of
        {ok, Fd} ->
            F = fun
                (FF, read) -> {FF, rl(Fd)};
                (_, close) -> file:close(Fd)
            end,
            age_entries(),
            parse_acl_line(F(F, read), all),
            del_aged_entries();
        {error, Reason} ->
            error_logger:error_msg("can't load acl file ~p due to ~p", [File, Reason]),
            ok
    end.

load_from_list(List) ->
    put(vmq_acl_list, List),
    F = fun
        (FF, read) ->
            case get(vmq_acl_list) of
                [I | Rest] ->
                    put(vmq_acl_list, Rest),
                    {FF, I};
                [] ->
                    {FF, eof}
            end;
        (_, close) ->
            put(vmq_acl_list, undefined),
            ok
    end,
    age_entries(),
    parse_acl_line(F(F, read), all),
    del_aged_entries().

split_topic_label(Rest) ->
    case binary:split(Rest, <<"label ">>) of
        [Topic, Label] ->
            case string:lexemes(Label, " ") of
                [_] ->
                    {Topic, string:trim(Label)};
                [FirstLabel | _] ->
                    error_logger:warning_msg("can't add multiple label values!"),
                    {Topic, FirstLabel}
            end;
        _ ->
            {Rest, <<>>}
    end.

parse_acl_line({F, <<"#", _/binary>>}, User) ->
    %% comment
    parse_acl_line(F(F, read), User);
parse_acl_line({F, <<"topic read ", Rest/binary>>}, User) ->
    {Topic, Label} = split_topic_label(Rest),
    in(read, User, Topic, Label),
    parse_acl_line(F(F, read), User);
parse_acl_line({F, <<"topic write ", Rest/binary>>}, User) ->
    {Topic, Label} = split_topic_label(Rest),
    in(write, User, Topic, Label),
    parse_acl_line(F(F, read), User);
parse_acl_line({F, <<"topic ", Rest/binary>>}, User) ->
    {Topic, Label} = split_topic_label(Rest),
    in(read, User, Topic, Label),
    in(write, User, Topic, Label),
    parse_acl_line(F(F, read), User);
parse_acl_line({F, <<"user ", User/binary>>}, _) ->
    UserLen = byte_size(User) - 1,
    <<SUser:UserLen/binary, _/binary>> = User,
    parse_acl_line(F(F, read), SUser);
parse_acl_line({F, <<"pattern read ", Rest/binary>>}, User) ->
    {Topic, Label} = split_topic_label(Rest),
    in(read, pattern, Topic, Label),
    parse_acl_line(F(F, read), User);
parse_acl_line({F, <<"pattern write ", Rest/binary>>}, User) ->
    {Topic, Label} = split_topic_label(Rest),
    in(write, pattern, Topic, Label),
    parse_acl_line(F(F, read), User);
parse_acl_line({F, <<"pattern ", Rest/binary>>}, User) ->
    {Topic, Label} = split_topic_label(Rest),
    in(read, pattern, Topic, Label),
    in(write, pattern, Topic, Label),
    parse_acl_line(F(F, read), User);
parse_acl_line({F, <<"token read ", Rest/binary>>}, User) ->
    {Topic, Label} = split_topic_label(Rest),
    insert_token(read, regex, string:trim(Topic), Label),
    parse_acl_line(F(F, read), User);
parse_acl_line({F, <<"token write ", Rest/binary>>}, User) ->
    {Topic, Label} = split_topic_label(Rest),
    insert_token(write, regex, string:trim(Topic), Label),
    parse_acl_line(F(F, read), User);
parse_acl_line({F, <<"token ", Rest/binary>>}, User) ->
    {Topic, Label} = split_topic_label(Rest),
    insert_token(read, regex, string:trim(Topic), Label),
    insert_token(write, regex, string:trim(Topic), Label),
    parse_acl_line(F(F, read), User);
parse_acl_line({F, <<"\n">>}, User) ->
    parse_acl_line(F(F, read), User);
parse_acl_line({F, eof}, _User) ->
    F(F, close),
    ok.

check(Type, [Word | _] = Topic, User, SubscriberId) when is_binary(Word) ->
    case check_all_acl(Type, Topic) of
        {true, MatchedAcl} ->
            {true, MatchedAcl};
        false when User == all -> false;
        false ->
            case check_user_acl(Type, User, Topic) of
                {true, MatchedAcl} ->
                    {true, MatchedAcl};
                false ->
                    case check_pattern_acl(Type, Topic, User, SubscriberId) of
                        {true, MatchedAcl} ->
                            {true, MatchedAcl};
                        false ->
                            check_token_acl(Type, Topic, User, SubscriberId)
                    end
            end
    end.

check_all_acl(Type, TIn) ->
    Tbl = t(Type, all),
    iterate_until_true(Tbl, fun(T) ->
        match(TIn, T, Tbl, T)
    end).

check_user_acl(Type, User, TIn) ->
    Tbl = t(Type, User),
    iterate_until_true(
        ets:match(Tbl, {{User, '$1'}, '_', '_'}),
        fun([T]) ->
            Key = {User, T},
            match(TIn, T, Tbl, Key)
        end
    ).

check_pattern_acl(Type, TIn, User, SubscriberId) ->
    Tbl = t(Type, pattern),
    iterate_until_true(Tbl, fun(P) ->
        T = topic(User, SubscriberId, P),
        match(TIn, T, Tbl, P)
    end).

check_token_acl(Type, TIn, User, SubscriberId) ->
    Tbl = t(Type, token),
    iterate_until_true(Tbl, fun(P) ->
        T = topic(User, SubscriberId, P),
        match(TIn, T, Tbl, P)
    end).

match(TIn, T, Tbl, Key) ->
    case match(TIn, T) of
        true ->
            case ets:lookup(Tbl, Key) of
                [{_, _, <<>>}] ->
                    MatchedAcl = #matched_acl{
                        pattern = iolist_to_binary(vmq_topic:unword(T))
                    },
                    {true, MatchedAcl};
                [{_, _, Label}] ->
                    MatchedAcl = #matched_acl{
                        name = Label, pattern = iolist_to_binary(vmq_topic:unword(T))
                    },
                    {true, MatchedAcl};
                _ ->
                    MatchedAcl = #matched_acl{
                        pattern = iolist_to_binary(vmq_topic:unword(T))
                    },
                    {true, MatchedAcl}
            end;
        _ ->
            false
    end.

topic(User, {MP, ClientId}, Topic) ->
    subst(list_to_binary(MP), User, ClientId, Topic, []).

subst(MP, User, ClientId, [U | Rest], Acc) when U == ?USER_SUP ->
    subst(MP, User, ClientId, Rest, [User | Acc]);
subst(MP, User, ClientId, [C | Rest], Acc) when C == ?CLIENT_SUP ->
    subst(MP, User, ClientId, Rest, [ClientId | Acc]);
subst(MP, User, ClientId, [M | Rest], Acc) when M == ?MOUNTPOINT_SUP ->
    subst(MP, User, ClientId, Rest, [MP | Acc]);
subst(MP, User, ClientId, [W | Rest], Acc) when is_tuple(W) ->
    {Id, Delim, Idx} = W,
    case Id of
        <<"u">> ->
            subst(MP, User, ClientId, Rest, [
                string:nth_lexeme(User, Idx, binary_to_list(Delim)) | Acc
            ]);
        <<"c">> ->
            subst(MP, User, ClientId, Rest, [
                string:nth_lexeme(ClientId, Idx, binary_to_list(Delim)) | Acc
            ])
    end;
subst(MP, User, ClientId, [W | Rest], Acc) ->
    subst(MP, User, ClientId, Rest, [W | Acc]);
subst(_, _, _, [], Acc) ->
    lists:reverse(Acc).

in(Type, User, Topic, Label) when is_binary(Topic) ->
    TopicLen = byte_size(Topic) - 1,
    <<STopic:TopicLen/binary, _/binary>> = Topic,
    case validate(STopic) of
        {ok, Words} ->
            {Tbl, Obj} = t(Type, User, Words, Label),
            ets:insert(Tbl, Obj);
        {error, Reason} ->
            error_logger:warning_msg("can't validate ~p acl topic ~p for user ~p due to ~p", [
                Type, STopic, User, Reason
            ])
    end.

insert_token(Type, _, Topic, Label) when is_binary(Topic) ->
    case parse_topic(Topic) of
        [] ->
            ok;
        Words ->
            {Tbl, Obj} = t(Type, token, Words, Label),
            ets:insert(Tbl, Obj)
    end.

parse_topic(Topic) ->
    case validate(Topic) of
        {ok, Words} ->
            lists:reverse(parse_tokens(Words, []));
        {error, Reason} ->
            error_logger:error_msg("can't validate acl topic ~p due to ~p", [Topic, Reason]),
            []
    end.

parse_tokens([], Acc) ->
    Acc;
parse_tokens([<<"%", A/binary>> | Words], Acc) ->
    L = ets:lookup(?REGEX_TABLE, re_pattern),
    case re:run(A, lists:nth(1, L), [{capture, all_but_first, binary}]) of
        {match, X} ->
            parse_tokens(Words, [
                {lists:nth(1, X), lists:nth(2, X), binary_to_integer(lists:nth(3, X))} | Acc
            ]);
        _ ->
            error_logger:warning_msg(
                "can't validate ACL topic due to topic not formatted appropriately ~p ~p", [
                    A, Words
                ]
            ),
            []
    end;
parse_tokens([Word | Words], Acc) ->
    parse_tokens(Words, [Word | Acc]).

validate(Topic) ->
    vmq_topic:validate_topic(subscribe, Topic).

t(read, token) -> vmq_enhanced_auth_acl_read_token;
t(write, token) -> vmq_enhanced_auth_acl_write_token;
t(read, all) -> vmq_enhanced_auth_acl_read_all;
t(write, all) -> vmq_enhanced_auth_acl_write_all;
t(read, pattern) -> vmq_enhanced_auth_acl_read_pattern;
t(write, pattern) -> vmq_enhanced_auth_acl_write_pattern;
t(read, _User) -> vmq_enhanced_auth_acl_read_user;
t(write, _User) -> vmq_enhanced_auth_acl_write_user.

t(read, token, Topic, Label) -> {vmq_enhanced_auth_acl_read_token, {Topic, 1, Label}};
t(write, token, Topic, Label) -> {vmq_enhanced_auth_acl_write_token, {Topic, 1, Label}};
t(read, all, Topic, Label) -> {vmq_enhanced_auth_acl_read_all, {Topic, 1, Label}};
t(write, all, Topic, Label) -> {vmq_enhanced_auth_acl_write_all, {Topic, 1, Label}};
t(read, pattern, Topic, Label) -> {vmq_enhanced_auth_acl_read_pattern, {Topic, 1, Label}};
t(write, pattern, Topic, Label) -> {vmq_enhanced_auth_acl_write_pattern, {Topic, 1, Label}};
t(read, User, Topic, Label) -> {vmq_enhanced_auth_acl_read_user, {{User, Topic}, 1, Label}};
t(write, User, Topic, Label) -> {vmq_enhanced_auth_acl_write_user, {{User, Topic}, 1, Label}}.

iterate_until_true(T, Fun) when is_atom(T) ->
    iterate_ets_until_true(T, ets:first(T), Fun);
iterate_until_true(T, Fun) when is_list(T) ->
    iterate_list_until_true(T, Fun).

iterate_ets_until_true(_, '$end_of_table', _) ->
    false;
iterate_ets_until_true(Table, K, Fun) ->
    case Fun(K) of
        {true, MatchedAcl} -> {true, MatchedAcl};
        false -> iterate_ets_until_true(Table, ets:next(Table, K), Fun)
    end.

iterate_list_until_true([], _) ->
    false;
iterate_list_until_true([T | Rest], Fun) ->
    case Fun(T) of
        {true, MatchedAcl} -> {true, MatchedAcl};
        false -> iterate_list_until_true(Rest, Fun)
    end.

rl({ok, Data}) -> Data;
rl({error, Reason}) -> exit(Reason);
rl(eof) -> eof;
rl(Fd) -> rl(file:read_line(Fd)).

age_entries() ->
    lists:foreach(fun age_entries/1, ?TABLES).
age_entries(T) ->
    iterate(T, fun(K) -> ets:update_element(T, K, {2, 2}) end).

del_aged_entries() ->
    lists:foreach(fun del_aged_entries/1, ?TABLES).
del_aged_entries(T) ->
    ets:match_delete(T, {'_', 2, '_'}).

iterate(T, Fun) ->
    iterate(T, Fun, ets:first(T)).
iterate(_, _, '$end_of_table') ->
    ok;
iterate(T, Fun, K) ->
    Fun(K),
    iterate(T, Fun, ets:next(T, K)).

is_auth_on_register_disabled() ->
    E = ?EnableAuthOnRegister,
    if
        E == true -> false;
        true -> true
    end.

is_acl_auth_disabled() ->
    E = ?EnableAclHooks,
    if
        E == true -> false;
        true -> true
    end.

is_topic_invalid(vmq_reg_redis_trie, Topic) ->
    case vmq_topic:contains_wildcard(Topic) of
        true ->
            not is_complex_topic_whitelisted(Topic);
        _ ->
            false
    end;
is_topic_invalid(_, _) ->
    false.

is_complex_topic_whitelisted([<<"$share">>, _Group | Topic]) ->
    is_complex_topic_whitelisted(Topic);
is_complex_topic_whitelisted([<<"$share">> | _] = _Topic) ->
    false;
is_complex_topic_whitelisted(Topic) ->
    MPTopic = {"", Topic},
    case ets:lookup(vmq_redis_trie_node, MPTopic) of
        [#trie_node{topic = undefined}] ->
            false;
        [_] ->
            true;
        _ ->
            false
    end.

insert_regex() ->
    case lists:member(?REGEX_TABLE, ets:all()) of
        true ->
            ok;
        false ->
            ets:new(?REGEX_TABLE, ?TABLE_OPTS),
            {ok, MP} = re:compile(?ARGS_EXTRACT_REGEX),
            ets:insert(?REGEX_TABLE, MP)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Helpers for jwt authentication
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
verify(Password, SecretKey) ->
    try jwerl:verify(Password, hs256, SecretKey) of
        _ -> jwerl:verify(Password, hs256, SecretKey)
    catch
        error:_Error -> {error, invalid_signature}
    end.

check_rid(Claims, UserName) ->
    case maps:find(rid, Claims) of
        {ok, Value} ->
            if
                Value =:= UserName -> ok;
                %else block
                true -> error
            end;
        error ->
            error
    end.

-ifdef(TEST).
%%%%%%%%%%%%%
%%% Tests
%%%%%%%%%%%%%

acl_test_() ->
    [
        {"Simple ACL Test - vmq_reg_trie", ?setup(fun simple_acl/1)},
        {"Simple ACL Test - Delete aged acl test", ?setup(fun delete_aged_acl_test/1)},
        {"Simple ACL Test - vmq_reg_redis_trie",
            {setup, fun setup_vmq_reg_redis_trie/0, fun teardown/1, fun simple_acl/1}},
        {"Complex ACL Test - Add complex topic",
            {setup, fun setup_vmq_reg_redis_trie/0, fun teardown/1, fun add_complex_acl_test/1}},
        {"Complex ACL Test - Delete complex topic",
            {setup, fun setup_vmq_reg_redis_trie/0, fun teardown/1, fun delete_complex_acl_test/1}},
        {"Complex ACL Test - Sub-topic whitelisting",
            {setup, fun setup_vmq_reg_redis_trie/0, fun teardown/1, fun subtopic_subscribe_test/1}}
    ].

setup() ->
    ok = application:set_env(vmq_enhanced_auth, enable_acl_hooks, true),
    ets:new(?TOPIC_LABEL_TABLE, [named_table, public, {write_concurrency, true}]),
    insert_regex(),
    init().
setup_vmq_reg_redis_trie() ->
    ok = application:set_env(vmq_enhanced_auth, enable_acl_hooks, true),
    ok = application:set_env(vmq_server, default_reg_view, vmq_reg_redis_trie),
    init(),
    ets:new(?TOPIC_LABEL_TABLE, [named_table, public, {write_concurrency, true}]),
    ets:new(vmq_redis_trie_node, [{keypos, 2} | ?TABLE_OPTS]),
    ets:new(vmq_redis_trie, [{keypos, 2} | ?TABLE_OPTS]),
    ets:insert(
        vmq_redis_trie_node,
        {trie_node, {"", [<<"x">>, <<"y">>, <<"z">>, <<"#">>]}, [
            <<"x">>, <<"y">>, <<"z">>, <<"#">>
        ]}
    ),
    vmq_reg_redis_trie.
teardown(RegView) ->
    case RegView of
        vmq_reg_redis_trie ->
            ets:delete(vmq_redis_trie),
            ets:delete(vmq_redis_trie_node);
        _ ->
            ok
    end,
    ets:delete(vmq_enhanced_auth_acl_read_all),
    ets:delete(vmq_enhanced_auth_acl_write_all),
    ets:delete(vmq_enhanced_auth_acl_read_user),
    ets:delete(vmq_enhanced_auth_acl_write_user),
    ets:delete(?TOPIC_LABEL_TABLE),
    application:unset_env(vmq_enhanced_auth, enable_acl_hooks),
    application:unset_env(vmq_server, default_reg_view).

simple_acl(_) ->
    ACL = [
        <<"# simple comment\n">>,
        <<"topic read a/b/c\n">>,
        <<"# other comment \n">>,
        <<"topic write a/b/c\n">>,
        <<"\n">>,
        <<"# ACL for user 'test'\n">>,
        <<"user test\n">>,
        <<"topic x/y/z/#\n">>,
        <<"# ACL with label\n">>,
        <<"topic read x/y/z label simple_read\n">>,
        <<"topic write x/y/z label simple_write\n">>,
        <<"# ACL for user 'test_user'\n">>,
        <<"user test_user\n">>,
        <<"topic a/b/c/# label simple_read_write\n">>,
        <<"# some patterns\n">>,
        <<"pattern read a/%m/%u/%c\n">>,
        <<"token read example_pattern/%(c, :, 3)\n">>,
        <<"pattern write a/%m/%u/%c\n">>,
        <<"token read p/q/%( u  , :, 2)/r">>,
        <<"token read p/q/%( c  , :, 3)/+">>,
        <<"token write write-topic/p/q/%( c  , :, 3)/+">>,
        <<"# some patterns with label\n">>,
        <<"pattern read %m/%u/%c label pattern_read\n">>,
        <<"token read example/%(c, :, 3) label token_read\n">>,
        <<"pattern write %m/%u/%c label pattern_write\n">>,
        <<"token read a/b/%( u  , :, 2)/c label token_read_u2">>,
        <<"token read a/b/%( c  , :, 3)/+ label token_write">>,
        <<"token write write-topic/a/b/%( c  , :, 3)/+ label token_write_c3">>,
        <<"# ACL with wrong labels\n">>,
        <<"token read a/b label">>,
        <<"token write a/b label a b">>
    ],
    load_from_list(ACL),
    [
        ?_assertEqual(
            [[{[<<"a">>, <<"b">>, <<"c">>], 1, <<>>}]],
            ets:match(vmq_enhanced_auth_acl_read_all, '$1')
        ),
        ?_assertEqual(
            [[{[<<"a">>, <<"b">>, <<"c">>], 1, <<>>}]],
            ets:match(vmq_enhanced_auth_acl_write_all, '$1')
        ),
        ?_assertEqual(
            [
                [{{<<"test">>, [<<"x">>, <<"y">>, <<"z">>, <<"#">>]}, 1, <<>>}],
                %ACL with Label
                [{{<<"test">>, [<<"x">>, <<"y">>, <<"z">>]}, 1, <<"simple_read">>}],
                [
                    {
                        {<<"test_user">>, [<<"a">>, <<"b">>, <<"c">>, <<"#">>]},
                        1,
                        <<"simple_read_write">>
                    }
                ]
            ],
            ets:match(vmq_enhanced_auth_acl_read_user, '$1')
        ),
        ?_assertEqual(
            [
                [{{<<"test">>, [<<"x">>, <<"y">>, <<"z">>, <<"#">>]}, 1, <<>>}],
                %ACL with Label
                [{{<<"test">>, [<<"x">>, <<"y">>, <<"z">>]}, 1, <<"simple_write">>}],
                [
                    {
                        {<<"test_user">>, [<<"a">>, <<"b">>, <<"c">>, <<"#">>]},
                        1,
                        <<"simple_read_write">>
                    }
                ]
            ],
            ets:match(vmq_enhanced_auth_acl_write_user, '$1')
        ),
        ?_assertEqual(
            [
                [{[<<"%m">>, <<"%u">>, <<"%c">>], 1, <<"pattern_read">>}],
                [{[<<"a">>, <<"%m">>, <<"%u">>, <<"%c">>], 1, <<>>}]
            ],
            ets:match(vmq_enhanced_auth_acl_read_pattern, '$1')
        ),
        ?_assertEqual(
            [
                [{[<<"%m">>, <<"%u">>, <<"%c">>], 1, <<"pattern_write">>}],
                [{[<<"a">>, <<"%m">>, <<"%u">>, <<"%c">>], 1, <<>>}]
            ],
            ets:match(vmq_enhanced_auth_acl_write_pattern, '$1')
        ),
        %% positive auth_on_subscribe
        ?_assertEqual(
            {ok, [
                {
                    [<<"a">>, <<"b">>, <<"id">>, <<"c">>],
                    0,
                    {matched_acl, <<"token_write">>, <<"a/b/id/+">>}
                },
                {
                    [<<"a">>, <<"b">>, <<"1">>, <<"c">>],
                    0,
                    {matched_acl, <<"token_read_u2">>, <<"a/b/1/c">>}
                }
            ]},
            auth_on_subscribe(
                <<"user:1:name">>,
                {"", <<"my:client:id">>},
                [
                    {[<<"a">>, <<"b">>, <<"id">>, <<"c">>], 0},
                    {[<<"a">>, <<"b">>, <<"1">>, <<"c">>], 0}
                ]
            )
        ),
        ?_assertEqual(
            {ok, [
                {[<<"a">>, <<"b">>, <<"c">>], 0, {matched_acl, undefined, <<"a/b/c">>}},
                {[<<"x">>, <<"y">>, <<"z">>, <<"#">>], 0, {matched_acl, undefined, <<"x/y/z/#">>}},
                {
                    [<<>>, <<"test">>, <<"my-client-id">>],
                    0,
                    {matched_acl, <<"pattern_read">>, <<"/test/my-client-id">>}
                }
            ]},
            auth_on_subscribe(
                <<"test">>,
                {"", <<"my-client-id">>},
                [
                    {[<<"a">>, <<"b">>, <<"c">>], 0},
                    {[<<"x">>, <<"y">>, <<"z">>, <<"#">>], 0},
                    {[<<"">>, <<"test">>, <<"my-client-id">>], 0}
                ]
            )
        ),
        ?_assertEqual(
            {ok, [
                {[<<"a">>, <<"b">>, <<"c">>], 0, {matched_acl, undefined, <<"a/b/c">>}},
                {[<<"x">>, <<"y">>, <<"z">>, <<"#">>], 0, {matched_acl, undefined, <<"x/y/z/#">>}},
                {
                    [<<"example">>, <<"profile-id">>],
                    0,
                    {matched_acl, <<"token_read">>, <<"example/profile-id">>}
                }
            ]},
            auth_on_subscribe(
                <<"test">>,
                {"", <<"device-id:owner-id:profile-id">>},
                [
                    {[<<"a">>, <<"b">>, <<"c">>], 0},
                    {[<<"x">>, <<"y">>, <<"z">>, <<"#">>], 0},
                    {[<<"example">>, <<"profile-id">>], 0}
                ]
            )
        ),

        %% colon separated username
        ?_assertEqual(
            {ok, [
                {[<<"a">>, <<"b">>, <<"c">>], 0, {matched_acl, undefined, <<"a/b/c">>}},
                {[<<"x">>, <<"y">>, <<"z">>, <<"#">>], 0, {matched_acl, undefined, <<"x/y/z/#">>}},
                {
                    [<<>>, <<"test">>, <<"my-client-id">>],
                    0,
                    {matched_acl, <<"pattern_read">>, <<"/test/my-client-id">>}
                }
            ]},
            auth_on_subscribe(
                <<"test">>,
                {"", <<"my-client-id">>},
                [
                    {[<<"a">>, <<"b">>, <<"c">>], 0},
                    {[<<"x">>, <<"y">>, <<"z">>, <<"#">>], 0},
                    {[<<"">>, <<"test">>, <<"my-client-id">>], 0}
                ]
            )
        ),
        ?_assertEqual(
            {ok, [
                {[<<"a">>, <<"b">>, <<"c">>], 0, {matched_acl, undefined, <<"a/b/c">>}},
                {
                    [<<"x">>, <<"y">>, <<"z">>, <<"#">>],
                    not_allowed,
                    {matched_acl, undefined, undefined}
                },
                {
                    [<<>>, <<"test">>, <<"my-client-id">>],
                    not_allowed,
                    {matched_acl, undefined, undefined}
                }
            ]},
            auth_on_subscribe(
                <<"invalid-user">>,
                {"", <<"my-client-id">>},
                [
                    {[<<"a">>, <<"b">>, <<"c">>], 0},
                    {[<<"x">>, <<"y">>, <<"z">>, <<"#">>], 0},
                    {[<<"">>, <<"test">>, <<"my-client-id">>], 0}
                ]
            )
        ),
        ?_assertEqual(
            {ok, [{matched_acl, {matched_acl, undefined, <<"a/b/c">>}}]},
            auth_on_publish(
                <<"test">>,
                {"", <<"my-client-id">>},
                1,
                [<<"a">>, <<"b">>, <<"c">>],
                <<"payload">>,
                false
            )
        ),
        ?_assertEqual(
            {ok, [{matched_acl, {matched_acl, <<"token_write_c3">>, <<"write-topic/a/b/id/+">>}}]},
            auth_on_publish(
                <<"test">>,
                {"", <<"my:client:id">>},
                1,
                [<<"write-topic">>, <<"a">>, <<"b">>, <<"id">>, <<"c">>],
                <<"payload">>,
                false
            )
        ),
        ?_assertEqual(
            {ok, [{matched_acl, {matched_acl, undefined, <<"x/y/z/#">>}}]},
            auth_on_publish(
                <<"test">>,
                {"", <<"my-client-id">>},
                1,
                [<<"x">>, <<"y">>, <<"z">>, <<"test">>],
                <<"payload">>,
                false
            )
        ),
        ?_assertEqual(
            {ok, [{matched_acl, {matched_acl, <<"pattern_write">>, <<"/test/my-client-id">>}}]},
            auth_on_publish(
                <<"test">>,
                {"", <<"my-client-id">>},
                1,
                [<<"">>, <<"test">>, <<"my-client-id">>],
                <<"payload">>,
                false
            )
        ),
        ?_assertEqual(
            next,
            auth_on_publish(
                <<"invalid-user">>,
                {"", <<"my-client-id">>},
                1,
                [<<"x">>, <<"y">>, <<"z">>, <<"test">>],
                <<"payload">>,
                false
            )
        ),
        ?_assertEqual(
            next,
            auth_on_publish(
                <<"invalid-user">>,
                {"", <<"my-client-id">>},
                1,
                [<<"">>, <<"test">>, <<"my-client-id">>],
                <<"payload">>,
                false
            )
        ),
        %% ACL with Labels
        ?_assertEqual(
            [
                [{[<<"example_pattern">>, {<<"c">>, <<":">>, 3}], 1, <<>>}],
                [{[<<"example">>, {<<"c">>, <<":">>, 3}], 1, <<"token_read">>}],
                [{[<<"p">>, <<"q">>, {<<"c">>, <<":">>, 3}, <<"+">>], 1, <<>>}],
                [{[<<"a">>, <<"b label">>], 1, <<>>}],
                [{[<<"a">>, <<"b">>, {<<"u">>, <<":">>, 2}, <<"c">>], 1, <<"token_read_u2">>}],
                [{[<<"a">>, <<"b">>, {<<"c">>, <<":">>, 3}, <<"+">>], 1, <<"token_write">>}],
                [{[<<"p">>, <<"q">>, {<<"u">>, <<":">>, 2}, <<"r">>], 1, <<>>}]
            ],
            ets:match(vmq_enhanced_auth_acl_read_token, '$1')
        ),
        ?_assertEqual(
            [
                [{[<<"write-topic">>, <<"p">>, <<"q">>, {<<"c">>, <<":">>, 3}, <<"+">>], 1, <<>>}],
                [
                    {
                        [<<"write-topic">>, <<"a">>, <<"b">>, {<<"c">>, <<":">>, 3}, <<"+">>],
                        1,
                        <<"token_write_c3">>
                    }
                ],
                [{[<<"a">>, <<"b">>], 1, <<"a">>}]
            ],
            ets:match(vmq_enhanced_auth_acl_write_token, '$1')
        ),
        ?_assertEqual(
            {<<"a/b ">>, <<"ab_topic">>},
            split_topic_label(<<"a/b label ab_topic\n">>)
        ),
        ?_assertEqual(
            {<<"a/b label\n">>, <<>>},
            split_topic_label(<<"a/b label\n">>)
        ),
        ?_assertEqual(
            {<<"a/b ">>, <<"a">>},
            split_topic_label(<<"a/b label a b\n">>)
        )
    ].
delete_aged_acl_test(_) ->
    ACL = [
        <<"topic a/b/c label abc\n">>,
        <<"user test\n">>,
        <<"topic a/b/c/# label simple_read_write\n">>,
        <<"pattern read a/%m/%u/%c label pattern_read\n">>,
        <<"pattern write a/%m/%u/%c\n">>,
        <<"token read example/%(c, :, 3) label token_read\n">>,
        <<"token write a/b/%( c  , :, 3)/+ label token_write">>
    ],
    load_from_list(ACL),
    NewACL = [
        <<"topic a/b/d label abd\n">>,
        <<"user test\n">>,
        <<"topic a/b/d/# label simple_read_write\n">>,
        <<"pattern read b/%m/%u/%clabel pattern_read\n">>,
        <<"pattern write b/%m/%u/%c\n">>,
        <<"token read example_acl/%(c, :, 3) label token_read\n">>,
        <<"token write a/c/%( c  , :, 3)/+ label token_write">>
    ],
    load_from_list(NewACL),
    [
        ?_assertEqual(
            [],
            ets:match(vmq_enhanced_auth_acl_read_pattern, {'_', 2, '$1'})
        ),
        ?_assertEqual(
            [],
            ets:match(vmq_enhanced_auth_acl_write_pattern, {'_', 2, '$1'})
        ),
        ?_assertEqual(
            [],
            ets:match(vmq_enhanced_auth_acl_read_all, {'_', 2, '$1'})
        ),
        ?_assertEqual(
            [],
            ets:match(vmq_enhanced_auth_acl_write_all, {'_', 2, '$1'})
        ),
        ?_assertEqual(
            [],
            ets:match(vmq_enhanced_auth_acl_read_user, {'_', 2, '$1'})
        ),
        ?_assertEqual(
            [],
            ets:match(vmq_enhanced_auth_acl_write_user, {'_', 2, '$1'})
        ),
        ?_assertEqual(
            [],
            ets:match(vmq_enhanced_auth_acl_read_token, {'_', 2, '$1'})
        ),
        ?_assertEqual(
            [],
            ets:match(vmq_enhanced_auth_acl_write_token, {'_', 2, '$1'})
        )
    ].
add_complex_acl_test(_) ->
    ACL = [<<"topic abc/xyz/#\n">>],
    load_from_list(ACL),
    Topic1 = [<<"abc">>, <<"xyz">>, <<"+">>, <<"1">>],
    Topic2 = [<<"abc">>, <<"xyz">>, <<"+">>, <<"2">>],
    vmq_reg_redis_trie:add_complex_topic("", Topic1),
    vmq_reg_redis_trie:add_complex_topic("", Topic2),
    [
        ?_assertEqual(
            {ok, [
                {Topic1, 0, {matched_acl, undefined, <<"abc/xyz/#">>}}
            ]},
            auth_on_subscribe(
                <<"test">>,
                {"", <<"my-client-id">>},
                [
                    {Topic1, 0}
                ]
            )
        ),
        ?_assertEqual(
            {ok, [
                {Topic2, 0, {matched_acl, undefined, <<"abc/xyz/#">>}}
            ]},
            auth_on_subscribe(
                <<"test">>,
                {"", <<"my-client-id">>},
                [
                    {Topic2, 0}
                ]
            )
        )
    ].
delete_complex_acl_test(_) ->
    ACL = [<<"topic abc/xyz/#\n">>],
    load_from_list(ACL),
    Topic1 = [<<"abc">>, <<"xyz">>, <<"+">>, <<"1">>],
    Topic2 = [<<"abc">>, <<"xyz">>, <<"+">>, <<"2">>],
    SubTopic = [<<"abc">>, <<"xyz">>, <<"+">>],
    vmq_reg_redis_trie:add_complex_topic("", Topic1),
    vmq_reg_redis_trie:add_complex_topic("", Topic2),
    vmq_reg_redis_trie:delete_complex_topic("", Topic1),
    vmq_reg_redis_trie:delete_complex_topic("", SubTopic),
    [
        ?_assertEqual(
            next,
            auth_on_subscribe(
                <<"test">>,
                {"", <<"my-client-id">>},
                [
                    {Topic1, 0}
                ]
            )
        ),
        ?_assertEqual(
            {ok, [
                {Topic2, 0, {matched_acl, undefined, <<"abc/xyz/#">>}}
            ]},
            auth_on_subscribe(
                <<"test">>,
                {"", <<"my-client-id">>},
                [
                    {Topic2, 0}
                ]
            )
        )
    ].
subtopic_subscribe_test(_) ->
    ACL = [<<"topic abc/xyz/#\n">>],
    load_from_list(ACL),
    Topic1 = [<<"abc">>, <<"xyz">>, <<"+">>, <<"1">>, <<"+">>],
    Topic2 = [<<"abc">>, <<"xyz">>, <<"+">>, <<"2">>],
    SubTopic1 = [<<"abc">>, <<"xyz">>, <<"+">>, <<"1">>],
    SubTopic2 = [<<"abc">>, <<"xyz">>, <<"+">>],
    vmq_reg_redis_trie:add_complex_topic("", Topic1),
    vmq_reg_redis_trie:add_complex_topic("", SubTopic1),
    [
        ?_assertEqual(
            {ok, [
                {Topic1, 0, {matched_acl, undefined, <<"abc/xyz/#">>}}
            ]},
            auth_on_subscribe(
                <<"test">>,
                {"", <<"my-client-id">>},
                [
                    {Topic1, 0}
                ]
            )
        ),
        ?_assertEqual(
            {ok, [
                {SubTopic1, 0, {matched_acl, undefined, <<"abc/xyz/#">>}}
            ]},
            auth_on_subscribe(
                <<"test">>,
                {"", <<"my-client-id">>},
                [
                    {SubTopic1, 0}
                ]
            )
        ),
        ?_assertEqual(
            next,
            auth_on_subscribe(
                <<"test">>,
                {"", <<"my-client-id">>},
                [
                    {SubTopic2, 0}
                ]
            )
        ),
        ?_assertEqual(
            true,
            is_complex_topic_whitelisted(Topic1)
        ),
        ?_assertEqual(
            false,
            is_complex_topic_whitelisted(Topic2)
        ),
        ?_assertEqual(
            true,
            is_complex_topic_whitelisted(SubTopic1)
        ),
        ?_assertEqual(
            false,
            is_complex_topic_whitelisted(SubTopic2)
        )
    ].
-endif.
