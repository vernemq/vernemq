
%% Copyright 2019 Octavo Labs AG Zurich Switzerland (https://octavolabs.com)
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
-module(vmq_topic).

-import(lists, [reverse/1]).

%% ------------------------------------------------------------------------
%% Topic semantics and usage
%% ------------------------------------------------------------------------
%% A topic must be at least one character long.
%%
%% Topic names are case sensitive. For example, ACCOUNTS and Accounts are two different topics.
%%
%% Topic names can include the space character. For example, Accounts payable is a valid topic.
%%
%% A leading "/" creates a distinct topic. For example, /finance is different from finance. /finance matches "+/+" and "/+", but not "+".
%%
%% Do not include the null character (Unicode \x0000) in any topic.
%%
%% The following principles apply to the construction and content of a topic tree:
%%
%% The length is limited to 64k but within that there are no limits to the number of levels in a topic tree.
%%
%% There can be any number of root nodes; that is, there can be any number of topic trees.
%% ------------------------------------------------------------------------

-export([new/1,
         match/2,
         validate_topic/2,
         contains_wildcard/1,
         unword/1,
         word/1,
         triples/1]).

-define(MAX_LEN, 65536).
-define(DEFAULT_MAX_TOPIC_DEPTH, 10).

new(Name) when is_list(Name) ->
    {topic, Name}.

%% ------------------------------------------------------------------------
%% topic match
%% ------------------------------------------------------------------------
match([], []) ->
	true;
match([H|T1], [H|T2]) ->
	match(T1, T2);
match([_H|T1], [<<"+">>|T2]) ->
	match(T1, T2);
match(_, [<<"#">>]) ->
	true;
match([_H1|_], [_H2|_]) ->
	false;
match([], [_H|_T2]) ->
	false;
match(_, _) -> false.


%% ------------------------------------------------------------------------
%% topic validate
%% ------------------------------------------------------------------------
triples([Word|_] = Topic) when is_binary(Word) ->
    triples(reverse(Topic), []).

triples([Word], Acc) ->
    [{root, Word, [Word]}|Acc];
triples([Word|Rest] = Topic, Acc) ->
    triples(Rest, [{reverse(Rest), Word, reverse(Topic)}|Acc]).

unword(Topic) ->
    vernemq_dev_api:unword_topic(Topic).

word(Topic) ->
    re:split(Topic, <<"/">>).

validate_topic(_Type, <<>>) ->
    {error, no_empty_topic_allowed};
validate_topic(_Type, Topic) when byte_size(Topic) > ?MAX_LEN ->
    {error, subscribe_topic_too_long};
validate_topic(publish, Topic) ->
    validate_publish_topic(Topic, [], []);
validate_topic(subscribe, Topic) ->
    validate_subscribe_topic(Topic, [], []).

contains_wildcard([<<"+">>|_]) -> true;
contains_wildcard([<<"#">>]) -> true;
contains_wildcard([_|Rest]) ->
    contains_wildcard(Rest);
contains_wildcard([]) -> false.

validate_topic_depth(Topic) ->
    MaxTopicLength = application:get_env(vmq_server, topic_max_depth, ?DEFAULT_MAX_TOPIC_DEPTH),
    case length(Topic) > MaxTopicLength of
        true -> {error, topic_max_depth_exceeded};
        false -> {ok, Topic}
    end.

validate_publish_topic(<<>>, WordAcc, TopicAcc) ->
    Topic = reverse([acc_word(WordAcc) | TopicAcc]),
    validate_topic_depth(Topic);
%% Topic Names and Topic Filters MUST NOT include the null character (Unicode U+0000) [MQTT-4.7.3-2]
validate_publish_topic(<<0/utf8, _Rest/binary>>, _WordAcc, _TopicAcc) ->
    {error, no_null_allowed_in_topic};
validate_publish_topic(<<"+"/utf8, _Rest/binary>>, _WordAcc, _TopicAcc) ->
    {error, 'no_+_allowed_in_publish'};
validate_publish_topic(<<"#"/utf8, _Rest/binary>>, _WordAcc, _TopicAcc) ->
    {error, 'no_#_allowed_in_publish'};
validate_publish_topic(<<"//"/utf8, Rest/binary>>, WordAcc, TopicAcc) ->
    validate_publish_topic(Rest, [], [<<>>, acc_word(WordAcc) | TopicAcc]);
validate_publish_topic(<<"/"/utf8, Rest/binary>>, WordAcc, TopicAcc) ->
    validate_publish_topic(Rest, [], [acc_word(WordAcc) | TopicAcc]);
validate_publish_topic(<<Codepoint/utf8, Rest/binary>>, WordAcc, TopicAcc) ->
    validate_publish_topic(Rest, [Codepoint | WordAcc], TopicAcc);
validate_publish_topic(<<_/binary>>, _WordAcc, _TopicAcc) ->
    {error, non_utf8_character}.

acc_word([]) ->
    <<>>;
acc_word(CodePointList) ->
    unicode:characters_to_binary(lists:reverse(CodePointList), utf8).

validate_subscribe_topic(<<>>, WordAcc, TopicAcc) ->
    validate_shared_subscription(lists:reverse([acc_word(WordAcc) | TopicAcc]));
validate_subscribe_topic(<<"+"/utf8>>, [], TopicAcc) ->
    validate_shared_subscription(lists:reverse([<<"+">> | TopicAcc]));
validate_subscribe_topic(<<"#"/utf8>>, [], TopicAcc) ->
    validate_shared_subscription(lists:reverse([<<"#">> | TopicAcc]));
validate_subscribe_topic(<<"+/"/utf8, Rest/binary>>, [], TopicAcc) ->
    validate_subscribe_topic(Rest, [], [<<"+">> | TopicAcc]);
validate_subscribe_topic(<<0/utf8, _Rest/binary>>, _WordAcc, _TopicAcc) ->
    {error, no_null_allowed_in_topic};
validate_subscribe_topic(<<"+"/utf8, _Rest/binary>>, _WordAcc, _TopicAcc) ->
    {error, 'no_+_allowed_in_word'};
validate_subscribe_topic(<<"#"/utf8, _Rest/binary>>, _WordAcc, _TopicAcc) ->
    {error, 'no_#_allowed_in_word'};
validate_subscribe_topic(<<"//"/utf8, Rest/binary>>, WordAcc, TopicAcc) ->
    validate_subscribe_topic(Rest, [], [<<>>, acc_word(WordAcc) | TopicAcc]);
validate_subscribe_topic(<<"/"/utf8, Rest/binary>>, WordAcc, TopicAcc) ->
    validate_subscribe_topic(Rest, [], [acc_word(WordAcc) | TopicAcc]);
validate_subscribe_topic(<<Codepoint/utf8, Rest/binary>>, WordAcc, TopicAcc) ->
    validate_subscribe_topic(Rest, [Codepoint | WordAcc], TopicAcc);
validate_subscribe_topic(<<_/binary>>, _WordAcc, _TopicAcc) ->
    {error, non_utf8_character}.

validate_shared_subscription([<<"$share">>, _Group, _FirstWord | _] = Topic) -> validate_topic_depth(Topic);
validate_shared_subscription([<<"$share">> | _] = _Topic) -> {error, invalid_shared_subscription};
validate_shared_subscription(Topic) -> validate_topic_depth(Topic).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

validate_no_wildcard_test() ->
    {ok,[<<>>,<<>>]}
    = validate_topic(subscribe, <<"/"/utf8>>),
    {ok,[<<>>,<<"a">>]}
    = validate_topic(subscribe, <<"/a">>),
    {ok, [<<"a">>, <<"b">>, <<"c">>]}
    = validate_topic(subscribe, <<"a/b/c"/utf8>>),
    {ok, [<<>>, <<"a">>, <<"b">>]}
    = validate_topic(subscribe, <<"/a/b"/utf8>>),
    {ok, [<<"test">>, <<"topic">>, <<>>]}
    = validate_topic(subscribe, <<"test/topic/"/utf8>>),
    {ok, [<<"test">>, <<>>, <<>>, <<>>, <<"a">>, <<>>, <<"topic">>]}
    = validate_topic(subscribe, <<"test////a//topic"/utf8>>),
    {ok, [<<>>, <<"test">>, <<>>, <<>>, <<>>, <<"a">>, <<>>, <<"topic">>]}
    = validate_topic(subscribe, <<"/test////a//topic"/utf8>>),
    {ok, [<<>>, <<>>, <<>>, <<>>, <<"foo">>, <<>>, <<>>, <<"bar">>]}
    = validate_topic(subscribe, <<"////foo///bar"/utf8>>),
    {ok, [<<"Юникод"/utf8>>, <<"åäö"/utf8>>]}
    = validate_topic(subscribe, <<"Юникод/åäö"/utf8>>),
    {error, no_null_allowed_in_topic}
    = validate_topic(subscribe, <<0/utf8, "foo/bar"/utf8>>),
    {error, non_utf8_character}
    = validate_topic(subscribe, unicode:characters_to_binary([16#10437], utf16, utf16)),
    AtomsList = [list_to_atom(integer_to_list(I)) || I <- lists:seq(1,100)],
    {error, non_utf8_character}
    = validate_topic(subscribe, term_to_binary(AtomsList)),
    {error, non_utf8_character}
    = validate_topic(subscribe, term_to_binary(this_is_an_atom)),

    {ok, [<<"foo">>, <<>>, <<"bar">>, <<>>, <<>>, <<"baz">>]}
    = validate_topic(publish, <<"foo//bar///baz"/utf8>>),
    {ok, [<<"foo">>, <<>>, <<"baz">>, <<>>, <<>>]}
    = validate_topic(publish, <<"foo//baz//"/utf8>>),
    {ok, [<<"foo">>, <<>>, <<"baz">>]}
    = validate_topic(publish, <<"foo//baz"/utf8>>),
    {ok, [<<"foo">>, <<>>, <<"baz">>, <<"bar">>]}
    = validate_topic(publish, <<"foo//baz/bar"/utf8>>),
    {ok, [<<>>, <<>>, <<>>, <<>>, <<"foo">>, <<>>, <<>>, <<"bar">>]}
    = validate_topic(publish, <<"////foo///bar"/utf8>>),
    {ok, [<<"Юникод"/utf8>>, <<"åäö"/utf8>>]}
    = validate_topic(publish, <<"Юникод/åäö"/utf8>>),
    {error, no_null_allowed_in_topic}
    = validate_topic(publish, <<0/utf8, "foo/bar"/utf8>>),
    {error, non_utf8_character}
    = validate_topic(publish, unicode:characters_to_binary([16#10437], utf16, utf16)),
    {error, non_utf8_character}
    = validate_topic(publish, term_to_binary([list_to_atom(integer_to_list(I)) || I <- lists:seq(1,100)])),
    {error, non_utf8_character}
    = validate_topic(publish, term_to_binary(this_is_an_atom)).

validate_wildcard_test() ->
    {ok, [<<>>, <<"+">>, <<"x">>]}
    = validate_topic(subscribe, <<"/+/x"/utf8>>),
    {ok, [<<>>, <<"a">>, <<"b">>, <<"c">>, <<"#">>]}
    = validate_topic(subscribe, <<"/a/b/c/#"/utf8>>),
    {ok, [<<"#">>]}
    = validate_topic(subscribe, <<"#">>),
    {ok, [<<"foo">>, <<"+">>, <<"baz">>]}
    = validate_topic(subscribe, <<"foo/+/baz"/utf8>>),
    {ok, [<<"foo">>, <<"+">>, <<"baz">>, <<"#">>]}
    = validate_topic(subscribe, <<"foo/+/baz/#"/utf8>>),
    {ok, [<<"foo">>, <<"foo">>, <<"baz">>, <<"#">>]}
    = validate_topic(subscribe, <<"foo/foo/baz/#"/utf8>>),
    {ok, [<<"foo">>, <<"#">>]} = validate_topic(subscribe, <<"foo/#"/utf8>>),
    {ok, [<<>>, <<"#">>]} = validate_topic(subscribe, <<"/#"/utf8>>),
    {ok, [<<"test">>, <<"topic">>, <<"+">>]} = validate_topic(subscribe, <<"test/topic/+"/utf8>>),
    {ok, [<<"foo">>, <<"+">>, <<>>]} = validate_topic(subscribe, <<"foo/+/"/utf8>>),
    {ok, [<<"foo">>, <<"+">>]} = validate_topic(subscribe, <<"foo/+"/utf8>>),
    {ok, [<<"Юникод"/utf8>>, <<"+">>, <<>>]} = validate_topic(subscribe, <<"Юникод/+/"/utf8>>),

    {ok,[<<"+">>, <<"+">>, <<"+">>, <<"+">>, <<"+">>, <<"+">>, <<"+">>, <<"test">>]}
    = validate_topic(subscribe, <<"+/+/+/+/+/+/+/test"/utf8>>),
    %% From MQTT 5 spec
    {ok, [<<"+">>, <<"tennis">>, <<"#">>]}
    = validate_topic(subscribe, <<"+/tennis/#"/utf8>>),

    {error, 'no_#_allowed_in_publish'} = validate_topic(publish, <<"test/#-"/utf8>>),
    {error, 'no_+_allowed_in_publish'} = validate_topic(publish, <<"test/+-"/utf8>>),
    {error, 'no_+_allowed_in_publish'} = validate_topic(publish, <<"test/+/"/utf8>>),
    {error, 'no_#_allowed_in_publish'} = validate_topic(publish, <<"test/#"/utf8>>),

	{error, 'no_#_allowed_in_word'} = validate_topic(subscribe, <<"a/#/c"/utf8>>),
    {error, 'no_#_allowed_in_word'} = validate_topic(subscribe, <<"#testtopic"/utf8>>),
    {error, 'no_#_allowed_in_word'} = validate_topic(subscribe, <<"testtopic#"/utf8>>),
    {error, 'no_+_allowed_in_word'} = validate_topic(subscribe, <<"+testtopic"/utf8>>),
    {error, 'no_+_allowed_in_word'} = validate_topic(subscribe, <<"testtopic/++"/utf8>>),
    {error, 'no_+_allowed_in_word'} = validate_topic(subscribe, <<"testtopic+"/utf8>>),
    {error, 'no_+_allowed_in_word'} = validate_topic(subscribe, <<"testtopic++/+"/utf8>>),
    {error, 'no_#_allowed_in_word'} = validate_topic(subscribe, <<"#testtopic/test"/utf8>>),
    {error, 'no_#_allowed_in_word'} = validate_topic(subscribe, <<"testtopic#/test"/utf8>>),
    {error, 'no_+_allowed_in_word'} = validate_topic(subscribe, <<"+testtopic/test"/utf8>>),
    {error, 'no_+_allowed_in_word'} = validate_topic(subscribe, <<"testtopic+/test"/utf8>>),
    {error, 'no_#_allowed_in_word'} = validate_topic(subscribe, <<"/test/#testtopic"/utf8>>),
    {error, 'no_#_allowed_in_word'} = validate_topic(subscribe, <<"/test/testtopic#"/utf8>>),
    {error, 'no_+_allowed_in_word'} = validate_topic(subscribe, <<"/test/+testtopic"/utf8>>),
    {error, 'no_+_allowed_in_word'} = validate_topic(subscribe, <<"/testtesttopic+"/utf8>>).

validate_shared_subscription_test() ->
    {error, invalid_shared_subscription} = validate_topic(subscribe, <<"$share/mygroup">>),
    {ok, [<<"$share">>, <<"mygroup">>, <<"a">>, <<"b">>]} = validate_topic(subscribe, <<"$share/mygroup/a/b">>).

validate_unword_test() ->
    rand:seed(exsplus, erlang:timestamp()),
    random_topics(1000),
    ok.

contains_wildcard_test() ->
    true = contains_wildcard([<<"a">>, <<"+">>, <<"b">>]),
    true = contains_wildcard([<<"#">>]),
    false = contains_wildcard([<<"a">>, <<"b">>, <<"c">>]).

validate_topic_depth_test() ->
    LongTopic = <<"+////////////////////////////////////////">>,
    application:set_env(vmq_server, topic_max_depth, ?DEFAULT_MAX_TOPIC_DEPTH),
    {error, topic_max_depth_exceeded} = validate_topic(subscribe, LongTopic),
    application:set_env(vmq_server, topic_max_depth, 20),
    {ok, _} = validate_topic(subscribe, <<"+////////////////">>),
    application:set_env(vmq_server, topic_max_depth, ?DEFAULT_MAX_TOPIC_DEPTH),
    ok.

random_topics(0) -> ok;
random_topics(N) when N > 0 ->
    NWords = rand:uniform(100),
    Words =
    lists:foldl(fun(_, AAcc) ->
                    case rand:uniform(3) of
                        1 ->
                            ["+/"|AAcc];
                        _ ->
                            [random_word(), "/"|AAcc]
                    end
                end, [], lists:seq(1, NWords)),
    Topic = iolist_to_binary(Words),
    application:set_env(vmq_server, topic_max_depth, 105),
    {ok, T} = validate_topic(subscribe, Topic),
    Topic = iolist_to_binary(unword(T)),
    random_topics(N - 1).

random_word() ->
    Words = "abcdefghijklmnopqrstuvwxyz0123456789",
    N = rand:uniform(length(Words)),
    S = rand:uniform(length(Words) + 1 - N),
    string:substr(Words, N, S).

-endif.
