
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
    validate_publish_topic(Topic, 0, []);
validate_topic(subscribe, Topic) ->
    validate_subscribe_topic(Topic, 0, []).

contains_wildcard([<<"+">>|_]) -> true;
contains_wildcard([<<"#">>]) -> true;
contains_wildcard([_|Rest]) ->
    contains_wildcard(Rest);
contains_wildcard([]) -> false.

validate_publish_topic(<<"+/", _/binary>>, _, _) -> {error, 'no_+_allowed_in_publish'};
validate_publish_topic(<<"+">>, _, _) -> {error, 'no_+_allowed_in_publish'};
validate_publish_topic(<<"#">>,_, _) -> {error, 'no_#_allowed_in_publish'};
validate_publish_topic(Topic, L, Acc) ->
    case Topic of
        <<Word:L/binary, "/", Rest/binary>> ->
            validate_publish_topic(Rest, 0, [Word|Acc]);
        <<Word:L/binary>> ->
            {ok, lists:reverse([Word|Acc])};
        <<_:L/binary, "+", _/binary>> ->
            {error, 'no_+_allowed_in_word'};
        <<_:L/binary, "#", _/binary>> ->
            {error, 'no_#_allowed_in_word'};
        _ ->
            validate_publish_topic(Topic, L + 1, Acc)
    end.

validate_subscribe_topic(<<"+/", Rest/binary>>, _, Acc) -> validate_subscribe_topic(Rest, 0, [<<"+">>|Acc]);
validate_subscribe_topic(<<"+">>, _, Acc) -> validate_shared_subscription(reverse([<<"+">>|Acc]));
validate_subscribe_topic(<<"#">>, _, Acc) -> validate_shared_subscription(reverse([<<"#">>|Acc]));
validate_subscribe_topic(Topic, L, Acc) ->
    case Topic of
        <<Word:L/binary, "/", Rest/binary>> ->
            validate_subscribe_topic(Rest, 0, [Word|Acc]);
        <<Word:L/binary>> ->
            validate_shared_subscription(reverse([Word|Acc]));
        <<_:L/binary, "+", _/binary>> ->
            {error, 'no_+_allowed_in_word'};
        <<_:L/binary, "#", _/binary>> ->
            {error, 'no_#_allowed_in_word'};
        _ ->
            validate_subscribe_topic(Topic, L + 1, Acc)
    end.

validate_shared_subscription([<<"$share">>, _Group, _FirstWord | _] = Topic) -> {ok, Topic};
validate_shared_subscription([<<"$share">> | _] = _Topic) -> {error, invalid_shared_subscription};
validate_shared_subscription(Topic) -> {ok, Topic}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

validate_no_wildcard_test() ->
    % no wildcard
    {ok, [<<"a">>, <<"b">>, <<"c">>]}
    = validate_topic(subscribe, <<"a/b/c">>),
    {ok, [<<>>, <<"a">>, <<"b">>]}
    = validate_topic(subscribe, <<"/a/b">>),
    {ok, [<<"test">>, <<"topic">>, <<>>]}
    = validate_topic(subscribe, <<"test/topic/">>),
    {ok, [<<"test">>, <<>>, <<>>, <<>>, <<"a">>, <<>>, <<"topic">>]}
    = validate_topic(subscribe, <<"test////a//topic">>),
    {ok, [<<>>, <<"test">>, <<>>, <<>>, <<>>, <<"a">>, <<>>, <<"topic">>]}
    = validate_topic(subscribe, <<"/test////a//topic">>),

    {ok, [<<"foo">>, <<>>, <<"bar">>, <<>>, <<>>, <<"baz">>]}
    = validate_topic(publish, <<"foo//bar///baz">>),
    {ok, [<<"foo">>, <<>>, <<"baz">>, <<>>, <<>>]}
    = validate_topic(publish, <<"foo//baz//">>),
    {ok, [<<"foo">>, <<>>, <<"baz">>]}
    = validate_topic(publish, <<"foo//baz">>),
    {ok, [<<"foo">>, <<>>, <<"baz">>, <<"bar">>]}
    = validate_topic(publish, <<"foo//baz/bar">>),
    {ok, [<<>>, <<>>, <<>>, <<>>, <<"foo">>, <<>>, <<>>, <<"bar">>]}
    = validate_topic(publish, <<"////foo///bar">>).

validate_wildcard_test() ->
    {ok, [<<>>, <<"+">>, <<"x">>]}
    = validate_topic(subscribe, <<"/+/x">>),
    {ok, [<<>>, <<"a">>, <<"b">>, <<"c">>, <<"#">>]}
    = validate_topic(subscribe, <<"/a/b/c/#">>),
    {ok, [<<"#">>]}
    = validate_topic(subscribe, <<"#">>),
    {ok, [<<"foo">>, <<"#">>]}
    = validate_topic(subscribe, <<"foo/#">>),
    {ok, [<<"foo">>, <<"+">>, <<"baz">>]}
    = validate_topic(subscribe, <<"foo/+/baz">>),
    {ok, [<<"foo">>, <<"+">>, <<"baz">>, <<"#">>]}
    = validate_topic(subscribe, <<"foo/+/baz/#">>),
    {ok, [<<"foo">>, <<"foo">>, <<"baz">>, <<"#">>]}
    = validate_topic(subscribe, <<"foo/foo/baz/#">>),
    {ok, [<<"foo">>, <<"#">>]} = validate_topic(subscribe, <<"foo/#">>),
    {ok, [<<>>, <<"#">>]} = validate_topic(subscribe, <<"/#">>),
    {ok, [<<"test">>, <<"topic">>, <<"+">>]} = validate_topic(subscribe, <<"test/topic/+">>),
    {ok, [<<"+">>, <<"+">>, <<"+">>, <<"+">>, <<"+">>,
          <<"+">>, <<"+">>, <<"+">>, <<"+">>, <<"+">>, <<"test">>]}
    = validate_topic(subscribe, <<"+/+/+/+/+/+/+/+/+/+/test">>),

    {error, 'no_#_allowed_in_word'} = validate_topic(publish, <<"test/#-">>),
    {error, 'no_+_allowed_in_word'} = validate_topic(publish, <<"test/+-">>),
    {error, 'no_+_allowed_in_publish'} = validate_topic(publish, <<"test/+/">>),
    {error, 'no_#_allowed_in_publish'} = validate_topic(publish, <<"test/#">>),

	{error, 'no_#_allowed_in_word'} = validate_topic(subscribe, <<"a/#/c">>),
    {error, 'no_#_allowed_in_word'} = validate_topic(subscribe, <<"#testtopic">>),
    {error, 'no_#_allowed_in_word'} = validate_topic(subscribe, <<"testtopic#">>),
    {error, 'no_+_allowed_in_word'} = validate_topic(subscribe, <<"+testtopic">>),
    {error, 'no_+_allowed_in_word'} = validate_topic(subscribe, <<"testtopic+">>),
    {error, 'no_#_allowed_in_word'} = validate_topic(subscribe, <<"#testtopic/test">>),
    {error, 'no_#_allowed_in_word'} = validate_topic(subscribe, <<"testtopic#/test">>),
    {error, 'no_+_allowed_in_word'} = validate_topic(subscribe, <<"+testtopic/test">>),
    {error, 'no_+_allowed_in_word'} = validate_topic(subscribe, <<"testtopic+/test">>),
    {error, 'no_#_allowed_in_word'} = validate_topic(subscribe, <<"/test/#testtopic">>),
    {error, 'no_#_allowed_in_word'} = validate_topic(subscribe, <<"/test/testtopic#">>),
    {error, 'no_+_allowed_in_word'} = validate_topic(subscribe, <<"/test/+testtopic">>),
    {error, 'no_+_allowed_in_word'} = validate_topic(subscribe, <<"/testtesttopic+">>).

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
    {ok, T} = validate_topic(subscribe, Topic),
    Topic = iolist_to_binary(unword(T)),
    random_topics(N - 1).

random_word() ->
    Words = "abcdefghijklmnopqrstuvwxyz0123456789",
    N = rand:uniform(length(Words)),
    S = rand:uniform(length(Words) + 1 - N),
    string:substr(Words, N, S).

-endif.
