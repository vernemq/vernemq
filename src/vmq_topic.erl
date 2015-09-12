%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% Developer of the eMQTT Code is <ery.lee@gmail.com>
%% Copyright (c) 2012 Ery Lee.  All rights reserved.
%%
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
         unword/1,
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

unword([Word|_] = Topic) when is_binary(Word) ->
    reverse(unword(Topic, []));
unword(Topic) when is_binary(Topic) ->
    Topic;
unword(undefined) -> undefined;
unword(empty) -> empty.

unword([<<>>], []) -> [$/];
unword([<<>>], Acc) -> Acc;
unword([], Acc) -> [$/|Acc];
unword([Word], Acc) -> [Word|Acc];
unword([<<>>|Topic], Acc) ->
    unword(Topic, [$/|Acc]);
unword([Word|Rest], Acc) ->
    unword(Rest, [$/, Word|Acc]).

validate_topic(_Type, <<>>) ->
    {error, no_empty_topic_allowed};
validate_topic(_Type, Topic) when byte_size(Topic) > ?MAX_LEN ->
    {error, subscribe_topic_too_long};
validate_topic(Type, Topic) ->
    validate_topic(Type, Topic, 0, []).

validate_topic(Type, Topic, L, Acc) ->
    case Topic of
        <<"+/", Rest/binary>> ->
            validate_topic_word(Type, <<"+">>, Rest, Acc);
        <<"+">> = W ->
            validate_topic_word(Type, W, undefined, Acc);
        <<"#">> = W ->
            validate_topic_word(Type, W, undefined, Acc);
        <<Word:L/binary, "/", Rest/binary>> ->
            validate_topic_word(Type, Word, Rest, Acc);
        <<Word:L/binary>> ->
            validate_topic_word(Type, Word, undefined, Acc);
        <<_:L/binary, "+", _/binary>> ->
            {error, 'no_+_allowed_in_word'};
        <<_:L/binary, "#", _/binary>> ->
            {error, 'no_#_allowed_in_word'};
        _ ->
            validate_topic(Type, Topic, L + 1, Acc)
    end.

validate_topic_word(publish, <<"+">>, _, _) ->
    {error, 'no_+_allowed_in_publish'};
validate_topic_word(publish, <<"#">>, _, _) ->
    {error, 'no_#_allowed_in_publish'};
validate_topic_word(subscribe, Word = <<"#">>, undefined, Acc) ->
    {ok, reverse([Word|Acc])};
validate_topic_word(subscribe, <<"#">>, _, _) ->
    {error, 'no_#_allowed_in_subscribe'};
validate_topic_word(_, Word, <<>>, Acc) ->
    {ok, reverse([<<>>, Word|Acc])};
validate_topic_word(_, Word, undefined, Acc) ->
    {ok, reverse([Word|Acc])};
validate_topic_word(Type, Word, Rest, Acc) ->
    validate_topic(Type, Rest, 0, [Word|Acc]).




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

validate_unword_test() ->
    {A,B,C} = now(),
    random:seed(A, B, C),
    random_topics(1000),
    ok.

random_topics(0) -> ok;
random_topics(N) when N > 0 ->
    NWords = random:uniform(100),
    Words =
    lists:foldl(fun(_, AAcc) ->
                    case random:uniform(3) of
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
    N = random:uniform(length(Words)),
    S = random:uniform(length(Words) + 1 - N),
    string:substr(Words, N, S).

-endif.
