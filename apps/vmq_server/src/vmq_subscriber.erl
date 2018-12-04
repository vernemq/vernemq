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

-module(vmq_subscriber).
-include("vmq_server.hrl").

-export([new/1,
         new/2,
         new/3,
         add/2,
         add/3,
         exists/2,
         remove/2,
         remove/3,
         fold/3,
         get_changes/1,
         get_changes/2,
         get_nodes/1,
         get_sessions/1,
         change_node_all/3,
         change_node/4,
         check_format/1]).

-type node_subs() :: {node(), boolean(), [subscription()]}.
-type subs() :: [node_subs()].
-type node_changes() :: {node(), [subscription()]}.
-type changes() :: [node_changes()].

-export_type([subs/0, changes/0]).

-spec new(boolean()) -> subs().
new(CleanSession) ->
    new(CleanSession, []).
new(CleanSession, TopicsWithQoS) ->
    new(CleanSession, TopicsWithQoS, node()).
new(CleanSession, TopicsWithQoS, Node) ->
    [{Node, CleanSession, TopicsWithQoS}].

-spec get_changes(subs()) -> changes().
get_changes(New) ->
    [{N, NSubs} || {N, _, NSubs} <- New].

-spec get_changes(subs(), subs()) -> {changes(), changes()}.
get_changes(Old, New) ->
    Removed = subtract(Old, New),
    Added = subtract(New, Old),
    {Removed, Added}.

-spec add(subs(), [subscription()]) -> {subs(), boolean()}.
add(Subs, TopicsWithQoS) ->
    add(Subs, TopicsWithQoS, node()).
add(Subs, TopicsWithQoS, Node) ->
    {OldNodeSubs, CleanSession, Present} = get_node_subs(Node, Subs),
    NewNodeSubs = lists:ukeymerge(1, lists:ukeysort(1, TopicsWithQoS), OldNodeSubs),
    case Present of
        true ->
            {lists:keyreplace(Node, 1, Subs, {Node, CleanSession, NewNodeSubs}),
             OldNodeSubs =/= NewNodeSubs};
        false ->
            {lists:keysort(1, [{Node, CleanSession, NewNodeSubs}|Subs]), true}
    end.

-spec exists(topic(), subs()) -> boolean().
exists(Topic, Subs) ->
    lists:any(fun({_, _, TopicsWithOpts}) ->
                      lists:keymember(Topic, 1, TopicsWithOpts)
              end, Subs).

-spec remove(subs(), [topic()]) -> {subs(), boolean()}.
remove(Subs, Topics) ->
    remove(Subs, Topics, node()).
remove(Subs, Topics, Node) ->
    {OldNodeSubs, CleanSession, Present} = get_node_subs(Node, Subs),
    case Present of
        true ->
            NewNodeSubs =
            lists:foldl(fun(Topic, Acc) ->
                                lists:keydelete(Topic, 1, Acc)
                        end, OldNodeSubs, Topics),
            {lists:keyreplace(Node, 1, Subs, {Node, CleanSession, NewNodeSubs}),
             OldNodeSubs =/= NewNodeSubs};
        false ->
            {Subs, false}
    end.

-spec change_node(subs(), node(), node(), boolean()) -> subs().
change_node(Subs, Node, NewNode, CleanSession) ->
    {OldNodeSubs, OldCleanSession, _} = get_node_subs(Node, Subs),
    {ExistingNewNodeSubs, NewCleanSession, NewNodePresent} = get_node_subs(NewNode, Subs),
    case NewNodePresent of
        true when OldCleanSession ->
            %% we don't require the OldNodeSubs
            lists:keydelete(Node, 1, Subs);
        true ->
            %% Remove duplicate subscriptions, ensure the subscription present
            %% on the target node remains untouched in case of duplicate subs.
            %% TargetSubs and OldSubs are implicitely keysorted
            %% force clean_session=false if required
            NewNodeSubs = lists:ukeymerge(1, ExistingNewNodeSubs, OldNodeSubs),
            lists:keyreplace(NewNode, 1, lists:keydelete(Node, 1, Subs),
                             {NewNode, CleanSession and NewCleanSession, NewNodeSubs});
        false ->
            lists:keysort(1, lists:keyreplace(Node, 1, Subs,
                                              {NewNode, CleanSession, OldNodeSubs}))
    end.

-spec change_node_all(subs(), node(), boolean()) -> {subs(), [node()]}.
change_node_all(Subs, NewNode, CleanSession) ->
    change_node_all(get_nodes(Subs), NewNode, Subs, CleanSession, []).

change_node_all([Node|Rest], NewNode, Subs, CleanSession, ChNodes) when Node =:= NewNode ->
    change_node_all(Rest, NewNode, Subs, CleanSession, ChNodes);
change_node_all([Node|Rest], NewNode, Subs, CleanSession, ChNodes) ->
   change_node_all(Rest, NewNode, change_node(Subs, Node, NewNode, CleanSession),
                   CleanSession, [Node|ChNodes]);
change_node_all([], _, Subs, _, ChNodes) ->
    {Subs, ChNodes}.

-spec check_format(any()) -> subs().
check_format(Subs0) ->
    maybe_convert_v0(Subs0).

%% @doc convert deprecated subscription format to current format (v1). The
%% new format was introduced in VerneMQ 0.15.1.
-spec maybe_convert_v0(any()) -> subs().
maybe_convert_v0([{Topic,_,_}|_] = Version0Subs) when is_list(Topic) ->
    %% Per default converted subscriptions use initially clean session=false,
    %% because we don't know better, and it will be subsequentially adjusted
    %% anyways.
    maybe_convert_v0(Version0Subs, new(false));
maybe_convert_v0(Subs) -> Subs.

maybe_convert_v0([{Topic, QoS, Node}|Version0Subs], NewStyleSubs) ->
    {NewSubs, _} = add(NewStyleSubs, [{Topic, QoS}], Node),
    maybe_convert_v0(Version0Subs, NewSubs);
maybe_convert_v0([], NewStyleSubs) -> NewStyleSubs.

%% returns only the Subscriptions of Subs1 that are not also
%% Subscriptions of Subs2. Assumes the subs are sorted by nodenames.
subtract(Subs1, Subs2) ->
    subtract(Subs1, Subs2, []).

subtract([S|Subs1], [S|Subs2], Acc) ->
    %% same node subscriptions
    subtract(Subs1, Subs2, Acc);
subtract([{N, _, NSubs1}|Subs1], [{N, _, NSubs2}|Subs2], Acc) ->
    %% same node with different node subscriptions
    case NSubs1 -- NSubs2 of
        [] ->
            subtract(Subs1, Subs2, Acc);
        NewNSubs ->
            subtract(Subs1, Subs2, [{N, NewNSubs}|Acc])
    end;
subtract([{N1, _, _}|_] = Subs1, [{N2,_,_}|T2], Acc) when N1 > N2->
    subtract(Subs1, T2, Acc);
subtract([{N, _, NSubs1}|Subs1], Subs2, Acc) ->
    subtract(Subs1, Subs2, [{N, NSubs1}|Acc]);
subtract([], _, Acc) -> lists:reverse(Acc).


get_nodes(Subs) ->
    lists:foldl(fun({N, _, _}, Acc) -> [N|Acc] end, [], Subs).

get_sessions(Subs) ->
    lists:foldl(fun({N, C, _}, Acc) -> [{N, C}|Acc] end, [], Subs).

get_node_subs(Node, Subs) ->
    case lists:keyfind(Node, 1, Subs) of
        false -> {[], true, false};
        {_, CleanSession, NSubs} -> {NSubs, CleanSession, true}
    end.

-spec fold(function(), any(), subs() | changes()) -> any().
fold(Fun, Acc, Subs) ->
    lists:foldl(fun({Node, _, NSubs}, AccAcc) ->
                        lists:foldl(
                          fun({Topic, SubInfo}, AccAccAcc) ->
                                  Fun({Topic, SubInfo, Node}, AccAccAcc)
                          end, AccAcc, NSubs);
                   ({Node, NChanges}, AccAcc) ->
                        lists:foldl(
                          fun({Topic, SubInfo}, AccAccAcc) ->
                                  Fun({Topic, SubInfo, Node}, AccAccAcc)
                          end, AccAcc, NChanges)
                end, Acc, Subs).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(t(S),[list_to_binary(S)]).

subtract_test_() ->
    A = [{node_a, true, [
                         {[<<"a">>], 0},
                         {[<<"b">>], 1},
                         {[<<"c">>], 2}
                        ]},
         {node_b, true, [
                          {[<<"d">>], 1},
                          {[<<"e">>], 2}
                         ]}
        ],
    B1 = [{node_a, true, [
                          {[<<"b">>], 1},
                          {[<<"c">>], 2}
                         ]},
          {node_b, true, [
                          {[<<"e">>], 2}
                         ]}
         ],
    B2 = [{node_a, true, []},
          {node_b, true, []}
         ],

    [
     ?_assertEqual([{node_a, [{[<<"a">>], 0}]},
                    {node_b, [{[<<"d">>], 1}]}], subtract(A, B1)),
     ?_assertEqual([{node_a, [{[<<"a">>], 0},
                              {[<<"b">>], 1},
                              {[<<"c">>], 2}]},
                    {node_b, [{[<<"d">>], 1},
                              {[<<"e">>], 2}]}], subtract(A, B2)),
     ?_assertEqual([], subtract(A, A))
    ].

get_changes_test_() ->
    Old = [{node_a, true, [{a,1},{b,1}]}],
    New = [{node_a, true, [{b,1}]}],
    ?_assertEqual({
       % Removed
       [{node_a, [{a,1}]}],
       % Added
       []
      }, get_changes(Old, New)).

get_changes_2_test_() ->
    Old = [{node_a, true, [{a,1}]}, {node_b, true, [{b,1}]}],
    New = [{node_a, true, [{a,1}]}, {node_c, true, [{c,1}]}],
    ?_assertEqual({
       % Removed
       [{node_b, [{b,1}]}],
       % Added
       [{node_c, [{c,1}]}]
      }, get_changes(Old, New)).


get_changes_3_test_() ->
    Old = [{node_a, true, [{a,1}]}, {node_b, true, [{b,1}]}, {node_c, true, [{c,1}]}],
    New = [{node_b, true, [{b,1}]}],
    ?_assertEqual({
       % Removed
       [{node_a, [{a,1}]}, {node_c, [{c, 1}]}],
       % Added
       []
      }, get_changes(Old, New)).

get_changes_4_test_() ->
    Old = [{node_b, true, [{b,1}]}, {node_c, true, [{c,1}]}],
    New = [{node_a, true, [{a,1}]}, {node_c, true, [{c,1}]}],
    ?_assertEqual({
       % Removed
       [{node_b, [{b,1}]}],
       % Added
       [{node_a, [{a,1}]}]
      }, get_changes(Old, New)).

change_node_test_() ->
    Subs = [{node_a, false, [{a,1},{b,1}]},
            {node_b, false, [{c,2}]}],
     ?_assertEqual([{node_b, false, [{a,1},{b,1},{c,2}]}],
                   change_node(Subs, node_a, node_b, false)).

change_node_all_test_() ->
    Subs = [{node_a, false, [{a,1},{b,1}]},
            {node_b, false, [{b,2},{c,2}]}],
     ?_assertEqual({[{node_c, false, [{a,1},{b,2},{c,2}]}], [node_a, node_b]},
                   change_node_all(Subs, node_c, false)).

add_subscription_test_() ->
    [
    ?_assertEqual({[{node(), true, [{a,1}, {b,2}]}], true}, add(new(true), [{a,1}, {b,2}])),
    ?_assertEqual({[{node(), true, [{a,1}, {b,2}]}], true}, add(new(true, [{a,1}]), [{b,2}])),
    ?_assertEqual({[{node(), true, [{a,1}, {b,2}]}], true}, add(new(true, [{a,1}, {b,1}]), [{b,2}])),
    ?_assertEqual({[{node(), true, [{a,1}, {b,2}]}], false}, add(new(true, [{a,1}, {b,2}]), [{b,2}]))
    ].

remove_subscription_test_() ->
    [
    ?_assertEqual({[{node(), true, []}], false}, remove(new(true), [a])),
    ?_assertEqual({[{node(), true, []}], true}, remove(new(true, [{a,1}]), [a])),
    ?_assertEqual({[{node(), true, [{b,2}]}], true}, remove(new(true, [{a,1},{b,2}]), [a]))
    ].

maybe_convert_v0_test_() ->
    Version0Subs = [{?t("a"), 0, node_a}, {?t("b"), 1, node_b}, {?t("c"), 2, node_c}],
    Subs = [{node_a, true, [{?t("a"), 0}]},
            {node_b, true, [{?t("b"), 1}]},
            {node_c, true, [{?t("c"), 2}]},
            {node(), false, []}],
    ?_assertEqual(Subs, maybe_convert_v0(Version0Subs)).
-endif.
