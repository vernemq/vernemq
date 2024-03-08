%% Copyright 2018 Erlio GmbH Basel Switzerland (http://erl.io)
%% Copyright 2018-2024 Octavo Labs/VerneMQ (https://vernemq.com/)
%% and Individual Contributors.
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
-include_lib("kernel/include/logger.hrl").

-export([
    new/1,
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
    get_changes/3,
    get_nodes/1,
    get_sessions/1,
    change_node_all/3,
    change_node/4
]).

-ifdef(TEST).
-export([
    subtract/2,
    sub_diff/2
]).
-endif.

-type node_subs() :: {node(), boolean(), [subscription()]}.
-type subs() :: [node_subs()].
-type node_changes() :: {node(), [subscription()]}.
-type changes() :: [node_changes()].

-export_type([subs/0, changes/0]).

-compile([nowarn_unused_function]).

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
    get_changes(subtract, Old, New).

-spec get_changes(atom(), subs(), subs()) -> {changes(), changes()}.
get_changes(subtract, Old, New) ->
    Removed = subtract(Old, New),
    Added = subtract(New, Old),

    {Removed, Added};
get_changes(one_pass_ordered, Old, New) ->
    {Removed, Added} = sub_diff(Old, New),
    {Removed, Added}.

-spec add(subs(), [subscription()]) -> {subs(), boolean()}.
add(Subs, TopicsWithQoS) ->
    add(Subs, TopicsWithQoS, node()).
add(Subs, TopicsWithQoS, Node) ->
    {OldNodeSubs, CleanSession, Present} = get_node_subs(Node, Subs),
    NewNodeSubs = lists:ukeymerge(1, lists:ukeysort(1, TopicsWithQoS), OldNodeSubs),
    case Present of
        true ->
            {
                lists:keyreplace(Node, 1, Subs, {Node, CleanSession, NewNodeSubs}),
                OldNodeSubs =/= NewNodeSubs
            };
        false ->
            {lists:keysort(1, [{Node, CleanSession, NewNodeSubs} | Subs]), true}
    end.

-spec exists(topic(), subs()) -> boolean().
exists(Topic, Subs) ->
    lists:any(
        fun({_, _, TopicsWithOpts}) ->
            lists:keymember(Topic, 1, TopicsWithOpts)
        end,
        Subs
    ).

-spec remove(subs(), [topic()]) -> {subs(), boolean()}.
remove(Subs, Topics) ->
    remove(Subs, Topics, node()).
remove(Subs, Topics, Node) ->
    {OldNodeSubs, CleanSession, Present} = get_node_subs(Node, Subs),
    case Present of
        true ->
            NewNodeSubs =
                lists:foldl(
                    fun(Topic, Acc) ->
                        lists:keydelete(Topic, 1, Acc)
                    end,
                    OldNodeSubs,
                    Topics
                ),
            {
                lists:keyreplace(Node, 1, Subs, {Node, CleanSession, NewNodeSubs}),
                OldNodeSubs =/= NewNodeSubs
            };
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
            %% TargetSubs and OldSubs are implicitly keysorted
            %% force clean_session=false if required
            NewNodeSubs = lists:ukeymerge(1, ExistingNewNodeSubs, OldNodeSubs),
            lists:keyreplace(
                NewNode,
                1,
                lists:keydelete(Node, 1, Subs),
                {NewNode, CleanSession and NewCleanSession, NewNodeSubs}
            );
        false ->
            lists:keysort(
                1,
                lists:keyreplace(
                    Node,
                    1,
                    Subs,
                    {NewNode, CleanSession, OldNodeSubs}
                )
            )
    end.

-spec change_node_all(subs(), node(), boolean()) -> {subs(), [node()]}.
change_node_all(Subs, NewNode, CleanSession) ->
    change_node_all(get_nodes(Subs), NewNode, Subs, CleanSession, []).

change_node_all([Node | Rest], NewNode, Subs, CleanSession, ChNodes) when Node =:= NewNode ->
    change_node_all(Rest, NewNode, Subs, CleanSession, ChNodes);
change_node_all([Node | Rest], NewNode, Subs, CleanSession, ChNodes) ->
    change_node_all(
        Rest,
        NewNode,
        change_node(Subs, Node, NewNode, CleanSession),
        CleanSession,
        [Node | ChNodes]
    );
change_node_all([], _, Subs, _, ChNodes) ->
    {Subs, ChNodes}.

%
% Expected two sorted lists and compares each element. Returns A -- B and B -- A.
%
list_diff(ListA, ListB) ->
    {AMinusB, BMinusA} = list_diff(ListA, ListB, [], []),
    {lists:reverse(AMinusB), lists:reverse(BMinusA)}.
list_diff([], [], AMinusB, BMinusA) ->
    {AMinusB, BMinusA};
list_diff([], [Y | T2], AMinusB, BMinusA) ->
    list_diff([], T2, AMinusB, [Y | BMinusA]);
list_diff([X | T1], [], AMinusB, BMinusA) ->
    list_diff(T1, [], [X | AMinusB], BMinusA);
list_diff([X | T1], [Y | T2], AMinusB, BMinusA) when X < Y ->
    list_diff(T1, [Y | T2], [X | AMinusB], BMinusA);
list_diff([X | T1], [Y | T2], AMinusB, BMinusA) when X > Y ->
    list_diff([X | T1], T2, AMinusB, [Y | BMinusA]);
list_diff([X | T1], [Y | T2], AMinusB, BMinusA) when X == Y ->
    list_diff(T1, T2, AMinusB, BMinusA).

%
% Exects a two lists of Subscriptions Subs1 und Subs2 as list of tuples [node, persistent, topic_list].
% The first list is considered the "old list" the seond one the new list. It returns a tuble where the
% first item includes all items that are in the first list and not in the second one, while the second tuple
% includes all items of the second list that are not in the first one.
%
% Precondition: all lists to be sorted!
%
sub_diff(Subs1, Subs2) ->
    sub_diff(Subs1, Subs2, [], []).
sub_diff([S | Subs1], [S | Subs2], Acc1, Acc2) ->
    %% same node subscriptions
    sub_diff(Subs1, Subs2, Acc1, Acc2);
sub_diff([{N, _, NSubs1} | Subs1], [{N, _, NSubs2} | Subs2], Acc1, Acc2) ->
    case list_diff(NSubs1, NSubs2) of
        {[], []} ->
            sub_diff(Subs1, Subs2, Acc1, Acc2);
        {NewNSubs1, []} ->
            sub_diff(Subs1, Subs2, [{N, NewNSubs1} | Acc1], Acc2);
        {[], NewNSubs2} ->
            sub_diff(Subs1, Subs2, Acc1, [{N, NewNSubs2} | Acc2]);
        {NewNSubs1, NewNSubs2} ->
            sub_diff(Subs1, Subs2, [{N, NewNSubs1} | Acc1], [{N, NewNSubs2} | Acc2])
    end;
% when N1 > N2 the whole N2 is a difference
sub_diff([{N1, _, _} | _] = Subs1, [{N2, _, NSubs2} | T2], Acc1, Acc2) when N1 > N2 ->
    sub_diff(Subs1, T2, Acc1, [{N2, NSubs2} | Acc2]);
% N1 < N2 whole N1 is a difference
sub_diff([{N, _, NSubs1} | Subs1], Subs2, Acc1, Acc2) ->
    sub_diff(Subs1, Subs2, [{N, NSubs1} | Acc1], Acc2);
%if the old list is empty, the whole remaining Sub2 is added
sub_diff([], [{N2, _, NSubs2} | T2], Acc1, Acc2) ->
    sub_diff([], T2, Acc1, [{N2, NSubs2} | Acc2]);
sub_diff([], [], Acc1, Acc2) ->
    {lists:reverse(Acc1), lists:reverse(Acc2)}.

%% returns only the Subscriptions of Subs1 that are not also
%% Subscriptions of Subs2. Assumes the subs are sorted by nodenames.
subtract(Subs1, Subs2) ->
    subtract(Subs1, Subs2, []).

subtract([S | Subs1], [S | Subs2], Acc) ->
    %% same node subscriptions
    subtract(Subs1, Subs2, Acc);
subtract([{N, _, NSubs1} | Subs1], [{N, _, NSubs2} | Subs2], Acc) ->
    %% same node with different node subscriptions
    % case {ordsets:is_set(NSubs1), ordsets:is_set(NSubs2)} of
    %    {true, true} -> ok;
    %    _ -> ?LOG_INFO("!!!!!!!!!!!!!!!!!!!!! Unfortunately not ordered!")
    %end,
    case NSubs1 -- NSubs2 of
        [] ->
            subtract(Subs1, Subs2, Acc);
        NewNSubs ->
            subtract(Subs1, Subs2, [{N, NewNSubs} | Acc])
    end;
subtract([{N1, _, _} | _] = Subs1, [{N2, _, _} | T2], Acc) when N1 > N2 ->
    subtract(Subs1, T2, Acc);
subtract([{N, _, NSubs1} | Subs1], Subs2, Acc) ->
    subtract(Subs1, Subs2, [{N, NSubs1} | Acc]);
subtract([], _, Acc) ->
    lists:reverse(Acc).

get_nodes(Subs) ->
    lists:foldl(fun({N, _, _}, Acc) -> [N | Acc] end, [], Subs).

get_sessions(Subs) ->
    lists:foldl(fun({N, C, _}, Acc) -> [{N, C} | Acc] end, [], Subs).

get_node_subs(Node, Subs) ->
    case lists:keyfind(Node, 1, Subs) of
        false -> {[], true, false};
        {_, CleanSession, NSubs} -> {NSubs, CleanSession, true}
    end.

-spec fold(function(), any(), subs() | changes()) -> any().
fold(Fun, Acc, Subs) ->
    lists:foldl(
        fun
            ({Node, _, NSubs}, AccAcc) ->
                lists:foldl(
                    fun({Topic, SubInfo}, AccAccAcc) ->
                        Fun({Topic, SubInfo, Node}, AccAccAcc)
                    end,
                    AccAcc,
                    NSubs
                );
            ({Node, NChanges}, AccAcc) ->
                lists:foldl(
                    fun({Topic, SubInfo}, AccAccAcc) ->
                        Fun({Topic, SubInfo, Node}, AccAccAcc)
                    end,
                    AccAcc,
                    NChanges
                )
        end,
        Acc,
        Subs
    ).
