-module(vmq_subscriber_SUITE).

-compile(export_all).
-compile(nowarn_export_all).
-include_lib("stdlib/include/assert.hrl").
-include("vmq_server.hrl").

init_per_suite(_Config) ->
    cover:start(),
    _Config.

end_per_suite(_Config) ->
    _Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_, Config) ->
    Config.

all() ->
    [
        subtract_test,
        get_changes_test,
        get_changes_2_test,
        get_changes_3_test,
        get_changes_4_test,
        change_node_test,
        change_node_all_test,
        add_subscription_test,
        remove_subscription_test
    ].
-define(t(S), [list_to_binary(S)]).

subtract_test(_) ->
    A1 = [
        {node_a, true, [
            {[<<"a">>], 0},
            {[<<"b">>], 1},
            {[<<"c">>], 2}
        ]},
        {node_b, true, [
            {[<<"d">>], 1},
            {[<<"e">>], 2}
        ]}
    ],
    A2 = [
        {node_a, true, [
            {[<<"c">>], 2},
            {[<<"f">>], 2}
        ]},
        {node_b, true, [
            {[<<"d">>], 1},
            {[<<"e">>], 2}
        ]}
    ],
    B1 = [
        {node_a, true, [
            {[<<"b">>], 1},
            {[<<"c">>], 2}
        ]},
        {node_b, true, [
            {[<<"e">>], 2}
        ]}
    ],
    B2 = [
        {node_a, true, []},
        {node_b, true, []}
    ],
    [
        {node_a, [{[<<"a">>], 0}]},
        {node_b, [{[<<"d">>], 1}]}
    ] = vmq_subscriber:subtract(A1, B1),
    [
        {node_a, [{[<<"a">>], 0}]},
        {node_b, [{[<<"d">>], 1}]}
    ] = vmq_subscriber:subtract(A1, B1),
    [] = vmq_subscriber:subtract(B1, A1),
    [
        {node_a, [
            {[<<"a">>], 0},
            {[<<"b">>], 1}
        ]}
    ] = vmq_subscriber:subtract(A1, A2),
    [{node_a, [{[<<"f">>], 2}]}] = vmq_subscriber:subtract(A2, A1),
    {[          {node_a, [
            {[<<"a">>], 0},
            {[<<"b">>], 1}
        ]}
    ],            [
          {node_a, [{[<<"f">>], 2}]}
        ]} = 
        vmq_subscriber:sub_diff(A1, A2),
        {[],            [
            {node_a, [{[<<"a">>], 0}]},
            {node_b, [{[<<"d">>], 1}]}
        ]} = vmq_subscriber:sub_diff(B1, A1),
    {[
            {node_a, [{[<<"a">>], 0}]},
            {node_b, [{[<<"d">>], 1}]}
        ], []} = 
        vmq_subscriber:sub_diff(A1, B1),
            [
                {node_a, [
                    {[<<"a">>], 0},
                    {[<<"b">>], 1},
                    {[<<"c">>], 2}
                ]},
                {node_b, [
                    {[<<"d">>], 1},
                    {[<<"e">>], 2}
                ]}
            ] = 
            vmq_subscriber:subtract(A1, B2),
            {[
                {node_a, [
                    {[<<"a">>], 0},
                    {[<<"b">>], 1},
                    {[<<"c">>], 2}
                ]},
                {node_b, [
                    {[<<"d">>], 1},
                    {[<<"e">>], 2}
                ]}
            ], []
        } = vmq_subscriber:sub_diff(A1, B2),
        [] = vmq_subscriber:subtract(A1, A1),
        {[], []} = vmq_subscriber:sub_diff(A1, A1)
        .

get_changes_test(_) ->
    Old = [{node_a, true, [{a, 1}, {b, 1}]}],
    New = [{node_a, true, [{b, 1}]}],
        {
            % Removed
            [{node_a, [{a, 1}]}],
            % Added
            []
        } = 
        vmq_subscriber:get_changes(Old, New)
    .

get_changes_2_test(_) ->
    Old = [{node_a, true, [{a, 1}]}, {node_b, true, [{b, 1}]}],
    New = [{node_a, true, [{a, 1}]}, {node_c, true, [{c, 1}]}],
    {
        % Removed
        [{node_b, [{b, 1}]}],
        % Added
        [{node_c, [{c, 1}]}]
    } = vmq_subscriber:get_changes(Old, New).

get_changes_3_test(_) ->
    Old = [{node_a, true, [{a, 1}]}, {node_b, true, [{b, 1}]}, {node_c, true, [{c, 1}]}],
    New = [{node_b, true, [{b, 1}]}],
    {
        % Removed
        [{node_a, [{a, 1}]}, {node_c, [{c, 1}]}],
        % Added
        []
    } = vmq_subscriber:get_changes(Old, New).

get_changes_4_test(_) ->
    Old = [{node_b, true, [{b, 1}]}, {node_c, true, [{c, 1}]}],
    New = [{node_a, true, [{a, 1}]}, {node_c, true, [{c, 1}]}],
    {
        % Removed
        [{node_b, [{b, 1}]}],
        % Added
        [{node_a, [{a, 1}]}]
    } = vmq_subscriber:get_changes(Old, New).

change_node_test(_) ->
    Subs = [
        {node_a, false, [{a, 1}, {b, 1}]},
        {node_b, false, [{c, 2}]}
    ],
    ?assertEqual(
        [{node_b, false, [{a, 1}, {b, 1}, {c, 2}]}],
        vmq_subscriber:change_node(Subs, node_a, node_b, false)
    ).

change_node_all_test(_) ->
    Subs = [
        {node_a, false, [{a, 1}, {b, 1}]},
        {node_b, false, [{b, 2}, {c, 2}]}
    ],
    ?assertEqual(
        {[{node_c, false, [{a, 1}, {b, 2}, {c, 2}]}], [node_a, node_b]},
        vmq_subscriber:change_node_all(Subs, node_c, false)
    ).

add_subscription_test(_) ->
    [
        ?assertEqual({[{node(), true, [{a, 1}, {b, 2}]}], true}, vmq_subscriber:add(vmq_subscriber:new(true), [{a, 1}, {b, 2}])),
        ?assertEqual(
            {[{node(), true, [{a, 1}, {b, 2}]}], true}, vmq_subscriber:add(vmq_subscriber:new(true, [{a, 1}]), [{b, 2}])
        ),
        ?assertEqual(
            {[{node(), true, [{a, 1}, {b, 2}]}], true}, vmq_subscriber:add(vmq_subscriber:new(true, [{a, 1}, {b, 1}]), [{b, 2}])
        ),
        ?assertEqual(
            {[{node(), true, [{a, 1}, {b, 2}]}], false}, vmq_subscriber:add(vmq_subscriber:new(true, [{a, 1}, {b, 2}]), [{b, 2}])
        )
    ].

remove_subscription_test(_) ->
    X = node(),
    {[{X, true, []}], false} = vmq_subscriber:remove(vmq_subscriber:new(true), [a]),
    {[{X, true, []}], true} =  vmq_subscriber:remove(vmq_subscriber:new(true, [{a, 1}]), [a]),
    {[{X, true, [{b, 2}]}], true} = vmq_subscriber:remove(vmq_subscriber:new(true, [{a, 1}, {b, 2}]), [a]).
