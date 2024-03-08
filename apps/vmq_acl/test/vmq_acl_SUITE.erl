%%%-------------------------------------------------------------------
-module(vmq_acl_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("eunit/include/eunit.hrl").

init_per_suite(Config) ->
    cover:start(),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    vmq_acl:init(),
    Config.
    
end_per_testcase(_, Config) ->
    vmq_acl:teardown(),
    Config.
    
all() ->
    [simple_acl_list, 
     simple_acl_file,
     spaces_acl_list,
     utf8_simple].

utf8_simple(_) ->
    vmq_acl:load_from_file(path(<<"utf8_test.acl">>)),
    [[{[<<"машина"/utf8>>, <<"скорость"/utf8>>], 1}]] = ets:match(vmq_acl_read_all, '$1').
        
simple_acl_file(_) ->
    vmq_acl:load_from_file(path(<<"simple_test.acl">>)),
    [[{[<<"a">>, <<"b">>, <<"c">>], 1}]] = ets:match(vmq_acl_read_all, '$1'),
    [[{[<<"a">>,<<"b">>,<<"c">>],1}],[{[<<"v">>,<<"w x">>,<<"y">>],1}]] = lists:sort(ets:match(vmq_acl_write_all, '$1')),
    [[{{<<"test">>, [<<"x">>, <<"y">>, <<"z">>, <<"#">>]}, 1}]] = ets:match(vmq_acl_read_user, '$1'),
    [[{{<<"test">>, [<<"x">>, <<"y">>, <<"z">>, <<"#">>]}, 1}]] = ets:match(vmq_acl_write_user, '$1'),
    [[{[<<"%m">>, <<"%u">>, <<"%c">>], 1}]] = ets:match(vmq_acl_read_pattern, '$1'),
    [[{[<<"%m">>, <<"%u">>, <<"%c">>], 1}]] = ets:match(vmq_acl_write_pattern, '$1'),
    ok = vmq_acl:auth_on_subscribe(
            <<"test">>,
            {"", <<"my-client-id">>},
            [
                {[<<"a">>, <<"b">>, <<"c">>], 0},
                {[<<"x">>, <<"y">>, <<"z">>, <<"#">>], 0},
                {[<<"">>, <<"test">>, <<"my-client-id">>], 0}
            ]
        ),
    next = 
        vmq_acl:auth_on_subscribe(
            <<"invalid-user">>,
            {"", <<"my-client-id">>},
            [
                {[<<"a">>, <<"b">>, <<"c">>], 0},
                {[<<"x">>, <<"y">>, <<"z">>, <<"#">>], 0},
                {[<<"">>, <<"test">>, <<"my-client-id">>], 0}
            ]
        ),
        ok =
        vmq_acl:auth_on_publish(
            <<"test">>,
            {"", <<"my-client-id">>},
            1,
            [<<"a">>, <<"b">>, <<"c">>],
            <<"payload">>,
            false
        ),
        ok =
        vmq_acl:auth_on_publish(
            <<"test">>,
            {"", <<"my-client-id">>},
            1,
            [<<"x">>, <<"y">>, <<"z">>, <<"blabla">>],
            <<"payload">>,
            false
        ), 
        ok =
        vmq_acl:auth_on_publish(
            <<"test">>,
            {"", <<"my-client-id">>},
            1,
            [<<"">>, <<"test">>, <<"my-client-id">>],
            <<"payload">>,
            false
        ),
        next = 
        vmq_acl:auth_on_publish(
            <<"invalid-user">>,
            {"", <<"my-client-id">>},
            1,
            [<<"x">>, <<"y">>, <<"z">>, <<"blabla">>],
            <<"payload">>,
            false
        ),
        next = 
        vmq_acl:auth_on_publish(
            <<"invalid-user">>,
            {"", <<"my-client-id">>},
            1,
            [<<"">>, <<"test">>, <<"my-client-id">>],
            <<"payload">>,
            false
        ).
 
simple_acl_list(_) ->
    ACL = [
        <<"# simple comment\n">>,
        <<"topic read a/b/c\n">>,
        <<"# other comment \n">>,
        <<"topic write a/b/c\n">>,
        <<"topic write x/y/z\n">>,
        <<"\n">>,
        <<"#\n">>,
        <<"# ACL for user 'test'\n">>,
        <<"user test\n">>,
        <<"\n">>,
        <<"\n">>,
        <<"##\n">>,
        <<"topic x/y/z/#\n">>,
        <<"# some patterns\n">>,
        <<"pattern read %m/%u/%c\n">>,
        <<"pattern write %m/%u/%c">>
    ],
 
    vmq_acl:load_from_list(ACL),
        [[{[<<"a">>, <<"b">>, <<"c">>], 1}]] = ets:match(vmq_acl_read_all, '$1'),
        [[{[<<"a">>,<<"b">>,<<"c">>],1}],[{[<<"x">>,<<"y">>,<<"z">>],1}]] = lists:sort(ets:match(vmq_acl_write_all, '$1')),
        [[{{<<"test">>, [<<"x">>, <<"y">>, <<"z">>, <<"#">>]}, 1}]] = ets:match(vmq_acl_read_user, '$1'),
        [[{{<<"test">>, [<<"x">>, <<"y">>, <<"z">>, <<"#">>]}, 1}]] = ets:match(vmq_acl_write_user, '$1'),
        [[{[<<"%m">>, <<"%u">>, <<"%c">>], 1}]] = ets:match(vmq_acl_read_pattern, '$1'),
        [[{[<<"%m">>, <<"%u">>, <<"%c">>], 1}]] = ets:match(vmq_acl_write_pattern, '$1'),
        %% positive auth_on_subscribe
        ok = vmq_acl:auth_on_subscribe(
                <<"test">>,
                {"", <<"my-client-id">>},
                [
                    {[<<"a">>, <<"b">>, <<"c">>], 0},
                    {[<<"x">>, <<"y">>, <<"z">>, <<"#">>], 0},
                    {[<<"">>, <<"test">>, <<"my-client-id">>], 0}
                ]
            ),
        next = 
            vmq_acl:auth_on_subscribe(
                <<"invalid-user">>,
                {"", <<"my-client-id">>},
                [
                    {[<<"a">>, <<"b">>, <<"c">>], 0},
                    {[<<"x">>, <<"y">>, <<"z">>, <<"#">>], 0},
                    {[<<"">>, <<"test">>, <<"my-client-id">>], 0}
                ]
            ),
        ok =
        vmq_acl:auth_on_publish(
            <<"test">>,
            {"", <<"my-client-id">>},
            1,
            [<<"a">>, <<"b">>, <<"c">>],
            <<"payload">>,
            false
        ),
        ok =
        vmq_acl:auth_on_publish(
            <<"test">>,
            {"", <<"my-client-id">>},
            1,
            [<<"x">>, <<"y">>, <<"z">>, <<"blabla">>],
            <<"payload">>,
            false
        ), 
        ok =
        vmq_acl:auth_on_publish(
            <<"test">>,
            {"", <<"my-client-id">>},
            1,
            [<<"">>, <<"test">>, <<"my-client-id">>],
            <<"payload">>,
            false
        ),
        next = 
        vmq_acl:auth_on_publish(
            <<"invalid-user">>,
            {"", <<"my-client-id">>},
            1,
            [<<"x">>, <<"y">>, <<"z">>, <<"blabla">>],
            <<"payload">>,
            false
        ),
        next = 
        vmq_acl:auth_on_publish(
            <<"invalid-user">>,
            {"", <<"my-client-id">>},
            1,
            [<<"">>, <<"test">>, <<"my-client-id">>],
            <<"payload">>,
            false
        ).
    
spaces_acl_list(_) ->
    ACL = [
        <<"topic read  a/b/c \n">>,
        <<"topic write   a/b/c\n">>,
        <<"topic write  x/y/z  \n">>,
        <<"\n">>,
        <<"user    test   \n">>,
        <<"topic  x/y/z/# \n">>
    ],
    
    vmq_acl:load_from_list(ACL),
        [[{[<<"a">>, <<"b">>, <<"c">>], 1}]] = ets:match(vmq_acl_read_all, '$1'),
        [[{[<<"a">>,<<"b">>,<<"c">>],1}],[{[<<"x">>,<<"y">>,<<"z">>],1}]] = lists:sort(ets:match(vmq_acl_write_all, '$1')),
        [[{{<<"test">>, [<<"x">>, <<"y">>, <<"z">>, <<"#">>]}, 1}]] =  ets:match(vmq_acl_read_user, '$1'),
        [[{{<<"test">>, [<<"x">>, <<"y">>, <<"z">>, <<"#">>]}, 1}]] = ets:match(vmq_acl_write_user, '$1').
            
path(File) ->
    Path = filename:dirname(
        proplists:get_value(source, ?MODULE:module_info(compile))),
    filename:join([Path, "acl", File]).