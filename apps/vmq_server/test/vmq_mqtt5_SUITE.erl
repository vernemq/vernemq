-module(vmq_mqtt5_SUITE).

-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([anon_success_test/1]).

-include_lib("common_test/include/ct.hrl").

init_per_suite(Config) ->
    cover:start(),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    vmq_test_utils:setup(),
    vmq_server_cmd:set_config(allow_anonymous, false),
    vmq_server_cmd:set_config(max_client_id_size, 23),
    vmq_server_cmd:listener_start(1888, []),
    Config.

end_per_testcase(_TestCase, _Config) ->
    vmq_test_utils:teardown(),
    ok.

all() ->
    [anon_success_test].

anon_success_test(_Config) ->
    vmq_server_cmd:set_config(allow_anonymous, true),
    vmq_config:configure_node(),
    %% allow_anonymous is proxied through vmq_config.erl
    Connect = vmq_parser_mqtt5:gen_connect("connect-success-test", [{keepalive,10}]),
    Connack = vmq_parser_mqtt5:gen_connack(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    ok = gen_tcp:close(Socket).
