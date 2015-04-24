-module(vmq_rate_limiter_SUITE).
-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([publish_rate_limit_test/1]).

-export([hook_auth_on_register/5,
         hook_auth_on_publish/6]).

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(_Config) ->
    cover:start(),
    _Config.

end_per_suite(_Config) ->
    _Config.

init_per_testcase(_Case, Config) ->
    vmq_test_utils:setup(),
    vmq_server_cmd:set_config(retry_interval, 10),
    vmq_server_cmd:set_config(allow_anonymous, false),
    vmq_server_cmd:listener_start(1888, []),
    Config.

end_per_testcase(_, Config) ->
    vmq_test_utils:teardown(),
    Config.

all() ->
    [publish_rate_limit_test].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
publish_rate_limit_test(_) ->
    Connect = packet:gen_connect("rate-limit-test", [{keepalive, 60}]),
    Connack = packet:gen_connack(0),
    Pub = fun(Socket, Id) ->
                  Publish = packet:gen_publish("rate/limit/test", 1,
                                               <<"message">>, [{mid, Id}]),
                  Puback = packet:gen_puback(Id),
                  ok = gen_tcp:send(Socket, Publish),
                  ok = packet:expect_packet(Socket, "puback", Puback)
          end,
    enable_hooks(),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    % sending 10 publishes should take us at least 10 seconds
    {T, _} = timer:tc(fun() ->
                              _ = [Pub(Socket, I) || I <- lists:seq(0, 10)]
                      end),
    ct:pal("time passed ~p", [T]),
    10 = T div 1000000,
    disable_hooks(),
    ok = gen_tcp:close(Socket).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks (as explicit as possible)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
hook_auth_on_register(_Peer, {"", "rate-limit-test"}, _User, _Password, _Clean) ->
    %% this will limit the publisher to 1 message/sec
    {ok, [{max_message_rate, 1}]}.

hook_auth_on_publish(_, {"", "rate-limit-test"}, _MsgId, _, _, _) -> ok.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Helper
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
enable_hooks() ->
    vmq_plugin_mgr:enable_module_plugin(
      auth_on_register, ?MODULE, hook_auth_on_register, 5),
    vmq_plugin_mgr:enable_module_plugin(
      auth_on_publish, ?MODULE, hook_auth_on_publish, 6).
disable_hooks() ->
    vmq_plugin_mgr:disable_module_plugin(
      auth_on_register, ?MODULE, hook_auth_on_register, 5),
    vmq_plugin_mgr:disable_module_plugin(
      auth_on_publish, ?MODULE, hook_auth_on_publish, 6).
