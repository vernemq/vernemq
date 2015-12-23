-module(vmq_multiple_sessions_SUITE).
-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([multiple_sessions_test/1,
         multiple_balanced_sessions_test/1]).

-export([hook_auth_on_subscribe/3,
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
    vmq_server_cmd:set_config(allow_anonymous, true),
    vmq_server_cmd:set_config(allow_multiple_sessions, true),
    vmq_server_cmd:set_config(retry_interval, 10),
    vmq_server_cmd:listener_start(1888, []),
    Config.

end_per_testcase(_, Config) ->
    vmq_test_utils:teardown(),
    Config.

all() ->
    [multiple_sessions_test,
     multiple_balanced_sessions_test].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

multiple_sessions_test(_) ->
    Connect = packet:gen_connect("multiple-sessions-test", [{keepalive,10}]),
    Subscribe = packet:gen_subscribe(53, "multiple/sessions/test", 0),
    Suback = packet:gen_suback(53, 0),
    enable_on_publish(),
    enable_on_subscribe(),
    F = fun(SessionPresent) ->
                Connack = packet:gen_connack(SessionPresent, 0),
                {ok, S} = packet:do_client_connect(Connect, Connack, []),
                ok = gen_tcp:send(S, Subscribe),
                ok = packet:expect_packet(S, "suback", Suback),
                S
        end,
    [Socket|_] = Sockets = [F(I > 1) || I <- lists:seq(1, 100)],
    Publish = packet:gen_publish("multiple/sessions/test", 0, <<"message">>, []),
    gen_tcp:send(Socket, Publish),
    _ = [ok = packet:expect_packet(S, "publish", Publish) || S <- Sockets],
    _ = [gen_tcp:close(S) || S <- Sockets],
    disable_on_subscribe(),
    disable_on_publish().

multiple_balanced_sessions_test(_) ->
    vmq_config:set_env(queue_deliver_mode, balance, false),
    Connect = packet:gen_connect("multiple-sessions-test", [{keepalive,10}]),
    Subscribe = packet:gen_subscribe(53, "multiple/sessions/test", 0),
    Suback = packet:gen_suback(53, 0),
    enable_on_publish(),
    enable_on_subscribe(),
    F = fun(SessionPresent) ->
                Connack = packet:gen_connack(SessionPresent, 0),
                {ok, S} = packet:do_client_connect(Connect, Connack, []),
                ok = gen_tcp:send(S, Subscribe),
                ok = packet:expect_packet(S, "suback", Suback),
                S
        end,
    Sockets = [F(I > 1) || I <- lists:seq(1, 100)],
    Publish = packet:gen_publish("multiple/sessions/test", 0, <<"message">>, []),
    _ = [gen_tcp:send(S, Publish) || S <- Sockets],
    ok = expect_packet(Sockets, Publish, length(Sockets)),
    ok = drain_packet(Sockets, Publish),
    _ = [gen_tcp:close(S) || S <- Sockets],
    disable_on_subscribe(),
    disable_on_publish().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
hook_auth_on_subscribe(_,{"", <<"multiple-sessions-test">>}, _) -> ok.

hook_auth_on_publish(_, {"", <<"multiple-sessions-test">>}, _MsgId, [<<"multiple">>,<<"sessions">>, <<"test">>], <<"message">>, false) -> ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Helper
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
enable_on_subscribe() ->
    vmq_plugin_mgr:enable_module_plugin(
      auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3).
enable_on_publish() ->
    vmq_plugin_mgr:enable_module_plugin(
      auth_on_publish, ?MODULE, hook_auth_on_publish, 6).
disable_on_subscribe() ->
    vmq_plugin_mgr:disable_module_plugin(
      auth_on_subscribe, ?MODULE, hook_auth_on_subscribe, 3).
disable_on_publish() ->
    vmq_plugin_mgr:disable_module_plugin(
      auth_on_publish, ?MODULE, hook_auth_on_publish, 6).

expect_packet(_, _, 0) -> ok;
expect_packet([Socket|Sockets], Publish, Acc) ->
    case packet:expect_packet(gen_tcp, Socket, "publish", Publish, 10) of
        ok ->
            expect_packet(Sockets ++ [Socket], Publish, Acc - 1);
        {error, timeout} ->
            expect_packet(Sockets ++ [Socket], Publish, Acc)
    end.

drain_packet([], _) -> ok;
drain_packet([Socket|Sockets], Publish) ->
    %% no socket should return us a packet
    {error, timeout} = packet:expect_packet(gen_tcp, Socket, "publish", Publish, 10),
    drain_packet(Sockets, Publish).


