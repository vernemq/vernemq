-module(vmq_rate_limiter_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(Config) ->
    S = vmq_test_utils:get_suite_rand_seed(),
    cover:start(),
    vmq_test_utils:setup(),
    vmq_server_cmd:set_config(allow_anonymous, false),
    vmq_server_cmd:set_config(retry_interval, 10),
    vmq_server_cmd:set_config(max_client_id_size, 100),
    vmq_server_cmd:listener_start(1888, [{allowed_protocol_versions, "3,4,5"}]),
    enable_hooks(),
    [S,{ct_hooks, vmq_cth}| Config].

end_per_suite(_Config) ->
    disable_hooks(),
    vmq_test_utils:teardown(),
    _Config.

init_per_group(mqttv4, Config) ->
    [{protover, 4}|Config];
init_per_group(mqttv5, Config) ->
    [{protover, 5}|Config].

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    vmq_test_utils:seed_rand(Config),
    Config.

end_per_testcase(_, Config) ->
    Config.

all() ->
    [{group, mqttv4},
     {group, mqttv5}].

groups() ->
    Tests = [publish_rate_limit_test,
             publish_throttle_test],
    [
     {mqttv4, [], Tests},
     {mqttv5, [], Tests}
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
publish_rate_limit_test(Cfg) ->
    vmq_metrics:reset_counters(),
    vmq_metrics:clear_stored_rates(),
    %% Rate Limit is enfored in auth_on_register hook
    ClientId = vmq_cth:ustr(Cfg),
    Username = <<"rate-limit-test">>,
    Password = <<"secret">>,

    Connect = mqtt5_v4compat:gen_connect(ClientId, [{username, Username},
                                                    {password, Password}], Cfg),
    Connack = mqtt5_v4compat:gen_connack(success, Cfg),
    Pub = fun(Sleep, Socket, Id) ->
                  Payload = vmq_test_utils:rand_bytes(1460),
                  Publish = mqtt5_v4compat:gen_publish("rate/limit/test", 1,
                                                       Payload, [{mid, Id}], Cfg),
                  Puback = mqtt5_v4compat:gen_puback(Id, Cfg),
                  ok = gen_tcp:send(Socket, Publish),
                  ok = mqtt5_v4compat:expect_packet(Socket, "puback", Puback, Cfg),
                  timer:sleep(Sleep)
          end,
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Cfg),
    {T, _} =
    timer:tc(
      fun() ->
              %% first we need to cancel calc_rates interval
              %% so it wont trigger while we send first 5 pubs
              %% or somehow overlap with sending next 10 pubs
              vmq_metrics:cancel_calc_rates_interval(),
              _ = [Pub(10, Socket, I) || I <- lists:seq(1, 5)],
              %% restart calc_rates interval and wait for 1500ms
              %% so one calc_rates will be proccessed (it takes 1000+ms)
              %% and we would have 500ms left
              %% to start sending 10 pubs before second calc_rates
              vmq_metrics:start_calc_rates_interval(),
              timer:sleep(1500),
              _ = [Pub(10, Socket, I) || I <- lists:seq(1, 10)]
      end),
    %% this should take us at least 10.5 seconds
    TimeInMs = round(T / 1000),
    io:format(user, "time passed in ms / sample: ~p~n", [TimeInMs]),
    true = TimeInMs > 10500,
    QPid = vmq_queue_sup_sup:get_queue_pid({"", list_to_binary(ClientId)}),
    ok = vmq_queue:force_disconnect(QPid, normal),
    ok = gen_tcp:close(Socket).

-define(THROTTLEMS, 100).

publish_throttle_test(Cfg) ->
    ClientId = vmq_cth:ustr(Cfg),
    Username = <<"throttle-user">>,
    Password = <<"secret">>,
    Topic = vmq_cth:utopic(Cfg),
    Connect = mqtt5_v4compat:gen_connect(ClientId, [{username, Username},
                                                    {password, Password}], Cfg),
    Connack = mqtt5_v4compat:gen_connack(success, Cfg),
    {ok, Socket} = mqtt5_v4compat:do_client_connect(Connect, Connack, [], Cfg),

    Pub = fun(Payload, Id) ->
                  Publish = mqtt5_v4compat:gen_publish(Topic, 1, Payload, [{mid, Id}], Cfg),
                  Puback = mqtt5_v4compat:gen_puback(Id, Cfg),
                  ok = gen_tcp:send(Socket, Publish),
                  ok = mqtt5_v4compat:expect_packet(Socket, "puback", Puback, Cfg)
          end,
    {T1, _} =
    timer:tc(fun() -> Pub(<<"don't throttle">>, 1) end),
    io:format(user, "time passed without trottling ~pms~n", [round(T1/1000)]),
    {T2, _} = timer:tc(fun() -> Pub(<<"throttlenext">>, 2),
                                Pub(<<"throttlenext">>, 3),
                                Pub(<<"whatever">>, 4) end),
    io:format(user, "time passed with trottling ~pms~n", [round(T2/1000)]),
    true = T2/1000 > (2*?THROTTLEMS),
    ok = gen_tcp:close(Socket).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Hooks (as explicit as possible)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
hook_auth_on_register(_Peer, _, <<"throttle-user">>, _Password, _Clean) -> ok;
hook_auth_on_register(_Peer, _, <<"rate-limit-test">>, _Password, _Clean) ->
    %% this will limit the publisher to 1 message/sec
    {ok, [{max_message_rate, 1}]}.

hook_auth_on_register_m5(_Peer, _, <<"throttle-user">>, _Password, _CleanStart, _) -> ok;
hook_auth_on_register_m5(_Peer, _, <<"rate-limit-test">>, _Password, _CleanStart, _) ->
    %% this will limit the publisher to 1 message/sec
    {ok, #{max_message_rate => 1}}.

hook_auth_on_publish(<<"throttle-user">>, _, _MsgId, _, <<"throttlenext">>, _) ->  {ok, [{throttle, ?THROTTLEMS}]};
hook_auth_on_publish(_, _, _MsgId, _, _, _) -> ok.
hook_auth_on_publish_m5(<<"throttle-user">>, _, _MsgId, _, <<"throttlenext">>, _, _) -> {ok, #{throttle => ?THROTTLEMS}};
hook_auth_on_publish_m5(_, _, _MsgId, _, _, _, _) -> ok.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Helper
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
enable_hooks() ->
    vmq_plugin_mgr:enable_module_plugin(
      auth_on_register, ?MODULE, hook_auth_on_register, 5),
    vmq_plugin_mgr:enable_module_plugin(
      auth_on_publish, ?MODULE, hook_auth_on_publish, 6),

    vmq_plugin_mgr:enable_module_plugin(
      auth_on_register_m5, ?MODULE, hook_auth_on_register_m5, 6),
    vmq_plugin_mgr:enable_module_plugin(
      auth_on_publish_m5, ?MODULE, hook_auth_on_publish_m5, 7).



disable_hooks() ->
    vmq_plugin_mgr:disable_module_plugin(
      auth_on_register, ?MODULE, hook_auth_on_register, 5),
    vmq_plugin_mgr:disable_module_plugin(
      auth_on_publish, ?MODULE, hook_auth_on_publish, 6),

    vmq_plugin_mgr:disable_module_plugin(
      auth_on_register_m5, ?MODULE, hook_auth_on_register_m5, 6),
    vmq_plugin_mgr:disable_module_plugin(
      auth_on_publish_m5, ?MODULE, hook_auth_on_publish_m5, 7).
