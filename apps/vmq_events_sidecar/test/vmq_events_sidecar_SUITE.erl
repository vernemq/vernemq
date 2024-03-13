-module(vmq_events_sidecar_SUITE).
-include_lib("vernemq_dev/include/vernemq_dev.hrl").
-include_lib("vmq_commons/include/vmq_types.hrl").
-include("vmq_events_sidecar_test.hrl").

-export([
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-compile([export_all]).
-compile([nowarn_export_all]).

init_per_suite(Config) ->
    ListenSock = start_tcp_server(),

    application:load(vmq_plugin),
    application:ensure_all_started(vmq_plugin),
    ok = vmq_plugin_mgr:enable_plugin(vmq_events_sidecar),
    {ok, _} = vmq_metrics:start_link(),

    {ok, _} = application:ensure_all_started(shackle),
    cover:start(),
    [{socket, ListenSock} |Config].

end_per_suite(Config) ->
    stop_tcp_server(proplists:get_value(socket, Config, [])),

    ok = vmq_plugin_mgr:disable_plugin(vmq_events_sidecar),
    application:stop(vmq_plugin),

    application:stop(shackle),
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_, Config) ->
    Config.

all() ->
    [on_session_expired_test,
     on_delivery_complete_test,
     on_register_test,
     on_register_empty_properties_test,
     on_publish_test,
     on_subscribe_test,
     on_unsubscribe_test,
     on_deliver_test,
     on_offline_message_test,
     on_client_wakeup_test,
     on_client_offline_test,
     on_client_gone_test,
     on_message_drop_test
    ].


start_tcp_server() ->
  events_sidecar_handler:start_tcp_server().
stop_tcp_server(S) ->
  events_sidecar_handler:stop_tcp_server(S).

%% Test cases
on_register_test(_) ->
    enable_hook(on_register),
    Self = pid_to_bin(self()),
    UserProps = [{"k1", "v1"}, {"k2","v2"}, {"k3","v3"}],
    [ok] = vmq_plugin:all(on_register,
                            [?PEER, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, Self, #{?P_USER_PROPERTY => UserProps}]),
    ok = exp_response(on_register_ok),
    disable_hook(on_register).

on_register_empty_properties_test(_) ->
  enable_hook(on_register),
  Self = pid_to_bin(self()),
  [ok] = vmq_plugin:all(on_register,
    [?PEER, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, Self, #{}]),
  ok = exp_response(on_register_ok),
  disable_hook(on_register).

on_publish_test(_) ->
    enable_hook(on_publish),
    Self = pid_to_bin(self()),
    [ok] = vmq_plugin:all(on_publish,
                           [Self, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, 1, ?TOPIC, ?PAYLOAD, false, #matched_acl{name = ?LABEL, pattern = ?PATTERN}]),
    ok = exp_response(on_publish_ok),
    disable_hook(on_publish).

on_subscribe_test(_) ->
    enable_hook(on_subscribe),
    Self = pid_to_bin(self()),
    [ok] = vmq_plugin:all(on_subscribe,
                            [Self, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, [{?TOPIC, 1, #matched_acl{name = ?LABEL, pattern = ?PATTERN}}, 
                                                                       {?TOPIC, not_allowed, #matched_acl{}}]]),
    ok = exp_response(on_subscribe_ok),
    disable_hook(on_subscribe).

on_unsubscribe_test(_) ->
    enable_hook(on_unsubscribe),
    Self = pid_to_bin(self()),
    ok = vmq_plugin:all_till_ok(on_unsubscribe,
                                [Self, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, [?TOPIC]]),
    ok = exp_response(on_unsubscribe_ok),
    disable_hook(on_unsubscribe).

on_deliver_test(_) ->
    enable_hook(on_deliver),
    Self = pid_to_bin(self()),
    ok = vmq_plugin:all_till_ok(on_deliver,
                                [Self, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, 1, ?TOPIC, ?PAYLOAD, false, #matched_acl{name = ?LABEL, pattern = ?PATTERN}, true]),
    ok = exp_response(on_deliver_ok),
    disable_hook(on_deliver).

on_delivery_complete_test(_) ->
  enable_hook(on_delivery_complete),
  Self = pid_to_bin(self()),
  [ok] = vmq_plugin:all(on_delivery_complete,[Self, {?MOUNTPOINT, ?ALLOWED_CLIENT_ID}, 1, ?TOPIC, ?PAYLOAD, false, #matched_acl{name = ?LABEL, pattern = ?PATTERN}, true]),
  ok = exp_response(on_delivery_complete_ok),
  disable_hook(on_delivery_complete).

on_offline_message_test(_) ->
    enable_hook(on_offline_message),
    Self = pid_to_bin(self()),
    [ok] = vmq_plugin:all(on_offline_message, [{?MOUNTPOINT, Self}, 1, ?TOPIC, ?PAYLOAD, false]),
    ok = exp_response(on_offline_message_ok),
    disable_hook(on_offline_message).

on_client_wakeup_test(_) ->
    enable_hook(on_client_wakeup),
    Self = pid_to_bin(self()),
    [ok] = vmq_plugin:all(on_client_wakeup, [{?MOUNTPOINT, Self}]),
    ok = exp_response(on_client_wakeup_ok),
    disable_hook(on_client_wakeup).

on_client_offline_test(_) ->
    enable_hook(on_client_offline),
    Self = pid_to_bin(self()),
    [ok] = vmq_plugin:all(on_client_offline, [{?MOUNTPOINT, Self}, ?REASON]),
    ok = exp_response(on_client_offline_ok),
    disable_hook(on_client_offline).

on_client_gone_test(_) ->
    enable_hook(on_client_gone),
    Self = pid_to_bin(self()),
    [ok] = vmq_plugin:all(on_client_gone, [{?MOUNTPOINT, Self}, ?REASON]),
    ok = exp_response(on_client_gone_ok),
    disable_hook(on_client_gone).

on_session_expired_test(_) ->
    enable_hook(on_session_expired),
    Self = pid_to_bin(self()),
    [ok] = vmq_plugin:all(on_session_expired, [{?MOUNTPOINT, Self}]),
    ok = exp_response(on_session_expired_ok),
    disable_hook(on_session_expired).

on_message_drop_test(_) ->
    enable_hook(on_message_drop),
    Self = pid_to_bin(self()),
    [ok] = vmq_plugin:all(on_message_drop, [{?MOUNTPOINT, Self}, fun() -> {?TOPIC, 1, ?PAYLOAD, #{}, #matched_acl{name = ?LABEL, pattern = ?PATTERN}} end, binary_to_atom(?MESSAGE_DROP_REASON)]),
    ok = exp_response(on_message_drop_ok),
    disable_hook(on_message_drop).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
enable_hook(Hook) ->
    ok = clique:run(["vmq-admin", "events", "enable", "hook=" ++ atom_to_list(Hook)]).

disable_hook(Hook) ->
    ok = clique:run(["vmq-admin", "events", "disable", "hook=" ++ atom_to_list(Hook)]),
    _ = vmq_events_sidecar_plugin:all_hooks().

pid_to_bin(Pid) ->
    list_to_binary(lists:flatten(io_lib:format("~p", [Pid]))).

exp_response(Exp) ->
    receive
        Exp -> ok;
        Got -> {received, Got, expected, Exp}
    after
        5000 ->
            {didnt_receive_response, Exp}
    end.

exp_nothing(Timeout) ->
    receive
        Got ->
            {received, Got, expected, nothing}
    after
        Timeout ->
            ok
    end.
