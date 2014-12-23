-module(vmq_exo).
-export([entries/0,
         subscribers/3,
         change_config/1]).

-export([incr_bytes_received/1,
         incr_bytes_sent/1,
         incr_inactive_clients/0,
         decr_inactive_clients/0,
         incr_expired_clients/0,
         incr_messages_received/1,
         incr_messages_sent/1,
         incr_publishes_dropped/1,
         incr_publishes_received/1,
         incr_publishes_sent/1,
         incr_subscription_count/0,
         decr_subscription_count/0,
         incr_socket_count/0,
         incr_connect_received/0]).

change_config(Config) ->
    case lists:keyfind(vmq_exo, 1, application:which_applications()) of
        false ->
            %% vmq_exo app is loaded but not started
            ok;
        _ ->
            %% vmq_exo app is not started
            {vmq_exo, VmqExoConfig} = lists:keyfind(vmq_exo, 1, Config),
            ExometerConfig = proplists:get_value(report, VmqExoConfig, []),
            {_, Reporters} = lists:keyfind(reporters, 1, ExometerConfig),
            case lists:keyfind(exometer_report_snmp, 1, Reporters) of
                false ->
                    application:stop(snmp);
                _ ->
                    application:ensure_all_started(snmp)
            end,
            application:stop(exometer),
            application:set_env(exometer, report, ExometerConfig),
            application:start(exometer)
    end.

incr_bytes_received(V) ->
    incr_item([bytes, received], V).

incr_bytes_sent(V) ->
    incr_item([bytes, sent], V).

incr_expired_clients() ->
    incr_item([expired_clients], 1).

incr_inactive_clients() ->
    incr_item([inactive_clients], 1).

decr_inactive_clients() ->
    incr_item([inactive_clients], -1).

incr_messages_received(V) ->
    incr_item([messages, received], V).

incr_messages_sent(V) ->
    incr_item([messages, sent], V).

incr_publishes_dropped(V) ->
    incr_item([publishes, dropped], V).

incr_publishes_received(V) ->
    incr_item([publishes, received], V).

incr_publishes_sent(V) ->
    incr_item([publishes, sent], V).

incr_subscription_count() ->
    incr_item([subscriptions], 1).

decr_subscription_count() ->
    incr_item([subscriptions], -1).

incr_socket_count() ->
    incr_item([sockets], 1).

incr_connect_received() ->
    incr_item([connects, received], 1).

incr_item(Entry, Val) ->
    exometer:update_or_create(Entry, Val).

entries() ->
    {ok, entries_()}.

entries_() ->
    [
    % {[memory], {function, erlang, memory, [], proplist,[total, processes]}, [{snmp, []}]},
     {[bytes, received], histogram, [{snmp, []}]},
     {[bytes, sent], histogram, [{snmp, []}]},
     {[messages, received], histogram, [{snmp, []}]},
     {[messages, sent], histogram, [{snmp, []}]},
     {[publishes, dropped], histogram, [{snmp, []}]},
     {[publishes, received], histogram, [{snmp, []}]},
     {[publishes, sent], histogram, [{snmp, []}]},
     {[connects, received], histogram, [{snmp, []}]},
     {[sockets], histogram, [{snmp, []}]},
     {[subscriptions], counter, [{snmp, []}]},
     {[expired_clients], counter, [{snmp, []}]},
     {[inactive_clients], counter, [{snmp, []}]}
    ].


subscribers(SNMPEnabled, GraphiteEnabled, CollectdEnabled) ->
    snmp_subscribers(SNMPEnabled)
    ++ graphite_subscribers(GraphiteEnabled)
    ++ collectd_subscribers(CollectdEnabled).

snmp_subscribers(true) ->
    application:start(snmp),
    [];
snmp_subscribers(false) ->
    application:stop(snmp),
    [].

graphite_subscribers(true) ->
    subscriptions(exometer_report_graphite);
graphite_subscribers(false) -> [].

collectd_subscribers(_) -> [].

subscriptions(Reporter) ->
    lists:foldl(
      fun({[memory] = M, _, _}, Acc) ->
              [{Reporter, M, total, 5000, false},
               {Reporter, M, processes, 5000, false}
               |Acc];
         ({M, _, _}, Acc) ->
              [T|H] = lists:reverse(M),
              [{Reporter, lists:reverse(H), T, 5000, false}|Acc]
      end, [], entries_()).
