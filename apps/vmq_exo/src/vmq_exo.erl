-module(vmq_exo).
-export([entries/0,
         subscribers/3]).

entries() ->
    {ok, entries_()}.

entries_() ->
    {ok, HostName} = inet:gethostname(),
    AHostName = list_to_atom(HostName),
    [
     {[AHostName, memory], {function, erlang, memory, [], proplist,[total, processes]}, [{snmp, []}]},
     {[list_to_atom(HostName)], {function, vmq_systree, summary, [], proplist, metrics()}, [{snmp, []}]}
    ].


subscribers(SNMPEnabled, GraphiteEnabled, CollectdEnabled) ->
    smpp_subscribers(SNMPEnabled)
    ++ graphite_subscribers(GraphiteEnabled)
    ++ collectd_subscribers(CollectdEnabled).

smpp_subscribers(_) -> [].

graphite_subscribers(true) ->
    subscriptions(exometer_report_graphite);
graphite_subscribers(false) -> [].

collectd_subscribers(_) -> [].



subscriptions(Reporter) ->
    {ok, HostName} = inet:gethostname(),
    AHostName = list_to_atom(HostName),
    [{Reporter, [erlang, memory], total, 5000, false},
     {Reporter, [erlang, memory], processes, 5000, false}|
     [{Reporter, [AHostName], M, 5000, false} || M <- metrics()]
    ].


metrics() ->
    [active_clients, inactive_clients, subscription_count,
     total_bytes_received, bytes_recv_avg_1min,
     bytes_recv_avg_5min, bytes_recv_avg_15min,
     total_bytes_sent, bytes_send_avg_1min,
     bytes_send_avg_5min, bytes_send_avg_15min,
     total_messages_received, messages_recv_avg_1min,
     messages_recv_avg_5min, messages_recv_avg_15min,
     total_messages_sent, messages_send_avg_1min,
     messages_send_avg_5min, messages_send_avg_15min,
     total_publishes_received, publishes_recv_avg_1min,
     publishes_recv_avg_5min, publishes_recv_avg_15min,
     total_publishes_sent, publishes_send_avg_1min,
     publishes_send_avg_5min, publishes_send_avg_15min,
     total_publishes_dropped, publishes_drop_avg_1min,
     publishes_drop_avg_5min, publishes_drop_avg_15min,
     connect_recv_avg_1min, connect_recv_avg_5min,
     connect_recv_avg_15min, socket_count_avg_1min,
     socket_count_avg_5min, socket_count_avg_15min,
     total_clients, inflight, retained, stored].
