%% Copyright 2014 Erlio GmbH Basel Switzerland (http://erl.io)
%%
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

-module(vmq_exo).
-export([incr_bytes_received/1,
         incr_bytes_sent/1,
         incr_expired_clients/0,
         incr_messages_received/1,
         incr_messages_sent/1,
         incr_publishes_dropped/1,
         incr_publishes_received/1,
         incr_publishes_sent/1,
         incr_subscription_count/0,
         decr_subscription_count/0,
         incr_socket_count/0,
         incr_connect_received/0,
         entries/0,
         entries/1]).

-export([start_link/0,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

incr_bytes_received(V) ->
    incr_item([bytes, received], V).

incr_bytes_sent(V) ->
    incr_item([bytes, sent], V).

incr_expired_clients() ->
    incr_item([expired_clients], 1).

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
    {ok, entries(undefined)}.

entries(undefined) ->
    [
     {[memory], {function, erlang, memory, [], proplist,[total, processes]}, []},
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
     {[clients, expired], counter, [{snmp, []}]},
     {[clients], {function, vmq_reg, client_stats, [], proplist, [active, inactive]}, []}
    ];
entries({ReporterMod, Interval}) ->
    subscribe(ReporterMod, entries(undefined), Interval).



subscribe(ReporterMod, [{Metric, histogram, _}|Rest], Interval) ->
    Datapoints = [max, min, mean, median],
    subscribe(ReporterMod, Metric, Datapoints, Interval),
    subscribe(ReporterMod, Rest, Interval);
subscribe(ReporterMod, [{Metric, {function, _, _, _, proplist, Items}, _}|Rest],
          Interval) ->
    subscribe(ReporterMod, Metric, Items, Interval),
    subscribe(ReporterMod, Rest, Interval);
subscribe(ReporterMod, [{Metric, {function, _M, _F, _A, value, _}, _}|Rest], Interval) ->
    subscribe(ReporterMod, Metric, default, Interval),
    subscribe(ReporterMod, Rest, Interval);
subscribe(ReporterMod, [{Metric, _, _}|Rest], Interval) ->
    subscribe(ReporterMod, Metric, value, Interval),
    subscribe(ReporterMod, Rest, Interval);
subscribe(_, [], _) -> ok.

subscribe(ReporterMod, Metric, [Datapoint|Rest], Interval) when is_atom(Datapoint) ->
    subscribe(ReporterMod, Metric, Datapoint, Interval),
    subscribe(ReporterMod, Metric, Rest, Interval);
subscribe(ReporterMod, Metric, Datapoint, Interval) when is_atom(Datapoint) ->
    case exometer_report:subscribe(ReporterMod, Metric, Datapoint, Interval) of
        ok ->
            ok;
        E ->
            exit({exometer_report_subscribe, E, ReporterMod, Metric, Datapoint})
    end;
subscribe(_, _, [], _) -> ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% GEN_SERVER
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, []).

-spec init([string()]) -> {'ok', []}.
init([]) ->
    {ok, []}.

-spec handle_call(_, _, _) -> {'reply', ok|{error, _}, _}.
handle_call({init_counter, Entry, Val}, _From, State) ->
    Reply =
    case exometer:update(Entry, Val) of
        {error, not_found} ->
            NewVal = get_default_val(Entry),
            case exometer_admin:auto_create_entry(Entry) of
                ok ->
                    exometer:update(Entry, NewVal);
                Error ->
                    Error
            end;
        ok ->
            ok
    end,
    {reply, Reply, State}.

-spec handle_cast(_, _) -> {'noreply', _}.
handle_cast(_Req, State) ->
    {noreply, State}.

-spec handle_info(_, _) -> {'noreply', _}.
handle_info(_Info, State) ->
    {noreply, State}.

-spec terminate(_, _) -> 'ok'.
terminate(_Reason, _State) ->
    ok.

-spec code_change(_, _, _) -> {'ok', _}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

get_default_val([subscriptions]) ->
    vmq_reg:fold_subscribers(
      fun({_, _, T}, Sum) when is_tuple(T) ->
              Sum + 1;
         (_, Sum) ->
              Sum
      end, 0).

