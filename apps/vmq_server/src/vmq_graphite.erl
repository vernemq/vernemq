%% Copyright 2018 Erlio GmbH Basel Switzerland (http://erl.io)
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
%%

-module(vmq_graphite).

-behaviour(gen_server).

-include_lib("vmq_metrics.hrl").

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).

-define(DEFAULT_PORT, 2003).
-define(DEFAULT_CONNECT_TIMEOUT, 5000).
-define(DEFAULT_RECONNECT_TIMEOUT, 2000).
-define(DEFAULT_INTERVAL, 20000).
-define(DEFAULT_INCLUDE_LABELS, false).

%% calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}}).
-define(UNIX_EPOCH, 62167219200).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    {ok, undefined, 0}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(timeout, undefined) ->
    case vmq_config:get_env(graphite_enabled) of
        false ->
            %% Nothing configured
            %% Retry in 5 Secs
            {noreply, undefined, 5000};
        true ->
            case vmq_config:get_env(graphite_host) of
                undefined ->
                    lager:error("can't connect to Graphite due to no host configured.", []),
                    {noreply, undefined, 5000};
                Host ->
                    Port = vmq_config:get_env(graphite_port, ?DEFAULT_PORT),
                    Timeout = vmq_config:get_env(graphite_connect_timeout,
                                                  ?DEFAULT_CONNECT_TIMEOUT),
                    case gen_tcp:connect(Host, Port, [{mode, list}], Timeout) of
                        {ok, Socket} ->
                            Interval = vmq_config:get_env(graphite_interval,
                                                           ?DEFAULT_INTERVAL),
                            {noreply, Socket, Interval};
                        {error, Reason} ->
                            lager:error("can't connect to Graphite due to ~p.",
                                        [Reason]),
                            {noreply, undefined, 5000}
                    end
            end
    end;
handle_info(timeout, Socket) ->
    case vmq_config:get_env(graphite_enabled) of
         false ->
            %% Nothing configured
            %% Retry in 5 Secs
            gen_tcp:close(Socket),
            {noreply, undefined, 5000};
         true ->
            ApiKey = vmq_config:get_env(graphite_api_key, ""),
            Prefix = vmq_config:get_env(graphite_prefix, ""),
            IncludeLabels = vmq_config:get_env(graphite_include_labels, ?DEFAULT_INCLUDE_LABELS),
            DoReconnect =
                lists:foldl(
                  fun (_, true) ->
                          %% error occured
                          true;
                      ({#metric_def{name = Metric, labels = Labels}, Val}, false) ->
                          Lines =
                          case IncludeLabels of
                              true ->
                                  lines(ApiKey, Prefix, Metric, Labels, Val);
                              false ->
                                  lines(ApiKey, Prefix, Metric, Val)
                          end,
                          case gen_tcp:send(Socket, Lines) of
                              ok ->
                                  false;
                              {error, _} ->
                                  true
                          end
                  end, false, vmq_metrics:metrics(#{aggregate => false})),
            case DoReconnect of
                true ->
                    gen_tcp:close(Socket),
                    ReconnectTimeout =
                        vmq_config:get_env(graphite_reconnect_timeout, ?DEFAULT_RECONNECT_TIMEOUT),
                    {noreply, undefined, ReconnectTimeout};
                false ->
                    Interval = vmq_config:get_env(graphite_interval, ?DEFAULT_INTERVAL),
                    {noreply, Socket, Interval}
            end
     end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
        {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% Format a graphite key from API key, prefix, prob, labels and datapoint
lines(APIKey, Prefix, Metric, Val) ->
    line(APIKey, Prefix, Metric, no_label, Val).

lines(APIKey, Prefix, Metric, Labels, Val) ->
    %% Always generate the metric with no labels
    [line(APIKey, Prefix, Metric, no_label, Val)
     |[line(APIKey, Prefix, Metric, Label, Val) || Label <- Labels]].

line(APIKey, Prefix, Metric, Label, {Count, Sum, Buckets}) when is_map(Buckets) ->
    SMetric = atom_to_list(Metric),
    CountLine = line(APIKey, Prefix, SMetric ++ "_count", Label, Count),
    SumLine = line(APIKey, Prefix, SMetric ++ "_sum", Label, Sum),
    Ret =
    maps:fold(
      fun(Bucket, BucketValue, Acc) ->
              [line(APIKey, Prefix, SMetric ++ "_"
                    ++ case Bucket of
                           infinity -> "bucket_inf";
                           _ -> "bucket_" ++ value(Bucket)
                       end, Label, BucketValue)| Acc]
      end, [CountLine, SumLine], Buckets),
    Ret;

line(APIKey, Prefix, Metric, Label, Val) ->
    [key(APIKey, Prefix, Metric, Label), " ", value(Val), " ", timestamp(), $\n].

key(APIKey, Prefix, Metric, no_label) ->
    key(APIKey, Prefix, Metric);
key(APIKey, Prefix, Metric, {Key, Val}) ->
    [key(APIKey, Prefix, Metric), $., atom_to_list(Key), $_, Val].

key([], [], Metric) ->
    name(Metric);
key([], Prefix, Metric) ->
    [Prefix, $., name(Metric)];
key(APIKey, [], Metric) ->
    [APIKey, $., name(Metric)];
key(APIKey, Prefix, Metric) ->
    [APIKey, $., Prefix, $., name(Metric)].

%% Add probe and datapoint within probe
name(Metric) when is_atom(Metric) ->
    name(atom_to_list(Metric));
name(Metric) ->
    [T|F] = lists:reverse(re:split(Metric, "_", [{return, list}])),
    [[I, $.] || I <- lists:reverse(F)] ++ [T].

%% Add value, int or float, converted to list
value(V) when is_integer(V) -> integer_to_list(V);
value(V) when is_float(V)   -> float_to_list(V);
value(_) -> 0.

timestamp() ->
    integer_to_list(unix_time()).

unix_time() ->
    datetime_to_unix_time(erlang:universaltime()).

datetime_to_unix_time({{_,_,_},{_,_,_}} = DateTime) ->
    calendar:datetime_to_gregorian_seconds(DateTime) - ?UNIX_EPOCH.
