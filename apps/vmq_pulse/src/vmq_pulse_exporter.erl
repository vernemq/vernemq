%% Copyright 2019 Octavo Labs AG Switzerland (http://octavolabs.com)
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
-module(vmq_pulse_exporter).

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER, ?MODULE).
-define(PULSE_VERSION, 1).
-define(PULSE_USER, "vmq-pulse-1").

-record(state, {}).

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
    self() ! push,
    % first value is garbage
    _ = cpu_sup:util(),
    {ok, #state{}}.

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
handle_info(push, State) ->
    ClusterId = vmq_pulse:get_cluster_id(),
    {ok, PulseUrl} = application:get_env(vmq_pulse, url),
    {ok, Interval} = application:get_env(vmq_pulse, push_interval),
    ApiKey = application:get_env(vmq_pulse, api_key, undefined),
    NodeId = atom_to_binary(node(), utf8),
    case (ClusterId =/= undefined) of
        true ->
            PulseData = create_pulse_data(ClusterId, NodeId),
            Body = zlib:compress(term_to_binary(PulseData)),
            try push_body(PulseUrl, ApiKey, Body) of
                ok ->
                    ok;
                {error, Reason} ->
                    error_logger:warning_msg("can't push pulse due to error ~p", [Reason])
            catch
                E:R ->
                    error_logger:warning_msg("can't push pulse due to unknown error ~p ~p", [E, R])
            end;
        false ->
            ignore
    end,
    erlang:send_after(interval(Interval), self(), push),
    {noreply, State}.

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
metrics() ->
    lists:filtermap(
        fun
            ({{metric_def, Type, Labels, _Id, Name, Desc}, Val}) ->
                {true, #{
                    type => Type,
                    labels => Labels,
                    name => Name,
                    desc => Desc,
                    value => Val
                }};
            (_UnsupportedMetricDef) ->
                false
        end,
        vmq_metrics:metrics(#{aggregate => false})
    ).

create_pulse_data(ClusterId, Node) ->
    PulseData0 =
        [
            {node, Node},
            {cluster_id, ClusterId},
            {pulse_version, ?PULSE_VERSION},
            {plugins, fun() -> vmq_plugin:info(all) end},
            {metrics, fun metrics/0},
            {cpu_util, fun cpu_sup:util/0},
            {mem_util, fun memsup:get_system_memory_data/0},
            {disk_util, fun disk_util/0},
            {applications, fun() ->
                [{App, Vsn} || {App, _, Vsn} <- application:which_applications()]
            end},
            {system_version, fun() -> erlang:system_info(system_version) end},
            {cluster_status, fun vmq_cluster:status/0},
            {uname, fun() -> os:cmd("uname -a") end}
        ],
    lists:map(fun({K, V}) -> {K, maybe_rescue(V)} end, PulseData0).

maybe_rescue(Fun) when is_function(Fun) ->
    try
        Fun()
    catch
        Class:Exception:Stacktrace ->
            {error, [
                {class, Class},
                {exception, Exception},
                {stacktrace, Stacktrace}
            ]}
    end;
maybe_rescue(Val) ->
    Val.

push_body(PulseUrl, ApiKey, Body) ->
    ContentType = "application/x-vmq-pulse-1",
    Headers =
        case ApiKey of
            undefined ->
                [];
            _ ->
                AuthStr = base64:encode_to_string(?PULSE_USER ++ ":" ++ ApiKey),
                [{"Authorization", "Basic " ++ AuthStr}]
        end,
    {ok, ConnectTimeout} = application:get_env(vmq_pulse, connect_timeout),
    HTTPOpts = [
        {timeout, 60000},
        {connect_timeout, ConnectTimeout * 1000},
        {autoredirect, true}
    ],

    case httpc:request(post, {PulseUrl, Headers, ContentType, Body}, HTTPOpts, []) of
        {ok, _} ->
            ok;
        {error, _Reason} = E ->
            E
    end.

interval(Int) when Int < 10 -> 10 * 1000;
interval(Int) -> Int * 1000.

disk_util() ->
    MsgStoreDisk =
        case application:get_env(vmq_server, message_store_impl) of
            {ok, vmq_generic_msg_store} ->
                {ok, MsgStoreOpts} = application:get_env(vmq_generic_msg_store, msg_store_opts),
                find_disk_for_directory(
                    filename:absname(proplists:get_value(store_dir, MsgStoreOpts))
                );
            _ ->
                unknown_message_store
        end,

    MetadataDisk =
        case metadata_backend() of
            vmq_swc ->
                {ok, SWCDataDir} = application:get_env(vmq_swc, data_dir),
                find_disk_for_directory(filename:absname(SWCDataDir));
            vmq_plumtree ->
                {ok, PlumtreeDataDir} = application:get_env(plumtree, plumtree_data_dir),
                find_disk_for_directory(filename:absname(PlumtreeDataDir))
        end,
    [{msg_store, MsgStoreDisk}, {meta, MetadataDisk}].

find_disk_for_directory(Directory) when is_list(Directory) ->
    find_disk_for_directory(list_to_binary(Directory));
find_disk_for_directory(Directory) when is_binary(Directory) ->
    {_, Disk} =
        lists:foldl(
            fun
                ({Mount, _, _} = Disk, {PrefixSize, _} = Acc) ->
                    case binary:longest_common_prefix([list_to_binary(Mount), Directory]) of
                        NewPrefixSize when NewPrefixSize > PrefixSize ->
                            {NewPrefixSize, Disk};
                        _ ->
                            Acc
                    end;
                (_, Acc) ->
                    Acc
            end,
            {0, no_disk},
            lists:keysort(1, disksup:get_disk_data())
        ),
    Disk.

metadata_backend() ->
    {ok, MetadataBackend} = application:get_env(vmq_server, metadata_impl),
    MetadataBackend.
