-module(vmq_churney).

-behaviour(gen_server).

%% API
-export([
    start_link/0,
    start_session/0
]).

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

-record(state, {ts, histogram = #{}}).

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

init([]) ->
    vmq_config:set_env(max_client_id_size, 100, false),
    vmq_config:set_env(allow_anonymous, true, false),
    erlang:send_after(10, self(), start_session),
    erlang:send_after(10000, self(), histogram),
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(start_session, State) ->
    spawn_monitor(fun start_session/0),
    {noreply, State#state{ts = os:timestamp()}};
handle_info({'DOWN', _MRef, process, _Pid, Reason}, #state{ts = Ts, histogram = Hist0} = State) ->
    Latency = timer:now_diff(os:timestamp(), Ts),
    Hist1 =
        case Reason of
            A when is_atom(A) ->
                histogram(A, Latency, Hist0);
            _ ->
                histogram(other_error, Latency, Hist0)
        end,
    erlang:send_after(10, self(), start_session),
    {noreply, State#state{ts = undefined, histogram = Hist1}};
handle_info(histogram, #state{histogram = Hist0} = State) ->
    maps:fold(
        fun(ResType, Data0, _Acc) ->
            L = length(Data0),
            Data1 = lists:sort(Data0),
            Min = lists:min(Data1),
            Max = lists:max(Data1),
            Avg = lists:sum(Data0) / L,
            Mid = L div 2,
            Rem = L rem 2,
            Median = lists:nth(Mid + Rem, Data1) + lists:nth(Mid + 1, Data1) / 2,
            lager:info("Churney[~p] n: ~p min: ~p max: ~p avg: ~p median: ~p", [
                ResType, L, Min, Max, Avg, Median
            ])
        end,
        ignore,
        Hist0
    ),
    erlang:send_after(10000, self(), histogram),
    {noreply, State#state{histogram = #{}}}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

histogram(ResType, Latency, Hist) ->
    maps:put(ResType, [Latency | maps:get(ResType, Hist, [])], Hist).

start_session() ->
    {{N1, AP1}, {N2, AP2}} = random_node_pair(),
    C1 = Topic = client_id(N1, N2, "sub"),
    C2 = client_id(N2, N1, "pub"),
    S1 = churney_connect(C1, AP1),
    S2 = churney_connect(C2, AP2),
    % client 1 subscribes
    churney_subscribe(S1, Topic),
    % ensure client 1 was replicated to Node 2
    TopicWords = [Topic],
    ensure_subscription_replicated(N2, C1, TopicWords, 100),
    % client 2 publishes
    Pub = churney_publish_send(S2, Topic, <<"hello world">>),
    churney_publish_recv(S1, Pub),
    gen_tcp:close(S1),
    gen_tcp:close(S2).

random_node_pair() ->
    Nodes = [N || {N, IsReady} <- vmq_cluster:status(), IsReady == true],
    case length(Nodes) > 0 of
        true ->
            N1 = lists:nth(rand:uniform(length(Nodes)), Nodes),
            N2 = lists:nth(rand:uniform(length(Nodes)), Nodes),
            {mqtt, Addr1, Port1, running, _, _} =
                lists:keyfind(mqtt, 1, rpc:call(N1, vmq_ranch_config, listeners, [])),
            {mqtt, Addr2, Port2, running, _, _} =
                lists:keyfind(mqtt, 1, rpc:call(N2, vmq_ranch_config, listeners, [])),
            {{N1, {Addr1, list_to_integer(Port1)}}, {N2, {Addr2, list_to_integer(Port2)}}};
        false ->
            exit(not_ready)
    end.

client_id(A, B, Suffix) ->
    list_to_binary(
        atom_to_list(A) ++
            "-" ++
            atom_to_list(B) ++
            "-" ++
            integer_to_list(erlang:phash2({node(), self(), os:timestamp()})) ++
            "-" ++
            Suffix
    ).

churney_connect(ClientId, {Addr, Port}) ->
    Connect = packet:gen_connect(ClientId, [
        {clean_session, true},
        {keepalive, 60},
        {username, undefined},
        {password, undefined},
        {proto_ver, 4}
    ]),
    Connack = packet:gen_connack(),
    case
        packet:do_client_connect(Connect, Connack, [
            {hostname, Addr},
            {port, Port}
        ])
    of
        {ok, Socket} ->
            Socket;
        _E ->
            exit(not_connack)
    end.

churney_subscribe(Socket, Topic) ->
    Subscribe = packet:gen_subscribe(1, [{Topic, 0}]),
    Suback = packet:gen_suback(1, 0),
    ok = gen_tcp:send(Socket, Subscribe),
    case packet:expect_packet(Socket, suback, Suback) of
        ok ->
            ok;
        _E ->
            exit(not_suback)
    end.

churney_publish_send(Socket, Topic, Payload) ->
    Publish = packet:gen_publish(Topic, 0, Payload, []),
    ok = gen_tcp:send(Socket, Publish),
    Publish.

churney_publish_recv(Socket, PublishFrame) ->
    case packet:expect_packet(Socket, publish, PublishFrame) of
        ok ->
            ok;
        _E ->
            exit(not_pub)
    end.

ensure_subscription_replicated(_, _, _, 0) ->
    exit(not_replicated);
ensure_subscription_replicated(Node, ClientId, Topic, N) ->
    FullPrefix = {vmq, subscriber},
    case
        rpc:call(
            Node,
            vmq_swc_plugin,
            metadata_get,
            [
                FullPrefix,
                {"", ClientId},
                [
                    {allow_put, false},
                    {default, this_is_the_default},
                    {resolver, fun(V) -> V end}
                ]
            ]
        )
    of
        this_is_the_default ->
            timer:sleep(10),
            ensure_subscription_replicated(Node, ClientId, Topic, N - 1);
        Subs0 ->
            Subs1 = vmq_subscriber:check_format(Subs0),
            case topic_exists(Topic, Subs1) of
                false ->
                    timer:sleep(10),
                    ensure_subscription_replicated(Node, ClientId, Topic, N - 1);
                true ->
                    ok
            end
    end.

topic_exists(Topic, Subs) ->
    lists:any(
        fun({_, _, TopicsWithOpts}) ->
            lists:keymember(Topic, 1, TopicsWithOpts)
        end,
        Subs
    ).
