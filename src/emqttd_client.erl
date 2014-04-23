-module(emqttd_client).
-behavior(gen_fsm).

-include("emqtt_frame.hrl").

%startup
-export([start_link/0,
	 start_link/1,
	 start_link/2]).

%api
-export([publish/2, publish/3, publish/4,
	 pubrel/2,
	 puback/2,
	 pubrec/2,
	 pubcomp/2,
	 subscribe/2,
	 unsubscribe/2,
	 ping/1,
	 disconnect/1]).


%% gen_fsm callbacks
-export([init/1,
	 handle_info/3,
	 handle_event/3,
	 handle_sync_event/4,
	 code_change/4,
	 terminate/3]).

% fsm state
-export([connecting/2,
	 connecting/3,
	 waiting_for_connack/2,
	 connected/2,
	 connected/3,
	 disconnected/2]).

-define(TCPOPTIONS, [binary,
		     {packet,    raw},
		     {reuseaddr, true},
		     {nodelay,   true},
		     {active, 	true},
		     {reuseaddr, true},
		     {send_timeout,  3000}]).

-define(TIMEOUT, 3000).
-define(QOS_0, 0).
-define(QOS_1, 1).
-define(QOS_2, 2).

-record(state, {host      :: inet:ip_address(),
		port      :: inet:port_number(),
		sock      :: gen_tcp:socket(),
		msgid = 0 :: non_neg_integer(),
		username  :: binary(),
		password  :: binary(),
		ref       :: dict:dict(),
        client    :: string() ,
        clean_session :: boolean(),
        last_will_topic :: string() | undefined,
        last_will_msg :: string() | undefined,
        last_will_qos :: non_neg_integer()}).

-record(mqtt_msg,
        {retain = false :: boolean(),
         qos = 0 :: non_neg_integer(),
         topic :: binary(),
         dup = 0 :: non_neg_integer(),
         message_id :: undefined | non_neg_integer(),
         payload :: binary() }).



%%--------------------------------------------------------------------
%% @doc Starts the server
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    start_link([]).

%%--------------------------------------------------------------------
%% @doc Starts the server with options.
%% @end
%%--------------------------------------------------------------------
-spec start_link([tuple()]) -> {ok, pid()} | ignore | {error, term()}.
start_link(Opts) when is_list(Opts) ->
    start_link(?MODULE, Opts).

%%--------------------------------------------------------------------
%% @doc Starts the server with name and options.
%% @end
%%--------------------------------------------------------------------
-spec start_link(atom(), [tuple()]) -> {ok, pid()} | ignore | {error, term()}.
start_link(Name, Opts) when is_atom(Name), is_list(Opts) ->
    gen_fsm:start_link({local, Name}, ?MODULE, [Name, Opts], []).

%%--------------------------------------------------------------------
%% @doc publish to broker.
%% @end
%%--------------------------------------------------------------------
-spec publish(C, Topic, Payload) -> ok | {ok, MsgId} when
      C :: pid() | atom(),
      Topic :: binary(),
      Payload :: binary(),
      MsgId :: non_neg_integer().
publish(C, Topic, Payload) when is_binary(Topic), is_binary(Payload) ->
    publish(C, #mqtt_msg{topic = Topic, payload = Payload}).

-spec publish(C, Topic, Payload, Qos) -> ok | {ok, MsgId} when
      C :: pid() | atom(),
      Topic :: binary(),
      Payload :: binary(),
      Qos :: non_neg_integer(),
      MsgId :: non_neg_integer().
publish(C, Topic, Payload, Qos) when is_binary(Topic), is_binary(Payload),
				     is_integer(Qos) ->
    publish(C, #mqtt_msg{topic = Topic, payload = Payload, qos = Qos}).

-spec publish(C, #mqtt_msg{}) -> ok | pubrec when
      C :: pid() | atom().
publish(C, Msg = #mqtt_msg{qos = ?QOS_0}) when is_record(Msg, mqtt_msg) ->
    gen_fsm:send_event(C, {publish, Msg});

publish(C, Msg = #mqtt_msg{qos = ?QOS_1}) when is_record(Msg, mqtt_msg) ->
    gen_fsm:sync_send_event(C, {publish, Msg});

publish(C, Msg = #mqtt_msg{qos = ?QOS_2}) when is_record(Msg, mqtt_msg) ->
    gen_fsm:sync_send_event(C, {publish, Msg}).

%%--------------------------------------------------------------------
%% @doc pubrec.
%% @end
%%--------------------------------------------------------------------
-spec pubrel(C, MsgId) -> ok when
      C :: pid() | atom(),
      MsgId :: non_neg_integer().
pubrel(C, MsgId) when is_integer(MsgId) ->
    gen_fsm:sync_send_event(C, {pubrel, MsgId}).

%%--------------------------------------------------------------------
%% @doc puback.
%% @end
%%--------------------------------------------------------------------
-spec puback(C, MsgId) -> ok when
      C :: pid() | atom(),
      MsgId :: non_neg_integer().
puback(C, MsgId) when is_integer(MsgId) ->
    gen_fsm:send_event(C, {puback, MsgId}).

%%--------------------------------------------------------------------
%% @doc pubrec.
%% @end
%%--------------------------------------------------------------------
-spec pubrec(C, MsgId) -> ok when
      C :: pid() | atom(),
      MsgId :: non_neg_integer().
pubrec(C, MsgId) when is_integer(MsgId) ->
    gen_fsm:send_event(C, {pubrec, MsgId}).

%%--------------------------------------------------------------------
%% @doc pubcomp.
%% @end
%%--------------------------------------------------------------------
-spec pubcomp(C, MsgId) -> ok when
      C :: pid() | atom(),
      MsgId :: non_neg_integer().
pubcomp(C, MsgId) when is_integer(MsgId) ->
    gen_fsm:send_event(C, {pubcomp, MsgId}).

%%--------------------------------------------------------------------
%% @doc subscribe request to broker.
%% @end
%%--------------------------------------------------------------------
-spec subscribe(C, Topics) -> ok when
      C :: pid() | atom(),
      Topics :: [ {binary(), non_neg_integer()} ].
subscribe(C, Topics) ->
    gen_fsm:send_event(C, {subscribe, Topics}).

%%--------------------------------------------------------------------
%% @doc unsubscribe request to broker.
%% @end
%%--------------------------------------------------------------------
-spec unsubscribe(C, Topics) -> ok when
      C :: pid() | atom(),
      Topics :: [ {binary(), non_neg_integer()} ].
unsubscribe(C, Topics) ->
    gen_fsm:send_event(C, {unsubscribe, Topics}).

%%--------------------------------------------------------------------
%% @doc Send ping to broker.
%% @end
%%--------------------------------------------------------------------
-spec ping(C) -> pong when
      C :: pid() | atom().
ping(C) ->
    gen_fsm:sync_send_event(C, ping).

%%--------------------------------------------------------------------
%% @doc Disconnect from broker.
%% @end
%%--------------------------------------------------------------------
-spec disconnect(C) -> ok when
      C :: pid() | atom().
disconnect(C) ->
    gen_fsm:send_event(C, disconnect).

%%gen_fsm callbacks
init([_Name, Args]) ->
    Host = proplists:get_value(host, Args, "localhost"),
    Port = proplists:get_value(port, Args, 1883),
    Username = proplists:get_value(username, Args, undefined),
    Password = proplists:get_value(password, Args, undefined),
    ClientId = proplists:get_value(client, Args, "emqttc"),
    CleanSession = proplists:get_value(clean_session, Args, true),
    LWTopic = proplists:get_value(last_will_topic, Args, undefined),
    LWMsg = proplists:get_value(last_will_msg, Args, undefined),
    State = #state{host = Host, port = Port, ref = dict:new(),
		   username = Username, password = Password, client=ClientId,
           clean_session=CleanSession, last_will_topic=LWTopic, last_will_msg=LWMsg},
    {ok, connecting, State, 0}.

disconnected(timeout, State) ->
    timer:sleep(3000),
    {next_state, connecting, State, 0}.

connecting(timeout, State) ->
    connect(State);

connecting(_Event, State) ->
    {next_state, connecting, State}.

connecting(_Event, _From, State) ->
    {reply, {error, connecting}, connecting, State}.

connect(#state{host = Host, port = Port} = State) ->
    io:format("connecting to ~p:~p~n", [Host, Port]),

    case gen_tcp:connect(Host, Port, ?TCPOPTIONS, ?TIMEOUT) of
	{ok, Sock} ->
	    io:format("tcp connected.~n"),
	    NewState = State#state{sock = Sock},
	    send_connect(NewState),
	    {next_state, waiting_for_connack, NewState};
	{error, Reason} ->
	    io:format("tcp connection failure: ~p~n", [Reason]),
	    reconnect(),
	    {next_state, connecting, State#state{sock = undefined}}
    end.

send_connect(#state{sock=Sock, username=Username, password=Password, client=ClientId,
                    clean_session=CleanSession, last_will_topic=LWTopic, last_will_msg=LWMsg,
                    last_will_qos=LWQoS}) ->
    Frame =
	#mqtt_frame{
	   fixed = #mqtt_frame_fixed{
		      type = ?CONNECT,
		      dup = 0,
		      qos = 1,
		      retain = 0},
	   variable = #mqtt_frame_connect{
			 username   = Username,
			 password   = Password,
			 proto_ver  = ?MQTT_PROTO_MAJOR,
			 clean_sess = CleanSession,
             will_flag  = (LWTopic /= undefined) and (LWMsg /= undefined),
             will_retain  = (LWTopic /= undefined) and (LWMsg /= undefined),
             will_qos   = case (LWTopic /= undefined) and (LWMsg /= undefined) of
                              true when is_integer(LWQoS) -> LWQoS;
                              _ -> 0
                          end,
             will_topic = LWTopic,
             will_msg = LWMsg,
			 keep_alive = 60,
			 client_id  = ClientId}},
    send_frame(Sock, Frame).

waiting_for_connack(_Event, State) ->
    %FIXME:
    {next_state, waiting_for_connack, State}.

connected({publish, Msg}, State=#state{sock=Sock, msgid=MsgId}) ->
    #mqtt_msg{retain     = Retain,
	      qos        = Qos,
	      topic      = Topic,
	      dup        = Dup,
	      payload    = Payload} = Msg,
    Frame = #mqtt_frame{
	       fixed = #mqtt_frame_fixed{type 	 = ?PUBLISH,
					 qos    = Qos,
					 retain = Retain,
					 dup    = Dup},
	       variable = #mqtt_frame_publish{topic_name = Topic,
					      message_id = if Qos == ?QOS_0 ->
								   undefined;
							      true ->
								   MsgId
							   end},
	       payload = Payload},
    send_frame(Sock, Frame),
    {next_state, connected, State#state{msgid=MsgId+1}};

connected({puback, MsgId}, State=#state{sock=Sock}) ->
    send_puback(Sock, ?PUBACK, MsgId),
    {next_state, connected, State};

connected({pubrec, MsgId}, State=#state{sock=Sock}) ->
    send_puback(Sock, ?PUBREC, MsgId),
    {next_state, connected, State};

connected({pubcomp, MsgId}, State=#state{sock=Sock}) ->
    send_puback(Sock, ?PUBCOMP, MsgId),
    {next_state, connected, State};

connected({subscribe, Topics}, State=#state{msgid=MsgId,sock=Sock}) ->
    Topics1 = [#mqtt_topic{name=Topic, qos=Qos} || {Topic, Qos} <- Topics],
    Frame = #mqtt_frame{
	       fixed = #mqtt_frame_fixed{type = ?SUBSCRIBE,
					 dup = 0,
					 qos = 1,
					 retain = 0},
	       variable = #mqtt_frame_subscribe{message_id  = MsgId,
						topic_table = Topics1}},
    send_frame(Sock, Frame),
    {next_state, connected, State#state{msgid=MsgId+1}};

connected({unsubscribe, Topics}, State=#state{sock=Sock, msgid=MsgId}) ->
    Frame = #mqtt_frame{
	       fixed = #mqtt_frame_fixed{type = ?UNSUBSCRIBE,
					 dup = 0,
					 qos = 1,
					 retain = 0},
	       variable = MsgId,
	       payload = Topics},
    send_frame(Sock, Frame),
    {next_state, connected, State};

connected(disconnect, State=#state{sock=Sock}) ->
    send_disconnect(Sock),
    {next_state, connected, State};

connected(_Event, State) ->
    {next_state, connected, State}.

connected({publish, Msg}, From,
	  State=#state{sock=Sock, msgid=MsgId, ref=Ref}) ->
    #mqtt_msg{retain     = Retain,
	      qos        = Qos,
	      topic      = Topic,
	      dup        = Dup,
	      payload    = Payload} = Msg,
    Frame = #mqtt_frame{
	       fixed = #mqtt_frame_fixed{type 	 = ?PUBLISH,
					 qos    = Qos,
					 retain = Retain,
					 dup    = Dup},
	       variable = #mqtt_frame_publish{topic_name = Topic,
					      message_id = if Qos == ?QOS_0 ->
								   undefined;
							      true ->
								   MsgId
							   end},
	       payload = Payload},
    send_frame(Sock, Frame),
    Ref2 = dict:append(publish, From, Ref),
    {next_state, connected, State#state{msgid=MsgId+1, ref=Ref2}};

connected({pubrel, MsgId}, From, State=#state{sock=Sock, ref=Ref}) ->
    send_puback(Sock, ?PUBREL, MsgId),
    Ref2 = dict:append(pubrel, From, Ref),
    {next_state, connected, State#state{ref=Ref2}};

connected(ping, From, State=#state{sock=Sock, ref = Ref}) ->
    send_ping(Sock),
    Ref2 = dict:append(ping, From, Ref),
    {next_state, connected, State#state{ref = Ref2}};

connected(Event, _From, State) ->
    io:format("unsupported event: ~p~n", [Event]),
    {reply, {error, unsupport}, connected, State}.

reconnect() ->
    %%FIXME
    erlang:send_after(30000, self(), {timeout, reconnect}).

%% connack message from broker.
handle_info({tcp, _Sock, <<?CONNACK:4/integer, _:4/integer, 2:8/integer,
			 _:8/integer, ReturnCode:8/unsigned-integer>>},
	    waiting_for_connack, State) ->
    case ReturnCode of
	?CONNACK_ACCEPT ->
	    io:format("connack: Connection Accepted~n"),
	    % gen_event:notify(emqttc_event, {connack_accept}),
	    {next_state, connected, State};
	?CONNACK_PROTO_VER ->
	    io:format("connack: NG(unacceptable protocol version)~n"),
	    {next_state, waiting_for_connack, State};
	?CONNACK_INVALID_ID ->
	    io:format("connack: NG(identifier rejected)~n"),
	    {next_state, waiting_for_connack, State};
	?CONNACK_SERVER ->
	    io:format("connack: NG(server unavailable)~n"),
	    {next_state, waiting_for_connack, State};
	?CONNACK_CREDENTIALS ->
	    io:format("connack: NG(bad user name or password)~n"),
	    {next_state, waiting_for_connack, State};
	?CONNACK_AUTH ->
	    io:format("connack: NG(not authorized)~n"),
	    {next_state, waiting_for_connack, State}
    end;

%% suback message from broker.
handle_info({tcp, _Sock, <<?SUBACK:4/integer, _:4/integer, _/binary>>},
	    connected, State) ->
    {next_state, connected, State};

%% pub message from broker(QoS = 0).
handle_info({tcp, _Sock, <<?PUBLISH:4/integer,
			   _:1/integer, ?QOS_0:2/integer, _:1/integer,
			   _Len:8/integer,
			   TopicSize:16/big-unsigned-integer,
			   Topic:TopicSize/binary,
			   Payload/binary>>},
	    connected, State) ->
    io:format("got msg ~p ~p~n", [Topic, Payload]),
    % gen_event:notify(emqttc_event, {publish, Topic, Payload}),
    {next_state, connected, State};

%% pub message from broker(QoS = 1 or 2).
handle_info({tcp, _Sock, <<?PUBLISH:4/integer,
			   _:1/integer, Qos:2/integer, _:1/integer,
			   _Len:8/integer,
			   TopicSize:16/big-unsigned-integer,
			   Topic:TopicSize/binary,
			   MsgId:16/big-unsigned-integer,
			   Payload/binary>>},
	    connected, State) when Qos =:= ?QOS_1; Qos =:= ?QOS_2 ->
    io:format("got msg ~p ~p~n", [Topic, Payload]),
    gen_fsm:send_event(self(), {puback, MsgId}),
    % gen_event:notify(emqttc_event, {publish, Topic, Payload, Qos, MsgId}),
    {next_state, connected, State};

%% pubrec message from broker.
handle_info({tcp, _Sock, <<?PUBACK:4/integer,
			   _:1/integer, _:2/integer, _:1/integer,
			   2:8/integer,
			   MsgId:16/big-unsigned-integer>>},
	    connected, State=#state{ref=Ref}) ->
    Ref2 = reply({ok, MsgId}, publish, Ref),
    {next_state, connected, State#state{ref=Ref2}};

%% pubrec message from broker.
handle_info({tcp, _Sock, <<?PUBREC:4/integer,
			   _:1/integer, _:2/integer, _:1/integer,
			   2:8/integer,
			   MsgId:16/big-unsigned-integer>>},
	    connected, State=#state{ref=Ref}) ->
    Ref2 = reply({ok, MsgId}, publish, Ref),
    {next_state, connected, State#state{ref=Ref2}};

%% pubcomp message from broker.
handle_info({tcp, _Sock, <<?PUBCOMP:4/integer,
			   _:1/integer, _:2/integer, _:1/integer,
			   2:8/integer,
			   MsgId:16/big-unsigned-integer>>},
	    connected, State=#state{ref=Ref}) ->
    Ref2 = reply({ok, MsgId}, pubrel, Ref),
    {next_state, connected, State#state{ref=Ref2}};

%% pingresp message from broker.
handle_info({tcp, _Sock, <<?PINGRESP:4/integer,
			   _:1/integer, _:2/integer, _:1/integer,
			   0:8/integer>>},
	    connected, State=#state{ref = Ref}) ->
    Ref2 = reply(ok, ping, Ref),
    {next_state, connected, State#state{ref = Ref2}};

handle_info({tcp, _Sock, Data}, connected, State) ->
    <<Code:4/integer, _:4/integer, _/binary>> = Data,
    io:format("data received from remote(code:~w): ~p~n", [Code, Data]),
    {next_state, connected, State};

handle_info({tcp_closed, Sock}, connected, State=#state{sock=Sock}) ->
    io:format("tcp_closed state goto disconnected.~n"),
    {next_state, disconnected, State, 0};

handle_info({timeout, reconnect}, connecting, S) ->
    connect(S);

handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(status, _From, StateName, State) ->
    Statistics = [{N, get(N)} || N <- [inserted]],
    {reply, {StateName, Statistics}, StateName, State};

handle_sync_event(stop, _From, _StateName, State) ->
    {stop, normal, ok, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

send_puback(Sock, Type, MsgId) ->
    Frame = #mqtt_frame{
	       fixed = #mqtt_frame_fixed{type = Type},
	       variable = #mqtt_frame_publish{message_id = MsgId}},
    send_frame(Sock, Frame).

send_disconnect(Sock) ->
    Frame = #mqtt_frame{
	       fixed = #mqtt_frame_fixed{type = ?DISCONNECT,
					 qos = 0,
					 retain = 0,
					 dup = 0}},
    send_frame(Sock, Frame).

send_ping(Sock) ->
    Frame = #mqtt_frame{
	       fixed = #mqtt_frame_fixed{type = ?PINGREQ,
					 qos = 1,
					 retain = 0,
					 dup = 0}},
    send_frame(Sock, Frame).

send_frame(Sock, Frame) ->
    erlang:port_command(Sock, emqtt_frame:serialise(Frame)).

reply(Reply, Name, Ref) ->
    case dict:find(Name, Ref) of
	{ok, []} ->
	    Ref;
	{ok, [From | FromTail]} ->
	    gen_fsm:reply(From, Reply),
	    dict:store(Name, FromTail, Ref)
    end.

