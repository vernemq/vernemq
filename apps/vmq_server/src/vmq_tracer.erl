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

%% @doc This module provides a simple tracing facility for VerneMQ
%% MQTT sessions. The original inspiration for a session tracer came
%% from Fred Hebert's fantastic `recon' tool and we gratefully
%% borrowed some small bits and pieces from there.
%% @end
-module(vmq_tracer).
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("vmq_commons/include/vmq_types.hrl").

-behaviour(gen_server).

%% API
-export([start_link/1,
         stop_tracing/0,
         start_session_trace/2,
         trace_existing_session/0,
         rate_tracer/4
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% for adhoc-testing
-export([sim_client/0]).

-define(SERVER, ?MODULE).

-record(state,
        {
          io_server :: pid(),
          max_rate :: {non_neg_integer(), non_neg_integer()},
          client_id :: client_id(),
          mountpoint :: mountpoint(),
          payload_limit :: non_neg_integer(),
          tracer :: pid(),
          %% A map of all the sessions we are currently tracing. The
          %% key is the pid of the session
          sessions :: list({pid(), reference()}),

          %% The pid of the queue corresponding to the client-id and
          %% mountpoint we're tracing.
          queue :: undefined | pid()
        }).

-type state() :: #state{}.

%% Internal type definitions
-type ftuple() :: {io:format(), [term()]}.
-type unprepf() :: [ftuple() | unprepf()].


%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(map()) -> {ok, Pid::pid()} | ignore | {error, Error::term()}.
start_link(Opts) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Opts, []).

trace_existing_session() ->
    gen_server:call(?SERVER, trace_existing_session).

start_session_trace(SessionPid, ConnFrame) ->
    gen_server:call(?SERVER, {start_session_trace, SessionPid, ConnFrame}).

stop_tracing() ->
    gen_server:call(?SERVER, stop_tracing).

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
init(#{io_server := IoServer,
       max_rate := {Max, Time} = MaxRate,
       mountpoint := Mountpoint,
       client_id := ClientId,
       payload_limit := PayloadLimit}) ->
    process_flag(trap_exit, true),
    TraceFun =
        fun(SessionPid, Frame) ->
                maybe_init_session_trace(SessionPid, Frame, ClientId)
        end,
    Tracer = spawn_link(?SERVER, rate_tracer, [Max, Time, 0, os:timestamp()]),
    vmq_config:set_env(trace_fun, TraceFun, false),
    {ok, #state{
            io_server = IoServer,
            max_rate = MaxRate,
            client_id = ClientId,
            mountpoint = Mountpoint,
            payload_limit = PayloadLimit,
            tracer = Tracer,
            sessions = [],
            queue = undefined
           }}.

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


handle_call(trace_existing_session, _From, #state{client_id = ClientId,
                                                  io_server = IoServer,
                                                  mountpoint = MP} = State) ->
    SId = {MP, ClientId},
    NewState =
        case vmq_queue_sup_sup:get_queue_pid(SId) of
            not_found ->
                io:format(IoServer, "~s No sessions found for client \"~s\"~n", [iso8601() ,ClientId]),
                State;
            QPid when is_pid(QPid) ->
                case vmq_queue:get_sessions(QPid) of
                    [] ->
                        io:format(IoServer, "~s No sessions found for client \"~s\"~n", [iso8601(), ClientId]),
                        State;
                    SPids ->
                        io:format(IoServer, "~s Starting trace for ~p existing sessions for client \"~s\" with PIDs~n"
                                 "    ~p~n", [iso8601(), length(SPids), ClientId, SPids]),
                        begin_session_trace(SPids, State)
                end
    end,
    {reply, ok, NewState};
handle_call({start_session_trace, SessionPid, ConnFrame}, _From,
            #state{client_id = ClientId,
                   payload_limit = PayloadLimit,
                   io_server = IoServer} = State) ->
    Opts = #{payload_limit => PayloadLimit},
    SId = {"", ClientId},
    io:format(IoServer, "~s New session with PID ~p found for client \"~s\"~n", [iso8601(), SessionPid, ClientId]),
    {F, D} = prepf(format_frame(to, SessionPid, os:timestamp(), SId, ConnFrame, Opts)),
    io:format(IoServer, F, D),
    NewState = begin_session_trace([SessionPid], State),
    {reply, ok, NewState};
handle_call(stop_tracing, _From, State) ->
    %% How to disable erlang tracing completely has been borrowed from
    %% the `recon` trace tool.
    erlang:trace(all, false, [all]),
    erlang:trace_pattern({'_','_','_'}, false, [local,meta,call_count,call_time]),
    erlang:trace_pattern({'_','_','_'}, false, []),
    {reply, ok, State}.

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
handle_info({'DOWN', _MRef, process, Pid, _},
            #state{io_server = IoServer,
                   client_id = ClientId} = State) ->
    io:format(IoServer, "~s ~p Trace session for ~s stopped~n", [iso8601(), Pid, ClientId]),
    State1 = remove_session_pid(Pid, State),
    {noreply, State1};
handle_info(Trace, #state{io_server = IoServer,
                          sessions=Sessions} = State) when is_tuple(Trace),
                                                           element(1,Trace) =:= trace_ts ->
    TracePid = get_pid_from_trace(extract_info(Trace)),
    case is_trace_active(TracePid, Sessions) of
        true ->
            {Format, Data} = format_trace(Trace, State),
            io:format(IoServer, Format, Data),
            State1 = handle_trace(Trace, State),
            {noreply, State1};
        false ->
            {noreply, State}
    end;
handle_info({'EXIT',Tracer,normal}, #state{tracer = Tracer,
                                           io_server = IoServer} = State) ->
    io:format(IoServer, "~s Trace rate limit trigged.~n", [iso8601()]),
    {stop, normal, State}.

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
    vmq_config:set_env(trace_fun, undefined, false),
    recon_trace:clear(),
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

-spec handle_trace(term(), state()) -> state().
handle_trace({trace, Pid, return_from, {vmq_plugin,all_till_ok,2}, Ret},
             #state{client_id = ClientId, mountpoint = Mountpoint,
                    io_server = IoServer,
                    tracer = Tracer} = State) ->
    case Ret of
        ok -> State;
        {ok, Payload} when is_binary(Payload) -> State;
        {ok, Modifiers} ->
            %% The only hook returning a subscriber_id as a modifier
            %% is the `auth_on_register` hook, so it should be fine to
            %% 'react' to it here even though this code is common to
            %% all return values from hooks.
            case proplists:get_value(subscriber_id, Modifiers, undefined) of
                undefined -> State;
                {Mountpoint, ClientId} ->
                    %% Modified but still the same, keep tracing!
                    State;
                {NewMountpoint, NewClientId} ->
                    {F, D} =
                        case {NewMountpoint, NewClientId} of
                            {Mountpoint, _} ->
                                {"~p client id for ~s modified to ~p, stopping trace~n",
                                 [Pid, ClientId, NewMountpoint]};
                            {_,ClientId} ->
                                {"~p mountpoint for ~s modified to ~p, stopping trace~n",
                                 [Pid, ClientId, NewMountpoint]};
                            {_,_} ->
                                {"~p mountpoint and client id for ~s modified to ~p and ~p,"
                                 " stopping trace~n",
                                 [Pid, ClientId, NewMountpoint, NewClientId]}
                        end,
                    io:format(IoServer, F, D),
                    State1 = remove_session_pid(Pid, State),
                    setup_trace(get_trace_pids(State1), Tracer),
                    State1
            end;
        _ -> State
    end;
handle_trace(_, State) ->
    State.

extract_info(TraceMsg) ->
    case tuple_to_list(TraceMsg) of
        [trace_ts, Pid, Type | Info] ->
            {TraceInfo, [Timestamp]} = lists:split(length(Info)-1, Info),
            {Type, Pid, Timestamp, TraceInfo};
        [trace, Pid, Type | TraceInfo] ->
            {Type, Pid, os:timestamp(), TraceInfo}
    end.

get_pid_from_trace({_, Pid,_,_}) ->
    Pid.

is_trace_active(Pid, Sessions) ->
    lists:keymember(Pid, 1, Sessions).

maybe_init_session_trace(SessionPid, #mqtt5_connect{client_id = ClientId} = ConnFrame, ClientId) ->
    start_session_trace(SessionPid, ConnFrame);
maybe_init_session_trace(SessionPid, #mqtt_connect{client_id = ClientId} = ConnFrame, ClientId) ->
    start_session_trace(SessionPid, ConnFrame);
maybe_init_session_trace(_,_,_) ->
    ignored.

-spec begin_session_trace(list(pid()), state()) -> state().
begin_session_trace(SessionPids,
                    #state{tracer = Tracer} = State) ->
    case add_session_pids(SessionPids, State) of
        State ->
            %% Dirty way to detect there's nothing new to trace
            %% (sessions are sorted).
            State;
        State1 ->
            setup_trace(get_trace_pids(State1), Tracer),
            State1
    end.

setup_trace(TracePids, Tracer) ->
    TSpecs =
        [{vmq_parser, serialise, vmq_m4_parser_serialize_ms()},
         {vmq_parser_mqtt5, serialise, vmq_m5_parser_serialize_ms()},
         {vmq_mqtt_fsm, connected, vmq_mqtt_fsm_connected_ms()},
         {vmq_mqtt5_fsm, connected, vmq_mqtt5_fsm_connected_ms()},
         {vmq_plugin, all_till_ok, vmq_plugin_hooks_ms()}],

    MatchOpts = [local],
    _Matches =
        [begin
             Arity = '_',
             Spec = Args,
             erlang:trace_pattern({Mod, Fun, Arity}, Spec, MatchOpts)
         end || {Mod, Fun, Args} <- TSpecs],
    [begin
         %% ignore if the process has died and erlang:trace
         %% throws a badarg.
         try
             erlang:trace(PSpec, true, [call, timestamp, {tracer, Tracer}])
         catch
             error:badarg -> ok
         end
     end
     || PSpec <- TracePids].

-spec add_session_pids(list(pid()), state()) -> state().
add_session_pids(SessionPids, #state{sessions = Sessions} = State) ->
    NewSessions = [{Pid, monitor(process, Pid)} || Pid <- SessionPids],
    State#state{sessions = lists:ukeysort(1, NewSessions ++ Sessions)}.

remove_session_pid(Pid, #state{sessions = Sessions} = State) ->
    {_, MRef} = lists:keyfind(Pid, 1, Sessions),
    demonitor(MRef, [flush]),
    State#state{sessions = lists:keydelete(Pid, 1, Sessions)}.

-spec get_trace_pids(state()) -> list(pid()).
get_trace_pids(#state{sessions = Sessions, queue = QueuePid}) ->
    case QueuePid of
        undefined ->
            [P || {P, _} <- Sessions];
        _ ->
            SessionPids = [P || {P, _} <- Sessions],
            [QueuePid | SessionPids]
    end.

%% This rate limit function was borrowed almost as is from the recon
%% trace tool developed by Fred Hebert.
rate_tracer(Max, Time, Count, Start) ->
    receive
        Msg ->
            vmq_tracer ! Msg,
            Now = os:timestamp(),
            Delay = timer:now_diff(Now, Start) div 1000,
            if Delay > Time -> rate_tracer(Max, Time, 0, Now)
             ; Max > Count -> rate_tracer(Max, Time, Count+1, Start)
             ; Max =:= Count -> exit(normal)
            end
    end.

vmq_m4_parser_serialize_ms() ->
    dbg:fun2ms(
      fun([#mqtt_connect{}]) -> ok;
         ([#mqtt_connack{}]) -> ok;
         ([#mqtt_publish{}]) -> ok;
         ([#mqtt_puback{}]) -> ok;
         ([#mqtt_pubrec{}]) -> ok;
         ([#mqtt_pubrel{}]) -> ok;
         ([#mqtt_pubcomp{}]) -> ok;
         ([#mqtt_subscribe{}]) -> ok;
         ([#mqtt_unsubscribe{}]) -> ok;
         ([#mqtt_suback{}]) -> ok;
         ([#mqtt_unsuback{}]) -> ok;
         ([#mqtt_pingreq{}]) -> ok;
         ([#mqtt_pingresp{}]) -> ok;
         ([#mqtt_disconnect{}]) -> ok end).

vmq_m5_parser_serialize_ms() ->
    dbg:fun2ms(
      fun([#mqtt5_connect{}]) -> ok;
         ([#mqtt5_connack{}]) -> ok;
         ([#mqtt5_publish{}]) -> ok;
         ([#mqtt5_puback{}]) -> ok;
         ([#mqtt5_pubrec{}]) -> ok;
         ([#mqtt5_pubrel{}]) -> ok;
         ([#mqtt5_pubcomp{}]) -> ok;
         ([#mqtt5_subscribe{}]) -> ok;
         ([#mqtt5_unsubscribe{}]) -> ok;
         ([#mqtt5_suback{}]) -> ok;
         ([#mqtt5_unsuback{}]) -> ok;
         ([#mqtt5_pingreq{}]) -> ok;
         ([#mqtt5_pingresp{}]) -> ok;
         ([#mqtt5_disconnect{}]) -> ok;
         ([#mqtt5_auth{}]) -> ok end).

vmq_mqtt_fsm_connected_ms() ->
    dbg:fun2ms(
      fun([#mqtt_connect{},_]) -> ok;
         ([#mqtt_connack{},_]) -> ok;
         ([#mqtt_publish{},_]) -> ok;
         ([#mqtt_puback{},_]) -> ok;
         ([#mqtt_pubrec{},_]) -> ok;
         ([#mqtt_pubrel{},_]) -> ok;
         ([#mqtt_pubcomp{},_]) -> ok;
         ([#mqtt_subscribe{},_]) -> ok;
         ([#mqtt_unsubscribe{},_]) -> ok;
         ([#mqtt_suback{},_]) -> ok;
         ([#mqtt_unsuback{},_]) -> ok;
         ([#mqtt_pingreq{},_]) -> ok;
         ([#mqtt_pingresp{},_]) -> ok;
         ([#mqtt_disconnect{},_]) -> ok end).

vmq_mqtt5_fsm_connected_ms() ->
    dbg:fun2ms(
      fun([#mqtt5_connect{},_]) -> ok;
         ([#mqtt5_connack{},_]) -> ok;
         ([#mqtt5_publish{},_]) -> ok;
         ([#mqtt5_puback{},_]) -> ok;
         ([#mqtt5_pubrec{},_]) -> ok;
         ([#mqtt5_pubrel{},_]) -> ok;
         ([#mqtt5_pubcomp{},_]) -> ok;
         ([#mqtt5_subscribe{},_]) -> ok;
         ([#mqtt5_unsubscribe{},_]) -> ok;
         ([#mqtt5_suback{},_]) -> ok;
         ([#mqtt5_unsuback{},_]) -> ok;
         ([#mqtt5_pingreq{},_]) -> ok;
         ([#mqtt5_pingresp{},_]) -> ok;
         ([#mqtt5_disconnect{},_]) -> ok;
         ([#mqtt5_auth{},_]) -> ok end).

vmq_plugin_hooks_ms() ->
    [{[auth_on_register,'_'],[],[{return_trace}]},
     {[auth_on_publish,'_'],[],[{return_trace}]},
     {[auth_on_subscribe,'_'],[],[{return_trace}]},
     {[auth_on_register_m5,'_'],[],[{return_trace}]},
     {[auth_on_publish_m5,'_'],[],[{return_trace}]},
     {[auth_on_subscribe_m5,'_'],[],[{return_trace}]},
     {[on_auth_m5,'_'],[],[{return_trace}]}
     %%{[on_register,'_'],[],[{return_trace}]},
     %%{[on_publish,'_'],[],[{return_trace}]},
     %%{[on_deliver,'_'],[],[{return_trace}]},
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Trace formatting
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec format_trace(term(), state()) -> ftuple().
format_trace(Trace, #state{client_id=ClientId,
                           mountpoint=Mountpoint,
                           payload_limit=PayloadLimit}) ->
    SId = {Mountpoint, ClientId},
    Opts = #{payload_limit => PayloadLimit,
             sid => SId},
    Unprepared =
        case extract_info(Trace) of
            {call, Pid, Timestamp, [{vmq_parser, serialise, [Msg]}]} ->
                format_frame(from, Pid, Timestamp, SId, Msg, Opts);
            {call, Pid, Timestamp, [{vmq_mqtt_fsm, connected, [Msg, _]}]} ->
                format_frame(to, Pid, Timestamp, SId, Msg, Opts);
            {call, Pid, Timestamp, [{vmq_parser_mqtt5, serialise, [Msg]}]} ->
                format_frame(from, Pid, Timestamp, SId, Msg, Opts);
            {call, Pid, Timestamp, [{vmq_mqtt5_fsm, connected, [Msg, _]}]} ->
                format_frame(to, Pid, Timestamp, SId, Msg, Opts);
            {call, Pid, Timestamp, [{vmq_plugin, all_till_ok, [Hook, Args]}]} ->
                format_all_till_ok(Hook, Pid, Timestamp, Args, Opts);
            {return_from, Pid, Timestamp, [{vmq_plugin,all_till_ok,2}, Ret]} ->
                format_all_till_ok_ret(Ret, Pid, Timestamp, Opts);
            _ ->
                format_unknown_trace(Trace)
        end,
    prepf(lists:flatten(Unprepared)).

format_all_till_ok(Hook, Pid, Timestamp, Args, Opts) ->
    [ftimestamp(Timestamp), r(" "), fpid(Pid), r(" "), format_all_till_ok_(Hook, Args, Opts)].

format_all_till_ok_(auth_on_register = Hook,
                    [Peer, SubscriberId, User, Password, CleanSession], _Opts) ->
    {"Calling ~p(~p,~p,~s,~s,~p) ~n",
     [Hook, Peer, SubscriberId, User, Password, CleanSession]};
format_all_till_ok_(auth_on_publish = Hook, [User, SubscriberId, QoS, Topic, Payload, IsRetain],
                   _Opts) ->
    {"Calling ~p(~s,~p,~p,~s,~p) with payload:~n"
     "    ~s~n",
     [Hook, User, SubscriberId, QoS, jtopic(Topic), IsRetain, Payload]};
format_all_till_ok_(auth_on_subscribe = Hook, [User, SubscriberId, Topics], _Opts) ->
    [{"Calling ~p(~s,~p) with topics:~n",
      [Hook, User, SubscriberId]}, ftopics(Topics)];

format_all_till_ok_(auth_on_register_m5 = Hook,
                    [Peer, SubscriberId, User, Password, CleanStart, Props], Opts) ->
    [{"Calling ~p(~p,~p,~s,~s,~p) ~n",
     [Hook, Peer, SubscriberId, User, Password, CleanStart]},
     format_props(Props, Opts)];
format_all_till_ok_(auth_on_publish_m5 = Hook,
                    [User, SubscriberId, QoS, Topic, Payload, IsRetain, Props],
                    Opts) ->
    [{"Calling ~p(~s,~p,~p,~s,~p) with payload:~n"
      "    ~s~n",
      [Hook, User, SubscriberId, QoS, jtopic(Topic), IsRetain, Payload]},
     format_props(Props, Opts)];
format_all_till_ok_(auth_on_subscribe_m5 = Hook, [User, SubscriberId, Topics, Props], Opts) ->
    [{"Calling ~p(~s,~p) with topics:~n",
      [Hook, User, SubscriberId]}, ftopics(Topics), format_props(Props, Opts)].


format_all_till_ok_ret(Ret, Pid, Timestamp, Opts) ->
    [ftimestamp(Timestamp), r(" "), fpid(Pid), r(" "), format_all_till_ok_ret_(Ret, Opts)].

format_all_till_ok_ret_(ok, _Opts) ->
    {"Hook returned \"ok\"~n", []};
format_all_till_ok_ret_({ok, []}, _Opts) ->
    {"Hook returned \"ok\"~n", []};
format_all_till_ok_ret_({ok, Payload}, #{payload_limit := Limit}) when is_binary(Payload) ->
    {"Hook returned \"ok\" with modified payload:~n"
     "    ~s~n", [trunc_payload(Payload, Limit)]};
format_all_till_ok_ret_({ok, Modifiers}, _Opts) ->
    [{"Hook returned \"ok\" with modifiers:~n", []},
     fmodifiers(Modifiers)];
format_all_till_ok_ret_(Other,_Opts) ->
    {"Hook returned ~p~n", [Other]}.

format_frame(Direction, Pid, Timestamp, SId, M, Opts) ->
    [ftimestamp(Timestamp), r(" "), fpid(Pid), r(" "), dir(Direction), r(" "),
     sid(SId), r(" "), format_frame_(M, Opts)].

format_props(#{}=M, _Opts) when map_size(M) =:= 0 ->
    [];
format_props(undefined, _Opts) ->
    [];
format_props(Props, _Opts) ->
    {"    with properties: ~p~n", [Props]}.

format_lwt(undefined,_Opts) ->
    [];
format_lwt(#mqtt5_lwt{
              will_properties = Props,
              will_retain = Retain,
              will_qos = QoS,
              will_topic = Topic,
              will_msg = Msg
             }, Opts) ->
    format_lwt(Retain, QoS, Topic, Msg, Props, Opts).

format_lwt(Retain, _QoS, _Topic, _Msg, _Props, _Opts)
  when Retain =:= undefined ->
    [];
format_lwt(Retain, QoS, Topic, Msg, Props, #{payload_limit := Limit} = Opts) ->
    [{"    with LWT(wr: ~p, wq: ~p, wt: ~s) with payload:~n"
     "    ~s~n",
      [Retain, QoS, jtopic(Topic), trunc_payload(Msg, Limit)]},
     format_props(Props, Opts)].

format_frame_(#mqtt_pingreq{}, _) ->
    {"PINGREQ()~n", []};
format_frame_(#mqtt_pingresp{}, _) ->
    {"PINGRESP()~n", []};
format_frame_(#mqtt_connect{proto_ver = Ver, username = Username,
                           password = Password, clean_session = CleanSession,
                           keep_alive = KeepAlive, client_id = ClientId, will_retain = WillRetain,
                           will_qos = WillQoS, will_topic = WillTopic, will_msg = WillMsg}, Opts) ->
    [{"CONNECT(c: ~s, v: ~p, u: ~s, p: ~s, cs: ~p, ka: ~p)~n",
      [ClientId, Ver, Username, Password, CleanSession, KeepAlive]},
     format_lwt(WillRetain, WillQoS, WillTopic, WillMsg, undefined, Opts)];
format_frame_(#mqtt_connack{session_present = SP, return_code = RC}, _) ->
    {"CONNACK(sp: ~p, rc: ~p)~n", [fflag(SP), RC]};
format_frame_(#mqtt_publish{message_id = MId, topic = Topic, qos = QoS, retain = Retain,
                            dup = Dup, payload = Payload}, #{payload_limit := Limit}) ->
    {"PUBLISH(d~p, q~p, r~p, m~p, \"~s\") with payload:~n"
     "    ~s~n", [fflag(Dup), QoS, fflag(Retain), fmid(MId), jtopic(Topic), trunc_payload(Payload, Limit)]};
format_frame_(#mqtt_puback{message_id = MId}, _) ->
    {"PUBACK(m~p)~n", [fmid(MId)]};
format_frame_(#mqtt_pubrec{message_id = MId}, _) ->
    {"PUBREC(m~p)~n", [fmid(MId)]};
format_frame_(#mqtt_pubrel{message_id = MId}, _) ->
    {"PUBREL(m~p)~n", [fmid(MId)]};
format_frame_(#mqtt_pubcomp{message_id = MId}, _) ->
    {"PUBCOMP(m~p)~n", [fmid(MId)]};
format_frame_(#mqtt_subscribe{message_id = MId, topics = Topics}, _) ->
    [{"SUBSCRIBE(m~p) with topics:~n", [fmid(MId)]}, ftopics(Topics)];
format_frame_(#mqtt_suback{message_id = MId, qos_table = QoSTable}, _) ->
    {"SUBACK(m~p, qt~p)~n", [fmid(MId), QoSTable]};
format_frame_(#mqtt_unsubscribe{message_id = MId}, _) ->
    {"UNSUBSCRIBE(m~p)~n", [fmid(MId)]};
format_frame_(#mqtt_unsuback{message_id = MId}, _) ->
    {"UNSUBACK(m~p)~n", [fmid(MId)]};
format_frame_(#mqtt_disconnect{}, _) ->
    {"DISCONNECT()~n", []};

format_frame_(#mqtt5_pingreq{}, _) ->
    {"PINGREQ()~n", []};
format_frame_(#mqtt5_pingresp{}, _) ->
    {"PINGRESP()~n", []};
format_frame_(#mqtt5_connect{proto_ver = Ver, username = Username,
                            password = Password, clean_start = CleanStart,
                            keep_alive = KeepAlive, client_id = ClientId,
                            lwt = LWT, properties = Props}, Opts) ->
    [{"CONNECT(c: ~s, v: ~p, u: ~s, p: ~s, cs: ~p, ka: ~p)~n",
      [ClientId, Ver, Username, Password, CleanStart, KeepAlive]},
     format_props(Props, Opts),
     format_lwt(LWT, Opts)];
format_frame_(#mqtt5_connack{session_present = SP, reason_code = RC,
                             properties=Props}, Opts) ->
    [{"CONNACK(sp: ~p, rc: ~p(~p))~n", [fflag(SP), rc2rcn(RC), RC]}, format_props(Props, Opts)];
format_frame_(#mqtt5_publish{message_id = MId, topic = Topic, qos = QoS, retain = Retain,
                             dup = Dup, payload = Payload, properties = Props},
              #{payload_limit := Limit} = Opts) ->
    [{"PUBLISH(d~p, q~p, r~p, m~p, \"~s\") with payload:~n"
     "    ~s~n", [fflag(Dup), QoS, fflag(Retain), fmid(MId), jtopic(Topic), trunc_payload(Payload, Limit)]},
     format_props(Props, Opts)];
format_frame_(#mqtt5_puback{message_id = MId, reason_code=RC, properties=Props}, Opts) ->
    [{"PUBACK(m~p, rc: ~p(~p))~n", [fmid(MId), rc2rcn(RC), RC]},
     format_props(Props, Opts)];
format_frame_(#mqtt5_pubrec{message_id = MId, reason_code=RC, properties=Props}, Opts) ->
    [{"PUBREC(m~p), rc: ~p(~p))~n", [fmid(MId), rc2rcn(RC), RC]},
     format_props(Props, Opts)];
format_frame_(#mqtt5_pubrel{message_id = MId, reason_code=RC, properties=Props}, Opts) ->
    [{"PUBREL(m~p), rc: ~p(~p))~n", [fmid(MId), rc2rcn(RC), RC]},
     format_props(Props, Opts)];
format_frame_(#mqtt5_pubcomp{message_id = MId, reason_code=RC, properties=Props}, Opts) ->
    [{"PUBCOMP(m~p), rc: ~p(~p))~n", [fmid(MId), rc2rcn(RC), RC]},
     format_props(Props, Opts)];
format_frame_(#mqtt5_subscribe{message_id = MId, topics = Topics, properties=Props}, Opts) ->
    [{"SUBSCRIBE(m~p) with topics:~n", [fmid(MId)]},
     ftopics(Topics), format_props(Props, Opts)];
format_frame_(#mqtt5_suback{message_id = MId, reason_codes = RCs, properties = Props}, Opts) ->
    [{"SUBACK(m~p, reason_codes:~p)~n", [fmid(MId), RCs]},
     format_props(Props, Opts)];
format_frame_(#mqtt5_unsubscribe{message_id = MId, topics=Topics, properties=Props}, Opts) ->
    [{"UNSUBSCRIBE(m~p) with topics:~n", [fmid(MId)]},
     ftopics(Topics), format_props(Props, Opts)];
format_frame_(#mqtt5_unsuback{message_id = MId, reason_codes=RCs, properties=Props}, Opts) ->
    [{"UNSUBACK(m~p, reason_codes:~p)~n", [fmid(MId), RCs]},
     format_props(Props, Opts)];
format_frame_(#mqtt5_disconnect{reason_code=RC, properties=Props}, Opts) ->
    [{"DISCONNECT(rc:~p(~p))~n", [disconnectrc2rcn(RC), RC]},
     format_props(Props, Opts)];
format_frame_(#mqtt5_auth{reason_code=RC, properties=Props}, Opts) ->
    [{"AUTH(rc:~p(~p))~n", [rc2rcn(RC), RC]},
     format_props(Props, Opts)];

format_frame_(Unknown,_) ->
    {io_lib:format("UNKNOWN: ~p~n", [Unknown]), []}.


trunc_payload(Payload, Limit) when byte_size(Payload) =< Limit ->
    Payload;
trunc_payload(Payload, Limit) ->
    <<Truncated:Limit/binary, _/binary>> = Payload,
    <<Truncated/binary, " (truncated)">>.


%% takes a list of possibly nested {FormatString,Data} {tuples and
%% normalizes it into a single {F,D} tuple.
-spec prepf(unprepf()) -> ftuple().
prepf(L) ->
    {F, D} =
        lists:foldl(
          fun({F,D}, {FAcc, DAcc}) ->
                  {[F|FAcc], [D|DAcc]};
             (S, {FAcc, DAcc}) when is_list(S) ->
                  {[S | FAcc], DAcc}
          end,
          {[],[]},
          lists:flatten(L)),
    {lists:concat(lists:reverse(F)), lists:concat(lists:reverse(D))}.

r(S) ->
    {S, []}.

fmid(undefined) -> 0;
fmid(Mid) -> Mid.

fflag(1) -> 1;
fflag(0) -> 0;
fflag(true) -> 1;
fflag(false) -> 0.

jtopic(T) when is_list(T) ->
    erlang:iolist_to_binary(vmq_topic:unword(T)).

ftopics(Topics) ->
    lists:foldl(
      fun({Topic, QoS}, Acc) when is_integer(QoS), is_list(Topic) ->
              [{"    q:~p, t: \"~s\"~n", [QoS, jtopic(Topic)]}|Acc];
         ({Topic, {QoS, SubOpts}}, Acc) ->
              NL = maps:get(no_local, SubOpts, undefined),
              RAP = maps:get(rap, SubOpts, undefined),
              RH = maps:get(retain_handling, SubOpts, undefined),
              [{"    q:~p, no_local:~p, rap:~p, rh:~p~n"
                "    t: \"~s\"~n", [QoS, NL, RAP, RH, jtopic(Topic)]}|Acc];
         (#mqtt5_subscribe_topic{
             topic = Topic,
             qos = QoS,
             no_local = NL,
             rap = RAP,
             retain_handling = RH
            }, Acc) ->
              [{"    q:~p, no_local:~p, rap:~p, rh:~p~n"
                "    t: \"~s\"~n", [QoS, NL, RAP, RH, jtopic(Topic)]}|Acc]
      end,
      [],
      Topics).

fmodifiers(Modifiers) ->
    lists:foldl(
      fun({retry_interval, Val}, Acc) ->
              [{"    retry_interval: ~pms~n", [Val]}|Acc];
         ({upgrade_qos, Val}, Acc) ->
              [{"    upgrade_qos: ~p~n", [Val]}|Acc];
         ({max_message_rate, Val}, Acc) ->
              [{"    max_message_rate: ~p msgs/s~n", [Val]}|Acc];
         ({max_message_size, Val}, Acc) ->
              [{"    max_message_size: ~p bytes~n", [Val]}|Acc];
         ({max_inflight_messages, Val}, Acc) ->
              [{"    max_inflight_messages: ~p msgs~n", [Val]}|Acc];
         ({clean_session, Val}, Acc) ->
              [{"    clean_session: ~p~n", [Val]}|Acc];
         (V, Acc) ->
              [{"    ~p~n", [V]}|Acc]
      end,
      [],
      Modifiers).

fpid(Pid) ->
    {"~p", [Pid]}.

ftimestamp(Timestamp) ->
    Iso8601Formatted = iso8601(Timestamp),
    {"~s", [Iso8601Formatted]}.

%% @doc Convert a `os:timestamp()' or a calendar-style `{date(), time()}'
%% tuple to an ISO 8601 formatted binary. Note that this function always
%% returns a binary with no offset (i.e., ending in "Z").
iso8601() ->
    Timestamp = os:timestamp(),
    iso8601(Timestamp).

iso8601({_, _, _}=Timestamp) ->
    iso8601(calendar:now_to_datetime(Timestamp));
iso8601({{Y, Mo, D}, {H, Mn, S}}) when is_float(S) ->
    FmtStr = "~4.10.0B-~2.10.0B-~2.10.0BT~2.10.0B:~2.10.0B:~9.6.0fZ",
    IsoStr = io_lib:format(FmtStr, [Y, Mo, D, H, Mn, S]),
    list_to_binary(IsoStr);
iso8601({{Y, Mo, D}, {H, Mn, S}}) ->
    FmtStr = "~4.10.0B-~2.10.0B-~2.10.0BT~2.10.0B:~2.10.0B:~2.10.0BZ",
    IsoStr = io_lib:format(FmtStr, [Y, Mo, D, H, Mn, S]),
    list_to_binary(IsoStr).

sid({"", CId}) ->
    {"CID: \"~s\"", [CId]};
sid({MP, CId}) ->
    {"MP: \"~s\" CID: \"~s\"", [MP, CId]}.

dir(from) -> {"MQTT SEND:", []};
dir(to) -> {"MQTT RECV:", []}.

format_unknown_trace(V) ->
    [{"~s Unknown trace! ~p~n", [iso8601(), V]}].

sim_client() ->
    Connect = packetv5:gen_connect("simclient", [{keepalive, 60}]),
    Connack = packetv5:gen_connack(0, 0, #{}),
    {ok, S} = packetv5:do_client_connect(Connect, Connack, [{port, 1883}]),
    Topic = <<"sim/topic">>,
    Subscribe = packetv5:gen_subscribe(77, [packetv5:gen_subtopic(Topic,0)],
                                       #{p_user_property => [{<<"key1">>, <<"val1">>},
                                                             {<<"key2">>, <<"val2">>}]}),
    ok = gen_tcp:send(S, Subscribe),
    SubAck = packetv5:gen_suback(77, [0], #{}),
    ok = packetv5:expect_frame(S, SubAck),
    Pub = packetv5:gen_publish(Topic, 0, <<"simmsg">>,
                               [{properties, #{p_user_property =>
                                                   [{<<"key1">>, <<"val1">>},
                                                    {<<"key1">>, <<"val2">>},
                                                    {<<"key2">>, <<"val2">>}]}}]),
    ok = gen_tcp:send(S, Pub).

rc2rcn(RC) ->
    vmq_parser_mqtt5:rc2rcn(RC).

disconnectrc2rcn(?M5_NORMAL_DISCONNECT) ->
    ?NORMAL_DISCONNECT;
disconnectrc2rcn(RC) -> rc2rcn(RC).
