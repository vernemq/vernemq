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

-module(vmq_session_sup_sup).
-include_lib("public_key/include/public_key.hrl").
-behaviour(gen_server).

%% API
-export([start_link/3,
         reader/1,
         writer/1,
         session/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {writer_pid,
                reader_pid,
                session_pid}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Socket, Handler, Opts) ->
    gen_server:start_link(?MODULE, [Socket, Handler, Opts], []).

reader(Pid) ->
    call(Pid, {get, reader}).

writer(Pid) ->
    call(Pid, {get, writer}).

session(Pid) ->
    call(Pid, {get, session}).

call(Pid, Req) ->
    gen_server:call(Pid, Req).

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
init([Socket, Handler, Opts]) ->
    Transport = t(Handler),
    {ok, WriterPid} = vmq_writer:start_link(Transport, Handler, Socket),
    {ok, Peer} = apply(i(Transport), peername, [Socket]),
    NewOpts =
    case Transport of
        ssl ->
            case proplists:get_value(use_identity_as_username, Opts, false) of
                true ->
                    [{preauth, socket_to_common_name(Socket)}|Opts];
                false ->
                    Opts
            end;
        _ ->
            Opts
    end,
    SendFun = fun(F) -> vmq_writer:send(WriterPid, F), ok end,
    {ok, SessionPid} = vmq_session:start_link(Peer, SendFun, NewOpts),
    {ok, ReaderPid} = vmq_reader:start_link(SessionPid, Handler, Transport),
    process_flag(trap_exit, true),
    {ok, #state{writer_pid=WriterPid,
                reader_pid=ReaderPid,
                session_pid=SessionPid}}.

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
handle_call({get, What}, _From, State) ->
    Reply =
    case What of
        reader -> State#state.reader_pid;
        writer -> State#state.writer_pid;
        session -> State#state.session_pid
    end,
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
handle_info({'EXIT', Pid, Reason}, #state{writer_pid=Pid} = State) ->
    maybe_log_reason(writer, Pid, Reason),
    shutdown([reader_pid, session_pid], State);
handle_info({'EXIT', Pid, Reason}, #state{reader_pid=Pid} = State) ->
    maybe_log_reason(reader, Pid, Reason),
    shutdown([writer_pid, session_pid], State);
handle_info({'EXIT', Pid, Reason}, #state{session_pid=Pid} = State) ->
    maybe_log_reason(session, Pid, Reason),
    shutdown([reader_pid, writer_pid], State);
handle_info(timeout, State) ->
    maybe_terminate(State);
handle_info(_Info, State) ->
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
t(vmq_tcp_listener) -> gen_tcp;
t(vmq_ssl_listener) -> ssl;
t(vmq_ws_listener) -> gen_tcp;
t(vmq_wss_listener) -> ssl.

i(gen_tcp) -> inet;
i(ssl) -> ssl.

maybe_log_reason(What, Pid, normal) ->
    lager:debug("~p ~p terminated normally", [What, Pid]);
maybe_log_reason(What, Pid, shutdown) ->
    lager:debug("~p ~p terminated due to shutdown", [What, Pid]);
maybe_log_reason(What, Pid, Reason) ->
    lager:warning("~p ~p terminated abnormally due to ~p", [What, Pid, Reason]).


shutdown([reader_pid|Rest], #state{reader_pid=Pid} = State) ->
    exit(Pid, shutdown),
    shutdown(Rest, State);
shutdown([writer_pid|Rest], #state{writer_pid=Pid} = State) ->
    exit(Pid, shutdown),
    shutdown(Rest, State);
shutdown([session_pid|Rest], #state{session_pid=Pid} = State) ->
    exit(Pid, shutdown),
    shutdown(Rest, State);
shutdown([], State) -> {noreply, State, 0}.

maybe_terminate(#state{reader_pid=ReaderPid,
                       writer_pid=WriterPid,
                       session_pid=SessionPid} = State) ->
    case [P || P <- [ReaderPid, WriterPid, SessionPid],
               is_process_alive(P)] of
        [] ->
            {stop, normal, State};
        _ ->
            {noreply, State, 100}
    end.


-spec socket_to_common_name({'sslsocket',_,pid() | {port(),_}}) ->
                                   'undefined' | [any()].
socket_to_common_name(Socket) ->
    case ssl:peercert(Socket) of
        {error, no_peercert} ->
            undefined;
        {ok, Cert} ->
            OTPCert = public_key:pkix_decode_cert(Cert, otp),
            TBSCert = OTPCert#'OTPCertificate'.tbsCertificate,
            Subject = TBSCert#'OTPTBSCertificate'.subject,
            extract_cn(Subject)
    end.

-spec extract_cn({'rdnSequence', list()}) -> undefined | list().
extract_cn({rdnSequence, List}) ->
    extract_cn2(List).

-spec extract_cn2(list()) -> undefined | list().
extract_cn2([[#'AttributeTypeAndValue'{
                 type=?'id-at-commonName',
                 value={utf8String, CN}}]|_]) ->
    unicode:characters_to_list(CN);
extract_cn2([_|Rest]) ->
    extract_cn2(Rest);
extract_cn2([]) -> undefined.
