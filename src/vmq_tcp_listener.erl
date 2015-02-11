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

-module(vmq_tcp_listener).
-behaviour(gen_server).

%% API
-export([start_link/2,
         setopts/4,
         getopts/1,
         accept/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {listener,
                acceptor,
                tcp_opts,
                other_opts, %% currently only ssl
                mountpoint,
                handler}).

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
start_link(ListenAddr, ListenPort) ->
    gen_server:start_link(?MODULE, [ListenAddr, ListenPort], []).

setopts(ListenerPid, Handler, MountPoint, TransportOpts) ->
    gen_server:call(ListenerPid, {setopts, Handler,
                                  MountPoint, TransportOpts}, infinity).

getopts(ListenerPid) ->
    gen_server:call(ListenerPid, getopts, infinity).

accept(ListenerPid) ->
    gen_server:call(ListenerPid, accept, infinity).

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
init([Addr, Port]) ->
    process_flag(trap_exit, true),
    case gen_tcp:listen(Port, [{ip, Addr}, {reuseaddr, true}]) of
        {ok, ListenSocket} ->
            %%Create first accepting process
            {ok, #state{listener = ListenSocket}};
        {error, Reason} ->
            {stop, Reason}
    end.

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
handle_call({setopts, Handler, MountPoint, {TCPOpts, OtherOpts}}, _From,
            #state{listener=ListenerSocket} = State) ->
    case prim_inet:setopts(ListenerSocket, [binary|TCPOpts]) of
        ok ->
            {reply, ok, State#state{handler=Handler,
                                    mountpoint=MountPoint,
                                    tcp_opts=TCPOpts,
                                    other_opts=OtherOpts}};
        {error, Reason} ->
            lager:error("can't set socket options for handler ~p due to ~p
                         opts: ~p", [Handler, Reason, [binary|TCPOpts]]),
            {reply, {error, {setopts, Reason}}, State}
    end;
handle_call(getopts, _From, State) ->
    #state{handler=Handler,
           mountpoint=MountPoint,
           tcp_opts=TCPOpts,
           other_opts=TransportOpts} = State,
    Reply = {Handler, MountPoint, {TCPOpts, TransportOpts}},
    {reply, Reply, State};
handle_call(accept, _From, #state{listener=ListenSocket} = State) ->
    AcceptorRef =
    case State#state.acceptor of
        undefined ->
            {ok, Ref} = prim_inet:async_accept(ListenSocket, -1),
            Ref;
        Ref ->
            Ref
    end,
    {reply, ok, State#state{acceptor=AcceptorRef}}.

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
handle_info({inet_async, ListenSocket, Ref, {ok, TCPSocket}},
            #state{listener=ListenSocket, acceptor=Ref,
                   handler=Handler, mountpoint=MountPoint,
                   other_opts=TransportOpts} = State) ->
    try
        case set_sockopt(ListenSocket, TCPSocket) of
            ok -> ok;
            {error, Reason} -> exit({set_sockopt, Reason})
        end,

        {ok, Peer} = inet:peername(TCPSocket),

        case apply(Handler, upgrade_connection, [TCPSocket, TransportOpts]) of
            {ok, {UpgradedSocket, SessionOpts}} ->
                {ok, Pid} = supervisor:start_child(
                              vmq_session_sup,
                              [Peer, Handler,
                               [{mountpoint, MountPoint}|SessionOpts]]),
                apply(case Handler of
                          vmq_wss_transport -> ssl;
                          vmq_ssl_transport -> ssl;
                          _ -> gen_tcp
                      end, controlling_process, [UpgradedSocket, Pid]),
                vmq_tcp_transport:handover(Pid, UpgradedSocket);

            {error, _} ->
                ignore
        end,

        %% Signal the network driver that we are ready to accept
        %% another connection
        NNewRef =
        case prim_inet:async_accept(ListenSocket, -1) of
            {ok, NewRef} ->
                NewRef;
            {error, NewRef} ->
                exit({async_accept, inet:format_error(NewRef)})
        end,

        {noreply, State#state{acceptor=NNewRef}}
    catch
        exit:Error ->
            lager:error("Error in async accept: ~p", [Error]),
            {stop, Error, State}
    end;

handle_info({inet_async, ListSock, Ref, Error},
            #state{listener=ListSock, acceptor=Ref} = State) ->
    lager:error("Error in socket acceptor ~p", [Error]),
    {stop, Error, State};

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
terminate(_Reason, #state{listener=ListenSocket}) ->
    gen_tcp:close(ListenSocket),
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

%% Taken from prim_inet.  We are merely copying some socket options from the
%% listening socket to the new client socket.
set_sockopt(ListSock, CliSocket) ->
    true = inet_db:register_socket(CliSocket, inet_tcp),
    case prim_inet:getopts(ListSock, [active, nodelay, keepalive,
                                      delay_send, priority, tos]) of
        {ok, Opts} ->
            set_sockopts(CliSocket, Opts);
        Error ->
            gen_tcp:close(CliSocket),
            Error
    end.

set_sockopts(Socket, Opts) ->
    case vmq_config:get_env(tune_tcp_buffer_size, false) of
        true ->
            case get_max_buffer_size(Socket) of
                {ok, BufSize} ->
                    prim_inet:setopts(Socket, [{buffer, BufSize}|Opts]);
                Error ->
                    gen_tcp:close(Socket),
                    Error
            end;
        false ->
            prim_inet:setopts(Socket, Opts)
    end.

get_max_buffer_size(Socket) ->
    case prim_inet:getopts(Socket, [sndbuf, recbuf, buffer]) of
        {ok, BufSizes} ->
            {ok, lists:max([Sz || {_Opt, Sz} <- BufSizes])};
        Error ->
            Error
    end.
