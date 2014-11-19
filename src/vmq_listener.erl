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

-module(vmq_listener).

-behaviour(gen_server).

%% API
-export([start_link/4,
        change_config/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {listener,
                acceptor,
                transport_opts,
                handler_opts,
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
start_link(ListenPort, TransportOpts, HandlerMod, HandlerOpts) ->
    gen_server:start_link(?MODULE, [ListenPort, TransportOpts,
                                    HandlerMod, HandlerOpts], []).

change_config(ListenerPid, Config) ->
    gen_server:call(ListenerPid, {change_config, Config}, infinity).

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
init([ListenPort, TransportOpts, HandlerMod, HandlerOpts]) ->
    process_flag(trap_exit, true),
    TCPListenOptions = vmq_config:get_env(tcp_listen_options),
    case gen_tcp:listen(ListenPort, TCPListenOptions) of
    {ok, ListenSocket} ->
        %%Create first accepting process
        {ok, Ref} = prim_inet:async_accept(ListenSocket, -1),
        {ok, #state{listener = ListenSocket,
                    acceptor = Ref,
                    transport_opts=TransportOpts,
                    handler=HandlerMod,
                    handler_opts=HandlerOpts}};
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
handle_call({change_config, _Config}, _From, State) ->
    %% TODO: implement
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
handle_info({inet_async, ListenSocket, Ref, {ok, TCPSocket}},
            #state{listener=ListenSocket, acceptor=Ref,
                   handler=Handler, handler_opts=HandlerOpts,
                   transport_opts=TransportOpts} = State) ->
    try
        case set_sockopt(ListenSocket, TCPSocket) of
            ok -> ok;
            {error, Reason} -> exit({set_sockopt, Reason})
        end,

        case t(Handler) of
            ssl ->
                %% upgrade TCP socket
                case ssl:ssl_accept(TCPSocket, TransportOpts) of
                    {ok, SSLSocket} ->
                        vmq_session_sup:start_session(SSLSocket, Handler,
                                                      HandlerOpts);
                    {error, Reason1} ->
                        lager:warning("can't upgrade SSL due to ~p", [Reason1])
                end;
            gen_tcp ->
                vmq_session_sup:start_session(TCPSocket, Handler, HandlerOpts)
        end,

        %% Signal the network driver that we are ready to accept another connection
        NNewRef =
        case prim_inet:async_accept(ListenSocket, -1) of
            {ok,    NewRef} ->
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
t(vmq_tcp_listener) -> gen_tcp;
t(vmq_ssl_listener) -> ssl;
t(vmq_ws_listener) -> gen_tcp;
t(vmq_wss_listener) -> ssl.

%% Taken from prim_inet.  We are merely copying some socket options from the
%% listening socket to the new client socket.
set_sockopt(ListSock, CliSocket) ->
    true = inet_db:register_socket(CliSocket, inet_tcp),
    case prim_inet:getopts(ListSock, [active, nodelay, keepalive,
                                      delay_send, priority, tos]) of
        {ok, Opts} ->
            case prim_inet:setopts(CliSocket, Opts) of
                ok    ->
                    ok;
                Error ->
                    gen_tcp:close(CliSocket),
                    Error
            end;
        Error ->
            gen_tcp:close(CliSocket),
            Error
    end.
