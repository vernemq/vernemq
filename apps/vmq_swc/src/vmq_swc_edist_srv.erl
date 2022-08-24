%% Copyright 2018 Octavo Labs AG Zurich Switzerland (https://octavolabs.com)
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

-module(vmq_swc_edist_srv).
-include("vmq_swc.hrl").

-behaviour(gen_server).

%% API
-export([
    start_link/1,
    transport_init/2,
    start_connection/2,
    stop_connection/2,
    rpc/5,
    rpc_cast/5
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

-record(state, {config}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec start_link(SwcConfig :: config()) -> {ok, Pid :: pid()} | ignore | {error, Error :: term()}.
start_link(#swc_config{group = SwcGroup} = Config) ->
    gen_server:start_link({local, name(SwcGroup)}, ?MODULE, [Config], []).

transport_init(_Config, _Opts) ->
    ok.

start_connection(_Config, _Member) ->
    ok.

stop_connection(_Config, _Member) ->
    ok.

rpc(SwcGroup, RemotePeer, Module, Function, Args) ->
    gen_server:call({name(SwcGroup), RemotePeer}, {apply, Module, Function, Args}, infinity).

rpc_cast(SwcGroup, RemotePeer, Module, Function, Args) ->
    gen_server:cast({name(SwcGroup), RemotePeer}, {apply, Module, Function, Args}).

name(SwcGroup) ->
    list_to_atom("vmq_swc_edist_" ++ atom_to_list(SwcGroup)).

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
init([Config]) ->
    {ok, #state{config = Config}}.

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
handle_call({apply, Module, Function, Args}, From, State) ->
    _ = rpc_apply(From, Module, Function, Args, State#state.config),
    {noreply, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

rpc_apply(From, Module, Function, Args, Config) ->
    spawn(
        fun() ->
            Reply =
                try
                    apply(Module, Function, Args ++ [Config])
                catch
                    E:R ->
                        {error, {Module, Function, length(Args) + 1, {E, R}}}
                end,
            case From of
                undefined -> ok;
                _ -> gen_server:reply(From, Reply)
            end
        end
    ).

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
handle_cast({apply, Module, Function, Args}, State) ->
    _ = rpc_apply(undefined, Module, Function, Args, State#state.config),
    {noreply, State};
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
