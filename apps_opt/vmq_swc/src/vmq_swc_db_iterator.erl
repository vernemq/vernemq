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

-module(vmq_swc_db_iterator).
-include("vmq_swc.hrl").
-behaviour(gen_server).
-export([start_link/1,
         iterator/5,
         iterator_next/1,
         iterator_close/1,

         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).


-record(iterator, {backend :: module(),
                   ref :: reference(),
                   state :: any(),
                   proc :: pid()}).
-type iterator() :: #iterator{}.

-export_type([iterator/0]).

-record(state, {config, iterators}).

start_link(#swc_config{itr=ItrName} = Config) ->
    gen_server:start_link({local, ItrName}, ?MODULE, [Config], []).

-spec iterator(config(), module(), type(), key() | first, opts()) -> iterator().
iterator(#swc_config{itr=ItrName}, Backend, Type, FirstKey, Opts) ->
    gen_server:call(ItrName, {iterator_new, Backend, Type, FirstKey, Opts, self()}, infinity).

-spec iterator_next(iterator()) -> {{key(), value()}, iterator()} | '$end_of_table'.
iterator_next(#iterator{backend=Backend, state=IteratorState0} = Itr) ->
    case Backend:iterator_next(IteratorState0) of
        {KV, IteratorState1} ->
            {KV, Itr#iterator{state=IteratorState1}};
        '$end_of_table' = EoT ->
            iterator_close(Itr),
            EoT
    end.

-spec iterator_close(iterator()) -> ok.
iterator_close(Iterator) ->
    gen_server:call(Iterator#iterator.proc, {iterator_close, Iterator}, infinity).


init([#swc_config{} = Config]) ->
    {ok, #state{config=Config, iterators= #{}}}.

handle_call({iterator_new, Backend, Type, FirstKey, Opts, Owner}, _From, #state{config=Config, iterators=Itrs0} = State0) ->
    IteratorState = Backend:iterator(Config, Type, FirstKey, Opts),
    IteratorRef = make_ref(),
    Mref = monitor(process, Owner),
    Iterator = #iterator{backend=Backend, proc=self(), ref=IteratorRef, state=IteratorState},
    Itrs1 = maps:put({monitor, Mref}, Iterator, Itrs0),
    Itrs2 = maps:put({iterator, IteratorRef}, {monitor, Mref}, Itrs1),
    {reply, Iterator, State0#state{iterators=Itrs2}};

handle_call({iterator_close, #iterator{backend=Backend, ref=IteratorRef, state=IteratorState}}, _From, #state{iterators=Itrs0} = State0) ->
    {monitor, Mref} = maps:get({iterator, IteratorRef}, Itrs0),
    demonitor(Mref, [flush]),
    Itrs1 = maps:remove({iterator, IteratorRef}, Itrs0),
    Itrs2 = maps:remove({monitor, Mref}, Itrs1),
    Backend:iterator_close(IteratorState),
    {reply, ok, State0#state{iterators=Itrs2}};

handle_call(_Req, _From, State) ->
    {reply, {error, not_implemented}, State}.

handle_cast(_Req, State) ->
    {noreply, State}.

handle_info({'DOWN', Mref, process, _Pid, _Reason}, #state{iterators=Itrs0} = State0) ->
    #iterator{backend=Backend, ref=IteratorRef, state=IteratorState} = maps:get({monitor, Mref}, Itrs0),
    Itrs1 = maps:remove({monitor, Mref}, Itrs0),
    Itrs2 = maps:remove({iterator, IteratorRef}, Itrs1),
    Backend:iterator_close(IteratorState),
    {noreply, State0#state{iterators=Itrs2}}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, _NewVsn, State) ->
    State.
